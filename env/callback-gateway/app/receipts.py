"""外部通道回执与送达确认（external channel receipts & delivery confirmation）。

在既有通知路由（``notif_send_tasks``）、旧链路发送任务
（``approval_notification_deliveries``）与审计（``events``）之上，提供：

1. **消息登记**：email/webhook 发送成功后登记外部服务返回的 ``message_id``
   （``external_messages``），它是回执匹配与送达确认的锚点；同一发送任务重试/故障
   转移会登记多条，旧行置 ``superseded``，轨迹保留。
2. **签名回执入口**（``POST /receipts/{channel}``）：按通道独立密钥环 HMAC-SHA256
   验签后原文落盘（``receipts``，永不改写），处理 ``delivered``/``bounced``/
   ``complained``/``expired`` 等结果。
3. **幂等与终态保护**：重复回执由
   ``UNIQUE(channel,message_id,event,receipt_hash)`` 幂等；乱序回执不允许把已确认
   终态改回处理中（首条终态赢，迟到终态只留历史）；无法匹配发送任务的回执进待核对
   队列（``matched=unmatched``），人工可绑定但不能改写原始回执。
4. **超时扫描与策略**：发送任务在 ``confirm_timeout_seconds`` 内没有回执，由后台扫描
   标记 ``awaiting_confirmation``，再按当前策略自动沿通道计划故障转移重试
   （``confirm_retries`` 计数），超限或策略为 manual 时转人工（任务
   ``awaiting_manual``）。
5. **管理与重放**：按通道/接收人/时间/状态查询回执原文、消息状态变化历史与待核对
   队列；``POST .../receipts/{id}/replay`` 用存证原文安全重放（幂等、无第二次外部
   效果）。

所有状态转移都是写事务（BEGIN IMMEDIATE，全局串行）内的条件更新 + 唯一约束：服务
重启、外部重复投递、并发消费只能有一个赢家，不会重复改变状态，也不会产生第二次外部
效果（重试/故障转移只调度发送任务，实际外发仍走既有的单赢家领取与通道尝试幂等）。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import logging
import re
import sqlite3
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import audit
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.receipts")

# ---- 常量 -------------------------------------------------------------------

CHANNEL_EMAIL = "email"
CHANNEL_WEBHOOK = "webhook"
EXTERNAL_CHANNELS = (CHANNEL_EMAIL, CHANNEL_WEBHOOK)

SOURCE_ROUTE = "route"
SOURCE_LEGACY = "legacy"

# 回执事件（规范化后）
EV_DELIVERED = "delivered"
EV_BOUNCED = "bounced"
EV_COMPLAINED = "complained"
EV_EXPIRED = "expired"
EV_UNKNOWN = "unknown"
TERMINAL_EVENTS = (EV_DELIVERED, EV_BOUNCED, EV_COMPLAINED, EV_EXPIRED)
FAILURE_EVENTS = (EV_BOUNCED, EV_COMPLAINED, EV_EXPIRED)

# 终态优先级：delivered 与失败终态同级（首条终态赢，乱序不覆盖）
TERMINAL_RANK = {EV_DELIVERED: 1, EV_BOUNCED: 2, EV_COMPLAINED: 2,
                 EV_EXPIRED: 2, EV_UNKNOWN: 0}

# 消息/任务状态
MSG_PENDING = "pending"
MSG_RESENDING = "resending"
MSG_DELIVERED = "delivered"
MSG_BOUNCED = "bounced"
MSG_COMPLAINED = "complained"
MSG_EXPIRED = "expired"
MSG_AWAITING_CONFIRMATION = "awaiting_confirmation"
MSG_SUPERSEDED = "superseded"

# 发送任务侧新增状态（其余复用 notif_routing 的 pending/in_flight/sent/...）
TASK_AWAITING_MANUAL = "awaiting_manual"

# 回执匹配状态
MATCH_UNMATCHED = "unmatched"
MATCH_APPLIED = "applied"
MATCH_BOUND = "bound"
MATCH_IGNORED = "ignored"

# 通道切换原因（写入 notif_channel_switches）与审计事件
SW_RECEIPT_FAILED = "receipt_failed"
SW_RECEIPT_TIMEOUT = "receipt_timeout"

# 策略动作
ACTION_RETRY = "retry"
ACTION_MANUAL = "manual"

# 旧链路投递可被重排/取消的状态
_TASK_OPEN_STATUSES = ("pending", "in_flight", "failed",
                       "awaiting_confirmation", TASK_AWAITING_MANUAL)


# ---- 请求模型 ----------------------------------------------------------------

class OperatorRequest(BaseModel):
    operator: str


class ReceiptKeyRequest(BaseModel):
    operator: str
    kid: str
    secret: str


class ReceiptKeyRetireRequest(BaseModel):
    operator: str
    grace_until: str | float | None = None      # ISO-8601 或 epoch 秒


class ReceiptPolicyRequest(BaseModel):
    operator: str
    confirm_timeout_seconds: float | None = None
    confirm_max_retries: int | None = None
    on_bounced: str | None = None
    on_complained: str | None = None
    on_expired: str | None = None
    reason: str | None = None


class BindReceiptRequest(BaseModel):
    operator: str
    task_id: int | None = None                  # 路由发送任务（source=route）
    delivery_id: int | None = None              # 旧链路投递（source=legacy）
    note: str | None = None


class IgnoreReceiptRequest(BaseModel):
    operator: str
    reason: str | None = None


class ManualResolveRequest(BaseModel):
    operator: str
    action: str                                 # retry | ignore
    note: str | None = None


# ---- 小工具 ------------------------------------------------------------------

def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} is required")
    return str(value).strip()


def parse_time(value) -> float | None:
    """epoch 秒（数字/数字串）或带时区 ISO-8601；无法解析返回 None。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if not isinstance(value, str):
        return None
    s = value.strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        pass
    try:
        dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    except ValueError:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()


def _json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _history(cur, *, kind: str, message_pk: int | None, receipt_id: int | None,
             send_task_id: int | None, channel: str, recipient: str | None,
             from_status: str | None, to_status: str, reason: str | None = None,
             detail: dict | None = None, operator: str | None = None,
             ts: float) -> None:
    cur.execute(
        """INSERT INTO receipt_status_history
           (kind, message_pk, receipt_id, send_task_id, channel, recipient,
            from_status, to_status, reason, detail, operator, created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
        (kind, message_pk, receipt_id, send_task_id, channel, recipient,
         from_status, to_status, reason,
         _json_dumps(detail or {}), operator, ts))


# ============================================================================
# 引导：策略 + 通道密钥（环境变量）
# ============================================================================

def bootstrap(db: Database, settings: Settings, now: float | None = None) -> None:
    """启动时：用环境变量为每个通道补首个 active 回执密钥（库里已有该通道任意密钥
    行则不覆盖），并把单例行策略对齐配置缺省（仅当还是 bootstrap 默认行）。幂等。"""
    now = time.time() if now is None else now
    seeds = {CHANNEL_EMAIL: settings.receipt_email_secret,
             CHANNEL_WEBHOOK: settings.receipt_webhook_secret}
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM receipt_policy WHERE id=1").fetchone()
        if row is not None and row["updated_by"] == "bootstrap":
            cur.execute(
                """UPDATE receipt_policy SET confirm_timeout_seconds=?,
                   confirm_max_retries=? WHERE id=1
                   AND updated_by='bootstrap'""",
                (settings.receipt_confirm_timeout_seconds,
                 settings.receipt_confirm_max_retries))
        for channel, secret in seeds.items():
            if not secret:
                continue
            exists = cur.execute(
                "SELECT 1 FROM receipt_keys WHERE channel=? LIMIT 1",
                (channel,)).fetchone()
            if exists is not None:
                continue
            kid = f"{channel}-bootstrap"
            cur.execute(
                """INSERT INTO receipt_keys (channel,kid,secret,status,created_by,
                   created_at) VALUES (?,?,?, 'active', 'bootstrap', ?)""",
                (channel, kid, secret, now))
            audit.record(cur, "receipt_key_seeded", None, None,
                         {"channel": channel, "kid": kid}, ts=now)


# ============================================================================
# 验签：与回调入口密钥环相互独立的按通道密钥
# ============================================================================

def verify_receipt_signature(cur: sqlite3.Cursor, channel: str,
                             header: str | None, body: bytes,
                             now: float) -> tuple[bool, str, str | None]:
    """返回 (ok, reason, kid)。报文头与回调入口同构：``kid=..,ts=..,sig=..``，
    签名串为 ``f"{ts}\\n" + 原始请求体`` 的 HMAC-SHA256(hex)。fail-closed：
    该通道没有任何可用密钥即拒绝。"""
    if not header:
        return False, "missing_signature_header", None
    parts = {}
    for seg in header.split(","):
        if "=" in seg:
            k, v = seg.split("=", 1)
            parts[k.strip()] = v.strip()
    kid, ts_s, sig = parts.get("kid"), parts.get("ts"), parts.get("sig")
    if not (kid and ts_s and sig):
        return False, "malformed_signature_header", kid
    row = cur.execute(
        "SELECT * FROM receipt_keys WHERE channel=? AND kid=?",
        (channel, kid)).fetchone()
    if row is None:
        return False, "unknown_kid", kid
    if row["status"] == "retired":
        grace = row["grace_until"]
        if grace is None or now > grace:
            return False, "retired_key_grace_expired", kid
    elif row["status"] != "active":
        return False, "key_not_usable", kid
    try:
        ts = int(ts_s)
    except ValueError:
        return False, "bad_timestamp", kid
    # 回执时间戳容差沿用回调入口的 5 分钟默认（防重放）
    if abs(now - ts) > 300:
        return False, "timestamp_out_of_tolerance", kid
    expected = hmac.new(row["secret"].encode(),
                        f"{ts}\n".encode() + body,
                        hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, sig):
        return False, "signature_mismatch", kid
    return True, "ok", kid


# ============================================================================
# 消息登记（发送成功后调用）
# ============================================================================

def _load_policy(cur: sqlite3.Cursor) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM receipt_policy WHERE id=1").fetchone()
    if row is None:  # 极端旧库：补缺省行
        cur.execute(
            """INSERT OR IGNORE INTO receipt_policy
               (id, confirm_timeout_seconds, confirm_max_retries, on_bounced,
                on_complained, on_expired, updated_by, updated_at, reason)
               VALUES (1, 3600, 2, 'retry', 'manual', 'retry',
                       'bootstrap', 0, 'default')""")
        row = cur.execute("SELECT * FROM receipt_policy WHERE id=1").fetchone()
    return row


def register_external_message_tx(cur, *, channel: str, message_id: str | None,
                                 id_source: str, source: str,
                                 send_task_id: int | None = None,
                                 delivery_id: int | None = None,
                                 attempt_id: int | None = None,
                                 recipient: str, address: str | None,
                                 event_id: int | None, event_type: str | None,
                                 policy_row: sqlite3.Row | None = None,
                                 ts: float) -> int:
    """发送成功事务内：登记外部消息并把同一任务此前的未终结消息置 superseded。

    返回 external_messages.id。``UNIQUE(channel,message_id)`` 兜底并发/重入：
    - 撞在同一任务同一行：幂等返回已登记行；
    - 撞在别的任务：真实外部编号异常，落审计并改本地后缀编号重新登记（绝不串单）。
    """
    policy_row = policy_row or _load_policy(cur)
    deadline = ts + float(policy_row["confirm_timeout_seconds"])
    if not message_id:
        id_source = "local"
        message_id = f"local:{channel}:{send_task_id or delivery_id or 'x'}:{ts_ms(ts)}"
    for attempt in range(3):
        try:
            cur.execute(
                """INSERT INTO external_messages
                   (channel,message_id,id_source,source,send_task_id,delivery_id,
                    attempt_id,recipient,address,event_id,event_type,status,
                    confirm_deadline,confirm_retries,registered_at,updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?, 'pending', ?, 0, ?, ?)""",
                (channel, message_id, id_source, source, send_task_id, delivery_id,
                 attempt_id, recipient, address, event_id, event_type,
                 deadline, ts, ts))
            break
        except sqlite3.IntegrityError:
            existing = cur.execute(
                "SELECT * FROM external_messages WHERE channel=? AND message_id=?",
                (channel, message_id)).fetchone()
            same_unit = (source == SOURCE_ROUTE and
                         existing["send_task_id"] == send_task_id) or \
                        (source == SOURCE_LEGACY and
                         existing["delivery_id"] == delivery_id)
            if same_unit:
                return existing["id"]
            audit.record(cur, "external_message_id_collision", None, None, {
                "channel": channel, "message_id": message_id,
                "existing_task_id": existing["send_task_id"],
                "existing_delivery_id": existing["delivery_id"],
                "incoming_task_id": send_task_id,
                "incoming_delivery_id": delivery_id}, ts=ts)
            message_id = f"{message_id}~dup{int(ts*1000)}{attempt}"
            id_source = "local"
    else:  # pragma: no cover - 防御：连续撞号不应发生
        raise RuntimeError("could not allocate external message id")
    msg_pk = cur.lastrowid

    # 同一发送单元此前未终结的消息行置 superseded（终态行保留，回执轨迹完整）
    if source == SOURCE_ROUTE and send_task_id is not None:
        old = cur.execute(
            """SELECT id, status FROM external_messages
               WHERE send_task_id=? AND id<>? AND status NOT IN
               ('delivered','bounced','complained','expired','superseded')""",
            (send_task_id, msg_pk)).fetchall()
        for r in old:
            cur.execute(
                "UPDATE external_messages SET status='superseded', updated_at=? "
                "WHERE id=?", (ts, r["id"]))
            _history(cur, kind="message", message_pk=r["id"], receipt_id=None,
                     send_task_id=send_task_id, channel=channel, recipient=recipient,
                     from_status=r["status"], to_status=MSG_SUPERSEDED,
                     reason="superseded_by_new_attempt",
                     detail={"new_message_pk": msg_pk,
                             "new_message_id": message_id}, ts=ts)
        cur.execute(
            """UPDATE notif_send_tasks SET receipt_status='pending',
               receipt_reason=NULL, external_message_id=?, updated_at=? WHERE id=?""",
            (msg_pk, ts, send_task_id))
    elif source == SOURCE_LEGACY and delivery_id is not None:
        old = cur.execute(
            """SELECT id, status FROM external_messages
               WHERE delivery_id=? AND id<>? AND status NOT IN
               ('delivered','bounced','complained','expired','superseded')""",
            (delivery_id, msg_pk)).fetchall()
        for r in old:
            cur.execute(
                "UPDATE external_messages SET status='superseded', updated_at=? "
                "WHERE id=?", (ts, r["id"]))
            _history(cur, kind="message", message_pk=r["id"], receipt_id=None,
                     send_task_id=None, channel=channel, recipient=recipient,
                     from_status=r["status"], to_status=MSG_SUPERSEDED,
                     reason="superseded_by_new_attempt",
                     detail={"new_message_pk": msg_pk}, ts=ts)
        cur.execute(
            """UPDATE approval_notification_deliveries SET receipt_status='pending',
               external_message_id=?, updated_at=? WHERE id=?""",
            (msg_pk, ts, delivery_id))
    return msg_pk


def ts_ms(ts: float) -> str:
    return str(int(ts * 1000))


# ============================================================================
# 回执解析
# ============================================================================

_RECEIPT_ALIASES = {
    "event": ("event", "status", "state", "result", "event_type", "eventType",
              "notificationType"),
    "message_id": ("message_id", "messageId", "message-id", "msg_id", "mail_id",
                   "mailId", "id"),
    "recipient": ("recipient", "email", "address", "to", "mail_to"),
    "timestamp": ("timestamp", "ts", "event_time", "created_at", "time",
                  "occurred_at"),
}
_EVENT_SYNONYMS = {
    "delivered": EV_DELIVERED, "deliver": EV_DELIVERED,
    "delivery": EV_DELIVERED, "success": EV_DELIVERED,
    "sent": EV_DELIVERED, "delivery_": EV_DELIVERED,
    "bounced": EV_BOUNCED, "bounce": EV_BOUNCED, "hard_bounce": EV_BOUNCED,
    "soft_bounce": EV_BOUNCED, "permanent_failure": EV_BOUNCED,
    "complained": EV_COMPLAINED, "complaint": EV_COMPLAINED,
    "spam": EV_COMPLAINED, "spamcomplaint": EV_COMPLAINED,
    "expired": EV_EXPIRED, "expire": EV_EXPIRED, "timeout": EV_EXPIRED,
    "deferred_expired": EV_EXPIRED,
}


def _normalize_event(raw) -> str:
    if not isinstance(raw, str):
        return EV_UNKNOWN
    key = re.sub(r"[^a-z0-9]+", "_", raw.strip().lower()).strip("_")
    return _EVENT_SYNONYMS.get(key, EV_UNKNOWN)


def _first_field(payload: dict, names: tuple[str, ...]):
    for n in names:
        if n in payload and payload[n] not in (None, ""):
            return payload[n]
    return None


def parse_receipt_payload(raw: bytes) -> tuple[dict, str, str | None, str | None,
                                               float | None]:
    """解析回执 JSON：返回 (payload, event, message_id, recipient, provider_ts)。

    兼容常见 webhook 字段命名（SES/SendGrid/通用）；无法识别的事件归一为 unknown
    （原文仍落盘进待核对队列，不驱动状态机）。
    """
    payload = json.loads(raw.decode("utf-8"))
    if not isinstance(payload, dict):
        raise ValueError("receipt body must be a JSON object")
    event_raw = _first_field(payload, _RECEIPT_ALIASES["event"])
    message_id = _first_field(payload, _RECEIPT_ALIASES["message_id"])
    recipient = _first_field(payload, _RECEIPT_ALIASES["recipient"])
    provider_ts = parse_time(_first_field(payload, _RECEIPT_ALIASES["timestamp"]))
    event = _normalize_event(event_raw)
    if message_id is not None and not isinstance(message_id, str):
        message_id = str(message_id)
    if recipient is not None and not isinstance(recipient, str):
        recipient = str(recipient)
    return payload, event, message_id, recipient, provider_ts


# ============================================================================
# 回执应用：状态机（幂等 + 终态保护 + 乱序保护）
# ============================================================================

def _insert_receipt_row(cur, *, channel: str, message_id: str, event: str,
                        recipient: str | None, raw: bytes, content_type: str | None,
                        kid: str | None, provider_ts: float | None,
                        matched: str, message_pk: int | None,
                        send_task_id: int | None, duplicate_of: int | None,
                        ts: float) -> int:
    receipt_hash = hashlib.sha256(raw).hexdigest()
    cur.execute(
        """INSERT INTO receipts
           (channel,message_id,event,recipient,receipt_hash,raw_body,content_type,
            signature_kid,provider_ts,matched,message_pk,send_task_id,
            terminal_rank,duplicate_of,received_at,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (channel, message_id, event, recipient, receipt_hash,
         raw.decode("utf-8", errors="replace"), content_type, kid, provider_ts,
         matched, message_pk, send_task_id, TERMINAL_RANK.get(event, 0),
         duplicate_of, ts, ts))
    return cur.lastrowid


def _schedule_route_failover(cur, task, plan: list[dict], idx: int, *,
                             reason: str, detail: dict, event: str,
                             receipt_id: int, msg_pk: int,
                             policy: sqlite3.Row, ts: float) -> bool:
    """失败回执/超时确认后：按当前通道计划故障转移到下一通道。

    返回 True 表示已调度（新通道立即可发或本通道重发，由既有 worker 领取），
    False 表示没有可重试余地（调用方转人工）。不直接做通道 IO——外发仍走既有的
    单赢家领取/尝试链路，因此不会产生第二次外部效果。
    """
    retries = int(task["receipt_retries"] or 0)
    max_retries = int(policy["confirm_max_retries"])
    if retries >= max_retries:
        return False
    next_idx = idx + 1
    new_round = int(task["round"]) + 1
    if next_idx >= len(plan):
        # 计划耗尽但还有重试额度：从首选通道开新一轮
        if not plan:
            return False
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
               current_channel=NULL, round=?, next_retry_at=NULL,
               receipt_status='resending', receipt_reason=?,
               receipt_retries=receipt_retries+1, external_message_id=NULL,
               updated_at=? WHERE id=?""",
            (new_round, reason, ts, task["id"]))
        cur.execute(
            """INSERT INTO notif_channel_switches
               (task_id,event_id,recipient,from_channel,to_channel,reason,
                detail,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task["id"], task["event_id"], task["recipient"],
             plan[-1]["channel"] if plan else None, None, reason,
             _json_dumps({"event": event, "receipt_id": receipt_id,
                          "new_round": new_round, **detail}), ts))
    else:
        nxt = plan[next_idx]["channel"]
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', attempt_index=?,
               current_channel=NULL, round=?, next_retry_at=NULL,
               receipt_status='resending', receipt_reason=?,
               receipt_retries=receipt_retries+1, external_message_id=NULL,
               updated_at=? WHERE id=?""",
            (next_idx, new_round, reason, ts, task["id"]))
        cur.execute(
            """INSERT INTO notif_channel_switches
               (task_id,event_id,recipient,from_channel,to_channel,reason,
                detail,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task["id"], task["event_id"], task["recipient"],
             plan[idx]["channel"], nxt, reason,
             _json_dumps({"event": event, "receipt_id": receipt_id,
                          "attempt_index": next_idx, **detail}), ts))
    _bump_retries(cur, msg_pk, ts)
    # 旧消息行已被下一道的新登记替代：标记 superseded 并停止超时扫描（轨迹保留）
    cur.execute(
        """UPDATE external_messages SET status='superseded',
           confirm_deadline=NULL, updated_at=? WHERE id=?""",
        (ts, msg_pk))
    _history(cur, kind="message", message_pk=msg_pk, receipt_id=receipt_id,
             send_task_id=task["id"], channel=task["sent_channel"] or "",
             recipient=task["recipient"], from_status=MSG_RESENDING,
             to_status=MSG_SUPERSEDED, reason=reason,
             detail={"event": event, "confirm_retries": retries + 1}, ts=ts)
    audit.record(cur,
                 "receipt_delivery_rescheduled" if reason == SW_RECEIPT_FAILED
                 else "receipt_confirmation_rescheduled", None, None, {
                     "send_task_id": task["id"], "receipt_id": receipt_id,
                     "external_message_pk": msg_pk, "event": event,
                     "confirm_retries": retries + 1,
                     "max_retries": max_retries}, ts=ts)
    return True


def _bump_retries(cur, msg_pk: int, ts: float) -> None:
    cur.execute(
        "UPDATE external_messages SET confirm_retries=confirm_retries+1, "
        "status='resending', updated_at=? WHERE id=?", (ts, msg_pk))


def _move_task_to_manual(cur, task, *, reason: str, event: str | None,
                         receipt_id: int | None, msg_pk: int | None,
                         detail: dict, ts: float) -> None:
    cur.execute(
        """UPDATE notif_send_tasks SET status=?, next_retry_at=NULL,
           receipt_reason=?, updated_at=? WHERE id=?""",
        (TASK_AWAITING_MANUAL, reason, ts, task["id"]))
    if msg_pk is not None:
        _history(cur, kind="message", message_pk=msg_pk, receipt_id=receipt_id,
                 send_task_id=task["id"], channel=task["sent_channel"] or "",
                 recipient=task["recipient"], from_status=None,
                 to_status=TASK_AWAITING_MANUAL, reason=reason,
                 detail={"event": event, **detail}, operator=None, ts=ts)
    audit.record(cur, "receipt_awaiting_manual", None, None, {
        "send_task_id": task["id"], "receipt_id": receipt_id,
        "external_message_pk": msg_pk, "event": event,
        "reason": reason, **detail}, ts=ts)


def _apply_matched_receipt_tx(cur, *, msg: sqlite3.Row, event: str,
                              receipt_id: int, provider_ts: float | None,
                              raw_detail: dict, ts: float) -> dict:
    """把一条已匹配消息的回执套进状态机。返回处置说明 dict。

    终态保护：消息已有终态（delivered/bounced/complained/expired）时，新终态回执
    不覆盖（首条终态赢），只留 ``receipt_terminal_ignored`` 审计与历史——乱序回执
    不可能把已确认终态改回处理中。
    """
    cur.execute("UPDATE receipts SET matched='applied' WHERE id=?", (receipt_id,))
    terminal = {"delivered", "bounced", "complained", "expired"}
    previous = msg["status"]
    outcome = {"applied": True, "terminal": event in TERMINAL_EVENTS,
               "previous_status": previous, "event": event}

    if event == EV_UNKNOWN:
        # 非终态/不可识别事件：留痕不驱动
        cur.execute("UPDATE receipts SET matched='applied' WHERE id=?", (receipt_id,))
        _history(cur, kind="receipt", message_pk=msg["id"], receipt_id=receipt_id,
                 send_task_id=msg["send_task_id"], channel=msg["channel"],
                 recipient=msg["recipient"], from_status=None,
                 to_status=EV_UNKNOWN, reason="unknown_event_recorded",
                 detail=raw_detail, ts=ts)
        audit.record(cur, "receipt_unknown_recorded", None, None, {
            "receipt_id": receipt_id, "external_message_pk": msg["id"],
            "send_task_id": msg["send_task_id"],
            "delivery_id": msg["delivery_id"],
            "channel": msg["channel"], "message_id": msg["message_id"]}, ts=ts)
        return outcome

    if previous in terminal:
        # 终态保护：首条终态赢；重复/乱序终态只留历史，不改消息与任务状态
        _history(cur, kind="message", message_pk=msg["id"], receipt_id=receipt_id,
                 send_task_id=msg["send_task_id"], channel=msg["channel"],
                 recipient=msg["recipient"], from_status=previous,
                 to_status=previous, reason="terminal_state_kept",
                 detail={"incoming_event": event,
                         "provider_ts": provider_ts, **raw_detail}, ts=ts)
        audit.record(cur, "receipt_terminal_ignored", None, None, {
            "receipt_id": receipt_id, "external_message_pk": msg["id"],
            "send_task_id": msg["send_task_id"], "delivery_id": msg["delivery_id"],
            "channel": msg["channel"], "kept_status": previous,
            "incoming_event": event}, ts=ts)
        outcome.update(kept=previous, changed=False)
        return outcome

    # 首条终态落到本消息
    cur.execute(
        """UPDATE external_messages SET status=?, active_receipt_id=?,
           updated_at=? WHERE id=?""", (event, receipt_id, ts, msg["id"]))
    cur.execute(
        "UPDATE receipts SET terminal_rank=? WHERE id=?",
        (TERMINAL_RANK[event], receipt_id))
    _history(cur, kind="message", message_pk=msg["id"], receipt_id=receipt_id,
             send_task_id=msg["send_task_id"], channel=msg["channel"],
             recipient=msg["recipient"], from_status=previous, to_status=event,
             reason="receipt_received",
             detail={"provider_ts": provider_ts, **raw_detail}, ts=ts)

    policy = _load_policy(cur)
    detail = {"receipt_id": receipt_id, "provider_ts": provider_ts, **raw_detail}

    if msg["source"] == SOURCE_ROUTE and msg["send_task_id"] is not None:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (msg["send_task_id"],)).fetchone()
        if event == EV_DELIVERED:
            cur.execute(
                """UPDATE notif_send_tasks SET receipt_status='delivered',
                   receipt_reason=NULL, updated_at=? WHERE id=?""",
                (ts, task["id"]))
            audit.record(cur, "receipt_delivered", None, None, {
                "send_task_id": task["id"], "receipt_id": receipt_id,
                "external_message_pk": msg["id"], "channel": msg["channel"],
                "recipient": task["recipient"]}, ts=ts)
            outcome.update(changed=True, disposition="delivered")
            return outcome

        # 失败终态：按事件策略 retry / manual
        action_policy = {EV_BOUNCED: policy["on_bounced"],
                         EV_COMPLAINED: policy["on_complained"],
                         EV_EXPIRED: policy["on_expired"]}[event]
        audit_event = {"bounced": "receipt_bounced",
                       "complained": "receipt_complained",
                       "expired": "receipt_expired"}[event]
        audit.record(cur, audit_event, None, None, {
            "send_task_id": task["id"], "receipt_id": receipt_id,
            "external_message_pk": msg["id"], "channel": msg["channel"],
            "recipient": task["recipient"],
            "policy_action": action_policy, **raw_detail}, ts=ts)
        if action_policy == ACTION_RETRY and task["status"] not in \
                ("cancelled", "quarantined"):
            plan = json.loads(task["plan_json"] or "[]")
            idx = int(task["attempt_index"])
            # 失败回执针对的是已成功投递的通道：该通道必在计划中；从它的位置转移
            ch_pos = next((i for i, p in enumerate(plan)
                           if p["channel"] == msg["channel"]),
                          min(idx, max(len(plan) - 1, 0)))
            scheduled = _schedule_route_failover(
                cur, task, plan, ch_pos, reason=SW_RECEIPT_FAILED,
                detail=detail, event=event, receipt_id=receipt_id,
                msg_pk=msg["id"], policy=policy, ts=ts)
            if scheduled:
                _history(cur, kind="receipt", message_pk=msg["id"],
                         receipt_id=receipt_id, send_task_id=task["id"],
                         channel=msg["channel"], recipient=msg["recipient"],
                         from_status=previous, to_status=MSG_RESENDING,
                         reason="failover_scheduled",
                         detail={"event": event}, ts=ts)
                outcome.update(changed=True, disposition="rescheduled")
                return outcome
        # 策略 manual、重试超限或任务已终结：转人工
        _move_task_to_manual(cur, task, reason=f"{event}", event=event,
                             receipt_id=receipt_id, msg_pk=msg["id"],
                             detail=raw_detail, ts=ts)
        cur.execute(
            "UPDATE notif_send_tasks SET receipt_status=? WHERE id=?",
            (event, task["id"]))
        outcome.update(changed=True, disposition="manual")
        return outcome

    # 旧链路投递：没有通道计划，失败终态一律转人工处理
    if msg["source"] == SOURCE_LEGACY and msg["delivery_id"] is not None:
        d = cur.execute(
            "SELECT * FROM approval_notification_deliveries WHERE id=?",
            (msg["delivery_id"],)).fetchone()
        if event == EV_DELIVERED:
            cur.execute(
                "UPDATE approval_notification_deliveries SET receipt_status='delivered'"
                " WHERE id=?", (msg["delivery_id"],))
            audit.record(cur, "receipt_delivered", None, None, {
                "delivery_id": msg["delivery_id"], "receipt_id": receipt_id,
                "external_message_pk": msg["id"], "channel": msg["channel"]}, ts=ts)
            outcome.update(changed=True, disposition="delivered")
            return outcome
        cur.execute(
            "UPDATE approval_notification_deliveries SET receipt_status=? WHERE id=?",
            (event, msg["delivery_id"]))
        audit.record(cur,
                     {"bounced": "receipt_bounced",
                      "complained": "receipt_complained",
                      "expired": "receipt_expired"}[event], None, None, {
                          "delivery_id": msg["delivery_id"],
                          "receipt_id": receipt_id,
                          "external_message_pk": msg["id"],
                          "channel": msg["channel"],
                          "recipient": d["recipient"] if d else None,
                          **raw_detail}, ts=ts)
        if d is not None and d["status"] in ("sent", "pending", "failed"):
            # 旧链路无自动通道计划：标记待人工（保留 sent 业务状态，回执侧可见）
            audit.record(cur, "receipt_awaiting_manual", None, None, {
                "delivery_id": msg["delivery_id"], "receipt_id": receipt_id,
                "external_message_pk": msg["id"], "event": event,
                "reason": f"legacy_{event}"}, ts=ts)
        outcome.update(changed=True, disposition="legacy_terminal")
        return outcome

    # 消息行既无路由任务也无旧投递（数据异常）：终态落消息，转人工核对
    audit.record(cur, "receipt_awaiting_manual", None, None, {
        "receipt_id": receipt_id, "external_message_pk": msg["id"],
        "event": event, "reason": "message_without_send_unit",
        "channel": msg["channel"]}, ts=ts)
    outcome.update(changed=True, disposition="orphan_manual")
    return outcome


# ============================================================================
# 回执接入（公共签名入口）
# ============================================================================

def ingest_receipt(db: Database, channel: str, raw: bytes,
                   signature: str | None, content_type: str | None,
                   now: float | None = None) -> tuple[int, dict]:
    """验签 -> 落盘 -> 匹配 -> 状态机，全部在写事务内完成（提交后才 2xx）。

    返回 (http_status, body)。重复投递返回 200 duplicate；匹配待核对 202 unmatched；
    正常应用 200 applied；验签失败 401（独立审计事务）；报文问题 4xx。
    """
    now = time.time() if now is None else now
    if channel not in EXTERNAL_CHANNELS:
        raise HTTPException(404, f"unknown receipt channel {channel!r}")

    with db.tx() as cur:
        ok, reason, kid = verify_receipt_signature(cur, channel, signature, raw, now)
        if not ok:
            audit.record(cur, "receipt_signature_fail", None, None,
                         {"channel": channel, "kid": kid, "reason": reason}, ts=now)
        # 解析即便验签失败也不做（防未授权报文触达状态机）
    if not ok:
        return 401, {"error": "signature_verification_failed", "reason": reason}

    try:
        payload, event, message_id, recipient, provider_ts = \
            parse_receipt_payload(raw)
    except (ValueError, UnicodeDecodeError, json.JSONDecodeError) as exc:
        with db.tx() as cur:
            audit.record(cur, "receipt_bad_payload", None, None,
                         {"channel": channel, "kid": kid, "error": str(exc)}, ts=now)
        return 422, {"error": "invalid_receipt_payload", "reason": str(exc)}
    if not message_id:
        with db.tx() as cur:
            audit.record(cur, "receipt_bad_payload", None, None,
                         {"channel": channel, "kid": kid,
                          "reason": "missing_message_id"}, ts=now)
        return 422, {"error": "invalid_receipt_payload",
                     "reason": "message_id is required"}

    with db.tx() as cur:
        audit.record(cur, "receipt_signature_ok", None, None,
                     {"channel": channel, "kid": kid}, ts=now)
        receipt_hash = hashlib.sha256(raw).hexdigest()
        dup = cur.execute(
            """SELECT id FROM receipts
               WHERE channel=? AND message_id=? AND event=? AND receipt_hash=?""",
            (channel, message_id, event, receipt_hash)).fetchone()
        if dup is not None:
            # 重复回执：不能再次驱动状态机（幂等）。写事务串行 + 先查后插保证并发
            # 重复投递只有第一个请求落原文行，其余全部直接返回首条（原文永不改写）。
            first = cur.execute("SELECT * FROM receipts WHERE id=?",
                                (dup["id"],)).fetchone()
            audit.record(cur, "receipt_duplicate", None, None, {
                "receipt_id": first["id"], "channel": channel,
                "message_id": message_id, "event": event}, ts=now)
            return 200, {"result": "duplicate", "receipt_id": first["id"],
                         "matched": first["matched"], "message_id": message_id,
                         "event": event}

        msg = cur.execute(
            """SELECT * FROM external_messages
               WHERE channel=? AND message_id=? AND status<>'superseded'
               ORDER BY id DESC LIMIT 1""",
            (channel, message_id)).fetchone()
        if msg is None:
            # 也可能匹配到已 superseded 的历史消息：仍进待核对，由人工决定绑定哪条
            receipt_id = _insert_receipt_row(
                cur, channel=channel, message_id=message_id, event=event,
                recipient=recipient, raw=raw, content_type=content_type, kid=kid,
                provider_ts=provider_ts, matched=MATCH_UNMATCHED, message_pk=None,
                send_task_id=None, duplicate_of=None, ts=now)
            _history(cur, kind="receipt", message_pk=None, receipt_id=receipt_id,
                     send_task_id=None, channel=channel, recipient=recipient,
                     from_status=None, to_status=MATCH_UNMATCHED,
                     reason="no_matching_message",
                     detail={"event": event, "message_id": message_id}, ts=now)
            audit.record(cur, "receipt_unmatched", None, None, {
                "receipt_id": receipt_id, "channel": channel,
                "message_id": message_id, "event": event,
                "recipient": recipient}, ts=now)
            return 202, {"result": "unmatched", "receipt_id": receipt_id,
                         "message_id": message_id, "event": event,
                         "status": "pending_review"}

        receipt_id = _insert_receipt_row(
            cur, channel=channel, message_id=message_id, event=event,
            recipient=recipient or msg["recipient"], raw=raw,
            content_type=content_type, kid=kid,
            provider_ts=provider_ts, matched=MATCH_APPLIED, message_pk=msg["id"],
            send_task_id=msg["send_task_id"], duplicate_of=None, ts=now)
        outcome = _apply_matched_receipt_tx(
            cur, msg=msg, event=event, receipt_id=receipt_id,
            provider_ts=provider_ts,
            raw_detail={"channel": channel, "message_id": message_id,
                        "recipient": recipient}, ts=now)
        return 200, {"result": "applied", "receipt_id": receipt_id,
                     "message_id": message_id, "event": event,
                     "disposition": outcome.get("disposition"),
                     "matched_message": msg["id"],
                     "kept_status": outcome.get("kept")}


# ============================================================================
# 超时扫描：无回执 -> 待确认 -> 按策略重试/转人工
# ============================================================================

def scan_confirmations(db: Database, now: float | None = None,
                       limit: int = 100) -> dict:
    """后台扫描：登记后超过 confirm_timeout 仍未终结（pending/resending）的消息。

    首轮标记 awaiting_confirmation 并按策略尝试沿计划故障转移；没有下一道/超出
    confirm_max_retries 时任务转 awaiting_manual。全部是条件 UPDATE，重启/并发
    扫描只有一个赢家。
    """
    now = time.time() if now is None else now
    due = db.query(
        """SELECT * FROM external_messages
           WHERE status IN ('pending','resending')
             AND confirm_deadline IS NOT NULL AND confirm_deadline <= ?
           ORDER BY confirm_deadline, id LIMIT ?""", (now, limit))
    marked, rescheduled, manual = 0, 0, 0
    for m in due:
        with db.tx() as cur:
            msg = cur.execute("SELECT * FROM external_messages WHERE id=?",
                              (m["id"],)).fetchone()
            if msg is None or msg["status"] not in (
                    MSG_PENDING, MSG_RESENDING):
                continue
            policy = _load_policy(cur)
            if msg["source"] != SOURCE_ROUTE or msg["send_task_id"] is None:
                # 旧链路无自动计划：消息置待确认（人工在管理端可见），不自动重发
                if msg["status"] != MSG_AWAITING_CONFIRMATION:
                    cur.execute(
                        "UPDATE external_messages SET status=?, updated_at=? WHERE id=?",
                        (MSG_AWAITING_CONFIRMATION, now, msg["id"]))
                    _history(cur, kind="message", message_pk=msg["id"],
                             receipt_id=None, send_task_id=None,
                             channel=msg["channel"], recipient=msg["recipient"],
                             from_status=msg["status"],
                             to_status=MSG_AWAITING_CONFIRMATION,
                             reason="no_receipt_timeout",
                             detail={"deadline": msg["confirm_deadline"]}, ts=now)
                    audit.record(cur, "receipt_confirmation_timeout", None, None, {
                        "external_message_pk": msg["id"],
                        "delivery_id": msg["delivery_id"],
                        "channel": msg["channel"], "message_id": msg["message_id"],
                        "deadline": msg["confirm_deadline"]}, ts=now)
                    marked += 1
                continue

            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (msg["send_task_id"],)).fetchone()
            if task is None or task["status"] in ("cancelled", "quarantined",
                                                  TASK_AWAITING_MANUAL):
                continue
            already = int(msg["status"] == MSG_AWAITING_CONFIRMATION)
            if not already:
                cur.execute(
                    "UPDATE external_messages SET status=?, updated_at=? WHERE id=?",
                    (MSG_AWAITING_CONFIRMATION, now, msg["id"]))
                cur.execute(
                    """UPDATE notif_send_tasks SET receipt_status=?,
                       receipt_reason='no_receipt_timeout', updated_at=? WHERE id=?""",
                    (MSG_AWAITING_CONFIRMATION, now, task["id"]))
                _history(cur, kind="message", message_pk=msg["id"], receipt_id=None,
                         send_task_id=task["id"], channel=msg["channel"],
                         recipient=msg["recipient"], from_status=msg["status"],
                         to_status=MSG_AWAITING_CONFIRMATION,
                         reason="no_receipt_timeout",
                         detail={"deadline": msg["confirm_deadline"]}, ts=now)
                audit.record(cur, "receipt_confirmation_timeout", None, None, {
                    "external_message_pk": msg["id"], "send_task_id": task["id"],
                    "channel": msg["channel"], "message_id": msg["message_id"],
                    "recipient": task["recipient"],
                    "deadline": msg["confirm_deadline"]}, ts=now)
                marked += 1

            # 自动故障转移（沿用任务入队时的通道计划快照）
            plan = json.loads(task["plan_json"] or "[]")
            idx = int(task["attempt_index"])
            ch_pos = next((i for i, p in enumerate(plan)
                           if p["channel"] == msg["channel"]), idx)
            can_retry = int(task["receipt_retries"] or 0) < \
                int(policy["confirm_max_retries"])
            if can_retry and plan and task["status"] in _TASK_OPEN_STATUSES + ("sent",):
                ok = _schedule_route_failover(
                    cur, task, plan, ch_pos, reason=SW_RECEIPT_TIMEOUT,
                    detail={"deadline": msg["confirm_deadline"]}, event=EV_EXPIRED,
                    receipt_id=None, msg_pk=msg["id"], policy=policy, ts=now)
                if ok:
                    rescheduled += 1
                    continue
            # 超出重试额度：转人工（消息停在 awaiting_confirmation，任务侧 awaiting_manual）
            _move_task_to_manual(cur, task, reason="no_receipt_timeout",
                                 event=None, receipt_id=None, msg_pk=msg["id"],
                                 detail={"deadline": msg["confirm_deadline"],
                                         "confirm_retries":
                                             task["receipt_retries"],
                                         "max_retries":
                                             policy["confirm_max_retries"]}, ts=now)
            manual += 1
    return {"marked_awaiting_confirmation": marked, "rescheduled": rescheduled,
            "awaiting_manual": manual, "scanned": len(due)}


# ============================================================================
# 人工：绑定未知回执 / 忽略 / 转人工任务处置 / 安全重放
# ============================================================================

def bind_unmatched_receipt(db: Database, receipt_id: int,
                           req: BindReceiptRequest) -> dict:
    """把待核对（unmatched/ignored）回执绑定到一条发送任务/旧投递的消息行。

    只建立关联并重放状态机；**原始回执内容（raw_body/channel/message_id/event）
    绝不改写**。绑定必须与回执通道相符；绑定后 matched=bound。
    """
    operator = _require(req.operator, "operator")
    if (req.task_id is None) == (req.delivery_id is None):
        raise HTTPException(422, "exactly one of task_id / delivery_id is required")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM receipts WHERE id=?",
                          (receipt_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "receipt not found")
        if row["matched"] in (MATCH_APPLIED, MATCH_BOUND):
            raise HTTPException(409, f"receipt already {row['matched']}")

        if req.task_id is not None:
            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (req.task_id,)).fetchone()
            if task is None:
                raise HTTPException(404, "send task not found")
            candidates = cur.execute(
                """SELECT * FROM external_messages
                   WHERE send_task_id=? AND channel=?
                   ORDER BY id DESC""",
                (req.task_id, row["channel"])).fetchall()
            unit = "send_task_id"
        else:
            task = cur.execute(
                "SELECT * FROM approval_notification_deliveries WHERE id=?",
                (req.delivery_id,)).fetchone()
            if task is None:
                raise HTTPException(404, "notification delivery not found")
            candidates = cur.execute(
                """SELECT * FROM external_messages
                   WHERE delivery_id=? AND channel=? ORDER BY id DESC""",
                (req.delivery_id, row["channel"])).fetchall()
            unit = "delivery_id"

        if not candidates:
            raise HTTPException(
                422,
                f"no registered external message on channel {row['channel']!r} "
                f"for the given {unit.replace('_', ' ')}")
        # 优先按回执的 message_id 精确匹配该任务的登记行；否则绑最新一条
        msg = next((c for c in candidates
                    if c["message_id"] == row["message_id"]), candidates[0])

        cur.execute(
            """UPDATE receipts SET matched='bound', message_pk=?,
               send_task_id=COALESCE(send_task_id,?), bound_by=?, bound_at=?,
               bind_note=? WHERE id=?""",
            (msg["id"], msg["send_task_id"], operator, now, req.note, receipt_id))
        _history(cur, kind="receipt", message_pk=msg["id"], receipt_id=receipt_id,
                 send_task_id=msg["send_task_id"], channel=row["channel"],
                 recipient=row["recipient"], from_status=row["matched"],
                 to_status="bound", reason="manual_bind",
                 detail={"operator": operator, "note": req.note,
                         "bound_message_pk": msg["id"],
                         "message_id_match":
                             msg["message_id"] == row["message_id"]},
                 operator=operator, ts=now)
        audit.record(cur, "receipt_manually_bound", None, None, {
            "receipt_id": receipt_id, "external_message_pk": msg["id"],
            "send_task_id": msg["send_task_id"], "delivery_id":
                msg["delivery_id"], "operator": operator,
            "note": req.note, "channel": row["channel"]}, ts=now)

        outcome = {"changed": False, "disposition": "bound_only"}
        if row["event"] in TERMINAL_EVENTS:
            outcome = _apply_matched_receipt_tx(
                cur, msg=msg, event=row["event"], receipt_id=receipt_id,
                provider_ts=row["provider_ts"],
                raw_detail={"manual_bind": True, "operator": operator}, ts=now)
        return {"result": "bound", "receipt_id": receipt_id,
                "external_message_pk": msg["id"],
                "send_task_id": msg["send_task_id"],
                "delivery_id": msg["delivery_id"],
                "disposition": outcome.get("disposition")}


def ignore_unmatched_receipt(db: Database, receipt_id: int,
                             req: IgnoreReceiptRequest) -> dict:
    """把待核对回执标记 ignored（不再出现在默认待核对队列；仍可查询/重新绑定）。"""
    operator = _require(req.operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM receipts WHERE id=?",
                          (receipt_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "receipt not found")
        if row["matched"] == MATCH_APPLIED:
            raise HTTPException(409, "receipt already applied to a message")
        if row["matched"] == MATCH_IGNORED:
            return {"result": "ignored", "receipt_id": receipt_id}
        cur.execute(
            "UPDATE receipts SET matched='ignored', bound_by=?, bound_at=?, "
            "bind_note=? WHERE id=?", (operator, now, req.reason, receipt_id))
        _history(cur, kind="receipt", message_pk=row["message_pk"],
                 receipt_id=receipt_id, send_task_id=row["send_task_id"],
                 channel=row["channel"], recipient=row["recipient"],
                 from_status=row["matched"], to_status=MATCH_IGNORED,
                 reason="manual_ignore", detail={"reason": req.reason},
                 operator=operator, ts=now)
        audit.record(cur, "receipt_ignored", None, None, {
            "receipt_id": receipt_id, "operator": operator,
            "reason": req.reason, "channel": row["channel"]}, ts=now)
        return {"result": "ignored", "receipt_id": receipt_id}


def resolve_manual_task(db, task_id: int, req: ManualResolveRequest,
                        settings: Settings) -> dict:
    """处置 awaiting_manual 的路由发送任务。

    - action=retry：从计划首选通道开新一轮（round+1，外部消息锚点清空，回执重新计时）；
    - action=ignore：仅清除待人工标记（回执终态保留，任务维持 sent/quarantined）。
    """
    operator = _require(req.operator, "operator")
    if req.action not in (ACTION_RETRY, "ignore"):
        raise HTTPException(422, "action must be 'retry' or 'ignore'")
    now = time.time()
    with db.tx() as cur:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (task_id,)).fetchone()
        if task is None:
            raise HTTPException(404, "send task not found")
        if task["status"] != TASK_AWAITING_MANUAL:
            raise HTTPException(409, f"task is {task['status']}, not awaiting_manual")
        prev_receipt = task["receipt_status"]
        if req.action == "ignore":
            cur.execute(
                """UPDATE notif_send_tasks SET status='sent',
                   updated_at=? WHERE id=?""", (now, task_id))
            # 确认不再发送：回收未使用的额度预占（已发送成功而 consumed 的不回收）
            from . import notif_quota
            notif_quota.release_for_task_tx(
                cur, task, "receipt_manual_ignored", now)
            audit.record(cur, "receipt_manual_resolved", None, None, {
                "send_task_id": task_id, "operator": operator,
                "action": "ignore", "note": req.note,
                "receipt_status": prev_receipt}, ts=now)
            return {"result": "ignored", "task_id": task_id}
        plan = json.loads(task["plan_json"] or "[]")
        # 从失败回执对应通道的下一道继续（沿用任务自己的计划快照）；计划已耗尽才回
        # 首选开新一轮。避免对已知失败通道重复外发。
        failed_idx = next((i for i, p in enumerate(plan)
                           if p["channel"] == task["sent_channel"]), -1)
        start_idx, new_round = failed_idx + 1, int(task["round"])
        if start_idx >= len(plan):
            start_idx, new_round = 0, new_round + 1
        # 人工发起的新发送轮次：额度代际 +1，按当时窗口重新预占（自动通道切换不经过
        # 这里——它们复用既有预占，不能绕过同一事件的额度限制）。
        from . import notif_quota
        new_generation = notif_quota.bump_generation_tx(
            cur, task, "receipt_manual_retry", now)
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', attempt_index=?,
               current_channel=NULL, round=?, next_retry_at=NULL,
               receipt_status='resending', receipt_reason=NULL,
               receipt_retries=receipt_retries+1, external_message_id=NULL,
               updated_at=? WHERE id=?""",
            (start_idx, new_round, now, task_id))
        if plan:
            cur.execute(
                """INSERT INTO notif_channel_switches
                   (task_id,event_id,recipient,from_channel,to_channel,reason,
                    detail,created_at)
                   VALUES (?,?,?,?,?,?,?,?)""",
                (task_id, task["event_id"], task["recipient"],
                 task["sent_channel"], plan[start_idx]["channel"], "manual_retry",
                 _json_dumps({"operator": operator, "note": req.note,
                              "previous_receipt_status": prev_receipt}), now))
        audit.record(cur, "receipt_manual_resolved", None, None, {
            "send_task_id": task_id, "operator": operator, "action": "retry",
            "note": req.note, "receipt_status": prev_receipt,
            "new_round": new_round, "attempt_index": start_idx,
            "new_quota_generation": new_generation}, ts=now)
        return {"result": "retry_scheduled", "task_id": task_id,
                "round": new_round, "attempt_index": start_idx,
                "quota_generation": new_generation}


def replay_receipt(db: Database, receipt_id: int, operator: str,
                   now: float | None = None) -> dict:
    """安全重放：取存证的回执原文，重新走一遍幂等状态机。

    已 applied/终态或重复的回执重放不会二次改变状态（终态保护 + 幂等）；待核对
    回执在对应消息后来登记时可借此完成匹配。**不产生任何第二次外部效果**——重放
    只做本地状态机应用，不重新发送通知。
    """
    operator = _require(operator, "operator")
    now = time.time() if now is None else now
    row = db.query_one("SELECT * FROM receipts WHERE id=?", (receipt_id,))
    if row is None:
        raise HTTPException(404, "receipt not found")
    raw = row["raw_body"].encode("utf-8")
    with db.tx() as cur:
        audit.record(cur, "receipt_replay_requested", None, None, {
            "receipt_id": receipt_id, "operator": operator,
            "channel": row["channel"], "message_id": row["message_id"],
            "event": row["event"], "matched": row["matched"]}, ts=now)

    # 待核对/忽略：尝试用当前消息表重新匹配后套状态机
    if row["matched"] in (MATCH_UNMATCHED, MATCH_IGNORED) and \
            row["message_pk"] is None:
        with db.tx() as cur:
            msg = cur.execute(
                """SELECT * FROM external_messages
                   WHERE channel=? AND message_id=? AND status<>'superseded'
                   ORDER BY id DESC LIMIT 1""",
                (row["channel"], row["message_id"])).fetchone()
            if msg is None:
                _history(cur, kind="receipt", message_pk=None,
                         receipt_id=receipt_id, send_task_id=None,
                         channel=row["channel"], recipient=row["recipient"],
                         from_status=row["matched"], to_status=row["matched"],
                         reason="replay_no_match",
                         detail={"operator": operator}, operator=operator, ts=now)
                audit.record(cur, "receipt_replay_no_match", None, None, {
                    "receipt_id": receipt_id, "operator": operator}, ts=now)
                return {"result": "still_unmatched", "receipt_id": receipt_id}
            cur.execute(
                """UPDATE receipts SET matched='bound', message_pk=?,
                   send_task_id=COALESCE(send_task_id,?), bound_by=?, bound_at=?,
                   bind_note='replay' WHERE id=?""",
                (msg["id"], msg["send_task_id"], operator, now, receipt_id))
            _history(cur, kind="receipt", message_pk=msg["id"],
                     receipt_id=receipt_id, send_task_id=msg["send_task_id"],
                     channel=row["channel"], recipient=row["recipient"],
                     from_status=row["matched"], to_status="bound",
                     reason="replay_matched", detail={"operator": operator},
                     operator=operator, ts=now)
            outcome = {"disposition": "bound_only", "changed": False}
            if row["event"] in TERMINAL_EVENTS:
                outcome = _apply_matched_receipt_tx(
                    cur, msg=msg, event=row["event"], receipt_id=receipt_id,
                    provider_ts=row["provider_ts"],
                    raw_detail={"replay": True, "operator": operator}, ts=now)
            audit.record(cur, "receipt_replayed", None, None, {
                "receipt_id": receipt_id, "operator": operator,
                "external_message_pk": msg["id"],
                "disposition": outcome.get("disposition")}, ts=now)
            return {"result": "replayed", "receipt_id": receipt_id,
                    "matched_message": msg["id"],
                    "disposition": outcome.get("disposition")}

    # 已匹配/终态：用存证原文重新应用一次——终态保护保证不回退、不二次转移
    with db.tx() as cur:
        latest = cur.execute("SELECT * FROM receipts WHERE id=?",
                             (receipt_id,)).fetchone()
        msg_pk = latest["message_pk"]
        if msg_pk is None:
            return {"result": "still_unmatched", "receipt_id": receipt_id}
        msg = cur.execute("SELECT * FROM external_messages WHERE id=?",
                          (msg_pk,)).fetchone()
        if msg is None:
            return {"result": "message_gone", "receipt_id": receipt_id}
        outcome = _apply_matched_receipt_tx(
            cur, msg=msg, event=latest["event"], receipt_id=receipt_id,
            provider_ts=latest["provider_ts"],
            raw_detail={"replay": True, "operator": operator,
                        "previous_match": latest["matched"]}, ts=now)
        audit.record(cur, "receipt_replayed", None, None, {
            "receipt_id": receipt_id, "operator": operator,
            "external_message_pk": msg_pk,
            "disposition": outcome.get("disposition"),
            "changed": outcome.get("changed", False),
            "kept_status": outcome.get("kept")}, ts=now)
        return {"result": "replayed", "receipt_id": receipt_id,
                "matched_message": msg_pk, "changed": outcome.get("changed", False),
                "kept_status": outcome.get("kept"),
                "disposition": outcome.get("disposition")}


# ============================================================================
# 密钥 / 策略管理
# ============================================================================

def list_receipt_keys(db: Database) -> dict:
    rows = db.query(
        "SELECT * FROM receipt_keys ORDER BY channel, id DESC")
    return {"keys": [{"id": r["id"], "channel": r["channel"], "kid": r["kid"],
                      "status": r["status"], "grace_until": r["grace_until"],
                      "created_by": r["created_by"], "created_at": r["created_at"],
                      "retired_at": r["retired_at"], "retired_by": r["retired_by"]}
                     for r in rows]}   # 密钥明文永不回显


def create_receipt_key(db: Database, channel: str,
                       req: ReceiptKeyRequest) -> dict:
    if channel not in EXTERNAL_CHANNELS:
        raise HTTPException(404, f"unknown channel {channel!r}")
    operator = _require(req.operator, "operator")
    kid = _require(req.kid, "kid")
    secret = _require(req.secret, "secret")
    now = time.time()
    with db.tx() as cur:
        dup = cur.execute(
            "SELECT 1 FROM receipt_keys WHERE channel=? AND kid=?",
            (channel, kid)).fetchone()
        if dup is not None:
            raise HTTPException(409, f"kid {kid!r} already exists for {channel}")
        # 新 active 与旧 active 的过渡期由后续 retire（带 grace_until）显式结束；
        # 这里不自动把旧钥置 retired——否则无法再延长/提前结束过渡期。先下线旧 active
        # （置 retired + 默认 24h 过渡），再插入新 active（顺序受部分唯一索引约束）。
        old = cur.execute(
            """SELECT id FROM receipt_keys WHERE channel=? AND status='active'""",
            (channel,)).fetchall()
        if old:
            cur.execute(
                """UPDATE receipt_keys SET status='retired', grace_until=?,
                   retired_at=?, retired_by=?
                   WHERE channel=? AND status='active'""",
                (now + 86400, now, operator, channel))
        cur.execute(
            """INSERT INTO receipt_keys (channel,kid,secret,status,created_by,
               created_at) VALUES (?,?,?, 'active', ?,?)""",
            (channel, kid, secret, operator, now))
        grace = now + 86400 if old else None
        audit.record(cur, "receipt_key_rotated", None, None, {
            "channel": channel, "kid": kid, "operator": operator,
            "retired": [r["id"] for r in old],
            "grace_until": grace}, ts=now)
        return {"result": "created", "channel": channel, "kid": kid,
                "grace_until": grace}


def retire_receipt_key(db: Database, key_id: int,
                       req: ReceiptKeyRetireRequest) -> dict:
    operator = _require(req.operator, "operator")
    grace = parse_time(req.grace_until)
    now = time.time()
    if grace is None:
        grace = now + 86400
    # grace_until 可在过去：用于立即吊销旧钥（fail-closed，旧签名立刻失效）
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM receipt_keys WHERE id=?",
                          (key_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "receipt key not found")
        if row["status"] == "active":
            cur.execute(
                """UPDATE receipt_keys SET status='retired', grace_until=?,
                   retired_at=?, retired_by=? WHERE id=?""",
                (grace, now, operator, key_id))
        else:
            # 已 retired：允许收紧（或延长）过渡期，用于紧急吊销旧钥
            cur.execute(
                "UPDATE receipt_keys SET grace_until=? WHERE id=?",
                (grace, key_id))
        audit.record(cur, "receipt_key_retired", None, None, {
            "channel": row["channel"], "kid": row["kid"],
            "operator": operator, "grace_until": grace}, ts=now)
        return {"result": "retired", "id": key_id, "grace_until": grace}


def get_policy(db: Database) -> dict:
    row = db.query_one("SELECT * FROM receipt_policy WHERE id=1")
    return {"confirm_timeout_seconds": row["confirm_timeout_seconds"],
            "confirm_max_retries": row["confirm_max_retries"],
            "on_bounced": row["on_bounced"], "on_complained": row["on_complained"],
            "on_expired": row["on_expired"], "updated_by": row["updated_by"],
            "updated_at": row["updated_at"], "reason": row["reason"]}


def update_policy(db: Database, req: ReceiptPolicyRequest) -> dict:
    operator = _require(req.operator, "operator")
    for name, value in (("on_bounced", req.on_bounced),
                        ("on_complained", req.on_complained),
                        ("on_expired", req.on_expired)):
        if value is not None and value not in (ACTION_RETRY, ACTION_MANUAL):
            raise HTTPException(422, f"{name} must be 'retry' or 'manual'")
    if req.confirm_timeout_seconds is not None and \
            req.confirm_timeout_seconds <= 0:
        raise HTTPException(422, "confirm_timeout_seconds must be > 0")
    if req.confirm_max_retries is not None and req.confirm_max_retries < 0:
        raise HTTPException(422, "confirm_max_retries must be >= 0")
    now = time.time()
    with db.tx() as cur:
        old = _load_policy(cur)
        cur.execute(
            """UPDATE receipt_policy SET
               confirm_timeout_seconds=COALESCE(?, confirm_timeout_seconds),
               confirm_max_retries=COALESCE(?, confirm_max_retries),
               on_bounced=COALESCE(?, on_bounced),
               on_complained=COALESCE(?, on_complained),
               on_expired=COALESCE(?, on_expired),
               updated_by=?, updated_at=?, reason=? WHERE id=1""",
            (req.confirm_timeout_seconds, req.confirm_max_retries,
             req.on_bounced, req.on_complained, req.on_expired,
             operator, now, req.reason))
        new = _load_policy(cur)
        audit.record(cur, "receipt_policy_set", None, None, {
            "operator": operator, "reason": req.reason,
            "before": {"confirm_timeout_seconds": old["confirm_timeout_seconds"],
                       "confirm_max_retries": old["confirm_max_retries"],
                       "on_bounced": old["on_bounced"],
                       "on_complained": old["on_complained"],
                       "on_expired": old["on_expired"]},
            "after": {"confirm_timeout_seconds": new["confirm_timeout_seconds"],
                      "confirm_max_retries": new["confirm_max_retries"],
                      "on_bounced": new["on_bounced"],
                      "on_complained": new["on_complained"],
                      "on_expired": new["on_expired"]}}, ts=now)
    return {"result": "updated", "policy": get_policy(db)}


# ============================================================================
# 查询：回执原文 / 消息 / 历史 / 待核对队列
# ============================================================================

_TIME_FILTERS = {
    "received_from": ("r.received_at >= ?",),
    "received_to": ("r.received_at <= ?",),
}


def list_receipts(db, *, channel: str | None = None, recipient: str | None = None,
                  status: str | None = None, matched: str | None = None,
                  message_id: str | None = None, event: str | None = None,
                  time_from: float | None = None, time_to: float | None = None,
                  limit: int = 100) -> dict:
    sql = ["SELECT r.* FROM receipts r WHERE 1=1"]
    params: list = []
    if channel:
        sql.append("AND r.channel=?")
        params.append(channel)
    if recipient:
        sql.append("AND r.recipient=?")
        params.append(recipient)
    if message_id:
        sql.append("AND r.message_id=?")
        params.append(message_id)
    if event:
        sql.append("AND r.event=?")
        params.append(event)
    if matched:
        sql.append("AND r.matched=?")
        params.append(matched)
    if status:
        # status 过滤消息侧的终态
        sql.append("AND EXISTS (SELECT 1 FROM external_messages m "
                   "WHERE m.id=r.message_pk AND m.status=?)")
        params.append(status)
    if time_from is not None:
        sql.append("AND r.received_at>=?")
        params.append(time_from)
    if time_to is not None:
        sql.append("AND r.received_at<=?")
        params.append(time_to)
    sql.append("ORDER BY r.id DESC LIMIT ?")
    params.append(limit)
    rows = db.query(" ".join(sql), tuple(params))
    return {"receipts": [_receipt_view(r) for r in rows], "count": len(rows)}


def _receipt_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "channel": r["channel"], "message_id": r["message_id"],
            "event": r["event"], "recipient": r["recipient"],
            "matched": r["matched"], "message_pk": r["message_pk"],
            "send_task_id": r["send_task_id"], "delivery_id": None,
            "signature_kid": r["signature_kid"], "provider_ts": r["provider_ts"],
            "terminal_rank": r["terminal_rank"], "duplicate_of": r["duplicate_of"],
            "bound_by": r["bound_by"], "bound_at": r["bound_at"],
            "bind_note": r["bind_note"], "received_at": r["received_at"],
            "raw_body": r["raw_body"], "content_type": r["content_type"]}


def get_receipt(db, receipt_id: int) -> dict:
    r = db.query_one("SELECT * FROM receipts WHERE id=?", (receipt_id,))
    if r is None:
        raise HTTPException(404, "receipt not found")
    view = _receipt_view(r)
    view["delivery_id"] = None
    if r["message_pk"]:
        m = db.query_one("SELECT delivery_id FROM external_messages WHERE id=?",
                         (r["message_pk"],))
        if m is not None:
            view["delivery_id"] = m["delivery_id"]
    view["history"] = _history_for_receipt(db, receipt_id)
    return {"receipt": view}


def _history_for_receipt(db, receipt_id: int) -> list[dict]:
    return [{"id": h["id"], "kind": h["kind"], "message_pk": h["message_pk"],
             "receipt_id": h["receipt_id"], "send_task_id": h["send_task_id"],
             "channel": h["channel"], "recipient": h["recipient"],
             "from_status": h["from_status"], "to_status": h["to_status"],
             "reason": h["reason"], "detail": json.loads(h["detail"]),
             "operator": h["operator"], "created_at": h["created_at"]}
            for h in db.query(
                "SELECT * FROM receipt_status_history WHERE receipt_id=? "
                "ORDER BY id", (receipt_id,))]


def list_messages(db, *, channel: str | None = None, recipient: str | None = None,
                  status: str | None = None, message_id: str | None = None,
                  task_id: int | None = None, delivery_id: int | None = None,
                  time_from: float | None = None, time_to: float | None = None,
                  limit: int = 100) -> dict:
    sql = "SELECT * FROM external_messages WHERE 1=1"
    params: list = []
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if status:
        sql += " AND status=?"
        params.append(status)
    if message_id:
        sql += " AND message_id=?"
        params.append(message_id)
    if task_id is not None:
        sql += " AND send_task_id=?"
        params.append(task_id)
    if delivery_id is not None:
        sql += " AND delivery_id=?"
        params.append(delivery_id)
    if time_from is not None:
        sql += " AND registered_at>=?"
        params.append(time_from)
    if time_to is not None:
        sql += " AND registered_at<=?"
        params.append(time_to)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"messages": [{
        "id": r["id"], "channel": r["channel"], "message_id": r["message_id"],
        "id_source": r["id_source"], "source": r["source"],
        "send_task_id": r["send_task_id"], "delivery_id": r["delivery_id"],
        "attempt_id": r["attempt_id"], "recipient": r["recipient"],
        "address": r["address"], "event_id": r["event_id"],
        "event_type": r["event_type"], "status": r["status"],
        "confirm_deadline": r["confirm_deadline"],
        "confirm_retries": r["confirm_retries"],
        "active_receipt_id": r["active_receipt_id"],
        "registered_at": r["registered_at"], "updated_at": r["updated_at"]}
        for r in rows], "count": len(rows)}


def get_message(db, message_pk: int) -> dict:
    r = db.query_one("SELECT * FROM external_messages WHERE id=?", (message_pk,))
    if r is None:
        raise HTTPException(404, "external message not found")
    history = [{"id": h["id"], "kind": h["kind"], "from_status": h["from_status"],
                "to_status": h["to_status"], "reason": h["reason"],
                "receipt_id": h["receipt_id"], "send_task_id": h["send_task_id"],
                "operator": h["operator"],
                "detail": json.loads(h["detail"]),
                "created_at": h["created_at"]}
               for h in db.query(
                   "SELECT * FROM receipt_status_history WHERE message_pk=? "
                   "ORDER BY id", (message_pk,))]
    receipts = [_receipt_view(x) for x in db.query(
        "SELECT * FROM receipts WHERE message_pk=? ORDER BY id", (message_pk,))]
    return {"message": {
        "id": r["id"], "channel": r["channel"], "message_id": r["message_id"],
        "id_source": r["id_source"], "source": r["source"],
        "send_task_id": r["send_task_id"], "delivery_id": r["delivery_id"],
        "recipient": r["recipient"], "address": r["address"],
        "status": r["status"], "confirm_deadline": r["confirm_deadline"],
        "confirm_retries": r["confirm_retries"],
        "active_receipt_id": r["active_receipt_id"],
        "registered_at": r["registered_at"], "updated_at": r["updated_at"]},
        "history": history, "receipts": receipts}


def list_history(db, *, channel: str | None = None, recipient: str | None = None,
                 status: str | None = None, task_id: int | None = None,
                 message_pk: int | None = None, time_from: float | None = None,
                 time_to: float | None = None, limit: int = 100) -> dict:
    sql = "SELECT * FROM receipt_status_history WHERE 1=1"
    params: list = []
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if status:
        sql += " AND to_status=?"
        params.append(status)
    if task_id is not None:
        sql += " AND send_task_id=?"
        params.append(task_id)
    if message_pk is not None:
        sql += " AND message_pk=?"
        params.append(message_pk)
    if time_from is not None:
        sql += " AND created_at>=?"
        params.append(time_from)
    if time_to is not None:
        sql += " AND created_at<=?"
        params.append(time_to)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"history": [{"id": h["id"], "kind": h["kind"],
                         "message_pk": h["message_pk"],
                         "receipt_id": h["receipt_id"],
                         "send_task_id": h["send_task_id"],
                         "channel": h["channel"], "recipient": h["recipient"],
                         "from_status": h["from_status"],
                         "to_status": h["to_status"], "reason": h["reason"],
                         "detail": json.loads(h["detail"]),
                         "operator": h["operator"],
                         "created_at": h["created_at"]} for h in rows],
            "count": len(rows)}


def review_queue(db, *, include_ignored: bool = False, limit: int = 100) -> dict:
    """待核对队列：无法匹配发送任务的回执 + 等待人工处置的发送任务/消息。"""
    where = "matched='unmatched'"
    if include_ignored:
        where = "matched IN ('unmatched','ignored')"
    receipts = db.query(
        f"SELECT * FROM receipts WHERE {where} ORDER BY id DESC LIMIT ?",
        (limit,))
    manual_tasks = db.query(
        """SELECT t.* FROM notif_send_tasks t
           WHERE t.status='awaiting_manual' ORDER BY t.id DESC LIMIT ?""",
        (limit,))
    stale_messages = db.query(
        """SELECT * FROM external_messages
           WHERE status='awaiting_confirmation' ORDER BY confirm_deadline,id
           LIMIT ?""", (limit,))
    return {
        "unmatched_receipts": [_receipt_view(r) for r in receipts],
        "awaiting_manual_tasks": [{
            "task_id": t["id"], "recipient": t["recipient"],
            "event_type": t["event_type"], "channel_hint": t["sent_channel"],
            "receipt_status": t["receipt_status"],
            "receipt_reason": t["receipt_reason"],
            "external_message_id": t["external_message_id"],
            "updated_at": t["updated_at"]} for t in manual_tasks],
        "awaiting_confirmation_messages": [{
            "message_pk": m["id"], "channel": m["channel"],
            "message_id": m["message_id"], "recipient": m["recipient"],
            "send_task_id": m["send_task_id"], "delivery_id": m["delivery_id"],
            "confirm_deadline": m["confirm_deadline"],
            "confirm_retries": m["confirm_retries"]} for m in stale_messages]}


# ============================================================================
# 路由：公共接入口 + 管理端
# ============================================================================

def create_receipts_public_router(db: Database) -> APIRouter:
    """外部通道回执接入口（独立按通道密钥验签）。"""
    router = APIRouter(tags=["receipts"])

    @router.post("/receipts/{channel}")
    async def receive(channel: str, request: Request):
        body = await request.body()
        headers = {k.lower(): v for k, v in request.headers.items()}
        status, content = ingest_receipt(
            db, channel, body, headers.get("x-signature"),
            headers.get("content-type"))
        return JSONResponse(status_code=status, content=content)

    return router


def create_receipts_admin_router(db: Database, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["receipts-admin"])

    # ---- 回执 / 消息 / 历史查询 -------------------------------------------
    @router.get("/receipts")
    def receipts(channel: str | None = None, recipient: str | None = None,
                 status: str | None = None, matched: str | None = None,
                 message_id: str | None = None, event: str | None = None,
                 time_from: float | None = None, time_to: float | None = None,
                 limit: int = Query(100, le=1000)):
        """按通道/接收人/时间/状态查询回执原文（含重复与待核对）。"""
        return list_receipts(
            db, channel=channel, recipient=recipient, status=status,
            matched=matched, message_id=message_id, event=event,
            time_from=time_from, time_to=time_to, limit=limit)

    @router.get("/receipts/{receipt_id}")
    def receipt_detail(receipt_id: int):
        return get_receipt(db, receipt_id)

    @router.post("/receipts/{receipt_id}/bind")
    def bind(receipt_id: int, req: BindReceiptRequest):
        """人工把未知回执绑定到发送任务（不改正文）。"""
        return bind_unmatched_receipt(db, receipt_id, req)

    @router.post("/receipts/{receipt_id}/ignore")
    def ignore(receipt_id: int, req: IgnoreReceiptRequest):
        return ignore_unmatched_receipt(db, receipt_id, req)

    @router.post("/receipts/{receipt_id}/replay")
    def replay(receipt_id: int, req: OperatorRequest):
        """用存证原文安全重放（幂等；终态不回退；不产生第二次外部效果）。"""
        return replay_receipt(db, receipt_id, req.operator)

    @router.get("/external-messages")
    def messages(channel: str | None = None, recipient: str | None = None,
                 status: str | None = None, message_id: str | None = None,
                 task_id: int | None = None, delivery_id: int | None = None,
                 time_from: float | None = None, time_to: float | None = None,
                 limit: int = Query(100, le=1000)):
        return list_messages(
            db, channel=channel, recipient=recipient, status=status,
            message_id=message_id, task_id=task_id, delivery_id=delivery_id,
            time_from=time_from, time_to=time_to, limit=limit)

    @router.get("/external-messages/{message_pk}")
    def message_detail(message_pk: int):
        """单条外部消息：回执状态变化历史 + 全部回执原文。"""
        return get_message(db, message_pk)

    @router.get("/receipt-history")
    def history(channel: str | None = None, recipient: str | None = None,
                status: str | None = None, task_id: int | None = None,
                message_pk: int | None = None, time_from: float | None = None,
                time_to: float | None = None, limit: int = Query(100, le=1000)):
        return list_history(
            db, channel=channel, recipient=recipient, status=status,
            task_id=task_id, message_pk=message_pk, time_from=time_from,
            time_to=time_to, limit=limit)

    @router.get("/receipt-review-queue")
    def review(include_ignored: bool = False, limit: int = Query(100, le=1000)):
        """待核对队列：未匹配回执 + 待人工任务 + 超时待确认消息。"""
        return review_queue(db, include_ignored=include_ignored, limit=limit)

    @router.post("/approval-notifications/routing/tasks/{task_id}/receipt-resolve")
    def resolve_task(task_id: int, req: ManualResolveRequest):
        """处置 awaiting_manual 的发送任务：retry（开新一轮）/ ignore。"""
        return resolve_manual_task(db, task_id, req, settings)

    # ---- 策略 / 密钥 ------------------------------------------------------
    @router.get("/receipt-policy")
    def policy_get():
        return get_policy(db)

    @router.put("/receipt-policy")
    def policy_set(req: ReceiptPolicyRequest):
        return update_policy(db, req)

    @router.get("/receipt-keys")
    def keys_list():
        return list_receipt_keys(db)

    @router.post("/receipt-keys/{channel}")
    def keys_create(channel: str, req: ReceiptKeyRequest):
        return create_receipt_key(db, channel, req)

    @router.post("/receipt-keys/{key_id}/retire")
    def keys_retire(key_id: int, req: ReceiptKeyRetireRequest):
        return retire_receipt_key(db, key_id, req)

    return router

"""通知通道路由与故障转移（notification routing & failover）。

在既有审批通知、待办与升级链路之上提供**版本化的通道选择、按通道熔断与有序故障转移**。
本模块与既有「每通道一条 approval_notification_deliveries」链路互斥：

- 从未发布过路由版本（notif_route_current.route_version IS NULL）时，通知仍走旧链路
  （contacts.channels + 聚合/静默），存量行为完全不变；
- 一旦发布路由版本，新生成的待办改走本模块：每个「事件 × 接收人」只入**一条**
  notif_send_tasks，固化入队时版本的有序通道计划快照，之后发布/回滚都不影响它。

通道（channel）
- email / webhook：既有两个外发通道（地址取联系人目录）；
- inbox：站内通知。审批链路里站内待办始终逐条即时生成（绝不被聚合/静默/路由吞掉），
  因此 inbox 在发送任务里只是「最后兜底通道」——计划走到 inbox 时记一条成功尝试即完成，
  保证同一事件对同一接收人始终有且仅有一次业务通知，不会因外发通道全挂而漏通知。

路由版本（notif_route_versions / notif_route_current）
- POST /routes 提交配置：按事件类型给出通道链（优先级顺序、启用状态、接收条件、
  每通道超时与尝试次数）。校验通过整份生效（版本号单调递增），失败保留当前版本；
- POST /routes/rollback 回滚到上一生效版本（原因必填），只影响之后入队的任务；
- 发送任务按入队时快照选通道，已入队任务不随发布/回滚改变。

熔断与故障转移（notif_channel_state / notif_send_attempts / notif_channel_switches）
- 通道在「时间窗口内连续失败」（含超时）达到阈值即 open：期间不向该通道派发新请求；
- open 冷却 cooldown 秒后转 half_open，只放**一条**恢复探针；探针成功才 closed 重新
  接流量，探针失败立即回到 open；
- 首选通道超时（立即，不在本通道重试）或连续失败达到该通道 max_attempts 时才切换到
  下一通道；首选被熔断时新任务直接从下一通道开始（切换原因 breaker_open）；
- 每次尝试、每次切换的原因与最终结果（sent/quarantined）逐行落盘并写审计。

幂等与并发
- notif_send_tasks UNIQUE(event_id, recipient)：同一事件对同一接收人跨通道只产生
  一条业务通知任务；worker 领取是条件 UPDATE（pending -> in_flight，单赢家），
  重复扫描、服务重启（recover 把 in_flight 退回 pending）、并发 worker 都不会重复发送；
- notif_send_attempts 记录每次尝试；熔断统计直接查它（窗口内、自上次成功起的连续
  失败数），不维护任何会漂移的计数器。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeout

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit, notifications as notif
from . import receipts
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications.routing")

# ---- 常量 -------------------------------------------------------------------

CHANNEL_EMAIL = "email"
CHANNEL_WEBHOOK = "webhook"
CHANNEL_INBOX = "inbox"
ROUTABLE_CHANNELS = (CHANNEL_EMAIL, CHANNEL_WEBHOOK, CHANNEL_INBOX)
# 外发通道（地址来自联系人目录，受熔断约束）；inbox 为站内兜底，永不可熔断
EXTERNAL_CHANNELS = (CHANNEL_EMAIL, CHANNEL_WEBHOOK)

TASK_PENDING = "pending"
TASK_IN_FLIGHT = "in_flight"
TASK_SENT = "sent"
TASK_FAILED = "failed"
TASK_QUARANTINED = "quarantined"
TASK_CANCELLED = "cancelled"
# 回执失败转人工 / 额度超额转人工（receipts / notif_quota 设置）
TASK_AWAITING_MANUAL = "awaiting_manual"
# 内容模板渲染失败（变量缺失/类型不符/无语言版本/超长/敏感未脱敏）：终态，worker 不
# 自动派发，管理员修复模板/变量后经 retry-render 重新渲染回 pending
TASK_RENDER_FAILED = "render_failed"

# 任务终态（不会再被 worker 派发；取消/渲染失败隔离在列）
TASK_TERMINAL_STATES = (TASK_SENT, TASK_QUARANTINED, TASK_CANCELLED,
                        TASK_RENDER_FAILED)

BREAKER_CLOSED = "closed"
BREAKER_OPEN = "open"
BREAKER_HALF_OPEN = "half_open"

ATTEMPT_SUCCESS = "success"
ATTEMPT_FAILURE = "failure"
ATTEMPT_TIMEOUT = "timeout"

# 切换原因
SW_SELECTED = "selected"
SW_TIMEOUT = "timeout"
SW_CONSECUTIVE_FAILURES = "consecutive_failures"
SW_BREAKER_OPEN = "breaker_open"
SW_CHANNEL_DISABLED = "channel_disabled"
SW_NO_ADDRESS = "no_address"
SW_PLAN_EXHAUSTED = "plan_exhausted"
# 模板渲染失败（变量缺失/类型不符/无语言版本/超长/敏感未脱敏），通道从计划剔除
SW_TEMPLATE_RENDER_FAILED = "template_render_failed"

# 无路由版本/无匹配规则时的内置兜底计划：邮件 -> webhook -> 站内（发布了路由但该事件
# 类型未配置时使用，保证路由开启后仍 fail-safe，站内待办始终兜底）
FALLBACK_PLAN = [
    {"channel": CHANNEL_EMAIL, "enabled": True, "timeout_seconds": None,
     "max_attempts": None, "condition": None},
    {"channel": CHANNEL_WEBHOOK, "enabled": True, "timeout_seconds": None,
     "max_attempts": None, "condition": None},
    {"channel": CHANNEL_INBOX, "enabled": True, "timeout_seconds": None,
     "max_attempts": 1, "condition": None},
]


class ChannelTimeout(Exception):
    """通道发送超过配置的 timeout_seconds（被工作线程强杀）。"""


# ---- 请求模型 ----------------------------------------------------------------

class ChannelCondition(BaseModel):
    actionable: bool | None = None        # 只要可操作/纯告知事件
    source_type: str | None = None        # batch | change
    risk_level: str | None = None         # normal | high（仅 batch 事件）
    recipients: list[str] | None = None   # 接收人白名单（缺省=任意）


class ChannelRouteSpec(BaseModel):
    channel: str
    enabled: bool = True
    priority: int = 100
    timeout_seconds: float | None = None     # 缺省取 NOTIF_CHANNEL_TIMEOUT_SECONDS
    max_attempts: int | None = None          # 该通道连续失败几次后切换；缺省取 NOTIF_MAX_ATTEMPTS
    condition: ChannelCondition | None = None


class EventRouteRule(BaseModel):
    event_type: str | None = None            # None/空/'*' = 缺省规则
    channels: list[ChannelRouteSpec]


class RoutingConfigRequest(BaseModel):
    operator: str
    note: str = ""
    rules: list[EventRouteRule]
    # 可选：随发布调整各通道熔断参数与启用状态（不传则沿用现有/默认）
    breaker: dict[str, dict] | None = None


class RollbackRequest(BaseModel):
    operator: str
    reason: str


class OperatorRequest(BaseModel):
    operator: str


class ChannelStateRequest(BaseModel):
    operator: str
    enabled: bool
    failure_threshold: int | None = None
    window_seconds: float | None = None
    cooldown_seconds: float | None = None
    reset: bool = False                      # True：强制回到 closed（人工排障后恢复）


# ============================================================================
# 配置校验 / 规范化
# ============================================================================

def _validate_config(config: dict, settings: Settings) -> tuple[list[dict], dict]:
    """校验并规范化路由配置。返回 (rules, breaker_config)。

    失败抛 HTTPException(422, 原因列表式 message)，当前配置原样保留。
    """
    if not isinstance(config, dict) or not isinstance(config.get("rules"), list):
        raise HTTPException(422, "config must be an object with a non-empty 'rules' list")
    if not config["rules"]:
        raise HTTPException(422, "rules must be a non-empty list")
    norm_rules: list[dict] = []
    seen_types: set[str] = set()
    for i, rule in enumerate(config["rules"]):
        if not isinstance(rule, dict) or not isinstance(rule.get("channels"), list):
            raise HTTPException(422, f"rule {i}: must be an object with a 'channels' list")
        etype = rule.get("event_type")
        if etype in ("", "*"):
            etype = None
        if etype is not None:
            etype = str(etype)
            if etype in seen_types:
                raise HTTPException(422, f"duplicate event_type {etype!r} in rules")
            seen_types.add(etype)
        channels = rule["channels"]
        if not channels:
            raise HTTPException(422, f"rule {i} ({etype or 'default'}): channels "
                                    "must be a non-empty list")
        norm_channels, channel_seen = [], set()
        for ch in channels:
            name = ch.get("channel")
            if name not in ROUTABLE_CHANNELS:
                raise HTTPException(422, f"rule {i}: unsupported channel {name!r} "
                                         f"(support {','.join(ROUTABLE_CHANNELS)})")
            enabled = bool(ch.get("enabled", True))
            priority = int(ch.get("priority", 100))
            timeout = ch.get("timeout_seconds")
            if timeout is not None:
                timeout = float(timeout)
                if timeout <= 0:
                    raise HTTPException(422, f"channel {name}: timeout_seconds must be > 0")
            else:
                timeout = settings.notif_channel_timeout_seconds
            max_attempts = ch.get("max_attempts")
            if max_attempts is not None:
                max_attempts = int(max_attempts)
                if max_attempts < 1:
                    raise HTTPException(422, f"channel {name}: max_attempts must be >= 1")
            else:
                # inbox 一次成功即可；外发通道缺省沿用通知重试上限
                max_attempts = (1 if name == CHANNEL_INBOX
                               else settings.notif_max_attempts)
            cond = ch.get("condition")
            norm_cond = None
            if cond is not None:
                if not isinstance(cond, dict):
                    raise HTTPException(422, f"channel {name}: condition must be an object")
                st = cond.get("source_type")
                if st is not None and st not in ("batch", "change"):
                    raise HTTPException(422, f"channel {name}: condition.source_type "
                                            "must be 'batch' or 'change'")
                rl = cond.get("risk_level")
                if rl is not None and rl not in ("normal", "high"):
                    raise HTTPException(422, f"channel {name}: condition.risk_level "
                                            "must be 'normal' or 'high'")
                recips = cond.get("recipients")
                if recips is not None:
                    if not isinstance(recips, list) or not recips or \
                            any(not str(r).strip() for r in recips):
                        raise HTTPException(422, f"channel {name}: condition.recipients "
                                                "must be a non-empty string list")
                    recips = sorted({str(r).strip() for r in recips})
                ab = cond.get("actionable")
                norm_cond = {
                    "actionable": (None if ab is None else bool(ab)),
                    "source_type": st, "risk_level": rl, "recipients": recips}
            if name in channel_seen:
                raise HTTPException(422, f"rule {i} ({etype or 'default'}): "
                                        f"channel {name} listed more than once")
            channel_seen.add(name)
            norm_channels.append({"channel": name, "enabled": enabled,
                                  "priority": priority,
                                  "timeout_seconds": timeout,
                                  "max_attempts": max_attempts,
                                  "condition": norm_cond})
        # 优先级顺序（priority 升序；相同 priority 按提交顺序），禁用通道保留在配置里
        norm_channels.sort(key=lambda c: c["priority"])
        norm_rules.append({"event_type": etype, "channels": norm_channels})
    # 熔断参数校验
    breaker_cfg: dict = {}
    for ch, params in (config.get("breaker") or {}).items():
        if ch not in ROUTABLE_CHANNELS:
            raise HTTPException(422, f"breaker: unsupported channel {ch!r}")
        if not isinstance(params, dict):
            raise HTTPException(422, f"breaker.{ch}: must be an object")
        entry: dict = {}
        if params.get("failure_threshold") is not None:
            entry["failure_threshold"] = int(params["failure_threshold"])
            if entry["failure_threshold"] < 1:
                raise HTTPException(422, f"breaker.{ch}: failure_threshold must be >= 1")
        if params.get("window_seconds") is not None:
            entry["window_seconds"] = float(params["window_seconds"])
            if entry["window_seconds"] <= 0:
                raise HTTPException(422, f"breaker.{ch}: window_seconds must be > 0")
        if params.get("cooldown_seconds") is not None:
            entry["cooldown_seconds"] = float(params["cooldown_seconds"])
            if entry["cooldown_seconds"] < 0:
                raise HTTPException(422, f"breaker.{ch}: cooldown_seconds must be >= 0")
        if "enabled" in params:
            entry["enabled"] = bool(params["enabled"])
        if entry:
            breaker_cfg[ch] = entry
    return norm_rules, breaker_cfg


def publish_routing(db: Database, req: RoutingConfigRequest,
                    settings: Settings) -> dict:
    """整份发布路由配置：校验通过 -> 新版本落盘 + 推进当前指针（同一事务）。

    被拒绝的提交也落一条 rejected 版本记录（含原因），当前版本保持不变。
    """
    operator = notif._require(req.operator, "operator")
    raw = {"rules": [r.model_dump() for r in req.rules],
           "breaker": req.breaker}
    now = time.time()
    try:
        norm_rules, breaker_cfg = _validate_config(raw, settings)
    except HTTPException as exc:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO notif_route_versions
                   (version, result, config_json, operator, reason, created_at)
                   VALUES (NULL,'rejected',?,?,?,?)""",
                (json.dumps(raw, ensure_ascii=False), operator,
                 json.dumps(exc.detail, ensure_ascii=False), now))
            audit.record(cur, "notif_route_rejected", None, None,
                         {"operator": operator, "reason": exc.detail}, ts=now)
        raise
    config = {"rules": norm_rules}
    with db.tx() as cur:
        row = cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM notif_route_versions "
            "WHERE result IN ('applied','rollback')").fetchone()
        version = int(row["v"])
        cur.execute(
            """INSERT INTO notif_route_versions
               (version, result, config_json, operator, reason, created_at)
               VALUES (?, 'applied', ?, ?, NULL, ?)""",
            (version, json.dumps(config, ensure_ascii=False, sort_keys=True),
             operator, now))
        prev = cur.execute(
            "SELECT route_version FROM notif_route_current WHERE id=1").fetchone()
        prev_version = prev["route_version"] if prev is not None else None
        cur.execute(
            """UPDATE notif_route_current SET route_version=?, updated_by=?,
               updated_at=?, reason=? WHERE id=1""",
            (version, operator, now, req.note or None))
        _upsert_breaker_settings_tx(cur, breaker_cfg, settings, now)
        audit.record(cur, "notif_route_published", None, None, {
            "version": version, "operator": operator, "note": req.note,
            "prev_version": prev_version,
            "rules": [r["event_type"] or "*" for r in norm_rules]}, ts=now)
    return {"result": "applied", "version": version}


def rollback_routing(db: Database, req: RollbackRequest) -> dict:
    """回滚到上一生效版本（原因必填）。只推进指针，不生成新版本内容；只影响之后
    入队的任务——已入队任务持有自己的快照，永不改变。"""
    operator = notif._require(req.operator, "operator")
    reason = (req.reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason must be non-empty for rollback")
    now = time.time()
    with db.tx() as cur:
        current = cur.execute(
            "SELECT route_version FROM notif_route_current WHERE id=1").fetchone()
        cur_version = current["route_version"] if current else None
        if cur_version is None:
            raise HTTPException(409, "no routing version is currently applied")
        # 上一生效版本：当前版本之前最近一条 applied/rollback 指向的版本
        prev = cur.execute(
            """SELECT version FROM notif_route_versions
               WHERE result IN ('applied','rollback') AND version < ?
               ORDER BY id DESC LIMIT 1""", (cur_version,)).fetchone()
        if prev is None or prev["version"] is None:
            raise HTTPException(409, "no previous routing version to roll back to")
        target = int(prev["version"])
        target_row = cur.execute(
            "SELECT config_json FROM notif_route_versions WHERE version=? "
            "ORDER BY id DESC LIMIT 1", (target,)).fetchone()
        cur.execute(
            """INSERT INTO notif_route_versions
               (version, result, config_json, operator, reason, created_at)
               VALUES (?, 'rollback', ?, ?, ?, ?)""",
            (target, target_row["config_json"], operator, reason, now))
        cur.execute(
            """UPDATE notif_route_current SET route_version=?, updated_by=?,
               updated_at=?, reason=? WHERE id=1""",
            (target, operator, now, reason))
        audit.record(cur, "notif_route_rolled_back", None, None, {
            "from_version": cur_version, "version": target,
            "operator": operator, "reason": reason}, ts=now)
    return {"result": "rolled_back", "version": target}


def _ensure_channel_states(cur: sqlite3.Cursor, settings: Settings, now: float) -> None:
    """保证三个通道各有一行健康状态（首次发布/启动时按默认配置补齐；inbox 永不可熔断）。"""
    for ch in ROUTABLE_CHANNELS:
        exists = cur.execute("SELECT 1 AS x FROM notif_channel_state WHERE channel=?",
                             (ch,)).fetchone()
        if exists:
            continue
        cur.execute(
            """INSERT INTO notif_channel_state
               (channel, state, enabled, failure_threshold, window_seconds,
                cooldown_seconds, opened_at, updated_at)
               VALUES (?, 'closed', 1, ?, ?, ?, NULL, ?)""",
            (ch, settings.notif_breaker_failure_threshold,
             settings.notif_breaker_window_seconds,
             settings.notif_breaker_cooldown_seconds, now))


def _upsert_breaker_settings_tx(cur: sqlite3.Cursor, breaker_cfg: dict,
                                settings: Settings, now: float) -> None:
    _ensure_channel_states(cur, settings, now)
    for ch, params in breaker_cfg.items():
        if "failure_threshold" in params:
            cur.execute("UPDATE notif_channel_state SET failure_threshold=?, updated_at=? "
                        "WHERE channel=?",
                        (params["failure_threshold"], now, ch))
        if "window_seconds" in params:
            cur.execute("UPDATE notif_channel_state SET window_seconds=?, updated_at=? "
                        "WHERE channel=?", (params["window_seconds"], now, ch))
        if "cooldown_seconds" in params:
            cur.execute("UPDATE notif_channel_state SET cooldown_seconds=?, updated_at=? "
                        "WHERE channel=?", (params["cooldown_seconds"], now, ch))
        if "enabled" in params:
            cur.execute("UPDATE notif_channel_state SET enabled=?, updated_at=? "
                        "WHERE channel=?", (1 if params["enabled"] else 0, now, ch))


# ============================================================================
# 路由解析与入队（在待办创建事务内调用）
# ============================================================================

def current_version(db: Database) -> int | None:
    row = db.query_one("SELECT route_version FROM notif_route_current WHERE id=1")
    return row["route_version"] if row is not None else None


def _load_version(cur: sqlite3.Cursor, version: int) -> dict:
    row = cur.execute(
        "SELECT config_json FROM notif_route_versions WHERE version=? "
        "AND result IN ('applied','rollback') ORDER BY id DESC LIMIT 1",
        (version,)).fetchone()
    return json.loads(row["config_json"]) if row else {"rules": []}


def _condition_matches(cond: dict | None, *, actionable: bool, source_type: str,
                       risk_level: str | None, recipient: str) -> bool:
    if cond is None:
        return True
    if cond.get("actionable") is not None and bool(cond["actionable"]) != bool(actionable):
        return False
    if cond.get("source_type") is not None and cond["source_type"] != source_type:
        return False
    if cond.get("risk_level") is not None:
        if source_type != "batch" or risk_level != cond["risk_level"]:
            return False
    recips = cond.get("recipients")
    if recips is not None and recipient not in recips:
        return False
    return True


def _event_risk_level(cur: sqlite3.Cursor, source_type: str,
                      batch_id: int | None) -> str | None:
    if source_type != notif.SOURCE_BATCH or batch_id is None:
        return None
    row = cur.execute("SELECT risk_level FROM replay_batches WHERE id=?",
                      (batch_id,)).fetchone()
    return row["risk_level"] if row else None


def resolve_plan(cur: sqlite3.Cursor, config: dict, *, event_type: str,
                 actionable: bool, source_type: str, risk_level: str | None,
                 recipient: str) -> list[dict]:
    """按当前版本配置解析该事件的有序通道链：显式 event_type 规则优先，否则缺省规则，
    都没有则内置兜底计划。再按启用状态与接收条件过滤出本次实际通道序列。"""
    rules = config.get("rules") or []
    rule = next((r for r in rules if r.get("event_type") == event_type), None)
    if rule is None:
        rule = next((r for r in rules if r.get("event_type") is None), None)
    chain = FALLBACK_PLAN if rule is None else rule["channels"]
    plan = []
    for spec in chain:
        if not spec.get("enabled", True):
            continue
        if not _condition_matches(spec.get("condition"), actionable=actionable,
                                  source_type=source_type, risk_level=risk_level,
                                  recipient=recipient):
            continue
        plan.append(spec)
    return plan


def _channel_address(cur: sqlite3.Cursor, channel: str,
                     recipient: str) -> sqlite3.Row | None:
    if channel == CHANNEL_INBOX:
        return None
    return cur.execute(
        "SELECT email, webhook_url FROM approval_contacts WHERE name=? AND active=1",
        (recipient,)).fetchone()


def enqueue_for_todo_tx(cur: sqlite3.Cursor, todo, event, settings: Settings,
                        now: float) -> dict:
    """待办创建事务内：若已发布路由版本，则按版本快照为该「事件×接收人」入一条
    发送任务（UNIQUE 兜底并发/重放）；未发布路由版本时什么都不做（走旧链路）。

    返回 {"routed": bool, "task_id": int|None, "plan": [channels], "skipped_reason": str|None}
    """
    pointer = cur.execute(
        "SELECT route_version FROM notif_route_current WHERE id=1").fetchone()
    version = pointer["route_version"] if pointer is not None else None
    if version is None:
        return {"routed": False, "task_id": None, "plan": [], "skipped_reason": None}
    existing = cur.execute(
        "SELECT id FROM notif_send_tasks WHERE event_id=? AND recipient=?",
        (event["id"], todo["recipient"])).fetchone()
    if existing is not None:
        # 重启/重复扫描/并发：同一事件对同一接收人绝不产生第二条业务通知任务
        return {"routed": True, "task_id": existing["id"], "plan": [],
                "skipped_reason": "duplicate_event_recipient"}
    _ensure_channel_states(cur, settings, now)
    config = _load_version(cur, version)
    risk_level = _event_risk_level(cur, todo["source_type"], todo["batch_id"])
    specs = resolve_plan(
        cur, config, event_type=event["event_type"],
        actionable=bool(event["actionable"]), source_type=todo["source_type"],
        risk_level=risk_level, recipient=todo["recipient"])
    # 额度快照（按接收人 × 事件级别 × 时间窗口）：与路由版本相互独立，入队时命中即固化，
    # 之后额度配置的发布/回滚不改变本任务的占用规则。
    from . import notif_quota
    qsnap = notif_quota.snapshot_for_enqueue(
        cur, recipient=todo["recipient"], event_type=event["event_type"])
    # 先落任务（拿到 task_id），再逐条解析地址，使「无地址跳过」的切换记录也能关联到
    # 本任务（完整切换轨迹）。计划为 [] 时任务在同一事务内取消并留 skipped 审计。
    cur.execute(
        """INSERT INTO notif_send_tasks
           (todo_id, event_id, recipient, event_type, source_type, batch_id,
            change_id, node_id, subject, body, ordinal, status, route_version,
            route_snapshot, plan_json, attempt_index, total_attempts,
            current_channel, round, next_retry_at,
            quota_version, quota_rule_id, quota_level, quota_snapshot,
            created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?, 'pending', ?, ?, '[]', 0, 0, NULL, 1, NULL,
                   ?,?,?,?, ?, ?)""",
        (todo["id"], event["id"], todo["recipient"], event["event_type"],
         todo["source_type"], todo["batch_id"], todo["change_id"], todo["node_id"],
         event["subject"], event["body"], todo["id"], version,
         json.dumps({"version": version, "event_type": event["event_type"],
                     "plan": [{"channel": s["channel"],
                               "timeout_seconds": s["timeout_seconds"],
                               "max_attempts": s["max_attempts"],
                               "condition": s.get("condition")} for s in specs]},
                    ensure_ascii=False, sort_keys=True),
         qsnap["quota_version"] if qsnap else None,
         qsnap["rule_id"] if qsnap else None,
         qsnap["level"] if qsnap else None,
         json.dumps(qsnap["snapshot"], ensure_ascii=False, sort_keys=True)
         if qsnap else None,
         now, now))
    task_id = cur.lastrowid
    # 内容模板：按计划中的每个具体通道在入队事务内渲染一次（语言回退、变量/类型/长度/
    # 敏感校验都在此刻完成），渲染结果固化进计划项与 content_snapshot；之后模板编辑、
    # 新版本发布都不影响本任务。渲染失败的通道从计划剔除并落 notif_template_render_failures。
    from . import notif_templates
    payload = json.loads(event["payload"] or "{}")
    plan: list[dict] = []
    content_snapshot: dict = {}
    template_meta = None
    render_failures: list[dict] = []
    for spec in specs:
        ch = spec["channel"]
        address = None
        if ch == CHANNEL_EMAIL:
            contact = _channel_address(cur, ch, todo["recipient"])
            address = contact["email"] if contact else None
            if not address:
                _record_switch_tx(cur, task_id, event["id"], todo["recipient"],
                                  None, ch, SW_NO_ADDRESS,
                                  {"note": "contact has no email address"}, now)
                continue
        elif ch == CHANNEL_WEBHOOK:
            contact = _channel_address(cur, ch, todo["recipient"])
            address = contact["webhook_url"] if contact else None
            if not address:
                _record_switch_tx(cur, task_id, event["id"], todo["recipient"],
                                  None, ch, SW_NO_ADDRESS,
                                  {"note": "contact has no webhook url"}, now)
                continue
        # 该通道已可用：解析模板并渲染
        try:
            tres = notif_templates.render_for_channel_tx(
                cur, event_type=event["event_type"], channel=ch,
                recipient=todo["recipient"], supplied=payload, settings=settings)
        except notif_templates.TemplateError as exc:
            fid = notif_templates.record_failure_tx(
                cur, entity_type="task", channel=ch, todo_id=todo["id"],
                event_id=event["id"], recipient=todo["recipient"],
                event_type=event["event_type"], exc=exc, send_task_id=task_id,
                now=now)
            render_failures.append({"channel": ch, "failure_id": fid,
                                    "code": exc.code, "message": exc.message})
            _record_switch_tx(cur, task_id, event["id"], todo["recipient"],
                              None, ch, SW_TEMPLATE_RENDER_FAILED,
                              {"code": exc.code, "message": exc.message,
                               "failure_id": fid}, now)
            continue
        if not tres["templated"]:
            # 未配置模板：沿用事件静态正文（行为与引入模板前完全一致）
            rsubject, rbody = event["subject"], event["body"]
            rlang, lang_chain, version_id, var_snapshot = None, [], None, {}
            channel_payload = payload
        else:
            rendered = tres["rendered"]
            rsubject, rbody = rendered["subject"], rendered["body"]
            rlang, lang_chain = rendered["language"], rendered["language_chain"]
            version_id = tres["template_version_id"]
            var_snapshot = rendered["variables_snapshot"]
            template_meta = {
                "template_version_id": version_id,
                "variables": tres["template"]["variables"],
                "languages": tres["template"]["languages"],
                "fallback_languages": tres["template"]["fallback_languages"]}
            channel_payload = {**payload, "subject": rsubject, "body": rbody}
        sha = notif_templates.content_hash(
            channel=ch, subject=rsubject, body=rbody)
        item = {"channel": ch, "address": address,
                "timeout_seconds": spec["timeout_seconds"],
                "max_attempts": spec["max_attempts"],
                "condition": spec.get("condition"),
                # 固化的最终内容：发送器与重试都只读这里，不重新渲染
                "subject": rsubject, "body": rbody,
                "template_version_id": version_id, "language": rlang,
                "language_chain": lang_chain, "content_sha256": sha}
        if ch == CHANNEL_WEBHOOK:
            item["webhook_payload"] = channel_payload
        plan.append(item)
        content_snapshot[ch] = {
            "template_version_id": version_id, "language": rlang,
            "language_chain": lang_chain, "subject": rsubject, "body": rbody,
            "variables_snapshot": var_snapshot, "content_sha256": sha}
    if not plan:
        if render_failures:
            # 全部通道因模板渲染失败被剔除：终态 render_failed（不派发、不自动重试、
            # 不发任何外部消息），失败原因逐通道可查，修复后可手动 retry-render。
            cur.execute(
                """UPDATE notif_send_tasks SET status='render_failed',
                   plan_json='[]', render_status='failed',
                   render_failure_reason=?, template_snapshot=?, content_snapshot=?,
                   current_channel=NULL, updated_at=? WHERE id=?""",
                (json.dumps(render_failures, ensure_ascii=False),
                 json.dumps(template_meta, ensure_ascii=False, sort_keys=True),
                 json.dumps(content_snapshot, ensure_ascii=False, sort_keys=True),
                 now, task_id))
            audit.record(cur, "notif_send_task_render_blocked", None, None, {
                "send_task_id": task_id, "todo_id": todo["id"],
                "notify_event_id": event["id"], "event_type": event["event_type"],
                "source_type": todo["source_type"],
                "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
                "node_id": todo["node_id"], "recipient": todo["recipient"],
                "route_version": version, "failures": render_failures}, ts=now)
            return {"routed": True, "task_id": task_id, "plan": [],
                    "skipped_reason": "template_render_failed",
                    "render_failures": render_failures}
        # 没有任何可路由通道（无地址）：任务取消（区别于从未入队），全程留痕
        cur.execute(
            "UPDATE notif_send_tasks SET status='cancelled', plan_json='[]', "
            "cancelled_reason='no_routable_channel', updated_at=? WHERE id=?",
            (now, task_id))
        audit.record(cur, "notif_send_task_skipped", None, None, {
            "send_task_id": task_id, "todo_id": todo["id"],
            "notify_event_id": event["id"], "event_type": event["event_type"],
            "source_type": todo["source_type"],
            "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
            "node_id": todo["node_id"], "recipient": todo["recipient"],
            "route_version": version,
            "reason": "no_routable_channel"}, ts=now)
        return {"routed": True, "task_id": task_id, "plan": [],
                "skipped_reason": "no_routable_channel"}
    cur.execute(
        """UPDATE notif_send_tasks
           SET plan_json=?, content_snapshot=?, template_snapshot=?,
               render_status=?, updated_at=? WHERE id=?""",
        (json.dumps(plan, ensure_ascii=False, sort_keys=True),
         json.dumps(content_snapshot, ensure_ascii=False, sort_keys=True),
         json.dumps(template_meta, ensure_ascii=False, sort_keys=True),
         "rendered" if template_meta is not None else "not_templated",
         now, task_id))
    _record_switch_tx(cur, task_id, event["id"], todo["recipient"], None,
                      plan[0]["channel"], SW_SELECTED,
                      {"route_version": version,
                       "plan": [p["channel"] for p in plan],
                       "languages": {p["channel"]: p["language"] for p in plan}}, now)
    audit.record(cur, "notif_send_task_enqueued", None, None, {
        "send_task_id": task_id, "todo_id": todo["id"],
        "notify_event_id": event["id"], "event_type": event["event_type"],
        "source_type": todo["source_type"],
        "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
        "node_id": todo["node_id"], "recipient": todo["recipient"],
        "route_version": version,
        "plan": [p["channel"] for p in plan],
        "languages": {p["channel"]: p["language"] for p in plan},
        "templated_channels": [p["channel"] for p in plan
                               if p["template_version_id"] is not None],
        "render_failures": render_failures}, ts=now)
    return {"routed": True, "task_id": task_id,
            "plan": [p["channel"] for p in plan], "skipped_reason": None,
            "render_failures": render_failures}


# ============================================================================
# 取消（待办关闭/升级停止/联系人停用，在调用方事务内）
# ============================================================================

def cancel_task_tx(cur: sqlite3.Cursor, task_id: int, reason: str,
                   now: float) -> bool:
    """把一条未终结的发送任务（pending/in_flight/failed）置 cancelled。

    条件更新兜底：已 sent/quarantined/cancelled 的任务不受影响（已发出的外部效果
    无法撤回，轨迹保留）。返回是否发生了转移。
    """
    row = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                      (task_id,)).fetchone()
    if row is None or row["status"] in (TASK_SENT, TASK_QUARANTINED, TASK_CANCELLED):
        return False
    cur.execute(
        """UPDATE notif_send_tasks SET status='cancelled', cancelled_reason=?,
           next_retry_at=NULL, updated_at=? WHERE id=?
           AND status IN ('pending','in_flight','failed','awaiting_manual',
                          'awaiting_confirmation','render_failed')""",
        (reason, now, task_id))
    # 若它占用了某通道的恢复探针，释放探针归属（通道回到 open，下轮重新探针）
    cur.execute(
        """UPDATE notif_channel_state SET state='open', probe_task_id=NULL,
           probe_at=NULL, updated_at=? WHERE probe_task_id=?""",
        (now, task_id))
    # 回收该任务未使用的额度预占（已发送 consumed 的不回收；取消即确认不会再发送）
    from . import notif_quota
    notif_quota.release_for_task_tx(cur, row, reason, now)
    audit.record(cur, "notif_send_task_cancelled", None, None, {
        "send_task_id": task_id, "todo_id": row["todo_id"],
        "event_type": row["event_type"], "recipient": row["recipient"],
        "current_channel": row["current_channel"], "reason": reason}, ts=now)
    return True


def cancel_tasks_for_todo_tx(cur: sqlite3.Cursor, todo_id: int, reason: str,
                             now: float) -> int:
    """取消某待办全部未终结的发送任务（与旧链路取消 approval_notification_deliveries
    同口径，由待办处理/对账/停用/升级停止在同一事务内调用）。"""
    rows = cur.execute(
        "SELECT id FROM notif_send_tasks WHERE todo_id=? "
        "AND status IN ('pending','in_flight','failed','awaiting_manual',"
        "'awaiting_confirmation','render_failed')", (todo_id,)).fetchall()
    n = 0
    for r in rows:
        if cancel_task_tx(cur, r["id"], reason, now):
            n += 1
    return n


# ============================================================================
# 熔断
# ============================================================================

def _consecutive_failures(cur: sqlite3.Cursor, channel: str,
                          window_seconds: float, now: float,
                          round_no: int | None = None) -> int:
    """该通道在统计窗口内、自最近一次成功以来的连续失败数（超时计失败）。

    直接查 notif_send_attempts：找到窗口内最近一条成功，数其后到现在的失败条数；
    无成功则数窗口内全部失败。round_no 给定时只统计当前发送轮次（requeue 后重新计数）。
    """
    since = now - window_seconds
    sql = ("SELECT result, round FROM notif_send_attempts "
           "WHERE channel=? AND created_at>=?")
    params: list = [channel, since]
    if round_no is not None:
        sql += " AND round=?"
        params.append(round_no)
    sql += " ORDER BY id DESC"
    rows = cur.execute(sql, tuple(params)).fetchall()
    failures = 0
    for r in rows:
        if r["result"] == ATTEMPT_SUCCESS:
            break
        failures += 1
    return failures


def _advance_breaker_on_result(cur: sqlite3.Cursor, state: sqlite3.Row,
                               result: str, *, is_probe: bool, task_id: int,
                               now: float) -> str:
    """根据一次尝试结果推进熔断状态，返回推进后的状态。

    - success：closed（half_open 探针成功 -> closed，重新接流量）；
    - failure/timeout：half_open 探针失败立即 open；closed 下窗口内连续失败达到阈值
      转 open；已经 open 保持 open。
    """
    ch, new_state = state["channel"], state["state"]
    if result == ATTEMPT_SUCCESS:
        cur.execute(
            """UPDATE notif_channel_state SET state='closed', opened_at=NULL,
               last_failure_at=NULL, probe_task_id=NULL, probe_at=NULL,
               updated_at=? WHERE channel=?""", (now, ch))
        if new_state == BREAKER_HALF_OPEN:
            audit.record(cur, "notif_channel_recovered", None, None,
                         {"channel": ch, "probe_task_id": task_id}, ts=now)
        return BREAKER_CLOSED
    cur.execute("UPDATE notif_channel_state SET last_failure_at=?, updated_at=? "
                "WHERE channel=?", (now, now, ch))
    if new_state == BREAKER_HALF_OPEN:
        cur.execute(
            """UPDATE notif_channel_state SET state='open', opened_at=COALESCE(opened_at,?),
               probe_task_id=NULL, probe_at=NULL, updated_at=? WHERE channel=?""",
            (now, now, ch))
        audit.record(cur, "notif_channel_probe_failed", None, None,
                     {"channel": ch, "probe_task_id": task_id,
                      "result": result}, ts=now)
        return BREAKER_OPEN
    failures = _consecutive_failures(cur, ch, state["window_seconds"], now)
    if new_state == BREAKER_CLOSED and failures >= state["failure_threshold"]:
        cur.execute(
            """UPDATE notif_channel_state SET state='open', opened_at=?,
               probe_task_id=NULL, probe_at=NULL, updated_at=? WHERE channel=?""",
            (now, now, ch))
        audit.record(cur, "notif_channel_opened", None, None, {
            "channel": ch, "consecutive_failures": failures,
            "failure_threshold": state["failure_threshold"],
            "window_seconds": state["window_seconds"]}, ts=now)
        return BREAKER_OPEN
    return new_state


def _admit_for_dispatch(cur: sqlite3.Cursor, task: sqlite3.Row, ch: str,
                        now: float) -> tuple[bool, str | None, dict | None]:
    """派发闸门：该通道此刻能否接收这条任务。

    返回 (admitted, reason, state_row)。reason 为 None 表示可派发；否则是切换原因
    （breaker_open/channel_disabled）。open 通道冷却到期时在本事务转 half_open 并把
    探针占用给本任务（唯一探针，条件更新保证并发下单一赢家）。
    """
    state = cur.execute("SELECT * FROM notif_channel_state WHERE channel=?",
                        (ch,)).fetchone()
    if state is None:
        return True, None, None
    if not state["enabled"]:
        return False, SW_CHANNEL_DISABLED, dict(state)
    if ch == CHANNEL_INBOX:
        return True, None, dict(state)
    if state["state"] == BREAKER_CLOSED:
        return True, None, dict(state)
    if state["state"] == BREAKER_HALF_OPEN:
        # 探针只能有一个：归属别的任务则本任务跳过该通道
        if state["probe_task_id"] is not None and state["probe_task_id"] != task["id"]:
            return False, SW_BREAKER_OPEN, dict(state)
        return True, None, dict(state)
    # open：冷却到期 -> 尝试转 half_open 并占用探针
    if state["opened_at"] is not None and \
            now >= state["opened_at"] + state["cooldown_seconds"]:
        changed = cur.execute(
            """UPDATE notif_channel_state SET state='half_open', probe_task_id=?,
               probe_at=?, updated_at=? WHERE channel=? AND state='open'""",
            (task["id"], now, now, ch)).rowcount
        if changed:
            audit.record(cur, "notif_channel_probe_started", None, None,
                         {"channel": ch, "probe_task_id": task["id"]}, ts=now)
            return True, None, dict(state) | {"state": BREAKER_HALF_OPEN,
                                              "probe_task_id": task["id"]}
        return False, SW_BREAKER_OPEN, dict(state)
    return False, SW_BREAKER_OPEN, dict(state)


# ============================================================================
# 发送：尝试 / 故障转移
# ============================================================================

# 进程级线程池：同步 sender（SMTP/HTTP）的超时强杀兜底
_executor = ThreadPoolExecutor(max_workers=4, thread_name_prefix="notif-sender")

# 进程级默认配置：入队发生在 replay/delegation/gate 的业务事务内（那些调用点没有
# Settings 参数），由应用装配时 configure_routing(settings) 注入；熔断参数缺省取它。
_settings: Settings | None = None


def configure_routing(settings: Settings) -> None:
    global _settings
    _settings = settings


def _settings_at(cur: sqlite3.Cursor) -> Settings:
    """入队侧的配置：优先用进程注入的 Settings；未注入时用环境变量配置。"""
    if _settings is not None:
        return _settings
    from .config import Settings
    return Settings.from_env()


def _inbox_send(task: sqlite3.Row) -> None:
    """站内通道：待办已在生成时即时落盘，这里只确认（不产生第二个业务通知）。"""
    log.info("inbox todo=%s recipient=%s", task["todo_id"], task["recipient"])


def _call_sender(task: sqlite3.Row, plan_item: dict, channel: str,
                 senders: dict, timeout: float) -> tuple[str, str | None, float, str | None]:
    """在工作线程里调用 sender 并强制超时。

    返回 (result, error, duration, message_id)。sender 可在成功时返回外部服务给出的
    message_id（非空字符串）；返回 None/其他时由回执模块生成本地占位编号 local:...。
    """
    if channel == CHANNEL_INBOX:
        fn, args = _inbox_send, (task,)
    else:
        sender = senders.get(channel)
        if sender is None:
            return ATTEMPT_FAILURE, f"no sender configured for channel {channel!r}", 0.0, None
        if channel == CHANNEL_EMAIL:
            # 用入队时固化在计划项里的模板渲染正文（不重新渲染，模板后续编辑不影响本任务）
            fn, args = sender, (plan_item["address"],
                                plan_item.get("subject", task["subject"]),
                                plan_item.get("body", task["body"]))
        else:
            fn, args = sender, (plan_item["address"],
                               plan_item.get("webhook_payload") or {})
    start = time.monotonic()
    fut = _executor.submit(fn, *args)
    try:
        ret = fut.result(timeout=timeout)
    except FuturesTimeout:
        return ATTEMPT_TIMEOUT, f"channel {channel} timed out after {timeout}s", \
            time.monotonic() - start, None
    except Exception as exc:  # noqa: BLE001 - 任何外发异常都计入失败/熔断
        return ATTEMPT_FAILURE, str(exc), time.monotonic() - start, None
    message_id = ret.strip() if isinstance(ret, str) and ret.strip() else None
    return ATTEMPT_SUCCESS, None, time.monotonic() - start, message_id


def _record_attempt_tx(cur, *, task_id: int, channel: str, attempt_index: int,
                       round_no: int, probe: bool, result: str, duration: float,
                       error: str | None, now: float) -> int:
    cur.execute(
        """INSERT INTO notif_send_attempts
           (task_id, channel, attempt_index, round, probe, result, duration,
            error, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (task_id, channel, attempt_index, round_no, 1 if probe else 0,
         result, duration, error, now))
    return cur.lastrowid


def _record_switch_tx(cur, task_id: int | None, event_id: int, recipient: str,
                      from_ch: str | None, to_ch: str | None, reason: str,
                      detail: dict, now: float) -> int:
    cur.execute(
        """INSERT INTO notif_channel_switches
           (task_id, event_id, recipient, from_channel, to_channel, reason,
            detail, created_at)
           VALUES (?,?,?,?,?,?,?,?)""",
        (task_id, event_id, recipient, from_ch, to_ch, reason,
         json.dumps(detail or {}, ensure_ascii=False, sort_keys=True), now))
    return cur.lastrowid


def _mark_sent(cur, task, channel: str, now: float, plan_item: dict | None = None) -> None:
    # content_sha256 取入队时固化在计划项里的正文指纹（未配置模板时为 NULL）
    content_sha = (plan_item or {}).get("content_sha256")
    cur.execute(
        """UPDATE notif_send_tasks SET status='sent', sent_channel=?, sent_at=?,
           current_channel=?, content_sha256=COALESCE(?, content_sha256),
           next_retry_at=NULL, last_error=NULL, updated_at=?
           WHERE id=?""", (channel, now, channel, content_sha, now, task["id"]))


def _quarantine(cur, task, now: float, error: str) -> None:
    """计划全部通道耗尽（外发全失败且无 inbox 兜底）：隔离，等待人工 requeue。"""
    cur.execute(
        """UPDATE notif_send_tasks SET status='quarantined', quarantined_at=?,
           next_retry_at=NULL, last_error=?, updated_at=? WHERE id=?""",
        (now, error, now, task["id"]))
    _record_switch_tx(cur, task["id"], task["event_id"], task["recipient"],
                      task["current_channel"], None, SW_PLAN_EXHAUSTED,
                      {"error": error}, now)


def process_send_task(db: Database, task_id: int, senders: dict, settings: Settings,
                      now: float | None = None) -> str:
    """推进一条发送任务：从当前通道开始按计划尝试，超时/连续失败/熔断则切换下一通道，
    直到成功（sent）、本轮全计划耗尽（quarantined）或需要退避等待（pending）。

    返回任务最终状态。通道 IO 在写事务外执行；每一次尝试/切换/熔断转移都独立短事务
    落盘，因此崩溃/重启后从库内状态继续，不会重复发送成功过的通道。
    """
    now = time.time() if now is None else now
    with db.tx() as cur:
        # 条件领取：pending/failed -> in_flight 单赢家（in_flight 由崩溃 recover 回收，
        # 不允许第二个 worker 重复领取，否则同一任务会被并发执行两次通道 IO）。
        claimed = cur.execute(
            "UPDATE notif_send_tasks SET status='in_flight', updated_at=? "
            "WHERE id=? AND status IN ('pending','failed')",
            (now, task_id)).rowcount
        if not claimed:
            row = cur.execute("SELECT status FROM notif_send_tasks WHERE id=?",
                              (task_id,)).fetchone()
            return row["status"] if row else "missing"
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (task_id,)).fetchone()
        # 待办已关闭：未发出任务同事务取消（与旧链路一致）
        todo = cur.execute("SELECT status FROM approval_todos WHERE id=?",
                           (task["todo_id"],)).fetchone()
        if todo is None or todo["status"] not in notif.OPEN_TODO_STATUSES:
            cancel_task_tx(cur, task_id, "todo_closed", now)
            return TASK_CANCELLED
        # 额度闸门：领取前在同一事务原子预占。重复扫描/失败重试复用同代预占不重复占用；
        # 超额按入队时快照延迟（回 pending）/降级 inbox（改计划）/转人工。
        from . import notif_quota
        gate = notif_quota.admit_or_defer_tx(cur, task, now)
        if gate["decision"] == "delay":
            return TASK_PENDING
        if gate["decision"] == "manual":
            return TASK_AWAITING_MANUAL

    # 单轮内沿计划逐通道推进。每一步独立短事务：先过派发闸门（熔断/停用/探针占用），
    # 再在事务外做通道 IO，最后在新事务里落尝试结果、推进熔断与计划下标。
    while True:
        # ---- 步骤 1：读任务 + 派发闸门 -------------------------------------
        with db.tx() as cur:
            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (task_id,)).fetchone()
            if task["status"] in (TASK_SENT, TASK_QUARANTINED, TASK_CANCELLED,
                                  TASK_PENDING):
                return task["status"]
            plan = json.loads(task["plan_json"])
            idx = task["attempt_index"]
            if idx >= len(plan):
                return task["status"]
            item = plan[idx]
            ch = item["channel"]
            admitted, reason, state = _admit_for_dispatch(cur, task, ch, now)
            if not admitted:
                terminated = _skip_unavailable_channel(
                    cur, task, plan, idx, ch, reason, now)
                if terminated:
                    return task_status(cur, task_id)
                continue  # 已切到下一通道，同轮继续闸门检查
            is_probe = bool(state and state.get("state") == BREAKER_HALF_OPEN and
                            state.get("probe_task_id") == task_id)
            timeout = float(item.get("timeout_seconds")
                            or settings.notif_channel_timeout_seconds)
            # 本轮该通道已尝试次数（同通道重试只计当前轮次；requeue 开新一轮重新计数）
            used = cur.execute(
                "SELECT COUNT(*) AS c FROM notif_send_attempts "
                "WHERE task_id=? AND channel=? AND round=?",
                (task_id, ch, task["round"])).fetchone()["c"]
            attempt_no = task["total_attempts"] + 1
            cur.execute(
                "UPDATE notif_send_tasks SET current_channel=?, total_attempts=?, "
                "updated_at=? WHERE id=?", (ch, attempt_no, now, task_id))
            snapshot_item = dict(item)

        # ---- 步骤 2：通道 IO（事务外，超时强杀） ----------------------------
        result, error, duration, provider_message_id = _call_sender(
            task, snapshot_item, ch, senders, timeout)

        # ---- 步骤 3：落尝试结果 + 推进熔断 + 决定去留 -----------------------
        with db.tx() as cur:
            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (task_id,)).fetchone()
            attempt_id = _record_attempt_tx(cur, task_id=task_id, channel=ch,
                               attempt_index=idx, round_no=task["round"],
                               probe=is_probe, result=result, duration=duration,
                               error=error, now=now)
            state = cur.execute("SELECT * FROM notif_channel_state WHERE channel=?",
                                (ch,)).fetchone()
            _advance_breaker_on_result(cur, state, result, is_probe=is_probe,
                                       task_id=task_id, now=now)
            if result == ATTEMPT_SUCCESS:
                _mark_sent(cur, task, ch, now, item)
                # 预占转 consumed（含降级预占）：本事件的额度在窗口内真正用掉，
                # 后续重试/回执通道切换复用同一预占、不再重复占用。
                from . import notif_quota
                notif_quota.mark_consumed_tx(cur, task, now)
                # email/webhook：登记外部服务返回的 message_id（送达确认锚点）。
                # inbox 为站内兜底，不跟踪外部回执（receipt_status=not_required）。
                external_pk = None
                if ch in EXTERNAL_CHANNELS:
                    external_pk = receipts.register_external_message_tx(
                        cur, channel=ch, message_id=provider_message_id,
                        id_source="provider" if provider_message_id else "local",
                        source=receipts.SOURCE_ROUTE, send_task_id=task_id,
                        attempt_id=attempt_id, recipient=task["recipient"],
                        address=snapshot_item.get("address"),
                        event_id=task["event_id"], event_type=task["event_type"],
                        ts=now)
                audit.record(cur, "notif_send_sent", None, None, {
                    "send_task_id": task_id, "todo_id": task["todo_id"],
                    "notify_event_id": task["event_id"],
                    "event_type": task["event_type"], "recipient": task["recipient"],
                    "channel": ch, "attempt": attempt_no,
                    "route_version": task["route_version"],
                    "probe": is_probe, "duration": duration,
                    "external_message_pk": external_pk,
                    "message_id": provider_message_id}, ts=now)
                return TASK_SENT
            cur.execute(
                "UPDATE notif_send_tasks SET last_error=?, updated_at=? WHERE id=?",
                (error, now, task_id))
            plan = json.loads(task["plan_json"])
            max_attempts = int(snapshot_item.get("max_attempts")
                               or settings.notif_max_attempts)
            used_after = used + 1
            # 恢复探针失败：通道已回 open，无下一通道可切时等待下一次探针（不能隔离，
            # 否则熔断恢复后该通知永远丢失）；有下一通道则按熔断原因切换。
            if is_probe:
                if idx + 1 >= len(plan):
                    cooldown = float(state["cooldown_seconds"])
                    cur.execute(
                        """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
                           current_channel=NULL, next_retry_at=?, updated_at=? WHERE id=?""",
                        (now + cooldown, now, task_id))
                    _record_switch_tx(cur, task_id, task["event_id"],
                                      task["recipient"], ch, None, SW_PLAN_EXHAUSTED,
                                      {"reason": SW_BREAKER_OPEN,
                                       "probe_failed": True,
                                       "awaiting_recovery": True,
                                       "next_retry_at": now + cooldown}, now)
                    return TASK_PENDING
                switch_reason = SW_BREAKER_OPEN
            else:
                switch_reason = None
                if result == ATTEMPT_TIMEOUT:
                    switch_reason = SW_TIMEOUT           # 超时立即切换，不重试本通道
                elif used_after >= max_attempts:
                    switch_reason = SW_CONSECUTIVE_FAILURES  # 连续失败达上限才切换
            if switch_reason is not None:
                nxt = _advance_to_next_channel(
                    cur, task, plan, idx, ch, switch_reason, now,
                    detail={"attempts": used_after, "max_attempts": max_attempts,
                            "result": result, "error": error})
                if nxt is None:
                    # 已隔离（终结）或转入等待熔断恢复（pending）
                    return task_status(cur, task_id)
                continue  # 切到下一通道，同轮继续
            # 未达切换条件：留在本通道，按指数退避排下次重试
            delay = min(settings.notif_retry_base_seconds * (2 ** (used_after - 1)),
                        settings.notif_retry_cap_seconds)
            cur.execute(
                """UPDATE notif_send_tasks SET status='pending', next_retry_at=?,
                   updated_at=? WHERE id=?""", (now + delay, now, task_id))
            audit.record(cur, "notif_send_retry_scheduled", None, None, {
                "send_task_id": task_id, "channel": ch,
                "attempts": used_after, "max_attempts": max_attempts,
                "next_retry_at": now + delay, "delay_seconds": delay,
                "result": result, "error": error}, ts=now)
            return TASK_PENDING


def task_status(cur: sqlite3.Cursor, task_id: int) -> str:
    return cur.execute("SELECT status FROM notif_send_tasks WHERE id=?",
                       (task_id,)).fetchone()["status"]


def _skip_unavailable_channel(cur, task, plan, idx, ch, reason, now) -> bool:
    """当前通道不可派发（熔断/停用/探针被占）：切到下一通道。

    返回 True 表示任务已终结/转入等待（quarantined 语义之外的 pending 也算终止本轮）；
    False 表示已推进到下一通道，调用方继续同轮循环。
    """
    next_idx = idx + 1
    if next_idx >= len(plan):
        # 计划通道此刻都不可用：等待最近一个 open 通道冷却后重新从首选通道尝试
        wait = _earliest_cooldown(cur, now)
        retry_at = wait or now + 1.0
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
               current_channel=NULL, next_retry_at=?, updated_at=? WHERE id=?""",
            (retry_at, now, task["id"]))
        _record_switch_tx(cur, task["id"], task["event_id"], task["recipient"],
                          ch, None, SW_PLAN_EXHAUSTED,
                          {"reason": reason, "awaiting_recovery": True,
                           "next_retry_at": retry_at}, now)
        audit.record(cur, "notif_send_awaiting_channels", None, None, {
            "send_task_id": task["id"], "channel": ch, "reason": reason,
            "next_retry_at": retry_at}, ts=now)
        return True
    nxt = plan[next_idx]["channel"]
    cur.execute(
        "UPDATE notif_send_tasks SET attempt_index=?, current_channel=NULL, "
        "updated_at=? WHERE id=?", (next_idx, now, task["id"]))
    _record_switch_tx(cur, task["id"], task["event_id"], task["recipient"],
                      ch, nxt, reason, {"attempt_index": next_idx}, now)
    audit.record(cur, "notif_channel_switched", None, None, {
        "send_task_id": task["id"], "from_channel": ch, "to_channel": nxt,
        "reason": reason, "attempt_index": next_idx}, ts=now)
    return False


def _advance_to_next_channel(cur, task, plan, idx, ch, reason, now,
                             detail: dict) -> str | None:
    """当前通道耗尽（超时/连续失败/探针失败）：推进到下一通道。

    返回下一通道名；已无下一通道时：
    - 该通道已熔断（open）-> 任务等待冷却后的恢复探针（attempt_index 回 0，
      next_retry_at=冷却到期），熔断恢复后自动重发，不进隔离；
    - 否则（未熔断且计划无 inbox 兜底）-> quarantined，等待人工 requeue。
    返回 None 表示任务已终结/转入等待（调用方不再继续本通道）。
    """
    next_idx = idx + 1
    if next_idx >= len(plan):
        state = cur.execute("SELECT * FROM notif_channel_state WHERE channel=?",
                            (ch,)).fetchone()
        if state is not None and state["state"] == BREAKER_OPEN and \
                state["opened_at"] is not None:
            recover_at = state["opened_at"] + state["cooldown_seconds"]
            cur.execute(
                """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
                   current_channel=NULL, next_retry_at=?, updated_at=? WHERE id=?""",
                (recover_at, now, task["id"]))
            _record_switch_tx(cur, task["id"], task["event_id"], task["recipient"],
                              ch, None, SW_PLAN_EXHAUSTED,
                              {"reason": reason, "awaiting_recovery": True,
                               "next_retry_at": recover_at, **detail}, now)
            audit.record(cur, "notif_send_awaiting_channels", None, None, {
                "send_task_id": task["id"], "channel": ch, "reason": reason,
                "next_retry_at": recover_at}, ts=now)
            return None
        _quarantine(cur, task, now, detail.get("error") or f"channel {ch} exhausted")
        audit.record(cur, "notif_send_quarantined", None, None, {
            "send_task_id": task["id"], "recipient": task["recipient"],
            "event_type": task["event_type"], "last_channel": ch,
            "reason": reason, **detail}, ts=now)
        return None
    nxt = plan[next_idx]["channel"]
    cur.execute(
        "UPDATE notif_send_tasks SET attempt_index=?, current_channel=NULL, "
        "updated_at=? WHERE id=?", (next_idx, now, task["id"]))
    _record_switch_tx(cur, task["id"], task["event_id"], task["recipient"],
                      ch, nxt, reason, {"attempt_index": next_idx, **detail}, now)
    audit.record(cur, "notif_channel_switched", None, None, {
        "send_task_id": task["id"], "from_channel": ch, "to_channel": nxt,
        "reason": reason, "attempt_index": next_idx, **detail}, ts=now)
    return nxt


def _earliest_cooldown(cur, now: float) -> float | None:
    row = cur.execute(
        """SELECT MIN(opened_at + cooldown_seconds) AS t FROM notif_channel_state
           WHERE state='open' AND enabled=1 AND opened_at IS NOT NULL""").fetchone()
    value = row["t"] if row else None
    return value if value is not None and value > now else None


# ============================================================================
# worker 入口
# ============================================================================

def dispatch_due_tasks(db: Database, senders: dict, settings: Settings,
                       now: float | None = None, limit: int = 100) -> int:
    """派发所有到期的发送任务（按 ordinal 原事件顺序）。"""
    now = time.time() if now is None else now
    rows = db.query(
        """SELECT id FROM notif_send_tasks
           WHERE status IN ('pending','failed')
             AND (next_retry_at IS NULL OR next_retry_at <= ?)
           ORDER BY ordinal, id LIMIT ?""", (now, limit))
    for row in rows:
        process_send_task(db, row["id"], senders, settings, now)
    return len(rows)


def recover_in_flight_tasks(db: Database, now: float | None = None) -> int:
    """启动恢复：崩溃时卡在 in_flight 的任务退回 pending（尝试历史都在库里，
    熔断窗口统计与通道快照保证不会对已成功通道重发）。"""
    now = time.time() if now is None else now
    with db.tx() as cur:
        rows = cur.execute(
            "SELECT id, current_channel FROM notif_send_tasks WHERE status='in_flight'"
        ).fetchall()
        for r in rows:
            cur.execute(
                "UPDATE notif_send_tasks SET status='pending', updated_at=? WHERE id=?",
                (now, r["id"]))
            # 释放可能占用的恢复探针（通道回 open，下轮重新探针）
            cur.execute(
                """UPDATE notif_channel_state SET state='open', probe_task_id=NULL,
                   probe_at=NULL, opened_at=COALESCE(opened_at,?), updated_at=?
                   WHERE probe_task_id=?""", (now, now, r["id"]))
            audit.record(cur, "notif_send_task_recovered", None, None,
                         {"send_task_id": r["id"],
                          "channel": r["current_channel"]}, ts=now)
    # 额度对账：退回 pending 的任务保留其预占（下轮领取复用）；防御性回收已终结任务的
    # 孤儿预占。in_flight 退回 pending 本身不重复占用（预占行按 generation 复用）。
    from . import notif_quota
    with db.tx() as cur:
        notif_quota.reconcile_on_recover_tx(cur, now)
    return len(rows)


def requeue_task(db: Database, task_id: int, operator: str,
                 settings: Settings) -> dict:
    """人工把隔离的发送任务重新派发：从计划首个通道开新一轮（round+1，重新计数）。"""
    operator = notif._require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                          (task_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "notification send task not found")
        if row["status"] != TASK_QUARANTINED:
            raise HTTPException(409, f"send task is {row['status']}, not quarantined")
        from . import notif_quota
        new_gen = notif_quota.bump_generation_tx(
            cur, row, "manual_requeue", now)
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
               round=round+1, next_retry_at=NULL, last_error=NULL,
               current_channel=NULL, quarantined_at=NULL, updated_at=?
               WHERE id=?""", (now, task_id))
        audit.record(cur, "notif_send_task_requeued", None, None, {
            "send_task_id": task_id, "operator": operator,
            "recipient": row["recipient"], "event_type": row["event_type"],
            "new_round": row["round"] + 1,
            "new_quota_generation": new_gen}, ts=now)
    return {"result": "requeued", "task_id": task_id, "round": row["round"] + 1,
            "quota_generation": new_gen}


# ============================================================================
# 查询 / 管理
# ============================================================================

def get_current_routing(db: Database) -> dict:
    with db.tx() as cur:
        pointer = cur.execute(
            "SELECT * FROM notif_route_current WHERE id=1").fetchone()
        version = pointer["route_version"] if pointer else None
        out = {"current_version": version, "updated_by": None,
               "updated_at": None, "reason": None, "config": None,
               "rollback_available": False}
        if pointer is not None:
            out.update(updated_by=pointer["updated_by"],
                       updated_at=pointer["updated_at"], reason=pointer["reason"])
        if version is not None:
            out["config"] = _load_version(cur, version)
            prev = cur.execute(
                """SELECT version FROM notif_route_versions
                   WHERE result IN ('applied','rollback') AND version < ?
                   ORDER BY id DESC LIMIT 1""", (version,)).fetchone()
            out["rollback_available"] = prev is not None
        return out


def list_route_versions(db: Database, limit: int = 100) -> dict:
    rows = db.query(
        "SELECT * FROM notif_route_versions ORDER BY id DESC LIMIT ?", (limit,))
    return {"versions": [{
        "id": r["id"], "version": r["version"], "result": r["result"],
        "operator": r["operator"], "reason": r["reason"],
        "created_at": r["created_at"],
        # 配置仅在需要时可通过详情查看；列表不回显大 JSON
        "has_config": r["config_json"] is not None} for r in rows]}


def _attempts_view(db: Database, task_id: int) -> list[dict]:
    return [{"id": r["id"], "channel": r["channel"],
             "attempt_index": r["attempt_index"], "round": r["round"],
             "probe": bool(r["probe"]), "result": r["result"],
             "duration": r["duration"], "error": r["error"],
             "created_at": r["created_at"]}
            for r in db.query(
                "SELECT * FROM notif_send_attempts WHERE task_id=? ORDER BY id",
                (task_id,))]


def _switches_view(db: Database, task_id: int) -> list[dict]:
    return [{"id": r["id"], "from_channel": r["from_channel"],
             "to_channel": r["to_channel"], "reason": r["reason"],
             "detail": json.loads(r["detail"]), "created_at": r["created_at"]}
            for r in db.query(
                "SELECT * FROM notif_channel_switches WHERE task_id=? ORDER BY id",
                (task_id,))]


def _task_view(db: Database, row: sqlite3.Row, *, with_history: bool = False) -> dict:
    item = {
        "id": row["id"], "todo_id": row["todo_id"], "event_id": row["event_id"],
        "recipient": row["recipient"], "event_type": row["event_type"],
        "source_type": row["source_type"], "batch_id": row["batch_id"],
        "change_id": row["change_id"], "node_id": row["node_id"],
        "status": row["status"], "route_version": row["route_version"],
        "plan": [p["channel"] for p in json.loads(row["plan_json"])],
        "attempt_index": row["attempt_index"],
        "total_attempts": row["total_attempts"],
        "current_channel": row["current_channel"],
        "sent_channel": row["sent_channel"], "sent_at": row["sent_at"],
        "next_retry_at": row["next_retry_at"], "round": row["round"],
        "last_error": row["last_error"], "cancelled_reason": row["cancelled_reason"],
        "receipt_status": row["receipt_status"],
        "receipt_reason": row["receipt_reason"],
        "receipt_retries": row["receipt_retries"],
        "external_message_id": row["external_message_id"],
        "quota_version": row["quota_version"],
        "quota_rule_id": row["quota_rule_id"],
        "quota_level": row["quota_level"],
        "quota_status": row["quota_status"],
        "quota_reason": row["quota_reason"],
        "quota_generation": row["quota_generation"],
        "quota_snapshot": None,
        "render_status": row["render_status"],
        "render_failure_reason": (json.loads(row["render_failure_reason"])
                                  if row["render_failure_reason"] else None),
        "template_snapshot": None,
        "content_snapshot": None,
        "content_sha256": row["content_sha256"],
        "created_at": row["created_at"], "updated_at": row["updated_at"]}
    if with_history:
        item["attempts"] = _attempts_view(db, row["id"])
        item["switches"] = _switches_view(db, row["id"])
        item["route_snapshot"] = json.loads(row["route_snapshot"])
        if row["quota_snapshot"]:
            item["quota_snapshot"] = json.loads(row["quota_snapshot"])
        if row["template_snapshot"]:
            item["template_snapshot"] = json.loads(row["template_snapshot"])
        if row["content_snapshot"]:
            item["content_snapshot"] = json.loads(row["content_snapshot"])
    return item


def list_send_tasks(db: Database, *, status_filter: str | None = None,
                    recipient: str | None = None, channel: str | None = None,
                    event_type: str | None = None, batch_id: int | None = None,
                    route_version: int | None = None, limit: int = 100,
                    with_history: bool = False) -> dict:
    sql, params = "SELECT * FROM notif_send_tasks WHERE 1=1", []
    if status_filter:
        sql += " AND status=?"
        params.append(status_filter)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if event_type:
        sql += " AND event_type=?"
        params.append(event_type)
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if route_version is not None:
        sql += " AND route_version=?"
        params.append(route_version)
    if channel:
        # 当前/计划/已发送通道命中均可查
        sql += (" AND (current_channel=? OR sent_channel=? OR "
                "plan_json LIKE ?)")
        params.extend((channel, channel, f'%"{channel}"%'))
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"tasks": [_task_view(db, r, with_history=with_history) for r in rows],
            "count": len(rows)}


def get_send_task(db: Database, task_id: int) -> dict:
    row = db.query_one("SELECT * FROM notif_send_tasks WHERE id=?", (task_id,))
    if row is None:
        raise HTTPException(404, "notification send task not found")
    return {"task": _task_view(db, row, with_history=True)}


def channel_health(db: Database, settings: Settings | None = None,
                   now: float | None = None) -> dict:
    """通道健康：熔断状态、窗口统计（连续失败/成功）、探针归属与配置。"""
    now = time.time() if now is None else now
    with db.tx() as cur:
        if settings is not None:
            _ensure_channel_states(cur, settings, now)
        rows = cur.execute(
            "SELECT * FROM notif_channel_state ORDER BY channel").fetchall()
        out = []
        for r in rows:
            failures = _consecutive_failures(
                cur, r["channel"], r["window_seconds"], now)
            recover_at = (r["opened_at"] + r["cooldown_seconds"]
                          if r["state"] == BREAKER_OPEN and r["opened_at"]
                          else None)
            out.append({
                "channel": r["channel"], "state": r["state"],
                "enabled": bool(r["enabled"]),
                "failure_threshold": r["failure_threshold"],
                "window_seconds": r["window_seconds"],
                "cooldown_seconds": r["cooldown_seconds"],
                "opened_at": r["opened_at"], "recover_at": recover_at,
                "last_failure_at": r["last_failure_at"],
                "probe_task_id": r["probe_task_id"], "probe_at": r["probe_at"],
                "window_consecutive_failures": failures,
                "will_open_at_threshold": failures >= r["failure_threshold"]
                and r["state"] == BREAKER_CLOSED})
        return {"channels": out, "now": now}


def set_channel_state(db: Database, channel: str, req: ChannelStateRequest,
                      settings: Settings) -> dict:
    """运营手工启用/停用通道、调整熔断参数，或强制复位熔断（人工排障后恢复）。

    停用期间该通道不接收任何新派发（已在该通道重试等待中的任务下一轮切换下一通道）。
    """
    if channel not in ROUTABLE_CHANNELS:
        raise HTTPException(422, f"unsupported channel {channel!r}")
    operator = notif._require(req.operator, "operator")
    now = time.time()
    with db.tx() as cur:
        _ensure_channel_states(cur, settings, now)
        row = cur.execute("SELECT * FROM notif_channel_state WHERE channel=?",
                          (channel,)).fetchone()
        if row is None:
            raise HTTPException(404, "channel state not found")
        if channel == CHANNEL_INBOX and not req.enabled:
            raise HTTPException(422, "the inbox channel cannot be disabled")
        if req.failure_threshold is not None:
            if req.failure_threshold < 1:
                raise HTTPException(422, "failure_threshold must be >= 1")
            cur.execute("UPDATE notif_channel_state SET failure_threshold=? WHERE channel=?",
                        (req.failure_threshold, channel))
        if req.window_seconds is not None:
            if req.window_seconds <= 0:
                raise HTTPException(422, "window_seconds must be > 0")
            cur.execute("UPDATE notif_channel_state SET window_seconds=? WHERE channel=?",
                        (req.window_seconds, channel))
        if req.cooldown_seconds is not None:
            if req.cooldown_seconds < 0:
                raise HTTPException(422, "cooldown_seconds must be >= 0")
            cur.execute("UPDATE notif_channel_state SET cooldown_seconds=? WHERE channel=?",
                        (req.cooldown_seconds, channel))
        cur.execute("UPDATE notif_channel_state SET enabled=?, updated_at=? WHERE channel=?",
                    (1 if req.enabled else 0, now, channel))
        reset = False
        if req.reset and row["state"] != BREAKER_CLOSED:
            cur.execute(
                """UPDATE notif_channel_state SET state='closed', opened_at=NULL,
                   last_failure_at=NULL, probe_task_id=NULL, probe_at=NULL,
                   updated_at=? WHERE channel=?""", (now, channel))
            reset = True
        audit.record(cur, "notif_channel_state_set", None, None, {
            "channel": channel, "operator": operator, "enabled": req.enabled,
            "failure_threshold": req.failure_threshold,
            "window_seconds": req.window_seconds,
            "cooldown_seconds": req.cooldown_seconds, "reset": reset}, ts=now)
    return {"result": "updated", "channel": channel, "enabled": req.enabled,
            "reset": reset}


def list_switches(db: Database, *, task_id: int | None = None,
                  recipient: str | None = None, channel: str | None = None,
                  reason: str | None = None, limit: int = 100) -> dict:
    sql, params = "SELECT * FROM notif_channel_switches WHERE 1=1", []
    if task_id is not None:
        sql += " AND task_id=?"
        params.append(task_id)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if channel:
        sql += " AND (to_channel=? OR from_channel=?)"
        params.extend((channel, channel))
    if reason:
        sql += " AND reason=?"
        params.append(reason)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"switches": [{"id": r["id"], "task_id": r["task_id"],
                          "event_id": r["event_id"], "recipient": r["recipient"],
                          "from_channel": r["from_channel"],
                          "to_channel": r["to_channel"], "reason": r["reason"],
                          "detail": json.loads(r["detail"]),
                          "created_at": r["created_at"]} for r in rows]}


def list_attempts(db: Database, *, task_id: int | None = None,
                  channel: str | None = None, result: str | None = None,
                  limit: int = 100) -> dict:
    sql, params = "SELECT * FROM notif_send_attempts WHERE 1=1", []
    if task_id is not None:
        sql += " AND task_id=?"
        params.append(task_id)
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    if result:
        sql += " AND result=?"
        params.append(result)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"attempts": [{"id": r["id"], "task_id": r["task_id"],
                          "channel": r["channel"], "attempt_index": r["attempt_index"],
                          "round": r["round"], "probe": bool(r["probe"]),
                          "result": r["result"], "duration": r["duration"],
                          "error": r["error"], "created_at": r["created_at"]}
                         for r in rows]}


# ============================================================================
# 路由
# ============================================================================

def create_routing_router(db: Database, settings: Settings,
                          senders_provider) -> APIRouter:
    router = APIRouter(prefix="/admin/approval-notifications/routing",
                       tags=["approval-notification-routing"])

    def senders():
        return senders_provider() if callable(senders_provider) else senders_provider

    @router.get("/current")
    def current():
        """当前生效路由版本、配置快照与是否可回滚。"""
        return get_current_routing(db)

    @router.post("/versions")
    def publish(req: RoutingConfigRequest):
        """发布新路由版本（校验通过整份生效；失败保留当前版本并落 rejected 记录）。"""
        return publish_routing(db, req, settings)

    @router.get("/versions")
    def versions(limit: int = Query(100, le=1000)):
        """路由版本历史（含 applied/rollback/rejected）。"""
        return list_route_versions(db, limit)

    @router.post("/rollback")
    def rollback(req: RollbackRequest):
        """回滚到上一生效版本（原因必填）；只影响之后入队的任务。"""
        return rollback_routing(db, req)

    @router.get("/tasks")
    def tasks(status: str | None = None, recipient: str | None = None,
              channel: str | None = None, event_type: str | None = None,
              batch_id: int | None = None, route_version: int | None = None,
              history: bool = False, limit: int = Query(100, le=1000)):
        """待发送/在途/已发送/隔离/取消的发送任务查询（可带尝试与切换历史）。"""
        return list_send_tasks(
            db, status_filter=status, recipient=recipient, channel=channel,
            event_type=event_type, batch_id=batch_id, route_version=route_version,
            limit=limit, with_history=history)

    @router.get("/tasks/{task_id}")
    def task_detail(task_id: int):
        return get_send_task(db, task_id)

    @router.post("/tasks/{task_id}/requeue")
    def task_requeue(task_id: int, req: OperatorRequest):
        """把隔离的发送任务从计划首个通道开新一轮重发。"""
        result = requeue_task(db, task_id, req.operator, settings)
        dispatch_due_tasks(db, senders(), settings)
        return result

    @router.get("/channels/health")
    def channels_health():
        """通道健康状态：closed/open/half_open、窗口连续失败、冷却与探针。"""
        return channel_health(db, settings)

    @router.post("/channels/{channel}/state")
    def set_channel(channel: str, req: ChannelStateRequest):
        """启用/停用通道、调整熔断参数或强制复位熔断。"""
        return set_channel_state(db, channel, req, settings)

    @router.get("/switches")
    def switches(task_id: int | None = None, recipient: str | None = None,
                 channel: str | None = None, reason: str | None = None,
                 limit: int = Query(100, le=1000)):
        """通道切换历史（选中/超时/连续失败/熔断/停用/耗尽）。"""
        return list_switches(db, task_id=task_id, recipient=recipient,
                             channel=channel, reason=reason, limit=limit)

    @router.get("/attempts")
    def attempts(task_id: int | None = None, channel: str | None = None,
                 result: str | None = None, limit: int = Query(100, le=1000)):
        """每次通道尝试记录（成功/失败/超时、耗时、探针标记）。"""
        return list_attempts(db, task_id=task_id, channel=channel,
                             result=result, limit=limit)

    return router

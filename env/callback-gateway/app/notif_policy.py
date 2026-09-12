"""审批通知的聚合、静默时段与升级策略（通知策略层）。

本模块只负责「外发投递（邮件/webhook）何时、以什么形态发出」与「升级提醒发给谁」，
不改变既有审批/待办主链路：

聚合（aggregation）
- 运营按接收人、来源批次与事件类型配置聚合窗口（approval_aggregation_rules）；
- 命中规则的投递在窗口内落 held 并入 approval_notification_groups；窗口关闭时同组
  每个通道合并为一条摘要投递（digest），成员行置 aggregated 保留可查；
- 站内待办始终即时逐条生成——聚合只合并外发，绝不吞掉可操作待办；
- 窗口内来源全部落定（成员待办全部关闭）的组直接取消，不再外发摘要。

静默时段（quiet hours）
- 按接收人/通道配置每日重复（UTC）或一次性时间窗（approval_quiet_schedules）；
- 窗内只保留站内待办：邮件/webhook 落 delayed（delayed_until=预计放行时间）；
- 时段结束（或配置被删除/暂停导致放行提前）由 worker 按 (ordinal,id) 放回 pending，
  ordinal=待办创建顺序，保证「按原事件顺序发送」。

升级（escalation）
- 按节点角色配置逐级升级接收人与触发时限（approval_escalation_policies）：
  after_seconds=待办生成后 N 秒，或 before_deadline_seconds=截止前 N 秒；
- 每条原始可操作待办在每个命中策略的每一级至多触发一次（唯一索引 + 条件状态转移，
  重复扫描/重启/并发不重发），升级级别逐行落 approval_escalation_levels；
- 升级通知是新的可操作待办（接收人仍须凭原节点角色/委托回写审批，不绕过任何门禁）；
- 原接收人处理待办（act_on_todo）或来源落定（sweep/惰性对账/停用联系人）时
  stop_escalations_for_todo_tx 同事务停止：后续级别不再触发、已升级别未发出投递取消。

并发/可靠性
- 所有状态转移在 SQLite 写事务（BEGIN IMMEDIATE，全局串行）内以条件 UPDATE/
  唯一索引完成；通道 IO 在事务外。worker 任意重复运行、进程重启都不会重复发送或
  跳过关键审批提醒。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time
from datetime import datetime, timezone

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit, notifications as notif
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications.policy")

# ---- 常量 -------------------------------------------------------------------

RULE_MATCH_ALL = "*"      # 接收人/事件类型的通配
CHANGE_ROLE = "change"    # 升级策略中匹配策略变更单待办的伪角色


# ---- 请求模型 ----------------------------------------------------------------

class AggregationRuleRequest(BaseModel):
    operator: str
    recipient: str | None = None          # 缺省=全体接收人
    batch_id: int | None = None           # 缺省=任意批次（change 来源不按批次匹配）
    event_type: str | None = None         # 缺省=全部事件类型
    window_seconds: float
    active: bool = True


class QuietScheduleRequest(BaseModel):
    operator: str
    recipient: str | None = None          # 缺省=全体
    channel: str | None = None            # 缺省=email+webhook；否则 email|webhook
    daily: bool = True                    # True=每日重复（UTC）；False=一次性
    start_time: str | None = None         # daily=True："HH:MM"（UTC）
    end_time: str | None = None
    start_at: float | None = None         # daily=False：epoch 秒（或 ISO-8601 字符串）
    end_at: float | None = None
    note: str = ""


class EscalationLevelSpec(BaseModel):
    recipients: list[str]
    after_seconds: float | None = None
    before_deadline_seconds: float | None = None


class EscalationPolicyRequest(BaseModel):
    operator: str
    name: str
    roles: list[str]                      # 节点角色（'any' 通配；'change' 匹配变更单）
    levels: list[EscalationLevelSpec]
    active: bool = True


class ActiveToggleRequest(BaseModel):
    operator: str
    active: bool = True


# ---- 小工具 ------------------------------------------------------------------

def _parse_hhmm(value: str | None) -> str:
    if not value or len(value) != 5 or value[2] != ":":
        raise HTTPException(422, f"invalid time {value!r}: expect HH:MM (UTC, 24h)")
    hh, mm = value[:2], value[3:]
    if not (hh.isdigit() and mm.isdigit()):
        raise HTTPException(422, f"invalid time {value!r}: expect HH:MM (UTC, 24h)")
    h, m = int(hh), int(mm)
    if not (0 <= h <= 23 and 0 <= m <= 59):
        raise HTTPException(422, f"invalid time {value!r}: hour 0-23, minute 0-59")
    return f"{h:02d}:{m:02d}"


def _epoch(value: float | str | None, field: str) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    if not text:
        return None
    try:
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            raise HTTPException(
                422, f"invalid {field} {value!r}: ISO-8601 must include a timezone "
                     "(e.g. 2026-09-12T22:00:00Z)")
        return dt.timestamp()
    except HTTPException:
        raise
    except ValueError:
        raise HTTPException(422, f"invalid {field} {value!r}: epoch seconds or ISO-8601")


def _rule_matches(rule_value, actual) -> bool:
    """规则维度匹配：NULL/空/'*' 为通配；否则相等。"""
    if rule_value is None or rule_value == "" or rule_value == RULE_MATCH_ALL:
        return True
    return rule_value == actual


# ============================================================================
# 聚合
# ============================================================================

def find_aggregation_group(cur: sqlite3.Cursor, *, todo, recipient: str,
                           channel: str, now: float) -> sqlite3.Row | None:
    """为一条待生成的外发投递匹配聚合规则；命中则找/开一个聚合组（同事务）。

    组键 = 规则 + 接收人 + 通道 + 具体来源（批次/变更单）。不指定批次的规则也按
    各自来源分组，避免把不同审批的提醒合并进同一条摘要。
    已到关闭时间但尚未被 worker flush 的 open 组不再接收新成员（新事件独立发送，
    下个窗口重新开组），保证窗口边界确定、不漏关键提醒。
    """
    rules = cur.execute(
        "SELECT * FROM approval_aggregation_rules WHERE active=1 "
        "ORDER BY id").fetchall()
    matched = None
    for rule in rules:
        if not _rule_matches(rule["recipient"], recipient):
            continue
        if not _rule_matches(rule["event_type"], todo["event_type"]):
            continue
        if rule["batch_id"] is not None:
            # 指定批次：仅同类型且同批次的事件匹配
            if todo["source_type"] != notif.SOURCE_BATCH or todo["batch_id"] is None \
                    or todo["batch_id"] != rule["batch_id"]:
                continue
        matched = rule
        break
    if matched is None:
        return None
    window = float(matched["window_seconds"])
    if window <= 0:
        return None
    g = cur.execute(
        """SELECT * FROM approval_notification_groups
           WHERE rule_id=? AND recipient=? AND channel=? AND source_type=?
             AND COALESCE(batch_id,-1)=COALESCE(?,-1)
             AND COALESCE(change_id,-1)=COALESCE(?,-1)
           ORDER BY id DESC LIMIT 1""",
        (matched["id"], recipient, channel, todo["source_type"],
         todo["batch_id"], todo["change_id"])).fetchone()
    if g is not None and g["status"] == "open" and g["window_closes_at"] > now:
        return g
    # 无开放组（首事件/已关闭/已取消）-> 以当前事件开窗
    cur.execute(
        """INSERT INTO approval_notification_groups
           (rule_id, recipient, channel, source_type, batch_id, change_id,
            window_seconds, status, window_opened_at, window_closes_at,
            created_at, updated_at)
           VALUES (?,?,?,?,?,?,?, 'open', ?, ?, ?, ?)""",
        (matched["id"], recipient, channel, todo["source_type"],
         todo["batch_id"], todo["change_id"], window, now, now + window, now, now))
    return cur.execute("SELECT * FROM approval_notification_groups WHERE id=?",
                       (cur.lastrowid,)).fetchone()


def audit_held(cur, *, delivery_id: int, todo, recipient: str, channel: str,
               group: sqlite3.Row, now: float) -> None:
    audit.record(cur, "approval_delivery_held", None, None, {
        "delivery_id": delivery_id, "todo_id": todo["id"],
        "recipient": recipient, "channel": channel,
        "event_type": todo["event_type"],
        "source_type": todo["source_type"],
        "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
        "node_id": todo["node_id"],
        "aggregation_group_id": group["id"], "rule_id": group["rule_id"],
        "window_closes_at": group["window_closes_at"]}, ts=now)


def flush_due_groups(db: Database, now: float | None = None) -> int:
    """关闭到期聚合组：

    - 成员的待办全部已关闭 -> 组 cancelled（摘要不再需要，held 投递一并取消）；
    - 否则每通道生成一条摘要投递（pending；若恰在静默时段则 delayed），成员行置
      aggregated。已 flush/cancel 的组条件更新挡下，重复扫描/重启不重发。
    """
    now = time.time() if now is None else now
    groups = db.query(
        "SELECT * FROM approval_notification_groups "
        "WHERE status='open' AND window_closes_at<=? ORDER BY id", (now,))
    flushed = 0
    for g in groups:
        with db.tx() as cur:
            locked = cur.execute(
                "SELECT * FROM approval_notification_groups WHERE id=?",
                (g["id"],)).fetchone()
            if locked["status"] != "open" or locked["window_closes_at"] > now:
                continue
            members = cur.execute(
                """SELECT m.*, d.address AS address, d.subject AS subject,
                          d.body AS body, d.payload AS payload,
                          t.status AS todo_status, t.id AS todo_id,
                          e.subject AS event_subject, e.payload AS event_payload
                   FROM approval_notification_group_members m
                   JOIN approval_notification_deliveries d ON d.id=m.delivery_id
                   JOIN approval_todos t ON t.id=m.todo_id
                   JOIN approval_notify_events e ON e.id=m.event_id
                   WHERE m.group_id=? AND d.status='held'
                   ORDER BY m.delivery_id""",
                (g["id"],)).fetchall()
            if not members:
                # held 成员已被其他路径取消（待办处理/对账/停用）：组落定取消并留痕
                cur.execute(
                    "UPDATE approval_notification_groups SET status='cancelled', "
                    "updated_at=? WHERE id=? AND status='open'", (now, g["id"]))
                audit.record(cur, "approval_aggregation_group_cancelled", None, None, {
                    "aggregation_group_id": g["id"], "rule_id": g["rule_id"],
                    "recipient": g["recipient"], "channel": g["channel"],
                    "members": 0,
                    "reason": "all_sources_closed"}, ts=now)
                continue
            open_members = [m for m in members if m["todo_status"] in
                            notif.OPEN_TODO_STATUSES]
            if not open_members:
                # 窗口内来源全部落定：摘要不再需要
                cur.execute(
                    "UPDATE approval_notification_deliveries SET status='cancelled', "
                    "updated_at=? WHERE group_id=? AND status='held'",
                    (now, g["id"]))
                cur.execute(
                    "UPDATE approval_notification_groups SET status='cancelled', "
                    "updated_at=? WHERE id=? AND status='open'", (now, g["id"]))
                audit.record(cur, "approval_aggregation_group_cancelled", None, None, {
                    "aggregation_group_id": g["id"], "rule_id": g["rule_id"],
                    "recipient": g["recipient"], "channel": g["channel"],
                    "members": len(members),
                    "reason": "all_sources_closed"}, ts=now)
                continue
            subject, body, digest_payload = _build_digest(cur, locked, open_members)
            anchor = open_members[0]
            address = anchor["address"]
            # 摘要同样受静默时段约束（静默中则延迟到时段结束，按 ordinal 排序）
            resume_at = quiet_resume_at(
                cur, recipient=g["recipient"], channel=g["channel"], now=now)
            status = "delayed" if resume_at is not None else "pending"
            cur.execute(
                """INSERT INTO approval_notification_deliveries
                   (todo_id, recipient, channel, address, subject, body, payload,
                    status, next_retry_at, group_id, delayed_until, ordinal,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,NULL,?,?,?,?,?)""",
                (anchor["todo_id"], g["recipient"], g["channel"], address,
                 subject, body,
                 json.dumps(digest_payload, ensure_ascii=False, sort_keys=True)
                 if g["channel"] == notif.CHANNEL_WEBHOOK else None,
                 status, g["id"], resume_at, anchor["todo_id"], now, now))
            digest_id = cur.lastrowid
            # 成员行被摘要替代（内容保留可查，不再单独发送）
            cur.execute(
                "UPDATE approval_notification_deliveries SET status='aggregated', "
                "updated_at=? WHERE group_id=? AND status='held'",
                (now, g["id"]))
            cur.execute(
                "UPDATE approval_notification_groups SET status='flushed', "
                "flushed_at=?, updated_at=? WHERE id=? AND status='open'",
                (now, now, g["id"]))
            audit.record(cur, "approval_aggregation_group_flushed", None, None, {
                "aggregation_group_id": g["id"], "rule_id": g["rule_id"],
                "recipient": g["recipient"], "channel": g["channel"],
                "source_type": g["source_type"],
                "source_batch_id": g["batch_id"], "change_id": g["change_id"],
                "members_total": len(members), "members_open": len(open_members),
                "digest_delivery_id": digest_id,
                "digest_status": status,
                "window_opened_at": g["window_opened_at"],
                "window_closes_at": g["window_closes_at"]}, ts=now)
            if status == "delayed":
                audit.record(cur, "approval_delivery_delayed", None, None, {
                    "delivery_id": digest_id, "todo_id": anchor["todo_id"],
                    "recipient": g["recipient"], "channel": g["channel"],
                    "event_type": "aggregated_digest",
                    "delayed_until": resume_at,
                    "aggregation_group_id": g["id"]}, ts=now)
            flushed += 1
    return flushed


def _build_digest(cur: sqlite3.Cursor, group: sqlite3.Row,
                  members: list[sqlite3.Row]) -> tuple[str, str, dict]:
    """把组成员合并为一条摘要（主题/正文/结构化 webhook 负载）。"""
    by_type: dict[str, int] = {}
    for m in members:
        by_type[m["event_type"]] = by_type.get(m["event_type"], 0) + 1
    type_summary = "、".join(f"{t}×{c}" for t, c in sorted(by_type.items()))
    if group["source_type"] == notif.SOURCE_BATCH:
        scope = f"批次 #{group['batch_id']}"
    else:
        scope = f"策略变更单 #{group['change_id']}"
    subject = f"[审批通知摘要] {scope} {len(members)} 条提醒"
    lines = [f"{scope} 在聚合窗口内合并了 {len(members)} 条审批提醒："]
    for i, m in enumerate(members, 1):
        lines.append(f"{i}. [{m['event_type']}] {m['event_subject']}")
    lines.append("请在站内待办中查看并处理（各待办独立可操作，本摘要不替代任何待办）。")
    payload = {
        "event": "aggregated_digest",
        "digest": True,
        "aggregation_group_id": group["id"],
        "rule_id": group["rule_id"],
        "source_type": group["source_type"],
        "batch_id": group["batch_id"],
        "change_id": group["change_id"],
        "count": len(members),
        "event_counts": by_type,
        "window_opened_at": group["window_opened_at"],
        "window_closes_at": group["window_closes_at"],
        "items": [{"todo_id": m["todo_id"], "delivery_id": m["delivery_id"],
                   "event_type": m["event_type"], "subject": m["event_subject"],
                   "event_payload": json.loads(m["event_payload"] or "{}")}
                  for m in members],
    }
    return subject, "\n".join(lines), payload


# ============================================================================
# 静默时段
# ============================================================================

def _daily_window_seconds(start_hm: str, end_hm: str, now: float) -> tuple[float, float]:
    """把每日重复窗口（UTC HH:MM）换算为覆盖 now 的 [start,end]（支持跨午夜）。

    先看今日 start 起的窗口（end<=start 时结束在次日）；now 早于今日 start 时，
    再看是否落在昨日 start 跨到今日的窗口里。
    """
    dt = datetime.fromtimestamp(now, tz=timezone.utc)
    sh, sm = int(start_hm[:2]), int(start_hm[3:])
    eh, em = int(end_hm[:2]), int(end_hm[3:])
    midnight = dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_start = midnight + sh * 3600 + sm * 60
    today_end = midnight + eh * 3600 + em * 60
    if today_end <= today_start:  # 跨午夜：结束在次日
        today_end += 86400
    if now >= today_start:
        return today_start, today_end
    return today_start - 86400, today_end - 86400


def _schedule_active_window(sch: sqlite3.Row, now: float) -> tuple[float, float] | None:
    """返回该计划在 now 时刻所在的静默窗 [start,end]；不在窗内返回 None。"""
    if sch["daily"]:
        start, end = _daily_window_seconds(sch["start_time"], sch["end_time"], now)
    else:
        start, end = sch["start_at"], sch["end_at"]
    if start is None or end is None:
        return None
    if start <= now < end:
        return start, end
    return None


def quiet_resume_at(cur: sqlite3.Cursor, *, recipient: str, channel: str,
                    now: float) -> float | None:
    """该接收人/通道此刻是否处于静默：返回预计放行时间（取覆盖当前的最晚窗口结束），
    否则 None。通道维度 NULL/'*' 匹配两个通道。"""
    schedules = cur.execute(
        "SELECT * FROM approval_quiet_schedules WHERE active=1 ORDER BY id").fetchall()
    resume = None
    for sch in schedules:
        if not _rule_matches(sch["recipient"], recipient):
            continue
        if not _rule_matches(sch["channel"], channel):
            continue
        window = _schedule_active_window(sch, now)
        if window is None:
            continue
        end = window[1]
        resume = end if resume is None else max(resume, end)
    return resume


def audit_delayed(cur, *, delivery_id: int, todo, recipient: str, channel: str,
                  resume_at: float, now: float) -> None:
    audit.record(cur, "approval_delivery_delayed", None, None, {
        "delivery_id": delivery_id, "todo_id": todo["id"],
        "recipient": recipient, "channel": channel,
        "event_type": todo["event_type"],
        "source_type": todo["source_type"],
        "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
        "node_id": todo["node_id"],
        "delayed_until": resume_at}, ts=now)


def release_delayed(db: Database, now: float | None = None) -> int:
    """静默结束放行：delayed 投递重新计算静默状态——

    - 仍在静默（另一窗口/更长窗口覆盖，或配置变更）-> 仅推进 delayed_until；
    - 静默结束 -> 按 (ordinal,id) 放回 pending（dispatcher 同序发送，恢复原事件顺序）。
    条件更新 + 同事务审计保证重复扫描/重启每条只放行一次。
    """
    now = time.time() if now is None else now
    rows = db.query(
        "SELECT * FROM approval_notification_deliveries WHERE status='delayed' "
        "ORDER BY ordinal, id")
    released = 0
    for row in rows:
        with db.tx() as cur:
            locked = cur.execute(
                "SELECT * FROM approval_notification_deliveries WHERE id=?",
                (row["id"],)).fetchone()
            if locked["status"] != "delayed":
                continue
            todo = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                               (locked["todo_id"],)).fetchone()
            # 待办已关闭（处理/取消/升级停止）：不补发，直接取消
            if todo is None or todo["status"] not in notif.OPEN_TODO_STATUSES:
                cur.execute(
                    "UPDATE approval_notification_deliveries SET status='cancelled', "
                    "delayed_until=NULL, updated_at=? WHERE id=? AND status='delayed'",
                    (now, locked["id"]))
                audit.record(cur, "approval_delivery_cancelled", None, None, {
                    "delivery_id": locked["id"], "todo_id": locked["todo_id"],
                    "recipient": locked["recipient"], "channel": locked["channel"],
                    "reason": "todo_closed_during_quiet"}, ts=now)
                continue
            resume_at = quiet_resume_at(
                cur, recipient=locked["recipient"], channel=locked["channel"], now=now)
            if resume_at is not None and resume_at > now:
                if (locked["delayed_until"] or 0) != resume_at:
                    cur.execute(
                        "UPDATE approval_notification_deliveries SET delayed_until=?, "
                        "updated_at=? WHERE id=? AND status='delayed'",
                        (resume_at, now, locked["id"]))
                continue
            changed = cur.execute(
                """UPDATE approval_notification_deliveries
                   SET status='pending', next_retry_at=?, delayed_until=NULL,
                       updated_at=?
                   WHERE id=? AND status='delayed'""",
                (now, now, locked["id"])).rowcount
            if changed:
                audit.record(cur, "approval_delivery_released", None, None, {
                    "delivery_id": locked["id"], "todo_id": locked["todo_id"],
                    "recipient": locked["recipient"], "channel": locked["channel"],
                    "ordinal": locked["ordinal"],
                    "held_since": locked["created_at"],
                    "event_type": todo["event_type"]}, ts=now)
                released += 1
    return released


# ============================================================================
# 升级
# ============================================================================

def _todo_roles(cur: sqlite3.Cursor, todo) -> list[str]:
    """待办来源可承担的角色集合：批次节点取 allowed_roles；变更单为伪角色 'change'。"""
    if todo["source_type"] == notif.SOURCE_CHANGE:
        return [CHANGE_ROLE]
    if todo["node_id"] is None:
        return []
    node = cur.execute("SELECT allowed_roles FROM replay_approval_nodes WHERE id=?",
                       (todo["node_id"],)).fetchone()
    if node is None:
        return []
    return json.loads(node["allowed_roles"])


def _todo_deadline(cur: sqlite3.Cursor, todo) -> float | None:
    if todo["source_type"] == notif.SOURCE_CHANGE:
        ch = cur.execute("SELECT expires_at FROM replay_policy_changes WHERE id=?",
                         (todo["change_id"],)).fetchone()
        return ch["expires_at"] if ch else None
    if todo["node_id"] is None:
        return None
    node = cur.execute("SELECT deadline FROM replay_approval_nodes WHERE id=?",
                       (todo["node_id"],)).fetchone()
    return node["deadline"] if node else None


def _policy_matches(policy_roles: list[str], todo_roles: list[str]) -> bool:
    if "any" in policy_roles:
        return True
    return any(r in todo_roles for r in policy_roles)


def _fire_escalation(cur, esc: sqlite3.Row, policy: sqlite3.Row,
                     level_idx: int, spec: dict, now: float) -> None:
    """触发一个升级级别：置 escalation firing/fired 计数，发出升级通知事件+待办，
    落 approval_escalation_levels。事件 event_key 含 (策略,原始待办,级别) 唯一。"""
    level = level_idx + 1
    base_todo = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                            (esc["base_todo_id"],)).fetchone()
    recipients = sorted(set(spec.get("recipients") or []))
    # 发起人/批次发起人不作为升级接收人（职责分离与既有通知口径一致）；
    # 升级给原接收人本人无意义，一并排除。
    excluded = {base_todo["recipient"]}
    if base_todo["source_type"] == notif.SOURCE_BATCH and base_todo["batch_id"]:
        b = cur.execute("SELECT operator FROM replay_batches WHERE id=?",
                        (base_todo["batch_id"],)).fetchone()
        if b:
            excluded.add(b["operator"])
    elif base_todo["source_type"] == notif.SOURCE_CHANGE and base_todo["change_id"]:
        ch = cur.execute("SELECT operator FROM replay_policy_changes WHERE id=?",
                         (base_todo["change_id"],)).fetchone()
        if ch:
            excluded.add(ch["operator"])
    configured_recipients = [r for r in recipients if r not in excluded]
    # 级别接收人按策略配置快照（晚注册的联系人之后由 backfill 补发，不漏关键提醒）；
    # 本次立即生成待办只发给当前活跃联系人
    contact_rows = cur.execute(
        "SELECT name FROM approval_contacts WHERE active=1").fetchall()
    active_contacts = {r["name"] for r in contact_rows}
    recipients = [r for r in configured_recipients if r in active_contacts]
    if base_todo["source_type"] == notif.SOURCE_BATCH:
        node = cur.execute("SELECT * FROM replay_approval_nodes WHERE id=?",
                           (base_todo["node_id"],)).fetchone()
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (base_todo["batch_id"],)).fetchone()
        scope = f"重放批次 #{batch['id']} 节点 {node['seq']+1}"
        subject = f"[审批升级 L{level}] {scope} 临近截止仍未处理"
        body = (f"{scope} 的原接收人 {base_todo['recipient']} 尚未处理，"
                f"截止时间 {node['deadline']}，现升级通知（级别 {level}）。"
                f"请在站内待办中处理。")
        payload = {"batch_id": batch["id"], "node_id": node["id"],
                   "event": "escalated", "escalation_level": level,
                   "policy_id": policy["id"], "base_todo_id": base_todo["id"],
                   "original_recipient": base_todo["recipient"],
                   "deadline": node["deadline"]}
        key = f"escalation:policy:{policy['id']}:todo:{base_todo['id']}:lvl:{level}"
        kwargs = dict(source_type=notif.SOURCE_BATCH, batch_id=batch["id"],
                      node_id=node["id"], roles=json.loads(node["allowed_roles"]))
    else:
        ch = cur.execute("SELECT * FROM replay_policy_changes WHERE id=?",
                         (base_todo["change_id"],)).fetchone()
        scope = f"策略变更单 #{ch['id']}"
        subject = f"[审批升级 L{level}] {scope} 临近截止仍未审批"
        body = (f"{scope} 的原接收人 {base_todo['recipient']} 尚未审批，"
                f"截止时间 {ch['expires_at']}，现升级通知（级别 {level}）。")
        payload = {"change_id": ch["id"], "event": "escalated",
                   "escalation_level": level, "policy_id": policy["id"],
                   "base_todo_id": base_todo["id"],
                   "original_recipient": base_todo["recipient"],
                   "expires_at": ch["expires_at"]}
        key = f"escalation:policy:{policy['id']}:todo:{base_todo['id']}:lvl:{level}"
        kwargs = dict(source_type=notif.SOURCE_CHANGE, change_id=ch["id"],
                      roles=[CHANGE_ROLE])
    # 事件幂等落盘（event_key 唯一）；_emit_tx 在同事务内为升级接收人生成可操作待办，
    # 其外发投递继续走聚合/静默路由。
    created = notif._emit_tx(
        cur, event_key=key, event_type="escalated", recipients=set(recipients),
        subject=subject, body=body, actionable=True, payload=payload,
        audit_extra={"escalation_policy_id": policy["id"],
                     "escalation_level": level,
                     "base_todo_id": base_todo["id"],
                     "original_recipient": base_todo["recipient"]},
        now=now, **kwargs)
    notify_event = cur.execute(
        "SELECT * FROM approval_notify_events WHERE event_key=?", (key,)).fetchone()
    cur.execute(
        """INSERT INTO approval_escalation_levels
           (escalation_id, policy_id, base_todo_id, level, recipients,
            notify_event_id, fired_at)
           VALUES (?,?,?,?,?,?,?)""",
        (esc["id"], policy["id"], esc["base_todo_id"], level,
         json.dumps(configured_recipients, ensure_ascii=False),
         notify_event["id"] if notify_event else None, now))
    cur.execute(
        "UPDATE approval_escalations SET levels_fired=levels_fired+1, "
        "updated_at=? WHERE id=?", (now, esc["id"]))
    audit.record(cur, "approval_escalation_fired", None, None, {
        "escalation_id": esc["id"], "policy_id": policy["id"],
        "base_todo_id": base_todo["id"], "level": level,
        "source_type": base_todo["source_type"],
        "source_batch_id": base_todo["batch_id"],
        "change_id": base_todo["change_id"], "node_id": base_todo["node_id"],
        "original_recipient": base_todo["recipient"],
        "recipients": configured_recipients,
        "recipients_notified_now": recipients, "todos_created": created,
        "trigger_after_seconds": spec.get("after_seconds"),
        "trigger_before_deadline_seconds": spec.get("before_deadline_seconds")},
        ts=now)


def _source_key(todo) -> str | None:
    """升级去重键：同一节点/变更单视为同一升级来源（该接收人在节点上的多个
    可操作待办——activated/vote/deadline——共享一条升级链）。"""
    if todo["source_type"] == notif.SOURCE_BATCH:
        return f"node:{todo['node_id']}" if todo["node_id"] is not None else None
    return f"change:{todo['change_id']}" if todo["change_id"] is not None else None


def _open_actionable_todos(cur: sqlite3.Cursor) -> list[sqlite3.Row]:
    """当前仍开放且来源仍可操作的可操作待办（升级事件本身不再升级）。

    按 (来源, 原始接收人) 归组，取该组最早的待办作为升级锚点（计时起点 open_at）。
    """
    rows = cur.execute(
        """SELECT * FROM approval_todos
           WHERE actionable=1 AND status IN ('unread','read')
             AND event_type<>'escalated'
           ORDER BY id""").fetchall()
    groups: dict[tuple[str, str], sqlite3.Row] = {}
    for todo in rows:
        if todo["source_type"] == notif.SOURCE_BATCH:
            actionable = notif._batch_node_actionable(cur, todo)
        else:
            actionable = notif._change_actionable(cur, todo)
        if not actionable:
            continue
        key = _source_key(todo)
        if key is None:
            continue
        gk = (key, todo["recipient"])
        if gk not in groups:
            groups[gk] = todo  # ORDER BY id：最早一条做锚点
    return list(groups.values())


def scan_escalations(db: Database, now: float | None = None) -> int:
    """扫描开放的可操作待办，按命中的升级策略触发到期级别。

    去重单元 = (策略, 来源节点/变更单, 原始接收人)：该接收人在同一节点上的多个
    待办（激活/投票提醒/临期）只产生一条升级链。(policy_id,source_key,base_recipient)
    唯一约束 + (policy_id,base_todo_id,level) 级别唯一 + 触发前写事务内重新校验，
    重复扫描、服务重启、并发 worker 都不会重复升级；链停止后后续级别全部跳过，
    关键审批提醒不会漏（锚点待办开放期间每轮都会复核到期级别）。
    """
    now = time.time() if now is None else now
    policies = db.query(
        "SELECT * FROM approval_escalation_policies WHERE active=1 ORDER BY id")
    if not policies:
        return 0
    fired = 0
    with db.tx() as cur:
        anchors = _open_actionable_todos(cur)
        for locked_todo in anchors:
            todo_roles = _todo_roles(cur, locked_todo)
            deadline = _todo_deadline(cur, locked_todo)
            source_key = _source_key(locked_todo)
            for policy in policies:
                levels = json.loads(policy["levels_json"])
                if not _policy_matches(json.loads(policy["roles"]), todo_roles):
                    continue
                esc = cur.execute(
                    "SELECT * FROM approval_escalations WHERE policy_id=? "
                    "AND source_key=? AND base_recipient=?",
                    (policy["id"], source_key, locked_todo["recipient"])).fetchone()
                if esc is None:
                    try:
                        cur.execute(
                            """INSERT INTO approval_escalations
                               (policy_id, base_todo_id, base_recipient, source_key,
                                source_type, batch_id, change_id, node_id,
                                levels_total, status, created_at, updated_at)
                               VALUES (?,?,?,?,?,?,?,?,?, 'pending', ?, ?)""",
                            (policy["id"], locked_todo["id"], locked_todo["recipient"],
                             source_key, locked_todo["source_type"],
                             locked_todo["batch_id"], locked_todo["change_id"],
                             locked_todo["node_id"], len(levels), now, now))
                    except sqlite3.IntegrityError:
                        # 并发扫描：另一事务已建链，重新读取
                        esc = cur.execute(
                            "SELECT * FROM approval_escalations WHERE policy_id=? "
                            "AND source_key=? AND base_recipient=?",
                            (policy["id"], source_key,
                             locked_todo["recipient"])).fetchone()
                    else:
                        esc = cur.execute(
                            "SELECT * FROM approval_escalations WHERE id=?",
                            (cur.lastrowid,)).fetchone()
                        audit.record(cur, "approval_escalation_armed", None, None, {
                            "escalation_id": esc["id"], "policy_id": policy["id"],
                            "base_todo_id": locked_todo["id"],
                            "recipient": locked_todo["recipient"],
                            "source_key": source_key,
                            "source_type": locked_todo["source_type"],
                            "source_batch_id": locked_todo["batch_id"],
                            "change_id": locked_todo["change_id"],
                            "node_id": locked_todo["node_id"],
                            "levels_total": len(levels)}, ts=now)
                if esc["status"] == "stopped":
                    continue  # 原接收人已处理/来源落定：后续级别全部停止
                for idx, spec in enumerate(levels):
                    level = idx + 1
                    if level <= esc["levels_fired"]:
                        continue  # 已触发级别永不重发
                    due = False
                    after = spec.get("after_seconds")
                    before = spec.get("before_deadline_seconds")
                    if after is not None and now >= locked_todo["open_at"] + float(after):
                        due = True
                    if before is not None and deadline is not None \
                            and now >= deadline - float(before):
                        due = True
                    if not due:
                        continue
                    # 触发前条件状态转移：单一赢家（并发扫描/重启重复运行）
                    changed = cur.execute(
                        """UPDATE approval_escalations SET status='firing', updated_at=?
                           WHERE id=? AND status='pending' AND levels_fired=?""",
                        (now, esc["id"], esc["levels_fired"])).rowcount
                    if not changed:
                        continue
                    _fire_escalation(cur, esc, policy, idx, spec, now)
                    cur.execute(
                        "UPDATE approval_escalations SET status='pending', "
                        "updated_at=? WHERE id=?", (now, esc["id"]))
                    fired += 1


def stop_escalations_for_todo_tx(cur: sqlite3.Cursor, base_todo_id: int, *,
                                 reason: str, now: float) -> int:
    """原接收人处理待办或来源落定时停止升级（在调用方的写事务内）。

    按 (来源节点/变更单, 原始接收人) 定位升级链——该接收人在节点上的任一可操作待办
    被处理（activated/vote/deadline 等）都意味着其已响应，升级必须停止：
    - 后续级别不再触发（approval_escalations 落 stopped）；
    - 已触发级别尚未发出的邮件/webhook（pending/failed/held/delayed，含聚合组中的
      held）同事务取消；已发出的外部效果无法撤回，轨迹保留；
    - 升级接收人的站内待办不关闭——他们若仍可操作，处理回写走原审批门禁（节点通常
      已落定会被挡下并惰性对账）。
    幂等：只转移非 stopped 的链，返回停止的升级链数量。
    """
    todo = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                       (base_todo_id,)).fetchone()
    if todo is None:
        return 0
    # 升级待办（接收人=升级对象而非原始接收人）不锚定链；升级接收人处理由其自身动作
    # 导致的来源落定（sweep）走 source_closed 路径，此处只处理原始接收人的链
    rows = cur.execute(
        "SELECT * FROM approval_escalations WHERE base_recipient=? "
        "AND COALESCE(node_id,-1)=COALESCE(?,-1) "
        "AND COALESCE(change_id,-1)=COALESCE(?,-1) AND status<>'stopped'",
        (todo["recipient"], todo["node_id"], todo["change_id"])).fetchall()
    stopped = 0
    for esc in rows:
        levels = cur.execute(
            "SELECT * FROM approval_escalation_levels WHERE escalation_id=?",
            (esc["id"],)).fetchall()
        cancelled = 0
        for lv in levels:
            if lv["notify_event_id"] is None:
                continue
            todo_ids = [r["id"] for r in cur.execute(
                "SELECT id FROM approval_todos WHERE event_id=?",
                (lv["notify_event_id"],)).fetchall()]
            if not todo_ids:
                continue
            qmarks = ",".join("?" * len(todo_ids))
            cancelled += cur.execute(
                f"""UPDATE approval_notification_deliveries
                    SET status='cancelled', next_retry_at=NULL, delayed_until=NULL,
                        updated_at=?
                    WHERE todo_id IN ({qmarks})
                      AND status IN ('pending','failed','held','delayed')""",
                (now, *todo_ids)).rowcount
        cur.execute(
            "UPDATE approval_escalations SET status='stopped', stop_reason=?, "
            "stopped_at=?, updated_at=? WHERE id=? AND status<>'stopped'",
            (reason, now, now, esc["id"])).rowcount
        audit.record(cur, "approval_escalation_stopped", None, None, {
            "escalation_id": esc["id"], "policy_id": esc["policy_id"],
            "base_todo_id": base_todo_id, "reason": reason,
            "levels_fired": esc["levels_fired"], "levels_total": esc["levels_total"],
            "cancelled_deliveries": cancelled}, ts=now)
        stopped += 1
    return stopped


# ============================================================================
# 管理：规则 CRUD
# ============================================================================

def create_aggregation_rule(db: Database, req: AggregationRuleRequest) -> dict:
    operator = notif._require(req.operator, "operator")
    if req.window_seconds <= 0:
        raise HTTPException(422, "window_seconds must be > 0")
    if req.event_type is not None and req.event_type in ("", RULE_MATCH_ALL):
        event_type = None
    else:
        event_type = req.event_type
    recipient = (req.recipient or "").strip() or None
    now = time.time()
    with db.tx() as cur:
        if req.batch_id is not None:
            if cur.execute("SELECT 1 AS x FROM replay_batches WHERE id=?",
                           (req.batch_id,)).fetchone() is None:
                raise HTTPException(404, f"batch {req.batch_id} not found")
        cur.execute(
            """INSERT INTO approval_aggregation_rules
               (recipient, batch_id, event_type, window_seconds, active,
                created_by, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (recipient, req.batch_id, event_type, req.window_seconds,
             1 if req.active else 0, operator, now, now))
        rule_id = cur.lastrowid
        audit.record(cur, "approval_aggregation_rule_set", None, None, {
            "rule_id": rule_id, "operator": operator, "recipient": recipient,
            "batch_id": req.batch_id, "event_type": event_type,
            "window_seconds": req.window_seconds, "active": req.active}, ts=now)
    return {"result": "created", "rule_id": rule_id}


def list_aggregation_rules(db: Database, active: bool | None = None) -> dict:
    sql, params = "SELECT * FROM approval_aggregation_rules WHERE 1=1", []
    if active is not None:
        sql += " AND active=?"
        params.append(1 if active else 0)
    sql += " ORDER BY id"
    return {"rules": [_agg_rule_view(r) for r in db.query(sql, tuple(params))]}


def set_aggregation_rule_active(db: Database, rule_id: int, active: bool,
                                operator: str) -> dict:
    operator = notif._require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM approval_aggregation_rules WHERE id=?",
                          (rule_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "aggregation rule not found")
        cur.execute("UPDATE approval_aggregation_rules SET active=?, updated_at=? "
                    "WHERE id=?", (1 if active else 0, now, rule_id))
        audit.record(cur, "approval_aggregation_rule_set", None, None, {
            "rule_id": rule_id, "operator": operator, "active": active,
            "action": "activate" if active else "deactivate"}, ts=now)
    return {"result": "updated", "rule_id": rule_id, "active": active}


def _agg_rule_view(row) -> dict:
    return {"id": row["id"], "recipient": row["recipient"],
            "batch_id": row["batch_id"], "event_type": row["event_type"],
            "window_seconds": row["window_seconds"],
            "active": bool(row["active"]), "created_by": row["created_by"],
            "created_at": row["created_at"], "updated_at": row["updated_at"]}


def list_groups(db: Database, *, status_filter: str | None = None,
                recipient: str | None = None, batch_id: int | None = None,
                rule_id: int | None = None, limit: int = 100) -> dict:
    sql, params = "SELECT * FROM approval_notification_groups WHERE 1=1", []
    if status_filter:
        sql += " AND status=?"
        params.append(status_filter)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if rule_id is not None:
        sql += " AND rule_id=?"
        params.append(rule_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    groups = []
    for g in db.query(sql, tuple(params)):
        item = _group_view(db, g)
        groups.append(item)
    return {"groups": groups}


def _group_view(db: Database, g: sqlite3.Row) -> dict:
    members = db.query(
        """SELECT m.delivery_id, m.todo_id, m.event_id, m.event_type, m.joined_at,
                  d.status AS delivery_status
           FROM approval_notification_group_members m
           JOIN approval_notification_deliveries d ON d.id=m.delivery_id
           WHERE m.group_id=? ORDER BY m.delivery_id""", (g["id"],))
    digest = db.query_one(
        """SELECT d.id AS delivery_id, d.status, d.sent_at, d.channel
           FROM approval_notification_deliveries d
           LEFT JOIN approval_notification_group_members m ON m.delivery_id=d.id
           WHERE d.group_id=? AND m.delivery_id IS NULL LIMIT 1""",
        (g["id"],))
    return {
        "id": g["id"], "rule_id": g["rule_id"], "recipient": g["recipient"],
        "channel": g["channel"], "source_type": g["source_type"],
        "batch_id": g["batch_id"], "change_id": g["change_id"],
        "window_seconds": g["window_seconds"], "status": g["status"],
        "window_opened_at": g["window_opened_at"],
        "window_closes_at": g["window_closes_at"], "flushed_at": g["flushed_at"],
        "members": [dict(m) for m in members],
        "member_count": len(members),
        "digest_delivery": dict(digest) if digest else None}


# ---- 静默时段 CRUD ------------------------------------------------------------

def create_quiet_schedule(db: Database, req: QuietScheduleRequest) -> dict:
    operator = notif._require(req.operator, "operator")
    channel = (req.channel or "").strip() or None
    if channel is not None and channel not in notif.SUPPORTED_CHANNELS:
        raise HTTPException(422, f"unsupported channel {channel!r} "
                                 f"(support {','.join(notif.SUPPORTED_CHANNELS)})")
    recipient = (req.recipient or "").strip() or None
    now = time.time()
    if req.daily:
        start_time = _parse_hhmm(req.start_time)
        end_time = _parse_hhmm(req.end_time)
        start_at = end_at = None
    else:
        start_at = _epoch(req.start_at, "start_at")
        end_at = _epoch(req.end_at, "end_at")
        if start_at is None or end_at is None:
            raise HTTPException(422, "start_at and end_at are required for a "
                                     "one-off quiet schedule")
        if end_at <= start_at:
            raise HTTPException(422, "end_at must be later than start_at")
        start_time = end_time = None
    with db.tx() as cur:
        cur.execute(
            """INSERT INTO approval_quiet_schedules
               (recipient, channel, daily, start_time, end_time, start_at, end_at,
                active, created_by, note, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,1,?,?,?,?)""",
            (recipient, channel, 1 if req.daily else 0, start_time, end_time,
             start_at, end_at, operator, req.note, now, now))
        schedule_id = cur.lastrowid
        audit.record(cur, "approval_quiet_schedule_set", None, None, {
            "schedule_id": schedule_id, "operator": operator,
            "recipient": recipient, "channel": channel, "daily": req.daily,
            "start_time": start_time, "end_time": end_time,
            "start_at": start_at, "end_at": end_at, "note": req.note}, ts=now)
    return {"result": "created", "schedule_id": schedule_id}


def list_quiet_schedules(db: Database, active: bool | None = None) -> dict:
    sql, params = "SELECT * FROM approval_quiet_schedules WHERE 1=1", []
    if active is not None:
        sql += " AND active=?"
        params.append(1 if active else 0)
    sql += " ORDER BY id"
    return {"schedules": [_schedule_view(r) for r in db.query(sql, tuple(params))]}


def set_quiet_schedule_active(db: Database, schedule_id: int, active: bool,
                              operator: str) -> dict:
    operator = notif._require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM approval_quiet_schedules WHERE id=?",
                          (schedule_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "quiet schedule not found")
        cur.execute("UPDATE approval_quiet_schedules SET active=?, updated_at=? "
                    "WHERE id=?", (1 if active else 0, now, schedule_id))
        audit.record(cur, "approval_quiet_schedule_set", None, None, {
            "schedule_id": schedule_id, "operator": operator, "active": active,
            "action": "activate" if active else "deactivate"}, ts=now)
    return {"result": "updated", "schedule_id": schedule_id, "active": active}


def _schedule_view(row) -> dict:
    return {"id": row["id"], "recipient": row["recipient"],
            "channel": row["channel"], "daily": bool(row["daily"]),
            "start_time": row["start_time"], "end_time": row["end_time"],
            "start_at": row["start_at"], "end_at": row["end_at"],
            "active": bool(row["active"]), "created_by": row["created_by"],
            "note": row["note"], "created_at": row["created_at"],
            "updated_at": row["updated_at"]}


# ---- 升级策略 CRUD ------------------------------------------------------------

def create_escalation_policy(db: Database, req: EscalationPolicyRequest) -> dict:
    operator = notif._require(req.operator, "operator")
    name = notif._require(req.name, "name")
    roles = sorted(set(req.roles))
    if not roles:
        raise HTTPException(422, "roles must be a non-empty list "
                                 "(node roles, 'any', or 'change')")
    if not req.levels:
        raise HTTPException(422, "levels must be a non-empty list")
    levels_json = []
    last_after = -1.0
    for i, lv in enumerate(req.levels, 1):
        recipients = sorted(set(lv.recipients))
        if not recipients:
            raise HTTPException(422, f"level {i}: recipients must be non-empty")
        if lv.after_seconds is None and lv.before_deadline_seconds is None:
            raise HTTPException(422, f"level {i}: one of after_seconds or "
                                     "before_deadline_seconds is required")
        if lv.after_seconds is not None and lv.after_seconds < 0:
            raise HTTPException(422, f"level {i}: after_seconds must be >= 0")
        if lv.before_deadline_seconds is not None and lv.before_deadline_seconds < 0:
            raise HTTPException(422, f"level {i}: before_deadline_seconds must be >= 0")
        if lv.after_seconds is not None and lv.after_seconds <= last_after:
            raise HTTPException(422, f"level {i}: after_seconds must increase "
                                     "across levels")
        if lv.after_seconds is not None:
            last_after = float(lv.after_seconds)
        levels_json.append({"recipients": recipients,
                            "after_seconds": (float(lv.after_seconds)
                                              if lv.after_seconds is not None else None),
                            "before_deadline_seconds": (
                                float(lv.before_deadline_seconds)
                                if lv.before_deadline_seconds is not None else None)})
    now = time.time()
    with db.tx() as cur:
        cur.execute(
            """INSERT INTO approval_escalation_policies
               (name, roles, levels_json, active, created_by, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?)""",
            (name, json.dumps(roles, ensure_ascii=False),
             json.dumps(levels_json, ensure_ascii=False),
             1 if req.active else 0, operator, now, now))
        policy_id = cur.lastrowid
        audit.record(cur, "approval_escalation_policy_set", None, None, {
            "policy_id": policy_id, "operator": operator, "name": name,
            "roles": roles, "levels": levels_json, "active": req.active}, ts=now)
    return {"result": "created", "policy_id": policy_id}


def list_escalation_policies(db: Database, active: bool | None = None) -> dict:
    sql, params = "SELECT * FROM approval_escalation_policies WHERE 1=1", []
    if active is not None:
        sql += " AND active=?"
        params.append(1 if active else 0)
    sql += " ORDER BY id"
    return {"policies": [_policy_view(r) for r in db.query(sql, tuple(params))]}


def set_escalation_policy_active(db: Database, policy_id: int, active: bool,
                                 operator: str) -> dict:
    operator = notif._require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM approval_escalation_policies WHERE id=?",
                          (policy_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "escalation policy not found")
        cur.execute("UPDATE approval_escalation_policies SET active=?, updated_at=? "
                    "WHERE id=?", (1 if active else 0, now, policy_id))
        audit.record(cur, "approval_escalation_policy_set", None, None, {
            "policy_id": policy_id, "operator": operator, "active": active,
            "action": "activate" if active else "deactivate"}, ts=now)
    return {"result": "updated", "policy_id": policy_id, "active": active}


def _policy_view(row) -> dict:
    return {"id": row["id"], "name": row["name"],
            "roles": json.loads(row["roles"]),
            "levels": json.loads(row["levels_json"]),
            "active": bool(row["active"]), "created_by": row["created_by"],
            "created_at": row["created_at"], "updated_at": row["updated_at"]}


def list_escalations(db: Database, *, status_filter: str | None = None,
                     recipient: str | None = None, batch_id: int | None = None,
                     policy_id: int | None = None, base_todo_id: int | None = None,
                     limit: int = 100) -> dict:
    sql, params = "SELECT * FROM approval_escalations WHERE 1=1", []
    if status_filter:
        sql += " AND status=?"
        params.append(status_filter)
    if recipient:
        sql += " AND base_todo_id IN (SELECT id FROM approval_todos WHERE recipient=?)"
        params.append(recipient)
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if policy_id is not None:
        sql += " AND policy_id=?"
        params.append(policy_id)
    if base_todo_id is not None:
        sql += " AND base_todo_id=?"
        params.append(base_todo_id)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    items = []
    for esc in db.query(sql, tuple(params)):
        levels = db.query(
            "SELECT * FROM approval_escalation_levels WHERE escalation_id=? "
            "ORDER BY level", (esc["id"],))
        items.append({
            "id": esc["id"], "policy_id": esc["policy_id"],
            "base_todo_id": esc["base_todo_id"],
            "source_type": esc["source_type"], "batch_id": esc["batch_id"],
            "change_id": esc["change_id"], "node_id": esc["node_id"],
            "levels_total": esc["levels_total"], "levels_fired": esc["levels_fired"],
            "status": esc["status"], "stop_reason": esc["stop_reason"],
            "stopped_at": esc["stopped_at"], "created_at": esc["created_at"],
            "updated_at": esc["updated_at"],
            "fired_levels": [{"level": lv["level"],
                              "recipients": json.loads(lv["recipients"]),
                              "notify_event_id": lv["notify_event_id"],
                              "fired_at": lv["fired_at"]} for lv in levels]})
    return {"escalations": items}


# ============================================================================
# 路由
# ============================================================================

def create_notif_policy_router(db: Database, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin/approval-notifications",
                       tags=["approval-notification-policy"])

    # ---- 聚合规则 ----------------------------------------------------------
    @router.post("/aggregation-rules")
    def create_aggregation_rule_endpoint(req: AggregationRuleRequest):
        """配置聚合窗口（按接收人/来源批次/事件类型）；窗口内外发重复提醒合并为摘要。"""
        return create_aggregation_rule(db, req)

    @router.get("/aggregation-rules")
    def list_aggregation_rules_endpoint(active: bool | None = None):
        return list_aggregation_rules(db, active)

    @router.post("/aggregation-rules/{rule_id}/active")
    def set_aggregation_rule_active_endpoint(rule_id: int, req: ActiveToggleRequest):
        return set_aggregation_rule_active(
            db, rule_id, req.active, req.operator)

    @router.get("/aggregation-groups")
    def list_groups_endpoint(status: str | None = None, recipient: str | None = None,
                             batch_id: int | None = None, rule_id: int | None = None,
                             limit: int = Query(100, le=1000)):
        """聚合组查询：窗口开启/已发摘要/取消及成员明细均可查。"""
        return list_groups(db, status_filter=status, recipient=recipient,
                           batch_id=batch_id, rule_id=rule_id, limit=limit)

    # ---- 静默时段 ----------------------------------------------------------
    @router.post("/quiet-schedules")
    def create_quiet_schedule_endpoint(req: QuietScheduleRequest):
        """配置静默时段：窗内只保留站内待办，邮件/webhook 延迟到时段结束按序发送。"""
        return create_quiet_schedule(db, req)

    @router.get("/quiet-schedules")
    def list_quiet_schedules_endpoint(active: bool | None = None):
        return list_quiet_schedules(db, active)

    @router.post("/quiet-schedules/{schedule_id}/active")
    def set_quiet_schedule_active_endpoint(schedule_id: int, req: ActiveToggleRequest):
        return set_quiet_schedule_active(
            db, schedule_id, req.active, req.operator)

    # ---- 升级策略 ----------------------------------------------------------
    @router.post("/escalation-policies")
    def create_escalation_policy_endpoint(req: EscalationPolicyRequest):
        """按节点角色配置逐级升级接收人与触发时限（after_seconds / 截止前 N 秒）。"""
        return create_escalation_policy(db, req)

    @router.get("/escalation-policies")
    def list_escalation_policies_endpoint(active: bool | None = None):
        return list_escalation_policies(db, active)

    @router.post("/escalation-policies/{policy_id}/active")
    def set_escalation_policy_active_endpoint(policy_id: int, req: ActiveToggleRequest):
        return set_escalation_policy_active(
            db, policy_id, req.active, req.operator)

    @router.get("/escalations")
    def list_escalations_endpoint(status: str | None = None,
                                  recipient: str | None = None,
                                  batch_id: int | None = None,
                                  policy_id: int | None = None,
                                  base_todo_id: int | None = None,
                                  limit: int = Query(100, le=1000)):
        """升级记录查询：每级触发时间、接收人、级别与停止原因均可查。"""
        return list_escalations(
            db, status_filter=status, recipient=recipient, batch_id=batch_id,
            policy_id=policy_id, base_todo_id=base_todo_id, limit=limit)

    return router

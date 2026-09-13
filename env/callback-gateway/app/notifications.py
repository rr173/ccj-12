"""审批通知与待办分发：在既有重放审批、委托与策略变更链路上生成站内待办，
并通过邮件 / webhook 两种外发通道通知。

事件（审批通知的触发点）
- 节点激活 activated（批次提交生成节点、串行链下一节点激活）；
- 节点收到投票 vote_received（法定人数未满时，告知同节点其他可审批人）；
- 接近截止时间 deadline_approaching（worker 扫描，每节点/变更单至多一次）；
- 节点拒绝 node_rejected / 节点超时 node_timeout / 批次放行 batch_approved /
  批次撤回 batch_cancelled（告知相关人，纯告知待办）；
- 策略变更需要审批 change_required，以及 change_approved/rejected/expired/applied。

接收人解析（按节点角色与当前有效委托）
- 指定角色节点的可操作待办：角色当前有效委托的受托人（且在联系人目录中）；
- 'any' 节点与策略变更单：联系人目录中除来源发起人之外的全部活跃联系人；
- 纯告知待办（批准/拒绝/超时/撤回/变更结果）：来源发起人；
- vote_received 的接收人 = 除投票人与发起人之外、当前可承担该节点的人。

幂等与并发
- approval_notify_events.event_key 唯一：同一事件重放/并发只落一行；
- approval_todos UNIQUE(event_id, recipient)：同一事件对同一接收人至多一个待办；
- 处理待办在同一写事务里「认领（条件更新 unread/read -> claimed 状态门禁）->
  回写原审批动作」，重复点击、过期待办、并发处理都不会重复投票或越过审批门禁
  （原审批函数的角色/委托/状态/职责分离检查仍然全部生效）。

外发投递
- 站内待办始终生成；邮件/webhook 按联系人 channels 生成投递行；
- 发送失败按 base*2^(attempts-1) 指数退避重试，超过 NOTIF_MAX_ATTEMPTS 进 quarantine；
- 待办关闭（已处理/过期/取消）时未发出的投递同事务取消；隔离投递可人工 requeue。

审计
- 生成、发送、重试、隔离、确认（已读/处理）与回写全部落 events；
  通知事件统一带 source_batch_id/change_id（不带 replay_batch_id），
  与重放批次自己的审计时间线互不污染。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit
from . import receipts
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications")

# ---- 常量 -------------------------------------------------------------------

SOURCE_BATCH = "batch"
SOURCE_CHANGE = "change"

TODO_UNREAD = "unread"
TODO_READ = "read"
TODO_HANDLED = "handled"
TODO_EXPIRED = "expired"
TODO_CANCELLED = "cancelled"
OPEN_TODO_STATUSES = (TODO_UNREAD, TODO_READ)
TODO_STATUSES = (TODO_UNREAD, TODO_READ, TODO_HANDLED, TODO_EXPIRED, TODO_CANCELLED)

CHANNEL_EMAIL = "email"
CHANNEL_WEBHOOK = "webhook"
SUPPORTED_CHANNELS = (CHANNEL_EMAIL, CHANNEL_WEBHOOK)

DELIVERY_PENDING = "pending"
DELIVERY_FAILED = "failed"
DELIVERY_SENT = "sent"
DELIVERY_QUARANTINED = "quarantined"
DELIVERY_CANCELLED = "cancelled"
# 聚合窗口内暂缓（窗口关闭后由摘要替代，本行置 aggregated 保留可查）
DELIVERY_HELD = "held"
# 已被聚合摘要替代（不再单独发送，内容保留）
DELIVERY_AGGREGATED = "aggregated"
# 静默时段内暂缓（时段结束按 ordinal 原事件顺序放行回 pending）
DELIVERY_DELAYED = "delayed"
# 尚未发送、可能被取消/暂缓/退避的状态（待办关闭或升级停止时统一取消）
UNSENT_DELIVERY_STATES = ("pending", "failed", "held", "delayed")

# 可回写动作 -> 节点端点 action
BATCH_ACTIONS = ("approve", "reject", "skip")
# 可回写动作 -> 变更单动作
CHANGE_ACTIONS = ("approve", "reject")

# ---- 请求模型 ----------------------------------------------------------------


class ContactUpsertRequest(BaseModel):
    name: str
    email: str | None = None
    webhook_url: str | None = None
    channels: list[str] = []            # 启用的外发通道；站内待办始终生成
    operator: str


class ContactDeactivateRequest(BaseModel):
    operator: str
    reason: str = ""


class TodoReadRequest(BaseModel):
    operator: str


class TodoActRequest(BaseModel):
    operator: str                       # 必须是待办接收人本人
    action: str                         # approve|reject|skip（节点）/ approve|reject（变更单）
    role: str | None = None             # 承担指定角色节点时必填（与原节点端点一致）
    delegation_id: int | None = None
    reason: str = ""                    # reject/skip 必填（由原审批门禁再次校验）
    note: str = ""


class DeliveryRequeueRequest(BaseModel):
    operator: str


# ---- 小工具 ------------------------------------------------------------------

def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return str(value).strip()


def _load_event(cur: sqlite3.Cursor, event_id: int) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM approval_notify_events WHERE id=?",
                      (event_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "notification event not found")
    return row


def _bump_version(cur: sqlite3.Cursor, source_type: str,
                  batch_id: int | None, change_id: int | None) -> int:
    """来源（批次/变更单）上的单调事件版本号。"""
    row = cur.execute(
        "SELECT COALESCE(MAX(state_version),0)+1 AS v FROM approval_notify_events "
        "WHERE source_type=? AND "
        + ("batch_id=?" if source_type == SOURCE_BATCH else "change_id=?"),
        (source_type, batch_id if source_type == SOURCE_BATCH else change_id),
    ).fetchone()
    return int(row["v"])


def _node_recipients(cur: sqlite3.Cursor, node_id: int, exclude: set[str]) -> set[str]:
    """指定角色节点当前可承担的接收人：允许角色当前有效委托的受托人 ∩ 活跃联系人。"""
    node = cur.execute("SELECT * FROM replay_approval_nodes WHERE id=?",
                       (node_id,)).fetchone()
    if node is None:
        return set()
    allowed = json.loads(node["allowed_roles"])
    designated = [r for r in allowed if r != "any"]
    now = time.time()
    if not designated:
        # 'any' 节点：全体活跃联系人（发起人在调用方按事件排除）
        rows = cur.execute(
            "SELECT name FROM approval_contacts WHERE active=1").fetchall()
        return {r["name"] for r in rows} - exclude
    qmarks = ",".join("?" * len(designated))
    rows = cur.execute(
        f"""SELECT DISTINCT d.delegatee AS name
            FROM replay_delegations d
            WHERE d.status='active' AND d.role IN ({qmarks})
              AND d.valid_from<=? AND d.valid_to>=?""",
        (*designated, now, now)).fetchall()
    delegatees = {r["name"] for r in rows}
    if not delegatees:
        return set()
    contact_rows = cur.execute(
        f"""SELECT name FROM approval_contacts
            WHERE active=1 AND name IN ({','.join('?' * len(delegatees))})""",
        tuple(delegatees)).fetchall()
    return {r["name"] for r in contact_rows} - exclude


def _change_recipients(cur: sqlite3.Cursor, change_id: int,
                       exclude: set[str]) -> set[str]:
    """策略变更单的接收人：全部活跃联系人，除提交人等。"""
    rows = cur.execute("SELECT name FROM approval_contacts WHERE active=1").fetchall()
    return {r["name"] for r in rows if r["name"] not in exclude}


def _insert_deliveries(cur: sqlite3.Cursor, todo_id: int, recipient: str,
                       subject: str, body: str, payload: dict, now: float) -> dict:
    """按联系人启用通道生成邮件/webhook 投递行（无通道则 0 条）。

    聚合规则命中时投递落 held 并入组（窗口关闭后由摘要替代）；否则静默时段命中时
    落 delayed（时段结束按 ordinal 原事件顺序放行）；都不命中才是立即可发的 pending。
    路由逻辑集中在 notif_policy（本模块在函数内惰性导入，避免循环依赖）。
    返回 {"created": n, "held": n, "delayed": n, "immediate": n}。
    """
    from . import notif_policy
    contact = cur.execute(
        "SELECT * FROM approval_contacts WHERE name=? AND active=1",
        (recipient,)).fetchone()
    if contact is None:
        return {"created": 0, "held": 0, "delayed": 0, "immediate": 0}
    channels = json.loads(contact["channels"])
    counts = {"created": 0, "held": 0, "delayed": 0, "immediate": 0}
    for channel in channels:
        if channel == CHANNEL_EMAIL:
            address = contact["email"]
            if not address:
                continue
            cpayload = None
        elif channel == CHANNEL_WEBHOOK:
            address = contact["webhook_url"]
            if not address:
                continue
            cpayload = json.dumps(payload or {}, ensure_ascii=False, sort_keys=True)
        else:
            continue
        todo = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                           (todo_id,)).fetchone()
        # 1) 聚合：命中规则 -> held 入组（窗口关闭合并为一条摘要）
        group = notif_policy.find_aggregation_group(
            cur, todo=todo, recipient=recipient, channel=channel, now=now)
        if group is not None:
            cur.execute(
                """INSERT INTO approval_notification_deliveries
                   (todo_id, recipient, channel, address, subject, body, payload,
                    status, next_retry_at, group_id, ordinal, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?, 'held', NULL, ?, ?, ?, ?)""",
                (todo_id, recipient, channel, address, subject, body, cpayload,
                 group["id"], todo_id, now, now))
            delivery_id = cur.lastrowid
            cur.execute(
                """INSERT OR IGNORE INTO approval_notification_group_members
                   (group_id, delivery_id, todo_id, event_id, event_type, joined_at)
                   VALUES (?,?,?,?,?,?)""",
                (group["id"], delivery_id, todo_id, todo["event_id"],
                 todo["event_type"], now))
            notif_policy.audit_held(cur, delivery_id=delivery_id, todo=todo,
                                    recipient=recipient, channel=channel,
                                    group=group, now=now)
            counts["created"] += 1
            counts["held"] += 1
            continue
        # 2) 静默时段：命中 -> delayed（站内待办已即时生成，仅外发暂缓）
        resume_at = notif_policy.quiet_resume_at(
            cur, recipient=recipient, channel=channel, now=now)
        if resume_at is not None:
            cur.execute(
                """INSERT INTO approval_notification_deliveries
                   (todo_id, recipient, channel, address, subject, body, payload,
                    status, next_retry_at, delayed_until, ordinal, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?, 'delayed', NULL, ?, ?, ?, ?)""",
                (todo_id, recipient, channel, address, subject, body, cpayload,
                 resume_at, todo_id, now, now))
            notif_policy.audit_delayed(cur, delivery_id=cur.lastrowid, todo=todo,
                                       recipient=recipient, channel=channel,
                                       resume_at=resume_at, now=now)
            counts["created"] += 1
            counts["delayed"] += 1
            continue
        # 3) 常规：立即可发（ordinal=待办 id，静默放行后据此恢复原事件顺序）
        cur.execute(
            """INSERT INTO approval_notification_deliveries
               (todo_id, recipient, channel, address, subject, body, payload,
                status, next_retry_at, ordinal, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,'pending',NULL,?,?,?)""",
            (todo_id, recipient, channel, address, subject, body, cpayload,
             todo_id, now, now))
        counts["created"] += 1
        counts["immediate"] += 1
    return counts


def _create_todos(cur: sqlite3.Cursor, event_row: sqlite3.Row,
                  recipients: set[str], now: float) -> int:
    """为事件的每个接收人幂等生成站内待办 + 外发投递；返回新建待办数。

    站内待办始终即时逐条生成（聚合只合并外发、绝不吞掉可操作待办）；外发投递按
    聚合/静默规则路由（held/delayed/pending）。"""
    created = 0
    for recipient in sorted(recipients):
        try:
            cur.execute(
                """INSERT INTO approval_todos
                   (event_id, recipient, status, source_type, batch_id, change_id,
                    node_id, event_type, event_version, actionable, title, open_at,
                    created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (event_row["id"], recipient, TODO_UNREAD, event_row["source_type"],
                 event_row["batch_id"], event_row["change_id"], event_row["node_id"],
                 event_row["event_type"], event_row["state_version"],
                 event_row["actionable"], event_row["subject"], now, now, now))
        except sqlite3.IntegrityError:
            # UNIQUE(event_id, recipient)：重放/并发下该接收人已有待办，跳过
            continue
        todo_id = cur.lastrowid
        # 已发布通知通道路由版本时，该待办走版本化路由/熔断/故障转移链路（一个事件×
        # 接收人一条发送任务，跨通道不重复通知）；否则走旧的「按联系人通道逐条投递」。
        from . import notif_routing
        routed = notif_routing.enqueue_for_todo_tx(
            cur, todo=cur.execute("SELECT * FROM approval_todos WHERE id=?",
                                  (todo_id,)).fetchone(),
            event=event_row, settings=notif_routing._settings_at(cur), now=now)
        if routed["routed"]:
            audit.record(cur, "approval_todo_generated", None, None, {
                "todo_id": todo_id, "notify_event_id": event_row["id"],
                "event_type": event_row["event_type"],
                "source_type": event_row["source_type"],
                "source_batch_id": event_row["batch_id"],
                "change_id": event_row["change_id"], "node_id": event_row["node_id"],
                "recipient": recipient, "event_version": event_row["state_version"],
                "actionable": bool(event_row["actionable"]),
                "routed": True, "route_task_id": routed["task_id"],
                "route_plan": routed["plan"],
                "route_skipped_reason": routed["skipped_reason"]}, ts=now)
            created += 1
            continue
        routing = _insert_deliveries(
            cur, todo_id, recipient, event_row["subject"], event_row["body"],
            json.loads(event_row["payload"]), now)
        audit.record(cur, "approval_todo_generated", None, None, {
            "todo_id": todo_id, "notify_event_id": event_row["id"],
            "event_type": event_row["event_type"],
            "source_type": event_row["source_type"],
            "source_batch_id": event_row["batch_id"],
            "change_id": event_row["change_id"], "node_id": event_row["node_id"],
            "recipient": recipient, "event_version": event_row["state_version"],
            "actionable": bool(event_row["actionable"]),
            "deliveries_created": routing["created"],
            "deliveries_held": routing["held"],
            "deliveries_delayed": routing["delayed"]}, ts=now)
        created += 1
    return created


def _emit_tx(cur: sqlite3.Cursor, *, event_key: str, event_type: str,
             source_type: str, recipients: set[str], subject: str, body: str,
             batch_id: int | None = None, change_id: int | None = None,
             node_id: int | None = None, roles: list[str] | None = None,
             actionable: bool = False, payload: dict | None = None,
             now: float | None = None, audit_extra: dict | None = None) -> int:
    """在当前事务内发出一个审批通知事件：事件幂等落盘 + 生成待办/投递 + 审计。

    返回新建待办数（0 表示事件去重命中或无接收人）。
    """
    now = time.time() if now is None else now
    version = _bump_version(cur, source_type, batch_id, change_id)
    try:
        cur.execute(
            """INSERT INTO approval_notify_events
               (event_key, event_type, source_type, batch_id, change_id, node_id,
                roles, actionable, state_version, subject, body, payload, created_at)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            (event_key, event_type, source_type, batch_id, change_id, node_id,
             json.dumps(roles or [], ensure_ascii=False), 1 if actionable else 0,
             version, subject, body,
             json.dumps(payload or {}, ensure_ascii=False, sort_keys=True), now))
    except sqlite3.IntegrityError:
        # event_key 唯一：同一事件（重放/并发/worker 重复扫描）不再生成待办
        return 0
    event = cur.execute("SELECT * FROM approval_notify_events WHERE id=?",
                        (cur.lastrowid,)).fetchone()
    audit.record(cur, "approval_notify_event", None, None, {
        "notify_event_id": event["id"], "event_type": event_type,
        "event_key": event_key, "source_type": source_type,
        "source_batch_id": batch_id, "change_id": change_id,
        "node_id": node_id, "actionable": actionable,
        "state_version": version, "recipients": len(recipients),
        **(audit_extra or {})}, ts=now)
    return _create_todos(cur, event, recipients, now)


# ---- 事务内钩子（由 replay/delegation/gate 的业务事务调用） --------------------

def emit_node_activated_tx(cur, node, batch, now: float) -> int:
    """审批节点激活：给当前可承担该节点的人生成可操作待办。"""
    allowed = json.loads(node["allowed_roles"])
    recipients = _node_recipients(cur, node["id"], exclude={batch["operator"]})
    subject = f"[审批待办] 重放批次 #{batch['id']} 节点 {node['seq']+1} 待审批"
    body = (f"重放批次 #{batch['id']}（发起人 {batch['operator']}，"
            f"风险 {batch['risk_level']}）的审批节点 {node['seq']+1} 已激活，"
            f"允许角色：{','.join(allowed)}，截止时间 {node['deadline']}。"
            f"请在站内待办中处理。")
    return _emit_tx(
        cur, event_key=f"node:activated:{node['id']}", event_type="activated",
        source_type=SOURCE_BATCH, batch_id=batch["id"], node_id=node["id"],
        roles=allowed, actionable=True, recipients=recipients,
        subject=subject, body=body,
        payload={"batch_id": batch["id"], "node_id": node["id"],
                 "node_seq": node["seq"], "event": "activated",
                 "deadline": node["deadline"], "risk_level": batch["risk_level"],
                 "submitted_by": batch["operator"]}, now=now,
        audit_extra={"submitted_by": batch["operator"]})


def emit_node_vote_tx(cur, node, batch, vote_row: sqlite3.Row,
                      approved_count: int, required: int, now: float) -> int:
    """节点收到一票且法定人数未满：提醒同节点其他可审批人还差多少票。"""
    if approved_count >= required:
        return 0
    recipients = _node_recipients(
        cur, node["id"], exclude={batch["operator"], vote_row["voter"]})
    # 已在该节点投过有效票的人不再收「还需投票」提醒
    voted_rows = cur.execute(
        "SELECT voter FROM replay_node_votes WHERE node_id=? AND status='valid'",
        (node["id"],)).fetchall()
    recipients -= {r["voter"] for r in voted_rows}
    subject = (f"[审批动态] 重放批次 #{batch['id']} 节点 {node['seq']+1} "
               f"{approved_count}/{required} 已赞成")
    body = (f"节点 {node['seq']+1} 收到 {vote_row['voter']} 的赞成票"
            f"（承担角色 {vote_row['voter_role']}），当前 {approved_count}/{required}，"
            f"还差 {required - approved_count} 票。")
    return _emit_tx(
        cur, event_key=f"vote:{vote_row['id']}", event_type="vote_received",
        source_type=SOURCE_BATCH, batch_id=batch["id"], node_id=node["id"],
        roles=json.loads(node["allowed_roles"]), actionable=True,
        recipients=recipients, subject=subject, body=body,
        payload={"batch_id": batch["id"], "node_id": node["id"],
                 "event": "vote_received", "voter": vote_row["voter"],
                 "approved_count": approved_count, "required_approvals": required,
                 "missing": required - approved_count,
                 "deadline": node["deadline"]}, now=now,
        audit_extra={"voter": vote_row["voter"],
                     "approved_count": approved_count, "required": required})


def emit_node_rejected_tx(cur, node, batch, operator: str, reason: str,
                          now: float) -> int:
    """节点拒绝（批次随之终止）：告知发起人（纯告知）。"""
    recipients = {batch["operator"]} - {operator}
    subject = f"[审批结果] 重放批次 #{batch['id']} 已被拒绝"
    body = (f"审批节点 {node['seq']+1} 被 {operator} 拒绝，批次终止，"
            f"未执行任务已取消。原因：{reason}")
    return _emit_tx(
        cur, event_key=f"node:rejected:{node['id']}", event_type="node_rejected",
        source_type=SOURCE_BATCH, batch_id=batch["id"], node_id=node["id"],
        roles=json.loads(node["allowed_roles"]), actionable=False,
        recipients=recipients, subject=subject, body=body,
        payload={"batch_id": batch["id"], "node_id": node["id"],
                 "event": "node_rejected", "rejected_by": operator, "reason": reason},
        now=now, audit_extra={"rejected_by": operator})


def emit_batch_approved_tx(cur, batch, operator: str, now: float) -> int:
    """全部节点满足、批次放行：告知发起人（纯告知）。"""
    recipients = {batch["operator"]} - {operator}
    subject = f"[审批结果] 重放批次 #{batch['id']} 已批准放行"
    body = f"批次 #{batch['id']} 的全部审批节点已满足（最后批准 {operator}），开始执行。"
    return _emit_tx(
        cur, event_key=f"batch:approved:{batch['id']}", event_type="batch_approved",
        source_type=SOURCE_BATCH, batch_id=batch["id"], actionable=False,
        recipients=recipients, subject=subject, body=body,
        payload={"batch_id": batch["id"], "event": "batch_approved",
                 "approved_by": operator}, now=now,
        audit_extra={"approved_by": operator})


def emit_node_timeout_tx(cur, node, batch, now: float) -> int:
    """节点超时、批次释放：告知发起人（纯告知）。"""
    recipients = {batch["operator"]}
    subject = f"[审批超时] 重放批次 #{batch['id']} 审批超时已释放"
    body = (f"审批节点 {node['seq']+1} 超过截止时间 {node['deadline']} 仍未决定，"
            f"批次已取消，未执行任务已取消。")
    return _emit_tx(
        cur, event_key=f"node:timeout:{node['id']}", event_type="node_timeout",
        source_type=SOURCE_BATCH, batch_id=batch["id"], node_id=node["id"],
        roles=json.loads(node["allowed_roles"]), actionable=False,
        recipients=recipients, subject=subject, body=body,
        payload={"batch_id": batch["id"], "node_id": node["id"],
                 "event": "node_timeout", "deadline": node["deadline"]}, now=now)


def emit_batch_cancelled_tx(cur, batch, operator: str, note: str,
                            was_awaiting_approval: bool, now: float) -> int:
    """批次被撤回/取消：待决时告知当前可审批人（纯告知）；始终告知发起人非本人场景。"""
    recipients: set[str] = set()
    if was_awaiting_approval:
        active_nodes = cur.execute(
            "SELECT id FROM replay_approval_nodes WHERE batch_id=? AND status='active'",
            (batch["id"],)).fetchall()
        for n in active_nodes:
            recipients |= _node_recipients(cur, n["id"], exclude={batch["operator"]})
    recipients |= ({batch["operator"]} - {operator})
    if not recipients:
        return 0
    subject = f"[审批撤回] 重放批次 #{batch['id']} 已取消"
    body = f"批次 #{batch['id']} 被 {operator} 取消。备注：{note or '（无）'}"
    return _emit_tx(
        cur, event_key=f"batch:cancelled:{batch['id']}", event_type="batch_cancelled",
        source_type=SOURCE_BATCH, batch_id=batch["id"], actionable=False,
        recipients=recipients, subject=subject, body=body,
        payload={"batch_id": batch["id"], "event": "batch_cancelled",
                 "cancelled_by": operator, "note": note,
                 "was_awaiting_approval": was_awaiting_approval}, now=now,
        audit_extra={"cancelled_by": operator})


def emit_change_required_tx(cur, change_row, now: float) -> int:
    """策略变更单提交需要审批：全体联系人（除提交人）收到可操作待办。"""
    recipients = _change_recipients(cur, change_row["id"],
                                    exclude={change_row["operator"]})
    subject = f"[策略变更待审] 变更单 #{change_row['id']} 等待审批"
    body = (f"{change_row['operator']} 提交了策略变更（{change_row['change_type']}，"
            f"风险 {change_row['risk_class']}），截止 {change_row['expires_at']}，"
            f"请审批。")
    return _emit_tx(
        cur, event_key=f"change:required:{change_row['id']}",
        event_type="change_required", source_type=SOURCE_CHANGE,
        change_id=change_row["id"], actionable=True, recipients=recipients,
        subject=subject, body=body,
        payload={"change_id": change_row["id"], "event": "change_required",
                 "change_type": change_row["change_type"],
                 "risk_class": change_row["risk_class"],
                 "submitted_by": change_row["operator"],
                 "expires_at": change_row["expires_at"]}, now=now,
        audit_extra={"submitted_by": change_row["operator"],
                     "risk_class": change_row["risk_class"]})


def emit_change_decided_tx(cur, change_row, action: str, operator: str,
                           reason: str, now: float) -> int:
    """策略变更单被批准/拒绝/超时/执行：告知提交人（纯告知）。

    批准/拒绝人必与提交人不同，故排除决定人不影响；'applied' 由提交人本人执行，
    不排除操作人，否则提交人收不到自己变更的执行结果。
    """
    type_map = {"approve": ("change_approved", "已批准"),
                "reject": ("change_rejected", "已拒绝"),
                "expired": ("change_expired", "已超时"),
                "applied": ("change_applied", "已执行")}
    event_type, label = type_map[action]
    exclude = {operator} if action in ("approve", "reject") else set()
    recipients = {change_row["operator"]} - exclude
    if not recipients:
        return 0
    subject = f"[策略变更{label}] 变更单 #{change_row['id']} {label}"
    body = (f"策略变更单 #{change_row['id']}（{change_row['change_type']}）{label}"
            + (f"，操作人 {operator}" if operator else "")
            + (f"，原因：{reason}" if reason else ""))
    return _emit_tx(
        cur, event_key=f"change:{action}:{change_row['id']}", event_type=event_type,
        source_type=SOURCE_CHANGE, change_id=change_row["id"], actionable=False,
        recipients=recipients, subject=subject, body=body,
        payload={"change_id": change_row["id"], "event": event_type,
                 "operator": operator, "reason": reason or None}, now=now,
        audit_extra={"operator": operator})


# ---- 截止时间扫描（worker 调用） ----------------------------------------------

def emit_deadline_events(db: Database, now: float, lead_seconds: float) -> int:
    """为接近截止时间的活动节点与待决变更单各生成一个 deadline_approaching 事件
    （event_key 去重，重复扫描不会产生第二个待办）。"""
    created = 0
    due_nodes = db.query(
        """SELECT n.*, b.operator AS batch_operator, b.risk_level AS risk_level
           FROM replay_approval_nodes n
           JOIN replay_batches b ON b.id = n.batch_id
           WHERE n.status='active' AND n.deadline IS NOT NULL
             AND n.deadline <= ? AND n.deadline > ?
             AND b.status='pending_approval' AND b.approval_status='pending'""",
        (now + lead_seconds, now))
    due_changes = db.query(
        """SELECT * FROM replay_policy_changes
           WHERE status IN ('pending','approved') AND expires_at <= ?
             AND expires_at > ? AND requires_approval=1""",
        (now + lead_seconds, now))
    if not due_nodes and not due_changes:
        return 0
    with db.tx() as cur:
        for n in due_nodes:
            if cur.execute(
                    "SELECT 1 AS x FROM approval_notify_events "
                    "WHERE event_key=?", (f"deadline:node:{n['id']}",)).fetchone():
                continue
            recipients = _node_recipients(cur, n["id"], exclude={n["batch_operator"]})
            batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                                (n["batch_id"],)).fetchone()
            created += _emit_tx(
                cur, event_key=f"deadline:node:{n['id']}",
                event_type="deadline_approaching", source_type=SOURCE_BATCH,
                batch_id=n["batch_id"], node_id=n["id"],
                roles=json.loads(n["allowed_roles"]), actionable=True,
                recipients=recipients,
                subject=f"[审批即将到期] 重放批次 #{n['batch_id']} 节点 {n['seq']+1}",
                body=(f"审批节点 {n['seq']+1} 将于 {n['deadline']} 到期，"
                      f"请尽快处理，超时将取消整个批次。"),
                payload={"batch_id": n["batch_id"], "node_id": n["id"],
                         "event": "deadline_approaching",
                         "deadline": n["deadline"]}, now=now)
        for ch in due_changes:
            if cur.execute(
                    "SELECT 1 AS x FROM approval_notify_events "
                    "WHERE event_key=?", (f"deadline:change:{ch['id']}",)).fetchone():
                continue
            recipients = _change_recipients(cur, ch["id"], exclude={ch["operator"]})
            created += _emit_tx(
                cur, event_key=f"deadline:change:{ch['id']}",
                event_type="deadline_approaching", source_type=SOURCE_CHANGE,
                change_id=ch["id"], actionable=True, recipients=recipients,
                subject=f"[审批即将到期] 策略变更单 #{ch['id']}",
                body=f"策略变更单 #{ch['id']} 将于 {ch['expires_at']} 到期，请尽快审批。",
                payload={"change_id": ch["id"], "event": "deadline_approaching",
                         "expires_at": ch["expires_at"]}, now=now)
    return created


# ---- 待办对账：投票后/来源进入终态时关闭失效的可操作待办 ------------------------

def _batch_node_actionable(cur: sqlite3.Cursor, todo) -> bool:
    """该批次节点待办现在是否仍可操作：节点仍 active、批次仍待审批、
    接收人未在该节点持有效票。"""
    node = cur.execute(
        "SELECT status FROM replay_approval_nodes WHERE id=?",
        (todo["node_id"],)).fetchone()
    batch = cur.execute(
        "SELECT status, approval_status FROM replay_batches WHERE id=?",
        (todo["batch_id"],)).fetchone()
    if node is None or batch is None:
        return False
    if node["status"] != "active":
        return False
    if batch["status"] != "pending_approval" or batch["approval_status"] != "pending":
        return False
    voted = cur.execute(
        "SELECT 1 AS x FROM replay_node_votes WHERE node_id=? AND voter=? "
        "AND status='valid' LIMIT 1",
        (todo["node_id"], todo["recipient"])).fetchone()
    if voted is not None:
        return False
    return True


def _change_actionable(cur: sqlite3.Cursor, todo) -> bool:
    ch = cur.execute(
        "SELECT status, requires_approval, operator FROM replay_policy_changes "
        "WHERE id=?", (todo["change_id"],)).fetchone()
    if ch is None:
        return False
    if not ch["requires_approval"] or ch["status"] != "pending":
        return False
    return todo["recipient"] != ch["operator"]


def sweep_stale_todos(db: Database, now: float | None = None) -> int:
    """关闭已失效的可操作待办（重复投票、节点落定、批次/变更进入终态）。

    幂等：只作用于 unread/read 的可操作待办；未发出的投递同事务取消。
    纯告知待办保留（接收人自行已读）。
    """
    now = time.time() if now is None else now
    rows = db.query(
        """SELECT * FROM approval_todos
           WHERE actionable=1 AND status IN ('unread','read')""")
    closed = 0
    if not rows:
        return 0
    with db.tx() as cur:
        for todo in rows:
            actionable = (_batch_node_actionable(cur, todo)
                          if todo["source_type"] == SOURCE_BATCH
                          else _change_actionable(cur, todo))
            if actionable:
                continue
            if todo["source_type"] == SOURCE_CHANGE:
                ch = cur.execute("SELECT status FROM replay_policy_changes WHERE id=?",
                                 (todo["change_id"],)).fetchone()
                source_status = ch["status"] if ch else "missing"
                is_expired = source_status == "expired"
            else:
                node = cur.execute("SELECT status FROM replay_approval_nodes WHERE id=?",
                                   (todo["node_id"],)).fetchone()
                # 节点超时落 expired（批次随之取消）；拒绝/批准/撤回/已投票等落 cancelled
                source_status = node["status"] if node else "missing"
                is_expired = source_status == "expired"
            new_status = TODO_EXPIRED if is_expired else TODO_CANCELLED
            reason = ("source_expired" if is_expired else "source_no_longer_actionable")
            cur.execute(
                """UPDATE approval_todos SET status=?, close_reason=?, closed_at=?,
                   updated_at=? WHERE id=? AND status IN ('unread','read')""",
                (new_status, reason, now, now, todo["id"]))
            cur.execute(
                """UPDATE approval_notification_deliveries SET status='cancelled',
                   updated_at=? WHERE todo_id=? AND status IN ('pending','failed','held','delayed')""",
                (now, todo["id"]))
            # 路由发送任务（若该待办走版本化路由链路）同事务取消
            from . import notif_routing
            notif_routing.cancel_tasks_for_todo_tx(
                cur, todo["id"], "todo_closed", now)
            # 来源落定：升级随之停止（后续级别不再触发，已升级别未发出投递取消）
            from . import notif_policy
            notif_policy.stop_escalations_for_todo_tx(
                cur, todo["id"], reason="source_closed", now=now)
            audit.record(cur, "approval_todo_closed", None, None, {
                "todo_id": todo["id"], "recipient": todo["recipient"],
                "event_type": todo["event_type"],
                "source_type": todo["source_type"],
                "source_batch_id": todo["batch_id"],
                "change_id": todo["change_id"], "node_id": todo["node_id"],
                "new_status": new_status, "reason": reason,
                "source_status": source_status}, ts=now)
            closed += 1
    return closed


# ---- 待办处理：确认已读 / 回写原审批动作 ----------------------------------------

def _get_todo_or_404(cur: sqlite3.Cursor, todo_id: int) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                      (todo_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "approval todo not found")
    return row


def mark_read(db: Database, todo_id: int, operator: str) -> dict:
    """确认已读：幂等（已读/已处理重复确认不产生副作用）；非本人待办 403。"""
    operator = _require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        todo = _get_todo_or_404(cur, todo_id)
        if todo["recipient"] != operator:
            raise HTTPException(403, "todo belongs to another recipient")
        if todo["status"] == TODO_UNREAD:
            cur.execute(
                "UPDATE approval_todos SET status='read', read_at=?, updated_at=? "
                "WHERE id=? AND status='unread'",
                (now, now, todo_id))
            audit.record(cur, "approval_todo_read", None, None, {
                "todo_id": todo_id, "recipient": operator,
                "event_type": todo["event_type"],
                "source_type": todo["source_type"],
                "source_batch_id": todo["batch_id"],
                "change_id": todo["change_id"]}, ts=now)
        todo = _get_todo_or_404(cur, todo_id)
        return _todo_view(cur, todo, now)


def act_on_todo(db: Database, todo_id: int, req: TodoActRequest) -> dict:
    """处理待办并回写原审批动作。

    同一写事务内完成：归属/可操作性门禁 -> 认领（unread/read 条件更新）->
    调用原审批决定（节点 decide_node_tx / 变更单 decide_change_tx，其全部角色、
    委托、职责分离、状态守卫仍然生效）-> 关闭待办。任一校验失败整体回滚：
    重复点击（已 handled/expired/cancelled）、过期待办、并发处理都只会得到 4xx，
    不会重复投票或越过审批门禁。
    """
    operator = _require(req.operator, "operator")
    action = (req.action or "").strip()
    reason = (req.reason or "").strip()
    if action in ("reject", "skip") and not reason:
        raise HTTPException(422, f"reason must be non-empty when handling with {action}")
    now = time.time()
    with db.tx() as cur:
        todo = _get_todo_or_404(cur, todo_id)
        if todo["recipient"] != operator:
            raise HTTPException(403, "todo belongs to another recipient")
        if not todo["actionable"]:
            raise HTTPException(409, "this todo is informational and cannot be acted on")
        if todo["status"] == TODO_HANDLED:
            raise HTTPException(409, "todo already handled")
        if todo["status"] in (TODO_EXPIRED, TODO_CANCELLED):
            raise HTTPException(409, f"todo is {todo['status']} and cannot be handled")
        # 来源门禁（与 worker 对账同一判定）：过期/已投票/来源不再待决一律挡下
        still_actionable = (_batch_node_actionable(cur, todo)
                            if todo["source_type"] == SOURCE_BATCH
                            else _change_actionable(cur, todo))
        if not still_actionable:
            # 惰性对账：把已失效待办落到终态（与 sweep 同口径），再返回 409
            if todo["source_type"] == SOURCE_CHANGE:
                ch = cur.execute("SELECT status FROM replay_policy_changes WHERE id=?",
                                 (todo["change_id"],)).fetchone()
                expired = ch is not None and ch["status"] == "expired"
            else:
                node = cur.execute("SELECT status FROM replay_approval_nodes WHERE id=?",
                                   (todo["node_id"],)).fetchone()
                expired = node is not None and node["status"] == "expired"
            new_status = TODO_EXPIRED if expired else TODO_CANCELLED
            cur.execute(
                """UPDATE approval_todos SET status=?, close_reason='stale_at_act',
                   closed_at=?, updated_at=? WHERE id=? AND status IN ('unread','read')""",
                (new_status, now, now, todo_id))
            cur.execute(
                """UPDATE approval_notification_deliveries SET status='cancelled',
                   updated_at=? WHERE todo_id=? AND status IN ('pending','failed','held','delayed')""",
                (now, todo_id))
            from . import notif_routing
            notif_routing.cancel_tasks_for_todo_tx(
                cur, todo_id, "todo_closed", now)
            from . import notif_policy
            notif_policy.stop_escalations_for_todo_tx(
                cur, todo_id, reason="source_closed", now=now)
            audit.record(cur, "approval_todo_closed", None, None, {
                "todo_id": todo_id, "recipient": operator,
                "event_type": todo["event_type"],
                "source_type": todo["source_type"],
                "source_batch_id": todo["batch_id"],
                "change_id": todo["change_id"], "node_id": todo["node_id"],
                "new_status": new_status, "reason": "stale_at_act"}, ts=now)
            raise HTTPException(409, f"approval source is no longer awaiting decision; "
                                    f"todo marked {new_status}")

        # 回写原审批动作（导入放在函数内，避免 notifications <-> replay/gate 循环导入）
        if todo["source_type"] == SOURCE_BATCH:
            if action not in BATCH_ACTIONS:
                raise HTTPException(422, f"invalid action {action!r} for a batch node "
                                         f"(expect one of {','.join(BATCH_ACTIONS)})")
            from .replay import decide_node_tx
            result = decide_node_tx(
                cur, db, todo["batch_id"], todo["node_id"], action, operator,
                role=req.role, delegation_id=req.delegation_id,
                reason=reason, note=req.note, now=now)
        else:
            if action not in CHANGE_ACTIONS:
                raise HTTPException(422, f"invalid action {action!r} for a policy change "
                                         f"(expect one of {','.join(CHANGE_ACTIONS)})")
            from .replay_policy_gate import decide_change_tx
            result = decide_change_tx(
                cur, todo["change_id"], action, operator,
                reason=reason, note=req.note, now=now)

        # 认领并关闭待办（条件更新兜底并发：两个并发处理最多一个成功）
        claimed = cur.execute(
            """UPDATE approval_todos SET status='handled', handled_at=?, handled_by=?,
               handle_action=?, close_reason='handled', closed_at=?, read_at=COALESCE(read_at,?),
               updated_at=?
               WHERE id=? AND status IN ('unread','read')""",
            (now, operator, action, now, now, now, todo_id)).rowcount
        if not claimed:
            # 理论上不可达（同一写事务已串行化），保守处理：回滚让原决定不生效
            raise HTTPException(409, "todo was concurrently handled")
        cur.execute(
            """UPDATE approval_notification_deliveries SET status='cancelled',
               updated_at=? WHERE todo_id=? AND status IN ('pending','failed','held','delayed')""",
            (now, todo_id))
        # 该待办的版本化路由发送任务同事务取消（已发出的外部效果轨迹保留）
        from . import notif_routing
        notif_routing.cancel_tasks_for_todo_tx(cur, todo_id, "handled", now)
        # 原接收人处理后升级通知必须停止：后续级别不再触发，已升级别未发出的投递取消
        # （同事务，与原决定原子提交）。
        from . import notif_policy
        notif_policy.stop_escalations_for_todo_tx(
            cur, todo_id, reason="handled", now=now)
        audit.record(cur, "approval_todo_handled", None, None, {
            "todo_id": todo_id, "recipient": operator, "action": action,
            "event_type": todo["event_type"],
            "source_type": todo["source_type"],
            "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
            "node_id": todo["node_id"],
            "role": req.role, "delegation_id": req.delegation_id}, ts=now)
        audit.record(cur, "approval_action_written_back", None, None, {
            "todo_id": todo_id, "operator": operator, "action": action,
            "source_type": todo["source_type"],
            "source_batch_id": todo["batch_id"], "change_id": todo["change_id"],
            "node_id": todo["node_id"], "result": result.get("result")}, ts=now)
        view = _todo_view(cur, _get_todo_or_404(cur, todo_id), now)
        return {"result": "handled", "todo": view, "decision": result}


# ---- 查询 --------------------------------------------------------------------

def _todo_view(cur: sqlite3.Cursor, todo, now: float) -> dict:
    deliveries = cur.execute(
        "SELECT * FROM approval_notification_deliveries WHERE todo_id=? ORDER BY id",
        (todo["id"],)).fetchall()
    failed = [d["channel"] for d in deliveries if d["status"] == DELIVERY_FAILED]
    quarantined = [d["channel"] for d in deliveries
                   if d["status"] == DELIVERY_QUARANTINED]
    held = [{"channel": d["channel"], "group_id": d["group_id"]}
            for d in deliveries if d["status"] == DELIVERY_HELD]
    delayed = [{"channel": d["channel"], "delayed_until": d["delayed_until"]}
               for d in deliveries if d["status"] == DELIVERY_DELAYED]
    aggregated = [d["channel"] for d in deliveries
                  if d["status"] == DELIVERY_AGGREGATED]
    event = cur.execute("SELECT subject, body, event_type, state_version FROM "
                        "approval_notify_events WHERE id=?",
                        (todo["event_id"],)).fetchone()
    return {
        "id": todo["id"], "recipient": todo["recipient"],
        "status": todo["status"],
        "source_type": todo["source_type"],
        "batch_id": todo["batch_id"], "change_id": todo["change_id"],
        "node_id": todo["node_id"],
        "event_type": todo["event_type"],
        "event_version": todo["event_version"],
        "current_event_version": event["state_version"] if event else None,
        "actionable": bool(todo["actionable"]),
        "title": todo["title"], "subject": event["subject"] if event else todo["title"],
        "read_at": todo["read_at"], "handled_at": todo["handled_at"],
        "handled_by": todo["handled_by"], "handle_action": todo["handle_action"],
        "close_reason": todo["close_reason"], "closed_at": todo["closed_at"],
        "created_at": todo["created_at"], "updated_at": todo["updated_at"],
        "deliveries": [{"id": d["id"], "channel": d["channel"], "address": d["address"],
                        "status": d["status"], "attempts": d["attempts"],
                        "next_retry_at": d["next_retry_at"],
                        "last_error": d["last_error"], "sent_at": d["sent_at"],
                        "group_id": d["group_id"],
                        "delayed_until": d["delayed_until"],
                        "ordinal": d["ordinal"]}
                       for d in deliveries],
        "failed_channels": failed, "quarantined_channels": quarantined,
        "held_channels": held, "delayed_channels": delayed,
        "aggregated_channels": aggregated,
        # 版本化路由链路（发布过路由版本后）：该待办的发送任务、尝试与切换
        "route_task": _route_task_view(cur, todo["id"]),
        # 该待办作为「原始接收人待办」时的升级状态（未升级为 None）
        "escalation": _escalation_view(cur, todo["id"]),
    }


def _route_task_view(cur: sqlite3.Cursor, todo_id: int) -> dict | None:
    """待办内嵌的版本化路由发送任务（含尝试与切换历史）；未走路由链路时为 None。"""
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE todo_id=?",
                       (todo_id,)).fetchone()
    if task is None:
        return None
    attempts = cur.execute(
        "SELECT * FROM notif_send_attempts WHERE task_id=? ORDER BY id",
        (task["id"],)).fetchall()
    switches = cur.execute(
        "SELECT * FROM notif_channel_switches WHERE task_id=? ORDER BY id",
        (task["id"],)).fetchall()
    return {
        "id": task["id"], "status": task["status"],
        "route_version": task["route_version"],
        "plan": [p["channel"] for p in json.loads(task["plan_json"])],
        "attempt_index": task["attempt_index"],
        "current_channel": task["current_channel"],
        "sent_channel": task["sent_channel"], "sent_at": task["sent_at"],
        "total_attempts": task["total_attempts"], "round": task["round"],
        "next_retry_at": task["next_retry_at"], "last_error": task["last_error"],
        "cancelled_reason": task["cancelled_reason"],
        "attempts": [{"channel": a["channel"], "result": a["result"],
                      "probe": bool(a["probe"]), "duration": a["duration"],
                      "error": a["error"], "created_at": a["created_at"]}
                     for a in attempts],
        "switches": [{"from_channel": s["from_channel"],
                      "to_channel": s["to_channel"], "reason": s["reason"],
                      "created_at": s["created_at"]} for s in switches]}


def _escalation_view(cur: sqlite3.Cursor, todo_id: int) -> dict | None:
    rows = cur.execute(
        "SELECT * FROM approval_escalations WHERE base_todo_id=? ORDER BY policy_id, id",
        (todo_id,)).fetchall()
    if not rows:
        return None
    out = []
    for esc in rows:
        levels = cur.execute(
            "SELECT * FROM approval_escalation_levels WHERE escalation_id=? "
            "ORDER BY level", (esc["id"],)).fetchall()
        out.append({
            "escalation_id": esc["id"], "policy_id": esc["policy_id"],
            "status": esc["status"], "levels_total": esc["levels_total"],
            "levels_fired": esc["levels_fired"], "stop_reason": esc["stop_reason"],
            "stopped_at": esc["stopped_at"],
            "fired_levels": [{"level": lv["level"],
                              "recipients": json.loads(lv["recipients"]),
                              "fired_at": lv["fired_at"],
                              "notify_event_id": lv["notify_event_id"]}
                             for lv in levels]})
    return out


def _filter_todos(db: Database, *, recipient: str | None = None,
                  source_type: str | None = None, batch_id: int | None = None,
                  change_id: int | None = None, node_id: int | None = None,
                  status: str | None = None, limit: int = 100) -> list:
    sql, params = "SELECT * FROM approval_todos WHERE 1=1", []
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if source_type:
        sql += " AND source_type=?"
        params.append(source_type)
    if batch_id is not None:
        sql += " AND batch_id=?"
        params.append(batch_id)
    if change_id is not None:
        sql += " AND change_id=?"
        params.append(change_id)
    if node_id is not None:
        sql += " AND node_id=?"
        params.append(node_id)
    if status:
        sql += " AND status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    return db.query(sql, tuple(params))


def list_todos(db: Database, *, recipient: str | None = None,
               source_type: str | None = None, source: str | None = None,
               batch_id: int | None = None, change_id: int | None = None,
               node_id: int | None = None, status: str | None = None,
               limit: int = 100, now: float | None = None) -> dict:
    """待办查询：可按接收人、来源（batch/change 或 batch:{id}/change:{id}）、
    来源 id、节点、状态过滤。"""
    now = time.time() if now is None else now
    if status is not None and status not in TODO_STATUSES:
        raise HTTPException(422, f"invalid status: {status!r} "
                                 f"(expect one of {','.join(TODO_STATUSES)})")
    if source_type is not None and source_type not in (SOURCE_BATCH, SOURCE_CHANGE):
        raise HTTPException(422, "source_type must be 'batch' or 'change'")
    if source is not None:
        if ":" in source:
            kind, _, sid = source.partition(":")
            try:
                source_id = int(sid)
            except ValueError:
                raise HTTPException(
                    422, f"invalid source id {sid!r}: source must be batch:{{id}} "
                         "or change:{id} with an integer id")
            if kind == SOURCE_BATCH:
                source_type, batch_id = SOURCE_BATCH, source_id
            elif kind == SOURCE_CHANGE:
                source_type, change_id = SOURCE_CHANGE, source_id
            else:
                raise HTTPException(422, "source must be batch:{id} or change:{id}")
        else:
            source_type = source
    rows = _filter_todos(db, recipient=recipient, source_type=source_type,
                         batch_id=batch_id, change_id=change_id, node_id=node_id,
                         status=status, limit=limit)
    with db.tx() as cur:  # 视图与计数同一快照
        todos = [_todo_view(cur, r, now) for r in rows]
    return {"todos": todos, "count": len(todos)}


def get_todo(db: Database, todo_id: int) -> dict:
    with db.tx() as cur:
        return _todo_view(cur, _get_todo_or_404(cur, todo_id), time.time())


def todo_summary(db: Database, recipient: str | None = None) -> dict:
    """待办看板：未读、已读、已处理、过期（失败）、取消（隔离）数量，
    以及外发失败/隔离数量；可按接收人过滤。"""
    where, params = "", []
    if recipient:
        where, params = " WHERE recipient=?", [recipient]
    counts = {r["status"]: r["c"] for r in db.query(
        f"SELECT status, COUNT(*) AS c FROM approval_todos{where} GROUP BY status",
        tuple(params))}
    djoin = "JOIN approval_todos t ON t.id = d.todo_id"
    dwhere, dparams = [], []
    if recipient:
        dwhere.append("t.recipient=?")
        dparams.append(recipient)
    dsql = (f"SELECT d.status AS status, COUNT(*) AS c FROM "
            f"approval_notification_deliveries d {djoin}")
    if dwhere:
        dsql += " WHERE " + " AND ".join(dwhere)
    dsql += " GROUP BY d.status"
    delivery_counts = {r["status"]: r["c"] for r in db.query(dsql, tuple(dparams))}
    # 版本化路由发送任务计数（可按接收人过滤）
    rsql = "SELECT status, COUNT(*) AS c FROM notif_send_tasks"
    rwhere, rparams = [], []
    if recipient:
        rwhere.append("recipient=?")
        rparams.append(recipient)
    if rwhere:
        rsql += " WHERE " + " AND ".join(rwhere)
    rsql += " GROUP BY status"
    route_counts = {r["status"]: r["c"] for r in db.query(rsql, tuple(rparams))}
    return {
        "recipient": recipient,
        "unread": counts.get(TODO_UNREAD, 0),
        "read": counts.get(TODO_READ, 0),
        "handled": counts.get(TODO_HANDLED, 0),
        "expired": counts.get(TODO_EXPIRED, 0),   # 过期待办数量
        "cancelled": counts.get(TODO_CANCELLED, 0),
        "delivery_pending": delivery_counts.get(DELIVERY_PENDING, 0),
        "delivery_failed": delivery_counts.get(DELIVERY_FAILED, 0),
        "delivery_quarantined": delivery_counts.get(DELIVERY_QUARANTINED, 0),
        # 聚合窗口内暂缓 / 已被摘要替代 / 静默时段延迟
        "delivery_held": delivery_counts.get(DELIVERY_HELD, 0),
        "delivery_aggregated": delivery_counts.get(DELIVERY_AGGREGATED, 0),
        "delivery_delayed": delivery_counts.get(DELIVERY_DELAYED, 0),
        "delivery_sent": delivery_counts.get(DELIVERY_SENT, 0),
        # 版本化路由链路（发布过路由版本后才有）：待发/在途/已发/隔离/取消
        "route_pending": route_counts.get("pending", 0),
        "route_in_flight": route_counts.get("in_flight", 0),
        "route_sent": route_counts.get("sent", 0),
        "route_quarantined": route_counts.get("quarantined", 0),
        "route_cancelled": route_counts.get("cancelled", 0),
    }


# ---- 联系人目录 ---------------------------------------------------------------

def _contact_view(row) -> dict:
    return {"name": row["name"], "email": row["email"], "webhook_url": row["webhook_url"],
            "channels": json.loads(row["channels"]), "active": bool(row["active"]),
            "created_by": row["created_by"], "created_at": row["created_at"],
            "updated_at": row["updated_at"], "deactivated_at": row["deactivated_at"],
            "deactivated_by": row["deactivated_by"]}


def _backfill_for_contact(cur, name: str, now: float) -> int:
    """联系人注册/重新启用后，为其仍可操作的历史事件补发待办
    （注册晚于通知生成也不会漏掉审批；纯告知事件不补发）。"""
    events = cur.execute(
        """SELECT e.* FROM approval_notify_events e
           WHERE e.actionable=1 ORDER BY e.id""").fetchall()
    created = 0
    for event in events:
        eligible = False
        if event["event_type"] == "escalated":
            # 升级事件：接收人是级别快照中的成员，且来源仍可操作（晚注册不漏关键提醒）
            if event["source_type"] == SOURCE_BATCH and event["node_id"] is not None:
                source_open = _batch_node_actionable(cur, _pseudo_todo(event, name))
            else:
                source_open = _change_actionable(cur, _pseudo_todo(event, name))
            if source_open:
                lv = cur.execute(
                    """SELECT recipients FROM approval_escalation_levels
                       WHERE notify_event_id=?""", (event["id"],)).fetchone()
                if lv is not None and name in json.loads(lv["recipients"]):
                    eligible = True
        elif event["source_type"] == SOURCE_BATCH and event["node_id"] is not None:
            if _batch_node_actionable(cur, _pseudo_todo(event, name)):
                # 还须是该事件节点当前允许的承担人（角色/委托与激活时同口径）
                recipients = _node_recipients(cur, event["node_id"], exclude=set())
                eligible = name in recipients
        elif event["source_type"] == SOURCE_CHANGE:
            ch = cur.execute("SELECT * FROM replay_policy_changes WHERE id=?",
                             (event["change_id"],)).fetchone()
            if ch is not None and _change_actionable(
                    cur, _pseudo_todo(event, name)):
                eligible = name != ch["operator"]
        if not eligible:
            continue
        created += _create_todos(cur, event, {name}, now)
    return created


def _pseudo_todo(event, name: str) -> dict:
    """_batch_node_actionable/_change_actionable 只读取 todo 的少数字段，
    用轻量字典代替真实待办行（这两个函数统一用下标访问）。"""
    return {"id": None, "recipient": name, "source_type": event["source_type"],
            "batch_id": event["batch_id"], "change_id": event["change_id"],
            "node_id": event["node_id"]}


def upsert_contact(db: Database, req: ContactUpsertRequest) -> dict:
    """注册/更新联系人；重新启用或补齐通道后，为仍可操作的历史事件补发待办。"""
    name = _require(req.name, "name")
    channels = sorted(set(req.channels or []))
    bad = [c for c in channels if c not in SUPPORTED_CHANNELS]
    if bad:
        raise HTTPException(422, f"unsupported channels: {','.join(bad)} "
                                 f"(support {','.join(SUPPORTED_CHANNELS)})")
    email = (req.email or "").strip() or None
    webhook = (req.webhook_url or "").strip() or None
    if CHANNEL_EMAIL in channels and not email:
        raise HTTPException(422, "email is required when the email channel is enabled")
    if CHANNEL_WEBHOOK in channels and not webhook:
        raise HTTPException(422, "webhook_url is required when the webhook channel is enabled")
    now = time.time()
    with db.tx() as cur:
        existing = cur.execute("SELECT * FROM approval_contacts WHERE name=?",
                               (name,)).fetchone()
        backfilled = 0
        if existing is None:
            cur.execute(
                """INSERT INTO approval_contacts
                   (name, email, webhook_url, channels, active, created_by,
                    created_at, updated_at)
                   VALUES (?,?,?,?,'1',?,?,?)""",
                (name, email, webhook, json.dumps(channels), req.operator, now, now))
            audit.record(cur, "approval_contact_upserted", None, None,
                         {"name": name, "operator": req.operator,
                          "channels": channels, "reactivated": False}, ts=now)
        else:
            cur.execute(
                """UPDATE approval_contacts SET email=?, webhook_url=?, channels=?,
                   active='1', deactivated_at=NULL, deactivated_by=NULL, updated_at=?
                   WHERE name=?""",
                (email, webhook, json.dumps(channels), now, name))
            audit.record(cur, "approval_contact_upserted", None, None,
                         {"name": name, "operator": req.operator,
                          "channels": channels,
                          "reactivated": not existing["active"]}, ts=now)
        # 为仍可操作的历史审批事件补发站内待办与投递
        backfilled = _backfill_for_contact(cur, name, now)
        row = cur.execute("SELECT * FROM approval_contacts WHERE name=?",
                          (name,)).fetchone()
        out = _contact_view(row)
        out["backfilled_todos"] = backfilled
        return out


def deactivate_contact(db: Database, name: str, req: ContactDeactivateRequest) -> dict:
    """停用联系人：不再接收新通知；其未处理的可操作待办关闭（保留历史可查）。"""
    operator = _require(req.operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM approval_contacts WHERE name=?",
                          (name,)).fetchone()
        if row is None:
            raise HTTPException(404, "contact not found")
        if not row["active"]:
            raise HTTPException(409, "contact already deactivated")
        cur.execute(
            """UPDATE approval_contacts SET active='0', deactivated_at=?,
               deactivated_by=?, updated_at=? WHERE name=?""",
            (now, operator, now, name))
        open_todos = cur.execute(
            """SELECT id FROM approval_todos
               WHERE recipient=? AND status IN ('unread','read')""", (name,)).fetchall()
        from . import notif_policy
        for t in open_todos:
            cur.execute(
                """UPDATE approval_todos SET status='cancelled',
                   close_reason='contact_deactivated', closed_at=?, updated_at=?
                   WHERE id=?""", (now, now, t["id"]))
            cur.execute(
                """UPDATE approval_notification_deliveries SET status='cancelled',
                   updated_at=? WHERE todo_id=? AND status IN ('pending','failed','held','delayed')""",
                (now, t["id"]))
            # 版本化路由发送任务一并取消
            from . import notif_routing
            notif_routing.cancel_tasks_for_todo_tx(
                cur, t["id"], "contact_deactivated", now)
            # 联系人停用：其原始待办的升级链一并停止
            notif_policy.stop_escalations_for_todo_tx(
                cur, t["id"], reason="source_closed", now=now)
        audit.record(cur, "approval_contact_deactivated", None, None,
                     {"name": name, "operator": operator, "reason": req.reason,
                      "closed_todos": len(open_todos)}, ts=now)
        return {"result": "deactivated", "name": name,
                "closed_todos": len(open_todos)}


def list_contacts(db: Database, active: bool | None = None) -> dict:
    sql, params = "SELECT * FROM approval_contacts", []
    if active is not None:
        sql += " WHERE active=?"
        params.append(1 if active else 0)
    sql += " ORDER BY name"
    return {"contacts": [_contact_view(r) for r in db.query(sql, tuple(params))]}


# ---- 外发投递重试 / 隔离（供 NotificationWorker 与管理端点调用） ----------------

def backoff_delay(settings: Settings, attempts: int) -> float:
    return min(settings.notif_retry_base_seconds * (2 ** (attempts - 1)),
               settings.notif_retry_cap_seconds)


def send_delivery(db: Database, delivery_id: int, senders, settings: Settings,
                  now: float | None = None) -> str:
    """尝试发送一条投递。成功 sent；失败累计 attempts，按指数退避排下次重试，
    超过 NOTIF_MAX_ATTEMPTS 进 quarantined。返回投递后的状态。

    senders: {"email": callable(addr, subject, body),
              "webhook": callable(addr, payload:dict)}，异常即视为发送失败。
    """
    now = time.time() if now is None else now
    with db.tx() as cur:
        row = cur.execute(
            "SELECT * FROM approval_notification_deliveries WHERE id=?",
            (delivery_id,)).fetchone()
        if row is None or row["status"] not in (DELIVERY_PENDING, DELIVERY_FAILED):
            return row["status"] if row else "missing"
        todo = cur.execute("SELECT * FROM approval_todos WHERE id=?",
                           (row["todo_id"],)).fetchone()
        if todo is None or todo["status"] not in OPEN_TODO_STATUSES:
            cur.execute(
                """UPDATE approval_notification_deliveries SET status='cancelled',
                   updated_at=? WHERE id=? AND status IN ('pending','failed','held','delayed')""",
                (now, delivery_id))
            audit.record(cur, "approval_delivery_cancelled", None, None,
                         {"delivery_id": delivery_id, "todo_id": row["todo_id"],
                          "channel": row["channel"], "reason": "todo_closed"}, ts=now)
            return DELIVERY_CANCELLED
    # 通道调用放在事务外（不可控的 IO 不得持有写锁）
    sender = senders.get(row["channel"])
    try:
        if sender is None:
            raise RuntimeError(f"no sender configured for channel {row['channel']!r}")
        if row["channel"] == CHANNEL_EMAIL:
            provider_message_id = sender(row["address"], row["subject"], row["body"])
        else:
            provider_message_id = sender(row["address"], json.loads(row["payload"] or "{}"))
    except Exception as exc:  # noqa: BLE001 - 任何外发失败都走重试/隔离
        attempts = row["attempts"] + 1
        with db.tx() as cur:
            if attempts >= settings.notif_max_attempts:
                new_status = DELIVERY_QUARANTINED
                next_retry = None
            else:
                new_status = DELIVERY_FAILED
                next_retry = now + backoff_delay(settings, attempts)
            cur.execute(
                """UPDATE approval_notification_deliveries
                   SET attempts=?, status=?, next_retry_at=?, last_error=?, updated_at=?
                   WHERE id=?""",
                (attempts, new_status, next_retry, str(exc), now, delivery_id))
            audit.record(cur,
                         "approval_delivery_quarantined"
                         if new_status == DELIVERY_QUARANTINED
                         else "approval_delivery_retry_scheduled",
                         None, None, {
                             "delivery_id": delivery_id, "todo_id": row["todo_id"],
                             "recipient": row["recipient"],
                             "channel": row["channel"], "attempts": attempts,
                             "next_retry_at": next_retry,
                             "delay_seconds": (next_retry - now
                                               if next_retry is not None else None),
                             "max_attempts": settings.notif_max_attempts,
                             "error": str(exc)}, ts=now)
        return new_status
    with db.tx() as cur:
        cur.execute(
            """UPDATE approval_notification_deliveries
               SET status='sent', attempts=attempts+1, sent_at=?, next_retry_at=NULL,
                   last_error=NULL, updated_at=? WHERE id=?
               AND status IN ('pending','failed')""",
            (now, now, delivery_id))
        # 登记外部服务返回的 message_id（送达确认锚点；无真实返回时本地占位）
        external_pk = receipts.register_external_message_tx(
            cur, channel=row["channel"],
            message_id=provider_message_id
            if isinstance(provider_message_id, str) and provider_message_id.strip()
            else None,
            id_source="provider" if isinstance(provider_message_id, str)
            and provider_message_id.strip() else "local",
            source=receipts.SOURCE_LEGACY, delivery_id=delivery_id,
            recipient=row["recipient"], address=row["address"],
            event_id=None, event_type=None, ts=now)
        cur.execute(
            "UPDATE approval_notification_deliveries SET external_message_id=? "
            "WHERE id=?", (external_pk, delivery_id))
        audit.record(cur, "approval_delivery_sent", None, None, {
            "delivery_id": delivery_id, "todo_id": row["todo_id"],
            "recipient": row["recipient"], "channel": row["channel"],
            "attempts": row["attempts"] + 1,
            "external_message_pk": external_pk}, ts=now)
    return DELIVERY_SENT


def dispatch_due(db: Database, senders, settings: Settings,
                 now: float | None = None, limit: int = 100) -> int:
    """发送所有到期的 pending/failed 投递（隔离/取消/暂缓的不发）。

    按 (ordinal, id) 排序：静默时段结束批量放行时，按原事件顺序发送。"""
    now = time.time() if now is None else now
    rows = db.query(
        """SELECT * FROM approval_notification_deliveries
           WHERE status IN ('pending','failed')
             AND (next_retry_at IS NULL OR next_retry_at <= ?)
           ORDER BY ordinal, id LIMIT ?""", (now, limit))
    for row in rows:
        send_delivery(db, row["id"], senders, settings, now)
    return len(rows)


def requeue_delivery(db: Database, delivery_id: int, operator: str) -> dict:
    """人工把隔离的外发投递重新放回发送队列（重置次数与退避）。"""
    operator = _require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM approval_notification_deliveries WHERE id=?",
                          (delivery_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "notification delivery not found")
        if row["status"] != DELIVERY_QUARANTINED:
            raise HTTPException(409, f"delivery is {row['status']}, not quarantined")
        cur.execute(
            """UPDATE approval_notification_deliveries
               SET status='pending', attempts=0, next_retry_at=NULL, last_error=NULL,
                   updated_at=? WHERE id=?""", (now, delivery_id))
        audit.record(cur, "approval_delivery_requeued", None, None,
                     {"delivery_id": delivery_id, "todo_id": row["todo_id"],
                      "recipient": row["recipient"], "channel": row["channel"],
                      "operator": operator}, ts=now)
        return {"result": "requeued", "delivery_id": delivery_id,
                "channel": row["channel"], "recipient": row["recipient"]}


def list_deliveries(db: Database, *, status_filter: str | None = None,
                    recipient: str | None = None, channel: str | None = None,
                    limit: int = 100) -> dict:
    sql, params = "SELECT * FROM approval_notification_deliveries WHERE 1=1", []
    if status_filter:
        sql += " AND status=?"
        params.append(status_filter)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if channel:
        sql += " AND channel=?"
        params.append(channel)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"deliveries": [{k: r[k] for k in r.keys()} for r in rows]}


# ---- 委托：创建/重新激活时为仍可操作的节点事件补发待办 --------------------------

def backfill_for_delegatee(cur, delegatee: str, now: float) -> int:
    """受托人拿到/恢复有效委托后，为其当前可承担、仍 active 的节点上尚未处理的
    可操作事件补发待办（激活/投票/到期提醒；同一事件同一人仍只生成一条）。

    在 delegation 创建/重新激活的业务事务内调用。
    """
    contact = cur.execute(
        "SELECT 1 AS x FROM approval_contacts WHERE name=? AND active=1",
        (delegatee,)).fetchone()
    if contact is None:
        return 0
    events = cur.execute(
        """SELECT e.* FROM approval_notify_events e
           WHERE e.source_type='batch' AND e.node_id IS NOT NULL
                 AND e.actionable=1
           ORDER BY e.id""").fetchall()
    created = 0
    for event in events:
        todo = _pseudo_todo(event, delegatee)
        if not _batch_node_actionable(cur, todo):
            continue
        recipients = _node_recipients(cur, event["node_id"], exclude=set())
        if delegatee not in recipients:
            continue
        created += _create_todos(cur, event, {delegatee}, now)
    return created


# ---- 路由 --------------------------------------------------------------------

def create_notifications_router(db: Database, settings: Settings,
                                senders_provider) -> APIRouter:
    """senders_provider：无参可调用，返回 {"email": fn, "webhook": fn} 字典；
    允许应用在测试中整体替换发送器（失败注入等）。"""
    router = APIRouter(prefix="/admin/approval-notifications",
                       tags=["approval-notifications"])

    def senders():
        return senders_provider() if callable(senders_provider) else senders_provider

    @router.get("/summary")
    def summary(recipient: str | None = None):
        """待办看板：未读/已读/已处理/过期/取消数量 + 外发失败/隔离数量。"""
        return todo_summary(db, recipient)

    @router.get("/todos")
    def todos(recipient: str | None = None, source_type: str | None = None,
              source: str | None = None, batch_id: int | None = None,
              change_id: int | None = None, node_id: int | None = None,
              status: str | None = None, limit: int = Query(100, le=1000)):
        """按接收人、来源（batch/change，或 batch:{id}/change:{id}）、状态查询待办。"""
        return list_todos(db, recipient=recipient, source_type=source_type,
                          source=source, batch_id=batch_id, change_id=change_id,
                          node_id=node_id, status=status, limit=limit)

    @router.get("/todos/{todo_id}")
    def get_todo_endpoint(todo_id: int):
        return {"todo": get_todo(db, todo_id)}

    @router.post("/todos/{todo_id}/read")
    def read_endpoint(todo_id: int, req: TodoReadRequest):
        """确认已读（幂等）。"""
        return {"todo": mark_read(db, todo_id, req.operator)}

    @router.post("/todos/{todo_id}/act")
    def act_endpoint(todo_id: int, req: TodoActRequest):
        """处理待办：回写原审批动作（节点 approve/reject/skip；变更单 approve/reject）。

        重复点击、过期待办、并发处理都不会重复投票或越过审批门禁。
        """
        return act_on_todo(db, todo_id, req)

    @router.get("/deliveries")
    def deliveries(status: str | None = None, recipient: str | None = None,
                   channel: str | None = None, limit: int = Query(100, le=1000)):
        """外发投递查询（可按状态/接收人/通道过滤）。"""
        return list_deliveries(db, status_filter=status, recipient=recipient,
                               channel=channel, limit=limit)

    @router.post("/deliveries/{delivery_id}/requeue")
    def requeue_endpoint(delivery_id: int, req: DeliveryRequeueRequest):
        """把隔离的外发投递重新放回发送队列（重置次数与退避），并立即尝试一次。"""
        result = requeue_delivery(db, delivery_id, req.operator)
        send_delivery(db, delivery_id, senders(), settings)
        return result

    @router.post("/contacts")
    def upsert_contact_endpoint(req: ContactUpsertRequest):
        """注册/更新审批通知联系人（站内待办 + 可选邮件/webhook 通道）。"""
        return upsert_contact(db, req)

    @router.get("/contacts")
    def list_contacts_endpoint(active: bool | None = None):
        return list_contacts(db, active)

    @router.post("/contacts/{name}/deactivate")
    def deactivate_contact_endpoint(name: str, req: ContactDeactivateRequest):
        """停用联系人：未处理待办关闭，未发出投递取消。"""
        return deactivate_contact(db, name, req)

    return router

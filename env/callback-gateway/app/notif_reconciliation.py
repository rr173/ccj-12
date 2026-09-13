"""通知状态对账与补偿（notification reconciliation & compensation）。

对账是一次带筛选条件的只读扫描：发送任务、通道尝试、额度预占、外部 message_id、
回执与状态历史在同一个短写事务中取快照并判定异常。写事务仅用于固化不可变对账结果，
绝不修改通知原始状态。检测规则版本（路由版本、额度版本、回执策略）随 job/finding
固化；之后发布新规则只影响新对账，不回改已有快照。

补偿动作在人工确认后单独发生：
- relink_receipt：把已有回执重新关联到已有外部消息/任务（不改正文，并复用回执状态机）；
- release_reservation：只释放确认仍为 reserved 且没有成功外部效果的孤儿预占；
- close_task：关闭确认不再发送的任务，保留尝试、消息、回执与审计轨迹；
- create_send_plan：仅为从未成功产生外部效果的任务创建补偿发送计划，由既有发送任务的
  单赢家领取、额度预占和通道尝试幂等保护执行。

所有补偿先做实时门禁，再把依据快照、前后状态、操作者和结果落盘；重复提交返回已有动作。
"""
from __future__ import annotations

import json
import sqlite3
import time
from typing import Any

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit, notif_quota, notif_routing, receipts
from .db import Database

# ---- 常量 -------------------------------------------------------------------

JOB_QUEUED = "queued"
JOB_SCANNING = "scanning"
JOB_PAUSED = "paused"
JOB_COMPLETED = "completed"
JOB_FAILED = "failed"

FINDING_OPEN = "open"
FINDING_COMPENSATING = "compensating"
FINDING_RESOLVED = "resolved"
FINDING_FAILED = "failed"

ACTION_RELINK = "relink_receipt"
ACTION_RELEASE = "release_reservation"
ACTION_CLOSE = "close_task"
ACTION_CREATE_PLAN = "create_send_plan"
ACTIONS = (ACTION_RELINK, ACTION_RELEASE, ACTION_CLOSE, ACTION_CREATE_PLAN)

PLAN_SCHEDULED = "scheduled"
PLAN_APPLIED = "applied"
PLAN_SUPERSEDED = "superseded"

ENTITY_TASK = "task"
ENTITY_RECEIPT = "receipt"
ENTITY_RESERVATION = "reservation"
ENTITY_MESSAGE = "message"

DEFAULT_PAGE_SIZE = 100
MAX_PAGE_SIZE = 1000
FAILED_RETRY_SECONDS = 5.0

# 已经被外部商接受（登记过 message_id）或有外发通道成功尝试，即视为有外部成功效果。
# 补偿发送不得再次制造新的外部发送意图；送达失败后的既有回执故障转移仍由回执模块负责。
EXTERNAL_SUCCESS_MESSAGE_STATUSES = (
    "pending", "resending", "delivered", "bounced", "complained", "expired",
    "awaiting_confirmation",
)


# ---- 请求模型 ----------------------------------------------------------------

class ReconciliationRequest(BaseModel):
    operator: str
    recipient: str | None = None
    event_id: int | None = None
    event_type: str | None = None
    status: str | None = None
    time_from: float | str | None = None
    time_to: float | str | None = None
    page_size: int = DEFAULT_PAGE_SIZE


class PauseRequest(BaseModel):
    operator: str
    reason: str | None = None


class CompensationRequest(BaseModel):
    operator: str
    note: str | None = None
    receipt_id: int | None = None
    message_pk: int | None = None
    task_id: int | None = None
    reservation_id: int | None = None


# ---- 小工具 ------------------------------------------------------------------

def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return str(value).strip()


def _time(value) -> float | None:
    return None if value is None else receipts.parse_time(value)


def _json_dumps(value: Any) -> str:
    return json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)


def _row(row: sqlite3.Row | None) -> dict | None:
    return None if row is None else {k: row[k] for k in row.keys()}


def _rows(rows: list[sqlite3.Row]) -> list[dict]:
    return [_row(r) for r in rows if r is not None]


def _loads(value: str | None, default):
    if not value:
        return default
    try:
        return json.loads(value)
    except (TypeError, json.JSONDecodeError):
        return default


def _policy_versions(cur: sqlite3.Cursor) -> dict:
    route = cur.execute(
        "SELECT route_version FROM notif_route_current WHERE id=1").fetchone()
    quota = cur.execute(
        "SELECT quota_version FROM notif_quota_current WHERE id=1").fetchone()
    policy = cur.execute("SELECT * FROM receipt_policy WHERE id=1").fetchone()
    return {
        "route_version": route["route_version"] if route else None,
        "quota_version": quota["quota_version"] if quota else None,
        "receipt_policy": _row(policy) or {},
    }


def _task_snapshot(cur: sqlite3.Cursor, task_id: int) -> dict:
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                       (task_id,)).fetchone()
    if task is None:
        return {}
    return {
        "task": _row(task),
        "attempts": _rows(cur.execute(
            "SELECT * FROM notif_send_attempts WHERE task_id=? ORDER BY id",
            (task_id,)).fetchall()),
        "switches": _rows(cur.execute(
            "SELECT * FROM notif_channel_switches WHERE task_id=? ORDER BY id",
            (task_id,)).fetchall()),
        "messages": _rows(cur.execute(
            "SELECT * FROM external_messages WHERE send_task_id=? ORDER BY id",
            (task_id,)).fetchall()),
        "reservations": _rows(cur.execute(
            "SELECT * FROM notif_quota_reservations WHERE task_id=? ORDER BY id",
            (task_id,)).fetchall()),
        "receipts": _rows(cur.execute(
            """SELECT r.* FROM receipts r JOIN external_messages m
               ON m.id=r.message_pk WHERE m.send_task_id=? ORDER BY r.id""",
            (task_id,)).fetchall()),
    }


def _receipt_snapshot(cur: sqlite3.Cursor, receipt_id: int) -> dict:
    receipt = cur.execute("SELECT * FROM receipts WHERE id=?",
                          (receipt_id,)).fetchone()
    out = {"receipt": _row(receipt)}
    if receipt is not None and receipt["message_pk"]:
        out["message"] = _row(cur.execute(
            "SELECT * FROM external_messages WHERE id=?",
            (receipt["message_pk"],)).fetchone())
    return out


def _reservation_snapshot(cur: sqlite3.Cursor, reservation_id: int) -> dict:
    r = cur.execute("SELECT * FROM notif_quota_reservations WHERE id=?",
                    (reservation_id,)).fetchone()
    out = {"reservation": _row(r)}
    if r is not None:
        out["task"] = _row(cur.execute(
            "SELECT * FROM notif_send_tasks WHERE id=?",
            (r["task_id"],)).fetchone())
    return out


def _message_snapshot(cur: sqlite3.Cursor, message_pk: int) -> dict:
    msg = cur.execute("SELECT * FROM external_messages WHERE id=?",
                      (message_pk,)).fetchone()
    out = {"message": _row(msg)}
    if msg is not None:
        out["receipts"] = _rows(cur.execute(
            "SELECT * FROM receipts WHERE message_pk=? ORDER BY id",
            (message_pk,)).fetchall())
        if msg["send_task_id"]:
            out["task"] = _row(cur.execute(
                "SELECT * FROM notif_send_tasks WHERE id=?",
                (msg["send_task_id"],)).fetchone())
    return out


def _has_external_success_effect(cur: sqlite3.Cursor, task_id: int) -> bool:
    """任务是否已在外部通道产生过被服务商接受的效果。

    有外部 message_id 登记，或 email/webhook 尝试成功，均不能由补偿再次发送。
    """
    row = cur.execute(
        """SELECT EXISTS(
             SELECT 1 FROM external_messages WHERE send_task_id=?
           ) AS x""", (task_id,)).fetchone()
    if row["x"]:
        return True
    row = cur.execute(
        """SELECT EXISTS(
             SELECT 1 FROM notif_send_attempts
             WHERE task_id=? AND channel IN ('email','webhook')
               AND result='success'
           ) AS x""", (task_id,)).fetchone()
    return bool(row["x"])


# ---- 创建 / 推进对账任务 ------------------------------------------------------

def create_reconciliation(db: Database, req: ReconciliationRequest) -> dict:
    operator = _require(req.operator, "operator")
    time_from = _time(req.time_from)
    time_to = _time(req.time_to)
    if time_from is not None and time_to is not None and time_from > time_to:
        raise HTTPException(422, "time_from must be <= time_to")
    if not any((req.recipient, req.event_id is not None, req.event_type,
                req.status, time_from is not None, time_to is not None)):
        raise HTTPException(
            422, "at least one filter (recipient/event/time/status) is required")
    page_size = int(req.page_size or DEFAULT_PAGE_SIZE)
    if not 1 <= page_size <= MAX_PAGE_SIZE:
        raise HTTPException(422, f"page_size must be between 1 and {MAX_PAGE_SIZE}")
    filters = {
        "recipient": (req.recipient or "").strip() or None,
        "event_id": req.event_id, "event_type":
            (req.event_type or "").strip() or None,
        "status": (req.status or "").strip() or None,
        "time_from": time_from, "time_to": time_to,
    }
    now = time.time()
    with db.tx() as cur:
        versions = _policy_versions(cur)
        cur.execute(
            """INSERT INTO notif_reconciliation_jobs
               (operator,status,filters_json,route_version,quota_version,
                receipt_policy_json,cursor_task_id,cursor_message_id,
                cursor_receipt_id,cursor_reservation_id,phase,scanned_count,
                findings_count,page_size,attempts,created_at,updated_at)
               VALUES (?, 'queued', ?,?,?,?, 0,0,0,0, 'tasks', 0,0, ?,0,?,?)""",
            (operator, _json_dumps(filters), versions["route_version"],
             versions["quota_version"], _json_dumps(versions["receipt_policy"]),
             page_size, now, now))
        job_id = cur.lastrowid
        audit.record(cur, "notif_reconciliation_created", None, None, {
            "job_id": job_id, "operator": operator, "filters": filters,
            "route_version": versions["route_version"],
            "quota_version": versions["quota_version"]}, ts=now)
    return {"result": "created", "job_id": job_id, "status": JOB_QUEUED}


def _task_where(filters: dict, alias: str = "t") -> tuple[str, list]:
    where, params = [], []
    if filters.get("recipient"):
        where.append(f"{alias}.recipient=?")
        params.append(filters["recipient"])
    if filters.get("event_id") is not None:
        where.append(f"{alias}.event_id=?")
        params.append(int(filters["event_id"]))
    if filters.get("event_type"):
        where.append(f"{alias}.event_type=?")
        params.append(filters["event_type"])
    if filters.get("status"):
        where.append(f"{alias}.status=?")
        params.append(filters["status"])
    if filters.get("time_from") is not None:
        where.append(f"{alias}.created_at>=?")
        params.append(float(filters["time_from"]))
    if filters.get("time_to") is not None:
        where.append(f"{alias}.created_at<=?")
        params.append(float(filters["time_to"]))
    return (" WHERE " + " AND ".join(where) if where else ""), params


def _receipt_where(filters: dict) -> tuple[str, list]:
    where, params = [], []
    if filters.get("recipient"):
        # 回执解析不到接收人时 r.recipient 为 NULL；同时允许按同 message_id 已登记消息的
        # 接收人过滤，确保未匹配回执仍能通过对账范围。
        where.append("""(r.recipient=? OR EXISTS(
           SELECT 1 FROM external_messages em
           WHERE em.channel=r.channel AND em.message_id=r.message_id
             AND em.recipient=?) OR EXISTS(
           SELECT 1 FROM approval_contacts ac
           WHERE ac.name=? AND ac.email=r.recipient))""")
        params.extend([filters["recipient"], filters["recipient"],
                       filters["recipient"]])
    if filters.get("event_type"):
        # 回执自身没有事件类型；通过同 message_id 的消息/发送任务过滤。
        where.append("""EXISTS(SELECT 1 FROM external_messages em
           WHERE em.channel=r.channel AND em.message_id=r.message_id
             AND em.event_type=?)""")
        params.append(filters["event_type"])
    if filters.get("event_id") is not None:
        where.append("""EXISTS(SELECT 1 FROM external_messages em
           WHERE em.channel=r.channel AND em.message_id=r.message_id
             AND em.event_id=?)""")
        params.append(int(filters["event_id"]))
    if filters.get("time_from") is not None:
        where.append("r.received_at>=?")
        params.append(float(filters["time_from"]))
    if filters.get("time_to") is not None:
        where.append("r.received_at<=?")
        params.append(float(filters["time_to"]))
    return (" WHERE " + " AND ".join(where) if where else ""), params


def _message_where(filters: dict) -> tuple[str, list]:
    where, params = [], []
    if filters.get("recipient"):
        where.append("m.recipient=?")
        params.append(filters["recipient"])
    if filters.get("event_id") is not None:
        where.append("m.event_id=?")
        params.append(int(filters["event_id"]))
    if filters.get("event_type"):
        where.append("m.event_type=?")
        params.append(filters["event_type"])
    if filters.get("status"):
        where.append("m.status=?")
        params.append(filters["status"])
    if filters.get("time_from") is not None:
        where.append("m.registered_at>=?")
        params.append(float(filters["time_from"]))
    if filters.get("time_to") is not None:
        where.append("m.registered_at<=?")
        params.append(float(filters["time_to"]))
    return (" WHERE " + " AND ".join(where) if where else ""), params


def _reservation_where(filters: dict) -> tuple[str, list]:
    where, params = [], []
    if filters.get("recipient"):
        where.append("r.recipient=?")
        params.append(filters["recipient"])
    if filters.get("event_id") is not None:
        where.append("r.event_id=?")
        params.append(int(filters["event_id"]))
    if filters.get("event_type"):
        where.append("t.event_type=?")
        params.append(filters["event_type"])
    if filters.get("status"):
        where.append("(t.status=? OR r.state=?)")
        params.extend((filters["status"], filters["status"]))
    if filters.get("time_from") is not None:
        where.append("r.created_at>=?")
        params.append(float(filters["time_from"]))
    if filters.get("time_to") is not None:
        where.append("r.created_at<=?")
        params.append(float(filters["time_to"]))
    return (" AND ".join(where) if where else ""), params


def _add_finding(cur, *, job_id: int, key: str, entity_type: str, entity_id: int,
                 reason: str, snapshot: dict, evidence: dict, actions: list[str],
                 recipient: str | None, event_id: int | None,
                 event_type: str | None, send_task_id: int | None = None,
                 receipt_id: int | None = None, reservation_id: int | None = None,
                 message_pk: int | None = None,
                 severity: str = "warning", now: float) -> bool:
    try:
        cur.execute(
            """INSERT INTO notif_reconciliation_findings
               (job_id,anomaly_key,entity_type,entity_id,severity,reason,status,
                recipient,event_id,event_type,send_task_id,receipt_id,
                reservation_id,message_pk,snapshot_json,evidence_json,
                suggested_actions_json,detected_at,updated_at)
               VALUES (?,?,?,?,?,?, 'open', ?,?,?,?,?,?,?,?,?,?,?,?)""",
            (job_id, key, entity_type, entity_id, severity, reason, recipient,
             event_id, event_type, send_task_id, receipt_id, reservation_id,
             message_pk, _json_dumps(snapshot), _json_dumps(evidence),
             _json_dumps(actions), now, now))
        cur.execute(
            "UPDATE notif_reconciliation_jobs SET findings_count=findings_count+1 "
            "WHERE id=?", (job_id,))
        return True
    except sqlite3.IntegrityError:
        return False


def _active_message(messages: list[dict]):
    return next((m for m in messages
                 if m.get("status") != "superseded" and m.get("id") is not None),
                None)


def _detect_task(cur, job: sqlite3.Row, task: sqlite3.Row, now: float) -> None:
    job_id = int(job["id"])
    snap = _task_snapshot(cur, task["id"])
    messages = snap["messages"]
    attempts = snap["attempts"]
    reservations = snap["reservations"]
    active_msg = _active_message(messages)
    success_external = [a for a in attempts
                        if a.get("channel") in ("email", "webhook")
                        and a.get("result") == "success"]
    base = {
        "job_id": job_id, "entity_type": ENTITY_TASK, "entity_id": task["id"],
        "recipient": task["recipient"], "event_id": task["event_id"],
        "event_type": task["event_type"], "send_task_id": task["id"],
        "now": now,
    }

    def add(reason: str, evidence: dict, actions: list[str],
            message_pk: int | None = None, severity: str = "warning"):
        _add_finding(cur, key=f"task:{task['id']}:{reason}", reason=reason,
                     snapshot=snap, evidence=evidence, actions=actions,
                     message_pk=message_pk, severity=severity, **base)

    if task["status"] == "sent":
        if not any(a.get("result") == "success" for a in attempts):
            add("sent_task_missing_success_attempt",
                {"task_status": "sent", "attempts": len(attempts)}, [])
        if success_external and not messages:
            add("external_success_without_message_registration",
                {"success_attempt_ids": [a["id"] for a in success_external]}, [])
        if active_msg and task["external_message_id"] != active_msg["id"]:
            add("task_message_pointer_mismatch",
                {"task_external_message_id": task["external_message_id"],
                 "active_message_pk": active_msg["id"]}, [])
        if active_msg and active_msg["status"] == "delivered" \
                and task["receipt_status"] != "delivered":
            add("delivered_message_not_reflected_on_task",
                {"active_message_pk": active_msg["id"],
                 "task_receipt_status": task["receipt_status"]},
                [ACTION_RELINK])
        if active_msg and active_msg["status"] in \
                ("bounced", "complained", "expired") and \
                task["receipt_status"] == "delivered":
            add("task_receipt_status_contradiction",
                {"active_message_pk": active_msg["id"],
                 "message_status": active_msg["status"],
                 "task_receipt_status": task["receipt_status"]},
                [ACTION_CLOSE])
    else:
        if active_msg and active_msg["status"] == "delivered":
            add("open_task_has_delivered_receipt",
                {"active_message_pk": active_msg["id"],
                 "task_status": task["status"]},
                [ACTION_RELINK, ACTION_CLOSE], message_pk=active_msg["id"])
        if task["status"] == "cancelled" and active_msg and \
                active_msg["status"] not in ("superseded",):
            add("cancelled_task_has_active_message",
                {"active_message_pk": active_msg["id"],
                 "message_status": active_msg["status"]}, [])
        if task["status"] in ("failed", "quarantined", "awaiting_manual") and \
                not _has_external_success_effect(cur, task["id"]):
            add("task_needs_compensation_send",
                {"task_status": task["status"],
                 "last_error": task["last_error"],
                 "receipt_reason": task["receipt_reason"]},
                [ACTION_CREATE_PLAN])

    # 任务/预占互相矛盾。
    current_res = next((r for r in reservations
                        if r["generation"] == task["quota_generation"]), None)
    active_res = [r for r in reservations if r["state"] == "reserved"]
    if task["quota_status"] in ("admitted", "consumed", "downgraded") and \
            current_res is None and task["status"] in ("in_flight", "sent"):
        add("task_missing_current_reservation",
            {"quota_status": task["quota_status"],
             "generation": task["quota_generation"]}, [],
            severity="critical")
    for r in active_res:
        tstatus = task["status"]
        if tstatus in ("sent", "cancelled", "quarantined"):
            # sent 但预占仍是 reserved 通常意味着登记/回执链路没有正确消耗；只有无外部
            # 成功效果时才建议释放，避免把服务商已接受消息的额度错误回收。
            orphan = tstatus in ("cancelled", "quarantined") or \
                not _has_external_success_effect(cur, task["id"])
            add(f"active_reservation_on_{tstatus}_task",
                {"reservation_id": r["id"], "task_status": tstatus,
                 "generation": r["generation"], "cost": r["cost"]},
                [ACTION_RELEASE] if orphan else [],
                message_pk=None)
        if r["generation"] != task["quota_generation"] and tstatus != "cancelled":
            add("stale_generation_reservation",
                {"reservation_id": r["id"],
                 "reservation_generation": r["generation"],
                 "task_generation": task["quota_generation"]},
                [ACTION_RELEASE])


def _detect_message(cur, job: sqlite3.Row, msg: sqlite3.Row,
                    now: float) -> None:
    job_id = int(job["id"])
    snap = _message_snapshot(cur, msg["id"])
    task = snap.get("task") or {}
    common = {"job_id": job_id, "entity_type": ENTITY_MESSAGE,
              "entity_id": msg["id"], "recipient": msg["recipient"],
              "event_id": msg["event_id"], "event_type": msg["event_type"],
              "send_task_id": msg["send_task_id"],
              "message_pk": msg["id"], "now": now}

    def add(reason: str, evidence: dict, actions: list[str],
            severity: str = "warning"):
        _add_finding(cur, key=f"message:{msg['id']}:{reason}",
                     reason=reason, snapshot=snap, evidence=evidence,
                     actions=actions, severity=severity, **common)

    if msg["send_task_id"] is None and msg["delivery_id"] is None:
        add("message_without_send_unit",
            {"source": msg["source"], "status": msg["status"]}, [],
            severity="critical")
    if msg["send_task_id"] is not None and not task:
        add("message_task_missing", {"send_task_id": msg["send_task_id"]}, [],
            severity="critical")
    if msg["status"] not in ("superseded",) and task and \
            task.get("status") == "cancelled":
        add("active_message_on_cancelled_task",
            {"message_status": msg["status"], "task_status": "cancelled"}, [])
    if msg["status"] in ("delivered", "bounced", "complained", "expired") and \
            not msg["active_receipt_id"]:
        add("terminal_message_without_active_receipt",
            {"message_status": msg["status"]}, [ACTION_RELINK])


def _detect_receipt(cur, job: sqlite3.Row, receipt: sqlite3.Row,
                    now: float) -> None:
    job_id = int(job["id"])
    snap = _receipt_snapshot(cur, receipt["id"])
    common = {"job_id": job_id, "entity_type": ENTITY_RECEIPT,
              "entity_id": receipt["id"], "recipient": receipt["recipient"],
              "event_id": None, "event_type": None,
              "receipt_id": receipt["id"], "now": now}

    def add(reason: str, evidence: dict, actions: list[str],
            message_pk: int | None = None, severity: str = "warning"):
        _add_finding(cur, key=f"receipt:{receipt['id']}:{reason}",
                     reason=reason, snapshot=snap, evidence=evidence,
                     actions=actions, message_pk=message_pk, severity=severity,
                     **common)

    if receipt["matched"] in ("unmatched", "ignored"):
        candidate = cur.execute(
            """SELECT * FROM external_messages WHERE channel=? AND message_id=?
               ORDER BY CASE WHEN status<>'superseded' THEN 0 ELSE 1 END, id DESC
               LIMIT 1""", (receipt["channel"], receipt["message_id"])).fetchone()
        if candidate is not None:
            add("receipt_matches_existing_message",
                {"candidate_message_pk": candidate["id"],
                 "candidate_status": candidate["status"],
                 "send_task_id": candidate["send_task_id"],
                 "delivery_id": candidate["delivery_id"]},
                [ACTION_RELINK], message_pk=candidate["id"])
        else:
            add("receipt_without_message",
                {"channel": receipt["channel"],
                 "message_id": receipt["message_id"], "event": receipt["event"]},
                [ACTION_RELINK])
        return

    msg = None
    if receipt["message_pk"]:
        msg = cur.execute("SELECT * FROM external_messages WHERE id=?",
                          (receipt["message_pk"],)).fetchone()
    if msg is None:
        add("matched_receipt_missing_message",
            {"message_pk": receipt["message_pk"]}, [ACTION_RELINK],
            severity="critical")
        return
    if msg["channel"] != receipt["channel"] or \
            msg["message_id"] != receipt["message_id"]:
        add("receipt_message_identity_mismatch",
            {"message_pk": msg["id"], "receipt_channel": receipt["channel"],
             "message_channel": msg["channel"],
             "receipt_message_id": receipt["message_id"],
             "message_message_id": msg["message_id"]},
            [ACTION_RELINK], message_pk=msg["id"], severity="critical")


def _detect_reservation(cur, job: sqlite3.Row, r: sqlite3.Row,
                        task: sqlite3.Row, now: float) -> None:
    job_id = int(job["id"])
    snap = {"reservation": _row(r), "task": _row(task)}
    common = {"job_id": job_id, "entity_type": ENTITY_RESERVATION,
              "entity_id": r["id"], "recipient": r["recipient"],
              "event_id": r["event_id"], "event_type": task["event_type"]
              if task else None, "send_task_id": r["task_id"],
              "reservation_id": r["id"], "now": now}

    def add(reason: str, evidence: dict, actions: list[str],
            severity: str = "warning"):
        _add_finding(cur, key=f"reservation:{r['id']}:{reason}",
                     reason=reason, snapshot=snap, evidence=evidence,
                     actions=actions, severity=severity, **common)

    if task is None:
        add("reservation_task_missing", {"task_id": r["task_id"]},
            [ACTION_RELEASE], severity="critical")
        return
    if r["state"] == "reserved":
        if task["status"] in ("sent", "cancelled", "quarantined") and \
                not (task["status"] == "sent" and
                     _has_external_success_effect(cur, task["id"])):
            add("orphan_reserved_reservation",
                {"task_status": task["status"], "generation": r["generation"],
                 "task_generation": task["quota_generation"]},
                [ACTION_RELEASE])
        if r["generation"] != task["quota_generation"] and \
                task["status"] != "cancelled":
            add("stale_generation_reservation",
                {"reservation_generation": r["generation"],
                 "task_generation": task["quota_generation"]},
                [ACTION_RELEASE])
    elif r["state"] == "consumed" and task["status"] == "cancelled":
        add("consumed_reservation_on_cancelled_task",
            {"generation": r["generation"]}, [])


def _process_chunk(cur, job: sqlite3.Row, now: float) -> None:
    filters = _loads(job["filters_json"], {})
    page_size = int(job["page_size"] or DEFAULT_PAGE_SIZE)
    phase = job["phase"]

    if phase == "tasks":
        where, params = _task_where(filters)
        rows = cur.execute(
            f"""SELECT * FROM notif_send_tasks t{where} AND t.id>?
                ORDER BY t.id LIMIT ?""",
            (*params, job["cursor_task_id"], page_size)).fetchall()
        for t in rows:
            _detect_task(cur, job, t, now)
        max_id = rows[-1]["id"] if rows else job["cursor_task_id"]
        cur.execute(
            """UPDATE notif_reconciliation_jobs
               SET cursor_task_id=?, scanned_count=scanned_count+?, updated_at=?
               WHERE id=?""", (max_id, len(rows), now, job["id"]))
        if len(rows) < page_size:
            cur.execute("UPDATE notif_reconciliation_jobs SET phase='messages' "
                        "WHERE id=?", (job["id"],))
        return

    if phase == "messages":
        where, params = _message_where(filters)
        sql = "SELECT * FROM external_messages m"
        if where:
            sql += where
        sql += (" AND " if where else " WHERE ") + "m.id>? ORDER BY m.id LIMIT ?"
        rows = cur.execute(sql, (*params, job["cursor_message_id"],
                                 page_size)).fetchall()
        for m in rows:
            _detect_message(cur, job, m, now)
        max_id = rows[-1]["id"] if rows else job["cursor_message_id"]
        cur.execute(
            """UPDATE notif_reconciliation_jobs
               SET cursor_message_id=?, scanned_count=scanned_count+?,
                   updated_at=? WHERE id=?""",
            (max_id, len(rows), now, job["id"]))
        if len(rows) < page_size:
            cur.execute(
                "UPDATE notif_reconciliation_jobs SET phase='receipts' "
                "WHERE id=?", (job["id"],))
        return

    if phase == "receipts":
        where, params = _receipt_where(filters)
        sql = f"SELECT r.* FROM receipts r{where}"
        sql += (" AND " if where else " WHERE ") + "r.id>? ORDER BY r.id LIMIT ?"
        rows = cur.execute(sql, (*params, job["cursor_receipt_id"],
                                 page_size)).fetchall()
        for r in rows:
            _detect_receipt(cur, job, r, now)
        max_id = rows[-1]["id"] if rows else job["cursor_receipt_id"]
        cur.execute(
            """UPDATE notif_reconciliation_jobs
               SET cursor_receipt_id=?, scanned_count=scanned_count+?, updated_at=?
               WHERE id=?""", (max_id, len(rows), now, job["id"]))
        if len(rows) < page_size:
            cur.execute(
                "UPDATE notif_reconciliation_jobs SET phase='reservations' "
                "WHERE id=?", (job["id"],))
        return

    if phase == "reservations":
        where, params = _reservation_where(filters)
        sql = """SELECT r.*, t.id AS t_id FROM notif_quota_reservations r
                 JOIN notif_send_tasks t ON r.task_id=t.id WHERE 1=1"""
        if where:
            sql += " AND " + where
        sql += " AND r.id>? ORDER BY r.id LIMIT ?"
        rows = cur.execute(sql, (*params, job["cursor_reservation_id"],
                                 page_size)).fetchall()
        for rr in rows:
            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (rr["task_id"],)).fetchone()
            _detect_reservation(cur, job, rr, task, now)
        max_id = rows[-1]["id"] if rows else job["cursor_reservation_id"]
        cur.execute(
            """UPDATE notif_reconciliation_jobs
               SET cursor_reservation_id=?, scanned_count=scanned_count+?,
                   updated_at=? WHERE id=?""",
            (max_id, len(rows), now, job["id"]))
        if len(rows) < page_size:
            cur.execute(
                "UPDATE notif_reconciliation_jobs SET phase='done' WHERE id=?",
                (job["id"],))


def process_due_jobs(db: Database, now: float | None = None,
                     max_chunks: int = 20) -> dict:
    """推进到期对账任务。每个分片独立事务；暂停只在下一分片前生效。"""
    now = time.time() if now is None else now
    processed = 0
    chunks = 0
    for _ in range(max_chunks):
        with db.tx() as cur:
            job = cur.execute(
                """SELECT * FROM notif_reconciliation_jobs
                   WHERE (status='queued'
                          OR (status='failed' AND (next_retry_at IS NULL
                                                   OR next_retry_at<=?)))
                     AND status <> 'paused'
                   ORDER BY id LIMIT 1""", (now,)).fetchone()
            if job is None:
                break
            cur.execute(
                """UPDATE notif_reconciliation_jobs
                   SET status='scanning', attempts=attempts+1,
                       started_at=COALESCE(started_at,?), last_error=NULL,
                       next_retry_at=NULL, updated_at=? WHERE id=?""",
                (now, now, job["id"]))
            job = cur.execute("SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                              (job["id"],)).fetchone()
            try:
                _process_chunk(cur, job, now)
                latest = cur.execute(
                    "SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                    (job["id"],)).fetchone()
                if latest["phase"] == "done":
                    cur.execute(
                        """UPDATE notif_reconciliation_jobs
                           SET status='completed', completed_at=?, updated_at=?
                           WHERE id=?""", (now, now, job["id"]))
                else:
                    cur.execute(
                        "UPDATE notif_reconciliation_jobs SET status='queued', "
                        "updated_at=? WHERE id=?", (now, job["id"]))
                audit.record(cur, "notif_reconciliation_chunk_scanned", None, None, {
                    "job_id": job["id"], "phase": latest["phase"],
                    "scanned_count": latest["scanned_count"],
                    "findings_count": latest["findings_count"]}, ts=now)
            except Exception as exc:
                cur.execute(
                    """UPDATE notif_reconciliation_jobs
                       SET status='failed', last_error=?, next_retry_at=?,
                           updated_at=? WHERE id=?""",
                    (str(exc), now + FAILED_RETRY_SECONDS, now, job["id"]))
                audit.record(cur, "notif_reconciliation_failed", None, None, {
                    "job_id": job["id"], "phase": job["phase"],
                    "error": str(exc)}, ts=now)
                processed += 1
                chunks += 1
                continue
        processed += 1
        chunks += 1
    return {"jobs_processed": processed, "chunks": chunks}


def recover_reconciliation_jobs(db: Database, now: float | None = None) -> int:
    """服务重启：扫描中任务安全退回 queued；已固化游标和快照保证续跑不重复。"""
    now = time.time() if now is None else now
    with db.tx() as cur:
        n = cur.execute(
            """UPDATE notif_reconciliation_jobs SET status='queued',
               next_retry_at=NULL, updated_at=?
               WHERE status='scanning'""", (now,)).rowcount
        if n:
            audit.record(cur, "notif_reconciliation_recovered", None, None,
                         {"recovered": n}, ts=now)
    return n


def pause_job(db: Database, job_id: int, req: PauseRequest) -> dict:
    operator = _require(req.operator, "operator")
    now = time.time()
    with db.tx() as cur:
        job = cur.execute("SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                          (job_id,)).fetchone()
        if job is None:
            raise HTTPException(404, "reconciliation job not found")
        if job["status"] not in (JOB_QUEUED, JOB_SCANNING, JOB_FAILED):
            raise HTTPException(409, f"job is {job['status']} and cannot be paused")
        cur.execute(
            """UPDATE notif_reconciliation_jobs SET status='paused', paused_by=?,
               paused_at=?, next_retry_at=NULL, updated_at=? WHERE id=?""",
            (operator, now, now, job_id))
        audit.record(cur, "notif_reconciliation_paused", None, None, {
            "job_id": job_id, "operator": operator, "reason": req.reason}, ts=now)
    return {"result": "paused", "job_id": job_id}


def resume_job(db: Database, job_id: int, operator: str) -> dict:
    operator = _require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        job = cur.execute("SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                          (job_id,)).fetchone()
        if job is None:
            raise HTTPException(404, "reconciliation job not found")
        if job["status"] != JOB_PAUSED:
            raise HTTPException(409, f"job is {job['status']}, not paused")
        cur.execute(
            """UPDATE notif_reconciliation_jobs SET status='queued',
               resumed_count=resumed_count+1, next_retry_at=NULL, updated_at=?
               WHERE id=?""", (now, job_id))
        audit.record(cur, "notif_reconciliation_resumed", None, None, {
            "job_id": job_id, "operator": operator}, ts=now)
    return {"result": "resumed", "job_id": job_id}


def retry_failed_job(db: Database, job_id: int, operator: str) -> dict:
    operator = _require(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        job = cur.execute("SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                          (job_id,)).fetchone()
        if job is None:
            raise HTTPException(404, "reconciliation job not found")
        if job["status"] != JOB_FAILED:
            raise HTTPException(409, f"job is {job['status']}, not failed")
        cur.execute(
            """UPDATE notif_reconciliation_jobs SET status='queued',
               next_retry_at=NULL, updated_at=? WHERE id=?""", (now, job_id))
        audit.record(cur, "notif_reconciliation_manual_retry", None, None, {
            "job_id": job_id, "operator": operator,
            "previous_error": job["last_error"]}, ts=now)
    return {"result": "retry_scheduled", "job_id": job_id}


# ---- 补偿 --------------------------------------------------------------------

def _get_finding(cur, finding_id: int) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM notif_reconciliation_findings WHERE id=?",
                      (finding_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "reconciliation finding not found")
    return row


def _record_compensation(cur, *, job_id: int, finding_id: int, action: str,
                         operator: str, status: str, target_type: str,
                         target_id: int | None, request: dict, before: dict,
                         after: dict, basis: dict, reason: str | None,
                         error: str | None, now: float) -> dict:
    cur.execute(
        """INSERT INTO notif_reconciliation_compensations
           (job_id,finding_id,action,operator,status,target_type,target_id,
            request_json,before_json,after_json,basis_snapshot_json,reason,
            error,created_at)
           VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
        (job_id, finding_id, action, operator, status, target_type, target_id,
         _json_dumps(request), _json_dumps(before), _json_dumps(after),
         _json_dumps(basis), reason, error, now))
    cid = cur.lastrowid
    audit.record(cur, "notif_reconciliation_compensation", None, None, {
        "compensation_id": cid, "job_id": job_id, "finding_id": finding_id,
        "action": action, "operator": operator, "status": status,
        "target_type": target_type, "target_id": target_id,
        "reason": reason, "error": error}, ts=now)
    return {"id": cid, "status": status, "target_type": target_type,
            "target_id": target_id}


def _existing_completed_action(cur, finding_id: int, action: str):
    return cur.execute(
        """SELECT * FROM notif_reconciliation_compensations
           WHERE finding_id=? AND action=? AND status IN ('applied','scheduled','noop')
           ORDER BY id DESC LIMIT 1""", (finding_id, action)).fetchone()


def _set_finding_status(cur, finding_id: int, status: str, now: float) -> None:
    cur.execute(
        "UPDATE notif_reconciliation_findings SET status=?, updated_at=? WHERE id=?",
        (status, now, finding_id))


def _compensation_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "job_id": r["job_id"], "finding_id": r["finding_id"],
            "action": r["action"], "operator": r["operator"], "status": r["status"],
            "target_type": r["target_type"], "target_id": r["target_id"],
            "request": _loads(r["request_json"], {}),
            "before": _loads(r["before_json"], {}),
            "after": _loads(r["after_json"], {}),
            "basis_snapshot": _loads(r["basis_snapshot_json"], {}),
            "reason": r["reason"], "error": r["error"],
            "created_at": r["created_at"]}


def _relink_receipt(cur, finding: sqlite3.Row, req: CompensationRequest,
                    now: float) -> dict:
    receipt_id = req.receipt_id or finding["receipt_id"]
    if not receipt_id:
        raise HTTPException(422, "receipt_id is required for relink_receipt")
    receipt = cur.execute("SELECT * FROM receipts WHERE id=?",
                          (receipt_id,)).fetchone()
    if receipt is None:
        raise HTTPException(404, "receipt not found")
    if receipt["matched"] == "applied":
        existing = _existing_completed_action(
            cur, finding["id"], ACTION_RELINK)
        if existing:
            return {"id": existing["id"], "status": existing["status"],
                    "target_type": "receipt", "target_id": receipt_id}
        raise HTTPException(409, "receipt is already applied; original receipts "
                                "cannot be rewritten")

    msg = None
    if req.message_pk is not None:
        msg = cur.execute("SELECT * FROM external_messages WHERE id=?",
                          (req.message_pk,)).fetchone()
    elif req.task_id is not None or finding["send_task_id"] is not None:
        task_id = req.task_id or finding["send_task_id"]
        msg = cur.execute(
            """SELECT * FROM external_messages WHERE send_task_id=? AND channel=?
               ORDER BY CASE WHEN message_id=? THEN 0 ELSE 1 END,
                        CASE WHEN status<>'superseded' THEN 0 ELSE 1 END, id DESC
               LIMIT 1""",
            (task_id, receipt["channel"], receipt["message_id"])).fetchone()
    else:
        msg = cur.execute(
            """SELECT * FROM external_messages WHERE channel=? AND message_id=?
               ORDER BY CASE WHEN status<>'superseded' THEN 0 ELSE 1 END, id DESC
               LIMIT 1""", (receipt["channel"], receipt["message_id"])).fetchone()
    if msg is None:
        raise HTTPException(422, "no existing external message selected; "
                                "relink cannot create a message or send again")
    if msg["channel"] != receipt["channel"]:
        raise HTTPException(422, "receipt channel does not match message channel")

    before = {"receipt": _row(receipt), "message": _row(msg)}
    previous_match = receipt["matched"]
    cur.execute(
        """UPDATE receipts SET matched='bound', message_pk=?,
           send_task_id=COALESCE(send_task_id,?), bound_by=?, bound_at=?,
           bind_note=? WHERE id=?""",
        (msg["id"], msg["send_task_id"], req.operator, now,
         req.note or "reconciliation_relink", receipt_id))
    receipts._history(
        cur, kind="receipt", message_pk=msg["id"], receipt_id=receipt_id,
        send_task_id=msg["send_task_id"], channel=msg["channel"],
        recipient=receipt["recipient"], from_status=previous_match,
        to_status="bound", reason="reconciliation_relink",
        detail={"operator": req.operator, "note": req.note,
                "finding_id": finding["id"]}, operator=req.operator, ts=now)

    disposition = "bound_only"
    if msg["send_task_id"] and msg["status"] not in ("superseded",) and \
            receipt["event"] in receipts.TERMINAL_EVENTS:
        # 先记录这是人工重新关联（bound），再复用状态机；状态机对自动匹配回执会置
        # applied，两种匹配态都表示原文未改写且已驱动后续状态。
        outcome = receipts._apply_matched_receipt_tx(
            cur, msg=msg, event=receipt["event"], receipt_id=receipt_id,
            provider_ts=receipt["provider_ts"],
            raw_detail={"reconciliation_relink": True, "operator": req.operator},
            ts=now)
        disposition = outcome.get("disposition") or "applied"
        # 对账补偿是人工重新关联，即使回执已驱动状态机，也保留 bound 匹配来源以便区分
        # 自动匹配；正文和历史仍不改动。
        cur.execute("UPDATE receipts SET matched='bound' WHERE id=?",
                    (receipt_id,))

    # delivered 回执落到仍开放的任务：关闭任务，防止自动/补偿再次发送。
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                       (msg["send_task_id"],)).fetchone() if msg["send_task_id"] \
        else None
    if task is not None and receipt["event"] == receipts.EV_DELIVERED and \
            task["status"] in ("pending", "in_flight", "failed",
                               "awaiting_manual", "quarantined"):
        _close_task_core(cur, task, "reconciliation_delivered_receipt", now)
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (task["id"],)).fetchone()
    if task is not None and msg["status"] != "superseded" and \
            (task["external_message_id"] is None or
             task["external_message_id"] == msg["id"]):
        cur.execute(
            "UPDATE notif_send_tasks SET external_message_id=? WHERE id=?",
            (msg["id"], task["id"]))
    after = {"receipt": _row(cur.execute(
        "SELECT * FROM receipts WHERE id=?", (receipt_id,)).fetchone()),
        "message": _row(cur.execute(
        "SELECT * FROM external_messages WHERE id=?", (msg["id"],)).fetchone())}
    if task is not None:
        after["task"] = _row(cur.execute(
            "SELECT * FROM notif_send_tasks WHERE id=?",
            (task["id"],)).fetchone())
    audit.record(cur, "notif_reconciliation_receipt_relinked", None, None, {
        "job_id": finding["job_id"], "finding_id": finding["id"],
        "receipt_id": receipt_id, "message_pk": msg["id"],
        "send_task_id": msg["send_task_id"], "operator": req.operator,
        "disposition": disposition}, ts=now)
    return {"status": "applied", "target_type": "receipt",
            "target_id": receipt_id, "before": before, "after": after,
            "disposition": disposition}


def _release_reservation(cur, finding: sqlite3.Row,
                         req: CompensationRequest, now: float) -> dict:
    rid = req.reservation_id or finding["reservation_id"]
    if not rid:
        raise HTTPException(422, "reservation_id is required")
    r = cur.execute("SELECT * FROM notif_quota_reservations WHERE id=?",
                    (rid,)).fetchone()
    if r is None:
        raise HTTPException(404, "reservation not found")
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                       (r["task_id"],)).fetchone()
    before = {"reservation": _row(r), "task": _row(task)}
    if r["state"] != "reserved":
        return {"status": "noop", "target_type": "reservation",
                "target_id": rid, "before": before,
                "after": before, "disposition": f"already_{r['state']}"}
    if task is not None and _has_external_success_effect(cur, task["id"]):
        raise HTTPException(
            409, "task already has an accepted external message or successful "
                 "external attempt; reservation will not be released by "
                 "reconciliation")
    cur.execute(
        "UPDATE notif_quota_reservations SET state='released', "
        "reason='reconciliation_orphan_release', updated_at=? WHERE id=?",
        (now, rid))
    if task is not None and r["generation"] == task["quota_generation"]:
        cur.execute(
            "UPDATE notif_send_tasks SET quota_status='released' WHERE id=?",
            (task["id"],))
    after = {"reservation": _row(cur.execute(
        "SELECT * FROM notif_quota_reservations WHERE id=?", (rid,)).fetchone()),
        "task": _row(cur.execute(
        "SELECT * FROM notif_send_tasks WHERE id=?",
        (r["task_id"],)).fetchone())}
    audit.record(cur, "notif_reconciliation_reservation_released", None, None, {
        "job_id": finding["job_id"], "finding_id": finding["id"],
        "reservation_id": rid, "task_id": r["task_id"],
        "operator": req.operator}, ts=now)
    return {"status": "applied", "target_type": "reservation",
            "target_id": rid, "before": before, "after": after}


def _close_task_core(cur, task: sqlite3.Row, reason: str, now: float) -> bool:
    if task["status"] in ("sent", "cancelled"):
        return False
    cur.execute(
        """UPDATE notif_send_tasks SET status='cancelled', cancelled_reason=?,
           next_retry_at=NULL, updated_at=? WHERE id=?
           AND status IN ('pending','in_flight','failed','quarantined',
                          'awaiting_manual','awaiting_confirmation')""",
        (reason, now, task["id"]))
    cur.execute(
        """UPDATE notif_channel_state SET state='open', probe_task_id=NULL,
           probe_at=NULL, updated_at=? WHERE probe_task_id=?""",
        (now, task["id"]))
    notif_quota.release_for_task_tx(cur, task, reason, now)
    cur.execute(
        """INSERT INTO notif_channel_switches
           (task_id,event_id,recipient,from_channel,to_channel,reason,
            detail,created_at)
           VALUES (?,?,?,?,NULL,?,?,?)""",
        (task["id"], task["event_id"], task["recipient"],
         task["current_channel"], "reconciliation_closed",
         _json_dumps({"reason": reason}), now))
    audit.record(cur, "notif_reconciliation_task_closed", None, None, {
        "send_task_id": task["id"], "recipient": task["recipient"],
        "event_type": task["event_type"], "reason": reason}, ts=now)
    return True


def _close_task(cur, finding: sqlite3.Row, req: CompensationRequest,
                now: float) -> dict:
    task_id = req.task_id or finding["send_task_id"]
    if not task_id:
        raise HTTPException(422, "task_id is required")
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                       (task_id,)).fetchone()
    if task is None:
        raise HTTPException(404, "send task not found")
    before = {"task": _row(task)}
    changed = _close_task_core(
        cur, task, req.note or "reconciliation_close_no_more_send", now)
    after = {"task": _row(cur.execute(
        "SELECT * FROM notif_send_tasks WHERE id=?", (task_id,)).fetchone())}
    audit.record(cur, "notif_reconciliation_close_requested", None, None, {
        "job_id": finding["job_id"], "finding_id": finding["id"],
        "send_task_id": task_id, "operator": req.operator,
        "changed": changed}, ts=now)
    return {"status": "applied" if changed else "noop",
            "target_type": "task", "target_id": task_id,
            "before": before, "after": after, "changed": changed}


def _create_send_plan(cur, finding: sqlite3.Row, req: CompensationRequest,
                      now: float) -> dict:
    task_id = req.task_id or finding["send_task_id"]
    if not task_id:
        raise HTTPException(422, "task_id is required")
    task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                       (task_id,)).fetchone()
    if task is None:
        raise HTTPException(404, "send task not found")
    if _has_external_success_effect(cur, task_id):
        raise HTTPException(
            409, "task already has an accepted external message or successful "
                 "external attempt; compensation send is forbidden")
    if task["status"] not in ("pending", "failed", "quarantined",
                              "awaiting_manual", "cancelled"):
        raise HTTPException(
            409, f"task status {task['status']} cannot accept a compensation plan")
    plan_key = f"finding:{finding['id']}:task:{task_id}"
    existing = cur.execute(
        "SELECT * FROM notif_compensation_send_plans WHERE plan_key=?",
        (plan_key,)).fetchone()
    before = {"task": _row(task)}
    if existing is not None:
        return {"status": existing["status"], "target_type": "send_plan",
                "target_id": existing["id"], "before": before,
                "after": {"plan": _row(existing)}, "idempotent": True}
    cur.execute(
        """INSERT INTO notif_compensation_send_plans
           (finding_id,job_id,task_id,plan_key,status,operator,note,
            scheduled_at,created_at,updated_at)
           VALUES (?,?,?,?, 'scheduled', ?,?, ?,?,?)""",
        (finding["id"], finding["job_id"], task_id, plan_key, req.operator,
         req.note, now, now, now))
    plan_id = cur.lastrowid
    cur.execute(
        "UPDATE notif_reconciliation_findings SET status='compensating', "
        "updated_at=? WHERE id=?", (now, finding["id"]))
    audit.record(cur, "notif_reconciliation_send_plan_created", None, None, {
        "plan_id": plan_id, "job_id": finding["job_id"],
        "finding_id": finding["id"], "send_task_id": task_id,
        "operator": req.operator}, ts=now)
    return {"status": PLAN_SCHEDULED, "target_type": "send_plan",
            "target_id": plan_id, "before": before,
            "after": {"plan_id": plan_id, "status": PLAN_SCHEDULED}}


def apply_compensation(db: Database, finding_id: int, action: str,
                       req: CompensationRequest) -> dict:
    operator = _require(req.operator, "operator")
    if action not in ACTIONS:
        raise HTTPException(422, f"action must be one of {','.join(ACTIONS)}")
    request = {"receipt_id": req.receipt_id, "message_pk": req.message_pk,
               "task_id": req.task_id, "reservation_id": req.reservation_id,
               "note": req.note}
    now = time.time()
    with db.tx() as cur:
        finding = _get_finding(cur, finding_id)
        completed = _existing_completed_action(cur, finding_id, action)
        if completed is not None:
            return {"result": "idempotent",
                    "compensation": _compensation_view(completed)}
        basis = {"finding_id": finding_id, "reason": finding["reason"],
                 "detected_at": finding["detected_at"],
                 "snapshot": _loads(finding["snapshot_json"], {}),
                 "evidence": _loads(finding["evidence_json"], {})}
        before: dict = {}
        try:
            if action == ACTION_RELINK:
                outcome = _relink_receipt(cur, finding, req, now)
            elif action == ACTION_RELEASE:
                outcome = _release_reservation(cur, finding, req, now)
            elif action == ACTION_CLOSE:
                outcome = _close_task(cur, finding, req, now)
            else:
                outcome = _create_send_plan(cur, finding, req, now)
            status = outcome.pop("status")
            target_type = outcome.pop("target_type")
            target_id = outcome.pop("target_id")
            before = outcome.pop("before", {})
            after = outcome.pop("after", {})
            record = _record_compensation(
                cur, job_id=finding["job_id"], finding_id=finding_id,
                action=action, operator=operator, status=status,
                target_type=target_type, target_id=target_id, request=request,
                before=before, after=after, basis=basis, reason=req.note,
                error=None, now=now)
            if status in ("applied", "noop") and \
                    finding["status"] != FINDING_RESOLVED:
                _set_finding_status(cur, finding_id, FINDING_RESOLVED, now)
            return {"result": status, "compensation_id": record["id"],
                    "target_type": target_type, "target_id": target_id,
                    "details": outcome}
        except HTTPException as exc:
            _record_compensation(
                cur, job_id=finding["job_id"], finding_id=finding_id,
                action=action, operator=operator, status="failed",
                target_type="unknown", target_id=None,
                request=request, before=before, after={}, basis=basis,
                reason=req.note, error=str(exc.detail), now=now)
            _set_finding_status(cur, finding_id, FINDING_FAILED, now)
            raise


def process_compensation_plans(db: Database, now: float | None = None,
                               limit: int = 100) -> dict:
    """把补偿计划应用为既有发送任务的一次新调度；不做通道 IO。"""
    now = time.time() if now is None else now
    applied = superseded = 0
    rows = db.query(
        """SELECT * FROM notif_compensation_send_plans
           WHERE status='scheduled' ORDER BY id LIMIT ?""", (limit,))
    for plan in rows:
        with db.tx() as cur:
            latest_plan = cur.execute(
                "SELECT * FROM notif_compensation_send_plans WHERE id=?",
                (plan["id"],)).fetchone()
            if latest_plan is None or latest_plan["status"] != PLAN_SCHEDULED:
                continue
            task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                               (plan["task_id"],)).fetchone()
            if task is None:
                cur.execute(
                    "UPDATE notif_compensation_send_plans SET status='cancelled',"
                    " updated_at=? WHERE id=?", (now, plan["id"]))
                continue
            # 并发回执/发送已经造成外部效果：计划永不应用。
            if _has_external_success_effect(cur, task["id"]):
                cur.execute(
                    """UPDATE notif_compensation_send_plans SET status=?,
                       applied_at=?, updated_at=? WHERE id=?""",
                    (PLAN_SUPERSEDED, now, now, plan["id"]))
                audit.record(cur, "notif_compensation_plan_superseded", None, None, {
                    "plan_id": plan["id"], "send_task_id": task["id"],
                    "reason": "external_success_observed"}, ts=now)
                superseded += 1
                continue
            notif_quota.bump_generation_tx(
                cur, task, "reconciliation_compensation_send", now)
            cur.execute(
                """UPDATE notif_send_tasks SET status='pending', attempt_index=0,
                   current_channel=NULL, round=round+1, next_retry_at=NULL,
                   last_error=NULL, quarantined_at=NULL, cancelled_reason=NULL,
                   receipt_status='resending', receipt_reason=NULL,
                   updated_at=? WHERE id=?""", (now, task["id"]))
            cur.execute(
                """INSERT INTO notif_channel_switches
                   (task_id,event_id,recipient,from_channel,to_channel,reason,
                    detail,created_at)
                   VALUES (?,?,?,NULL,NULL,'reconciliation_compensation',?,?)""",
                (task["id"], task["event_id"], task["recipient"],
                 _json_dumps({"plan_id": plan["id"], "operator": plan["operator"]}),
                 now))
            cur.execute(
                """UPDATE notif_compensation_send_plans SET status='applied',
                   applied_at=?, updated_at=? WHERE id=?""",
                (now, now, plan["id"]))
            cur.execute(
                "UPDATE notif_reconciliation_findings SET status='compensating', "
                "updated_at=? WHERE id=?", (now, plan["finding_id"]))
            audit.record(cur, "notif_compensation_plan_applied", None, None, {
                "plan_id": plan["id"], "send_task_id": task["id"],
                "finding_id": plan["finding_id"], "operator": plan["operator"]},
                ts=now)
            applied += 1
    return {"applied": applied, "superseded": superseded}


# ---- 查询 --------------------------------------------------------------------

def _job_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "operator": r["operator"], "status": r["status"],
            "filters": _loads(r["filters_json"], {}),
            "route_version": r["route_version"],
            "quota_version": r["quota_version"],
            "receipt_policy": _loads(r["receipt_policy_json"], {}),
            "phase": r["phase"], "cursor": {
                "task_id": r["cursor_task_id"],
                "message_id": r["cursor_message_id"],
                "receipt_id": r["cursor_receipt_id"],
                "reservation_id": r["cursor_reservation_id"]},
            "scanned_count": r["scanned_count"],
            "findings_count": r["findings_count"], "page_size": r["page_size"],
            "attempts": r["attempts"], "last_error": r["last_error"],
            "paused_by": r["paused_by"], "paused_at": r["paused_at"],
            "resumed_count": r["resumed_count"], "started_at": r["started_at"],
            "completed_at": r["completed_at"], "created_at": r["created_at"],
            "updated_at": r["updated_at"],
            "next_retry_at": r["next_retry_at"] if "next_retry_at" in r.keys()
            else None}


def list_jobs(db: Database, *, status: str | None = None,
              limit: int = 100, offset: int = 0) -> dict:
    sql = "SELECT * FROM notif_reconciliation_jobs"
    params: list = []
    if status:
        sql += " WHERE status=?"
        params.append(status)
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    rows = db.query(sql, (*params, limit, offset))
    total = db.query_one(
        "SELECT COUNT(*) AS c FROM notif_reconciliation_jobs"
        + (" WHERE status=?" if status else ""),
        (status,) if status else ())["c"]
    return {"jobs": [_job_view(r) for r in rows], "count": len(rows),
            "total": total, "limit": limit, "offset": offset}


def get_job(db: Database, job_id: int) -> dict:
    row = db.query_one("SELECT * FROM notif_reconciliation_jobs WHERE id=?",
                       (job_id,))
    if row is None:
        raise HTTPException(404, "reconciliation job not found")
    return {"job": _job_view(row)}


def _finding_view(r: sqlite3.Row, *, snapshot: bool = True) -> dict:
    out = {"id": r["id"], "job_id": r["job_id"], "anomaly_key": r["anomaly_key"],
           "entity_type": r["entity_type"], "entity_id": r["entity_id"],
           "severity": r["severity"], "reason": r["reason"], "status": r["status"],
           "recipient": r["recipient"], "event_id": r["event_id"],
           "event_type": r["event_type"], "send_task_id": r["send_task_id"],
           "receipt_id": r["receipt_id"],
           "reservation_id": r["reservation_id"], "message_pk": r["message_pk"],
           "suggested_actions": _loads(r["suggested_actions_json"], []),
           "detected_at": r["detected_at"], "updated_at": r["updated_at"]}
    if snapshot:
        out["snapshot"] = _loads(r["snapshot_json"], {})
        out["evidence"] = _loads(r["evidence_json"], {})
    return out


def list_findings(db: Database, *, job_id: int | None = None,
                  status: str | None = None, recipient: str | None = None,
                  event_type: str | None = None, reason: str | None = None,
                  entity_type: str | None = None, limit: int = 100,
                  offset: int = 0) -> dict:
    where, params = [], []
    for col, val in (("job_id", job_id), ("status", status),
                     ("recipient", recipient), ("event_type", event_type),
                     ("reason", reason), ("entity_type", entity_type)):
        if val is not None:
            where.append(f"{col}=?")
            params.append(val)
    sql = "SELECT * FROM notif_reconciliation_findings"
    if where:
        sql += " WHERE " + " AND ".join(where)
    count_sql = "SELECT COUNT(*) AS c FROM notif_reconciliation_findings"
    if where:
        count_sql += " WHERE " + " AND ".join(where)
    total = db.query_one(count_sql, tuple(params))["c"]
    sql += " ORDER BY id LIMIT ? OFFSET ?"
    rows = db.query(sql, (*params, limit, offset))
    return {"findings": [_finding_view(r) for r in rows], "count": len(rows),
            "total": total, "limit": limit, "offset": offset}


def get_finding(db: Database, finding_id: int) -> dict:
    row = db.query_one("SELECT * FROM notif_reconciliation_findings WHERE id=?",
                       (finding_id,))
    if row is None:
        raise HTTPException(404, "reconciliation finding not found")
    compensations = db.query(
        "SELECT * FROM notif_reconciliation_compensations WHERE finding_id=? "
        "ORDER BY id", (finding_id,))
    plans = db.query(
        "SELECT * FROM notif_compensation_send_plans WHERE finding_id=? "
        "ORDER BY id", (finding_id,))
    return {"finding": _finding_view(row),
            "compensations": [_compensation_view(c) for c in compensations],
            "send_plans": [_plan_view(p) for p in plans]}


def list_compensations(db: Database, *, job_id: int | None = None,
                       finding_id: int | None = None, operator: str | None = None,
                       action: str | None = None, status_filter: str | None = None,
                       limit: int = 100, offset: int = 0) -> dict:
    where, params = [], []
    for col, val in (("job_id", job_id), ("finding_id", finding_id),
                     ("operator", operator), ("action", action),
                     ("status", status_filter)):
        if val is not None:
            where.append(f"{col}=?")
            params.append(val)
    sql = "SELECT * FROM notif_reconciliation_compensations"
    if where:
        sql += " WHERE " + " AND ".join(where)
    total_sql = "SELECT COUNT(*) AS c FROM notif_reconciliation_compensations"
    if where:
        total_sql += " WHERE " + " AND ".join(where)
    total = db.query_one(total_sql, tuple(params))["c"]
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    rows = db.query(sql, (*params, limit, offset))
    return {"compensations": [_compensation_view(r) for r in rows],
            "count": len(rows), "total": total, "limit": limit,
            "offset": offset}


def _plan_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "finding_id": r["finding_id"], "job_id": r["job_id"],
            "task_id": r["task_id"], "plan_key": r["plan_key"],
            "status": r["status"], "operator": r["operator"], "note": r["note"],
            "scheduled_at": r["scheduled_at"], "applied_at": r["applied_at"],
            "created_at": r["created_at"], "updated_at": r["updated_at"]}


def list_plans(db: Database, *, status: str | None = None,
               task_id: int | None = None, job_id: int | None = None,
               limit: int = 100, offset: int = 0) -> dict:
    where, params = [], []
    if status:
        where.append("status=?")
        params.append(status)
    if task_id is not None:
        where.append("task_id=?")
        params.append(task_id)
    if job_id is not None:
        where.append("job_id=?")
        params.append(job_id)
    sql = "SELECT * FROM notif_compensation_send_plans"
    if where:
        sql += " WHERE " + " AND ".join(where)
    total_sql = "SELECT COUNT(*) AS c FROM notif_compensation_send_plans"
    if where:
        total_sql += " WHERE " + " AND ".join(where)
    total = db.query_one(total_sql, tuple(params))["c"]
    sql += " ORDER BY id DESC LIMIT ? OFFSET ?"
    rows = db.query(sql, (*params, limit, offset))
    return {"plans": [_plan_view(r) for r in rows], "count": len(rows),
            "total": total, "limit": limit, "offset": offset}


def list_job_events(db: Database, job_id: int, *, limit: int = 100,
                    offset: int = 0) -> dict:
    where = ["type LIKE 'notif_%reconciliation%' OR type IN "
             "('notif_compensation_plan_applied','notif_compensation_plan_superseded')"]
    # 覆盖审计事件名中的 notification 拼写（全部以 notif_reconciliation/notif_compensation 开头）。
    where = ["(type LIKE 'notif_reconciliation%' OR type LIKE "
             "'notif_compensation%')"]
    where.append(
        "CAST(json_extract(detail,'$.job_id') AS INTEGER)=?")
    rows = db.query(
        f"""SELECT id,ts,type,detail FROM events WHERE {' AND '.join(where)}
            ORDER BY id DESC LIMIT ? OFFSET ?""", (job_id, limit, offset))
    total = db.query_one(
        f"""SELECT COUNT(*) AS c FROM events WHERE {' AND '.join(where)}""",
        (job_id,))["c"]
    return {"events": [{"id": r["id"], "ts": r["ts"], "type": r["type"],
                        "detail": _loads(r["detail"], {})} for r in rows],
            "count": len(rows), "total": total, "limit": limit,
            "offset": offset}


# ---- 路由 --------------------------------------------------------------------

def create_reconciliation_router(db: Database) -> APIRouter:
    router = APIRouter(prefix="/admin/approval-notifications/reconciliation",
                       tags=["approval-notification-reconciliation"])

    @router.post("/jobs")
    def create_job(req: ReconciliationRequest):
        return create_reconciliation(db, req)

    @router.get("/jobs")
    def jobs(status: str | None = None, limit: int = Query(100, le=1000),
             offset: int = Query(0, ge=0)):
        return list_jobs(db, status=status, limit=limit, offset=offset)

    @router.get("/jobs/{job_id}")
    def job_detail(job_id: int):
        return get_job(db, job_id)

    @router.post("/jobs/{job_id}/pause")
    def pause(job_id: int, req: PauseRequest):
        return pause_job(db, job_id, req)

    @router.post("/jobs/{job_id}/resume")
    def resume(job_id: int, req: PauseRequest):
        return resume_job(db, job_id, req.operator)

    @router.post("/jobs/{job_id}/retry")
    def retry(job_id: int, req: PauseRequest):
        return retry_failed_job(db, job_id, req.operator)

    @router.get("/jobs/{job_id}/findings")
    def findings(job_id: int, status: str | None = None,
                 recipient: str | None = None, event_type: str | None = None,
                 reason: str | None = None, entity_type: str | None = None,
                 limit: int = Query(100, le=1000),
                 offset: int = Query(0, ge=0)):
        return list_findings(db, job_id=job_id, status=status,
                             recipient=recipient, event_type=event_type,
                             reason=reason, entity_type=entity_type,
                             limit=limit, offset=offset)

    @router.get("/jobs/{job_id}/events")
    def job_events(job_id: int, limit: int = Query(100, le=1000),
                   offset: int = Query(0, ge=0)):
        return list_job_events(db, job_id, limit=limit, offset=offset)

    @router.get("/findings")
    def all_findings(status: str | None = None, recipient: str | None = None,
                     event_type: str | None = None, reason: str | None = None,
                     entity_type: str | None = None,
                     limit: int = Query(100, le=1000),
                     offset: int = Query(0, ge=0)):
        return list_findings(db, status=status, recipient=recipient,
                             event_type=event_type, reason=reason,
                             entity_type=entity_type, limit=limit, offset=offset)

    @router.get("/findings/{finding_id}")
    def finding_detail(finding_id: int):
        return get_finding(db, finding_id)

    @router.post("/findings/{finding_id}/compensations/{action}")
    def compensate(finding_id: int, action: str, req: CompensationRequest):
        return apply_compensation(db, finding_id, action, req)

    @router.get("/compensations")
    def compensations(job_id: int | None = None, finding_id: int | None = None,
                      operator: str | None = None, action: str | None = None,
                      status: str | None = None,
                      limit: int = Query(100, le=1000),
                      offset: int = Query(0, ge=0)):
        return list_compensations(db, job_id=job_id, finding_id=finding_id,
                                  operator=operator, action=action,
                                  status_filter=status, limit=limit,
                                  offset=offset)

    @router.get("/send-plans")
    def send_plans(status: str | None = None, task_id: int | None = None,
                   job_id: int | None = None, limit: int = Query(100, le=1000),
                   offset: int = Query(0, ge=0)):
        return list_plans(db, status=status, task_id=task_id, job_id=job_id,
                          limit=limit, offset=offset)

    return router

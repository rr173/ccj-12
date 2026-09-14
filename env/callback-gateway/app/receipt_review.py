"""回执失败复核案件（receipt exception review cases）的自动建案。

失败回执（``bounced`` 等）在驱动既有回执状态机（自动故障转移 / 转人工）的同时，
必须为管理员生成一条**可供处理的复核案件**，并把现场固化为不可变证据：

- 接收人（``recipient``）、事件类型（``event_type``，如 bounced）与通道/外部消息
  编号锚点直接冗余在案件行，管理员列表即可筛选处理；
- 证据（``receipt_review_evidence``，只增不改）：回执原文（含 raw_body 与报文
  哈希）、外部消息登记行快照、路由发送任务快照，建案后绝不改写；
- **幂等**：去重单元 ``case_key=receipt:{receipt_id}`` 受 UNIQUE 约束，同一条回执
  最多只建一案，重复收到同一回执不可能产生第二条案件；同一外部消息的多条（内容
  不同的）失败回执证据追加进同一案件，已落盘字段绝不被后续回执覆盖。

所有函数都在调用方的写事务内执行（与回执落盘/状态机同一事务，提交后才 2xx），
因此服务重启、并发投递都不会出现「回执已处理但案件丢失」或重复建案。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time

from fastapi import APIRouter, Query

from .db import Database

# 自动建案的失败事件（与 receipts.FAILURE_EVENTS 同构，独立声明避免循环导入）
FAILURE_EVENT_TYPES = ("bounced", "complained", "expired")

CASE_SUBJECT_RECEIPT = "receipt"


# ---- 小工具 ------------------------------------------------------------------

def _json_dumps(obj) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True)


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _row_dict(row: sqlite3.Row | None) -> dict | None:
    return dict(row) if row is not None else None


def _sla_deadline(cur: sqlite3.Cursor, ts: float) -> float | None:
    policy = cur.execute(
        "SELECT sla_seconds FROM receipt_review_policy WHERE id=1").fetchone()
    return ts + float(policy["sla_seconds"]) if policy is not None else None


def _add_evidence(cur, *, case_id: int, kind: str, ref_type: str, ref_id: int | None,
                  title: str, snapshot: dict, ts: float,
                  added_by: str = "system") -> bool:
    """追加一条不可变证据。返回是否真的新增（唯一索引兜底重复引用）。"""
    content = _json_dumps(snapshot)
    cur.execute(
        """INSERT OR IGNORE INTO receipt_review_evidence
           (case_id, kind, ref_type, ref_id, title, content_sha256,
            snapshot_json, added_by, created_at)
           VALUES (?,?,?,?,?,?,?,?,?)""",
        (case_id, kind, ref_type, ref_id, title, _sha256_text(content),
         content, added_by, ts))
    return cur.rowcount == 1


def _receipt_snapshot(receipt: sqlite3.Row) -> dict:
    """回执证据快照：原文/哈希/验签 kid 等核对所需字段全部固化，之后不依赖原表。"""
    keep = ("id", "channel", "message_id", "event", "recipient", "receipt_hash",
            "raw_body", "content_type", "signature_kid", "provider_ts", "matched",
            "message_pk", "send_task_id", "received_at")
    return {k: receipt[k] for k in keep if k in receipt.keys()}


# ---- 自动建案（事务内） --------------------------------------------------------

def _create_case(cur, *, receipt: sqlite3.Row,
                 msg: sqlite3.Row | None, ts: float) -> sqlite3.Row:
    """建一条新失败回执案件并固化全部证据。调用前已确认案件不存在。"""
    event_type = receipt["event"]
    receipt_id = receipt["id"]
    task = None
    if msg is not None and msg["send_task_id"] is not None:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (msg["send_task_id"],)).fetchone()
    cur.execute(
        """INSERT INTO receipt_review_cases
           (case_key, status, subject, receipt_id, task_id, delivery_id,
            message_pk, recipient, channel, message_id, event_type, source,
            priority, sla_deadline, created_at, updated_at)
           VALUES (?, 'open', ?, ?, ?, ?, ?, ?, ?, ?, ?, 'auto', 'normal', ?, ?, ?)""",
        (f"receipt:{receipt_id}", CASE_SUBJECT_RECEIPT, receipt_id,
         task["id"] if task is not None else (msg["send_task_id"] if msg else None),
         msg["delivery_id"] if msg is not None else None,
         msg["id"] if msg is not None else None,
         receipt["recipient"], receipt["channel"], receipt["message_id"],
         event_type, _sla_deadline(cur, ts), ts, ts))
    case = cur.execute("SELECT * FROM receipt_review_cases WHERE id=?",
                       (cur.lastrowid,)).fetchone()

    # 证据 1：回执原文（「原始消息证据」——失败通知的报文现场）
    _add_evidence(cur, case_id=case["id"], kind="receipt",
                  ref_type="receipts", ref_id=receipt_id,
                  title=f"{event_type} 回执原文（{receipt['channel']}）",
                  snapshot=_receipt_snapshot(receipt), ts=ts)
    # 证据 2：外部消息登记行（接收人/通道/外部编号/发送单元）
    if msg is not None:
        _add_evidence(cur, case_id=case["id"], kind="message",
                      ref_type="external_messages", ref_id=msg["id"],
                      title="外部消息登记快照", snapshot=_row_dict(msg), ts=ts)
    # 证据 3：路由发送任务（通知事件/通道计划/当前状态）
    if task is not None:
        _add_evidence(cur, case_id=case["id"], kind="task",
                      ref_type="notif_send_tasks", ref_id=task["id"],
                      title="发送任务快照", snapshot=_row_dict(task), ts=ts)
    return case


def _append_receipt_evidence(cur, *, case: sqlite3.Row,
                             receipt: sqlite3.Row, ts: float) -> bool:
    """同一外部消息的又一条失败回执：证据追加进既有案件，案件字段不改写。"""
    added = _add_evidence(
        cur, case_id=case["id"], kind="receipt", ref_type="receipts",
        ref_id=receipt["id"],
        title=f"{receipt['event']} 回执原文（{receipt['channel']}）",
        snapshot=_receipt_snapshot(receipt), ts=ts)
    if added:
        cur.execute("UPDATE receipt_review_cases SET updated_at=? WHERE id=?",
                    (ts, case["id"]))
    return added


def relink_case_receipt_tx(cur: sqlite3.Cursor, *, receipt_id: int,
                           msg: sqlite3.Row, ts: float | None = None) -> dict | None:
    """待核对失败回执后来绑定/重放匹配到消息：复用既有案件（绝不建第二案），
    补全关联列并追加消息/任务证据。案件不存在（历史库）则按常规幂等建案。"""
    ts = time.time() if ts is None else ts
    receipt = cur.execute("SELECT * FROM receipts WHERE id=?",
                          (receipt_id,)).fetchone()
    if receipt is None or receipt["event"] not in FAILURE_EVENT_TYPES:
        return None
    case = cur.execute(
        "SELECT * FROM receipt_review_cases WHERE case_key=?",
        (f"receipt:{receipt_id}",)).fetchone()
    if case is None:
        return ensure_case_for_failure_receipt_tx(
            cur, receipt=receipt, msg=msg, ts=ts)
    # 冗余关联列在证据补全时填空（NULL 容忍），已落盘字段不改写
    task = None
    if msg["send_task_id"] is not None:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (msg["send_task_id"],)).fetchone()
    cur.execute(
        """UPDATE receipt_review_cases
           SET message_pk=COALESCE(message_pk,?),
               task_id=COALESCE(task_id,?), delivery_id=COALESCE(delivery_id,?),
               recipient=COALESCE(recipient,?), updated_at=?
           WHERE id=?""",
        (msg["id"], msg["send_task_id"], msg["delivery_id"],
         receipt["recipient"], ts, case["id"]))
    added = [
        _add_evidence(cur, case_id=case["id"], kind="message",
                      ref_type="external_messages", ref_id=msg["id"],
                      title="外部消息登记快照（绑定补全）",
                      snapshot=_row_dict(msg), ts=ts)]
    if task is not None:
        added.append(_add_evidence(
            cur, case_id=case["id"], kind="task",
            ref_type="notif_send_tasks", ref_id=task["id"],
            title="发送任务快照（绑定补全）", snapshot=_row_dict(task), ts=ts))
    if any(added):
        from . import audit
        audit.record(cur, "receipt_review_case_relinked", None, None, {
            "case_id": case["id"], "receipt_id": receipt_id,
            "external_message_pk": msg["id"]}, ts=ts)
    return {"case_id": case["id"], "created": False}


def ensure_case_for_failure_receipt_tx(cur: sqlite3.Cursor, *,
                                       receipt: sqlite3.Row,
                                       msg: sqlite3.Row | None,
                                       ts: float | None = None) -> dict | None:
    """确保一条失败回执有且仅有一条复核案件（在调用方事务内）。

    返回 ``{"case_id", "created"}``；非失败事件返回 None。

    - ``case_key=receipt:{id}`` 已存在（同一回执重复驱动，如重放）：直接返回既有
      案件，不再建第二条；
    - 同一外部消息已有失败案件（乱序/内容不同的重复失败回执）：不建新案，只把本
      回执原文追加为证据；
    - 否则建案并固化回执原文 + 消息/任务快照。
    """
    ts = time.time() if ts is None else ts
    event_type = receipt["event"]
    if event_type not in FAILURE_EVENT_TYPES:
        return None
    receipt_id = receipt["id"]

    case = cur.execute(
        "SELECT * FROM receipt_review_cases WHERE case_key=?",
        (f"receipt:{receipt_id}",)).fetchone()
    created = False
    if case is not None:
        # 本回执已有自己的案件（重复驱动/重放）：直接返回，绝不建第二案、不重复留证
        return {"case_id": case["id"], "created": False}
    if msg is not None:
        # 本条回执自己没有案件：同一外部消息的失败证据归集到既有首案
        case = cur.execute(
            """SELECT * FROM receipt_review_cases
               WHERE subject=? AND message_pk=? ORDER BY id LIMIT 1""",
            (CASE_SUBJECT_RECEIPT, msg["id"])).fetchone()
    if case is None:
        from . import audit
        case = _create_case(cur, receipt=receipt, msg=msg, ts=ts)
        audit.record(cur, "receipt_review_case_created", None, None, {
            "case_id": case["id"], "receipt_id": receipt_id,
            "external_message_pk": case["message_pk"],
            "channel": receipt["channel"], "message_id": receipt["message_id"],
            "recipient": receipt["recipient"], "event_type": event_type}, ts=ts)
        created = True
    else:
        from . import audit
        appended = _append_receipt_evidence(cur, case=case, receipt=receipt, ts=ts)
        if appended:
            audit.record(cur, "receipt_review_evidence_appended", None, None, {
                "case_id": case["id"], "receipt_id": receipt_id,
                "event_type": event_type}, ts=ts)
    return {"case_id": case["id"], "created": created}


# ---- 管理员查询 ----------------------------------------------------------------

def _case_view(row: sqlite3.Row) -> dict:
    return {
        "case_id": row["id"], "case_key": row["case_key"], "status": row["status"],
        "subject": row["subject"], "receipt_id": row["receipt_id"],
        "task_id": row["task_id"], "delivery_id": row["delivery_id"],
        "message_pk": row["message_pk"], "recipient": row["recipient"],
        "channel": row["channel"], "message_id": row["message_id"],
        "event_type": row["event_type"], "source": row["source"],
        "priority": row["priority"], "owner": row["owner"],
        "resolution": row["resolution"], "sla_deadline": row["sla_deadline"],
        "created_at": row["created_at"], "updated_at": row["updated_at"]}


def list_cases(db: Database, *, status: str | None = None,
               recipient: str | None = None, event_type: str | None = None,
               limit: int = 100) -> dict:
    where, params = [], []
    if status:
        where.append("status=?")
        params.append(status)
    if recipient:
        where.append("recipient=?")
        params.append(recipient)
    if event_type:
        where.append("event_type=?")
        params.append(event_type)
    sql = "SELECT * FROM receipt_review_cases"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"cases": [_case_view(r) for r in rows], "count": len(rows)}


def get_case(db: Database, case_id: int) -> dict:
    row = db.query_one("SELECT * FROM receipt_review_cases WHERE id=?", (case_id,))
    if row is None:
        from fastapi import HTTPException
        raise HTTPException(404, "review case not found")
    evidence = db.query(
        "SELECT * FROM receipt_review_evidence WHERE case_id=? ORDER BY id",
        (case_id,))
    decisions = db.query(
        "SELECT * FROM receipt_review_decisions WHERE case_id=? ORDER BY id",
        (case_id,))
    return {
        "case": _case_view(row),
        "evidence": [{
            "id": e["id"], "kind": e["kind"], "ref_type": e["ref_type"],
            "ref_id": e["ref_id"], "title": e["title"],
            "content_sha256": e["content_sha256"],
            "snapshot": json.loads(e["snapshot_json"]),
            "added_by": e["added_by"], "created_at": e["created_at"]}
            for e in evidence],
        "decisions": [{
            "id": d["id"], "action": d["action"], "operator": d["operator"],
            "reason": d["reason"], "note": d["note"], "status": d["status"],
            "effect": d["effect"], "created_at": d["created_at"]}
            for d in decisions]}


def create_review_router(db: Database) -> APIRouter:
    """复核案件管理员只读端点（建案由回执链路自动完成）。"""
    router = APIRouter(prefix="/admin/receipt-review", tags=["receipt-review"])

    @router.get("/cases")
    def cases(status: str | None = None, recipient: str | None = None,
              event_type: str | None = None, limit: int = Query(100, le=1000)):
        """复核案件列表：可按状态/接收人/事件类型筛选。"""
        return list_cases(db, status=status, recipient=recipient,
                          event_type=event_type, limit=limit)

    @router.get("/cases/{case_id}")
    def case_detail(case_id: int):
        """案件详情：不可变证据（回执原文/消息/任务快照）+ 处理决定轨迹。"""
        return get_case(db, case_id)

    return router

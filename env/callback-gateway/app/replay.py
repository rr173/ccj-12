"""业务重放模块：可追溯的历史回调重放。

流程：筛选预览（preview）-> 提交批次（submit）-> [高风险须审批] ->
后台重放 worker 逐条执行 -> 副作用走与正常处理完全相同的 outbox 幂等派发链路。

- 每条重放任务记录发起人、原始版本（delivery_id）、原因和进度（状态/次数/checkpoint）；
- 同一份内容不会生成第二个重放任务：批内 UNIQUE(batch_id, delivery_id) 去重；
  提交带 request_id 时重复提交返回原批次；跨批存在活动任务（pending/processing，
  含待审批批次占住的任务）的投递会被跳过（已完成的批次不阻塞以后再次重放
  ——那是有意为之的新批次）；
- 高风险审批：提交时可用 risk_level=high（并填 approval_note）标记高风险批次，
  批次进入 pending_approval 而不是 running，worker 在批准前不能领取其任何任务
  （跨批活动检查同时占住对应内容，防止绕过审批另开一批）；批准必须由不同于
  发起人的运营人员显式做出（POST .../approve），批准后批次才进入 running；
  拒绝（POST .../reject，必填拒绝原因）或超时（approval_deadline 到期由 worker
  扫描释放）把未执行任务整体置为终态 cancelled。批准/拒绝都是条件状态转移，
  重复批准/拒绝不会产生第二次效果；request_id 重复提交也不会产生第二次执行；
- 批次级并发配额：提交时可指定 max_concurrency（整个批次最多同时处理多少条），
  占用量 = 本批 processing 中的任务数，领取时在占位事务里实时推导——任务离开
  processing（完成/失败/取消/重启回收）槽位即释放，不存在需要单独回收的计数器，
  暂停/取消/重启后配额天然正确，任务不会永久卡住；
- 同一编号有序执行：同批次同 external_id 的多条历史版本按版本落盘时间
  （delivery_created_at 快照，再按 delivery_id 决胜）先后执行，前一条未进终态
  （done/failed/cancelled）时后一条不能被 worker 领取；
- 被配额/顺序挡住的任务：worker 在原因变化时写 replay_task_blocked 审计（轮询不
  重复刷），批次详情实时展示当前占用、等待数量和每条任务的阻塞原因；
- 批次可暂停/继续/取消；取消时未执行的任务与滞留的待派发副作用同事务取消；
  正在处理的任务同样标记 cancelled，其迟到的完成/失败结果落库时被条件更新
  挡下（保持 cancelled，不落副作用、不写完成记录），审计留 discarded 事件；
- 重启后 recover() 把卡在 processing 的任务退回 pending，从上次位置（attempts/
  checkpoint/next_retry_at 都落库）继续，占用的配额随之释放；
- 失败按指数退避单独重试，超限标记 failed，可人工单条重试，不阻塞其他编号；
- 重放副作用的幂等键以 replay:{task_id} 为作用域：同一任务重试/重启不会重复派发，
  下游仍按幂等键去重（与正常处理同一套 outbox + sink 保护）；新批次新任务才会
  真正再次产生外部效果——这正是重放的目的。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import audit
from .config import Settings
from .db import Database
from .handlers import business_handler

log = logging.getLogger("gateway.replay")


def replay_effect_key(task_id: int, effect_type: str, effect_payload: dict) -> str:
    """重放副作用幂等键：以重放任务为作用域，同一任务重算结果恒定。

    任务重试/服务重启 -> 键相同 -> INSERT OR IGNORE 去重，不会重复派发；
    下游 sink 也按该键去重，与正常处理同一套 exactly-once 保护。
    """
    canonical = json.dumps(effect_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(f"replay:{task_id}:{effect_type}:{canonical}".encode()).hexdigest()


# ---- 筛选与预览 ------------------------------------------------------------

class ReplayFilter(BaseModel):
    """历史回调筛选条件：编号、时间范围、处理结果，或直接指定版本号列表。"""
    external_id: str | None = None        # 业务编号
    status: str | None = None             # 处理结果：done|quarantined|pending|conflicted|superseded
    created_from: float | str | None = None  # 落盘时间起（epoch 秒或 ISO-8601）
    created_to: float | str | None = None    # 落盘时间止
    delivery_ids: list[int] | None = None    # 直接指定内容版本


class PreviewRequest(ReplayFilter):
    pass


class SubmitRequest(ReplayFilter):
    operator: str                  # 发起人（必填，落每条任务与审计）
    reason: str                    # 重放原因（必填）
    request_id: str | None = None  # 提交幂等键：重复提交返回原批次
    # 批次级并发配额：整个批次最多同时处理多少条任务；NULL 表示不限
    max_concurrency: int | None = Field(default=None, ge=1)
    # 风险等级：high 为高风险，必须由非发起人批准后才能进入 running；normal 直接运行
    risk_level: str = "normal"
    # 审批说明：高风险批次必填（为什么要做这次高风险重放，供审批人判断与事后追溯）
    approval_note: str | None = None


class BatchActionRequest(BaseModel):
    operator: str
    note: str = ""


class ApprovalRequest(BaseModel):
    """高风险批次的批准/拒绝请求：审批人必须是不同于发起人的运营人员。"""
    operator: str                  # 审批人（必填，须不同于批次发起人）
    note: str = ""                 # 批准备注（可选）


class RejectionRequest(BaseModel):
    operator: str                  # 审批人（必填，须不同于批次发起人）
    reason: str                    # 拒绝原因（必填，落批次与审计）
    note: str = ""


# 批次/审批状态
BATCH_RUNNING = "running"
BATCH_PAUSED = "paused"
BATCH_PENDING_APPROVAL = "pending_approval"
BATCH_REJECTED = "rejected"
BATCH_CANCELLED = "cancelled"
RISK_LEVELS = ("normal", "high")
APPROVAL_NOT_REQUIRED = "not_required"
APPROVAL_PENDING = "pending"
APPROVAL_APPROVED = "approved"
APPROVAL_REJECTED = "rejected"
APPROVAL_EXPIRED = "expired"


def _require_operator(operator: str, field: str = "operator"):
    if not operator or not operator.strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return operator.strip()


def _parse_time(value) -> float | None:
    """时间筛选支持 epoch 秒（数字）或 ISO-8601 字符串。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise HTTPException(422, f"invalid time filter: {value!r} "
                                 "(expect epoch seconds or ISO-8601)")


def _select_deliveries(db: Database, f: ReplayFilter):
    """预览与提交共用同一套筛选，保证「看到的就是将要提交的」。"""
    sql, params = "SELECT * FROM deliveries WHERE 1=1", []
    if f.external_id:
        sql += " AND external_id=?"
        params.append(f.external_id)
    if f.status:
        sql += " AND status=?"
        params.append(f.status)
    t_from, t_to = _parse_time(f.created_from), _parse_time(f.created_to)
    if t_from is not None:
        sql += " AND created_at>=?"
        params.append(t_from)
    if t_to is not None:
        sql += " AND created_at<=?"
        params.append(t_to)
    if f.delivery_ids:
        sql += f" AND id IN ({','.join('?' * len(f.delivery_ids))})"
        params.extend(f.delivery_ids)
    sql += " ORDER BY id"
    return db.query(sql, tuple(params))


def _skip_reason(delivery) -> str | None:
    """哪些版本不允许重放（返回 None 表示可重放）。

    只允许重放已脱离正常管线的版本（done/quarantined）：仍在管线中的、
    冲突冻结中的、人工未选中的版本重放出去会产生不该有的外部效果。
    """
    status = delivery["status"]
    if status == "superseded":
        return "superseded_version"
    if delivery["frozen"] or status == "conflicted":
        return "frozen_by_conflict"
    if status in ("pending", "processing"):
        return "still_in_pipeline"
    return None


def _active_replay_by_delivery(db: Database) -> dict[int, int]:
    """当前有活动重放任务（pending/processing）的 delivery -> batch_id。"""
    rows = db.query(
        "SELECT delivery_id, batch_id FROM replay_tasks WHERE status IN ('pending','processing')")
    return {r["delivery_id"]: r["batch_id"] for r in rows}


def _preview_item(db: Database, delivery, active: dict[int, int]) -> dict:
    skip = _skip_reason(delivery)
    if skip is None and delivery["id"] in active:
        skip = f"active_replay_in_batch:{active[delivery['id']]}"
    # 影响范围：该版本正常处理时产出过的外部副作用（重放将按同内容重新产出，
    # 走同一 outbox 幂等链路）；未处理过的版本则没有历史副作用可参照
    prior = db.query(
        "SELECT effect_type, payload, status FROM outbox "
        "WHERE delivery_id=? AND replay_task_id IS NULL ORDER BY id",
        (delivery["id"],),
    )
    return {
        "delivery_id": delivery["id"],
        "external_id": delivery["external_id"],
        "status": delivery["status"],
        "frozen": delivery["frozen"],
        "content_hash": delivery["content_hash"],
        "created_at": delivery["created_at"],
        "payload": delivery["payload"],
        "replayable": skip is None,
        "skip_reason": skip,
        "prior_effects": [{"effect_type": p["effect_type"], "status": p["status"],
                           "payload": json.loads(p["payload"])} for p in prior],
    }


def preview(db: Database, req: PreviewRequest) -> dict:
    """预览：将要重放的内容 + 影响范围（历史副作用），不落任何数据。"""
    rows = _select_deliveries(db, req)
    active = _active_replay_by_delivery(db)
    items = [_preview_item(db, d, active) for d in rows]
    return {"matched": len(items),
            "replayable": sum(1 for i in items if i["replayable"]),
            "items": items}


# ---- 提交批次 --------------------------------------------------------------

def submit(db: Database, req: SubmitRequest, approval_timeout: float) -> tuple[int, dict]:
    """一次性提交一批重放任务：批次 + 全部任务在一个事务里落盘。

    高风险批次（risk_level=high）落为 pending_approval：任务照常落盘并占住对应
    内容（其他批次不能再提交同一投递），但 worker 在非发起人明确批准前不领取。
    """
    operator = _require_operator(req.operator)
    if not req.reason.strip():
        raise HTTPException(422, "reason must be non-empty")
    if req.risk_level not in RISK_LEVELS:
        raise HTTPException(422, f"invalid risk_level: {req.risk_level!r} "
                                 f"(expect one of {','.join(RISK_LEVELS)})")
    approval_note = (req.approval_note or "").strip()
    high_risk = req.risk_level == "high"
    if high_risk and not approval_note:
        raise HTTPException(422, "approval_note is required for high risk batches")

    # 提交幂等：同一 request_id 重复提交（网络重试/双击）返回原批次
    if req.request_id:
        existing = db.query_one(
            "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
        if existing is not None:
            return 200, {"result": "duplicate", "batch_id": existing["id"],
                         "total": existing["total"],
                         "status": existing["status"],
                         "approval_status": existing["approval_status"]}

    rows = _select_deliveries(db, req)
    active = _active_replay_by_delivery(db)
    eligible, skipped = [], []
    for d in rows:
        reason = _skip_reason(d)
        if reason is None and d["id"] in active:
            reason = f"active_replay_in_batch:{active[d['id']]}"
        if reason is not None:
            skipped.append({"delivery_id": d["id"], "external_id": d["external_id"],
                            "reason": reason})
        else:
            eligible.append(d)

    if not eligible:
        return 422, {"error": "no_replayable_deliveries",
                     "matched": len(rows), "skipped": skipped}

    now = time.time()
    if high_risk:
        batch_status = BATCH_PENDING_APPROVAL
        approval_status = APPROVAL_PENDING
        deadline = now + approval_timeout
    else:
        batch_status = BATCH_RUNNING
        approval_status = APPROVAL_NOT_REQUIRED
        deadline = None
    filters = req.model_dump(exclude={"operator", "reason", "request_id", "max_concurrency",
                                      "risk_level", "approval_note"})
    filters_json = json.dumps(filters, ensure_ascii=False, default=str)
    try:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO replay_batches
                   (request_id, operator, reason, status, filters, max_concurrency,
                    risk_level, approval_note, approval_status, approval_deadline,
                    total, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (req.request_id, operator, req.reason, batch_status, filters_json,
                 req.max_concurrency, req.risk_level,
                 approval_note or None, approval_status, deadline,
                 len(eligible), now, now),
            )
            batch_id = cur.lastrowid
            for d in eligible:
                # INSERT OR IGNORE + UNIQUE(batch_id, delivery_id)：
                # 同一份内容重复加入也不会生成第二个重放任务；
                # delivery_created_at 快照原始版本的落盘时间，作为同编号有序执行的排序键
                cur.execute(
                    """INSERT OR IGNORE INTO replay_tasks
                       (batch_id, delivery_id, external_id, operator, reason,
                        status, delivery_created_at, created_at, updated_at)
                       VALUES (?,?,?,?,?,'pending',?,?,?)""",
                    (batch_id, d["id"], d["external_id"], operator, req.reason,
                     d["created_at"], now, now),
                )
            audit.record(cur, "replay_batch_created", None, None, {
                "replay_batch_id": batch_id, "operator": operator,
                "reason": req.reason, "request_id": req.request_id,
                "max_concurrency": req.max_concurrency,
                "risk_level": req.risk_level, "approval_note": approval_note or None,
                "status": batch_status, "approval_status": approval_status,
                "approval_deadline": deadline,
                "filters": filters, "total": len(eligible), "skipped": skipped}, ts=now)
    except sqlite3.IntegrityError:
        # 并发下 request_id 撞唯一键：返回已存在的那一批
        if req.request_id:
            existing = db.query_one(
                "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
            if existing is not None:
                return 200, {"result": "duplicate", "batch_id": existing["id"],
                             "total": existing["total"],
                             "status": existing["status"],
                             "approval_status": existing["approval_status"]}
        raise

    body = {"result": "created", "batch_id": batch_id,
            "total": len(eligible), "skipped": skipped,
            "status": batch_status, "risk_level": req.risk_level,
            "approval_status": approval_status}
    if deadline is not None:
        body["approval_deadline"] = deadline
    return 201, body


# ---- 批次控制：暂停 / 继续 / 取消 --------------------------------------------

def _get_batch_or_404(db: Database, batch_id: int):
    row = db.query_one("SELECT * FROM replay_batches WHERE id=?", (batch_id,))
    if row is None:
        raise HTTPException(404, "replay batch not found")
    return row


# ---- 高风险审批：批准 / 拒绝 / 超时释放 ---------------------------------------

def _approval_guard(batch, operator: str) -> None:
    """审批操作的共同前置：批次仍待决，且审批人不是发起人本人（职责分离）。"""
    if batch["approval_status"] != APPROVAL_PENDING \
            or batch["status"] != BATCH_PENDING_APPROVAL:
        raise HTTPException(
            409, f"batch approval is {batch['approval_status']}, "
                 f"batch is {batch['status']}, decision no longer accepted")
    if operator == batch["operator"]:
        raise HTTPException(403, "approver must be different from the batch operator")


def approve(db: Database, batch_id: int, operator: str, note: str = "") -> dict:
    """批准高风险批次：审批人须不同于发起人；批准后批次进入 running，worker 方可领取。

    条件更新（仅在仍为 pending_approval 时生效）保证重复/并发批准最多放行一次，
    不会产生第二次执行。
    """
    operator = _require_operator(operator, "operator")
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        _approval_guard(batch, operator)
        changed = cur.execute(
            """UPDATE replay_batches
               SET status='running', approval_status='approved', approver=?,
                   approved_at=?, approval_reason=NULL, updated_at=?
               WHERE id=? AND status='pending_approval'
                 AND approval_status='pending'""",
            (operator, now, now, batch_id),
        ).rowcount
        if not changed:  # 并发下已被另一笔审批决定（拒绝/超时/取消）
            raise HTTPException(409, "batch is no longer awaiting approval")
        audit.record(cur, "replay_batch_approved", None, None,
                     {"replay_batch_id": batch_id, "operator": operator,
                      "submitted_by": batch["operator"], "note": note.strip(),
                      "risk_level": batch["risk_level"]}, ts=now)
    return {"result": "approved", "batch_id": batch_id, "status": BATCH_RUNNING}


def reject(db: Database, batch_id: int, operator: str, reason: str,
           note: str = "") -> dict:
    """拒绝高风险批次：拒绝原因必填；未执行的任务整体置为终态 cancelled，
    占住的投递随之释放（之后可以重新提交新批次）。拒绝不可撤销。"""
    operator = _require_operator(operator, "operator")
    reason = (reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason must be non-empty when rejecting")
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        _approval_guard(batch, operator)
        changed = cur.execute(
            """UPDATE replay_batches
               SET status='rejected', approval_status='rejected', approver=?,
                   approval_reason=?, approved_at=NULL, updated_at=?, finished_at=?
               WHERE id=? AND status='pending_approval'
                 AND approval_status='pending'""",
            (operator, reason, now, now, batch_id),
        ).rowcount
        if not changed:
            raise HTTPException(409, "batch is no longer awaiting approval")
        cancelled_tasks = cur.execute(
            """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
               finished_at=?, updated_at=?
               WHERE batch_id=? AND status IN ('pending','processing')""",
            (now, now, batch_id),
        ).rowcount
        cur.execute(
            "UPDATE replay_batches SET cancelled=cancelled+?, updated_at=? WHERE id=?",
            (cancelled_tasks, now, batch_id),
        )
        audit.record(cur, "replay_batch_rejected", None, None,
                     {"replay_batch_id": batch_id, "operator": operator,
                      "submitted_by": batch["operator"], "reason": reason,
                      "note": note.strip(), "risk_level": batch["risk_level"],
                      "cancelled_tasks": cancelled_tasks}, ts=now)
    return {"result": "rejected", "batch_id": batch_id,
            "cancelled_tasks": cancelled_tasks}


def expire_approvals(db: Database, now: float) -> int:
    """审批超时释放：超过 approval_deadline 仍待决的高风险批次整体取消。

    由 replay worker 每轮在领取任务前调用（也因此可被手动 run_once 触发）。
    条件更新保证与人工批准/拒绝互斥：谁先提交谁生效，超时不会作用到已批准的
    批次上；重复扫描不会产生第二次效果。
    """
    due = db.query(
        """SELECT * FROM replay_batches
           WHERE status='pending_approval' AND approval_status='pending'
             AND approval_deadline IS NOT NULL AND approval_deadline <= ?""",
        (now,),
    )
    for batch in due:
        with db.tx() as cur:
            changed = cur.execute(
                """UPDATE replay_batches
                   SET status='cancelled', approval_status='expired',
                       updated_at=?, finished_at=?
                   WHERE id=? AND status='pending_approval'
                     AND approval_status='pending'""",
                (now, now, batch["id"]),
            ).rowcount
            if not changed:  # 并发下已被人工批准/拒绝/取消
                continue
            cancelled_tasks = cur.execute(
                """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
                   finished_at=?, updated_at=?
                   WHERE batch_id=? AND status IN ('pending','processing')""",
                (now, now, batch["id"]),
            ).rowcount
            cur.execute(
                "UPDATE replay_batches SET cancelled=cancelled+?, updated_at=? WHERE id=?",
                (cancelled_tasks, now, batch["id"]),
            )
            audit.record(cur, "replay_batch_approval_expired", None, None,
                         {"replay_batch_id": batch["id"],
                          "submitted_by": batch["operator"],
                          "risk_level": batch["risk_level"],
                          "approval_deadline": batch["approval_deadline"],
                          "cancelled_tasks": cancelled_tasks}, ts=now)
    return len(due)


def pause(db: Database, batch_id: int, operator: str) -> dict:
    """暂停：未执行的任务不再被 worker 拉取，滞留的重放副作用暂停派发。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] != "running":
            raise HTTPException(409, f"batch is {batch['status']}, cannot pause")
        cur.execute("UPDATE replay_batches SET status='paused', updated_at=? WHERE id=?",
                    (now, batch_id))
        audit.record(cur, "replay_batch_paused", None, None,
                     {"replay_batch_id": batch_id, "operator": operator}, ts=now)
    return {"result": "paused", "batch_id": batch_id}

def resume(db: Database, batch_id: int, operator: str) -> dict:
    """继续：从暂停处恢复，未完成的任务按各自位置继续执行。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] != "paused":
            raise HTTPException(409, f"batch is {batch['status']}, cannot resume")
        cur.execute("UPDATE replay_batches SET status='running', updated_at=? WHERE id=?",
                    (now, batch_id))
        audit.record(cur, "replay_batch_resumed", None, None,
                     {"replay_batch_id": batch_id, "operator": operator}, ts=now)
    return {"result": "resumed", "batch_id": batch_id}


def cancel(db: Database, batch_id: int, operator: str, note: str = "") -> dict:
    """取消：未执行的任务标记 cancelled；正在处理的任务一并标记，其迟到的
    完成/失败结果落库时会被 worker 的条件更新挡下（保持 cancelled，不再写
    完成记录或派发副作用）；已完成任务滞留的待派发副作用同事务取消
    （已派发的外部效果无法撤回，审计里保留完整轨迹）。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] not in ("running", "paused", "pending_approval"):
            raise HTTPException(409, f"batch is {batch['status']}, cannot cancel")
        awaiting_approval = batch["status"] == "pending_approval"
        cancelled_tasks = cur.execute(
            """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
               finished_at=?, updated_at=?
               WHERE batch_id=? AND status IN ('pending','processing')""",
            (now, now, batch_id),
        ).rowcount
        # 滞留的重放副作用按任务分组取消并记审计（内容保留可查）
        stuck = cur.execute(
            """SELECT t.id AS task_id, t.external_id, t.delivery_id, COUNT(o.id) AS c
               FROM outbox o JOIN replay_tasks t ON t.id = o.replay_task_id
               WHERE t.batch_id=? AND o.status='pending'
               GROUP BY t.id""",
            (batch_id,),
        ).fetchall()
        cur.execute(
            """UPDATE outbox SET status='cancelled'
               WHERE status='pending' AND replay_task_id IN
                     (SELECT id FROM replay_tasks WHERE batch_id=?)""",
            (batch_id,),
        )
        for row in stuck:
            audit.record(cur, "effect_cancelled", row["external_id"], row["delivery_id"],
                         {"replay_batch_id": batch_id, "replay_task_id": row["task_id"],
                          "cancelled": row["c"], "reason": "replay_batch_cancelled"}, ts=now)
        # 条件更新与并发的批准/拒绝/超时互斥（写事务串行 + 状态守卫）；
        # 待审批期间取消时保留 pending 审批轨迹，另在审计里标注撤回
        expected_status = "pending_approval" if awaiting_approval else batch["status"]
        changed = cur.execute(
            """UPDATE replay_batches SET status='cancelled', cancelled=cancelled+?,
               updated_at=?, finished_at=? WHERE id=? AND status=?""",
            (cancelled_tasks, now, now, batch_id, expected_status),
        ).rowcount
        if not changed:  # 并发下批次已被批准/拒绝/超时
            raise HTTPException(409, "batch is no longer in the state read at cancel time")
        audit.record(cur, "replay_batch_cancelled", None, None,
                     {"replay_batch_id": batch_id, "operator": operator, "note": note,
                      "was_awaiting_approval": awaiting_approval,
                      "cancelled_tasks": cancelled_tasks,
                      "done": batch["done"], "failed": batch["failed"]}, ts=now)
    return {"result": "cancelled", "batch_id": batch_id,
            "cancelled_tasks": cancelled_tasks}


def retry_task(db: Database, task_id: int, operator: str) -> dict:
    """失败任务单独重试：重置计数放回队列，只影响这一条，不阻塞其他编号。"""
    now = time.time()
    with db.tx() as cur:
        task = cur.execute("SELECT * FROM replay_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise HTTPException(404, "replay task not found")
        if task["status"] != "failed":
            raise HTTPException(409, f"task is {task['status']}, not failed")
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (task["batch_id"],)).fetchone()
        if batch["status"] == "cancelled":
            raise HTTPException(409, "batch is cancelled")
        cur.execute(
            """UPDATE replay_tasks SET status='pending', attempts=0, next_retry_at=NULL,
               last_error=NULL, blocked_reason=NULL, finished_at=NULL, updated_at=? WHERE id=?""",
            (now, task_id),
        )
        # 批次若已因失败收尾，重新打开继续跑
        cur.execute(
            """UPDATE replay_batches SET failed=failed-1, updated_at=?,
               status=CASE WHEN status='completed_with_failures' THEN 'running' ELSE status END,
               finished_at=CASE WHEN status='completed_with_failures' THEN NULL ELSE finished_at END
               WHERE id=?""",
            (now, task["batch_id"]),
        )
        audit.record(cur, "replay_task_retried", task["external_id"], task["delivery_id"],
                     {"replay_batch_id": task["batch_id"], "replay_task_id": task_id,
                      "operator": operator}, ts=now)
    return {"result": "requeued", "task_id": task_id, "batch_id": task["batch_id"]}


# ---- 查询 ------------------------------------------------------------------

# 重放任务的终态：前序版本进入其中之一，同编号的后一条才允许被领取
TERMINAL_TASK_STATUSES = ("done", "failed", "cancelled")


def _version_key(task) -> tuple:
    """同编号历史版本的执行顺序：按落盘时间，再按 delivery_id 决胜（稳定且确定）。"""
    return (task["delivery_created_at"], task["delivery_id"])


def _nearest_open_predecessor(tasks, task):
    """同批次同编号、版本更早且未进终态的最近一条任务；没有则 None（可执行）。"""
    if task["delivery_created_at"] is None:
        return None  # 老库遗留行没有版本时间快照，不参与有序约束
    earlier = [t for t in tasks
               if t["external_id"] == task["external_id"]
               and t["id"] != task["id"]
               and t["delivery_created_at"] is not None
               and _version_key(t) < _version_key(task)
               and t["status"] not in TERMINAL_TASK_STATUSES]
    if not earlier:
        return None
    return max(earlier, key=_version_key)


def _live_blocked_reason(batch, task, predecessor, in_flight: int,
                         now: float) -> str | None:
    """批次详情里每条 pending 任务「为什么还没被执行」的实时原因（None 表示可执行）。

    与 worker 领取闸门的判定顺序一致：批次状态 -> 退避等待 -> 前序版本 -> 并发配额。
    """
    if task["status"] != "pending":
        return None
    if batch["status"] == "pending_approval":
        return "awaiting_approval"
    if batch["status"] == "rejected":
        return "batch_rejected"
    if batch["status"] == "paused":
        return "batch_paused"
    if batch["status"] == "cancelled":
        return "batch_cancelled"
    if batch["status"] != "running":
        return f"batch_not_running:{batch['status']}"
    if task["next_retry_at"] is not None and task["next_retry_at"] > now:
        return "retry_backoff"
    if predecessor is not None:
        return f"waiting_predecessor:{predecessor['id']}"
    maxc = batch["max_concurrency"]
    if maxc is not None and in_flight >= maxc:
        return f"quota_exhausted:{in_flight}/{maxc}"
    return None


def _approval_view(row, now: float | None = None) -> dict:
    """批次当前审批状态与操作者：发起人、审批人、决定时间/原因，以及待决是否已超时。

    approved/可执行的前提是 approval_status='approved'；expired_on_time 只用于
    详情提示——状态转移以 worker 下一轮扫描（或手动 run_once）为准。
    """
    now = time.time() if now is None else now
    view = {
        "risk_level": row["risk_level"],
        "approval_note": row["approval_note"],
        "status": row["approval_status"],
        "submitted_by": row["operator"],
        "approver": row["approver"],
        "approved_at": row["approved_at"],
        "rejection_reason": row["approval_reason"],
        "deadline": row["approval_deadline"],
        "expired_on_time": (
            row["status"] == BATCH_PENDING_APPROVAL
            and row["approval_status"] == APPROVAL_PENDING
            and row["approval_deadline"] is not None
            and row["approval_deadline"] <= now),
    }
    return view


def _batch_view(row) -> dict:
    out = {k: row[k] for k in row.keys()}
    out["filters"] = json.loads(out["filters"])
    return out


def batch_detail(db: Database, batch_id: int) -> dict:
    """批次详情：进度计数、并发占用/等待数量、审批状态/操作者、每条任务状态与
    实时阻塞原因。"""
    batch = _get_batch_or_404(db, batch_id)
    tasks = db.query("SELECT * FROM replay_tasks WHERE batch_id=? ORDER BY id", (batch_id,))
    now = time.time()
    in_flight = sum(1 for t in tasks if t["status"] == "processing")
    waiting = sum(1 for t in tasks if t["status"] == "pending")
    views = []
    for t in tasks:
        v = {k: t[k] for k in t.keys()}
        # 实时计算的阻塞原因覆盖库里 worker 维护的最近值（可能滞后一个轮询周期）
        v["blocked_reason"] = _live_blocked_reason(
            batch, t, _nearest_open_predecessor(tasks, t), in_flight, now)
        views.append(v)
    out = _batch_view(batch)
    out["in_flight"] = in_flight  # 当前占用：正在处理的任务数（并发配额的占用量）
    out["waiting"] = waiting      # 等待数量：尚未进入执行的任务数
    out["approval"] = _approval_view(batch, now)  # 当前审批状态与操作者
    return {"batch": out, "tasks": views}


def batch_events(db: Database, batch_id: int, task_id: int | None, limit: int) -> dict:
    """一次重放的完整审计记录：该批次（可选单条任务）的全部事件，按时间正序。"""
    _get_batch_or_404(db, batch_id)
    sql = "SELECT * FROM events WHERE json_extract(detail,'$.replay_batch_id')=?"
    params: list = [batch_id]
    if task_id is not None:
        sql += " AND json_extract(detail,'$.replay_task_id')=?"
        params.append(task_id)
    sql += " ORDER BY id LIMIT ?"
    params.append(limit)
    events = []
    for r in db.query(sql, tuple(params)):
        d = {k: r[k] for k in r.keys()}
        d["detail"] = json.loads(d["detail"])
        events.append(d)
    return {"events": events}


# ---- 重放 worker ------------------------------------------------------------

class ReplayWorker:
    """逐条执行重放任务；与主 worker 相同的重试/退避语义，任务级隔离不互相阻塞。

    每轮先扫审批超时（到期未决的高风险批次整体取消），再走领取闸门——
    _process_one 的占位事务统一复核：批次在跑且审批已通过（或无需审批） ->
    同编号前序版本已进终态 -> 批次并发配额未满，满足才占位执行；
    被挡下的任务记录阻塞原因（状态展示 + 审计），下一轮换到槽位/前序终态/
    审批通过后自动放行。
    """

    def __init__(self, db: Database, settings: Settings, handler=business_handler,
                 clock=time.time):
        self.db = db
        self.settings = settings
        self.handler = handler
        self.clock = clock
        self._stop = asyncio.Event()

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("replay worker iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.settings.worker_poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self):
        self._stop.set()

    def recover(self) -> int:
        """启动恢复：上次崩溃时卡在 processing 的任务退回 pending，从上次位置继续
        （attempts/checkpoint/next_retry_at 都在库里；副作用靠幂等键去重不会重发）。
        这些任务占用的批次并发配额随状态离开 processing 自动释放，无需额外回收。"""
        now = self.clock()
        with self.db.tx() as cur:
            rows = cur.execute(
                "SELECT * FROM replay_tasks WHERE status='processing'").fetchall()
            for t in rows:
                cur.execute(
                    "UPDATE replay_tasks SET status='pending', updated_at=? WHERE id=?",
                    (now, t["id"]),
                )
                audit.record(cur, "replay_task_recovered", t["external_id"],
                             t["delivery_id"],
                             {"replay_batch_id": t["batch_id"], "replay_task_id": t["id"],
                              "attempts": t["attempts"]}, ts=now)
        return len(rows)

    def run_once(self):
        now = self.clock()
        # 先处理审批超时：到期仍无人批准的高风险批次整体取消（释放其占住的任务），
        # 必须先于领取，保证超时批次的任务本轮绝不会被领取
        expire_approvals(self.db, now)
        # 待审批批次的 pending 任务也取出交给领取闸门：闸门会以 awaiting_approval
        # 挡下（原因变化才写审计，轮询不刷表）；running 且审批通过/无需审批的才领取
        rows = self.db.query(
            """SELECT t.* FROM replay_tasks t
               JOIN replay_batches b ON b.id = t.batch_id
               WHERE t.status='pending'
                 AND b.status IN ('running','pending_approval')
                 AND (t.next_retry_at IS NULL OR t.next_retry_at <= ?)
               ORDER BY t.id LIMIT 100""",
            (now,),
        )
        for row in rows:
            self._process_one(row)

    @staticmethod
    def _open_predecessor(cur: sqlite3.Cursor, task):
        """同批次同编号、版本更早且未进终态的最近一条任务；没有则 None（可领取）。"""
        if task["delivery_created_at"] is None:
            return None  # 老库遗留行没有版本时间快照，不参与有序约束
        return cur.execute(
            """SELECT id, status FROM replay_tasks
               WHERE batch_id=? AND external_id=? AND id<>?
                 AND delivery_created_at IS NOT NULL
                 AND (delivery_created_at < ?
                      OR (delivery_created_at = ? AND delivery_id < ?))
                 AND status NOT IN ('done','failed','cancelled')
               ORDER BY delivery_created_at DESC, delivery_id DESC LIMIT 1""",
            (task["batch_id"], task["external_id"], task["id"],
             task["delivery_created_at"], task["delivery_created_at"],
             task["delivery_id"]),
        ).fetchone()

    def _mark_blocked(self, cur: sqlite3.Cursor, task, reason: str, now: float,
                      **detail):
        """记录任务被领取闸门挡下的原因：仅在原因变化时更新并写审计，
        轮询期间原因不变不重复刷事件；被领取（或取消/重试）时该字段清空。"""
        if task["blocked_reason"] == reason:
            return
        cur.execute(
            "UPDATE replay_tasks SET blocked_reason=?, updated_at=? WHERE id=?",
            (reason, now, task["id"]),
        )
        audit.record(cur, "replay_task_blocked", task["external_id"],
                     task["delivery_id"],
                     {"replay_batch_id": task["batch_id"],
                      "replay_task_id": task["id"],
                      "reason": reason,
                      "previous_reason": task["blocked_reason"],
                      **detail}, ts=now)

    def _process_one(self, task):
        now = self.clock()
        task_id = task["id"]
        # 领取闸门：占位事务里原子复核——拉取之后批次可能已被暂停/取消，或任务被
        # 人工改动；同编号前序版本未进终态、批次并发配额已满时都不得领取。
        # BEGIN IMMEDIATE 串行化所有写事务，检查与占位之间没有竞态。
        with self.db.tx() as cur:
            current = cur.execute(
                """SELECT t.*, b.status AS batch_status,
                          b.max_concurrency AS batch_max_concurrency,
                          b.approval_status AS batch_approval_status
                   FROM replay_tasks t JOIN replay_batches b ON b.id = t.batch_id
                   WHERE t.id=?""",
                (task_id,),
            ).fetchone()
            if current is None or current["status"] != "pending":
                return
            # 审批闸门：待审批（含已过截止点但尚未被扫描释放）的高风险批次不得领取，
            # 原因变化时记一次 blocked 审计；running 只可能来自普通批次（not_required）
            # 或已被非发起人明确批准（approved）的高风险批次；其余批次状态不再处理
            if current["batch_status"] == BATCH_PENDING_APPROVAL:
                self._mark_blocked(cur, current, "awaiting_approval", now)
                return
            if current["batch_status"] != "running":
                return
            if current["batch_approval_status"] not in (
                    APPROVAL_NOT_REQUIRED, APPROVAL_APPROVED):
                self._mark_blocked(cur, current, "awaiting_approval", now)
                return
            # 同一编号有序执行：存在版本更早且未进终态的前序任务 -> 不可领取
            predecessor = self._open_predecessor(cur, current)
            if predecessor is not None:
                self._mark_blocked(
                    cur, current, f"waiting_predecessor:{predecessor['id']}", now,
                    predecessor_task_id=predecessor["id"],
                    predecessor_status=predecessor["status"])
                return
            # 批次级并发配额：占用 = 本批 processing 中的任务数（实时推导，
            # 任务离开 processing 即释放，不存在需要单独回收的计数器）
            maxc = current["batch_max_concurrency"]
            if maxc is not None:
                in_flight = cur.execute(
                    "SELECT COUNT(*) AS c FROM replay_tasks "
                    "WHERE batch_id=? AND status='processing'",
                    (current["batch_id"],),
                ).fetchone()["c"]
                if in_flight >= maxc:
                    self._mark_blocked(
                        cur, current, f"quota_exhausted:{in_flight}/{maxc}", now,
                        in_flight=in_flight, max_concurrency=maxc)
                    return
            claimed = cur.execute(
                """UPDATE replay_tasks SET status='processing', attempts=attempts+1,
                   blocked_reason=NULL, updated_at=?
                   WHERE id=? AND status='pending'""",
                (now, task_id),
            ).rowcount
            if not claimed:
                return
            attempt = current["attempts"] + 1
            audit.record(cur, "replay_task_processing", current["external_id"],
                         current["delivery_id"],
                         {"replay_batch_id": current["batch_id"],
                          "replay_task_id": task_id,
                          "attempt": attempt}, ts=now)

        delivery = self.db.query_one(
            "SELECT * FROM deliveries WHERE id=?", (task["delivery_id"],))
        try:
            result = self.handler(delivery)
        except Exception as exc:  # noqa: BLE001 - 任何业务异常都走重试/失败
            self._handle_failure(task, attempt, exc, self.clock())
            return

        now = self.clock()
        with self.db.tx() as cur:
            # 条件更新落终态：handler 执行期间批次可能已被取消（任务 processing->
            # cancelled）。更新不到说明任务已不属于本次执行——保持 cancelled，
            # 丢弃迟到的结果：不落副作用、不写完成记录、不计进度
            finalized = cur.execute(
                """UPDATE replay_tasks SET status='done', checkpoint=?, next_retry_at=NULL,
                   finished_at=?, updated_at=? WHERE id=? AND status='processing'""",
                (json.dumps(result.get("checkpoint") or {}, ensure_ascii=False),
                 now, now, task_id),
            ).rowcount
            if not finalized:
                current = cur.execute("SELECT status FROM replay_tasks WHERE id=?",
                                      (task_id,)).fetchone()
                audit.record(cur, "replay_task_completion_discarded",
                             task["external_id"], task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "task_status": current["status"] if current else None},
                             ts=now)
                return
            for effect in result.get("effects", []):
                key = replay_effect_key(task_id, effect["type"], effect["payload"])
                cur.execute(
                    """INSERT OR IGNORE INTO outbox
                       (delivery_id, replay_task_id, effect_type, idempotency_key,
                        payload, created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (task["delivery_id"], task_id, effect["type"], key,
                     json.dumps(effect["payload"], ensure_ascii=False, sort_keys=True), now),
                )
            audit.record(cur, "replay_task_done", task["external_id"], task["delivery_id"],
                         {"replay_batch_id": task["batch_id"], "replay_task_id": task_id,
                          "attempt": attempt,
                          "effects": len(result.get("effects", [])),
                          "checkpoint": result.get("checkpoint")}, ts=now)
            self._bump_and_maybe_finish(cur, task["batch_id"], "done", now)

    def _handle_failure(self, task, attempt: int, exc: Exception, now: float):
        task_id = task["id"]
        with self.db.tx() as cur:
            # 与成功收尾同一守卫：处理期间被取消的任务保持 cancelled，
            # 不标记失败、不安排重试（否则会把已取消的任务复活回队列）
            still_processing = cur.execute(
                "SELECT 1 AS x FROM replay_tasks WHERE id=? AND status='processing'",
                (task_id,),
            ).fetchone()
            if still_processing is None:
                audit.record(cur, "replay_task_completion_discarded",
                             task["external_id"], task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "error": str(exc)}, ts=now)
                return
            if attempt >= self.settings.max_attempts:
                # 连续失败 -> 标记 failed，只影响这一条，其他编号照常执行
                cur.execute(
                    """UPDATE replay_tasks SET status='failed', next_retry_at=NULL,
                       last_error=?, finished_at=?, updated_at=? WHERE id=?""",
                    (str(exc), now, now, task_id),
                )
                audit.record(cur, "replay_task_failed", task["external_id"],
                             task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id,
                              "attempts": attempt, "error": str(exc)}, ts=now)
                self._bump_and_maybe_finish(cur, task["batch_id"], "failed", now)
            else:
                delay = min(
                    self.settings.retry_base_seconds * (2 ** (attempt - 1)),
                    self.settings.retry_cap_seconds,
                )
                cur.execute(
                    """UPDATE replay_tasks SET status='pending', next_retry_at=?,
                       last_error=?, updated_at=? WHERE id=?""",
                    (now + delay, str(exc), now, task_id),
                )
                audit.record(cur, "replay_task_retry_scheduled", task["external_id"],
                             task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "delay_seconds": delay, "error": str(exc)}, ts=now)

    def _bump_and_maybe_finish(self, cur: sqlite3.Cursor, batch_id: int,
                               counter: str, now: float):
        """进度计数与任务终态同事务更新；批次内无未完成任务时收尾。"""
        cur.execute(
            f"UPDATE replay_batches SET {counter}={counter}+1, updated_at=? WHERE id=?",
            (now, batch_id),
        )
        remaining = cur.execute(
            "SELECT COUNT(*) AS c FROM replay_tasks WHERE batch_id=? "
            "AND status IN ('pending','processing')",
            (batch_id,),
        ).fetchone()["c"]
        if remaining:
            return
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (batch_id,)).fetchone()
        if batch["status"] != "running":
            return
        final = "completed" if batch["failed"] == 0 else "completed_with_failures"
        cur.execute(
            "UPDATE replay_batches SET status=?, finished_at=?, updated_at=? WHERE id=?",
            (final, now, now, batch_id),
        )
        audit.record(cur, "replay_batch_completed", None, None,
                     {"replay_batch_id": batch_id, "result": final,
                      "total": batch["total"], "done": batch["done"],
                      "failed": batch["failed"], "cancelled": batch["cancelled"]}, ts=now)


# ---- 路由 ------------------------------------------------------------------

def create_replay_router(db: Database, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin/replays", tags=["replays"])

    @router.post("/preview")
    def preview_endpoint(req: PreviewRequest):
        """预览将要重放的内容和影响范围（历史副作用），不落任何数据。"""
        return preview(db, req)

    @router.post("")
    def submit_endpoint(req: SubmitRequest):
        """一次性提交一批重放任务（批次 + 任务单事务落盘）。

        高风险批次（risk_level=high 且带 approval_note）进入 pending_approval，
        待非发起人批准后才运行。
        """
        status, body = submit(db, req, settings.replay_approval_timeout_seconds)
        return JSONResponse(status_code=status, content=body)

    @router.get("")
    def list_batches(status: str | None = None, limit: int = Query(100, le=1000)):
        sql, params = "SELECT * FROM replay_batches", []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return {"batches": [_batch_view(r) for r in db.query(sql, tuple(params))]}

    @router.get("/{batch_id}")
    def get_batch(batch_id: int):
        """批次详情：发起人、原因、筛选快照、审批状态与操作者、进度计数
        + 每条任务的状态。"""
        return batch_detail(db, batch_id)

    @router.get("/{batch_id}/events")
    def get_batch_events(batch_id: int, task_id: int | None = None,
                         limit: int = Query(500, le=2000)):
        """该批次（可选单条任务）的完整审计记录，按时间正序。"""
        return batch_events(db, batch_id, task_id, limit)

    @router.post("/{batch_id}/approve")
    def approve_endpoint(batch_id: int, req: ApprovalRequest):
        """批准高风险批次：审批人必须不同于发起人；批准后批次进入 running。"""
        return approve(db, batch_id, req.operator, req.note)

    @router.post("/{batch_id}/reject")
    def reject_endpoint(batch_id: int, req: RejectionRequest):
        """拒绝高风险批次（拒绝原因必填）：未执行任务整体取消，占用随之释放。"""
        return reject(db, batch_id, req.operator, req.reason, req.note)

    @router.post("/{batch_id}/pause")
    def pause_endpoint(batch_id: int, req: BatchActionRequest):
        return pause(db, batch_id, req.operator)

    @router.post("/{batch_id}/resume")
    def resume_endpoint(batch_id: int, req: BatchActionRequest):
        return resume(db, batch_id, req.operator)

    @router.post("/{batch_id}/cancel")
    def cancel_endpoint(batch_id: int, req: BatchActionRequest):
        return cancel(db, batch_id, req.operator, req.note)

    @router.post("/tasks/{task_id}/retry")
    def retry_task_endpoint(task_id: int, req: BatchActionRequest):
        """失败任务单独重试（只影响这一条）。"""
        return retry_task(db, task_id, req.operator)

    return router

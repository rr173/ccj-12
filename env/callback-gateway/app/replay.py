"""业务重放模块：可追溯的历史回调重放。

流程：筛选预览（preview）-> 提交批次（submit）-> 后台重放 worker 逐条执行 ->
副作用走与正常处理完全相同的 outbox 幂等派发链路。

- 每条重放任务记录发起人、原始版本（delivery_id）、原因和进度（状态/次数/checkpoint）；
- 同一份内容不会生成第二个重放任务：批内 UNIQUE(batch_id, delivery_id) 去重；
  提交带 request_id 时重复提交返回原批次；跨批存在活动任务（pending/processing）
  的投递会被跳过（已完成的批次不阻塞以后再次重放——那是有意为之的新批次）；
- 批次可暂停/继续/取消；取消时未执行的任务与滞留的待派发副作用同事务取消；
- 重启后 recover() 把卡在 processing 的任务退回 pending，从上次位置（attempts/
  checkpoint/next_retry_at 都落库）继续；
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
from pydantic import BaseModel

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


class BatchActionRequest(BaseModel):
    operator: str
    note: str = ""


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

def submit(db: Database, req: SubmitRequest) -> tuple[int, dict]:
    """一次性提交一批重放任务：批次 + 全部任务在一个事务里落盘。"""
    if not req.operator.strip():
        raise HTTPException(422, "operator must be non-empty")
    if not req.reason.strip():
        raise HTTPException(422, "reason must be non-empty")

    # 提交幂等：同一 request_id 重复提交（网络重试/双击）返回原批次
    if req.request_id:
        existing = db.query_one(
            "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
        if existing is not None:
            return 200, {"result": "duplicate", "batch_id": existing["id"],
                         "total": existing["total"]}

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
    filters = req.model_dump(exclude={"operator", "reason", "request_id"})
    filters_json = json.dumps(filters, ensure_ascii=False, default=str)
    try:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO replay_batches
                   (request_id, operator, reason, status, filters, total, created_at, updated_at)
                   VALUES (?,?,?,'running',?,?,?,?)""",
                (req.request_id, req.operator, req.reason, filters_json,
                 len(eligible), now, now),
            )
            batch_id = cur.lastrowid
            for d in eligible:
                # INSERT OR IGNORE + UNIQUE(batch_id, delivery_id)：
                # 同一份内容重复加入也不会生成第二个重放任务
                cur.execute(
                    """INSERT OR IGNORE INTO replay_tasks
                       (batch_id, delivery_id, external_id, operator, reason,
                        status, created_at, updated_at)
                       VALUES (?,?,?,?,?,'pending',?,?)""",
                    (batch_id, d["id"], d["external_id"], req.operator, req.reason,
                     now, now),
                )
            audit.record(cur, "replay_batch_created", None, None, {
                "replay_batch_id": batch_id, "operator": req.operator,
                "reason": req.reason, "request_id": req.request_id,
                "filters": filters, "total": len(eligible), "skipped": skipped}, ts=now)
    except sqlite3.IntegrityError:
        # 并发下 request_id 撞唯一键：返回已存在的那一批
        if req.request_id:
            existing = db.query_one(
                "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
            if existing is not None:
                return 200, {"result": "duplicate", "batch_id": existing["id"],
                             "total": existing["total"]}
        raise

    return 201, {"result": "created", "batch_id": batch_id,
                 "total": len(eligible), "skipped": skipped}


# ---- 批次控制：暂停 / 继续 / 取消 --------------------------------------------

def _get_batch_or_404(db: Database, batch_id: int):
    row = db.query_one("SELECT * FROM replay_batches WHERE id=?", (batch_id,))
    if row is None:
        raise HTTPException(404, "replay batch not found")
    return row


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
    """取消：未执行的任务标记 cancelled；已完成任务滞留的待派发副作用同事务取消
    （已派发的外部效果无法撤回，审计里保留完整轨迹）。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] not in ("running", "paused"):
            raise HTTPException(409, f"batch is {batch['status']}, cannot cancel")
        cancelled_tasks = cur.execute(
            """UPDATE replay_tasks SET status='cancelled', finished_at=?, updated_at=?
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
        cur.execute(
            """UPDATE replay_batches SET status='cancelled', cancelled=cancelled+?,
               updated_at=?, finished_at=? WHERE id=?""",
            (cancelled_tasks, now, now, batch_id),
        )
        audit.record(cur, "replay_batch_cancelled", None, None,
                     {"replay_batch_id": batch_id, "operator": operator, "note": note,
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
               last_error=NULL, finished_at=NULL, updated_at=? WHERE id=?""",
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

def _batch_view(row) -> dict:
    out = {k: row[k] for k in row.keys()}
    out["filters"] = json.loads(out["filters"])
    return out


def batch_detail(db: Database, batch_id: int) -> dict:
    batch = _get_batch_or_404(db, batch_id)
    tasks = db.query("SELECT * FROM replay_tasks WHERE batch_id=? ORDER BY id", (batch_id,))
    return {"batch": _batch_view(batch),
            "tasks": [{k: t[k] for k in t.keys()} for t in tasks]}


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
    """逐条执行重放任务；与主 worker 相同的重试/退避语义，任务级隔离不互相阻塞。"""

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
        （attempts/checkpoint/next_retry_at 都在库里；副作用靠幂等键去重不会重发）。"""
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
        rows = self.db.query(
            """SELECT t.* FROM replay_tasks t
               JOIN replay_batches b ON b.id = t.batch_id
               WHERE t.status='pending' AND b.status='running'
                 AND (t.next_retry_at IS NULL OR t.next_retry_at <= ?)
               ORDER BY t.id LIMIT 100""",
            (now,),
        )
        for row in rows:
            self._process_one(row)

    def _process_one(self, task):
        now = self.clock()
        task_id = task["id"]
        # 条件更新占位：拉取之后批次可能已被暂停/取消，或任务被人工改动
        with self.db.tx() as cur:
            claimed = cur.execute(
                """UPDATE replay_tasks SET status='processing', attempts=attempts+1, updated_at=?
                   WHERE id=? AND status='pending'
                     AND batch_id IN (SELECT id FROM replay_batches WHERE status='running')""",
                (now, task_id),
            ).rowcount
            if not claimed:
                return
            attempt = task["attempts"] + 1
            audit.record(cur, "replay_task_processing", task["external_id"],
                         task["delivery_id"],
                         {"replay_batch_id": task["batch_id"], "replay_task_id": task_id,
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
            cur.execute(
                """UPDATE replay_tasks SET status='done', checkpoint=?, next_retry_at=NULL,
                   finished_at=?, updated_at=? WHERE id=?""",
                (json.dumps(result.get("checkpoint") or {}, ensure_ascii=False),
                 now, now, task_id),
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

def create_replay_router(db: Database) -> APIRouter:
    router = APIRouter(prefix="/admin/replays", tags=["replays"])

    @router.post("/preview")
    def preview_endpoint(req: PreviewRequest):
        """预览将要重放的内容和影响范围（历史副作用），不落任何数据。"""
        return preview(db, req)

    @router.post("")
    def submit_endpoint(req: SubmitRequest):
        """一次性提交一批重放任务（批次 + 任务单事务落盘）。"""
        status, body = submit(db, req)
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
        """批次详情：发起人、原因、筛选快照、进度计数 + 每条任务的状态。"""
        return batch_detail(db, batch_id)

    @router.get("/{batch_id}/events")
    def get_batch_events(batch_id: int, task_id: int | None = None,
                         limit: int = Query(500, le=2000)):
        """该批次（可选单条任务）的完整审计记录，按时间正序。"""
        return batch_events(db, batch_id, task_id, limit)

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

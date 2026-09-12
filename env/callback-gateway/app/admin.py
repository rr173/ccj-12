"""人工处置层：冲突比较/选定、隔离重投、审计查询。

人工选定某份内容后，该版本从它落盘时记录的 checkpoint 位置继续处理，
全程动作写审计日志，可追溯。
"""
from __future__ import annotations

import json
import time

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit
from .db import Database


def _row_to_dict(row) -> dict:
    return {k: row[k] for k in row.keys()}


class ResolveRequest(BaseModel):
    delivery_id: int          # 人工选定采用的内容版本
    operator: str             # 操作人（审计用）
    note: str = ""


class RequeueRequest(BaseModel):
    operator: str
    note: str = ""


def create_admin_router(db: Database) -> APIRouter:
    router = APIRouter(prefix="/admin", tags=["admin"])

    # ---- 投递查询 --------------------------------------------------------

    @router.get("/deliveries")
    def list_deliveries(external_id: str | None = None, status: str | None = None,
                        limit: int = Query(100, le=1000)):
        sql, params = "SELECT * FROM deliveries WHERE 1=1", []
        if external_id:
            sql += " AND external_id=?"
            params.append(external_id)
        if status:
            sql += " AND status=?"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return {"deliveries": [_row_to_dict(r) for r in db.query(sql, tuple(params))]}

    @router.get("/deliveries/{delivery_id}")
    def get_delivery(delivery_id: int):
        row = db.query_one("SELECT * FROM deliveries WHERE id=?", (delivery_id,))
        if row is None:
            raise HTTPException(404, "delivery not found")
        outbox = db.query("SELECT * FROM outbox WHERE delivery_id=?", (delivery_id,))
        return {"delivery": _row_to_dict(row),
                "outbox": [_row_to_dict(r) for r in outbox]}

    # ---- 冲突处置 --------------------------------------------------------

    @router.get("/conflicts")
    def list_conflicts(status: str | None = "open"):
        sql, params = "SELECT * FROM conflicts", []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY id DESC"
        return {"conflicts": [_row_to_dict(r) for r in db.query(sql, tuple(params))]}

    @router.get("/conflicts/{conflict_id}")
    def get_conflict(conflict_id: int):
        """返回冲突单及所有候选版本的完整内容，供人工比较。"""
        conflict = db.query_one("SELECT * FROM conflicts WHERE id=?", (conflict_id,))
        if conflict is None:
            raise HTTPException(404, "conflict not found")
        members = db.query(
            """SELECT d.* FROM deliveries d
               JOIN conflict_members m ON m.delivery_id = d.id
               WHERE m.conflict_id=? ORDER BY d.id""",
            (conflict_id,),
        )
        return {"conflict": _row_to_dict(conflict),
                "versions": [_row_to_dict(m) for m in members]}

    @router.post("/conflicts/{conflict_id}/resolve")
    def resolve_conflict(conflict_id: int, req: ResolveRequest):
        """人工选定一份内容：该版本解冻并从其 checkpoint 继续处理；
        其余版本标记 superseded（永不覆盖、永不删除，仅不再参与处理）。"""
        conflict = db.query_one("SELECT * FROM conflicts WHERE id=?", (conflict_id,))
        if conflict is None:
            raise HTTPException(404, "conflict not found")
        if conflict["status"] != "open":
            raise HTTPException(409, "conflict already resolved")

        members = db.query(
            "SELECT delivery_id FROM conflict_members WHERE conflict_id=?", (conflict_id,))
        member_ids = {m["delivery_id"] for m in members}
        if req.delivery_id not in member_ids:
            raise HTTPException(422, "delivery_id is not a member of this conflict")

        now = time.time()
        with db.tx() as cur:
            chosen = cur.execute(
                "SELECT * FROM deliveries WHERE id=?", (req.delivery_id,)).fetchone()
            for mid in member_ids:
                if mid == req.delivery_id:
                    if chosen["status"] in ("pending", "conflicted", "quarantined"):
                        # 从可追溯的位置继续：保留 checkpoint 与 attempts 历史
                        cur.execute(
                            """UPDATE deliveries SET status='pending', frozen=0,
                               next_retry_at=NULL, updated_at=? WHERE id=?""",
                            (now, mid),
                        )
                    else:
                        cur.execute(
                            "UPDATE deliveries SET frozen=0, updated_at=? WHERE id=?",
                            (now, mid),
                        )
                else:
                    cur.execute(
                        """UPDATE deliveries SET status='superseded', updated_at=?
                           WHERE id=? AND status IN ('pending','conflicted','quarantined')""",
                        (now, mid),
                    )
                    # 未选中版本滞留的待派发副作用一并取消（同事务），
                    # 保证只有被选中的版本会继续产生外部效果
                    cancelled = cur.execute(
                        "UPDATE outbox SET status='cancelled' "
                        "WHERE delivery_id=? AND status='pending'",
                        (mid,),
                    ).rowcount
                    if cancelled:
                        audit.record(cur, "effect_cancelled", conflict["external_id"], mid,
                                     {"conflict_id": conflict_id, "cancelled": cancelled,
                                      "reason": "version_not_selected"}, ts=now)
            resolution = {"chosen_delivery_id": req.delivery_id,
                          "operator": req.operator, "note": req.note}
            cur.execute(
                "UPDATE conflicts SET status='resolved', resolved_at=?, resolution=? WHERE id=?",
                (now, json.dumps(resolution, ensure_ascii=False), conflict_id),
            )
            audit.record(cur, "conflict_resolved", conflict["external_id"], req.delivery_id,
                         {"conflict_id": conflict_id, **resolution,
                          "superseded": sorted(member_ids - {req.delivery_id})}, ts=now)

        return {"result": "resolved", "conflict_id": conflict_id,
                "chosen_delivery_id": req.delivery_id}

    # ---- 隔离队列处置 ----------------------------------------------------

    @router.post("/deliveries/{delivery_id}/requeue")
    def requeue_delivery(delivery_id: int, req: RequeueRequest):
        """人工把隔离中的 delivery 重新放回待处理队列（重置重试计数）。"""
        row = db.query_one("SELECT * FROM deliveries WHERE id=?", (delivery_id,))
        if row is None:
            raise HTTPException(404, "delivery not found")
        if row["status"] != "quarantined":
            raise HTTPException(409, f"delivery is {row['status']}, not quarantined")
        now = time.time()
        with db.tx() as cur:
            cur.execute(
                """UPDATE deliveries SET status='pending', attempts=0,
                   next_retry_at=NULL, updated_at=? WHERE id=?""",
                (now, delivery_id),
            )
            audit.record(cur, "requeued", row["external_id"], delivery_id,
                         {"operator": req.operator, "note": req.note}, ts=now)
        return {"result": "requeued", "delivery_id": delivery_id}

    # ---- 审计查询 --------------------------------------------------------

    @router.get("/events")
    def list_events(type: str | None = None, external_id: str | None = None,
                    delivery_id: int | None = None, limit: int = Query(200, le=2000)):
        sql, params = "SELECT * FROM events WHERE 1=1", []
        if type:
            sql += " AND type=?"
            params.append(type)
        if external_id:
            sql += " AND external_id=?"
            params.append(external_id)
        if delivery_id is not None:
            sql += " AND delivery_id=?"
            params.append(delivery_id)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        events = []
        for r in db.query(sql, tuple(params)):
            d = _row_to_dict(r)
            d["detail"] = json.loads(d["detail"])
            events.append(d)
        return {"events": events}

    return router

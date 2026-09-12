"""审批委托：运营把某个审批角色在时间窗内委托给受托人，决定时凭有效委托承担角色。

委托生命周期：创建（active）-> 自然到期（expired，由 worker 扫描）或撤销（revoked）
-> 可重新激活（active，给新的有效期）。所有生命周期动作都写审计事件。

与审批节点/票的关系：
- 受托人对节点做决定时必须携带当前有效的委托（直接持角色承担的场景可不传委托）；
  决定在写事务里校验委托：属于本人、角色匹配、时间窗覆盖当前时刻且未撤销；
- 委托到期或撤销时，其投在**尚未满足（active）**节点上的赞成票同事务置为 invalid，
  节点的有效赞成人数回退、法定人数不足要重新等待；已经落定（达到法定人数/拒绝/
  跳过/超时）的节点与已经放行的批次不会被改写；
- 失效的票不会随重新激活自动复活——受托人须重新决定（新决定是一条新审计轨迹）；
- 同一受托人同一时刻可持有不同角色的多份委托；在一个批次里同一人仍只能在一个
  节点持有效票（由 replay_node_votes 的部分唯一索引强制）。
"""
from __future__ import annotations

import sqlite3
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import audit
from .db import Database

DELEGATION_ACTIVE = "active"
DELEGATION_REVOKED = "revoked"
DELEGATION_EXPIRED = "expired"
_DELEGATION_STATUSES = (DELEGATION_ACTIVE, DELEGATION_REVOKED, DELEGATION_EXPIRED)


class DelegationCreateRequest(BaseModel):
    role: str                      # 被委托的审批角色（须与节点允许角色匹配）
    delegatee: str                 # 受托人
    operator: str                  # 创建人（委托人/授权运营）
    valid_from: float | str        # 生效时间（epoch 秒或 ISO-8601），可为未来
    valid_to: float | str          # 失效时间（须晚于生效时间）
    note: str = ""


class DelegationRevokeRequest(BaseModel):
    operator: str
    reason: str                    # 撤销原因（必填，留痕可追溯）


class DelegationReactivateRequest(BaseModel):
    operator: str
    valid_from: float | str        # 重新生效时间
    valid_to: float | str          # 重新失效时间（须晚于生效时间）
    note: str = ""


def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return str(value).strip()


def _parse_time(value, field: str) -> float:
    """委托时间支持 epoch 秒（数字）或 ISO-8601 字符串。"""
    if value is None:
        raise HTTPException(422, f"{field} is required")
    if isinstance(value, bool):
        raise HTTPException(422, f"{field} must be epoch seconds or ISO-8601 timestamp")
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
        raise HTTPException(422, f"invalid {field}: {value!r} "
                                 "(expect epoch seconds or ISO-8601)")


def window(req) -> tuple[float, float]:
    valid_from = _parse_time(getattr(req, "valid_from", None), "valid_from")
    valid_to = _parse_time(getattr(req, "valid_to", None), "valid_to")
    if valid_to <= valid_from:
        raise HTTPException(422, "valid_to must be later than valid_from")
    return valid_from, valid_to


def is_delegation_current(row, now: float) -> bool:
    """委托当前是否有效：未撤销/未失效登记，且时间窗覆盖 now。"""
    return (row["status"] == DELEGATION_ACTIVE
            and row["valid_from"] <= now <= row["valid_to"])


def load_valid_for_role(cur: sqlite3.Cursor, operator: str, role: str,
                        now: float) -> sqlite3.Row | None:
    """取受托人当前可用于承担某角色的有效委托（任意一份）；无则 None。"""
    return cur.execute(
        """SELECT * FROM replay_delegations
           WHERE delegatee=? AND role=? AND status='active'
             AND valid_from<=? AND valid_to>=?
           ORDER BY id LIMIT 1""",
        (operator, role, now, now),
    ).fetchone()


def load_for_decision(cur: sqlite3.Cursor, delegation_id: int | None,
                      operator: str, roles: list[str], now: float) -> sqlite3.Row:
    """节点决定时校验并加载所引用的委托；不合法直接 4xx，不接受决定。

    - 委托存在、受托人为本人；
    - 委托角色在节点允许角色列表内；
    - 委托未撤销且时间窗覆盖当前时刻（到期扫描尚未跑时也在决定时拦住）。
    """
    if delegation_id is None:
        raise HTTPException(422, "delegation_id is required to act in a designated role")
    row = cur.execute("SELECT * FROM replay_delegations WHERE id=?",
                      (delegation_id,)).fetchone()
    if row is None:
        raise HTTPException(404, f"delegation {delegation_id} not found")
    if row["delegatee"] != operator:
        raise HTTPException(403, "delegation was not granted to this operator")
    if row["role"] not in roles:
        raise HTTPException(403, f"delegation role {row['role']!r} is not allowed for "
                                 f"this node (allowed: {','.join(roles)})")
    if row["status"] != DELEGATION_ACTIVE:
        raise HTTPException(409, f"delegation is {row['status']}, decision not accepted")
    if now < row["valid_from"]:
        raise HTTPException(409, "delegation is not yet effective")
    if now > row["valid_to"]:
        raise HTTPException(409, "delegation has expired")
    return row


def _invalidate_votes(cur: sqlite3.Cursor, delegation_id: int, now: float,
                      cause: str, **extra) -> list[dict]:
    """委托失效/撤销：把它投在尚未满足（active）节点上的有效赞成票置为 invalid。

    只作用于 active 节点——已落定节点（approved/rejected/skipped/expired/cancelled）
    上的票是历史事实，永不改写。返回每个受影响节点的计数摘要（供审计与响应）。
    """
    votes = cur.execute(
        """SELECT v.id AS vote_id, v.node_id AS node_id, v.batch_id AS batch_id,
                  v.voter AS voter, n.seq AS seq, n.role AS node_role,
                  n.required_approvals AS required_approvals
           FROM replay_node_votes v
           JOIN replay_approval_nodes n ON n.id = v.node_id
           JOIN replay_batches b ON b.id = v.batch_id
           WHERE v.delegation_id=? AND v.status='valid' AND v.vote='approve'
             AND n.status='active'
             AND b.status='pending_approval' AND b.approval_status='pending'""",
        (delegation_id,),
    ).fetchall()
    affected: dict[int, dict] = {}
    for v in votes:
        cur.execute(
            "UPDATE replay_node_votes SET status='invalid', invalidated_at=? WHERE id=?",
            (now, v["vote_id"]),
        )
        item = affected.setdefault(v["node_id"], {
            "node_id": v["node_id"], "batch_id": v["batch_id"], "seq": v["seq"],
            "role": v["node_role"], "required_approvals": v["required_approvals"],
            "invalidated_votes": 0, "voters": [],
        })
        item["invalidated_votes"] += 1
        item["voters"].append(v["voter"])
        audit.record(cur, "replay_node_vote_invalidated", None, None,
                     {"replay_batch_id": v["batch_id"], "node_id": v["node_id"],
                      "seq": v["seq"], "voter": v["voter"],
                      "delegation_id": delegation_id, "cause": cause, **extra}, ts=now)
    # 受影响节点重新计数：有效赞成票已不足法定人数 -> 继续等待（节点保持 active，
    # 批次保持 pending_approval；只更新统计与审计，不改写任何已落定记录）
    for item in affected.values():
        approved_now = cur.execute(
            "SELECT COUNT(*) AS c FROM replay_node_votes "
            "WHERE node_id=? AND status='valid' AND vote='approve'",
            (item["node_id"],),
        ).fetchone()["c"]
        item["approved_count"] = approved_now
        item["missing"] = max(0, item["required_approvals"] - approved_now)
        item["still_quorate"] = approved_now >= item["required_approvals"]
        audit.record(cur, "replay_approval_node_quorum_lost", None, None,
                     {"replay_batch_id": item["batch_id"], "node_id": item["node_id"],
                      "seq": item["seq"], "role": item["role"],
                      "required_approvals": item["required_approvals"],
                      "approved_count": approved_now,
                      "missing": item["missing"],
                      "invalidated_votes": item["invalidated_votes"],
                      "delegation_id": delegation_id, "cause": cause, **extra}, ts=now)
    return list(affected.values())


def create_delegation(db: Database, req: DelegationCreateRequest) -> tuple[int, dict]:
    role = _require(req.role, "role")
    delegatee = _require(req.delegatee, "delegatee")
    operator = _require(req.operator, "operator")
    valid_from, valid_to = window(req)
    note = (req.note or "").strip()
    now = time.time()
    with db.tx() as cur:
        cur.execute(
            """INSERT INTO replay_delegations
               (role, delegatee, delegator, status, valid_from, valid_to, note,
                created_at, updated_at)
               VALUES (?,?,?,'active',?,?,?,?,?)""",
            (role, delegatee, operator, valid_from, valid_to, note or None, now, now),
        )
        delegation_id = cur.lastrowid
        audit.record(cur, "replay_delegation_created", None, None,
                     {"delegation_id": delegation_id, "role": role,
                      "delegatee": delegatee, "operator": operator,
                      "valid_from": valid_from, "valid_to": valid_to,
                      "note": note or None}, ts=now)
    return 201, {"result": "created", "delegation_id": delegation_id,
                 "role": role, "delegatee": delegatee,
                 "valid_from": valid_from, "valid_to": valid_to,
                 "current": valid_from <= now <= valid_to}


def revoke_delegation(db: Database, delegation_id: int,
                      req: DelegationRevokeRequest) -> dict:
    operator = _require(req.operator, "operator")
    reason = _require(req.reason, "reason")
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM replay_delegations WHERE id=?",
                          (delegation_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "delegation not found")
        if row["status"] != DELEGATION_ACTIVE:
            raise HTTPException(409, f"delegation is {row['status']}, cannot revoke")
        cur.execute(
            """UPDATE replay_delegations SET status='revoked', revoked_at=?,
               revoked_by=?, revoke_reason=?, updated_at=? WHERE id=?""",
            (now, operator, reason, now, delegation_id),
        )
        affected = _invalidate_votes(cur, delegation_id, now, "revoked",
                                     revoked_by=operator, reason=reason)
        audit.record(cur, "replay_delegation_revoked", None, None,
                     {"delegation_id": delegation_id, "role": row["role"],
                      "delegatee": row["delegatee"], "operator": operator,
                      "reason": reason,
                      "affected_nodes": [a["node_id"] for a in affected]}, ts=now)
    return {"result": "revoked", "delegation_id": delegation_id,
            "affected_nodes": affected}


def reactivate_delegation(db: Database, delegation_id: int,
                          req: DelegationReactivateRequest) -> dict:
    """重新激活已撤销/已失效的委托：给一个新的有效时间窗，记审计。

    曾经因此失效的票不会自动复活（受托人重新决定才产生新票与新审计轨迹）。
    """
    operator = _require(req.operator, "operator")
    valid_from, valid_to = window(req)
    note = (req.note or "").strip()
    now = time.time()
    with db.tx() as cur:
        row = cur.execute("SELECT * FROM replay_delegations WHERE id=?",
                          (delegation_id,)).fetchone()
        if row is None:
            raise HTTPException(404, "delegation not found")
        if row["status"] == DELEGATION_ACTIVE:
            raise HTTPException(409, "delegation is already active")
        old_status = row["status"]
        cur.execute(
            """UPDATE replay_delegations SET status='active', valid_from=?, valid_to=?,
               note=COALESCE(?, note), revoked_at=NULL, revoked_by=NULL,
               revoke_reason=NULL, updated_at=? WHERE id=?""",
            (valid_from, valid_to, note or None, now, delegation_id),
        )
        audit.record(cur, "replay_delegation_reactivated", None, None,
                     {"delegation_id": delegation_id, "role": row["role"],
                      "delegatee": row["delegatee"], "operator": operator,
                      "previous_status": old_status,
                      "valid_from": valid_from, "valid_to": valid_to,
                      "note": note or None}, ts=now)
    return {"result": "reactivated", "delegation_id": delegation_id,
            "valid_from": valid_from, "valid_to": valid_to,
            "current": valid_from <= now <= valid_to}


def expire_delegations(db: Database, now: float) -> int:
    """到期扫描（worker 每轮在审批超时扫描前调用）：把时间窗已过但仍登记 active 的
    委托置为 expired，并使其投在未满足节点上的赞成票失效（节点重新等待法定人数）。

    条件更新 + 写事务串行：与撤销/决定互斥，重复扫描不会产生第二次效果。
    """
    due = db.query(
        "SELECT * FROM replay_delegations WHERE status='active' AND valid_to<=?",
        (now,),
    )
    count = 0
    for row in due:
        with db.tx() as cur:
            changed = cur.execute(
                "UPDATE replay_delegations SET status='expired', updated_at=? "
                "WHERE id=? AND status='active'",
                (now, row["id"]),
            ).rowcount
            if not changed:  # 并发下已被撤销/重新激活
                continue
            affected = _invalidate_votes(cur, row["id"], now, "expired",
                                         valid_to=row["valid_to"])
            audit.record(cur, "replay_delegation_expired", None, None,
                         {"delegation_id": row["id"], "role": row["role"],
                          "delegatee": row["delegatee"], "delegator": row["delegator"],
                          "valid_from": row["valid_from"], "valid_to": row["valid_to"],
                          "affected_nodes": [a["node_id"] for a in affected]}, ts=now)
            count += 1
    return count


def _view(row, now: float) -> dict:
    """委托展示：存储状态 + 当前是否有效（时间窗判定）。active 但未到生效时间显示
    pending；active 且已过失效时间（扫描尚未跑）显示 expired。"""
    stored = row["status"]
    if stored == DELEGATION_ACTIVE:
        current = row["valid_from"] <= now <= row["valid_to"]
        if now < row["valid_from"]:
            effective = "pending"
        elif now > row["valid_to"]:
            effective = DELEGATION_EXPIRED
        else:
            effective = DELEGATION_ACTIVE
    else:
        current = False
        effective = stored
    return {
        "id": row["id"], "role": row["role"], "delegatee": row["delegatee"],
        "delegator": row["delegator"], "status": stored, "effective_status": effective,
        "current": current, "valid_from": row["valid_from"], "valid_to": row["valid_to"],
        "note": row["note"], "revoked_at": row["revoked_at"],
        "revoked_by": row["revoked_by"], "revoke_reason": row["revoke_reason"],
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }


def list_delegations(db: Database, role: str | None, delegatee: str | None,
                     status_filter: str | None, limit: int, now: float) -> dict:
    sql, params = "SELECT * FROM replay_delegations WHERE 1=1", []
    if role:
        sql += " AND role=?"
        params.append(role)
    if delegatee:
        sql += " AND delegatee=?"
        params.append(delegatee)
    if status_filter:
        if status_filter not in _DELEGATION_STATUSES:
            raise HTTPException(422, f"invalid status: {status_filter!r} "
                                     f"(expect one of {','.join(_DELEGATION_STATUSES)})")
        sql += " AND status=?"
        params.append(status_filter)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"delegations": [_view(r, now) for r in rows]}


def create_delegation_router(db: Database) -> APIRouter:
    router = APIRouter(prefix="/admin/replay-delegations", tags=["replay-delegations"])

    @router.post("")
    def create_endpoint(req: DelegationCreateRequest):
        """为角色创建委托（受托人 + 生效/失效时间窗）；创建即写审计。"""
        status, body = create_delegation(db, req)
        return JSONResponse(status_code=status, content=body)

    @router.get("")
    def list_endpoint(role: str | None = None, delegatee: str | None = None,
                      status: str | None = None,
                      limit: int = Query(100, le=1000)):
        """列出委托（可按角色/受托人/存储状态过滤），含当前是否有效的判定。"""
        return list_delegations(db, role, delegatee, status, limit, time.time())

    @router.get("/{delegation_id}")
    def get_endpoint(delegation_id: int):
        row = db.query_one("SELECT * FROM replay_delegations WHERE id=?",
                           (delegation_id,))
        if row is None:
            raise HTTPException(404, "delegation not found")
        return _view(row, time.time())

    @router.post("/{delegation_id}/revoke")
    def revoke_endpoint(delegation_id: int, req: DelegationRevokeRequest):
        """撤销委托（原因必填）：未满足节点上基于它的赞成票随之失效，节点重新等待。"""
        return revoke_delegation(db, delegation_id, req)

    @router.post("/{delegation_id}/reactivate")
    def reactivate_endpoint(delegation_id: int, req: DelegationReactivateRequest):
        """重新激活已撤销/已失效的委托（新的有效期）；旧票不复活，须重新决定。"""
        return reactivate_delegation(db, delegation_id, req)

    return router

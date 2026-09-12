"""接入层核心：幂等落盘 + 冲突冻结。

规则：
1. (external_id, 内容哈希) 唯一 —— 同编号同内容只形成一次业务处理，重复投递直接返回原结果；
2. 同编号不同内容 —— 新版本照常落盘（不覆盖任何旧数据），整组冻结，开冲突单等人工比较选择；
3. 所有写入在一个事务里提交，提交成功后才向接入方返回确认（ACK）。
"""
from __future__ import annotations

import hashlib
import json
import time

from . import audit
from .db import Database


def content_hash_of(body: bytes) -> str:
    return hashlib.sha256(body).hexdigest()


def ingest(db: Database, external_id: str, body: bytes, headers: dict,
           now: float | None = None) -> dict:
    """把一次回调可靠落盘。返回给接入方的结果在事务提交后才产生。"""
    now = time.time() if now is None else now
    chash = content_hash_of(body)

    with db.tx() as cur:
        # 1) 同编号 + 完全相同内容 -> 幂等命中，不产生第二次业务处理
        dup = cur.execute(
            "SELECT * FROM deliveries WHERE external_id=? AND content_hash=?",
            (external_id, chash),
        ).fetchone()
        if dup is not None:
            audit.record(cur, "duplicate", external_id, dup["id"],
                         {"status": dup["status"]}, ts=now)
            return {"result": "duplicate", "delivery_id": dup["id"], "status": dup["status"]}

        # 2) 落盘新版本（绝不覆盖旧行）
        cur.execute(
            """INSERT INTO deliveries
               (external_id, content_hash, payload, status, frozen, created_at, updated_at)
               VALUES (?,?,?,?,0,?,?)""",
            (external_id, chash, body.decode("utf-8", errors="replace"), "pending", now, now),
        )
        delivery_id = cur.lastrowid

        siblings = cur.execute(
            "SELECT id, status FROM deliveries WHERE external_id=? AND id<>?",
            (external_id, delivery_id),
        ).fetchall()

        if not siblings:
            # 3) 全新编号：进入待处理队列
            audit.record(cur, "accepted", external_id, delivery_id,
                         {"content_hash": chash, "headers": _safe_headers(headers)}, ts=now)
            return {"result": "accepted", "delivery_id": delivery_id, "status": "pending"}

        # 4) 同编号但内容不同 -> 冻结整组 + 冲突单，禁止任何自动覆盖
        cur.execute(
            "UPDATE deliveries SET frozen=1, updated_at=? WHERE external_id=?",
            (now, external_id),
        )
        cur.execute(
            "UPDATE deliveries SET status='conflicted', updated_at=? "
            "WHERE external_id=? AND status='pending'",
            (now, external_id),
        )

        conflict = cur.execute(
            "SELECT id FROM conflicts WHERE external_id=? AND status='open'",
            (external_id,),
        ).fetchone()
        if conflict is None:
            cur.execute(
                "INSERT INTO conflicts (external_id, status, created_at) VALUES (?,'open',?)",
                (external_id, now),
            )
            conflict_id = cur.lastrowid
            event_type = "conflict_opened"
        else:
            conflict_id = conflict["id"]
            event_type = "conflict_member_added"

        members = cur.execute(
            "SELECT id FROM deliveries WHERE external_id=?", (external_id,),
        ).fetchall()
        for m in members:
            cur.execute(
                "INSERT OR IGNORE INTO conflict_members (conflict_id, delivery_id) VALUES (?,?)",
                (conflict_id, m["id"]),
            )

        audit.record(cur, event_type, external_id, delivery_id,
                     {"conflict_id": conflict_id,
                      "versions": [m["id"] for m in members],
                      "content_hash": chash}, ts=now)
        return {"result": "conflict", "delivery_id": delivery_id,
                "conflict_id": conflict_id, "status": "conflicted"}


def _safe_headers(headers: dict) -> dict:
    """审计里只留必要的头，签名头只留 kid，避免泄露。"""
    out = {}
    for k in ("x-callback-id", "content-type"):
        if k in headers:
            out[k] = headers[k]
    sig = headers.get("x-signature")
    if sig:
        for seg in sig.split(","):
            if seg.strip().startswith("kid="):
                out["signature_kid"] = seg.split("=", 1)[1].strip()
    return out

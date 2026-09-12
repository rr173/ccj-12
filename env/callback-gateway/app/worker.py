"""处理层：worker 循环。

- 只挑 status='pending' 且未冻结且到期的 delivery，逐条处理 —— 某条进入隔离
  （quarantined）不影响其他编号继续处理；
- 失败按指数退避重试（base * 2^(n-1)，封顶），连续失败达到 max_attempts 进隔离队列；
- 外部副作用先落 outbox（与 delivery 完成状态同事务），再由派发器执行；
  派发器执行前先看下游是否已应用该幂等键 —— 重启/重复投递都不会把副作用做两次。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import time

from . import audit
from .config import Settings
from .db import Database
from .handlers import IdempotentSink, business_handler

log = logging.getLogger("gateway.worker")


def effect_key(delivery_id: int, effect_type: str, effect_payload: dict) -> str:
    """副作用幂等键：由 delivery + 类型 + 规范化内容决定，重算结果恒定。"""
    canonical = json.dumps(effect_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(f"{delivery_id}:{effect_type}:{canonical}".encode()).hexdigest()


class Worker:
    def __init__(self, db: Database, settings: Settings, handler=business_handler,
                 sink: IdempotentSink | None = None, clock=time.time):
        self.db = db
        self.settings = settings
        self.handler = handler
        self.sink = sink or IdempotentSink(db, clock)
        self.clock = clock
        self._stop = asyncio.Event()

    # ---- 主循环 ----------------------------------------------------------

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("worker iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.settings.worker_poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self):
        self._stop.set()

    def run_once(self):
        self._process_due()
        self._dispatch_outbox()

    # ---- 业务处理 --------------------------------------------------------

    def _process_due(self):
        now = self.clock()
        rows = self.db.query(
            """SELECT * FROM deliveries
               WHERE status='pending' AND frozen=0
                 AND (next_retry_at IS NULL OR next_retry_at <= ?)
               ORDER BY id LIMIT 100""",
            (now,),
        )
        for row in rows:
            self._process_one(row)

    def _process_one(self, row):
        now = self.clock()
        delivery_id = row["id"]
        attempt = row["attempts"] + 1

        with self.db.tx() as cur:
            cur.execute(
                "UPDATE deliveries SET status='processing', attempts=?, updated_at=? WHERE id=?",
                (attempt, now, delivery_id),
            )
            audit.record(cur, "delivery_processing", row["external_id"], delivery_id,
                         {"attempt": attempt}, ts=now)

        try:
            result = self.handler(row)
        except Exception as exc:  # noqa: BLE001 - 任何业务异常都走重试/隔离
            self._handle_failure(row, attempt, exc, now)
            return

        with self.db.tx() as cur:
            for effect in result.get("effects", []):
                key = effect_key(delivery_id, effect["type"], effect["payload"])
                cur.execute(
                    """INSERT OR IGNORE INTO outbox
                       (delivery_id, effect_type, idempotency_key, payload, created_at)
                       VALUES (?,?,?,?,?)""",
                    (delivery_id, effect["type"], key,
                     json.dumps(effect["payload"], ensure_ascii=False, sort_keys=True), now),
                )
            cur.execute(
                "UPDATE deliveries SET status='done', checkpoint=?, next_retry_at=NULL, updated_at=? "
                "WHERE id=?",
                (json.dumps(result.get("checkpoint") or {}, ensure_ascii=False), now, delivery_id),
            )
            audit.record(cur, "delivery_done", row["external_id"], delivery_id,
                         {"attempt": attempt,
                          "effects": len(result.get("effects", [])),
                          "checkpoint": result.get("checkpoint")}, ts=now)

    def _handle_failure(self, row, attempt: int, exc: Exception, now: float):
        delivery_id = row["id"]
        max_attempts = self.settings.max_attempts
        with self.db.tx() as cur:
            if attempt >= max_attempts:
                # 连续失败 -> 隔离队列；只影响这一条，其他编号照常处理
                cur.execute(
                    "UPDATE deliveries SET status='quarantined', next_retry_at=NULL, updated_at=? "
                    "WHERE id=?",
                    (now, delivery_id),
                )
                audit.record(cur, "quarantined", row["external_id"], delivery_id,
                             {"attempts": attempt, "error": str(exc)}, ts=now)
            else:
                delay = min(
                    self.settings.retry_base_seconds * (2 ** (attempt - 1)),
                    self.settings.retry_cap_seconds,
                )
                cur.execute(
                    "UPDATE deliveries SET status='pending', next_retry_at=?, updated_at=? WHERE id=?",
                    (now + delay, now, delivery_id),
                )
                audit.record(cur, "retry_scheduled", row["external_id"], delivery_id,
                             {"attempt": attempt, "delay_seconds": delay, "error": str(exc)}, ts=now)

    # ---- 副作用派发（outbox） -------------------------------------------

    def _dispatch_outbox(self):
        rows = self.db.query(
            "SELECT * FROM outbox WHERE status='pending' ORDER BY id LIMIT 100"
        )
        for row in rows:
            self._dispatch_one(row)

    def _dispatch_one(self, row):
        now = self.clock()
        key = row["idempotency_key"]
        try:
            # 重启恢复的关键：下游若已应用过该幂等键，直接标记执行完毕，绝不再发
            if not self.sink.already_applied(key):
                self.sink.send(key, row["effect_type"], json.loads(row["payload"]))
        except Exception as exc:  # noqa: BLE001
            attempts = row["attempts"] + 1
            status = "failed" if attempts >= self.settings.max_attempts else "pending"
            with self.db.tx() as cur:
                cur.execute(
                    "UPDATE outbox SET attempts=?, status=? WHERE id=?",
                    (attempts, status, row["id"]),
                )
                audit.record(cur, "effect_failed", None, row["delivery_id"],
                             {"outbox_id": row["id"], "attempts": attempts,
                              "status": status, "error": str(exc)}, ts=now)
            return
        with self.db.tx() as cur:
            cur.execute(
                "UPDATE outbox SET status='executed', executed_at=? WHERE id=?",
                (now, row["id"]),
            )
            audit.record(cur, "effect_executed", None, row["delivery_id"],
                         {"outbox_id": row["id"], "idempotency_key": key,
                          "effect_type": row["effect_type"]}, ts=now)

"""处理层的可插拔部分：业务处理器 + 外部副作用出口。

business_handler：把一条 delivery 变成若干「外部副作用」（effects）和一个 checkpoint。
    真实项目里在这里接自己的业务逻辑；返回的 effects 会先落 outbox（与 delivery
    状态同事务提交），再由派发器执行 —— 崩溃也丢不了。

IdempotentSink：模拟下游系统。下游凭 Idempotency-Key 去重（sink_effects 表），
    即使网关在「已发送、未标记」之间崩溃重启，也不会把副作用真正做两次。
"""
from __future__ import annotations

import json
import time

from .db import Database


class TransientError(Exception):
    """可重试的业务错误（网络抖动、下游 5xx 等）。"""


def business_handler(delivery) -> dict:
    """默认处理器：解析 JSON 报文，产出一个 downstream.notify 副作用。

    报文里带 "force_error": true 时抛 TransientError —— 用于演示/测试重试与隔离。
    """
    payload = json.loads(delivery["payload"])
    if payload.get("force_error"):
        raise TransientError("payload requested failure (force_error=true)")
    return {
        "checkpoint": {"stage": "planned", "handler": "business_handler/v1"},
        "effects": [{
            "type": "downstream.notify",
            "payload": {
                "external_id": delivery["external_id"],
                "document": payload,
            },
        }],
    }


class IdempotentSink:
    """下游系统的替身：按幂等键保证效果只被应用一次。"""

    def __init__(self, db: Database, clock=time.time):
        self.db = db
        self.clock = clock

    def already_applied(self, idempotency_key: str) -> bool:
        return self.db.query_one(
            "SELECT 1 AS x FROM sink_effects WHERE idempotency_key=?",
            (idempotency_key,),
        ) is not None

    def send(self, idempotency_key: str, effect_type: str, payload: dict) -> None:
        """把副作用发给下游。下游按幂等键去重：重复发送不会重复应用。"""
        with self.db.tx() as cur:
            cur.execute(
                """INSERT OR IGNORE INTO sink_effects (idempotency_key, effect_type, payload, applied_at)
                   VALUES (?,?,?,?)""",
                (idempotency_key, effect_type,
                 json.dumps(payload, ensure_ascii=False, sort_keys=True), self.clock()),
            )

    def applied_count(self, idempotency_key: str) -> int:
        row = self.db.query_one(
            "SELECT COUNT(*) AS c FROM sink_effects WHERE idempotency_key=?",
            (idempotency_key,),
        )
        return row["c"]

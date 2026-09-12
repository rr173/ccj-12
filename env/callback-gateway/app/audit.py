"""审计日志：只增不删，所有关键动作（签名、冲突、重试、隔离、副作用、人工处置）都落一条。"""
from __future__ import annotations

import json
import sqlite3
import time


def record(cur: sqlite3.Cursor, event_type: str, external_id: str | None = None,
           delivery_id: int | None = None, detail: dict | None = None,
           ts: float | None = None) -> None:
    """在当前事务内写一条审计事件（随业务事务一起提交，保证不丢）。"""
    cur.execute(
        "INSERT INTO events (ts, type, external_id, delivery_id, detail) VALUES (?,?,?,?,?)",
        (time.time() if ts is None else ts, event_type, external_id, delivery_id,
         json.dumps(detail or {}, ensure_ascii=False, sort_keys=True)),
    )

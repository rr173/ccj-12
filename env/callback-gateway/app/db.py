"""持久层：SQLite（WAL + FULL sync），所有写操作走事务，提交后才算落盘。

表结构：
- deliveries       每一次回调内容的一个版本（同一 external_id 可有多份不同内容）
- conflicts        同编号不同内容产生的冲突单
- conflict_members 冲突单与内容版本的关联
- outbox           外部副作用的发件箱（幂等键保证 exactly-once）
- sink_effects     模拟下游系统的已应用记录（下游按幂等键去重）
- events           只增不删的审计日志（签名/冲突/重试/处置全部可查）
"""
from __future__ import annotations

import sqlite3
import threading
from contextlib import contextmanager

SCHEMA = """
CREATE TABLE IF NOT EXISTS deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id   TEXT NOT NULL,               -- 接入方的业务编号
    content_hash  TEXT NOT NULL,               -- 原始报文的 SHA-256
    payload       TEXT NOT NULL,               -- 原始报文（不做任何改写）
    status        TEXT NOT NULL,               -- pending|processing|done|conflicted|quarantined|superseded
    frozen        INTEGER NOT NULL DEFAULT 0,  -- 冲突冻结：1 时处理层跳过
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,                        -- 下次可重试时间（epoch 秒），NULL 表示立即可处理
    checkpoint    TEXT,                        -- JSON：处理位置快照，人工处置后从这里继续
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    UNIQUE (external_id, content_hash)         -- 同编号+同内容只落一条
);
CREATE INDEX IF NOT EXISTS idx_deliveries_pick ON deliveries(status, frozen, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_deliveries_ext ON deliveries(external_id);

CREATE TABLE IF NOT EXISTS conflicts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    external_id TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'open',  -- open|resolved
    created_at  REAL NOT NULL,
    resolved_at REAL,
    resolution  TEXT                           -- JSON：选定版本、操作人、备注
);
CREATE INDEX IF NOT EXISTS idx_conflicts_status ON conflicts(status);

CREATE TABLE IF NOT EXISTS conflict_members (
    conflict_id INTEGER NOT NULL REFERENCES conflicts(id),
    delivery_id INTEGER NOT NULL REFERENCES deliveries(id),
    PRIMARY KEY (conflict_id, delivery_id)
);

CREATE TABLE IF NOT EXISTS outbox (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    delivery_id     INTEGER NOT NULL REFERENCES deliveries(id),
    effect_type     TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,      -- 幂等键：重启/重复投递不重复执行
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|executed|failed
    attempts        INTEGER NOT NULL DEFAULT 0,
    executed_at     REAL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);

-- 模拟“下游系统”的已应用效果表：下游凭幂等键去重，是 exactly-once 的最后一道保险
CREATE TABLE IF NOT EXISTS sink_effects (
    idempotency_key TEXT PRIMARY KEY,
    effect_type     TEXT NOT NULL,
    payload         TEXT NOT NULL,
    applied_at      REAL NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    ts          REAL NOT NULL,
    type        TEXT NOT NULL,                 -- signature_ok|signature_fail|accepted|duplicate|conflict_opened|...
    external_id TEXT,
    delivery_id INTEGER,
    detail      TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_events_type ON events(type);
CREATE INDEX IF NOT EXISTS idx_events_ext ON events(external_id);
"""


class Database:
    """单连接 + 可重入锁；WAL 模式下读写不互斥，写事务串行。"""

    def __init__(self, path: str):
        self._conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
        self._conn.row_factory = sqlite3.Row
        self._lock = threading.RLock()
        with self._lock:
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=FULL")   # 提交即落盘，ACK 才可靠
            self._conn.execute("PRAGMA foreign_keys=ON")
            self._conn.executescript(SCHEMA)

    @contextmanager
    def tx(self):
        """写事务：yield 一个 cursor，正常结束 COMMIT，异常 ROLLBACK。"""
        with self._lock:
            cur = self._conn.cursor()
            try:
                cur.execute("BEGIN IMMEDIATE")
                yield cur
                cur.execute("COMMIT")
            except BaseException:
                cur.execute("ROLLBACK")
                raise
            finally:
                cur.close()

    def query(self, sql: str, params: tuple = ()) -> list[sqlite3.Row]:
        with self._lock:
            return self._conn.execute(sql, params).fetchall()

    def query_one(self, sql: str, params: tuple = ()):
        rows = self.query(sql, params)
        return rows[0] if rows else None

    def close(self):
        with self._lock:
            self._conn.close()

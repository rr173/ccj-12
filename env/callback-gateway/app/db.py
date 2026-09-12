"""持久层：SQLite（WAL + FULL sync），所有写操作走事务，提交后才算落盘。

表结构：
- deliveries       每一次回调内容的一个版本（同一 external_id 可有多份不同内容）
- conflicts        同编号不同内容产生的冲突单
- conflict_members 冲突单与内容版本的关联
- outbox           外部副作用的发件箱（幂等键保证 exactly-once；replay_task_id 标识重放产生的行）
- sink_effects     模拟下游系统的已应用记录（下游按幂等键去重）
- events           只增不删的审计日志（签名/冲突/重试/处置/重放全部可查）
- key_config_versions  密钥配置每次切换/尝试的记录（版本、操作者、时间、结果）
- replay_batches   重放批次：一次提交的一组重放任务（发起人、原因、筛选快照、进度、并发配额）
- replay_tasks     单条重放任务：原始版本、状态机、重试位置（checkpoint），批内按内容去重；
                   带版本时间快照（同编号有序执行的排序键）与最近阻塞原因（审计去重用）
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
    replay_task_id  INTEGER REFERENCES replay_tasks(id),  -- NULL=正常处理；否则为重放产生的副作用
    effect_type     TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,      -- 幂等键：重启/重复投递不重复执行
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',  -- pending|executed|failed|cancelled（人工未选中而取消）
    attempts        INTEGER NOT NULL DEFAULT 0,
    executed_at     REAL,
    created_at      REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_outbox_status ON outbox(status);
CREATE INDEX IF NOT EXISTS idx_outbox_replay_task ON outbox(replay_task_id);

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

-- 密钥配置轮换记录：每次提交（无论成败）一条，重启后据此恢复最后一次成功配置
CREATE TABLE IF NOT EXISTS key_config_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER,               -- applied 时的新版本号（单调递增）；rejected 为 NULL
    result      TEXT NOT NULL,         -- applied | rejected
    config      TEXT,                  -- applied：规范化后的配置 JSON；rejected：原始提交内容
    operator    TEXT NOT NULL,
    reason      TEXT,                  -- rejected 的校验失败原因（JSON 数组）
    created_at  REAL NOT NULL
);

-- 重放批次：运营一次提交的一组重放任务
CREATE TABLE IF NOT EXISTS replay_batches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT UNIQUE,           -- 提交幂等键：重复提交返回原批次，不生成第二批任务
    operator    TEXT NOT NULL,         -- 发起人
    reason      TEXT NOT NULL,         -- 重放原因
    status      TEXT NOT NULL DEFAULT 'running',  -- running|paused|completed|completed_with_failures|cancelled
    filters     TEXT NOT NULL DEFAULT '{}',       -- 提交时的筛选条件快照（可追溯）
    max_concurrency INTEGER,           -- 批次级并发配额：最多同时处理的任务数；NULL 不限
    total       INTEGER NOT NULL DEFAULT 0,       -- 进度：任务总数 / 各终态计数
    done        INTEGER NOT NULL DEFAULT 0,
    failed      INTEGER NOT NULL DEFAULT 0,
    cancelled   INTEGER NOT NULL DEFAULT 0,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    finished_at REAL
);

-- 单条重放任务：每条都记录发起人、原始版本（delivery_id）、原因和进度
CREATE TABLE IF NOT EXISTS replay_tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id      INTEGER NOT NULL REFERENCES replay_batches(id),
    delivery_id   INTEGER NOT NULL REFERENCES deliveries(id),  -- 原始版本（内容不复制，引用落盘原文）
    external_id   TEXT NOT NULL,
    operator      TEXT NOT NULL,       -- 发起人（冗余到每条，单条可查）
    reason        TEXT NOT NULL,       -- 原因（冗余到每条）
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending|processing|done|failed|cancelled
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,                -- 下次可重试时间（epoch 秒），NULL 表示立即可执行
    checkpoint    TEXT,                -- JSON：处理位置快照，重启后从这里继续
    last_error    TEXT,
    delivery_created_at REAL,          -- 原始版本的落盘时间快照：同编号有序执行的排序键
    blocked_reason  TEXT,              -- worker 维护的最近阻塞原因（审计去重用；实时原因见批次详情）
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    finished_at   REAL,
    UNIQUE (batch_id, delivery_id)     -- 同一份内容在同一批里只生成一个重放任务
);
CREATE INDEX IF NOT EXISTS idx_replay_tasks_pick ON replay_tasks(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_replay_tasks_batch ON replay_tasks(batch_id);
CREATE INDEX IF NOT EXISTS idx_replay_tasks_active ON replay_tasks(delivery_id, status);
CREATE INDEX IF NOT EXISTS idx_replay_tasks_order ON replay_tasks(batch_id, external_id, status);
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
            self._migrate()   # 先补旧库的列，再建表（SCHEMA 里的索引依赖新列）
            self._conn.executescript(SCHEMA)

    def _migrate(self):
        """对老版本数据库就地补列（新库由 SCHEMA 直接建出完整结构，这里自动跳过）。"""
        tables = {r["name"] for r in self._conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table'")}
        if "outbox" in tables:
            cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(outbox)")}
            if cols and "replay_task_id" not in cols:
                self._conn.execute(
                    "ALTER TABLE outbox ADD COLUMN replay_task_id INTEGER "
                    "REFERENCES replay_tasks(id)")
        if "replay_batches" in tables:
            cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(replay_batches)")}
            if cols and "max_concurrency" not in cols:
                self._conn.execute(
                    "ALTER TABLE replay_batches ADD COLUMN max_concurrency INTEGER")
        if "replay_tasks" in tables:
            cols = {r["name"] for r in self._conn.execute("PRAGMA table_info(replay_tasks)")}
            if cols and "delivery_created_at" not in cols:
                self._conn.execute(
                    "ALTER TABLE replay_tasks ADD COLUMN delivery_created_at REAL")
                # 回填排序键：取自原始版本的落盘时间（deliveries 永不改写 created_at）
                self._conn.execute(
                    """UPDATE replay_tasks SET delivery_created_at =
                       (SELECT created_at FROM deliveries
                        WHERE deliveries.id = replay_tasks.delivery_id)""")
            if cols and "blocked_reason" not in cols:
                self._conn.execute(
                    "ALTER TABLE replay_tasks ADD COLUMN blocked_reason TEXT")

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

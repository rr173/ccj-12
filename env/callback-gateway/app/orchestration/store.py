"""编排模块持久层：表结构 + 图版本/实例/异常/审计的读写。

所有写操作都在 Database.tx()（BEGIN IMMEDIATE，写事务全库串行）内完成，
因此「回调入口 / 期限扫描 / 人工处置 / 重启恢复」之间天然只有一个有效结果。

审计顺序：orch_audit.id 是全实例严格有序的全局序号；同一实例内 seq 单调递增
（实例事件从 1 开始），因此按 (instance_id, seq, id) 可重放出实例的完整有序轨迹，
按 id 可重放全部流程的全局顺序。图版本变化（无实例）只写全局事件（seq=NULL）。

表：
- orch_graph_versions    每次发布的不可变图快照（含 rejected 尝试）；回滚只推进 current
- orch_graph_current     每种业务流程的当前生效版本指针（新实例创建时读取并固化）
- orch_instances         流程实例：固定 graph_version；active 唯一约束保证同关联键不重复建
- orch_node_states       实例×节点：状态机、收到的不同版本、冲突、选定版本、阻塞原因、跳过
- orch_callbacks         每份回调原文（含归属历史：orphan->bound、late 标记，绝不改写）
- orch_effects           节点业务处理产出的外部效果（幂等键唯一，pending->executed/failed）
- orch_missing_events    期限到达仍缺前置/缺回调的可查询异常（open->resolved/voided）
- orch_audit             全局+按实例有序审计（seq 在实例内单调，全局按 id 排序）
"""
from __future__ import annotations

import json
import time

from ..db import Database

SCHEMA_ORCH = """
-- 图版本：发布成功写不可变快照；result=rejected 的尝试也留痕（reason JSON 数组）。
-- 版本号在 process_type 内单调递增，任何后续发布/回滚都不修改本行内容。
CREATE TABLE IF NOT EXISTS orch_graph_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    process_type TEXT NOT NULL,
    version     INTEGER,                       -- applied/rollback 指向的版本；rejected=NULL
    result      TEXT NOT NULL,                 -- applied | rejected
    spec_json   TEXT NOT NULL,                 -- applied：规范化快照；rejected：原始提交
    reason      TEXT,                          -- rejected 原因（JSON 数组）
    created_by  TEXT NOT NULL,
    note        TEXT,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_orch_graph_ver_type
    ON orch_graph_versions(process_type, version);

-- 当前生效版本指针（每种流程一行）。回滚=把指针改回旧版本，旧实例持有自己的
-- graph_version 不受影响；只有之后新建的实例读到新指针。
CREATE TABLE IF NOT EXISTS orch_graph_current (
    process_type TEXT PRIMARY KEY,
    graph_version INTEGER NOT NULL REFERENCES orch_graph_versions(id),
    updated_by  TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    reason      TEXT
);

-- 流程实例：创建时把 graph_version（orch_graph_versions.id）固化下来。
-- status=active 的实例在同流程+关联键上至多一条（部分唯一索引兜底并发建实例）。
CREATE TABLE IF NOT EXISTS orch_instances (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_key TEXT NOT NULL,                 -- 对外标识（uuid）
    process_type TEXT NOT NULL,
    correlation_key TEXT NOT NULL,              -- 从回调内容取值位置取出的关联键
    graph_version_id INTEGER NOT NULL REFERENCES orch_graph_versions(id),
    status       TEXT NOT NULL DEFAULT 'active', -- active|completed|terminated
    created_by   TEXT NOT NULL DEFAULT 'system',
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    completed_at REAL,
    terminated_at REAL,
    terminate_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_orch_inst_active_key
    ON orch_instances(process_type, correlation_key) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_orch_inst_type ON orch_instances(process_type, status);
CREATE INDEX IF NOT EXISTS idx_orch_inst_key ON orch_instances(instance_key);

-- 节点状态（实例创建时每个图节点一行）。
-- status: WAITING(未就绪) | READY(就绪待处理) | PROCESSING(业务处理中) |
--         COMPLETED | SKIPPED | BLOCKED(业务失败) | TERMINATED(随实例终止)
-- versions_json: [{"callback_id","hash","received_at"}] 收到的内容不同版本（只增）
-- conflict: none|open|resolved；selected_callback_id 为最终采用版本
CREATE TABLE IF NOT EXISTS orch_node_states (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  INTEGER NOT NULL REFERENCES orch_instances(id),
    node_code    TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'WAITING',
    versions_json TEXT NOT NULL DEFAULT '[]',
    conflict     TEXT NOT NULL DEFAULT 'none',  -- none|open|resolved
    selected_callback_id INTEGER REFERENCES orch_callbacks(id),
    blocked_reason TEXT,
    attempts     INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    deadline     REAL,                          -- created_at + wait_seconds（NULL=无期限）
    deadline_checked_at REAL,
    released_at  REAL,                          -- 前置满足、进入 READY 的时间（汇合释放点）
    completed_at REAL,
    skipped_at   REAL,
    skipped_by   TEXT,
    skip_reason  TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    UNIQUE (instance_id, node_code)
);
CREATE INDEX IF NOT EXISTS idx_orch_node_status ON orch_node_states(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_orch_node_inst ON orch_node_states(instance_id);
CREATE INDEX IF NOT EXISTS idx_orch_node_deadline
    ON orch_node_states(deadline) WHERE deadline IS NOT NULL;

-- 回调原文（只增，绝不改写 payload）。
-- ownership: received(按取值位置自动入位) | orphan(取不到/匹配不上，待核对) |
--            bound(人工重关联到实例节点) | late(终止/终节点后迟到，只记录)
-- bound_from_callback_id 非空时表示本回调是从那条 orphan 重新关联而来（归属链保留）。
CREATE TABLE IF NOT EXISTS orch_callbacks (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    callback_uid TEXT NOT NULL UNIQUE,          -- 外部回调幂等标识（头 X-Callback-Id 或内容指纹）
    process_type TEXT,                          -- orphan 时可为 NULL（完全无法识别）
    correlation_key TEXT,                       -- 按当时生效图解析出的关联键（可能为空）
    payload_hash TEXT NOT NULL,                 -- 规范化内容 SHA-256（版本异同判定）
    payload      TEXT NOT NULL,
    received_at  REAL NOT NULL,
    ownership    TEXT NOT NULL,                 -- received|orphan|bound|late
    instance_id  INTEGER REFERENCES orch_instances(id),
    node_code    TEXT,
    bound_from_callback_id INTEGER REFERENCES orch_callbacks(id),
    bound_by     TEXT,
    bound_at     REAL,
    bound_note   TEXT,
    late_reason  TEXT
);
CREATE INDEX IF NOT EXISTS idx_orch_cb_match
    ON orch_callbacks(process_type, correlation_key, ownership);
CREATE INDEX IF NOT EXISTS idx_orch_cb_inst ON orch_callbacks(instance_id, node_code);
CREATE INDEX IF NOT EXISTS idx_orch_cb_orphan ON orch_callbacks(ownership) WHERE ownership='orphan';

-- 外部副作用：节点 COMPLETED 同事务写入；幂等键全局唯一，重启/重试绝不重复产生。
CREATE TABLE IF NOT EXISTS orch_effects (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id     INTEGER NOT NULL REFERENCES orch_instances(id),
    node_code       TEXT NOT NULL,
    idempotency_key TEXT NOT NULL UNIQUE,
    effect_type     TEXT NOT NULL,
    payload         TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending', -- pending|executed|failed
    attempts        INTEGER NOT NULL DEFAULT 0,
    last_error      TEXT,
    created_at      REAL NOT NULL,
    executed_at     REAL
);
CREATE INDEX IF NOT EXISTS idx_orch_effect_status ON orch_effects(status);
CREATE INDEX IF NOT EXISTS idx_orch_effect_inst ON orch_effects(instance_id);

-- 缺失事件异常：期限到达时由扫描生成；同一实例×节点同时只有一条 open
-- （部分唯一索引兜底扫描与重关联/跳过并发）。kind 区分缺回调与缺前置；
-- blocked_paths 为受阻分支（从该节点出发的后继依赖路径）。
CREATE TABLE IF NOT EXISTS orch_missing_events (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    instance_id  INTEGER NOT NULL REFERENCES orch_instances(id),
    node_code    TEXT NOT NULL,
    kind         TEXT NOT NULL,                  -- missing_callback | missing_prerequisite
    reason_code  TEXT NOT NULL,                  -- deadline_passed | no_prerequisite
    detail_json  TEXT NOT NULL DEFAULT '{}',
    blocked_paths_json TEXT NOT NULL DEFAULT '[]',
    deadline     REAL NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',   -- open|resolved|voided
    resolved_by  TEXT,
    resolved_at  REAL,
    resolve_reason TEXT,                         -- callback_arrived|skipped|instance_terminated
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_orch_missing_open
    ON orch_missing_events(instance_id, node_code) WHERE status='open';
CREATE INDEX IF NOT EXISTS idx_orch_missing_inst
    ON orch_missing_events(instance_id, status);
CREATE INDEX IF NOT EXISTS idx_orch_missing_query
    ON orch_missing_events(status, deadline);

-- 审计：seq 在单个实例内单调（实例事件）；实例无关事件 seq=NULL，按 id 取全局顺序。
CREATE TABLE IF NOT EXISTS orch_audit (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    ts           REAL NOT NULL,
    seq          INTEGER,
    instance_id  INTEGER,
    event_type   TEXT NOT NULL,
    node_code    TEXT,
    callback_id  INTEGER,
    detail_json  TEXT NOT NULL DEFAULT '{}'
);
CREATE INDEX IF NOT EXISTS idx_orch_audit_inst ON orch_audit(instance_id, seq);
CREATE INDEX IF NOT EXISTS idx_orch_audit_type ON orch_audit(event_type);
"""


def init_orch_schema(db: Database) -> None:
    """幂等建表（幂等由 IF NOT EXISTS 保证；调用方持写锁）。"""
    with db._lock:  # noqa: SLF001 - 与 Database 同包协作，避免两个线程并发 executescript
        db._conn.executescript(SCHEMA_ORCH)  # noqa: SLF001


# ---- 审计辅助 -------------------------------------------------------------

def audit(cur, event_type: str, instance_id: int | None = None,
          node_code: str | None = None, callback_id: int | None = None,
          detail: dict | None = None, ts: float | None = None) -> None:
    """在当前事务写审计；实例事件的 seq 在该实例内取 max+1（并发由写事务串行保证）。"""
    seq = None
    if instance_id is not None:
        row = cur.execute(
            "SELECT COALESCE(MAX(seq),0)+1 AS s FROM orch_audit WHERE instance_id=?",
            (instance_id,),
        ).fetchone()
        seq = row["s"]
    cur.execute(
        """INSERT INTO orch_audit
           (ts, seq, instance_id, event_type, node_code, callback_id, detail_json)
           VALUES (?,?,?,?,?,?,?)""",
        (time.time() if ts is None else ts, seq, instance_id, event_type,
         node_code, callback_id,
         json.dumps(detail or {}, ensure_ascii=False, sort_keys=True)),
    )

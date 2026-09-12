"""持久层：SQLite（WAL + FULL sync），所有写操作走事务，提交后才算落盘。

表结构：
- deliveries       每一次回调内容的一个版本（同一 external_id 可有多份不同内容）
- conflicts        同编号不同内容产生的冲突单
- conflict_members 冲突单与内容版本的关联
- outbox           外部副作用的发件箱（幂等键保证 exactly-once；replay_task_id 标识重放产生的行）
- sink_effects     模拟下游系统的已应用记录（下游按幂等键去重）
- events           只增不删的审计日志（签名/冲突/重试/处置/重放全部可查）
- key_config_versions  密钥配置每次切换/尝试的记录（版本、操作者、时间、结果）
- replay_batches   重放批次：一次提交的一组重放任务（发起人、原因、筛选快照、进度、并发配额、
                    风险等级与审批状态：高风险批次须由非发起人批准后才能进入 running；
                    带提交时采用的审批策略快照——策略更新不影响已提交批次）
- replay_tasks     单条重放任务：原始版本、状态机、重试位置（checkpoint），批内按内容去重；
                   带版本时间快照（同编号有序执行的排序键）与最近阻塞原因（审计去重用）
- replay_policy_versions  审批策略每次变更/尝试的记录（版本、操作者、时间、结果）；
                    提交批次时按当前生效策略生成审批节点，已提交批次不受后续变更影响
- replay_policy_stable    每个风险等级当前的稳定策略版本（灰度回滚的目标版本；
                    候选试运行/灰度期间它继续承接未命中候选的全部流量）
- replay_policy_releases  候选策略灰度发布单：风险等级、候选版本、批次规模闸门、
                    分流百分比、批次序号（单调递增）与生命周期
                    （candidate/paused/promoted/rolled_back/superseded）；
                    分流结果只取决于等级/编号/闸门/百分比，重复提交与重启保持不变
- replay_approval_nodes   批次的多级审批节点：允许承担的角色列表、法定人数
                    （required_approvals）、实际审批人、截止时间；串行链逐节点激活，
                    并行节点同时待决；节点有效赞成票达到法定人数才满足，任一拒绝/
                    超时终止批次，全部满足后批次才进入 running
- replay_node_votes       节点上的每一票（赞成/拒绝/跳过）：实际承担角色与所用委托；
                    同一节点同一人只保留一张有效票（部分唯一索引），同一批次同一人
                    不能在多个节点持有效票；委托在节点满足前失效/撤销时其票置为无效
- replay_delegations      审批委托：运营把某角色在生效/失效时间窗内委托给受托人，
                    可撤销、重新激活；到期由 worker 扫描失效，节点决定时必须仍有效
- replay_policy_changes   策略变更单：影响预览（含策略版本与生成时间）+ 审批门禁；
                    高风险变更须由不同于提交人的运营审批后才能执行，拒绝/超时/重复
                    提交/并发审批都只是变更单状态转移，执行与配置变更同一事务，
                    不会产生部分生效
- approval_contacts       审批通知联系人目录：站内待办接收人花名册 + 可选邮件/webhook 通道
- approval_notify_events  审批通知事件（去重单元：event_key 唯一），带事件版本
- approval_todos          站内待办：同一事件对同一接收人至多一条；记录来源批次/变更单、
                    节点、接收人、事件版本与状态；处理后回写原审批动作，重复/过期/
                    并发处理不重复投票、不越过审批门禁
- approval_notification_deliveries 邮件/webhook 外发投递：指数退避重试，超限隔离
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
    status      TEXT NOT NULL DEFAULT 'running',  -- running|paused|pending_approval|rejected|completed|completed_with_failures|cancelled
    filters     TEXT NOT NULL DEFAULT '{}',       -- 提交时的筛选条件快照（可追溯）
    max_concurrency INTEGER,           -- 批次级并发配额：最多同时处理的任务数；NULL 不限
    risk_level       TEXT NOT NULL DEFAULT 'normal',  -- normal|high：高风险须先审批
    approval_note    TEXT,             -- 提交时的审批说明（高风险必填，随批次可追溯）
    approval_status  TEXT NOT NULL DEFAULT 'not_required',  -- not_required|pending|approved|rejected|expired
    approver         TEXT,             -- 批准/拒绝人（须不同于发起人 operator）
    approval_reason  TEXT,             -- 拒绝原因（拒绝时必填）
    approved_at      REAL,             -- 明确批准的时间（epoch 秒）；NULL 表示尚未批准
    approval_deadline REAL,            -- 当前待决节点的审批截止时间（= 活动节点最早截止）；超时未决由 worker 释放
    policy_version INTEGER,            -- 提交时命中的审批策略版本；NULL 表示内置默认策略
    policy_snapshot TEXT,              -- 提交时解析出的策略快照 JSON（规则/模式/节点规格/分流命中），策略更新不影响本批
    policy_lane  TEXT NOT NULL DEFAULT 'stable',  -- 分流命中：stable|candidate（命中的是稳定版本还是灰度候选）
    rollout_id   INTEGER REFERENCES replay_policy_releases(id),  -- 命中的灰度发布单（稳定流量为 NULL）
    rollout_seq  INTEGER,              -- 提交时该风险等级的发布批次序号（第几次灰度发布，随批次留痕）
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

-- 审批策略版本：每次提交（无论成败）一条；提交重放批次时以最后一份 applied 策略为准
CREATE TABLE IF NOT EXISTS replay_policy_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER,               -- applied 时的策略版本号（单调递增）；rejected 为 NULL
    result      TEXT NOT NULL,         -- applied | rejected
    policy      TEXT,                  -- applied：规范化后的策略 JSON；rejected：原始提交内容
    operator    TEXT NOT NULL,
    reason      TEXT,                  -- rejected 的校验失败原因（JSON 数组）
    created_at  REAL NOT NULL
);

-- 每个风险等级当前的稳定审批策略版本。整份策略提交（POST replay-policies）后，其规则
-- 覆盖到的风险等级在这里整份切换；候选灰度转正（promote）只切换对应等级这一行。
-- 回滚 = 把这一行改回上一稳定版本（promote/整份切换时记 prev 版本），只影响之后的新提交。
CREATE TABLE IF NOT EXISTS replay_policy_stable (
    risk_level   TEXT PRIMARY KEY,     -- high | normal
    policy_version INTEGER,            -- 稳定策略版本号；NULL = 内置默认策略
    rule_name    TEXT,                 -- 该等级在稳定策略中命中的规则名（便于展示）
    prev_policy_version INTEGER,       -- 上一稳定版本（回滚目标；NULL 表示无更早版本）
    updated_by   TEXT NOT NULL,
    updated_at   REAL NOT NULL,
    reason       TEXT                  -- 最近一次切换/回滚原因
);

-- 候选策略灰度发布单：同一风险等级至多一条未结束（candidate/paused）的发布。
-- 分流由 risk_level + min_size/max_size（批次规模闸门）+ rollout_percent（百分比）
-- 决定，结果对同一批次恒定（稳定哈希），与提交时机、重启无关。
CREATE TABLE IF NOT EXISTS replay_policy_releases (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    risk_level  TEXT NOT NULL,         -- 灰度针对的风险等级：high | normal
    rollout_seq INTEGER NOT NULL,      -- 该风险等级的发布批次序号（从 1 单调递增）
    candidate_version INTEGER NOT NULL,-- 候选策略版本号（replay_policy_versions.version）
    candidate_rule_name TEXT,          -- 候选策略中该等级将命中的规则名（试运行校验时确认）
    stable_version_at_publish INTEGER, -- 发布时该等级的稳定版本快照（NULL=内置默认；展示/审计用）
    min_size    INTEGER,               -- 批次规模闸门：任务条数下限（NULL=不限）
    max_size    INTEGER,               -- 批次规模闸门：任务条数上限（NULL=不限）
    rollout_percent INTEGER NOT NULL,  -- 命中闸门的批次分流到候选的百分比（1-100）
    status      TEXT NOT NULL,         -- candidate|paused|promoted|rolled_back|superseded
    operator    TEXT NOT NULL,         -- 发布人
    note        TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    paused_at   REAL,
    paused_by   TEXT,
    pause_reason TEXT,
    promoted_at REAL,
    promoted_by TEXT,
    rolled_back_at REAL,
    rolled_back_by TEXT,
    rollback_reason TEXT,              -- 回滚原因（必填，随发布单与审计留痕）
    superseded_at REAL,
    superseded_by TEXT
);
-- 同一风险等级至多一条未结束的灰度发布（部分唯一索引兜底并发发布）
CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_policy_releases_open
    ON replay_policy_releases(risk_level)
    WHERE status IN ('candidate','paused');
CREATE INDEX IF NOT EXISTS idx_replay_policy_releases_status
    ON replay_policy_releases(status);
CREATE INDEX IF NOT EXISTS idx_replay_policy_releases_level_seq
    ON replay_policy_releases(risk_level, rollout_seq);

-- 批次的多级审批节点：提交时按策略快照一次性生成，之后不随策略变更而改变。
-- 串行链：只有当前节点 active，上一节点满足后才激活下一节点（激活时才起算截止时间）；
-- 并行：所有节点同时 active，全部满足才放行。任一节点拒绝/超时即终止整个批次。
-- 一个节点可有多个允许角色（allowed_roles JSON），required_approvals 为该节点法定
-- 人数：有效赞成票达到法定人数节点才满足（串行/并行都按节点分别计数）。
CREATE TABLE IF NOT EXISTS replay_approval_nodes (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    batch_id     INTEGER NOT NULL REFERENCES replay_batches(id),
    seq          INTEGER NOT NULL,          -- 链内顺序（0 起）
    role         TEXT NOT NULL,             -- 主指定角色（与 allowed_roles[0] 一致；'any' 表示任何非发起人）
    allowed_roles TEXT NOT NULL DEFAULT '["any"]',  -- 允许承担该节点的角色列表（JSON）
    required_approvals INTEGER NOT NULL DEFAULT 1,  -- 法定人数：有效赞成票达到此数节点才满足
    status       TEXT NOT NULL DEFAULT 'waiting',  -- waiting|active|approved|rejected|skipped|expired|cancelled
    timeout_seconds REAL NOT NULL,          -- 本节点审批时限（激活时起算）
    decided_by   TEXT,                      -- 使节点落定（达到法定人数/拒绝/跳过）的最后决定人（兼容展示，永不改写）
    decided_role TEXT,                      -- 该决定人实际承担的角色
    decision_reason TEXT,                   -- 拒绝/跳过原因（这两类决定必填）
    decision_note TEXT,                     -- 决定备注
    activated_at REAL,                      -- 节点进入待决的时间
    deadline     REAL,                      -- 本节点截止时间；超时由 worker 释放整个批次
    decided_at   REAL,
    created_at   REAL NOT NULL,
    UNIQUE (batch_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_replay_approval_nodes_batch ON replay_approval_nodes(batch_id, status);
CREATE INDEX IF NOT EXISTS idx_replay_approval_nodes_due ON replay_approval_nodes(status, deadline);

-- 审批委托：运营（operator）把某个审批角色（role）在 [valid_from, valid_to] 时间窗内
-- 委托给受托人（delegatee）。决定时受托人凭有效委托承担该角色；委托撤销/到期后，
-- 其投在尚未满足的节点上的票随之失效（已满足/落定节点不被改写）。
CREATE TABLE IF NOT EXISTS replay_delegations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    role        TEXT NOT NULL,              -- 被委托的审批角色
    delegatee   TEXT NOT NULL,              -- 受托人（凭委托承担该角色的运营人员）
    delegator   TEXT NOT NULL,              -- 委托人/创建人（授予角色权力的运营人员）
    status      TEXT NOT NULL DEFAULT 'active',  -- active|revoked（到期由扫描改写为 expired）
    valid_from  REAL NOT NULL,
    valid_to    REAL NOT NULL,
    note        TEXT,
    revoked_at  REAL,
    revoked_by  TEXT,
    revoke_reason TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replay_delegations_role ON replay_delegations(role, status);
CREATE INDEX IF NOT EXISTS idx_replay_delegations_due ON replay_delegations(status, valid_to);

-- 节点上的每一票。approve 票在委托失效/撤销后可能置为 invalid（节点尚未满足时）；
-- reject/skip 票一投即让节点落定（拒绝还会终止整个批次），始终保持 valid 不可改写。
-- 同一节点同一人至多一张有效票（防重复计数/并发重复决定）；同一批次同一人不能在
-- 多个节点持有效票（不能在同一批次承担多个节点）。
CREATE TABLE IF NOT EXISTS replay_node_votes (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    node_id       INTEGER NOT NULL REFERENCES replay_approval_nodes(id),
    batch_id      INTEGER NOT NULL REFERENCES replay_batches(id),
    vote          TEXT NOT NULL,            -- approve|reject|skip
    voter         TEXT NOT NULL,            -- 实际投票人
    voter_role    TEXT NOT NULL,            -- 投票人实际承担的角色（与节点允许角色一致）
    delegation_id INTEGER REFERENCES replay_delegations(id),  -- 所用委托；NULL=直接持角色承担
    status        TEXT NOT NULL DEFAULT 'valid',  -- valid|invalid（仅 approve 票可能失效）
    reason        TEXT,
    note          TEXT,
    created_at    REAL NOT NULL,
    invalidated_at REAL
);
-- 同一节点同一人：最多一张有效票（失效后可以重新投票）
CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_votes_node_voter_valid
    ON replay_node_votes(node_id, voter) WHERE status='valid';
-- 同一批次同一人：最多在一个节点持有效票（不能承担多个节点）
CREATE UNIQUE INDEX IF NOT EXISTS idx_replay_votes_batch_voter_valid
    ON replay_node_votes(batch_id, voter) WHERE status='valid';
CREATE INDEX IF NOT EXISTS idx_replay_votes_node ON replay_node_votes(node_id, status);

-- 策略变更单：「提交新策略 / 发布候选版本」的影响预览与审批门禁。
-- 提交即固化影响预览与基线（当前 applied 版本 + 各等级稳定指针）；pending/approved
-- 期间不改变任何线上配置（旧稳定版本继续服务）。高风险变更（risk_class=high）必须由
-- 不同于提交人的运营 approve 后才能 apply；apply 把「整份策略生效 / 候选灰度发布」与
-- 变更单落定放在同一事务，并校验基线未被其他变更推进（stale 拒绝）——拒绝、超时
-- （expires_at，worker 扫描 + 决定时惰性判定）、重复提交（request_id 幂等）、并发审批
-- （写事务串行 + 条件状态转移）都只是变更单状态转移，不会产生部分生效。
CREATE TABLE IF NOT EXISTS replay_policy_changes (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    request_id  TEXT UNIQUE,           -- 提交幂等键：重复提交返回原变更单，不生成第二张
    change_type TEXT NOT NULL,         -- apply_policy | publish_release
    status      TEXT NOT NULL,         -- pending|approved|applied|rejected|expired
    operator    TEXT NOT NULL,         -- 提交人（高风险变更的审批人必须不同于此人）
    note        TEXT,
    policy      TEXT,                  -- apply_policy：规范化策略 JSON（批准后按此整份生效）
    candidate_version INTEGER,         -- publish_release：候选策略版本
    risk_level  TEXT,                  -- publish_release：灰度风险等级
    min_size    INTEGER,               -- publish_release：批次规模闸门
    max_size    INTEGER,
    rollout_percent INTEGER,           -- publish_release：分流百分比
    base_version INTEGER,              -- 预览基线：当前 applied 策略版本（NULL=内置默认）
    base_stable_versions TEXT NOT NULL,-- 预览基线：各风险等级稳定指针 JSON
    risk_class  TEXT NOT NULL,         -- high|standard：high 必须经他人审批后才能执行
    requires_approval INTEGER NOT NULL,
    preview     TEXT NOT NULL,         -- 影响预览 JSON（含策略版本与生成时间，随单固化）
    decision    TEXT,                  -- approved|rejected（审批决定）
    decided_by  TEXT,
    decided_at  REAL,
    decision_reason TEXT,              -- 拒绝原因（拒绝必填）
    decision_note TEXT,
    applied_version INTEGER,           -- apply_policy 生效后的策略版本（变更后版本）
    release_id  INTEGER,               -- publish_release 创建的灰度发布单
    applied_by  TEXT,
    applied_at  REAL,
    expires_at  REAL NOT NULL,         -- 审批超时：到期未决/未执行的变更不能再生效
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_replay_policy_changes_status
    ON replay_policy_changes(status, expires_at);

-- 审批通知联系人目录：站内待办的接收人花名册（指定角色节点的通知对象还须持有当前
-- 有效的角色委托）。email/webhook 为可选外发通道，inbox（站内待办）始终生成。
CREATE TABLE IF NOT EXISTS approval_contacts (
    name        TEXT PRIMARY KEY,          -- 运营人员标识（与批次/变更单 operator、委托受托人同一命名空间）
    email       TEXT,                      -- 邮件通道地址（channels 含 email 时必填）
    webhook_url TEXT,                      -- webhook 通道地址（channels 含 webhook 时必填）
    channels    TEXT NOT NULL DEFAULT '[]', -- 启用的外发通道 JSON：子集 ["email","webhook"]；站内待办不受此限
    active      INTEGER NOT NULL DEFAULT 1,-- 1 接收新通知；停用后不再生成待办，未处理待办关闭
    created_by  TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    deactivated_at REAL,
    deactivated_by TEXT
);

-- 审批通知事件：去重单元。event_key 唯一（如 node:activated:{node_id}、
-- deadline:{node_id}、vote:{vote_id}、change:required:{change_id}），同一事件
-- 重放/并发触发只落一行；state_version 为该来源（批次/变更单）上事件的单调版本号。
CREATE TABLE IF NOT EXISTS approval_notify_events (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_key     TEXT NOT NULL UNIQUE,
    event_type    TEXT NOT NULL,  -- activated|vote_received|deadline_approaching|
                                  -- node_rejected|node_timeout|batch_approved|
                                  -- batch_cancelled|change_required|change_approved|
                                  -- change_rejected|change_expired|change_applied
    source_type   TEXT NOT NULL,  -- batch | change
    batch_id      INTEGER REFERENCES replay_batches(id),
    change_id     INTEGER REFERENCES replay_policy_changes(id),
    node_id       INTEGER,        -- 批次审批节点事件的节点 id
    roles         TEXT NOT NULL DEFAULT '[]',  -- 事件对应节点的允许角色快照（'any' 为通配）
    actionable    INTEGER NOT NULL DEFAULT 1,  -- 1=待办可回写审批动作；0=纯告知
    state_version INTEGER NOT NULL DEFAULT 1,  -- 来源上的事件版本（单调递增）
    subject       TEXT NOT NULL,
    body          TEXT NOT NULL,
    payload       TEXT NOT NULL DEFAULT '{}',  -- webhook 通道的结构化负载
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notify_events_batch ON approval_notify_events(batch_id);
CREATE INDEX IF NOT EXISTS idx_notify_events_change ON approval_notify_events(change_id);
CREATE INDEX IF NOT EXISTS idx_notify_events_node ON approval_notify_events(node_id);

-- 站内待办：每个事件对每个接收人至多一条（UNIQUE(event_id, recipient) 兜底并发/重放）。
-- 记录来源批次/变更单、节点、接收人、事件版本与状态机；处理后回写原审批动作
-- （handle_action），重复处理/过期待办/并发处理由条件更新与来源门禁挡下。
CREATE TABLE IF NOT EXISTS approval_todos (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    event_id      INTEGER NOT NULL REFERENCES approval_notify_events(id),
    recipient     TEXT NOT NULL,              -- 接收人（联系人名 / 发起人）
    status        TEXT NOT NULL DEFAULT 'unread',  -- unread|read|handled|expired|cancelled
    source_type   TEXT NOT NULL,              -- batch | change（冗余自事件，便于按来源查询）
    batch_id      INTEGER,
    change_id     INTEGER,
    node_id       INTEGER,
    event_type    TEXT NOT NULL,
    event_version INTEGER NOT NULL,           -- 生成时的事件版本（乐观门禁/展示）
    actionable    INTEGER NOT NULL DEFAULT 1,
    title         TEXT NOT NULL,
    read_at       REAL,
    handled_at    REAL,
    handled_by    TEXT,
    handle_action TEXT,                        -- 回写的原审批动作：approve|reject|skip
    close_reason  TEXT,
    closed_at     REAL,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    UNIQUE (event_id, recipient)
);
CREATE INDEX IF NOT EXISTS idx_approval_todos_recipient
    ON approval_todos(recipient, status);
CREATE INDEX IF NOT EXISTS idx_approval_todos_source
    ON approval_todos(source_type, batch_id, change_id);
CREATE INDEX IF NOT EXISTS idx_approval_todos_status ON approval_todos(status);
CREATE INDEX IF NOT EXISTS idx_approval_todos_node ON approval_todos(node_id);

-- 外发通知（邮件/webhook）：每条待办按联系人启用通道生成；失败按指数退避重试
-- （base*2^(attempts-1)，封顶），超过次数进 quarantine；待办关闭时未发出的投递取消。
CREATE TABLE IF NOT EXISTS approval_notification_deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    todo_id       INTEGER NOT NULL REFERENCES approval_todos(id),
    recipient     TEXT NOT NULL,
    channel       TEXT NOT NULL,              -- email | webhook
    address       TEXT NOT NULL,              -- email: 邮箱；webhook: URL
    subject       TEXT,
    body          TEXT,
    payload       TEXT,                       -- webhook 的 JSON 负载
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending|sent|failed|quarantined|cancelled
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    last_error    TEXT,
    sent_at       REAL,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_pick
    ON approval_notification_deliveries(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_todo
    ON approval_notification_deliveries(todo_id);
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
            self._migrate_pending_approval_nodes()   # 依赖新表，必须在 SCHEMA 之后
            self._migrate_node_votes()               # 法定人数/投票：回填老库已落定节点的票
            self._migrate_stable_pointers()          # 灰度：按最近 applied 策略补每个等级的稳定指针

    def _migrate_stable_pointers(self):
        """灰度功能引入前的老库：没有稳定指针表数据。按最近一次 applied 策略为每个
        风险等级补一条稳定指针（该策略即当时的「整份生效」策略，行为与升级前一致：
        两个等级都解析到同一版本；从未提交过策略时不补——继续走内置默认策略）。"""
        latest = self._conn.execute(
            "SELECT MAX(version) AS v FROM replay_policy_versions WHERE result='applied'"
        ).fetchone()["v"]
        if latest is None:
            return
        existing = {r["risk_level"] for r in self._conn.execute(
            "SELECT risk_level FROM replay_policy_stable")}
        now_rows = self._conn.execute(
            "SELECT created_at FROM replay_policy_versions WHERE version=? LIMIT 1",
            (latest,)).fetchone()
        created_at = now_rows["created_at"] if now_rows else 0.0
        for level in ("high", "normal"):
            if level in existing:
                continue
            self._conn.execute(
                """INSERT INTO replay_policy_stable
                   (risk_level, policy_version, rule_name, prev_policy_version,
                    updated_by, updated_at, reason)
                   VALUES (?,?,NULL,NULL,'migration',?,'backfilled_from_latest_applied')""",
                (level, latest, created_at))

    def _migrate_pending_approval_nodes(self):
        """老库中仍待决的高风险批次没有审批节点行：补一个内置默认节点
        （角色 any、沿用原批次截止时间），保证它们在新版本下仍可批准/拒绝/超时释放。"""
        rows = self._conn.execute(
            """SELECT b.id, b.created_at, b.approval_deadline FROM replay_batches b
               WHERE b.status='pending_approval' AND b.approval_status='pending'
                 AND NOT EXISTS (SELECT 1 FROM replay_approval_nodes n
                                 WHERE n.batch_id = b.id)""").fetchall()
        for r in rows:
            deadline = r["approval_deadline"]
            timeout = (deadline - r["created_at"]) if deadline else 3600.0
            self._conn.execute(
                """INSERT INTO replay_approval_nodes
                   (batch_id, seq, role, status, timeout_seconds,
                    activated_at, deadline, created_at)
                   VALUES (?,0,'any','active',?,?,?,?)""",
                (r["id"], max(timeout, 1.0), r["created_at"], deadline, r["created_at"]))

    def _migrate_node_votes(self):
        """老库中已落定（approved/rejected/skipped）的节点没有逐票记录：按节点上的
        决定人补一张票，使新模型下计数、「同一人不承担多节点」与审计视图保持完整。
        幂等：已有票的节点不重复补。仍 active 的老节点不补——它们的票将在决定时落。"""
        rows = self._conn.execute(
            """SELECT n.id AS node_id, n.batch_id AS batch_id, n.status AS status,
                      n.decided_by AS decided_by, n.decided_role AS decided_role,
                      n.decision_reason AS reason, n.decision_note AS note,
                      n.decided_at AS decided_at
               FROM replay_approval_nodes n
               WHERE n.status IN ('approved','rejected','skipped')
                 AND n.decided_by IS NOT NULL
                 AND NOT EXISTS (SELECT 1 FROM replay_node_votes v
                                 WHERE v.node_id = n.id)""").fetchall()
        for r in rows:
            vote = {"approved": "approve", "rejected": "reject",
                    "skipped": "skip"}[r["status"]]
            ts = r["decided_at"] or 0.0
            self._conn.execute(
                """INSERT INTO replay_node_votes
                   (node_id, batch_id, vote, voter, voter_role, delegation_id,
                    status, reason, note, created_at, invalidated_at)
                   VALUES (?,?,?,?,?,NULL,'valid',?,?,?,NULL)""",
                (r["node_id"], r["batch_id"], vote, r["decided_by"],
                 r["decided_role"] or "any", r["reason"], r["note"], ts))

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
            # 高风险审批：老库批次视为普通风险、无需审批（已在跑/已收尾的批次状态不变）
            for name, ddl in (
                ("risk_level", "TEXT NOT NULL DEFAULT 'normal'"),
                ("approval_note", "TEXT"),
                ("approval_status", "TEXT NOT NULL DEFAULT 'not_required'"),
                ("approver", "TEXT"),
                ("approval_reason", "TEXT"),
                ("approved_at", "REAL"),
                ("approval_deadline", "REAL"),
                # 多级审批策略：老库批次没有策略快照（NULL = 内置默认策略/早于策略功能）
                ("policy_version", "INTEGER"),
                ("policy_snapshot", "TEXT"),
                # 灰度分流：老批次视为稳定车道、无发布单
                ("policy_lane", "TEXT NOT NULL DEFAULT 'stable'"),
                ("rollout_id", "INTEGER"),
                ("rollout_seq", "INTEGER"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE replay_batches ADD COLUMN {name} {ddl}")
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
        if "replay_approval_nodes" in tables:
            # 法定人数/多角色：老节点视为单角色、法定人数 1
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(replay_approval_nodes)")}
            if cols and "allowed_roles" not in cols:
                self._conn.execute(
                    "ALTER TABLE replay_approval_nodes ADD COLUMN "
                    "allowed_roles TEXT NOT NULL DEFAULT '[\"any\"]'")
                # 回填允许角色：以老节点的指定角色为准
                self._conn.execute(
                    "UPDATE replay_approval_nodes SET "
                    "allowed_roles = json_array(role)")
            if cols and "required_approvals" not in cols:
                self._conn.execute(
                    "ALTER TABLE replay_approval_nodes ADD COLUMN "
                    "required_approvals INTEGER NOT NULL DEFAULT 1")

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

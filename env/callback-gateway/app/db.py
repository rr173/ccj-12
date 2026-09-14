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
- approval_notification_deliveries 邮件/webhook 外发投递：指数退避重试，超限隔离；
                    聚合窗口内为 held（窗口关闭合并为一条摘要）、静默时段为 delayed
                    （时段结束按原事件顺序放行）
- approval_aggregation_rules / approval_notification_groups /
  approval_notification_group_members  通知聚合：运营按接收人、来源批次与事件类型配置
                    聚合窗口，窗口内同组重复提醒的邮件/webhook 先 held，窗口关闭合并为
                    一条摘要（站内待办始终即时生成，不合并、不延迟）
- approval_quiet_schedules   静默时段：按接收人/通道配置每日重复或一次性时间窗，
                    窗内邮件/webhook 落 delayed（站内待办保留），时段结束按原事件顺序发送
- approval_escalation_policies / approval_escalations   升级策略：按节点角色配置逐级
                    升级接收人与触发时限（待办生成后 N 秒或截止前 N 秒）；每级触发与
                    升级级别落盘可查；原接收人处理（或来源落定）后升级立即停止，
                    未发出的升级通知同事务取消
- notif_route_versions / notif_route_current  通知通道路由配置的版本化快照：运营按事件
                    类型配置 email/webhook/inbox 的优先级、启用状态与接收条件；发送任务按
                    入队时的版本快照选择通道；新版本发布/回滚只影响之后入队的任务
- notif_send_tasks  路由发送任务：同一事件对同一接收人至多一条（UNIQUE(event_id,recipient)，
                    跨通道不重复产生业务通知）；固化路由版本与有序通道计划快照，记录当前
                    通道位置、尝试次数与最终结果（sent/quarantined/cancelled）
- notif_send_attempts  每个任务在每个通道上的每次尝试（成功/失败/超时，耗时与错误），
                    同时作为通道熔断「时间窗口内连续失败」的统计来源
- notif_channel_switches  通道切换历史：选中首选、超时/连续失败/熔断/通道停用切换、
                    最终成功或全部耗尽（原因逐条留痕）
- notif_channel_state   通道健康与熔断状态（closed/open/half_open）、打开时间、恢复探针
                    归属与手工启用/停用；熔断期间不向该通道派发新请求
- external_messages  外部通道消息登记：email/webhook 发送成功后登记外部服务返回的
                    message_id（无真实返回时由本地生成 local:{...} 占位），是回执匹配
                    与送达确认的锚点；source=route|legacy 分别对应版本化路由发送任务
                    与旧链路 approval_notification_deliveries；同一任务可因重试/故障
                    转移登记多条，被替代的行置 superseded，仅当前行接受终态
- receipts           外部回执原文（只增不删、绝不改写）：message_id + 通道 + 事件 +
                    内容哈希唯一（重复投递幂等）；能匹配消息的置 applied，不能匹配的进
                    unmatched 待核对队列，人工绑定后转 bound（仍不改正文）；终态回执
                    （delivered/bounced/complained/expired）不允许把已确认终态改回处理中
- receipt_status_history  外部消息/回执状态变化历史：每次入位、确认、重试、转人工、
                    绑定、重放只增一条，管理员可按通道/接收人/时间/状态查询完整轨迹
- receipt_keys       外部回执验签密钥（按通道 email/webhook，可轮换、可带过渡期）：
                    与回调入口的密钥环相互独立；active 行用于验签，旧行过渡期内可用
- receipt_policy     送达确认策略（单例行 id=1）：确认超时秒数、失败/超时后自动沿
                    通道计划重试的最大次数，超限或 action=manual 转人工
- notif_quota_versions / notif_quota_current  接收人通知额度与抑制窗口的版本化配置：
                    运营按接收人（NULL=全体）、事件级别（info|normal|critical）配置
                    时间窗口、额度、单事件占用与超额处理（delay|downgrade|manual）；
                    发送任务固化入队时的额度规则快照，配置更新/回滚不影响已入队任务
- notif_quota_reservations  发送任务领取前的原子预占（同一「规则版本×规则×接收人×
                    窗口」滚动/固定窗口内的占用汇总即当前消耗）；同一事件重试、回执
                    驱动的通道切换只复用本任务的预占（generation 不变不重复占用），
                    取消/人工忽略/确认不再发送时回收（reserved->released），发送成功
                    转 consumed；窗口到期后旧窗口行不再计入消耗（等同释放可用额度）
- notif_reconciliation_jobs / notif_reconciliation_findings  通知状态对账：运营按接收人、
                    事件、时间范围与状态发起只读扫描；异常保存检测时的不可变快照、原因、
                    当时规则版本与可用补偿。任务可分页、暂停/继续、失败重试，重启恢复
- notif_reconciliation_compensations / notif_compensation_send_plans  对账补偿：重新关联
                    回执、释放孤儿预占、关闭不再发送任务、创建补偿发送计划；所有补偿幂等，
                    补偿前/后状态与依据随操作留痕，不改写原始回执和历史审计
- notif_template_versions / notif_template_current  通知内容模板版本（按事件类型×通道，
                    支持 '*' 通配，多语言）：草稿可反复编辑，发布为不可变版本并推进当前
                    指针；发布校验失败落 rejected。变量声明必填/默认值/敏感/类型，占位符
                    与声明一致、敏感变量必须 |mask
- notif_template_render_failures  模板渲染失败记录（变量缺失/类型不符/无语言版本/超长/
                    敏感未脱敏）：阻塞期间不产生任何外发效果，按发送任务×通道或旧链路
                    投递幂等（未解除唯一），修复后重试渲染成功自动标记 resolved
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
    language    TEXT,                        -- 语言偏好（如 zh/en）；NULL=取系统缺省 NOTIF_DEFAULT_LANGUAGE
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
    open_at       REAL NOT NULL DEFAULT 0,    -- 进入开放（可处理）状态的时间：升级计时起点（=创建时间）
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
-- 聚合：group_id 非空表示该条已并入聚合组（held=窗口内暂缓，aggregated=已被摘要替代，
-- 内容保留可查不再单独发送）；静默：delayed=静默时段内暂缓，delayed_until 记录预计
-- 放行时间（时段变化/结束时由 worker 提前放行）。ordinal 为待办创建时的事件顺序号
-- （= approval_todos.id），静默放行后按 (ordinal,id) 恢复原事件顺序发送。
CREATE TABLE IF NOT EXISTS approval_notification_deliveries (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    todo_id       INTEGER NOT NULL REFERENCES approval_todos(id),
    recipient     TEXT NOT NULL,
    channel       TEXT NOT NULL,              -- email | webhook
    address       TEXT NOT NULL,              -- email: 邮箱；webhook: URL
    subject       TEXT,
    body          TEXT,
    payload       TEXT,                       -- webhook 的 JSON 负载
    status        TEXT NOT NULL DEFAULT 'pending',  -- pending|sent|failed|quarantined|cancelled|held|aggregated|delayed
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    last_error    TEXT,
    sent_at       REAL,
    group_id      INTEGER,                       -- 所属聚合组（approval_notification_groups.id；该表后建，故不声明外键）
    delayed_until REAL,
    ordinal       INTEGER NOT NULL DEFAULT 0,
    receipt_status TEXT,                         -- 外部回执状态（旧链路登记 message_id 后跟踪）
    external_message_id INTEGER REFERENCES external_messages(id),
    template_version_id INTEGER,                -- 入队渲染时采用的模板版本（NULL=静态正文/未配置）
    template_language TEXT,                      -- 实际采用语言
    content_sha256 TEXT,                         -- 最终正文指纹
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_pick
    ON approval_notification_deliveries(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_todo
    ON approval_notification_deliveries(todo_id);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_group
    ON approval_notification_deliveries(group_id);
CREATE INDEX IF NOT EXISTS idx_notif_deliveries_delayed
    ON approval_notification_deliveries(status, delayed_until);

-- 通知聚合规则：运营按接收人（NULL=全部）、来源批次（NULL=任意批次，指定批次时还可
-- 配事件类型；不指定批次时为避免跨审批串扰，组仍按各自批次/变更单划分）、事件类型
-- （NULL/空=全部）配置聚合窗口秒数。命中规则的通知，其邮件/webhook 在窗口内先 held，
-- 窗口关闭时同组每个通道合并为一条摘要。站内待办不受规则影响，始终即时逐条生成。
CREATE TABLE IF NOT EXISTS approval_aggregation_rules (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient   TEXT,                         -- NULL/空=全体接收人
    batch_id    INTEGER REFERENCES replay_batches(id),  -- NULL=任意来源批次（change 来源不按批次匹配）
    event_type  TEXT,                         -- NULL/空=全部事件类型
    window_seconds REAL NOT NULL,             -- 聚合窗口：首个事件落组后多少秒关闭
    active      INTEGER NOT NULL DEFAULT 1,
    created_by  TEXT NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_agg_rules_match
    ON approval_aggregation_rules(active, recipient, batch_id, event_type);

-- 聚合组：规则 + 接收人 + 通道 + 具体来源（批次/变更单）唯一。open=窗口开启中，
-- flushed=已发出摘要，cancelled=窗口内来源全部落定、摘要不再需要。
CREATE TABLE IF NOT EXISTS approval_notification_groups (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    rule_id      INTEGER NOT NULL REFERENCES approval_aggregation_rules(id),
    recipient    TEXT NOT NULL,
    channel      TEXT NOT NULL,
    source_type  TEXT NOT NULL,               -- batch | change
    batch_id     INTEGER,
    change_id    INTEGER,
    window_seconds REAL NOT NULL,
    status       TEXT NOT NULL DEFAULT 'open',  -- open|flushed|cancelled
    window_opened_at REAL NOT NULL,
    window_closes_at REAL NOT NULL,
    flushed_at   REAL,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    UNIQUE (rule_id, recipient, channel, source_type, batch_id, change_id)
);
CREATE INDEX IF NOT EXISTS idx_notif_groups_pick
    ON approval_notification_groups(status, window_closes_at);

-- 聚合组成员：组内每个被暂缓的投递（held 的 delivery 行）与来源待办，用于生成摘要、
-- 查询「哪些提醒被合并进了哪条摘要」；（组,投递）唯一兜底并发。
CREATE TABLE IF NOT EXISTS approval_notification_group_members (
    group_id    INTEGER NOT NULL REFERENCES approval_notification_groups(id),
    delivery_id INTEGER NOT NULL REFERENCES approval_notification_deliveries(id),
    todo_id     INTEGER NOT NULL REFERENCES approval_todos(id),
    event_id    INTEGER NOT NULL REFERENCES approval_notify_events(id),
    event_type  TEXT NOT NULL,
    joined_at   REAL NOT NULL,
    PRIMARY KEY (group_id, delivery_id)
);
CREATE INDEX IF NOT EXISTS idx_notif_group_members_todo
    ON approval_notification_group_members(todo_id);

-- 静默时段：每日重复（weekday_daily=1，start_time/end_time 为 UTC "HH:MM"，跨午夜表示
-- 从前一日 start 到当日 end）或一次性（start_at/end_at epoch）。按接收人（NULL=全体）
-- 与通道（NULL/空=邮件+webhook）匹配；窗内只保留站内待办，外发投递落 delayed，
-- 时段结束按 ordinal（原事件顺序）放行。
CREATE TABLE IF NOT EXISTS approval_quiet_schedules (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    recipient    TEXT,                        -- NULL/空=全体接收人
    channel      TEXT,                        -- NULL/空=email+webhook；否则 email|webhook
    daily        INTEGER NOT NULL DEFAULT 1,  -- 1=每日重复（UTC）；0=一次性
    start_time   TEXT,                        -- daily=1：开始 "HH:MM"（UTC）
    end_time     TEXT,                        -- daily=1：结束 "HH:MM"（UTC，可小于 start 表示跨午夜）
    start_at     REAL,                        -- daily=0：一次性开始
    end_at       REAL,                        -- daily=0：一次性结束
    active       INTEGER NOT NULL DEFAULT 1,
    created_by   TEXT NOT NULL,
    note         TEXT,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_quiet_schedules_match
    ON approval_quiet_schedules(active, recipient, channel);

-- 升级策略：按节点角色（roles JSON，'any' 匹配通配节点；change 来源的待办可配
-- role='change'）逐级配置升级接收人与触发时限（after_seconds=待办生成后 N 秒，或
-- before_deadline_seconds=截止前 N 秒）。多条策略可同时命中，每级各自触发。
CREATE TABLE IF NOT EXISTS approval_escalation_policies (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT NOT NULL,
    roles       TEXT NOT NULL DEFAULT '[]',   -- JSON 角色数组（含 'any' 时匹配通配节点；'change' 匹配变更单）
    levels_json TEXT NOT NULL,                -- [{"recipients":[...], "after_seconds":N|null,
                                              --  "before_deadline_seconds":N|null}]，级别即下标+1
    active      INTEGER NOT NULL DEFAULT 1,
    created_by  TEXT NOT NULL,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_escalation_policies_match
    ON approval_escalation_policies(active);

-- 升级记录：同一来源（节点/变更单）的同一原始接收人在每个命中策略下至多一条链
-- （UNIQUE(policy_id, source_key, base_recipient)：该接收人在节点上可能同时有
-- activated/deadline 等多个可操作待办，只升级一次，不重复提醒）。base_todo_id 锚定
-- 该接收人最早的开放待办（计时起点）。fired=已发出升级通知；stopped=原接收人已处理
-- 或来源落定，后续级别不再触发且未发出的升级投递取消；levels_fired 记录已发级别。
CREATE TABLE IF NOT EXISTS approval_escalations (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    policy_id    INTEGER NOT NULL REFERENCES approval_escalation_policies(id),
    base_todo_id INTEGER NOT NULL REFERENCES approval_todos(id),
    base_recipient TEXT NOT NULL,            -- 原始接收人（链按 来源+接收人 去重）
    source_key   TEXT NOT NULL,              -- 'node:{id}' | 'change:{id}'
    source_type  TEXT NOT NULL,
    batch_id     INTEGER,
    change_id    INTEGER,
    node_id      INTEGER,
    levels_total INTEGER NOT NULL,
    levels_fired INTEGER NOT NULL DEFAULT 0,
    status       TEXT NOT NULL DEFAULT 'pending',  -- pending|firing|stopped
    stop_reason  TEXT,                        -- handled|source_closed
    stopped_at   REAL,
    created_at   REAL NOT NULL,
    updated_at   REAL NOT NULL,
    UNIQUE (policy_id, source_key, base_recipient)
);
CREATE INDEX IF NOT EXISTS idx_escalations_status ON approval_escalations(status);
CREATE INDEX IF NOT EXISTS idx_escalations_todo ON approval_escalations(base_todo_id);

-- 已触发的升级级别：关联升级事件生成的待办，停止升级时据此取消其未发出投递。
-- （策略,原始待办,级别）唯一：同一级别的升级通知绝不发第二次。
CREATE TABLE IF NOT EXISTS approval_escalation_levels (
    id             INTEGER PRIMARY KEY AUTOINCREMENT,
    escalation_id  INTEGER NOT NULL REFERENCES approval_escalations(id),
    policy_id      INTEGER NOT NULL REFERENCES approval_escalation_policies(id),
    base_todo_id   INTEGER NOT NULL REFERENCES approval_todos(id),
    level          INTEGER NOT NULL,
    recipients     TEXT NOT NULL,             -- JSON 该级接收人快照
    notify_event_id INTEGER REFERENCES approval_notify_events(id),
    fired_at       REAL NOT NULL,
    UNIQUE (policy_id, base_todo_id, level)
);
CREATE INDEX IF NOT EXISTS idx_escalation_levels_esc
    ON approval_escalation_levels(escalation_id);

-- 通知通道路由配置版本：每次发布（含被拒绝的提交）一条。applied=配置整份生效后的
-- 版本号（单调递增）；rollback 行记录一次回滚动作（version=回滚目标版本）；rejected
-- 为校验失败的提交（config_json 保留原始内容，reason 记录失败原因）。
CREATE TABLE IF NOT EXISTS notif_route_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER,               -- applied/rollback 时的目标版本号；rejected 为 NULL
    result      TEXT NOT NULL,         -- applied|rejected|rollback
    config_json TEXT,                  -- applied/rollback：规范化配置；rejected：原始提交
    operator    TEXT NOT NULL,
    reason      TEXT,                  -- rejected/rollback 的原因
    created_at  REAL NOT NULL
);

-- 当前生效路由版本指针（单行 id=1）。发布/回滚只推进这一行，发送任务入队时在此读版本
-- 并固化快照；指针推进不影响已入队任务（它们持有自己的 route_snapshot）。
CREATE TABLE IF NOT EXISTS notif_route_current (
    id          INTEGER PRIMARY KEY CHECK (id=1),
    route_version INTEGER,             -- 当前生效版本；NULL=尚未配置（走旧链路）
    updated_by  TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    reason      TEXT
);
INSERT OR IGNORE INTO notif_route_current (id, route_version, updated_by, updated_at)
VALUES (1, NULL, 'bootstrap', 0);

-- 通道健康/熔断状态（每通道一行：email/webhook/inbox）。熔断窗口内连续失败达到阈值则
-- open（不派发新请求），冷却 cooldown 秒后转 half_open 放一条恢复探针，探针成功才 closed
-- 重新接流量；探针失败回到 open。窗口统计来自 notif_send_attempts（成功即重新计数）。
CREATE TABLE IF NOT EXISTS notif_channel_state (
    channel       TEXT PRIMARY KEY,    -- email|webhook|inbox
    state         TEXT NOT NULL DEFAULT 'closed',  -- closed|open|half_open
    enabled       INTEGER NOT NULL DEFAULT 1,      -- 运营手工停用：派发与探针一律不选
    failure_threshold INTEGER NOT NULL,            -- 窗口内连续失败多少次熔断
    window_seconds REAL NOT NULL,                  -- 失败统计时间窗口
    cooldown_seconds REAL NOT NULL,                -- open 后多久允许一条恢复探针
    opened_at     REAL,
    last_failure_at REAL,
    probe_task_id INTEGER,                          -- half_open 时占用探针的任务
    probe_at      REAL,
    updated_at    REAL NOT NULL
);

-- 路由发送任务：一个审批通知事件对一个接收人至多一条（UNIQUE(event_id,recipient)
-- 兜底重启/重复扫描/并发），它替代「每通道一条 approval_notification_deliveries 行」——
-- 任务按入队时版本快照里的有序通道计划逐个尝试，成功一条即终态 sent，绝不跨通道重复
-- 通知；全计划耗尽（无 inbox 兜底）才 quarantined，可人工 requeue 重开一轮。
CREATE TABLE IF NOT EXISTS notif_send_tasks (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    todo_id       INTEGER NOT NULL REFERENCES approval_todos(id),
    event_id      INTEGER NOT NULL REFERENCES approval_notify_events(id),
    recipient     TEXT NOT NULL,
    event_type    TEXT NOT NULL,
    source_type   TEXT NOT NULL,
    batch_id      INTEGER,
    change_id     INTEGER,
    node_id       INTEGER,
    subject       TEXT NOT NULL,
    body          TEXT NOT NULL,
    ordinal       INTEGER NOT NULL DEFAULT 0,     -- 待办创建顺序（=todo id），按序派发
    status        TEXT NOT NULL DEFAULT 'pending', -- pending|in_flight|sent|failed|quarantined|cancelled
    route_version INTEGER NOT NULL,               -- 入队时命中的路由版本（快照版本）
    route_snapshot TEXT NOT NULL,                 -- 入队时通道计划快照 JSON（版本/通道/地址/条件）
    plan_json     TEXT NOT NULL,                  -- [{channel,address,timeout_seconds,max_attempts,condition}]
    attempt_index INTEGER NOT NULL DEFAULT 0,     -- 当前/下一尝试通道在计划中的下标
    total_attempts INTEGER NOT NULL DEFAULT 0,
    current_channel TEXT,
    last_error    TEXT,
    sent_channel  TEXT,
    sent_at       REAL,
    next_retry_at REAL,
    round         INTEGER NOT NULL DEFAULT 1,     -- 发送轮次：requeue 开新一轮，窗口连续失败按轮内统计
    quarantined_at REAL,
    cancelled_reason TEXT,
    receipt_status TEXT NOT NULL DEFAULT 'not_required',
        -- not_required（未登记外部消息，如 inbox）|pending|resending|delivered|
        -- bounced|complained|expired|awaiting_confirmation|awaiting_manual|superseded
    receipt_reason TEXT,                          -- 失败/转人工原因（回执 detail 或 no_receipt_timeout）
    receipt_retries INTEGER NOT NULL DEFAULT 0,   -- 回执驱动的自动故障转移累计次数（跨消息登记行持久）
    external_message_id INTEGER REFERENCES external_messages(id),  -- 当前外部消息登记行
    -- 接收人通知额度与抑制窗口：入队时命中的规则快照（NULL=未发布额度配置或无规则命中，
    -- 领取时不受额度闸门约束）；quota_status 记录该任务在额度链路的最近一次处置。
    quota_version INTEGER,                    -- 入队时命中的额度配置版本
    quota_rule_id TEXT,                       -- 命中规则标识（'*'=缺省规则；NULL=无规则）
    quota_level TEXT,                         -- 解析出的事件级别 info|normal|critical
    quota_snapshot TEXT,                      -- 入队时规则快照 JSON（window/limit/cost/on_exceeded）
    quota_status TEXT NOT NULL DEFAULT 'none',
        -- none|admitted|delayed|downgraded|manual|ignored|consumed|released
    quota_reason TEXT,                        -- 超额处置/延迟/回收原因
    quota_generation INTEGER NOT NULL DEFAULT 1,  -- 预占代：人工 requeue/重试 +1（重新预占），
                                                  -- 失败重试/回执驱动通道切换不变（复用预占）
    -- 内容模板固化：入队时按接收人语言渲染，之后模板编辑/发布不影响本任务。
    -- template_*_id 为 NULL 表示该键未配置模板（沿用事件静态正文，行为与无模板时一致）。
    -- content_snapshot 存每通道最终采用的 {版本/语言/语言回退链/标题/正文/webhook负载/哈希}。
    template_snapshot TEXT,                -- 解析到的模板定义快照（版本/变量声明/可用语言/回退）
    content_snapshot   TEXT,               -- 每通道渲染结果快照 JSON
    content_sha256     TEXT,               -- 实际发送通道的正文指纹（发送后回填，轨迹锚点）
    render_status      TEXT NOT NULL DEFAULT 'not_templated',
        -- not_templated（未配置模板）|rendered|failed（渲染失败，不派发）
    render_failure_reason TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL,
    UNIQUE (event_id, recipient)
);
CREATE INDEX IF NOT EXISTS idx_notif_send_pick
    ON notif_send_tasks(status, next_retry_at);
CREATE INDEX IF NOT EXISTS idx_notif_send_todo ON notif_send_tasks(todo_id);
CREATE INDEX IF NOT EXISTS idx_notif_send_recipient ON notif_send_tasks(recipient, status);
CREATE INDEX IF NOT EXISTS idx_notif_send_quota
    ON notif_send_tasks(quota_version, quota_rule_id, recipient);
CREATE INDEX IF NOT EXISTS idx_notif_send_quota_status
    ON notif_send_tasks(quota_status);

-- 每次通道尝试一行（含恢复探针）：result=success|failure|timeout，duration 秒。
-- 熔断窗口统计「该通道最近 window_seconds 内、自上次成功以来的连续失败数」即查本表。
CREATE TABLE IF NOT EXISTS notif_send_attempts (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES notif_send_tasks(id),
    channel     TEXT NOT NULL,
    attempt_index INTEGER NOT NULL,
    round       INTEGER NOT NULL,
    probe       INTEGER NOT NULL DEFAULT 0,   -- 1=熔断恢复探针（half_open 唯一一条）
    result      TEXT NOT NULL,               -- success|failure|timeout
    duration    REAL,
    error       TEXT,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_attempts_task ON notif_send_attempts(task_id, id);
CREATE INDEX IF NOT EXISTS idx_notif_attempts_channel
    ON notif_send_attempts(channel, created_at);

-- 通道切换历史：首选选中、每次切换（原因 timeout/consecutive_failures/breaker_open/
-- channel_disabled/no_address）、最终成功或计划全部耗尽。管理员查询切换轨迹用。
CREATE TABLE IF NOT EXISTS notif_channel_switches (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES notif_send_tasks(id),
    event_id    INTEGER NOT NULL,
    recipient   TEXT NOT NULL,
    from_channel TEXT,
    to_channel  TEXT,
    reason      TEXT NOT NULL,               -- selected|timeout|consecutive_failures|breaker_open|channel_disabled|no_address|plan_exhausted
    detail      TEXT NOT NULL DEFAULT '{}',
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_switches_task ON notif_channel_switches(task_id, id);
CREATE INDEX IF NOT EXISTS idx_notif_switches_channel
    ON notif_channel_switches(to_channel, id);

-- 外部通道消息登记：email/webhook 每次发送成功后登记外部服务返回的 message_id。
-- 它是回执匹配与送达确认的锚点：回执入口按 (channel, message_id) 找到本行。同一发送
-- 任务因本通道重试或故障转移到新通道时会登记多行，旧行被同事务置 superseded（轨迹保留），
-- 只有 status != 'superseded' 的行接受终态回执。
CREATE TABLE IF NOT EXISTS external_messages (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    channel       TEXT NOT NULL,              -- email | webhook
    message_id    TEXT NOT NULL,              -- 外部服务返回的消息编号；本地占位为 local:...
    id_source     TEXT NOT NULL DEFAULT 'provider',  -- provider=外部返回；local=无返回时本地生成
    source        TEXT NOT NULL,               -- route | legacy（路由发送任务 / 旧链路投递）
    send_task_id  INTEGER REFERENCES notif_send_tasks(id),
    delivery_id   INTEGER REFERENCES approval_notification_deliveries(id),
    attempt_id    INTEGER REFERENCES notif_send_attempts(id),
    recipient     TEXT NOT NULL,
    address       TEXT,                        -- 实际发送地址（邮箱 / webhook URL）
    event_id      INTEGER,                     -- 审批通知事件 id（便于按事件查询）
    event_type    TEXT,
    status        TEXT NOT NULL DEFAULT 'pending',
        -- pending|resending|delivered|bounced|complained|expired|
        -- awaiting_confirmation|superseded（转人工 awaiting_manual 落在发送任务/投递行）
    confirm_deadline REAL,                    -- 待确认截止（登记时刻 + 策略超时）；NULL=不跟踪
    confirm_retries INTEGER NOT NULL DEFAULT 0,  -- 已自动故障转移/重试次数
    active_receipt_id INTEGER REFERENCES receipts(id),  -- 驱动当前终态的回执（首条终态）
    registered_at REAL NOT NULL,
    updated_at    REAL NOT NULL,
    UNIQUE (channel, message_id)               -- 同一通道同一外部消息编号只登记一次
);
CREATE INDEX IF NOT EXISTS idx_external_messages_task ON external_messages(send_task_id);
CREATE INDEX IF NOT EXISTS idx_external_messages_delivery ON external_messages(delivery_id);
CREATE INDEX IF NOT EXISTS idx_external_messages_match
    ON external_messages(channel, message_id, status);
CREATE INDEX IF NOT EXISTS idx_external_messages_scan
    ON external_messages(status, confirm_deadline);
CREATE INDEX IF NOT EXISTS idx_external_messages_recipient
    ON external_messages(recipient, status);

-- 外部回执原文：只增不删、绝不改写。同一 message_id 的重复回执由
-- UNIQUE(channel,message_id,event,receipt_hash) 幂等（不同内容的迟到/乱序回执仍各自留盘）。
-- matched=unmatched 时进入待核对队列；人工绑定到消息后 matched=bound（正文不变）。
CREATE TABLE IF NOT EXISTS receipts (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    channel       TEXT NOT NULL,              -- email | webhook
    message_id    TEXT NOT NULL,
    event         TEXT NOT NULL,              -- 规范化结果：delivered|bounced|complained|expired|unknown
    recipient     TEXT,
    receipt_hash  TEXT NOT NULL,              -- 原始报文 SHA-256（同内容重复投递判定）
    raw_body      TEXT NOT NULL,              -- 原始报文（按 UTF-8 保留；人工核对/重放依据）
    content_type  TEXT,
    signature_kid TEXT,                       -- 验签所用密钥 kid
    provider_ts   REAL,                       -- 回执自带的事件时间（若有）
    matched       TEXT NOT NULL DEFAULT 'unmatched',  -- unmatched|applied|bound|ignored
    message_pk    INTEGER REFERENCES external_messages(id),  -- 匹配/绑定到的消息登记行
    send_task_id  INTEGER,                    -- 匹配时冗余（查询/审计用）
    bound_by      TEXT,                       -- 人工绑定操作者
    bound_at      REAL,
    bind_note     TEXT,
    terminal_rank INTEGER NOT NULL DEFAULT 0, -- delivered=1 / bounced,complained,expired=2 / unknown=0
    duplicate_of  INTEGER REFERENCES receipts(id),  -- 重复投递指向首条回执
    received_at   REAL NOT NULL,
    created_at    REAL NOT NULL,
    UNIQUE (channel, message_id, event, receipt_hash)
);
CREATE INDEX IF NOT EXISTS idx_receipts_match ON receipts(matched, message_pk);
CREATE INDEX IF NOT EXISTS idx_receipts_message ON receipts(channel, message_id);
CREATE INDEX IF NOT EXISTS idx_receipts_task ON receipts(send_task_id);
CREATE INDEX IF NOT EXISTS idx_receipts_recipient ON receipts(recipient);

-- 状态变化历史：消息登记、回执确认、乱序/终态冲突、自动重试/转人工、人工绑定/忽略、
-- 安全重放都只增一行。kind=message 记 external_messages 的状态转移；kind=receipt 记
-- 回执的匹配/绑定处置。
CREATE TABLE IF NOT EXISTS receipt_status_history (
    id           INTEGER PRIMARY KEY AUTOINCREMENT,
    kind         TEXT NOT NULL,               -- message | receipt
    message_pk   INTEGER REFERENCES external_messages(id),
    receipt_id   INTEGER REFERENCES receipts(id),
    send_task_id INTEGER,
    channel      TEXT NOT NULL,
    recipient    TEXT,
    from_status  TEXT,
    to_status    TEXT NOT NULL,
    reason       TEXT,
    detail       TEXT NOT NULL DEFAULT '{}',
    operator     TEXT,
    created_at   REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_receipt_history_message
    ON receipt_status_history(message_pk, id);
CREATE INDEX IF NOT EXISTS idx_receipt_history_receipt
    ON receipt_status_history(receipt_id, id);
CREATE INDEX IF NOT EXISTS idx_receipt_history_query
    ON receipt_status_history(channel, recipient, to_status, id);

-- 外部回执验签密钥（与回调入口密钥环相互独立）。每通道至多一行 active；轮换时旧行置
-- retired 并给 grace_until（过渡期内仍可验签），超期一律拒绝。
CREATE TABLE IF NOT EXISTS receipt_keys (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    channel     TEXT NOT NULL,                -- email | webhook
    kid         TEXT NOT NULL,
    secret      TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active',  -- active | retired
    grace_until REAL,                         -- retired 过渡期截止（epoch 秒）
    created_by  TEXT NOT NULL,
    created_at  REAL NOT NULL,
    retired_at  REAL,
    retired_by  TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_receipt_keys_active
    ON receipt_keys(channel) WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_receipt_keys_channel ON receipt_keys(channel, kid);

-- 送达确认策略（单例行 id=1）：发送登记后 confirm_timeout_seconds 内没有终态回执则标记
-- awaiting_confirmation；随后自动沿任务的通道计划故障转移（confirm_retries 计数），
-- 达到 confirm_max_retries 或失败回执策略 action=manual 时转 awaiting_manual。
CREATE TABLE IF NOT EXISTS receipt_policy (
    id          INTEGER PRIMARY KEY CHECK (id=1),
    confirm_timeout_seconds REAL NOT NULL,
    confirm_max_retries INTEGER NOT NULL,
    on_bounced  TEXT NOT NULL DEFAULT 'retry',  -- retry | manual
    on_complained TEXT NOT NULL DEFAULT 'manual',
    on_expired  TEXT NOT NULL DEFAULT 'retry',
    updated_by  TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    reason      TEXT
);
INSERT OR IGNORE INTO receipt_policy
    (id, confirm_timeout_seconds, confirm_max_retries, on_bounced,
     on_complained, on_expired, updated_by, updated_at, reason)
VALUES (1, 3600, 2, 'retry', 'manual', 'retry', 'bootstrap', 0, 'default');

-- 接收人通知额度与抑制窗口的版本化配置：每次发布（含被拒绝的提交）一条。
-- applied=整份配置生效后的版本号（单调递增）；rollback 行记录一次回滚动作
-- （version=回滚目标版本）；rejected 为校验失败的提交（config_json 保留原始内容）。
CREATE TABLE IF NOT EXISTS notif_quota_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    version     INTEGER,               -- applied/rollback 时的目标版本号；rejected 为 NULL
    result      TEXT NOT NULL,         -- applied|rejected|rollback
    config_json TEXT,                  -- applied/rollback：规范化配置；rejected：原始提交
    operator    TEXT NOT NULL,
    reason      TEXT,                  -- rejected/rollback 的原因
    created_at  REAL NOT NULL
);

-- 当前生效额度配置指针（单行 id=1）。发布/回滚只推进这一行；发送任务固化入队时命中的
-- 规则快照（notif_send_tasks.quota_snapshot），配置更新不改变已入队任务的占用规则。
CREATE TABLE IF NOT EXISTS notif_quota_current (
    id           INTEGER PRIMARY KEY CHECK (id=1),
    quota_version INTEGER,            -- 当前生效版本；NULL=尚未配置（额度闸门不生效）
    updated_by   TEXT NOT NULL,
    updated_at   REAL NOT NULL,
    reason       TEXT
);
INSERT OR IGNORE INTO notif_quota_current (id, quota_version, updated_by, updated_at)
VALUES (1, NULL, 'bootstrap', 0);

-- 额度预占：发送任务领取（pending/failed -> in_flight）前在同一写事务内原子预占。
-- 桶（bucket）= 规则版本 + 规则标识 + 接收人 + 窗口起点；桶内 state='reserved' 的
-- normal 预占 cost 之和即当前占用，达到 limit 即超额。downgraded 预占不计入桶消耗
-- （降级为站内通知不占外发额度），但留行可查。窗口到期后旧桶行不再被任何查询计入，
-- 等同释放可用额度（无需后台清理）。
-- generation=notif_send_tasks.quota_generation：同一事件的失败重试、回执驱动的通道
-- 切换 generation 不变，复用本预占，绝不重复占用；人工 requeue/重试开新一轮时
-- generation+1 并回收旧预占。
CREATE TABLE IF NOT EXISTS notif_quota_reservations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    task_id     INTEGER NOT NULL REFERENCES notif_send_tasks(id),
    event_id    INTEGER NOT NULL,
    recipient   TEXT NOT NULL,
    quota_version INTEGER NOT NULL,    -- 命中规则所属配置版本
    rule_id     TEXT NOT NULL,         -- 规则标识（规则配置内唯一；NULL 规则用 '*'）
    level       TEXT NOT NULL,         -- info|normal|critical（命中时的事件级别）
    bucket_start REAL NOT NULL,        -- 窗口起点（epoch 秒）
    window_seconds REAL NOT NULL,
    cost        INTEGER NOT NULL,      -- 本事件占用额度（正整数）
    kind        TEXT NOT NULL DEFAULT 'normal',  -- normal|downgraded
    state       TEXT NOT NULL DEFAULT 'reserved', -- reserved|consumed|released
    generation  INTEGER NOT NULL DEFAULT 1,       -- = 发送任务的 quota_generation
    reason      TEXT,                  -- downgraded/released 的原因（超额动作/取消原因等）
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
-- 同一任务同一代至多一条活跃预占（重复扫描/重试/重启恢复不重复占用）
CREATE UNIQUE INDEX IF NOT EXISTS idx_quota_reserv_active
    ON notif_quota_reservations(task_id, generation) WHERE state='reserved';
CREATE INDEX IF NOT EXISTS idx_quota_reserv_bucket
    ON notif_quota_reservations(quota_version, rule_id, recipient,
                                bucket_start, state);
CREATE INDEX IF NOT EXISTS idx_quota_reserv_task ON notif_quota_reservations(task_id, id);
CREATE INDEX IF NOT EXISTS idx_quota_reserv_window
    ON notif_quota_reservations(recipient, bucket_start);

-- 通知状态对账任务：一次运营发起的只读扫描。扫描条件与检测时的路由/额度/回执策略版本
-- 一并固化；任务本身可分页推进、暂停后继续、失败重试，服务重启后从游标恢复。
CREATE TABLE IF NOT EXISTS notif_reconciliation_jobs (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    operator      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'queued', -- queued|scanning|paused|completed|failed
    filters_json  TEXT NOT NULL DEFAULT '{}',
    route_version INTEGER,
    quota_version INTEGER,
    receipt_policy_json TEXT NOT NULL DEFAULT '{}',
    cursor_task_id INTEGER NOT NULL DEFAULT 0,
    cursor_message_id INTEGER NOT NULL DEFAULT 0,
    cursor_receipt_id INTEGER NOT NULL DEFAULT 0,
    cursor_reservation_id INTEGER NOT NULL DEFAULT 0,
    phase         TEXT NOT NULL DEFAULT 'tasks',    -- tasks|messages|receipts|reservations|done
    scanned_count INTEGER NOT NULL DEFAULT 0,
    findings_count INTEGER NOT NULL DEFAULT 0,
    page_size     INTEGER NOT NULL DEFAULT 100,
    last_error    TEXT,
    paused_by     TEXT,
    paused_at     REAL,
    resumed_count INTEGER NOT NULL DEFAULT 0,
    attempts      INTEGER NOT NULL DEFAULT 0,
    next_retry_at REAL,
    started_at    REAL,
    completed_at  REAL,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_recon_jobs_status
    ON notif_reconciliation_jobs(status, updated_at);

-- 对账异常：snapshot_json 是检测瞬间各链路实体的不可变快照；reason 是稳定异常代码。
-- anomaly_key 在同一 job 内去重，规则版本变化后重新对账会产生新 job/新 finding，绝不回改。
CREATE TABLE IF NOT EXISTS notif_reconciliation_findings (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL REFERENCES notif_reconciliation_jobs(id),
    anomaly_key   TEXT NOT NULL,
    entity_type   TEXT NOT NULL,                  -- task|receipt|reservation|message
    entity_id     INTEGER NOT NULL,
    severity      TEXT NOT NULL DEFAULT 'warning', -- info|warning|critical
    reason        TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'open',    -- open|compensating|resolved|ignored|failed
    recipient     TEXT,
    event_id      INTEGER,
    event_type    TEXT,
    send_task_id  INTEGER,
    receipt_id    INTEGER,
    reservation_id INTEGER,
    message_pk    INTEGER,
    snapshot_json TEXT NOT NULL,
    evidence_json TEXT NOT NULL DEFAULT '{}',
    suggested_actions_json TEXT NOT NULL DEFAULT '[]',
    detected_at   REAL NOT NULL,
    updated_at    REAL NOT NULL,
    UNIQUE (job_id, anomaly_key)
);
CREATE INDEX IF NOT EXISTS idx_notif_recon_findings_job
    ON notif_reconciliation_findings(job_id, id);
CREATE INDEX IF NOT EXISTS idx_notif_recon_findings_query
    ON notif_reconciliation_findings(recipient, event_type, status, reason);
CREATE INDEX IF NOT EXISTS idx_notif_recon_findings_entity
    ON notif_reconciliation_findings(entity_type, entity_id);

-- 人工补偿动作：每次选择都记录依据（finding 快照）、操作者、动作前后状态和结果。
-- UNIQUE(finding_id,action) 语义由应用在写事务中检查成功动作保证幂等；失败尝试允许保留多行。
CREATE TABLE IF NOT EXISTS notif_reconciliation_compensations (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id        INTEGER NOT NULL REFERENCES notif_reconciliation_jobs(id),
    finding_id    INTEGER NOT NULL REFERENCES notif_reconciliation_findings(id),
    action        TEXT NOT NULL,                  -- relink_receipt|release_reservation|close_task|create_send_plan
    operator      TEXT NOT NULL,
    status        TEXT NOT NULL DEFAULT 'applied', -- applied|noop|failed|scheduled
    target_type   TEXT NOT NULL,                  -- receipt|reservation|task|send_plan
    target_id     INTEGER,
    request_json  TEXT NOT NULL DEFAULT '{}',
    before_json   TEXT NOT NULL DEFAULT '{}',
    after_json    TEXT NOT NULL DEFAULT '{}',
    basis_snapshot_json TEXT NOT NULL,
    reason        TEXT,
    error         TEXT,
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_recon_comp_job
    ON notif_reconciliation_compensations(job_id, id);
CREATE INDEX IF NOT EXISTS idx_notif_recon_comp_target
    ON notif_reconciliation_compensations(target_type, target_id);

-- 补偿发送计划：只调度，不直接外发；由既有 notif_send_tasks 的单赢家领取、额度预占和
-- 通道尝试幂等保护执行。plan_key 保证同一异常/任务的计划重复提交不会产生第二次意图。
CREATE TABLE IF NOT EXISTS notif_compensation_send_plans (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    finding_id    INTEGER NOT NULL REFERENCES notif_reconciliation_findings(id),
    job_id        INTEGER NOT NULL REFERENCES notif_reconciliation_jobs(id),
    task_id       INTEGER NOT NULL REFERENCES notif_send_tasks(id),
    plan_key      TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL DEFAULT 'scheduled', -- scheduled|applied|superseded|cancelled
    operator      TEXT NOT NULL,
    note          TEXT,
    scheduled_at  REAL NOT NULL,
    applied_at    REAL,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_notif_comp_plan_status
    ON notif_compensation_send_plans(status, scheduled_at);
CREATE INDEX IF NOT EXISTS idx_notif_comp_plan_task
    ON notif_compensation_send_plans(task_id, status);

-- 通知内容模板版本（按「事件类型 × 通道」编排；'*' 表示通配事件类型/通配通道）。
-- 生命周期：draft（每个 event_type+channel 至多一份草稿）-> published（不可变，version
-- 在该键内单调递增）/ rejected（发布校验失败留痕，原始提交与原因保留，不推进当前指针）。
-- subjects_json/bodies_json 为 {语言: 文本}；variables_json 为变量声明
-- （必填/默认值/敏感/类型）；fallback_languages_json 为该模板声明的语言回退顺序；
-- content_sha256 为发布时规范化内容的指纹（重复发布识别）。
CREATE TABLE IF NOT EXISTS notif_template_versions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    event_type  TEXT NOT NULL,             -- 审批通知事件类型；'*'=通配
    channel     TEXT NOT NULL,             -- email|webhook|inbox；'*'=通配通道
    version     INTEGER,                   -- published 的版本号（键内单调递增）；draft/rejected 为 NULL
    status      TEXT NOT NULL,             -- draft|published|rejected
    variables_json TEXT NOT NULL DEFAULT '[]',
    subjects_json TEXT NOT NULL,           -- {lang: subject}；inbox 用作标题
    bodies_json TEXT NOT NULL,             -- {lang: body}
    languages_json TEXT NOT NULL,          -- 模板提供的可用语言列表 JSON
    fallback_languages_json TEXT NOT NULL DEFAULT '[]',
    content_sha256 TEXT,                   -- 规范化定义的指纹（发布时计算）
    operator    TEXT NOT NULL,
    note        TEXT,
    rejection_reason TEXT,                 -- rejected 的校验失败原因（JSON）
    created_at  REAL NOT NULL,
    published_at REAL
);
-- 每个「事件类型×通道」至多一份草稿（并发创建/编辑由部分唯一索引兜底）
CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_template_draft
    ON notif_template_versions(event_type, channel) WHERE status='draft';
CREATE INDEX IF NOT EXISTS idx_notif_template_versions_key
    ON notif_template_versions(event_type, channel, status, version);

-- 当前生效模板指针：每个「事件类型×通道」一行，指向最近 published 版本。
-- 发送/入队时在此解析（通配 '*' 按 event_type+channel -> event_type+* ->
-- *+channel -> *+* 顺序回退）；指针推进不影响已入队任务（它们持有自己的正文快照）。
CREATE TABLE IF NOT EXISTS notif_template_current (
    event_type  TEXT NOT NULL,
    channel     TEXT NOT NULL,
    template_version_id INTEGER NOT NULL REFERENCES notif_template_versions(id),
    version     INTEGER NOT NULL,
    updated_by  TEXT NOT NULL,
    updated_at  REAL NOT NULL,
    reason      TEXT,
    PRIMARY KEY (event_type, channel)
);

-- 模板渲染失败记录（可查询的失败原因）：变量缺失/类型不符/无语言版本/正文超限/
-- 敏感变量未脱敏。阻塞期间不产生任何外发效果；人工修复模板或变量后可重试渲染，
-- resolved_at 非空表示已解除（解除时保留记录与原因，不删除）。
CREATE TABLE IF NOT EXISTS notif_template_render_failures (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    entity_type TEXT NOT NULL,             -- task（版本化路由发送任务）| delivery（旧链路投递）
    send_task_id INTEGER REFERENCES notif_send_tasks(id),
    delivery_id INTEGER REFERENCES approval_notification_deliveries(id),
    todo_id     INTEGER,
    event_id    INTEGER NOT NULL,
    recipient   TEXT NOT NULL,
    channel     TEXT NOT NULL,
    event_type  TEXT NOT NULL,
    template_version_id INTEGER REFERENCES notif_template_versions(id),
    language    TEXT,                      -- 实际采用语言；无语言版本时为 NULL
    language_chain_json TEXT NOT NULL DEFAULT '[]',  -- 解析时尝试的语言回退链
    reason_code TEXT NOT NULL,             -- missing_template|missing_language|missing_variable|
                                           -- type_mismatch|body_too_long|subject_too_long|
                                           -- sensitive_unmasked|render_error
    reason_detail TEXT NOT NULL,
    variables_snapshot_json TEXT NOT NULL DEFAULT '{}',  -- 解析时变量（敏感值脱敏）快照
    resolved_by TEXT,
    resolved_at REAL,
    resolve_note TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
-- 同一发送任务同一通道、同一旧链路投递，阻塞期间至多一条未解除失败（重复渲染不产生第二份）
CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_render_fail_task_open
    ON notif_template_render_failures(send_task_id, channel)
    WHERE resolved_at IS NULL AND send_task_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_notif_render_fail_delivery_open
    ON notif_template_render_failures(delivery_id)
    WHERE resolved_at IS NULL AND delivery_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_notif_render_fail_query
    ON notif_template_render_failures(resolved_at, reason_code, recipient, event_type, channel);
CREATE INDEX IF NOT EXISTS idx_notif_render_fail_task
    ON notif_template_render_failures(send_task_id);

-- ============================================================================
-- 回执异常复核工作流（receipt exception review）
-- ============================================================================

-- 复核管理员名册（与 operator 同一命名空间）。role=reviewer 可认领/转派/备注/挂起，
-- role=senior 额外可执行重试/抑制/重关联/标记送达/关闭；无活动名册行的操作者对
-- 复核链路 fail-closed（读写均拒绝，除显式标注的公开引导外）。
CREATE TABLE IF NOT EXISTS receipt_review_admins (
    name        TEXT PRIMARY KEY,
    role        TEXT NOT NULL,              -- reviewer | senior
    active      INTEGER NOT NULL DEFAULT 1,
    created_by  TEXT NOT NULL,
    note        TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL,
    deactivated_at REAL
);

-- 复核案件：bounced/complained/expired/无法匹配的回执与确认超时自动建案，也可手工建案。
-- case_key 去重单元（receipt:{id} 一回执最多一案；task-timeout:{task_id}:{message_pk}
-- 同一超时消息一案）；同一 (subject,subject_id) 的多个回执证据可追加进同一案件。
-- 案件行的关联列在证据追加时冗余补全（NULL 容忍），已落盘字段绝不被后续回执改写。
CREATE TABLE IF NOT EXISTS receipt_review_cases (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    case_key      TEXT NOT NULL UNIQUE,
    status        TEXT NOT NULL DEFAULT 'open',   -- open|in_progress|on_hold|resolved|closed
    subject       TEXT NOT NULL,                  -- receipt | task
    receipt_id    INTEGER REFERENCES receipts(id),
    task_id       INTEGER REFERENCES notif_send_tasks(id),
    delivery_id   INTEGER,                        -- 旧链路 approval_notification_deliveries.id
    message_pk    INTEGER REFERENCES external_messages(id),
    recipient     TEXT,
    channel       TEXT,
    message_id    TEXT,                           -- 外部消息编号（查询锚点）
    event_type    TEXT,                           -- bounced|complained|expired|unmatched|confirmation_timeout
    source        TEXT NOT NULL DEFAULT 'auto',   -- auto | manual
    priority      TEXT NOT NULL DEFAULT 'normal', -- low|normal|high
    owner         TEXT,                           -- 当前认领人（NULL=待认领）
    owned_at      REAL,
    assigned_by   TEXT,
    note          TEXT,                           -- 最近处理意见（完整意见在 decisions 追加留痕）
    sla_deadline  REAL,                           -- 当前 SLA 截止（建案/升级时顺延）
    escalation_level INTEGER NOT NULL DEFAULT 0,
    last_escalated_at REAL,
    resolution    TEXT,                           -- 最终处置动作：retried|suppressed|relinked|
                                                  -- marked_delivered|closed_*
    resolved_by   TEXT,
    resolved_at   REAL,
    closed_by     TEXT,
    closed_at     REAL,
    close_reason  TEXT,
    created_at    REAL NOT NULL,
    updated_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_review_cases_status ON receipt_review_cases(status, sla_deadline);
CREATE INDEX IF NOT EXISTS idx_review_cases_owner ON receipt_review_cases(owner, status);
CREATE INDEX IF NOT EXISTS idx_review_cases_query
    ON receipt_review_cases(recipient, event_type, status, created_at);
CREATE INDEX IF NOT EXISTS idx_review_cases_receipt ON receipt_review_cases(receipt_id);
CREATE INDEX IF NOT EXISTS idx_review_cases_task ON receipt_review_cases(task_id);
CREATE INDEX IF NOT EXISTS idx_review_cases_message ON receipt_review_cases(message_pk);

-- 不可变证据快照：建案时按接收人/事件/原始消息固化，之后绝不更新（新事实=追加新行）。
-- kind=receipt 回执原文行+哈希；kind=message 外部消息登记；kind=task 发送任务；
-- kind=history 状态历史；kind=manual 管理员手工补充（可带附件）。
CREATE TABLE IF NOT EXISTS receipt_review_evidence (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id     INTEGER NOT NULL REFERENCES receipt_review_cases(id),
    kind        TEXT NOT NULL,                  -- receipt|message|task|history|delivery|manual
    ref_type    TEXT,                           -- receipts|external_messages|notif_send_tasks|...
    ref_id      INTEGER,
    title       TEXT,
    content_sha256 TEXT,                        -- 快照内容哈希（清理后仍可校验摘要）
    snapshot_json TEXT NOT NULL,
    added_by    TEXT NOT NULL,                  -- 'system' 或管理员
    attachment_id INTEGER,
    created_at  REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_review_evidence_case ON receipt_review_evidence(case_id, id);
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_evidence_ref
    ON receipt_review_evidence(case_id, kind, ref_type, ref_id)
    WHERE ref_id IS NOT NULL;

-- 案件决定（只增）：认领/转派/备注/挂起/恢复/重试/抑制/重关联/标记送达/解决/关闭全部落一行，
-- 每个决定带原因（effect 动作必填），并回写原通知状态。外部效果动作每案件每类至多一条
-- （部分唯一索引：重复操作返回原决定，绝不产生第二次外部效果，也不覆盖已有决定）。
CREATE TABLE IF NOT EXISTS receipt_review_decisions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id     INTEGER NOT NULL REFERENCES receipt_review_cases(id),
    request_id  TEXT,                            -- 提交幂等键（全局唯一，重复提交返回原决定）
    action      TEXT NOT NULL,                   -- claim|assign|note|suspend|resume|
                                                 -- retry_send|suppress|relink_task|
                                                 -- mark_delivered|resolve|reopen|close
    operator    TEXT NOT NULL,
    reason      TEXT,
    note        TEXT,
    status      TEXT NOT NULL DEFAULT 'applied', -- applied|noop|failed
    effect      TEXT,                            -- none|send_scheduled|suppressed|relinked|marked_delivered
    target_type TEXT,                            -- task|receipt|message|suppression|case
    target_id   INTEGER,
    from_status TEXT,
    to_status   TEXT,
    detail_json TEXT NOT NULL DEFAULT '{}',      -- 前后状态/依据/错误等
    created_at  REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_decisions_request
    ON receipt_review_decisions(request_id) WHERE request_id IS NOT NULL;
-- 每案件每个外部效果动作至多一条已应用决定（重试/抑制/重关联/标记送达不可对同案重复生效）
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_decisions_effect_once
    ON receipt_review_decisions(case_id, action)
    WHERE action IN ('retry_send','suppress','relink_task','mark_delivered')
      AND status='applied';
CREATE INDEX IF NOT EXISTS idx_review_decisions_case
    ON receipt_review_decisions(case_id, id);

-- SLA 升级记录：每次到点升级落一行（同案件同级别唯一，重启/重复扫描不重复升级）。
CREATE TABLE IF NOT EXISTS receipt_review_escalations (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id     INTEGER NOT NULL REFERENCES receipt_review_cases(id),
    level       INTEGER NOT NULL,
    reason      TEXT NOT NULL,                  -- sla_due
    from_owner  TEXT,
    notified_roles TEXT NOT NULL DEFAULT '[]',  -- 升级通知角色快照（站内待办，无外发效果）
    old_deadline REAL,
    new_deadline REAL,
    created_at  REAL NOT NULL,
    UNIQUE (case_id, level)
);
CREATE INDEX IF NOT EXISTS idx_review_escalations_case
    ON receipt_review_escalations(case_id, id);

-- 后续发送抑制名单：案件决定 suppress 时写入；领取（admit）/重试调度/计划重开时校验，
-- 命中即取消待发送任务（已发出的外部效果轨迹保留）。可按接收人（event_type NULL）
-- 或接收人×事件类型抑制；解除由终态案件关闭时自动放行，亦可手工解除（单独决定留痕）。
CREATE TABLE IF NOT EXISTS receipt_review_suppressions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id     INTEGER NOT NULL REFERENCES receipt_review_cases(id),
    recipient   TEXT NOT NULL,
    event_type  TEXT,                           -- NULL=该接收人的后续通知一律抑制
    reason      TEXT NOT NULL,
    created_by  TEXT NOT NULL,
    status      TEXT NOT NULL DEFAULT 'active', -- active | released
    released_at REAL,
    released_by TEXT,
    release_reason TEXT,
    created_at  REAL NOT NULL,
    updated_at  REAL NOT NULL
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_review_suppression_active
    ON receipt_review_suppressions(recipient, COALESCE(event_type, '*'))
    WHERE status='active';
CREATE INDEX IF NOT EXISTS idx_review_suppression_query
    ON receipt_review_suppressions(recipient, event_type, status);

-- 案件临时附件（管理员补充的二进制证据）：内容哈希随证据快照固化；案件关闭后可安全
-- 清理 blob（置 purged），证据行的摘要/哈希/决定/审计全部保留，历史不被改写。
CREATE TABLE IF NOT EXISTS receipt_review_attachments (
    id            INTEGER PRIMARY KEY AUTOINCREMENT,
    case_id       INTEGER NOT NULL REFERENCES receipt_review_cases(id),
    filename      TEXT NOT NULL,
    content_type  TEXT,
    size_bytes    INTEGER NOT NULL,
    content_sha256 TEXT NOT NULL,
    blob          TEXT,                         -- base64 原文；清理后置 NULL
    status        TEXT NOT NULL DEFAULT 'stored', -- stored | purged
    uploaded_by   TEXT NOT NULL,
    purged_at     REAL,
    purged_by     TEXT,
    created_at    REAL NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_review_attachments_case
    ON receipt_review_attachments(case_id, id);

-- 复核 SLA 策略（单例行 id=1）：建案后多少秒首限；每次升级顺延多少秒；最多升级级别
-- （0=关闭自动升级）；达到最高级后案件置 high 优先级等待 senior 处置。
CREATE TABLE IF NOT EXISTS receipt_review_policy (
    id            INTEGER PRIMARY KEY CHECK (id=1),
    sla_seconds   REAL NOT NULL,
    escalation_seconds REAL NOT NULL,
    max_escalation_level INTEGER NOT NULL,
    updated_by    TEXT NOT NULL,
    updated_at    REAL NOT NULL,
    reason        TEXT
);
INSERT OR IGNORE INTO receipt_review_policy
    (id, sla_seconds, escalation_seconds, max_escalation_level,
     updated_by, updated_at, reason)
VALUES (1, 14400, 7200, 2, 'bootstrap', 0, 'default');
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
        # 通知聚合/静默/升级：老库补列（新库由 SCHEMA 直接建出完整结构，自动跳过）
        if "approval_notification_deliveries" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(approval_notification_deliveries)")}
            if "group_id" not in cols:
                self._conn.execute(
                    "ALTER TABLE approval_notification_deliveries ADD COLUMN group_id INTEGER")
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notif_deliveries_group "
                    "ON approval_notification_deliveries(group_id)")
            if "delayed_until" not in cols:
                self._conn.execute(
                    "ALTER TABLE approval_notification_deliveries "
                    "ADD COLUMN delayed_until REAL")
                self._conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_notif_deliveries_delayed "
                    "ON approval_notification_deliveries(status, delayed_until)")
            if "ordinal" not in cols:
                self._conn.execute(
                    "ALTER TABLE approval_notification_deliveries "
                    "ADD COLUMN ordinal INTEGER NOT NULL DEFAULT 0")
                # 存量投递按投递 id 恢复事件顺序（与待办创建顺序一致）
                self._conn.execute(
                    "UPDATE approval_notification_deliveries SET ordinal=id WHERE ordinal=0")
        if "approval_todos" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(approval_todos)")}
            if "open_at" not in cols:
                self._conn.execute(
                    "ALTER TABLE approval_todos ADD COLUMN open_at REAL NOT NULL DEFAULT 0")
                # 存量待办的升级计时起点取创建时间
                self._conn.execute(
                    "UPDATE approval_todos SET open_at=created_at WHERE open_at=0")
        # 外部通道回执：路由发送任务与旧链路投递补「回执状态/原因/当前外部消息行」。
        # 存量任务一律视为 not_required（升级不改变其行为；之后新发送才登记 message_id）。
        if "notif_send_tasks" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(notif_send_tasks)")}
            for name, ddl in (
                ("receipt_status",
                 "TEXT NOT NULL DEFAULT 'not_required'"),
                ("receipt_reason", "TEXT"),
                ("receipt_retries",
                 "INTEGER NOT NULL DEFAULT 0"),
                # 迁移期 external_messages 尚未建表，不加 REFERENCES（新库由 SCHEMA 建）
                ("external_message_id", "INTEGER"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE notif_send_tasks ADD COLUMN {name} {ddl}")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_notif_send_receipt "
                "ON notif_send_tasks(receipt_status)")
        if "approval_notification_deliveries" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(approval_notification_deliveries)")}
            for name, ddl in (
                ("receipt_status", "TEXT"),
                ("external_message_id", "INTEGER"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE approval_notification_deliveries "
                        f"ADD COLUMN {name} {ddl}")
        # 接收人通知额度与抑制窗口：老库发送任务补额度快照/预占代列（存量任务视为
        # 发布额度配置前入队，quota_rule_id=NULL 永不被额度闸门拦截）。
        if "notif_send_tasks" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(notif_send_tasks)")}
            for name, ddl in (
                ("quota_version", "INTEGER"),
                ("quota_rule_id", "TEXT"),
                ("quota_level", "TEXT"),
                ("quota_snapshot", "TEXT"),
                ("quota_status", "TEXT NOT NULL DEFAULT 'none'"),
                ("quota_reason", "TEXT"),
                ("quota_generation",
                 "INTEGER NOT NULL DEFAULT 1"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE notif_send_tasks ADD COLUMN {name} {ddl}")
            self._conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_notif_send_quota "
                "ON notif_send_tasks(quota_version, quota_rule_id, recipient)")
        # 通知内容模板编排：老库联系人补语言偏好、发送任务/旧投递补内容固化列
        # （存量行视为无模板的静态正文，渲染器一律跳过，行为不变）。
        if "approval_contacts" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(approval_contacts)")}
            if "language" not in cols:
                self._conn.execute(
                    "ALTER TABLE approval_contacts ADD COLUMN language TEXT")
        if "notif_send_tasks" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(notif_send_tasks)")}
            for name, ddl in (
                ("template_snapshot", "TEXT"),
                ("content_snapshot", "TEXT"),
                ("content_sha256", "TEXT"),
                ("render_status",
                 "TEXT NOT NULL DEFAULT 'not_templated'"),
                ("render_failure_reason", "TEXT"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE notif_send_tasks ADD COLUMN {name} {ddl}")
        if "approval_notification_deliveries" in tables:
            cols = {r["name"] for r in self._conn.execute(
                "PRAGMA table_info(approval_notification_deliveries)")}
            for name, ddl in (
                ("template_version_id", "INTEGER"),
                ("template_language", "TEXT"),
                ("content_sha256", "TEXT"),
            ):
                if name not in cols:
                    self._conn.execute(
                        f"ALTER TABLE approval_notification_deliveries "
                        f"ADD COLUMN {name} {ddl}")

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

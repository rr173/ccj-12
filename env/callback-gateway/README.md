# callback-gateway — 外部系统回调网关

接收外部系统回调，**接入、处理、人工处置三层分离**：

```
                ┌────────────────────────────────────────────────────┐
  接入方 ──►    │ 接入层  POST /callbacks                              │
                │   验签(可轮换密钥环) → 幂等/冲突判定 → 事务落盘 → ACK │
                └──────────────┬─────────────────────────────────────┘
                               │ (SQLite WAL, synchronous=FULL)
                ┌──────────────▼─────────────────────────────────────┐
                │ 处理层  后台 worker                                 │
                │   到期拉取 → 业务处理 → 失败指数退避重试 → 超限隔离  │
                │   副作用先落 outbox，再按幂等键派发（exactly-once） │
                └──────────────┬─────────────────────────────────────┘
                               │
                ┌──────────────▼─────────────────────────────────────┐
                │ 人工处置层  /admin/*                                │
                │   冲突比较/选定续跑、隔离重投、全量审计查询          │
                ├────────────────────────────────────────────────────┤
                │ 业务重放层  /admin/replays/*                        │
                │   筛选预览 → 批量提交 → 多级审批 → 暂停/继续/取消     │
                │   审批策略版本化维护 /admin/replay-policies/*        │
                │     候选灰度发布·暂停/恢复/转正/回滚 …/releases/*     │
                │     影响预览·审批门禁 …/preview …/changes/*           │
                ├────────────────────────────────────────────────────┤
                │ 审批通知与待办  /admin/approval-notifications/*        │
                │   节点激活/投票/临期/拒绝/超时/策略变更 -> 站内待办       │
                │   邮件 + webhook 双通道；指数退避重试，超限隔离          │
                │   待办处理回写原审批动作（重复/过期/并发不重复投票）      │
                │   通知聚合：按接收人/批次/事件类型配窗口，重复提醒合并摘要 │
                │   静默时段：窗内只留站内待办，邮件/webhook 延迟后按序发送  │
                │   升级策略：按角色逐级升级、记录级别，原接收人处理即停止   │
                │   审批委托（生效/失效/撤销/再激活）                      │
                │     /admin/replay-delegations/*                      │
                │   重放副作用走同一 outbox 幂等链路                     │
                ├────────────────────────────────────────────────────┤
                │ 通知通道路由与故障转移  /admin/approval-notifications/routing/* │
                │   按事件类型配置 email/webhook/inbox 优先级·启用·接收条件     │
                │   版本化路由快照：任务按入队版本选道，发布/回滚不影响在途任务 │
                │   首选超时或连续失败才切换下一通道；尝试/切换/结果全程留痕    │
                │   时间窗口失败熔断：open 不派新请求，半开探针成功才恢复接流量 │
                │   同事件×接收人跨通道只一条业务通知（重启/重扫/并发幂等）     │
                └────────────────────────────────────────────────────┘
```

## 需求 → 实现对照

| 需求 | 实现 |
|---|---|
| 可轮换签名密钥，旧钥匙过渡期可用 | 密钥配置：`active` + `retired`（带 `grace_until`），见 `app/security.py` |
| 不重启热轮换、原子切换、版本可查 | `POST /admin/keys/rotate` 校验→落库→整份替换内存密钥环；`GET /admin/keys/current` / `/admin/keys/versions`，见 `app/keyconfig.py` |
| 同编号同内容只处理一次 | `deliveries UNIQUE(external_id, content_hash)`，重复投递返回 `duplicate`，见 `app/ingest.py` |
| 同编号不同内容冻结、人工比较选择、不覆盖 | 新版本独立落盘并整组 `frozen=1`，开冲突单；任何版本都不会被覆盖，见 `app/ingest.py`、`app/admin.py` |
| 落盘后才 ACK | 所有写入单事务提交（WAL + `synchronous=FULL`），提交后才返回响应 |
| 失败按间隔重试、连续失败隔离、不堵别的编号 | 指数退避 `base*2^(n-1)`；超限 `quarantined`；worker 按条拉取，隔离只影响该条，见 `app/worker.py` |
| 重启/重复投递副作用不做两次 | outbox 幂等键 + 派发前检查下游是否已应用该键，见 `app/worker.py`、`app/handlers.py` |
| 人工选定后从可追溯位置继续 | 选定版本保留 `checkpoint` 与 `attempts` 历史，解冻续跑；全程写 `events` 审计 |
| 只有被选中的版本继续产生外部效果 | 派发器只发「done 且未冻结」版本的 outbox；处置时未选中版本滞留的待派发副作用同事务取消（`cancelled`）并记审计，见 `app/worker.py`、`app/admin.py` |
| 查询签名/冲突/重试/处置记录 | `GET /admin/events`（只增不删的审计表） |
| 按编号/时间/处理结果筛选历史回调，先预览再提交 | `POST /admin/replays/preview` 与 `POST /admin/replays` 共用同一套筛选，看到的就是将要提交的，见 `app/replay.py` |
| 每条重放记录发起人、原始版本、原因、进度 | `replay_tasks` 逐条冗余 operator/reason/delivery_id，状态机 + attempts + checkpoint 即进度 |
| 同一份内容重复加入不生成第二个重放任务 | 批内 `UNIQUE(batch_id, delivery_id)`；`request_id` 重复提交返回原批次；跨批存在活动任务的投递自动跳过 |
| 重放可暂停/继续/取消 | `POST /admin/replays/{id}/pause|resume|cancel`；取消时未执行任务与滞留副作用同事务取消 |
| 高风险批次须他人审批后才能执行 | 提交带 `risk_level=high` + `approval_note`：批次落 `pending_approval`，worker 不领取；`POST .../approve`（审批人必须不同于发起人）放行，`POST .../reject`（必填原因）整体取消；超时由 worker 自动释放，见 `app/replay.py` |
| 审批策略可配置、版本化维护 | `POST /admin/replay-policies` 提交新策略：校验通过整份生效（版本号单调递增），失败保留当前策略；`GET .../current` / `.../versions` 查当前策略与每次变更/尝试记录，见 `app/replay_policy.py` |
| 按风险等级与批次规模生成串行/并行审批节点 | 策略规则按 `risk_level` + `min_size`/`max_size` 匹配，`mode=serial` 逐节点激活（各自起算截止时间），`mode=parallel` 全部同时待决；提交时生成 `replay_approval_nodes` |
| 每个节点记录允许角色、法定人数、实际审批人、截止时间 | `replay_approval_nodes`：allowed_roles / required_approvals / decided_by / deadline；节点有效赞成票达到法定人数才满足，串行节点按节点分别计数，并行节点各自达标才算全部满足 |
| 同一审批人不能重复计数或承担多个节点 | `replay_node_votes` 部分唯一索引：同一节点同一人至多一张有效票、同一批次同一人至多在一个节点持有效票（写事务串行 + 条件更新兜底并发），违反/重复决定 409 |
| 角色审批须凭当前有效的委托 | `POST /admin/replay-delegations` 为角色创建带生效/失效时间的委托（受托人、时间窗），决定时携带 `delegation_id`：本人、角色匹配、时间窗覆盖当前时刻且未撤销才接受；委托可撤销（原因必填）与重新激活，见 `app/delegation.py` |
| 委托到期/撤销后未满足节点重新等待，已落定节点不改写 | worker 每轮先扫委托到期；失效/撤销委托投在仍 `active` 节点上的赞成票置 `invalid`，有效人数回退、缺额重新等待；已达法定人数/拒绝/跳过/超时的节点永不改写 |
| 任一节点拒绝即终止批次，全部节点满足才放行 | 节点拒绝 → 批次 `rejected`、未执行任务整体取消；全部节点达到法定人数（或有理由跳过）→ 批次才进 `running`，worker 与 outbox 派发闸门口径不变 |
| 批次保存提交时的策略快照 | `replay_batches.policy_version` + `policy_snapshot`（规则/模式/节点规格含角色与法定人数）；策略更新只影响新提交的批次 |
| 同一风险等级维护多个候选策略并灰度分流 | `replay_policy_releases`：候选版本 + 批次规模闸门（`min_size/max_size`）+ `rollout_percent` 百分比分流；桶 `sha256(risk_level|分流键)%100`（分流键取 `request_id`，否则取本批投递 id 集合），重复提交/重启命中结果不变，见 `app/replay_rollout.py` |
| 候选策略发布前试运行校验 | `POST /admin/replay-policies/evaluate`：只读预演车道/bucket/命中版本与稳定/候选各自的规则节点链，不落数据 |
| 候选可暂停/恢复/转正/回滚 | `.../releases/{id}/pause|resume|promote|rollback`；暂停即停分流（新批次全走稳定版本）；转正把该风险等级稳定指针整份切到候选；回滚恢复上一稳定版本（原因必填），只影响之后的新提交，已生成审批节点的批次凭快照不变 |
| 批次记录命中的策略版本/分流规则/发布批次 | `replay_batches.policy_lane`（stable/candidate）+ `rollout_id` + `rollout_seq`；`policy_snapshot.routing` 存闸门/百分比/bucket/分流键，提交响应与批次详情直接可见 |
| 详情与策略历史展示稳定版本/候选版本/分流命中/暂停回滚原因 | `GET .../replay-policies/current` 附 `rollout`（每等级稳定/上一稳定版本 + 开放中的候选发布）；`GET .../releases` / `.../releases/{id}` 含状态、暂停/回滚原因与命中统计；批次详情 `approval.policy_routing` |
| 灰度发布与命中全程审计、并发无跨版本快照 | `replay_policy_release_published/paused/resumed/promoted/rolled_back/superseded` + 每批次 `replay_policy_batch_routed`；版本解析在批次写事务（BEGIN IMMEDIATE）内与发布变更串行，快照与列必来自同一份发布状态 |
| 策略变更前可预览影响范围 | `POST /admin/replay-policies/preview`（只读）：按当前批次规模与风险等级计算受影响范围（规模区间 × 等级）、预计命中比例（以历史批次为样本；发布候选时按闸门 × 百分比估算）、审批节点链前后对比与不兼容规则（高风险 fail-closed 缺口/被遮蔽规则/闸门无覆盖）；结果带策略版本与生成时间，不改变线上配置，见 `app/replay_policy_gate.py` |
| 高风险策略变更须他人审批后才能发布 | `replay_policy_changes` 变更单：提交即固化预览与基线；触及 high 等级/削弱审批强度/引入高风险缺口的变更（risk_class=high）必须由不同于提交人的运营 `approve` 后才能 `apply`；pending/approved 期间旧稳定版本继续服务 |
| 变更拒绝/超时/重复提交/并发审批不产生部分生效 | 拒绝与超时（`REPLAY_POLICY_CHANGE_TTL_SECONDS`，worker 扫描 + 决定时惰性判定）只是变更单状态转移；`request_id` 幂等键防重复提交；并发审批由写事务串行 + 条件状态转移保证单一赢家；`apply` 把配置变更与变更单落定放在同一事务，基线被推进过时拒绝（stale） |
| 发布后可查变更前后版本/预览/审批决定/审计 | `GET .../changes` / `.../changes/{id}`：base_version 与 applied_version（或 release_id）、固化的影响预览、审批决定（决定人/时间/原因）与 `replay_policy_change_*` 审计轨迹 |
| 详情展示每节点已批准人数/法定人数/有效委托/缺额 | 批次详情 `approval.nodes`：`approved_count`、`required_approvals`、`missing`、`quorum_reached`、`valid_delegations`（允许角色当前有效的委托）、每张票（含已失效票）；另有批次级 `missing_approvals` |
| 委托与决定全生命周期可审计 | `replay_delegation_created`/`revoked`/`reactivated`/`expired`、`replay_node_vote_invalidated`、`replay_approval_node_quorum_lost` + 既有节点/批次事件；节点可带原因跳过（视为满足，留痕） |
| 审批结果/拒绝原因/超时释放/批准后执行全部可审计 | `replay_batch_approved` / `replay_batch_rejected`（含 reason）/ `replay_batch_approval_expired` + 既有执行事件；批次详情含 `approval`（状态、发起人、审批人、批准时间、拒绝原因、截止时间） |
| 重复提交或重复/并发决定不会执行两次 | `request_id` 重复提交返回原批次；票有部分唯一索引、节点/批次决定是带状态守卫的条件更新（写事务串行），重复决定返回 409，不重复计数、不产生第二套任务与事件 |
| 批次级并发配额 | 提交时 `max_concurrency` 指定整批最多同时处理多少条；占用=本批 `processing` 任务数，领取时在占位事务里实时推导复核，见 `app/replay.py` |
| 同一编号多条历史版本按 created_at 先后执行 | 任务落盘快照 `delivery_created_at` 作排序键；前序未进终态（done/failed/cancelled）时条件更新拒绝领取后一条 |
| 批次详情显示占用/等待/每条阻塞原因 | `GET /admin/replays/{id}`：`in_flight`（当前占用）、`waiting`（等待数量）、每条任务的 `blocked_reason`（实时计算） |
| 暂停/取消/重启后配额正确回收 | 配额无计数器：任务离开 `processing`（完成/失败/取消/recover 退回）槽位即释放，不会永久卡住 |
| 额度不足/顺序冲突/失败重试/取消都有状态与审计 | `replay_task_blocked`（原因变化才写，轮询不刷表）+ 既有 `replay_task_retry_scheduled`/`replay_task_failed`/`replay_batch_cancelled` 事件 |
| 重启后未完成任务从上次位置继续 | 启动时 `ReplayWorker.recover()` 把卡在 processing 的任务退回 pending，attempts/checkpoint 都在库里 |
| 失败单独重试、不阻塞其他编号 | 任务级指数退避，超限标记 `failed`，`POST /admin/replays/tasks/{id}/retry` 单条重试 |
| 重放副作用与正常处理同样的幂等保护 | 重放副作用落同一 `outbox`（`replay_task_id` 标识），幂等键以 `replay:{task_id}` 为作用域，同一派发器 + 下游去重 |
| 每次重放的完整审计记录 | `GET /admin/replays/{id}/events`（可按单条任务过滤） |
| 审批节点激活/投票/临期/拒绝/超时/策略变更须通知到人 | `approval_notify_events` + `approval_todos`：按节点角色与当前有效委托解析接收人（'any' 节点/变更单取联系人目录除发起人外成员），见 `app/notifications.py` |
| 站内待办 + 邮件/webhook 双通道 | 待办始终生成；投递按联系人 `channels` 落 `approval_notification_deliveries`，发送器可注入（默认 noop） |
| 同一事件同一接收人只一个待办 | 事件 `event_key` 唯一 + `approval_todos UNIQUE(event_id, recipient)`，worker 重复扫描/重启/双击均去重 |
| 通知失败指数退避重试、超限隔离 | `base*2^(n-1)` 退避、`NOTIF_MAX_ATTEMPTS` 后 `quarantined`，可 `.../deliveries/{id}/requeue`；待办关闭时未发出投递取消 |
| 待办记录来源/节点/接收人/事件版本/状态 | `approval_todos`：batch_id/change_id、node_id、recipient、event_version、unread/read/handled/expired/cancelled |
| 处理待办回写原审批动作且防重复/防过期/防并发 | 认领与原决定（`decide_node_tx`/`decide_change_tx`）同一写事务；重复点击/过期/并发均 409，不重复投票、不越过门禁 |
| 待办看板与多维度查询 | `GET .../summary`（未读/已读/已处理/过期/取消 + 失败/隔离）；`GET .../todos` 按接收人/来源/节点/状态过滤 |
| 通知全链路审计 | 生成/发送/重试/隔离/确认/回写/关闭均落 `events`（`approval_*` 类型，独立 detail 键不污染批次时间线） |
| 重复提醒按窗口聚合为一条摘要 | `approval_aggregation_rules`（接收人/来源批次/事件类型 + 窗口秒数）：命中的邮件/webhook 窗口内 `held` 入 `approval_notification_groups`，窗口关闭每通道合并一条摘要（webhook 负载含全部成员，按原事件顺序），成员置 `aggregated` 保留可查；站内待办始终即时逐条生成，见 `app/notif_policy.py` |
| 静默时段只留站内待办、外发延迟并按原顺序发送 | `approval_quiet_schedules`（每日 UTC 重复/一次性、可按接收人与通道）：窗内外发落 `delayed`，站内待办不受影响；时段结束 worker 按 `(ordinal,id)`（=待办创建顺序）放回发送队列；计划停用立即放行；静默结束前待办已关闭则取消不补发 |
| 临期未处理按角色逐级升级、记录级别 | `approval_escalation_policies`（角色 + 逐级接收人，`after_seconds` 待办生成后 / `before_deadline_seconds` 截止前触发）：按「来源节点/变更单 + 原始接收人」去重，每级一条 `escalated` 可操作待办与 `approval_escalation_levels` 记录；重复扫描/重启/并发不重发（唯一约束 + 条件状态转移） |
| 原接收人处理后升级通知必须停止 | 待办处理/对账关闭/联系人停用在同一写事务调用 `stop_escalations_for_todo_tx`：升级链落 `stopped`（原因 handled/source_closed）、后续级别不再触发、已升级别未发出投递（pending/failed/held/delayed）同事务取消；已发出外部效果轨迹保留 |
| 聚合/延迟/升级/取消/恢复可查询 | `GET .../aggregation-rules`、`.../aggregation-groups`（组状态+成员+摘要投递）、`.../quiet-schedules`、`.../escalation-policies`、`.../escalations`（每级触发时间/接收人/停止原因）；待办视图内嵌 held/delayed 通道与升级链，看板增加 held/delayed/aggregated 计数 |
| 聚合/延迟/升级/取消/恢复全程审计 | `approval_delivery_held`、`approval_aggregation_group_flushed/cancelled`、`approval_delivery_delayed/released`、`approval_escalation_armed/fired/stopped`、各配置 `_set` 事件全部落 `events` |
| 重启/重复事件/并发扫描不重发不漏发 | 所有状态转移为写事务内条件 UPDATE + 唯一索引（组、升级链、级别、事件 event_key、待办 event+recipient）；worker 步骤序：静默放行→截止提醒→对账→升级扫描→聚合刷新→投递；通道 IO 在事务外，崩溃重启后从库内状态继续 |
| 运营按事件类型配置通道优先级/启用/接收条件 | `notif_route_versions` 版本化路由：规则按 event_type（缺省规则兜底）匹配，通道链按 priority 排序、可 enabled、可带 condition（actionable/source_type/risk_level/recipients 白名单），见 `app/notif_routing.py` |
| 发送任务按当前版本路由快照选通道 | 任务入队时固化 `route_version`+`route_snapshot`+有序 `plan_json`（含每道地址/超时/尝试次数/条件）；之后发布新版本或回滚都不改变已入队任务 |
| 首选通道超时或连续失败才切换下一通道 | 超时（`timeout_seconds`，工作线程强杀）立即切换；其他失败在本通道按 `max_attempts` 指数退避重试，达上限才切换；每次尝试落 `notif_send_attempts`，每次切换落 `notif_channel_switches`（selected/timeout/consecutive_failures/breaker_open/channel_disabled/no_address/plan_exhausted） |
| 通道按时间窗口统计失败熔断、自动恢复 | `notif_channel_state`：窗口内连续失败（超时计失败，成功即重新计数）达阈值 open；open 期间不派新请求；冷却后 half_open 放唯一探针，探针成功才 closed 接流量、失败回 open |
| 熔断期间不向该通道派新请求 | 派发闸门 `_admit_for_dispatch`：disabled/open（未到冷却）直接挡下，任务切换下一通道（原因 breaker_open/channel_disabled）；计划全部不可用时等待最近恢复时刻，不隔离、不丢通知 |
| 同一事件对同一接收人不同通道不重复通知 | `notif_send_tasks UNIQUE(event_id,recipient)`：一个事件×接收人只一条任务，沿通道链成功一条即 sent；inbox 为末位兜底（站内待办始终即时生成），外发全挂也不漏关键通知 |
| 重启/重复扫描/并发发送不越幂等 | 领取是条件 UPDATE（pending/failed→in_flight 单赢家）；启动 recover 把 in_flight 退回 pending；尝试与熔断统计只增落库，崩溃后从库内状态继续，不重发已成功通道 |
| 查询路由版本/待发任务/通道健康/切换历史/审计 | `GET .../routing/current`·`/versions`、`/tasks`(+/tasks/{id} 含 attempts/switches)、`/channels/health`、`/switches`、`/attempts`；审计走 `events`（`notif_route_*`/`notif_send_*`/`notif_channel_*`） |
| 发布新版本/回滚不影响已入队任务 | 发布整份生效（版本号单调递增，失败落 rejected 保留现版）；回滚到上一生效版本（原因必填）只推进 `notif_route_current` 指针，只影响之后入队的任务 |
| Docker 部署 | `Dockerfile` + `docker-compose.yml` |

## 快速开始

```bash
cp keys.example.json keys.json   # 修改里面的 secret
docker compose up --build
```

本地开发：

```bash
pip install -r requirements-dev.txt
python -m pytest tests/          # 208 个端到端测试
uvicorn app.main:create_app --factory --reload
```

## 接入方调用方式

```bash
BODY='{"order":"A-1001","amount":100}'
TS=$(date +%s)
SIG=$(printf '%s\n%s' "$TS" "$BODY" | openssl dgst -sha256 -hmac "当前密钥" -hex | awk '{print $2}')

curl -X POST http://localhost:8000/callbacks \
  -H "X-Callback-Id: A-1001" \
  -H "X-Signature: kid=k2,ts=$TS,sig=$SIG" \
  -H "Content-Type: application/json" \
  --data-raw "$BODY"
```

- 签名串为 `{ts}\n{原始请求体}` 的 HMAC-SHA256（hex）。
- 响应：`202 accepted`（已落盘，待处理）/ `200 duplicate`（同编号同内容，幂等命中）/
  `202 conflict`（同编号不同内容，已冻结待人工处置）/ `401`（验签失败，已记审计）。
- **只有数据可靠落盘后才会收到 2xx**；未收到 2xx 时请原样重发（幂等保证不会重复处理）。

## 密钥轮换（不重启）

运营通过管理端点提交新密钥配置，**校验通过才切换，且整份一次性生效**——正在处理的
请求只会看到完整的旧配置或完整的新配置。每次提交（无论成败）都记录配置版本、
操作者、时间和结果；重启后自动恢复最后一次成功应用的配置。

```bash
# 查看当前生效的配置版本（密钥明文打码，永不回显）
curl localhost:8000/admin/keys/current

# 提交新配置：k3 成为新签发密钥，k2 退为过渡密钥，k1 立即失效
curl -X POST localhost:8000/admin/keys/rotate \
  -H 'Content-Type: application/json' \
  -d '{
    "operator": "ops-li",
    "keys": [
      {"kid": "k3", "secret": "new-secret-3", "status": "active"},
      {"kid": "k2", "secret": "new-secret-2", "status": "retired", "grace_until": "2026-10-01T00:00:00Z"},
      {"kid": "k1", "secret": "old-secret-1", "status": "retired", "grace_until": "2020-01-01T00:00:00Z"}
    ]
  }'
# -> {"result": "applied", "version": 2}

# 查看每次切换/尝试的记录（含被拒绝的提交及原因）
curl localhost:8000/admin/keys/versions
```

- 校验规则：必须是 `{"keys": [...]}` 且非空；`kid` 非空不重复；`secret` 非空；
  至少一个 `active` 密钥；`retired` 密钥必须带合法的 `grace_until`（ISO-8601 时间）。
  任何一条不满足 → `422` + 具体原因，**当前配置原样保留**，失败也落审计。
- 切换语义：先在事务里写入新版本记录并提交，再原子替换内存密钥环；崩溃重启后
  从数据库恢复最后一次 `applied` 配置（首次启动用 `keys.json` 引导为第 1 版）。
- 过渡期内旧密钥签名仍被接受（审计记录使用的 `kid`）；超过 `grace_until` 一律
  拒绝（`retired_key_grace_expired`）。

## 人工处置

```bash
# 看冲突及所有候选版本内容（供比较）
curl localhost:8000/admin/conflicts?status=open
curl localhost:8000/admin/conflicts/1

# 选定某份内容继续处理（其余版本标记 superseded，内容保留可查，绝不覆盖）
curl -X POST localhost:8000/admin/conflicts/1/resolve \
  -H 'Content-Type: application/json' \
  -d '{"delivery_id": 2, "operator": "zhangsan", "note": "以金额200的为准"}'

# 隔离队列重投（重置重试计数）
curl -X POST localhost:8000/admin/deliveries/5/requeue \
  -H 'Content-Type: application/json' -d '{"operator": "zhangsan"}'

# 审计查询：每次签名、冲突、重试、隔离、副作用、处置
curl 'localhost:8000/admin/events?type=retry_scheduled'
curl 'localhost:8000/admin/events?external_id=A-1001'
```

## 业务重放

运营按编号、时间、处理结果筛选历史回调，**先预览内容和影响范围，再一次性提交一批
重放任务**。每条重放记录发起人、原始版本、原因和进度；全程可暂停/继续/取消，
重启后未完成的任务从上次位置继续。

```bash
# 1) 预览：将要重放的内容 + 影响范围（该版本正常处理时产出过的外部副作用）
#    筛选条件：external_id / status / created_from / created_to（epoch 秒或 ISO-8601）/ delivery_ids
curl -X POST localhost:8000/admin/replays/preview \
  -H 'Content-Type: application/json' \
  -d '{"external_id": "A-1001", "status": "done"}'
# -> {"matched": 1, "replayable": 1, "items": [{"delivery_id": 1, "payload": ...,
#      "replayable": true, "prior_effects": [{"effect_type": "downstream.notify", ...}]}]}
#    不可重放的版本会带 skip_reason：still_in_pipeline（仍在正常管线）、
#    frozen_by_conflict（冲突冻结中）、superseded_version（人工未选中的版本）、
#    active_replay_in_batch:N（另一批里已有该内容的活动重放任务）

# 2) 提交一批重放任务（批次 + 全部任务单事务落盘；request_id 为提交幂等键，
#    重复提交返回原批次，不会生成第二批任务；
#    max_concurrency 可选：整个批次最多同时处理多少条任务，缺省不限）
curl -X POST localhost:8000/admin/replays \
  -H 'Content-Type: application/json' \
  -d '{"external_id": "A-1001", "status": "done",
       "operator": "ops-li", "reason": "下游丢数据需补发", "request_id": "req-20260912-01",
       "max_concurrency": 2}'
# -> {"result": "created", "batch_id": 1, "total": 1, "skipped": [],
#     "status": "running", "risk_level": "normal", "approval_status": "not_required"}

# 2b) 高风险批次：标记 risk_level=high 并填写审批说明 approval_note。
#     批次落为 pending_approval（任务照常占住对应内容，但 worker 一律不领取），
#     按当前生效的审批策略生成审批节点链，全部节点满足后才进入 running。
#     （未配置策略时为内置默认策略：一名非发起人审批，时限
#      REPLAY_APPROVAL_TIMEOUT_SECONDS。）
curl -X POST localhost:8000/admin/replays \
  -H 'Content-Type: application/json' \
  -d '{"delivery_ids": [12, 13], "operator": "ops-li", "reason": "资金类回调补发",
       "risk_level": "high", "approval_note": "涉及 2 笔退款回调，已与下游核对窗口"}'
# -> {"result": "created", "batch_id": 2, "total": 2,
#     "status": "pending_approval", "approval_status": "pending",
#     "policy_version": null, "approval_nodes": 1,
#     "approval_deadline": 1757760000.0}

# 另一个运营人员（不能是发起人 ops-li）批准 / 拒绝（拒绝必须带原因）。
# 批次级入口在恰好一个节点待决时可用；多节点并行时用节点级入口（见下文）。
curl -X POST localhost:8000/admin/replays/2/approve \
  -H 'Content-Type: application/json' \
  -d '{"operator": "ops-wang", "note": "已电话核实，可以放行"}'
# -> {"result": "approved", "batch_id": 2, "status": "running"}
curl -X POST localhost:8000/admin/replays/2/reject \
  -H 'Content-Type: application/json' \
  -d '{"operator": "ops-wang", "reason": "影响面评估不通过", "note": "等下游就绪窗口"}'
# -> {"result": "rejected", "batch_id": 2, "cancelled_tasks": 2}

# 超过当前节点截止时间仍无人决定：replay worker 下一轮自动释放——
# 批次置 cancelled、approval_status=expired、未执行任务整体取消（审计可查）。
# 发起人也可以在待决期间主动 cancel 撤回。

# 3) 跟踪进度 / 控制执行
#    批次详情：进度计数 + max_concurrency + in_flight（当前占用）+ waiting（等待数量）
#    + approval（风险等级、审批状态、发起人、审批人、批准时间、拒绝原因、截止时间、
#      策略版本、各审批节点、当前待决节点、剩余节点、超时状态）
#    + 每条任务状态与 blocked_reason（被阻塞的原因，可执行为 null）
curl localhost:8000/admin/replays/1
curl -X POST localhost:8000/admin/replays/1/pause  -H 'Content-Type: application/json' -d '{"operator": "ops-li"}'
curl -X POST localhost:8000/admin/replays/1/resume -H 'Content-Type: application/json' -d '{"operator": "ops-li"}'
curl -X POST localhost:8000/admin/replays/1/cancel -H 'Content-Type: application/json' -d '{"operator": "ops-li", "note": "改走线下"}'

# 4) 失败任务单独重试（只影响这一条，不阻塞其他编号）
curl -X POST localhost:8000/admin/replays/tasks/3/retry \
  -H 'Content-Type: application/json' -d '{"operator": "ops-li"}'

# 5) 该批次的完整审计记录（可按 task_id 过滤到单条）
curl 'localhost:8000/admin/replays/1/events'
curl 'localhost:8000/admin/replays/1/events?task_id=3'
```

- 只有 `done` / `quarantined` 的版本可重放；仍在管线中、冲突冻结中、人工未选中的
  版本会被跳过并在预览/提交响应里给出原因。
- **批次级并发配额**：`max_concurrency` 限制整批同时处理的任务数。占用量不存
  计数器，而是领取时在占位事务里实时数本批 `processing` 任务数——任务完成、
  失败退避、被取消、或重启时被 recover 退回，槽位都立即释放，暂停/取消/重启
  之后配额天然正确，不会有任务因配额泄漏而永久卡住。
- **同一编号有序执行**：同一批次里同一 `external_id` 的多条历史版本按版本落盘
  时间（`delivery_created_at` 快照，相同再按 `delivery_id`）先后执行；前一条
  未进终态（done/failed/cancelled）时后一条不能被 worker 领取。前序失败退避
  期间后一条等待；前序终态失败/被取消后后一条放行，不会死锁。
- **被阻塞任务的状态与审计**：批次详情里每条 pending 任务带实时 `blocked_reason`
  —— `awaiting_approval`（高风险待审批）/ `batch_rejected`（已拒绝）/
  `batch_paused` / `retry_backoff` / `waiting_predecessor:{task_id}` /
  `quota_exhausted:{占用}/{上限}`；worker 每轮复核领取闸门，原因变化时写
  `replay_task_blocked` 审计事件（不变不重复写），与失败重试、取消的既有事件
  一样可按批次/任务查询。
- 重放副作用与正常处理**走同一条 outbox 幂等链路**：幂等键以 `replay:{task_id}`
  为作用域——同一任务重试、服务重启都不会重复派发，下游仍按幂等键去重；
  新批次的重放才会有意再次产生外部效果。
- 批次状态机：普通批次 `running → paused → running → completed /
  completed_with_failures`；需审批的批次先到 `pending_approval`——所有审批节点
  满足后进入 `running`，任一节点 `reject`（或发起人撤回）进 `rejected`/`cancelled`，
  任一节点超过其截止时间未决由 worker 自动释放为 `cancelled`
  （`approval_status=expired`）；`rejected`/`cancelled` 为终态；全部任务到终态后
  批次自动收尾。
- **审批的防绕过与幂等**：待审批期间任务以 `pending` 占住对应内容，
  跨批的「活动重放」检查会阻止另开一批重放同一投递；worker 领取查询与占位
  事务双重要求批次 `running` 且审批状态为 `approved`/`not_required`；outbox
  派发同样只放行 `running/completed/completed_with_failures` 批次。节点决定、
  批准、拒绝、超时、取消全部是带状态守卫的条件更新（写事务串行），重复提交
  （`request_id`）或重复/并发决定只会返回原批次或 `409`，不会产生第二套任务、
  第二套节点、第二份副作用或第二条决定事件。

## 多级审批策略

审批要求不是硬编码的：运营可以维护**版本化的审批策略**，提交批次时按
**风险等级与批次规模**匹配规则，为该批次生成**串行或并行**的审批节点链。

```bash
# 查看当前生效策略（从未提交过策略时为内置默认策略：
# 高风险需一名非发起人审批，普通批次直接运行）
curl localhost:8000/admin/replay-policies/current

# 提交新策略（校验通过整份生效，版本号单调递增；失败保留当前策略，
# 两种结果都落版本记录与审计）：
#   规则按序匹配，第一条「风险等级相符且批次规模落在区间内」的规则生效；
#   mode=serial  节点逐个激活，上一节点满足后才激活下一节点（各自起算截止时间）
#   mode=parallel 所有节点同时待决，全部满足才放行
#   节点可配置：
#     role 单个允许角色 / roles 角色列表（any = 任何非发起人）
#     required_approvals 法定人数（默认 1）：有效赞成票达到该数节点才满足
curl -X POST localhost:8000/admin/replay-policies \
  -H 'Content-Type: application/json' \
  -d '{
    "operator": "ops-li",
    "policy": {"rules": [
      {"name": "high-large", "risk_level": "high", "min_size": 10, "mode": "serial",
       "nodes": [
         {"roles": ["ops-lead", "oncall-lead"], "required_approvals": 2,
          "timeout_seconds": 1800},
         {"role": "finance-controller", "timeout_seconds": 3600}
       ]},
      {"name": "high-small", "risk_level": "high", "mode": "parallel",
       "nodes": [
         {"role": "ops-lead", "timeout_seconds": 3600},
         {"role": "security", "timeout_seconds": 3600}
       ]},
      {"name": "normal-default", "risk_level": "normal", "nodes": []}
    ]}
  }'
# -> {"result": "applied", "version": 1}

# 每次策略变更/尝试的记录（含被拒绝的提交及原因）
curl localhost:8000/admin/replay-policies/versions
```

### 审批策略灰度发布与回滚

整份提交是「立即全量生效」。需要先小流量验证新策略时，运营可以为**同一风险等级**
把某个已生效版本作为**候选**灰度：按**批次规模闸门**（`min_size`/`max_size`）与
**分流百分比**（`rollout_percent` 1-100）把新提交的批次逐步引流到候选版本；
稳定版本继续承接未命中的全部流量。候选可暂停、恢复、转正、回滚。

```bash
# 0) 先把候选策略作为一个正式版本提交（立即全量没关系——下一步再灰度一个旧版本，
#    或直接提交两个版本后把新版本发成候选；下面以 v2 稳定、v3 候选为例）
curl -X POST localhost:8000/admin/replay-policies -H 'Content-Type: application/json' \
  -d '{"operator":"ops-li","policy":{"rules": [...]}}'   # -> version 2（稳定）
curl -X POST localhost:8000/admin/replay-policies -H 'Content-Type: application/json' \
  -d '{"operator":"ops-li","policy":{"rules": [...]}}'   # -> version 3
# v3 是新版本，再把稳定策略作为 v4 整份提交（稳定指针=v4），随后把 v3 发成候选

# 1) 发布灰度：高风险、规模 1-20 条的批次 30% 走候选 v3（第 1 批发布）
curl -X POST localhost:8000/admin/replay-policies/releases \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-rel","risk_level":"high","candidate_version":3,
       "rollout_percent":30,"min_size":1,"max_size":20,"note":"小批先试"}'
# -> {"result":"published","release_id":1,"rollout_seq":1,
#     "candidate_version":3,"stable_version":4,"status":"candidate", ...}

# 1b) 发布前试运行校验（只读，不落数据）：预演某个批次会命中哪个车道/版本/规则
curl -X POST localhost:8000/admin/replay-policies/evaluate \
  -H 'Content-Type: application/json' \
  -d '{"risk_level":"high","total":12,"request_id":"req-20260912-7"}'
# -> {"would_hit_lane":"candidate","would_use_version":3,"routing_reason":"matched_candidate",
#     "bucket":7,"routing_key":"req:req-20260912-7",
#     "stable_rule":{...},"candidate_rule":{规则名/模式/节点链...},
#     "release":{闸门/百分比/命中统计...}}

# 2) 正常提交批次即可；响应直接带分流命中（policy_lane/rollout_id/rollout_seq/bucket）
curl -X POST localhost:8000/admin/replays -H 'Content-Type: application/json' \
  -d '{"delivery_ids":[12],"operator":"ops-li","reason":"补发","risk_level":"high",
       "approval_note":"...","request_id":"req-20260912-7"}'
# -> {"policy_version":3,"policy_lane":"candidate","rollout_id":1,"rollout_seq":1,
#     "routing_bucket":7,"approval_nodes":2, ...}

# 3) 观察：当前稳定版本/上一稳定版本/开放中的候选（含命中数）；发布单列表与详情
curl localhost:8000/admin/replay-policies/current          # 含 rollout.risk_levels.*
curl 'localhost:8000/admin/replay-policies/releases?risk_level=high'
curl localhost:8000/admin/replay-policies/releases/1       # 含 hits / pause_reason / rollback_reason

# 4) 异常时暂停（原因必填）：立即停止分流，之后新提交全走稳定版本；
#    已命中候选、已生成审批节点的批次继续按候选快照审批，不受影响
curl -X POST localhost:8000/admin/replay-policies/releases/1/pause \
  -H 'Content-Type: application/json' -d '{"operator":"ops-rel","reason":"候选审批链异常率升高"}'
curl -X POST localhost:8000/admin/replay-policies/releases/1/resume \
  -H 'Content-Type: application/json' -d '{"operator":"ops-rel"}'

# 5) 转正：该风险等级稳定版本整份切到候选（上一稳定版本记录为回滚目标）
curl -X POST localhost:8000/admin/replay-policies/releases/1/promote \
  -H 'Content-Type: application/json' -d '{"operator":"ops-rel"}'

# 6) 回滚（原因必填）：灰度中回滚=关闭发布单、全部新批次走稳定版本；
#    转正后回滚=稳定指针恢复为上一稳定版本。只影响之后的新提交——
#    已经生成审批节点的批次持有自己的策略快照，永不改变
curl -X POST localhost:8000/admin/replay-policies/releases/1/rollback \
  -H 'Content-Type: application/json' -d '{"operator":"ops-rel","reason":"紧急止损"}'
```

- **确定性分流**：是否命中候选只取决于 `(风险等级, 分流键, 规模闸门, 百分比)`——
  分流键优先取提交幂等键 `request_id`，否则取本批选中投递 id 的有序集合；
  `bucket = sha256(risk_level|分流键) 前 12 位 % 100`，`bucket < percent` 命中。
  结果与提交时刻、服务重启无关；同一 `request_id` 重复提交必然看到同一车道。
  批次落盘时把命中版本、车道、发布单与发布批次序号、闸门/百分比/bucket 全部固化到
  `policy_lane`/`rollout_id`/`rollout_seq` 与 `policy_snapshot.routing`，
  之后发布单暂停、调百分比或回滚都不改变已落盘批次。
- **同一风险等级至多一条未结束灰度**（candidate/paused；部分唯一索引 + 写事务串行
  兜底并发发布）。`rollout_seq` 是该等级的发布批次序号，单调递增、随批次留痕。
- **发布校验**：候选版本必须是已 applied 的版本、不能等于当前稳定版本；候选策略必须
  有规则覆盖该风险等级且规则规模区间与声明闸门相交（否则被引入候选车道的批次会
  无规则匹配，直接拒绝发布 `candidate_policy_does_not_cover_gate`）。
- **fail closed 不变**：无论命中哪条车道，高风险批次在该版本下无规则匹配仍以
  `no_applicable_policy` 拒绝提交（响应附带命中的车道与版本），不会因分流静默降低
  审批要求。
- **整份提交与灰度的关系**：`POST /admin/replay-policies` 仍整份生效——两个风险等级
  的稳定指针同事务切到新版本，当时尚未结束的灰度发布单整份关闭为 `superseded`
  （写审计）；切换与批次提交在写事务里串行，批次不会读到无版本或前后版本混搭的快照。
- **并发安全**：批次的版本解析发生在批次写事务内部（`BEGIN IMMEDIATE`，所有写事务
  串行），与发布/暂停/恢复/转正/回滚/整份切换互斥；解析到的版本与落盘的节点链、
  快照必然来自同一份发布状态。

- **节点链随批次落盘（策略快照）**：提交时解析出的规则与节点规格（含允许角色与
  法定人数）保存在 `replay_batches.policy_snapshot`（连同 `policy_version`），
  审批节点行一次性生成——之后策略再更新也**不改变已提交的批次**，只影响新提交。
- **fail closed**：已有生效策略时，高风险批次若没有任何规则匹配，提交直接被
  拒绝（`422 no_applicable_policy`），不会静默降低审批要求；普通批次无匹配
  规则则直接运行。
- **法定人数按节点分别计数**：串行节点逐个满足（上一节点达到人数后才激活下一
  节点）；并行节点各自达到法定人数才算全部满足。每张赞成票是
  `replay_node_votes` 中的一行，同一审批人在同一节点只有一张有效票（重复/
  并发决定返回 409，不重复计数），在同一批次也不能在多个节点持有效票。
- **每个节点**记录允许角色（`allowed_roles`，`any` 表示任何非发起人）、法定
  人数（`required_approvals`）、实际落定人（`decided_by`/`decided_role`）与
  截止时间（`deadline`，激活时起算）。承担指定角色必须凭本人**当前有效的
  审批委托**（见下节）。
- **职责分离**：审批人不能是批次发起人；一人在同一批次至多在一个节点持有效票
  （违反返回 403）。
- **节点级操作**（承担指定角色时须携带本人当前有效的 `delegation_id`；`any`
  节点可省略）：
  ```bash
  # 赞成票（法定人数 > 1 时需多人分别投票，达到人数节点才落定；返回
  # approved_count/required_approvals/missing 实时计数）
  curl -X POST localhost:8000/admin/replays/3/nodes/7/approve \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-wang", "role": "ops-lead", "delegation_id": 12,
         "note": "现场已核对"}'
  # 拒绝（必填原因）/ 跳过（必填原因，视为该节点已满足，留痕可追溯）
  curl -X POST localhost:8000/admin/replays/3/nodes/8/reject \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-zhao", "role": "finance-controller", "delegation_id": 13,
         "reason": "影响面评估不通过"}'
  curl -X POST localhost:8000/admin/replays/3/nodes/8/skip \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-zhao", "role": "finance-controller", "delegation_id": 13,
         "reason": "主管休假，值班经理代签已电话确认"}'
  ```
  任一节点**拒绝**或**超时**即终止整个批次（未执行任务整体取消）；所有节点
  达到法定人数（或被有理由**跳过**）后批次才进入 `running`，worker 方可领取。
  批次级 `/approve`、`/reject` 入口在恰好一个节点待决时仍然可用（单节点、
  法定人数 1 的批次行为与之前一致）。
- **批次详情**的 `approval` 展示：`nodes`（每个节点的允许角色、法定人数
  `required_approvals`、有效赞成人数 `approved_count`、还缺多少人 `missing`、
  `quorum_reached`、每张票 `votes`——含已失效票及其失效时间、允许角色当前
  有效的委托 `valid_delegations`、状态、实际落定人、截止时间、是否已超时）、
  批次级 `missing_approvals`、`current_node_ids`、`remaining_node_ids`、
  `policy_version` 与整体超时状态。
- **审计**：委托生命周期（`replay_delegation_created`/`revoked`/
  `reactivated`/`expired`）、票失效与法定人数回退（`replay_node_vote_invalidated`/
  `replay_approval_node_quorum_lost`）、节点赞成/拒绝/跳过/超时/激活
  （`replay_approval_node_*`）、批次放行/拒绝/超时释放
  （`replay_batch_approved`/`rejected`/`approval_expired`）、策略变更
  （`replay_policy_applied`/`rejected`）全部落 `events`，可按批次一次查全。

### 策略变更的影响预览与审批门禁

在整份提交与灰度发布之上，运营可以为「提交新策略」或「发布候选版本」先做
**影响预览**，再走**审批门禁**发布（`app/replay_policy_gate.py`）：

```bash
# 1) 影响预览（只读，不改变线上配置）：新策略文档与候选版本二选一
curl -X POST localhost:8000/admin/replay-policies/preview \
  -H 'Content-Type: application/json' \
  -d '{"policy": {"rules": [...]}}'
# 预演灰度发布：候选版本 + 风险等级 + 规模闸门 + 分流百分比
curl -X POST localhost:8000/admin/replay-policies/preview \
  -H 'Content-Type: application/json' \
  -d '{"candidate_version": 3, "risk_level": "high",
       "rollout_percent": 30, "min_size": 1, "max_size": 20}'
# -> {"generated_at": ..., "policy_version": 4, "version_status": "expected",
#     "base_version": 3, "risk_class": "high", "requires_approval": true,
#     "affected_scope": [{"risk_level": "high", "min_size": 1, "max_size": null,
#        "before_rule": "...", "after_rule": "...", "approval_nodes": {"before": [...], "after": [...]},
#        "weakens_approval": false}],
#     "estimated_hit": {"basis": "replay_batches_history", "sampled_batches": 42,
#        "affected_batches": 9, "estimated_hit_ratio": 0.214, ...},
#     "incompatible_rules": [{"type": "high_risk_uncovered", ...}]}

# 2) 提交变更单：预览与基线（当前 applied 版本 + 各等级稳定指针）随单固化，
#    状态 pending——不影响线上，旧稳定版本继续服务；request_id 为幂等键
curl -X POST localhost:8000/admin/replay-policies/changes \
  -H 'Content-Type: application/json' \
  -d '{"operator": "ops-li", "request_id": "chg-2026-001",
       "policy": {"rules": [...]}}'
# -> 201 {"result": "created", "change": {"id": 1, "status": "pending",
#         "risk_class": "high", "requires_approval": true, "preview": {...}}}

# 3) 高风险变更：由不同于提交人的运营批准（拒绝必填原因）；标准变更无需审批，
#    提交人可直接执行。超时（REPLAY_POLICY_CHANGE_TTL_SECONDS）未决自动 expired
curl -X POST localhost:8000/admin/replay-policies/changes/1/approve \
  -H 'Content-Type: application/json' -d '{"operator": "ops-wang"}'

# 4) 执行：配置变更（整份生效 / 创建灰度发布单）与变更单落定同一事务；
#    基线被其他变更推进过时拒绝（stale），需重新预览生成新变更单
curl -X POST localhost:8000/admin/replay-policies/changes/1/apply \
  -H 'Content-Type: application/json' -d '{"operator": "ops-li"}'
# -> {"result": "applied", "change": {"result": {"applied_version": 4, ...}}}

# 5) 发布后查询：变更前后版本、固化的影响预览、审批决定与审计轨迹
curl localhost:8000/admin/replay-policies/changes/1
```

- **预览内容**：受影响范围按新旧规则的规模边界切成区间逐段对比（命中规则、
  审批节点链前后对比、是否削弱审批强度）；预计命中比例以 `replay_batches` 历史
  批次的「风险等级 × 规模」分布为样本（发布候选时按闸门内批次占比 × 分流百分比
  估算进入候选车道的比例）；不兼容规则含高风险 fail-closed 缺口、被前序规则
  完全遮蔽的无效规则、候选策略不覆盖灰度闸门。
- **高风险判定**：受影响范围触及 high 等级、任一区间削弱审批强度（节点变少/
  法定人数变少/时限变长/从需审批变为无需审批）、或引入高风险无规则缺口——
  任一成立即 `risk_class=high`，必须经他人审批；否则为标准变更，提交人可直接执行。
- **不产生部分生效**：拒绝、超时、重复提交（`request_id` 幂等）、并发审批
  （写事务串行 + 条件状态转移，单一赢家）都只是变更单状态转移；`apply` 把
  「整份策略生效 / 候选灰度发布」与变更单落定放在同一事务，任一校验失败整体
  回滚。已提交的重放批次持有自己的策略快照，全程不受变更影响。

## 审批委托

运营可以把某个审批角色在**生效/失效时间窗**内委托给受托人（`app/delegation.py`）。
受托人对指定角色节点做决定时必须携带委托 id，决定在写事务里校验委托**当前有效**
（授给本人、角色在节点允许角色内、时间窗覆盖当前时刻、未撤销；即使到期扫描
尚未运行，过期委托也会在决定时被拒绝）。

```bash
# 创建委托（epoch 秒或 ISO-8601；未到生效时间显示 pending）
curl -X POST localhost:8000/admin/replay-delegations \
  -H 'Content-Type: application/json' \
  -d '{"role": "ops-lead", "delegatee": "ops-wang", "operator": "ops-admin",
       "valid_from": "2026-09-12T00:00:00Z", "valid_to": "2026-09-19T00:00:00Z",
       "note": "主管休假一周"}'
# -> {"result": "created", "delegation_id": 12, "current": true}

curl 'localhost:8000/admin/replay-delegations?role=ops-lead'   # 列表（可按角色/受托人/状态过滤）
curl localhost:8000/admin/replay-delegations/12               # 单条（含 current/effective_status）

# 撤销（原因必填）：未达到法定人数节点上基于它的赞成票立即失效，节点重新等待；
# 已落定节点（已达人数/拒绝/跳过/超时）不会被改写
curl -X POST localhost:8000/admin/replay-delegations/12/revoke \
  -H 'Content-Type: application/json' \
  -d '{"operator": "ops-admin", "reason": "授权提前结束"}'
# -> {"result": "revoked", "affected_nodes": [{"node_id": 7, "approved_count": 0,
#     "required_approvals": 2, "missing": 2, ...}]}

# 重新激活（给新的有效时间窗）；曾经失效的票不自动复活，须重新决定
curl -X POST localhost:8000/admin/replay-delegations/12/reactivate \
  -H 'Content-Type: application/json' \
  -d '{"operator": "ops-admin",
       "valid_from": "2026-09-20T00:00:00Z", "valid_to": "2026-09-26T00:00:00Z"}'
```

- **自然到期**：replay worker 每轮先扫委托到期（先于审批超时与任务领取），
  把过窗委托置 `expired` 并令其未满足节点上的票失效，节点缺额重新等待。
- **失效票**保留为 `invalid`（带 `invalidated_at` 与审计事件），不参与计数、
  不再占位（失效后该受托人可以重新投票或承担本批其他节点）。

## 审批通知与待办分发

在既有重放审批、委托与策略变更链路上，系统按审批事件生成**站内待办**，并通过
**邮件 / webhook** 两种通道外发通知（`app/notifications.py` + `app/notif_worker.py`）。

```bash
# 1) 维护联系人目录（站内待办始终生成；email/webhook 为可选外发通道）
curl -X POST localhost:8000/admin/approval-notifications/contacts \
  -H 'Content-Type: application/json' \
  -d '{"name":"ops-wang","operator":"ops-admin","channels":["email","webhook"],
       "email":"wang@example.com","webhook_url":"https://hooks.example.com/approval"}'
curl localhost:8000/admin/approval-notifications/contacts

# 2) 此后审批链上的事件自动生成待办（无需额外开关）：
#    - 节点激活：提交即激活的节点 + 串行链逐节点激活；
#      指定角色节点的接收人 = 该角色当前有效委托的受托人 ∩ 联系人目录；
#      'any' 节点与策略变更单 = 联系人目录中除发起人外的全部活跃联系人
#    - 收到投票：法定人数未满时提醒同节点其他可审批人（已投票者不再收）
#    - 接近截止：worker 扫描，每节点/变更单至多一次（NOTIF_DEADLINE_LEAD_SECONDS）
#    - 节点拒绝 / 超时 / 批次放行 / 撤回：纯告知待办（发起人；撤回同时告知当前可审批人）
#    - 策略变更需要审批：变更单 pending 即给可审批人生成可操作待办；
#      批准/拒绝/超时/执行后给提交人纯告知待办

# 3) 看待办：看板 + 查询（接收人 / 来源 / 状态）
curl localhost:8000/admin/approval-notifications/summary
# -> {"unread":1,"read":0,"handled":0,"expired":0,"cancelled":0,
#     "delivery_failed":0,"delivery_quarantined":0}
curl 'localhost:8000/admin/approval-notifications/todos?recipient=ops-wang&status=unread'
curl 'localhost:8000/admin/approval-notifications/todos?source=batch:7'   # batch:{id} / change:{id}
curl localhost:8000/admin/approval-notifications/todos/12

# 4) 确认已读（幂等）；处理待办即回写原审批动作
curl -X POST localhost:8000/admin/approval-notifications/todos/12/read \
  -H 'Content-Type: application/json' -d '{"operator":"ops-wang"}'
curl -X POST localhost:8000/admin/approval-notifications/todos/12/act \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-wang","action":"approve","role":"ops-lead",
       "delegation_id":12,"note":"待办里点的批准"}'
# -> {"result":"handled","todo":{"status":"handled","handle_action":"approve"},
#     "decision":{"result":"approved","batch_status":"running", ...}}
# 节点动作：approve / reject（原因必填）/ skip（原因必填）；
# 变更单动作：approve / reject（原因必填）
```

- **同一事件对同一接收人至多一个待办**：`approval_notify_events.event_key` 唯一
  （如 `node:activated:{node_id}`、`deadline:node:{node_id}`、`vote:{vote_id}`、
  `change:required:{change_id}`），`approval_todos UNIQUE(event_id, recipient)`
  兜底并发与重放；worker 重复扫描、重复点击、服务重启都不产生第二个待办。
- **待办记录**：来源批次/变更单（`batch_id`/`change_id`）、节点（`node_id`）、
  接收人、事件类型与**事件版本**（`event_version`，来源上单调递增）、状态
  （unread/read/handled/expired/cancelled）、回写的动作与处理人。
- **回写即原审批**：待办处理与原审批决定在**同一个写事务**内完成——认领待办
  （条件更新 unread/read）与投票/变更决定原子提交；原决定的全部门禁（角色、
  委托当前有效、职责分离、节点状态、法定人数、变更单状态）保持不变。
  重复点击（已 handled/expired/cancelled → 409）、过期待办（来源不再待决时
  惰性落终态并 409）、并发处理（写事务串行 + 条件认领，单一赢家）都不会
  重复投票或越过审批门禁；直接在原端点投票的人，其待办由 worker 对账关闭。
- **晚到不漏**：联系人注册/重新启用、委托创建/重新激活时，为仍可操作的历史
  事件补发待办（已落定节点/终态批次与变更单不补发，纯告知事件不补发）。
- **外发投递**：站内待办始终生成；邮件/webhook 按联系人 `channels` 生成投递行，
  webhook 负载为结构化 JSON（事件、批次/节点/变更单、截止时间等）。失败按
  `NOTIF_RETRY_BASE_SECONDS * 2^(n-1)`（封顶 `NOTIF_RETRY_CAP_SECONDS`）退避重试，
  超过 `NOTIF_MAX_ATTEMPTS` 进 `quarantined`，可在
  `POST .../deliveries/{id}/requeue` 人工重投（重置计数并立即尝试）；待办关闭时
  未发出的投递同事务取消。发送器可注入（默认 noop，接真实 SMTP/HTTP 时替换
  `NotificationWorker.senders`）。
- **看板**：`GET .../summary` 给出未读、已读、已处理、过期（expired）、取消
  数量，以及外发失败/隔离数量（可按接收人过滤）；列表支持接收人、来源类型/
  id、节点、状态过滤；每条待办内嵌其投递状态（失败/隔离通道）。
- **审计**：生成（`approval_notify_event`/`approval_todo_generated`）、发送
  （`approval_delivery_sent`）、重试与隔离
  （`approval_delivery_retry_scheduled`/`..._quarantined`/`..._requeued`）、
  确认与回写（`approval_todo_read`/`..._handled`/
  `approval_action_written_back`）、待办关闭（`approval_todo_closed`）全部落
  `events`。通知审计使用独立 detail 键（`source_batch_id`/`change_id`），
  不进入重放批次按 `replay_batch_id` 过滤的审计时间线。


## 通知聚合、静默时段与升级策略

在上述通知链路之上，运营可以配置**聚合窗口、静默时段与逐级升级**（`app/notif_policy.py`）。
三者只作用于外发通道（邮件/webhook）的「何时、以什么形态发出」与升级接收人，
**站内待办始终即时、逐条生成**——任何策略都不会吞掉或延迟关键审批待办。

### 1. 通知聚合

按**接收人、来源批次、事件类型**配置聚合窗口；窗口内同组（规则+接收人+通道+同一
批次/变更单）的重复提醒先 `held` 入组，窗口关闭时每个通道合并为**一条摘要**：

```bash
# 批次 7 内 LEAD_A 的全部外发提醒，60 秒内合并
curl -X POST localhost:8000/admin/approval-notifications/aggregation-rules \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","recipient":"ops-wang","batch_id":7,
       "window_seconds":60}'
# -> {"result":"created","rule_id":1}

# 省略 recipient=全体；省略 event_type=全部事件类型；省略 batch_id=任意来源
# （不指定批次时组仍按各自批次/变更单划分，不跨审批串扰）
curl -X POST localhost:8000/admin/approval-notifications/aggregation-rules \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","event_type":"vote_received","window_seconds":120}'

curl localhost:8000/admin/approval-notifications/aggregation-rules
curl 'localhost:8000/admin/approval-notifications/aggregation-groups?batch_id=7'
# 组视图：open/flushed/cancelled + 成员明细 + 摘要投递 id/状态
curl -X POST localhost:8000/admin/approval-notifications/aggregation-rules/1/active \
  -H 'Content-Type: application/json' -d '{"operator":"ops-admin","active":false}'
```

- 窗口内：站内待办照常生成（可立即处理），邮件/webhook 为 `held`（待办视图的
  `held_channels`、投递行的 `group_id` 可查）；窗口关闭：成员投递置 `aggregated`
  （内容保留可查），另发一条摘要——邮件为合并正文，webhook 负载
  `event=aggregated_digest`，`items[]` 按原事件顺序列出每条被合并提醒。
- 窗口内来源全部落定（成员待办都已处理/取消/过期）的组直接 `cancelled`，不再外发摘要。
- 摘要本身同样受静默时段约束（静默中则延迟到时段结束发送）。

### 2. 静默时段

按**接收人/通道**配置**每日重复（UTC，支持跨午夜）**或**一次性**时间窗。窗内
**只保留站内待办**，邮件/webhook 落 `delayed`；时段结束后按**原事件顺序**发送：

```bash
# 一次性静默（epoch 秒或带时区 ISO-8601）：今晚 22:00-次日 08:00 全体、双通道
curl -X POST localhost:8000/admin/approval-notifications/quiet-schedules \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","daily":false,
       "start_at":"2026-09-12T22:00:00Z","end_at":"2026-09-13T08:00:00Z"}'

# 每日重复（UTC）：ops-wang 每晚 22:00 到次日 02:00 只静默 webhook（email 照发）
curl -X POST localhost:8000/admin/approval-notifications/quiet-schedules \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","recipient":"ops-wang","channel":"webhook",
       "daily":true,"start_time":"22:00","end_time":"02:00"}'

curl localhost:8000/admin/approval-notifications/quiet-schedules
# 停用计划：下一轮 worker 立即放行（不必等到 end_at）
curl -X POST localhost:8000/admin/approval-notifications/quiet-schedules/1/active \
  -H 'Content-Type: application/json' -d '{"operator":"ops-admin","active":false}'
```

- 每条投递记 `delayed_until`；放行按 `(ordinal,id)` 排序，`ordinal`=待办创建顺序，
  因此严格「按原事件顺序」到达邮件/webhook；多个重叠窗口取最晚结束时间。
- 静默期间待办已被处理/取消的，放行时该投递直接 `cancelled`，不补发已过时提醒；
  发送失败仍走既有指数退避/隔离链路。

### 3. 升级策略

按**节点角色**（`'any'` 匹配通配节点，`'change'` 匹配策略变更单）配置**逐级
升级接收人**与触发时限：`after_seconds`（待办生成后 N 秒）或
`before_deadline_seconds`（截止前 N 秒），可只填其一，级别按 after 单调递增。

```bash
# ops-lead 节点：10 分钟未处理升级给值班经理，30 分钟未处理再升级给 VP
curl -X POST localhost:8000/admin/approval-notifications/escalation-policies \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","name":"lead-escalation","roles":["ops-lead"],
       "levels":[
         {"recipients":["oncall-mgr"],"after_seconds":600},
         {"recipients":["vp-zhang"],"after_seconds":1800}]}'

# 也可按截止时间：距截止 10 分钟升级
curl -X POST localhost:8000/admin/approval-notifications/escalation-policies \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","name":"deadline-escalation","roles":["any"],
       "levels":[{"recipients":["oncall-mgr"],"before_deadline_seconds":600}]}'

curl localhost:8000/admin/approval-notifications/escalation-policies
curl 'localhost:8000/admin/approval-notifications/escalations?batch_id=7'
```

- 升级去重单元是 **(策略, 来源节点/变更单, 原始接收人)**：同一接收人在节点上的
  激活/投票/临期多个待办只产生**一条升级链**；每个级别至多一条 `escalated`
  待办（升级接收人同样是可操作待办，回写仍走原审批门禁——指定角色节点须凭本人
  当前有效委托，不绕过任何角色/职责分离/状态检查）。
- 每级触发、接收人、时间落 `approval_escalation_levels`，列表与待办视图
  （`escalation` 字段）可见级别进度。
- **原接收人处理后升级立即停止**：待办处理（handled）、来源落定（节点批准/拒绝/
  超时/撤回、变更单落定）、联系人停用都在同一写事务把升级链置 `stopped`
  （原因 `handled`/`source_closed`），后续级别不再触发，已升级别**尚未发出**的
  邮件/webhook（含聚合 held、静默 delayed）同事务取消；已发出的外部效果无法撤回，
  轨迹保留可查。

### 4. worker 顺序、可靠性与审计

通知 worker 每轮顺序固定：**静默放行 → 截止提醒扫描 → 待办对账 → 升级扫描 →
聚合窗口刷新 → 外发投递**。所有状态转移都是写事务（`BEGIN IMMEDIATE`，全局串行）
内的条件 UPDATE 加唯一约束（聚合组、升级链、升级级别、事件 `event_key`、
待办 `event+recipient`），通道 IO 在事务外：

- 服务重启后从库内状态继续：开放的聚合组到期即 flush、delayed 到期即放行、
  到期级别即触发，已 flushed/flushed/fired 的记录被条件更新挡下，**不重复发送**；
- 重复事件（同一 event_key）、并发 worker 扫描只可能有一个赢家；
- 聚合窗口与静默叠加时：窗口内 held → 窗口关闭生成摘要，若仍在静默窗则摘要
  delayed，时段结束按序发送，两个维度都不漏不重。

审计事件（`GET /admin/events`）：

| 阶段 | 事件类型 |
|---|---|
| 聚合 | `approval_delivery_held`、`approval_aggregation_group_flushed`、`approval_aggregation_group_cancelled`、`approval_aggregation_rule_set` |
| 静默 | `approval_delivery_delayed`、`approval_delivery_released`、`approval_quiet_schedule_set`（静默中待办关闭而取消补发为 `approval_delivery_cancelled`） |
| 升级 | `approval_escalation_armed`、`approval_escalation_fired`、`approval_escalation_stopped`、`approval_escalation_policy_set` |

看板 `GET .../summary` 增加 `delivery_held`/`delivery_delayed`/
`delivery_aggregated`/`delivery_pending`/`delivery_sent` 计数（均可按接收人过滤）。


## 通知通道路由与故障转移

在既有通知、待办与升级链路之上，运营可以为**不同事件类型**配置 `email` / `webhook` /
`inbox`（站内）三类通道的**优先级、启用状态与接收条件**，并得到**版本化快照、有序故障
转移、按时间窗口的失败熔断与自动恢复**（`app/notif_routing.py`）。

- **互斥与兼容**：从未发布过路由版本时，通知完全走旧链路（联系人 `channels` +
  聚合/静默，行为不变）。一旦发布路由版本，之后新生成的待办改走本模块；聚合/静默只
  作用于旧链路，路由链路的「何时发」由熔断/退避决定，站内待办始终即时逐条生成。
- **一个事件 × 接收人只有一条业务通知**：路由链路为每个待办入**一条**发送任务
  （`UNIQUE(event_id,recipient)`），沿有序通道计划尝试，成功一条即终态；`inbox` 是
  末位兜底（待办本身已即时落盘，走到 inbox 只记一次成功确认），外发通道全挂也不漏关键
  审批通知。已在旧链路发出的通知不会被路由链路重复（二者按是否发布过版本互斥）。

### 1) 发布路由版本（整份生效，失败保留现版）

```bash
curl -X POST localhost:8000/admin/approval-notifications/routing/versions \
  -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","note":"邮件优先、webhook 兜底、站内兜底",
       "rules":[
         {"event_type":"activated","channels":[
            {"channel":"email","priority":10,"max_attempts":2,"timeout_seconds":5,
             "condition":{"actionable":true,"risk_level":"high"}},
            {"channel":"webhook","priority":20,"max_attempts":3},
            {"channel":"inbox","priority":99}]},
         {"event_type":null,"channels":[
            {"channel":"webhook"},{"channel":"email"},{"channel":"inbox"}]}
       ],
       "breaker":{
         "email":{"failure_threshold":5,"window_seconds":60,"cooldown_seconds":30},
         "webhook":{"failure_threshold":3,"window_seconds":60,"cooldown_seconds":30}}}'
# -> {"result":"applied","version":1}
```

- `rules` 按 `event_type` 匹配（`null`/缺省/`"*"` 为缺省规则；显式规则优先；都没有时
  用内置兜底计划 `email → webhook → inbox`）。同一事件类型不可重复。
- `channels` 是该事件的**有序故障转移链**（`priority` 升序，缺省 100）；可 `enabled:false`
  暂时停用；`condition` 支持 `actionable`、`source_type`（batch/change）、
  `risk_level`（仅 batch）、`recipients`（接收人白名单）——不满足条件的通道不进入本次计划。
- 每通道可配 `timeout_seconds`（超时**立即切换**，不在本通道重试）与
  `max_attempts`（其他失败在本通道按指数退避重试的次数，**达上限才切换**；缺省取
  `NOTIF_MAX_ATTEMPTS`，inbox 恒为 1）。
- 校验失败返回 `422` 并落一条 `rejected` 版本记录，当前版本原样保留。

### 2) 发送、故障转移与熔断（worker 自动完成）

- 任务入队即固化版本与通道计划快照（地址、超时、尝试次数、条件）；worker 按
  `(ordinal,id)`（原事件顺序）领取，领取是条件 UPDATE（单赢家）。
- **熔断统计按时间窗口**：通道在 `window_seconds` 内「自最近一次成功起的连续失败数」
  （超时计失败）达到 `failure_threshold` 即 `open`，落 `notif_channel_opened` 审计。
- **open 期间不派新请求**：新任务遇到首选 open 直接从下一通道开始（切换原因
  `breaker_open`，不消耗该通道尝试次数）；计划全部通道此刻不可用时，任务等待最近一个
  open 通道的冷却时刻（`pending`，不隔离、不丢通知）。
- **自动恢复**：open 冷却 `cooldown_seconds` 后转 `half_open`，只放**一条恢复探针**
  （条件更新保证唯一）；探针成功 → `closed` 重新接流量（`notif_channel_recovered`），
  探针失败 → 立即回 `open`。
- 每次尝试落 `notif_send_attempts`（成功/失败/超时、耗时、探针标记）；每次通道选择与
  切换落 `notif_channel_switches`（含原因与详情）；最终 `sent` / `quarantined`。
- 全部外发通道耗尽且计划无 inbox 兜底时任务 `quarantined`，可人工
  `POST .../routing/tasks/{id}/requeue` 从首选通道开**新一轮**（round+1，失败重新计数）。

### 3) 版本发布 / 回滚不影响已入队任务

```bash
# 发布 v2（只影响之后入队的任务；在途任务继续按 v1 快照）
curl -X POST .../routing/versions -d '{"operator":"ops-b","rules":[...]}'
# 回滚到上一生效版本（原因必填），同样只影响之后入队的任务
curl -X POST .../routing/rollback -H 'Content-Type: application/json' \
  -d '{"operator":"ops-b","reason":"紧急止损"}'
# -> {"result":"rolled_back","version":1}
```

### 4) 管理查询与手工干预

```bash
# 当前版本 + 配置快照 + 是否可回滚
curl .../routing/current
# 版本历史（applied / rollback / rejected）
curl .../routing/versions
# 待发送/在途/已发送/隔离/取消任务（?history=true 带尝试与切换历史）
curl '.../routing/tasks?status=sent&route_version=1&event_type=activated'
curl .../routing/tasks/12
# 通道健康：state(closed/open/half_open)、窗口连续失败、冷却/探针
curl .../routing/channels/health
# 切换历史 / 每次尝试
curl '.../routing/switches?reason=consecutive_failures'
curl '.../routing/attempts?channel=webhook&result=timeout'
# 启用/停用通道、调整熔断参数，或排障后强制复位 closed
curl -X POST .../routing/channels/webhook/state -H 'Content-Type: application/json' \
  -d '{"operator":"ops-admin","enabled":true,"reset":true}'
```

> `inbox` 为站内兜底通道，不可停用、不会被熔断。待办被处理/对账关闭、联系人停用或升级
> 停止时，其未发出的发送任务在同一事务置 `cancelled`（已发出的外部效果轨迹保留）。

审计事件：`notif_route_published` / `notif_route_rolled_back` / `notif_route_rejected`、
`notif_send_task_enqueued` / `notif_send_skipped` / `notif_send_sent` /
`notif_send_retry_scheduled` / `notif_send_quarantined` / `notif_send_requeued` /
`notif_send_cancelled` / `notif_send_awaiting_channels` / `notif_send_task_recovered`、
`notif_channel_switched` / `notif_channel_opened` / `notif_channel_probe_started` /
`notif_channel_probe_failed` / `notif_channel_recovered` / `notif_channel_state_set`。
看板 `GET .../summary` 增加 `route_pending`/`route_in_flight`/`route_sent`/
`route_quarantined`/`route_cancelled` 计数（可按接收人过滤）；待办视图内嵌 `route_task`
（计划、每次尝试与切换）。


## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_PATH` | `/data/gateway.db` | SQLite 路径（容器挂卷持久化） |
| `KEYS_FILE` | `/config/keys.json` | 密钥配置文件（仅首次启动引导用；之后以库中最后成功配置为准，轮换走 `/admin/keys/rotate`） |
| `RETRY_BASE_SECONDS` | `5` | 重试间隔基数（指数退避） |
| `RETRY_CAP_SECONDS` | `300` | 重试间隔上限 |
| `MAX_ATTEMPTS` | `5` | 连续失败上限，超过进隔离队列 |
| `WORKER_POLL_INTERVAL` | `1` | worker 轮询间隔（秒） |
| `SIGNATURE_TOLERANCE_SECONDS` | `300` | 签名时间戳容差（防重放） |
| `REPLAY_APPROVAL_TIMEOUT_SECONDS` | `3600` | 内置默认策略中高风险批次的审批超时（自定义策略后由各节点的 `timeout_seconds` 取代）；超时未决由 worker 自动释放（取消）批次 |
| `REPLAY_POLICY_CHANGE_TTL_SECONDS` | `3600` | 策略变更单的审批超时：到期未决/未执行的变更由 worker 置为 `expired`，不能再生效 |
| `NOTIF_RETRY_BASE_SECONDS` | `5` | 审批通知（邮件/webhook）发送失败的重试间隔基数（指数退避） |
| `NOTIF_RETRY_CAP_SECONDS` | `300` | 审批通知重试间隔上限 |
| `NOTIF_MAX_ATTEMPTS` | `5` | 审批通知连续发送失败上限，超过进隔离队列（可人工 requeue） |
| `NOTIF_DEADLINE_LEAD_SECONDS` | `300` | 审批截止前多少秒生成「即将到期」待办（每节点/变更单至多一次） |
| `NOTIF_BREAKER_WINDOW_SECONDS` | `60` | 通道熔断失败统计窗口（秒）：窗口内自上次成功起的连续失败达阈值即熔断 |
| `NOTIF_BREAKER_FAILURE_THRESHOLD` | `5` | 窗口内连续失败多少次熔断（可在发布路由时按通道覆盖） |
| `NOTIF_BREAKER_COOLDOWN_SECONDS` | `30` | 熔断 open 后多久允许一条 half_open 恢复探针 |
| `NOTIF_CHANNEL_TIMEOUT_SECONDS` | `10` | 通道发送超时兜底（秒，可按通道 `timeout_seconds` 覆盖）；超时立即切换下一通道 |
| `RUN_WORKER` | `true` | 是否在本进程跑后台 worker |

## 设计要点

- **副作用 exactly-once**：业务处理只负责把副作用写进 `outbox`（与状态变更同事务）；
  派发器执行前先查下游是否已应用该幂等键。即使在「已发送、未标记」之间崩溃重启，
  下游凭幂等键去重，效果也只应用一次。接真实下游时把 `IdempotentSink` 换成
  「带 `Idempotency-Key` 头的 HTTP 调用」即可。
- **可追溯续跑**：每条 delivery 带 `checkpoint`（JSON 位置快照）和完整 `attempts` 历史；
  人工选定版本后从该位置继续，每一步都落在 `events` 审计表。
- **只有被选中的版本产生外部效果**：冲突冻结期间整组的待派发副作用暂停；
  人工选定后，未选中版本滞留在 outbox 的副作用在同一事务里置为 `cancelled`
  （内容保留可查），派发器只放行「done 且未冻结」版本——旧版本不会再对外产生效果。
- **隔离不蔓延**：隔离是 per-delivery 的状态，worker 拉取时天然跳过，其他编号照常处理。
- **重放即审计**：重放的每个动作（创建/节点批准/拒绝/跳过/超时/激活/批次放行/
  撤回/暂停/继续/取消/执行/被配额或顺序挡下/失败/人工重试）都写 `events` 并带
  `replay_batch_id`/`replay_task_id`，一批重放的完整轨迹一次查全；重放任务只引用
  落盘原文（delivery_id），不复制内容，原始版本永不改写。
- **审批策略快照隔离**：批次提交时把所采用策略的版本与解析结果（规则、模式、
  节点规格、灰度车道与分流规则）随批次落盘，审批节点行一次性生成；策略版本化演进、
  灰度发布/暂停/回滚只影响新提交，已提交批次的审批链、角色与截止时间全程不变，
  事后可对照快照追溯每一次决定。
- **灰度分流确定性**：分流桶是 `(风险等级, 分流键)` 的稳定哈希（分流键=request_id
  或本批投递 id 集合），与提交时机和进程无关；选版本在批次写事务内完成，发布/
  暂停/回滚/整份切换与批次提交串行，稳定指针与发布单的更新对每个批次要么是完整旧
  状态、要么是完整新状态，不存在「无版本」或跨版本节点链。
- **并发配额实时推导**：批次占用量 = 该批 `processing` 任务数，领取与复核在同一
  写事务里完成（SQLite 写事务串行，多副本也不会超领）；不维护任何计数器，
  因此暂停/取消/崩溃重启都不存在「忘了释放」的路径。
- **通道路由快照隔离**：发送任务入队时固化路由版本与有序通道计划（地址/超时/尝试
  次数/接收条件），随后发布新版本或回滚只推进 `notif_route_current` 指针，只影响之后
  入队的任务；在途任务始终按自己的快照选道，不存在「跨版本混搭」或无计划窗口。
- **熔断零计数器**：通道熔断状态（closed/open/half_open）只存状态行，「窗口内连续
  失败数」实时查只增的 `notif_send_attempts`（找到窗口内最近一次成功，数其后失败），
  因此不存在计数器漂移/漏复位；half_open 探针用条件 UPDATE 归属，保证并发下唯一探针。
- **故障转移与幂等**：一个事件×接收人只有一条发送任务（UNIQUE 兜底重启/重复扫描/
  并发），通道链成功一条即终态，跨通道不重复通知；超时由进程级线程池强杀并立即切换，
  其他失败在本通道退避重试到 `max_attempts` 才切换；熔断中跳过的通道不消耗尝试次数，
  计划全不可用时等待恢复而非隔离，inbox 末位兜底保证关键通知不因外发全挂而漏发。
- **老库就地升级**：首次以新版本打开旧库时自动给 `outbox` 补 `replay_task_id` 列、
  给 `replay_batches`/`replay_tasks` 补 `max_concurrency`/`delivery_created_at`/
  `blocked_reason` 列（存量任务的排序键从 deliveries 回填），给 `replay_batches`
  补 `risk_level`/`approval_note`/`approval_status`/`approver`/`approval_reason`/
  `approved_at`/`approval_deadline` 列（存量批次视为普通风险、无需审批，状态不变），
  再补 `policy_version`/`policy_snapshot` 列并新建 `replay_policy_versions`/
  `replay_approval_nodes` 表；老库中仍待决的批次自动合成一个内置默认节点
  （沿用原截止时间），升级后可照常批准/拒绝/超时，无需手工迁移。灰度功能再打开时
  补 `policy_lane`/`rollout_id`/`rollout_seq` 列与 `replay_policy_stable`/
  `replay_policy_releases` 表：已有 applied 策略时按最近版本为每个风险等级回填稳定
  指针（行为与升级前一致），从未提交过策略则继续走内置默认；老批次一律视为稳定车道。
  聚合/静默/升级功能再打开时新建 `approval_aggregation_rules`、
  `approval_notification_groups(_members)`、`approval_quiet_schedules`、
  `approval_escalation_policies`、`approval_escalations(_levels)` 表，
  并给 `approval_notification_deliveries` 补 `group_id`/`delayed_until`/`ordinal`
  列（存量投递的 ordinal 回填为投递 id）、给 `approval_todos` 补 `open_at`
  （回填为 created_at）；升级前不存在规则/计划/策略，存量通知行为与升级前完全一致。
  通道路由功能再打开时新建 `notif_route_versions`/`notif_route_current`/
  `notif_send_tasks`/`notif_send_attempts`/`notif_channel_switches`/
  `notif_channel_state` 表；当前路由版本指针初始为 NULL，因此发布路由版本之前所有通知
  仍走旧链路，存量行为完全不变，首次发布后新通知才改走版本化路由/熔断/故障转移。

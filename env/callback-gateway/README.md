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
                │   重放副作用走同一 outbox 幂等链路                   │
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
| 每个节点记录指定角色、实际审批人、截止时间 | `replay_approval_nodes`：role / decided_by / decided_role / deadline；指定角色非 `any` 时审批人须声明匹配角色 |
| 审批人不能重复承担同一批次的多个节点 | 节点决定时同事务检查本批其他节点未被该审批人决定过（含批准/跳过/拒绝），违反 403 |
| 任一节点拒绝即终止批次，全部批准才放行 | 节点拒绝 → 批次 `rejected`、未执行任务整体取消；全部节点批准（或有理由跳过）→ 批次才进 `running`，worker 与 outbox 派发闸门口径不变 |
| 批次保存提交时的策略快照 | `replay_batches.policy_version` + `policy_snapshot`（规则/模式/节点规格）；策略更新只影响新提交的批次 |
| 详情展示当前节点、剩余节点与超时状态 | 批次详情 `approval`：`nodes`（各节点角色/审批人/截止时间/超时）、`current_node_ids`、`remaining_node_ids`、`expired_on_time` |
| 批准/拒绝/跳过/超时/策略变更都可审计 | `replay_approval_node_approved`/`rejected`/`skipped`/`expired`/`activated` + 既有批次级事件 + `replay_policy_applied`/`rejected`；节点可带原因跳过（视为满足，留痕） |
| 审批结果/拒绝原因/超时释放/批准后执行全部可审计 | `replay_batch_approved` / `replay_batch_rejected`（含 reason）/ `replay_batch_approval_expired` + 既有执行事件；批次详情含 `approval`（状态、发起人、审批人、批准时间、拒绝原因、截止时间） |
| 重复提交或重复批准不会执行两次 | `request_id` 重复提交返回原批次；节点决定、批准/拒绝/超时/取消都是带状态守卫的条件更新（写事务串行），重复决定返回 409，不产生第二套任务、节点与事件 |
| 批次级并发配额 | 提交时 `max_concurrency` 指定整批最多同时处理多少条；占用=本批 `processing` 任务数，领取时在占位事务里实时推导复核，见 `app/replay.py` |
| 同一编号多条历史版本按 created_at 先后执行 | 任务落盘快照 `delivery_created_at` 作排序键；前序未进终态（done/failed/cancelled）时条件更新拒绝领取后一条 |
| 批次详情显示占用/等待/每条阻塞原因 | `GET /admin/replays/{id}`：`in_flight`（当前占用）、`waiting`（等待数量）、每条任务的 `blocked_reason`（实时计算） |
| 暂停/取消/重启后配额正确回收 | 配额无计数器：任务离开 `processing`（完成/失败/取消/recover 退回）槽位即释放，不会永久卡住 |
| 额度不足/顺序冲突/失败重试/取消都有状态与审计 | `replay_task_blocked`（原因变化才写，轮询不刷表）+ 既有 `replay_task_retry_scheduled`/`replay_task_failed`/`replay_batch_cancelled` 事件 |
| 重启后未完成任务从上次位置继续 | 启动时 `ReplayWorker.recover()` 把卡在 processing 的任务退回 pending，attempts/checkpoint 都在库里 |
| 失败单独重试、不阻塞其他编号 | 任务级指数退避，超限标记 `failed`，`POST /admin/replays/tasks/{id}/retry` 单条重试 |
| 重放副作用与正常处理同样的幂等保护 | 重放副作用落同一 `outbox`（`replay_task_id` 标识），幂等键以 `replay:{task_id}` 为作用域，同一派发器 + 下游去重 |
| 每次重放的完整审计记录 | `GET /admin/replays/{id}/events`（可按单条任务过滤） |
| Docker 部署 | `Dockerfile` + `docker-compose.yml` |

## 快速开始

```bash
cp keys.example.json keys.json   # 修改里面的 secret
docker compose up --build
```

本地开发：

```bash
pip install -r requirements-dev.txt
python -m pytest tests/          # 94 个端到端测试
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
curl -X POST localhost:8000/admin/replay-policies \
  -H 'Content-Type: application/json' \
  -d '{
    "operator": "ops-li",
    "policy": {"rules": [
      {"name": "high-large", "risk_level": "high", "min_size": 10, "mode": "serial",
       "nodes": [
         {"role": "ops-lead", "timeout_seconds": 1800},
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

- **节点链随批次落盘（策略快照）**：提交时解析出的规则与节点规格保存在
  `replay_batches.policy_snapshot`（连同 `policy_version`），审批节点行一次性
  生成——之后策略再更新也**不改变已提交的批次**，只影响新提交。
- **fail closed**：已有生效策略时，高风险批次若没有任何规则匹配，提交直接被
  拒绝（`422 no_applicable_policy`），不会静默降低审批要求；普通批次无匹配
  规则则直接运行。
- **每个节点**记录指定角色（`role`，`any` 表示任何非发起人）、实际审批人
  （`decided_by`/`decided_role`）与截止时间（`deadline`，激活时起算）。
  指定角色非 `any` 的节点，审批人必须声明匹配的角色才能决定。
- **职责分离**：审批人不能是批次发起人，也不能重复承担同一批次的多个节点
  （批准/跳过/拒绝合计只算一次，违反返回 403）。
- **节点级操作**：
  ```bash
  # 批准 / 拒绝（必填原因）/ 跳过（必填原因，视为该节点已满足，留痕可追溯）
  curl -X POST localhost:8000/admin/replays/3/nodes/7/approve \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-wang", "role": "ops-lead", "note": "现场已核对"}'
  curl -X POST localhost:8000/admin/replays/3/nodes/8/reject \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-zhao", "role": "finance-controller", "reason": "影响面评估不通过"}'
  curl -X POST localhost:8000/admin/replays/3/nodes/8/skip \
    -H 'Content-Type: application/json' \
    -d '{"operator": "ops-zhao", "role": "finance-controller",
         "reason": "主管休假，值班经理代签已电话确认"}'
  ```
  任一节点**拒绝**或**超时**即终止整个批次（未执行任务整体取消）；所有节点
  **批准**（或被有理由**跳过**）后批次才进入 `running`，worker 方可领取。
  批次级 `/approve`、`/reject` 入口在恰好一个节点待决时仍然可用（单节点批次
  行为与之前一致）。
- **批次详情**的 `approval` 展示：`nodes`（每个节点的角色、状态、实际审批人、
  截止时间、是否已超时）、`current_node_ids`（当前待决节点）、
  `remaining_node_ids`（剩余节点）、`policy_version` 与整体超时状态。
- **审计**：节点批准/拒绝/跳过/超时/激活（`replay_approval_node_*`）、批次放行/
  拒绝/超时释放（`replay_batch_approved`/`rejected`/`approval_expired`）、策略
  变更（`replay_policy_applied`/`rejected`）全部落 `events`，可按批次一次查全。



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
  节点规格）随批次落盘，审批节点行一次性生成；策略版本化演进只影响新提交，
  已提交批次的审批链、角色与截止时间全程不变，事后可对照快照追溯每一次决定。
- **并发配额实时推导**：批次占用量 = 该批 `processing` 任务数，领取与复核在同一
  写事务里完成（SQLite 写事务串行，多副本也不会超领）；不维护任何计数器，
  因此暂停/取消/崩溃重启都不存在「忘了释放」的路径。
- **老库就地升级**：首次以新版本打开旧库时自动给 `outbox` 补 `replay_task_id` 列、
  给 `replay_batches`/`replay_tasks` 补 `max_concurrency`/`delivery_created_at`/
  `blocked_reason` 列（存量任务的排序键从 deliveries 回填），给 `replay_batches`
  补 `risk_level`/`approval_note`/`approval_status`/`approver`/`approval_reason`/
  `approved_at`/`approval_deadline` 列（存量批次视为普通风险、无需审批，状态不变），
  再补 `policy_version`/`policy_snapshot` 列并新建 `replay_policy_versions`/
  `replay_approval_nodes` 表；老库中仍待决的批次自动合成一个内置默认节点
  （沿用原截止时间），升级后可照常批准/拒绝/超时，无需手工迁移。

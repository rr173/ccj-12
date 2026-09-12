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
                └────────────────────────────────────────────────────┘
```

## 需求 → 实现对照

| 需求 | 实现 |
|---|---|
| 可轮换签名密钥，旧钥匙过渡期可用 | 密钥环 JSON：`active` + `retired`（带 `grace_until`），见 `app/security.py` |
| 同编号同内容只处理一次 | `deliveries UNIQUE(external_id, content_hash)`，重复投递返回 `duplicate`，见 `app/ingest.py` |
| 同编号不同内容冻结、人工比较选择、不覆盖 | 新版本独立落盘并整组 `frozen=1`，开冲突单；任何版本都不会被覆盖，见 `app/ingest.py`、`app/admin.py` |
| 落盘后才 ACK | 所有写入单事务提交（WAL + `synchronous=FULL`），提交后才返回响应 |
| 失败按间隔重试、连续失败隔离、不堵别的编号 | 指数退避 `base*2^(n-1)`；超限 `quarantined`；worker 按条拉取，隔离只影响该条，见 `app/worker.py` |
| 重启/重复投递副作用不做两次 | outbox 幂等键 + 派发前检查下游是否已应用该键，见 `app/worker.py`、`app/handlers.py` |
| 人工选定后从可追溯位置继续 | 选定版本保留 `checkpoint` 与 `attempts` 历史，解冻续跑；全程写 `events` 审计 |
| 查询签名/冲突/重试/处置记录 | `GET /admin/events`（只增不删的审计表） |
| Docker 部署 | `Dockerfile` + `docker-compose.yml` |

## 快速开始

```bash
cp keys.example.json keys.json   # 修改里面的 secret
docker compose up --build
```

本地开发：

```bash
pip install -r requirements-dev.txt
python -m pytest tests/          # 13 个端到端测试
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

## 密钥轮换

1. 生成新密钥，加入 `keys.json` 为 `active`；旧密钥改为 `retired` 并设 `grace_until`（过渡期截止）。
2. 通知接入方切到新 `kid` 签发。过渡期内旧钥匙签名仍被接受（审计中会记录使用的 `kid`）。
3. 过渡期结束后旧钥匙一律拒绝（`retired_key_grace_expired`）。

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

## 配置（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| `DATABASE_PATH` | `/data/gateway.db` | SQLite 路径（容器挂卷持久化） |
| `KEYS_FILE` | `/config/keys.json` | 密钥环文件 |
| `RETRY_BASE_SECONDS` | `5` | 重试间隔基数（指数退避） |
| `RETRY_CAP_SECONDS` | `300` | 重试间隔上限 |
| `MAX_ATTEMPTS` | `5` | 连续失败上限，超过进隔离队列 |
| `WORKER_POLL_INTERVAL` | `1` | worker 轮询间隔（秒） |
| `SIGNATURE_TOLERANCE_SECONDS` | `300` | 签名时间戳容差（防重放） |
| `RUN_WORKER` | `true` | 是否在本进程跑后台 worker |

## 设计要点

- **副作用 exactly-once**：业务处理只负责把副作用写进 `outbox`（与状态变更同事务）；
  派发器执行前先查下游是否已应用该幂等键。即使在「已发送、未标记」之间崩溃重启，
  下游凭幂等键去重，效果也只应用一次。接真实下游时把 `IdempotentSink` 换成
  「带 `Idempotency-Key` 头的 HTTP 调用」即可。
- **可追溯续跑**：每条 delivery 带 `checkpoint`（JSON 位置快照）和完整 `attempts` 历史；
  人工选定版本后从该位置继续，每一步都落在 `events` 审计表。
- **隔离不蔓延**：隔离是 per-delivery 的状态，worker 拉取时天然跳过，其他编号照常处理。

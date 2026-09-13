"""服务装配：接入端点 + 后台 worker 生命周期。

分层：
- 接入（本文件 POST /callbacks）：验签 -> 落盘 -> 确认，不做任何业务处理；
- 处理（worker.py 后台任务）：重试、隔离、副作用派发；
- 人工处置（admin.py）：冲突比较/选定、隔离重投、审计查询；
- 业务重放（replay.py）：筛选预览 -> 批量提交 -> 多级审批（replay_policy.py 策略
  按风险等级与批次规模生成串行/并行审批节点，每节点带允许角色与法定人数；
  delegation.py 的角色委托须在决定时当前有效，全部节点满足才放行）-> 暂停/继续/
  取消 -> 审计查询。
"""
from __future__ import annotations

import asyncio
import contextlib
import time

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from . import audit
from .admin import create_admin_router
from .config import Settings
from .db import Database
from .delegation import create_delegation_router
from .ingest import ingest
from .keyconfig import KeyConfigStore, KeyRotationService
from .notif_worker import NotificationWorker
from .notif_policy import create_notif_policy_router
from .notif_routing import configure_routing, create_routing_router
from .notifications import create_notifications_router
from .replay import ReplayWorker, create_replay_router
from .replay_policy import create_policy_router
from .replay_policy_gate import create_policy_gate_router
from .replay_rollout import create_rollout_router
from .security import KeyRingManager
from .worker import Worker


def create_app(settings: Settings | None = None) -> FastAPI:
    settings = settings or Settings.from_env()
    db = Database(settings.database_path)
    # 启动加载最后一次成功应用的密钥配置（库里没有则用 KEYS_FILE 引导为第 1 版）
    key_store = KeyConfigStore(db)
    keyring = KeyRingManager(
        key_store.bootstrap(settings.keys_file, settings.signature_tolerance_seconds))
    keys = KeyRotationService(key_store, keyring)
    worker = Worker(db, settings)
    replay_worker = ReplayWorker(db, settings)
    notif_worker = NotificationWorker(db, settings)
    # 通知通道路由：在业务事务内入队时读取的默认熔断/超时配置
    configure_routing(settings)

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        # 重启恢复：上次未完成（卡在 processing）的重放任务退回待处理，从上次位置继续
        replay_worker.recover()
        # 通知路由：卡在 in_flight 的发送任务退回 pending（尝试/熔断状态都在库里）
        notif_worker.recover()
        tasks = []
        if settings.run_worker:
            tasks.append(asyncio.create_task(worker.run_forever()))
            tasks.append(asyncio.create_task(replay_worker.run_forever()))
            tasks.append(asyncio.create_task(notif_worker.run_forever()))
        yield
        worker.stop()
        replay_worker.stop()
        notif_worker.stop()
        for task in tasks:
            await task
        db.close()

    app = FastAPI(title="callback-gateway", lifespan=lifespan)
    app.state.db = db
    app.state.keyring = keyring
    app.state.keys = keys
    app.state.worker = worker
    app.state.replay_worker = replay_worker
    app.state.notif_worker = notif_worker
    app.state.settings = settings

    @app.post("/callbacks")
    async def receive_callback(request: Request):
        body = await request.body()
        headers = {k.lower(): v for k, v in request.headers.items()}
        external_id = headers.get("x-callback-id")
        if not external_id:
            return JSONResponse(status_code=400, content={"error": "missing X-Callback-Id header"})

        # 1) 验签（无论成败都写审计，失败记录独立事务提交，不随请求失败丢失）
        ok, reason, kid = keyring.verify(headers.get("x-signature"), body)
        with db.tx() as cur:
            audit.record(cur, "signature_ok" if ok else "signature_fail",
                         external_id, None, {"kid": kid, "reason": reason})
        if not ok:
            return JSONResponse(status_code=401, content={"error": "signature_verification_failed", "reason": reason})

        # 2) 幂等落盘（事务提交后才会走到这里）
        result = ingest(db, external_id, body, headers)

        # 3) 落盘成功，给出明确确认
        status_code = 200 if result["result"] == "duplicate" else 202
        return JSONResponse(status_code=status_code, content=result)

    @app.get("/healthz")
    def healthz():
        return {"status": "ok", "time": time.time()}

    app.include_router(create_admin_router(db, keys))
    app.include_router(create_replay_router(db, settings))
    app.include_router(create_policy_router(db, settings.replay_approval_timeout_seconds))
    app.include_router(create_rollout_router(db, settings.replay_approval_timeout_seconds))
    app.include_router(create_policy_gate_router(
        db, settings.replay_approval_timeout_seconds,
        settings.replay_policy_change_ttl_seconds))
    app.include_router(create_delegation_router(db))
    app.include_router(create_notifications_router(
        db, settings, lambda: notif_worker.senders))
    app.include_router(create_notif_policy_router(db, settings))
    app.include_router(create_routing_router(
        db, settings, lambda: notif_worker.senders))
    return app


# 启动方式：uvicorn app.main:create_app --factory
# （不在导入期建 app，避免测试导入时误用默认路径建库）

"""服务装配：接入端点 + 后台 worker 生命周期。

分层：
- 接入（本文件 POST /callbacks）：验签 -> 落盘 -> 确认，不做任何业务处理；
- 处理（worker.py 后台任务）：重试、隔离、副作用派发；
- 人工处置（admin.py）：冲突比较/选定、隔离重投、审计查询。
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
from .ingest import ingest
from .keyconfig import KeyConfigStore, KeyRotationService
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

    @contextlib.asynccontextmanager
    async def lifespan(app: FastAPI):
        task = None
        if settings.run_worker:
            task = asyncio.create_task(worker.run_forever())
        yield
        if task is not None:
            worker.stop()
            await task
        db.close()

    app = FastAPI(title="callback-gateway", lifespan=lifespan)
    app.state.db = db
    app.state.keyring = keyring
    app.state.keys = keys
    app.state.worker = worker
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
    return app


# 启动方式：uvicorn app.main:create_app --factory
# （不在导入期建 app，避免测试导入时误用默认路径建库）

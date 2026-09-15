"""编排后台循环：到期缺失事件扫描、BLOCKED 节点退避重试、副作用幂等派发。"""
from __future__ import annotations

import asyncio
import logging
import time

from ..handlers import IdempotentSink
from .engine import OrchestrationEngine

log = logging.getLogger("gateway.orchestration.worker")


class OrchestrationWorker:
    def __init__(self, engine: OrchestrationEngine, db, poll_interval: float = 1.0,
                 sink=None, clock=time.time):
        self.engine = engine
        self.db = db
        self.poll_interval = poll_interval
        self.clock = clock
        # 复用网关既有的幂等下游替身；真实部署替换为自己的下游适配器（须幂等）
        self.sink = sink or IdempotentSink(db, clock)
        self._stop = asyncio.Event()

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("orchestration worker iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self):
        self._stop.set()

    def run_once(self):
        self.engine.scan_deadlines()
        self.engine.retry_due_blocked()
        self.engine.dispatch_pending_effects(self.sink)

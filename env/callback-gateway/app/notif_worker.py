"""审批通知后台 worker：截止提醒扫描 -> 待办对账 -> 外发投递（重试/隔离）。

- 接近截止时间的活动节点/待决变更单生成 deadline_approaching 事件（幂等，每来源一次）；
- 已失效的可操作待办（接收人已投票、节点落定、批次/变更进入终态）关闭，未发出投递取消；
- pending/failed 的邮件/webhook 投递按指数退避发送，超过 NOTIF_MAX_ATTEMPTS 隔离。

与主 worker、replay worker 同样的单连接写事务串行模型：所有状态转移都是
条件更新，重复运行/重启不产生第二次效果。通道调用（邮件/webhook IO）在事务外，
可通过注入自定义 sender 测试失败重试与隔离。
"""
from __future__ import annotations

import asyncio
import logging
import time

from . import notifications as notif
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications.worker")


def _noop_email(address: str, subject: str, body: str) -> None:
    """默认邮件出口：无外发依赖时直接成功（真实部署替换为 SMTP/HTTP 邮件服务）。"""
    log.info("email to=%s subject=%s", address, subject)


def _noop_webhook(address: str, payload: dict) -> None:
    """默认 webhook 出口：占位成功（真实部署替换为带签名的 HTTP POST）。"""
    log.info("webhook url=%s event=%s", address, payload.get("event"))


class NotificationWorker:
    def __init__(self, db: Database, settings: Settings, senders: dict | None = None,
                 clock=time.time):
        self.db = db
        self.settings = settings
        self.senders = senders or {notif.CHANNEL_EMAIL: _noop_email,
                                   notif.CHANNEL_WEBHOOK: _noop_webhook}
        self.clock = clock
        self._stop = asyncio.Event()

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("notification worker iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(),
                                       self.settings.worker_poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self):
        self._stop.set()

    def run_once(self):
        now = self.clock()
        # 1) 接近截止时间：为活动节点/待决变更单补发提醒事件（每来源至多一次）
        notif.emit_deadline_events(
            self.db, now, self.settings.notif_deadline_lead_seconds)
        # 2) 待办对账：关闭已失效的可操作待办（取消其未发出投递）
        notif.sweep_stale_todos(self.db, now)
        # 3) 外发投递：到期重试、失败退避、超限隔离
        notif.dispatch_due(self.db, self.senders, self.settings, now)

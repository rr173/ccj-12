"""审批通知后台 worker：

  静默放行 -> 截止提醒 -> 待办对账 -> 升级扫描 -> 聚合窗口刷新 -> 外发投递。

- release_delayed：静默时段结束，把 delayed 投递按 (ordinal,id)=原事件顺序放回发送队列；
  仍处于另一静默窗的仅推进预计放行时间；
- 接近截止时间的活动节点/待决变更单生成 deadline_approaching 事件（幂等，每来源一次）；
- 已失效的可操作待办（接收人已投票、节点落定、批次/变更进入终态）关闭，未发出投递取消；
- scan_escalations：开放的可操作待办到达升级时限时，按策略逐级通知升级接收人
  （原接收人处理/来源落定后停止）；
- flush_due_groups：到期聚合组合并为一条摘要投递（来源全部落定的组取消）；
- pending/failed 的邮件/webhook 投递按指数退避发送，超过 NOTIF_MAX_ATTEMPTS 隔离。

与主 worker、replay worker 同样的单连接写事务串行模型：所有状态转移都是条件更新，
重复运行/重启不产生第二次效果。聚合 flush 与静默放行先于 dispatch，因此摘要/延迟通知
在同一轮即可按序发出。通道调用（邮件/webhook IO）在事务外，可注入自定义 sender。
"""
from __future__ import annotations

import asyncio
import logging
import time

from . import notif_policy
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
        # 0) 静默时段结束：delayed 按原事件顺序放回发送队列（先于一切生成/发送）
        notif_policy.release_delayed(self.db, now)
        # 1) 接近截止时间：为活动节点/待决变更单补发提醒事件（每来源至多一次）
        notif.emit_deadline_events(
            self.db, now, self.settings.notif_deadline_lead_seconds)
        # 2) 待办对账：关闭已失效的可操作待办（取消其未发出投递，停止其升级链）
        notif.sweep_stale_todos(self.db, now)
        # 3) 升级扫描：到时限的开放待办逐级通知升级接收人（幂等，每级一次）
        notif_policy.scan_escalations(self.db, now)
        # 4) 聚合窗口：到期组合并为一条摘要（全部落定的组取消）
        notif_policy.flush_due_groups(self.db, now)
        # 5) 外发投递：到期重试、失败退避、超限隔离（按 ordinal 原事件顺序）
        notif.dispatch_due(self.db, self.senders, self.settings, now)

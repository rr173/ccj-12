"""配置：全部可通过环境变量覆盖，便于 Docker 部署。"""
from __future__ import annotations

import os
from dataclasses import dataclass, field


@dataclass
class Settings:
    # 数据库文件路径（容器内挂卷持久化）
    database_path: str = "/data/gateway.db"
    # 签名密钥环文件（JSON，含 active / retired 密钥）
    keys_file: str = "/config/keys.json"
    # 处理失败后的重试基数（秒），实际间隔 = base * 2^(attempts-1)，封顶 retry_cap
    retry_base_seconds: float = 5.0
    retry_cap_seconds: float = 300.0
    # 连续失败多少次后进入隔离队列
    max_attempts: int = 5
    # 处理 worker 轮询间隔（秒）
    worker_poll_interval: float = 1.0
    # 签名时间戳容差（秒），防重放
    signature_tolerance_seconds: int = 300
    # 高风险重放批次的审批超时（秒）：提交后超时仍未由他人批准，自动释放（取消）批次
    replay_approval_timeout_seconds: float = 3600.0
    # 策略变更单的审批超时（秒）：到期未决/未执行的变更置为 expired，不能再生效
    replay_policy_change_ttl_seconds: float = 3600.0
    # 审批通知：外发（邮件/webhook）失败后的指数退避重试基数/上限/最大次数（超限隔离）
    notif_retry_base_seconds: float = 5.0
    notif_retry_cap_seconds: float = 300.0
    notif_max_attempts: int = 5
    # 审批截止前多少秒生成 deadline_approaching 提醒事件（每节点/变更单至多一次）
    notif_deadline_lead_seconds: float = 300.0
    # 通知通道路由/熔断：失败统计窗口、窗口内连续失败熔断阈值、熔断冷却（半开探针）秒数、
    # 发送超时兜底秒数（通道级 timeout_seconds 缺省值；发件调用另被工作线程强杀超时）
    notif_breaker_window_seconds: float = 60.0
    notif_breaker_failure_threshold: int = 5
    notif_breaker_cooldown_seconds: float = 30.0
    notif_channel_timeout_seconds: float = 10.0
    # 外部通道回执与送达确认：发送成功后多久没有外部回执算「待确认」（缺省策略，可由
    # /admin/receipt-policy 在线调整）；失败回执（bounced/complained/expired）或超时后
    # 按当前策略自动沿通道计划重试（最多 confirm_max_retries 次）或转人工
    receipt_confirm_timeout_seconds: float = 3600.0
    receipt_confirm_max_retries: int = 2
    # 外部回执入口（POST /receipts/{channel}）的引导签名密钥（按通道覆盖用
    # RECEIPT_EMAIL_SECRET / RECEIPT_WEBHOOK_SECRET）；未配置时须先由管理员登记密钥，
    # 回执验签一律 fail-closed
    receipt_email_secret: str | None = None
    receipt_webhook_secret: str | None = None
    # 是否随服务启动后台 worker（测试时可关闭，手动驱动）
    run_worker: bool = True

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_path=os.environ.get("DATABASE_PATH", cls.database_path),
            keys_file=os.environ.get("KEYS_FILE", cls.keys_file),
            retry_base_seconds=float(os.environ.get("RETRY_BASE_SECONDS", cls.retry_base_seconds)),
            retry_cap_seconds=float(os.environ.get("RETRY_CAP_SECONDS", cls.retry_cap_seconds)),
            max_attempts=int(os.environ.get("MAX_ATTEMPTS", cls.max_attempts)),
            worker_poll_interval=float(os.environ.get("WORKER_POLL_INTERVAL", cls.worker_poll_interval)),
            signature_tolerance_seconds=int(
                os.environ.get("SIGNATURE_TOLERANCE_SECONDS", cls.signature_tolerance_seconds)
            ),
            replay_approval_timeout_seconds=float(os.environ.get(
                "REPLAY_APPROVAL_TIMEOUT_SECONDS", cls.replay_approval_timeout_seconds)),
            replay_policy_change_ttl_seconds=float(os.environ.get(
                "REPLAY_POLICY_CHANGE_TTL_SECONDS", cls.replay_policy_change_ttl_seconds)),
            notif_retry_base_seconds=float(os.environ.get(
                "NOTIF_RETRY_BASE_SECONDS", cls.notif_retry_base_seconds)),
            notif_retry_cap_seconds=float(os.environ.get(
                "NOTIF_RETRY_CAP_SECONDS", cls.notif_retry_cap_seconds)),
            notif_max_attempts=int(os.environ.get(
                "NOTIF_MAX_ATTEMPTS", cls.notif_max_attempts)),
            notif_deadline_lead_seconds=float(os.environ.get(
                "NOTIF_DEADLINE_LEAD_SECONDS", cls.notif_deadline_lead_seconds)),
            notif_breaker_window_seconds=float(os.environ.get(
                "NOTIF_BREAKER_WINDOW_SECONDS", cls.notif_breaker_window_seconds)),
            notif_breaker_failure_threshold=int(os.environ.get(
                "NOTIF_BREAKER_FAILURE_THRESHOLD",
                cls.notif_breaker_failure_threshold)),
            notif_breaker_cooldown_seconds=float(os.environ.get(
                "NOTIF_BREAKER_COOLDOWN_SECONDS", cls.notif_breaker_cooldown_seconds)),
            notif_channel_timeout_seconds=float(os.environ.get(
                "NOTIF_CHANNEL_TIMEOUT_SECONDS", cls.notif_channel_timeout_seconds)),
            receipt_confirm_timeout_seconds=float(os.environ.get(
                "RECEIPT_CONFIRM_TIMEOUT_SECONDS",
                cls.receipt_confirm_timeout_seconds)),
            receipt_confirm_max_retries=int(os.environ.get(
                "RECEIPT_CONFIRM_MAX_RETRIES",
                cls.receipt_confirm_max_retries)),
            receipt_email_secret=os.environ.get("RECEIPT_EMAIL_SECRET"),
            receipt_webhook_secret=os.environ.get("RECEIPT_WEBHOOK_SECRET"),
            run_worker=os.environ.get("RUN_WORKER", "true").lower() == "true",
        )

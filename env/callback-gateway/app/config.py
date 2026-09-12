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
            run_worker=os.environ.get("RUN_WORKER", "true").lower() == "true",
        )

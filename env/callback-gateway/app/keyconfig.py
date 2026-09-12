"""签名密钥配置的热轮换与版本管理。

流程：管理入口提交新配置 -> 校验 -> 落库 -> 原子切换内存密钥环。

- 每次提交（无论成败）都在 key_config_versions 落一条记录：配置版本、操作者、
  时间、结果（applied/rejected），可查每一次切换历史；
- 校验失败：只写 rejected 记录，当前配置原样保留；
- 校验通过：先在事务里写入 applied 记录（版本号单调递增）并提交，再原子切换
  内存中的密钥环——即使切换后立刻崩溃，重启也会从库里加载这最后一次成功配置；
- 启动时库里没有 applied 配置，则用 KEYS_FILE 引导为第 1 版（操作者 system-bootstrap）。
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass

from .db import Database
from .security import KeyRing, KeyRingManager, validate_key_config

# 拒绝记录里保存的原始报文上限，避免超大提交撑爆审计表
_MAX_STORED_SUBMISSION = 4000


class KeyConfigStore:
    """key_config_versions 表的读写；版本号在写事务内分配，并发提交不会重号。"""

    def __init__(self, db: Database):
        self._db = db

    def bootstrap(self, keys_file: str, tolerance_seconds: int) -> KeyRing:
        """启动加载：优先库中最后一次 applied 配置；否则用密钥文件引导为第 1 版。"""
        row = self.current_applied()
        if row is not None:
            keys, errors = validate_key_config(json.loads(row["config"]))
            if errors:
                # 库里的配置当初校验通过，不该失败；失败说明数据被改坏，拒绝带病启动
                raise RuntimeError(
                    f"stored key config v{row['version']} is invalid: {errors}")
            return KeyRing(keys, tolerance_seconds)

        with open(keys_file, "r", encoding="utf-8") as f:
            raw = json.load(f)
        keys, errors = validate_key_config(raw)
        if errors:
            raise ValueError(f"invalid keys file {keys_file}: {errors}")
        self.record_applied("system-bootstrap", {"keys": raw["keys"]})
        return KeyRing(keys, tolerance_seconds)

    def record_applied(self, operator: str, config: dict) -> int:
        """在一个事务里分配新版本号并写入 applied 记录，返回新版本号。"""
        with self._db.tx() as cur:
            row = cur.execute("SELECT MAX(version) AS v FROM key_config_versions").fetchone()
            version = (row["v"] or 0) + 1
            cur.execute(
                """INSERT INTO key_config_versions
                   (version, result, config, operator, reason, created_at)
                   VALUES (?,?,?,?,NULL,?)""",
                (version, "applied",
                 json.dumps(config, ensure_ascii=False, sort_keys=True),
                 operator, time.time()),
            )
            return version

    def record_rejected(self, operator: str, raw_config: str, reasons: list[str]) -> None:
        with self._db.tx() as cur:
            cur.execute(
                """INSERT INTO key_config_versions
                   (version, result, config, operator, reason, created_at)
                   VALUES (NULL,'rejected',?,?,?,?)""",
                (raw_config[:_MAX_STORED_SUBMISSION], operator,
                 json.dumps(reasons, ensure_ascii=False), time.time()),
            )

    def current_applied(self) -> sqlite3.Row | None:
        return self._db.query_one(
            "SELECT * FROM key_config_versions WHERE result='applied' "
            "ORDER BY version DESC LIMIT 1")

    def history(self, limit: int) -> list[sqlite3.Row]:
        return self._db.query(
            "SELECT * FROM key_config_versions ORDER BY id DESC LIMIT ?", (limit,))


@dataclass
class KeyRotationService:
    """轮换编排：校验 -> 落库 -> 原子切换。管理端点只调用这里。"""

    store: KeyConfigStore
    manager: KeyRingManager

    def rotate(self, raw_body: bytes) -> tuple[int, dict]:
        """处理一次轮换提交，返回 (HTTP 状态码, 响应体)。任何结果都会落审计记录。"""
        try:
            submitted = json.loads(raw_body)
        except (UnicodeDecodeError, json.JSONDecodeError):
            return self._reject("unknown", _safe_text(raw_body),
                                ["body_must_be_valid_json"])
        operator = submitted.get("operator") if isinstance(submitted, dict) else None
        if not isinstance(operator, str) or not operator.strip():
            return self._reject("unknown", _safe_text(raw_body),
                                ["operator_must_be_non_empty_string"])

        keys, errors = validate_key_config(submitted)
        if errors:
            return self._reject(operator, json.dumps(submitted, ensure_ascii=False), errors)

        # 先落库（事务提交），再原子切换内存配置：顺序反过来会在崩溃窗口内
        # 出现"内存已换、库里没有"的不一致；落库失败则抛 500，当前配置不动
        version = self.store.record_applied(operator, {"keys": submitted["keys"]})
        self.manager.swap_keys(keys)
        return 200, {"result": "applied", "version": version}

    def _reject(self, operator: str, raw_config: str, reasons: list[str]) -> tuple[int, dict]:
        self.store.record_rejected(operator, raw_config, reasons)
        return 422, {"error": "invalid_key_config", "reasons": reasons}

    def current(self) -> dict:
        """当前生效配置的版本视图（密钥明文永不回显）。"""
        row = self.store.current_applied()
        if row is None:  # 只可能发生在 bootstrap 之前的启动窗口
            raise RuntimeError("no applied key config")
        return {
            "version": row["version"],
            "operator": row["operator"],
            "applied_at": row["created_at"],
            "config": _mask_config(json.loads(row["config"])),
        }

    def history(self, limit: int = 100) -> list[dict]:
        """每次切换/尝试的记录（新的在前），密钥明文打码。"""
        out = []
        for row in self.store.history(limit):
            config = row["config"]
            if config:
                try:
                    config = _mask_config(json.loads(config))
                except json.JSONDecodeError:
                    pass  # 失败提交的原始报文不是 JSON，原样保留便于排查
            out.append({
                "id": row["id"],
                "version": row["version"],
                "result": row["result"],
                "operator": row["operator"],
                "reason": json.loads(row["reason"]) if row["reason"] else None,
                "config": config,
                "created_at": row["created_at"],
            })
        return out


def _safe_text(raw_body: bytes) -> str:
    return raw_body.decode("utf-8", errors="replace")


def _mask_config(config):
    """查询视图里绝不回显密钥明文。"""
    if not isinstance(config, dict):
        return config
    keys = config.get("keys")
    if not isinstance(keys, list):
        return config
    masked = [{**k, "secret": "***"} if isinstance(k, dict) and "secret" in k else k
              for k in keys]
    return {**config, "keys": masked}

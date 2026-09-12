"""签名校验：HMAC-SHA256 + 可热轮换的密钥环。

报文头：
    X-Signature: kid=<密钥ID>,ts=<unix秒>,sig=<hex>
签名串：
    f"{ts}\\n" + 原始请求体字节

密钥配置（JSON）支持不重启热轮换（见 keyconfig.py）：
- active   当前用于签发的密钥
- retired  已轮换下线的旧密钥，在 grace_until（过渡期截止）之前仍可用于验签，
           超过过渡期一律拒绝。这样接入方可以平滑切换，旧钥匙在过渡期仍可用。

KeyRing 本身不可变；轮换由 KeyRingManager 整份替换，正在处理的请求
只会看到完整的旧配置或完整的新配置，绝不会一半一半。
"""
from __future__ import annotations

import hashlib
import hmac
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any


@dataclass
class KeyEntry:
    kid: str
    secret: bytes
    status: str                 # active | retired
    grace_until: float | None   # 仅 retired 有效：过渡期截止时间（epoch 秒）


def validate_key_config(raw: Any) -> tuple[list[KeyEntry], list[str]]:
    """校验一份密钥配置。返回 (解析出的密钥列表, 错误列表)；错误非空则不得切换。

    规则：格式必须是 {"keys": [...]}；kid 不为空且不重复；secret 不为空；
    至少有一个 active 密钥；retired 密钥必须带合法的 grace_until（过渡期截止）。
    """
    if not isinstance(raw, dict) or not isinstance(raw.get("keys"), list) or not raw.get("keys"):
        return [], ["keys_must_be_non_empty_list"]

    errors: list[str] = []
    keys: list[KeyEntry] = []
    seen: set[str] = set()
    for i, item in enumerate(raw["keys"]):
        where = f"keys[{i}]"
        if not isinstance(item, dict):
            errors.append(f"{where}: entry_must_be_object")
            continue
        ok = True
        kid = item.get("kid")
        if not isinstance(kid, str) or not kid:
            errors.append(f"{where}: kid_must_be_non_empty_string")
            ok = False
        elif kid in seen:
            errors.append(f"{where}: duplicate_kid:{kid}")
            ok = False
        secret = item.get("secret")
        if not isinstance(secret, str) or not secret:
            errors.append(f"{where}: secret_must_be_non_empty_string")
            ok = False
        status = item.get("status", "active")
        if status not in ("active", "retired"):
            errors.append(f"{where}: status_must_be_active_or_retired")
            ok = False
        grace = item.get("grace_until")
        grace_ts = None
        if grace is not None:
            try:
                if not isinstance(grace, str):
                    raise ValueError("not a string")
                grace_ts = _parse_ts(grace)
            except ValueError:
                errors.append(f"{where}: invalid_grace_until")
                ok = False
        if status == "retired" and grace is None:
            errors.append(f"{where}: retired_key_requires_grace_until")
            ok = False
        if ok:
            seen.add(kid)
            keys.append(KeyEntry(kid=kid, secret=secret.encode(),
                                 status=status, grace_until=grace_ts))

    if not any(k.status == "active" for k in keys):
        errors.append("at_least_one_active_key_required")
    return keys, errors


class KeyRing:
    """一份不可变的密钥配置快照。"""

    def __init__(self, keys: list[KeyEntry], tolerance_seconds: int = 300):
        self._keys = {k.kid: k for k in keys}
        self.tolerance = tolerance_seconds

    @staticmethod
    def parse_header(header: str) -> dict[str, str]:
        parts = {}
        for seg in header.split(","):
            if "=" in seg:
                k, v = seg.split("=", 1)
                parts[k.strip()] = v.strip()
        return parts

    def verify(self, header: str | None, body: bytes, now: float | None = None) -> tuple[bool, str, str | None]:
        """返回 (是否通过, 原因, 使用的 kid)。所有结果都应写入审计日志。"""
        now = time.time() if now is None else now
        if not header:
            return False, "missing_signature_header", None
        parts = self.parse_header(header)
        kid, ts_s, sig = parts.get("kid"), parts.get("ts"), parts.get("sig")
        if not (kid and ts_s and sig):
            return False, "malformed_signature_header", kid

        key = self._keys.get(kid)
        if key is None:
            return False, "unknown_kid", kid
        if key.status == "retired":
            if key.grace_until is None or now > key.grace_until:
                return False, "retired_key_grace_expired", kid
        elif key.status != "active":
            return False, "key_not_usable", kid

        try:
            ts = int(ts_s)
        except ValueError:
            return False, "bad_timestamp", kid
        if abs(now - ts) > self.tolerance:
            return False, "timestamp_out_of_tolerance", kid

        expected = hmac.new(key.secret, f"{ts}\n".encode() + body, hashlib.sha256).hexdigest()
        if not hmac.compare_digest(expected, sig):
            return False, "signature_mismatch", kid
        return True, "ok", kid


def sign(secret: str, body: bytes, ts: int | None = None) -> str:
    """给接入方/测试用的签名 helper。"""
    ts = int(time.time()) if ts is None else ts
    sig = hmac.new(secret.encode(), f"{ts}\n".encode() + body, hashlib.sha256).hexdigest()
    return ts, sig


class KeyRingManager:
    """持有当前密钥环，支持不重启原子切换。

    verify() 在请求开始时一次性读走当前快照，整个请求只使用这一份配置；
    swap() 是单次引用替换。因此任何一次验签要么完全用旧配置、要么完全用
    新配置，正在处理的请求不会看到"半新半旧"的中间态。
    """

    def __init__(self, keyring: KeyRing):
        self._keyring = keyring
        self._lock = threading.Lock()

    @property
    def current(self) -> KeyRing:
        return self._keyring

    def verify(self, header: str | None, body: bytes,
               now: float | None = None) -> tuple[bool, str, str | None]:
        return self._keyring.verify(header, body, now)  # 单次读取 = 一份一致快照

    def swap_keys(self, keys: list[KeyEntry]) -> None:
        """整份替换当前配置（沿用当前的时间戳容差）。"""
        with self._lock:
            self._keyring = KeyRing(list(keys), self._keyring.tolerance)


def _parse_ts(s: str) -> float:
    dt = datetime.fromisoformat(s.replace("Z", "+00:00"))
    if dt.tzinfo is None:           # 没带时区的一律按 UTC 解释，避免依赖服务器本地时区
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc).timestamp()

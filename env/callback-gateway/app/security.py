"""签名校验：HMAC-SHA256 + 可轮换密钥环。

报文头：
    X-Signature: kid=<密钥ID>,ts=<unix秒>,sig=<hex>
签名串：
    f"{ts}\\n" + 原始请求体字节

密钥环（JSON 文件）支持热轮换：
- active   当前用于签发的密钥
- retired  已轮换下线的旧密钥，在 grace_until（过渡期截止）之前仍可用于验签，
           超过过渡期一律拒绝。这样接入方可以平滑切换，旧钥匙在过渡期仍可用。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import time
from dataclasses import dataclass
from datetime import datetime, timezone


@dataclass
class KeyEntry:
    kid: str
    secret: bytes
    status: str                 # active | retired
    grace_until: float | None   # 仅 retired 有效：过渡期截止时间（epoch 秒）


class KeyRing:
    def __init__(self, keys: list[KeyEntry], tolerance_seconds: int = 300):
        self._keys = {k.kid: k for k in keys}
        self.tolerance = tolerance_seconds

    @classmethod
    def from_file(cls, path: str, tolerance_seconds: int = 300) -> "KeyRing":
        with open(path, "r", encoding="utf-8") as f:
            raw = json.load(f)
        keys = []
        for item in raw["keys"]:
            grace = item.get("grace_until")
            keys.append(KeyEntry(
                kid=item["kid"],
                secret=item["secret"].encode(),
                status=item.get("status", "active"),
                grace_until=_parse_ts(grace) if grace else None,
            ))
        return cls(keys, tolerance_seconds)

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


def _parse_ts(s: str) -> float:
    return datetime.fromisoformat(s.replace("Z", "+00:00")).astimezone(timezone.utc).timestamp()

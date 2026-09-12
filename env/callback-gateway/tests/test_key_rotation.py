"""密钥热轮换：管理端提交配置 -> 校验 -> 原子切换 -> 落审计 -> 重启恢复。"""
from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import KeyEntry, KeyRing, KeyRingManager, sign

ACTIVE_SECRET = "new-secret"
OLD_SECRET = "old-secret"
V3_SECRET = "v3-secret"
FAR_FUTURE = "2099-01-01T00:00:00Z"
PAST = "2020-01-01T00:00:00Z"


def make_keys_file(tmp_path, grace_until=FAR_FUTURE):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"},
        {"kid": "k1", "secret": OLD_SECRET, "status": "retired", "grace_until": grace_until},
    ]}))
    return str(p)


def make_app(tmp_path, grace_until=FAR_FUTURE):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path, grace_until),
        run_worker=False,
    )
    return create_app(settings)


def post(client, external_id, body: bytes, secret=ACTIVE_SECRET, kid="k2"):
    ts, sig = sign(secret, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid={kid},ts={ts},sig={sig}",
    })


def rotate(client, payload):
    if isinstance(payload, (dict, list)):
        return client.post("/admin/keys/rotate", json=payload)
    return client.post("/admin/keys/rotate", content=payload)


V2_CONFIG = {
    "operator": "ops-li",
    "keys": [
        {"kid": "k3", "secret": V3_SECRET, "status": "active"},
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "retired", "grace_until": FAR_FUTURE},
        {"kid": "k1", "secret": OLD_SECRET, "status": "retired", "grace_until": PAST},
    ],
}


@pytest.fixture()
def client(tmp_path):
    with TestClient(make_app(tmp_path)) as c:
        yield c


# ---- 启动引导与版本查询 ------------------------------------------------------

def test_bootstrap_from_keys_file_becomes_version_1(client):
    cur = client.get("/admin/keys/current").json()
    assert cur["version"] == 1
    assert cur["operator"] == "system-bootstrap"
    assert {k["kid"]: k["status"] for k in cur["config"]["keys"]} == {
        "k2": "active", "k1": "retired"}
    # 密钥明文绝不回显
    assert ACTIVE_SECRET not in json.dumps(cur)
    assert all(k["secret"] == "***" for k in cur["config"]["keys"])


# ---- 热轮换：合法配置立即生效，不重启 -----------------------------------------

def test_rotate_applies_immediately_without_restart(client):
    r = rotate(client, V2_CONFIG)
    assert r.status_code == 200
    assert r.json() == {"result": "applied", "version": 2}

    # 新 active 密钥立即可签；k2 退为 retired，过渡期内仍可验；k1 过渡期已过，拒绝
    assert post(client, "ROT-1", b'{"order": 1}', secret=V3_SECRET, kid="k3").status_code == 202
    assert post(client, "ROT-2", b'{"order": 2}', secret=ACTIVE_SECRET, kid="k2").status_code == 202
    r = post(client, "ROT-3", b'{"order": 3}', secret=OLD_SECRET, kid="k1")
    assert r.status_code == 401
    assert r.json()["reason"] == "retired_key_grace_expired"

    cur = client.get("/admin/keys/current").json()
    assert cur["version"] == 2
    assert cur["operator"] == "ops-li"
    assert V3_SECRET not in json.dumps(cur)


def test_retired_key_usable_until_grace_deadline_then_rejected(client):
    """过渡期截止前旧密钥可验；把截止时间改到过去再次轮换后立即拒绝。"""
    assert rotate(client, V2_CONFIG).status_code == 200
    assert post(client, "G-1", b'{"o": 1}', secret=ACTIVE_SECRET, kid="k2").status_code == 202

    v3 = {"operator": "ops-li", "keys": [
        {"kid": "k3", "secret": V3_SECRET, "status": "active"},
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "retired", "grace_until": PAST},
    ]}
    assert rotate(client, v3).status_code == 200
    r = post(client, "G-2", b'{"o": 2}', secret=ACTIVE_SECRET, kid="k2")
    assert r.status_code == 401
    assert r.json()["reason"] == "retired_key_grace_expired"


# ---- 校验失败：保留当前配置，失败也落审计 --------------------------------------

BAD_CONFIGS = [
    (b"this is not json", "body_must_be_valid_json"),
    ({"keys": [{"kid": "x", "secret": "y"}]}, "operator_must_be_non_empty_string"),
    ({"operator": "ops", "keys": []}, "keys_must_be_non_empty_list"),
    ({"operator": "ops", "keys": "nope"}, "keys_must_be_non_empty_list"),
    ({"operator": "ops", "keys": [{"kid": "k9", "secret": "s", "status": "active"},
                                  {"kid": "k9", "secret": "t", "status": "active"}]},
     "duplicate_kid:k9"),
    ({"operator": "ops", "keys": [
        {"kid": "k9", "secret": "s", "status": "retired", "grace_until": FAR_FUTURE}]},
     "at_least_one_active_key_required"),
    ({"operator": "ops", "keys": [{"kid": "k9", "secret": "s", "status": "active"},
                                  {"kid": "k8", "secret": "t", "status": "retired"}]},
     "retired_key_requires_grace_until"),
    ({"operator": "ops", "keys": [
        {"kid": "k9", "secret": "s", "status": "active"},
        {"kid": "k8", "secret": "t", "status": "retired", "grace_until": "not-a-date"}]},
     "invalid_grace_until"),
    ({"operator": "ops", "keys": [{"kid": "k9", "status": "active"}]},
     "secret_must_be_non_empty_string"),
    ({"operator": "ops", "keys": [{"kid": "k9", "secret": "s", "status": "weird"}]},
     "status_must_be_active_or_retired"),
]


@pytest.mark.parametrize("payload,reason", BAD_CONFIGS,
                         ids=[r for _, r in BAD_CONFIGS])
def test_invalid_config_rejected_and_current_config_kept(client, payload, reason):
    r = rotate(client, payload)
    assert r.status_code == 422
    assert any(reason in msg for msg in r.json()["reasons"])

    # 当前配置原样保留：版本不变，现有密钥照常可用
    assert client.get("/admin/keys/current").json()["version"] == 1
    assert post(client, f"BAD-{reason}", b"{}", secret=ACTIVE_SECRET, kid="k2").status_code == 202

    # 失败提交也落了审计：操作者（取不到时 unknown）、时间、结果、原因
    rec = client.get("/admin/keys/versions").json()["versions"][0]
    assert rec["result"] == "rejected"
    assert rec["version"] is None
    assert any(reason in msg for msg in rec["reason"])
    assert rec["created_at"] > 0


# ---- 切换原子性：任何请求只看到完整的旧配置或完整的新配置 -----------------------

def test_verify_never_sees_half_switched_config():
    """并发轮换期间，每一次验签的结果都必须能由一份完整配置解释。

    （生产上一次请求 = 一次 verify 调用；若实现退化成"原地改密钥表"，
    单次 verify 就会看到 k1 已被删掉而 k2 还没加进来的中间态。）
    """
    past = time.time() - 10
    old_keys = [KeyEntry("k1", b"s1", "active", None)]
    new_keys = [KeyEntry("k2", b"s2", "active", None),
                KeyEntry("k1", b"s1", "retired", past)]
    mgr = KeyRingManager(KeyRing(old_keys))

    body = b"{}"
    ts = int(time.time())
    h1 = f"kid=k1,ts={ts},sig={sign('s1', body, ts)[1]}"
    h2 = f"kid=k2,ts={ts},sig={sign('s2', body, ts)[1]}"
    # 每个 kid 在"完整旧配置"或"完整新配置"下的可能结果；其余结果即"切了一半"
    allowed = {"k1": {"ok", "retired_key_grace_expired"}, "k2": {"unknown_kid", "ok"}}

    errors, stop = [], threading.Event()

    def swapper():
        while not stop.is_set():
            mgr.swap_keys(new_keys)
            mgr.swap_keys(old_keys)

    def verifier():
        while not stop.is_set():
            r1 = mgr.verify(h1, body)[1]
            r2 = mgr.verify(h2, body)[1]
            if r1 not in allowed["k1"] or r2 not in allowed["k2"]:
                errors.append((r1, r2))

    threads = ([threading.Thread(target=swapper) for _ in range(2)]
               + [threading.Thread(target=verifier) for _ in range(4)])
    for t in threads:
        t.start()
    time.sleep(0.5)
    stop.set()
    for t in threads:
        t.join()
    assert not errors


def test_in_flight_request_keeps_its_snapshot():
    """请求开始时取走的配置快照不受后续切换影响（确定性验证）。"""
    mgr = KeyRingManager(KeyRing([KeyEntry("k1", b"s1", "active", None)]))
    inflight = mgr.current                       # 请求开始时拿到的快照
    mgr.swap_keys([KeyEntry("k2", b"s2", "active", None)])

    body = b"{}"
    ts = int(time.time())
    h1 = f"kid=k1,ts={ts},sig={sign('s1', body, ts)[1]}"
    ok, _, _ = inflight.verify(h1, body)         # 进行中的请求：仍用旧快照，k1 有效
    assert ok
    assert mgr.verify(h1, body)[1] == "unknown_kid"   # 新请求：用新配置，k1 不存在


# ---- 重启恢复：继续使用最后一次成功配置 -----------------------------------------

def test_restart_keeps_last_applied_config(tmp_path):
    app1 = make_app(tmp_path)
    with TestClient(app1) as c1:
        assert rotate(c1, V2_CONFIG).json()["version"] == 2
        assert post(c1, "RS-1", b"{}", secret=V3_SECRET, kid="k3").status_code == 202

    # “重启”：同一数据库文件上重建服务（密钥文件仍是旧内容）
    app2 = make_app(tmp_path)
    with TestClient(app2) as c2:
        cur = c2.get("/admin/keys/current").json()
        assert cur["version"] == 2
        assert cur["operator"] == "ops-li"
        # 生效的是最后一次成功配置，不是密钥文件里的第 1 版
        assert post(c2, "RS-2", b"{}", secret=V3_SECRET, kid="k3").status_code == 202
        r = post(c2, "RS-3", b"{}", secret=OLD_SECRET, kid="k1")
        assert r.status_code == 401
        # 版本号继续单调递增
        assert rotate(c2, {"operator": "ops-wang", "keys": [
            {"kid": "k4", "secret": "v4-secret", "status": "active"},
        ]}).json()["version"] == 3


# ---- 切换历史 -----------------------------------------------------------------

def test_history_records_every_attempt(client):
    rotate(client, V2_CONFIG)                                   # applied v2
    rotate(client, {"operator": "ops-li", "keys": []})          # rejected
    rotate(client, b"garbage")                                  # rejected

    versions = client.get("/admin/keys/versions").json()["versions"]
    assert [v["result"] for v in versions] == ["rejected", "rejected", "applied", "applied"]
    assert versions[2]["version"] == 2 and versions[2]["operator"] == "ops-li"
    assert versions[3]["version"] == 1 and versions[3]["operator"] == "system-bootstrap"
    assert all(v["created_at"] > 0 for v in versions)
    # 明文密钥不出现在历史记录里
    assert V3_SECRET not in json.dumps(versions)

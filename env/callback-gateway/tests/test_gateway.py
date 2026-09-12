"""端到端测试：签名校验与轮换、幂等、冲突冻结、重试隔离、副作用 exactly-once、人工处置、审计。"""
from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.handlers import IdempotentSink
from app.main import create_app
from app.security import sign
from app.worker import Worker

ACTIVE_SECRET = "new-secret"
OLD_SECRET = "old-secret"
FAR_FUTURE = "2099-01-01T00:00:00Z"
PAST = "2020-01-01T00:00:00Z"


def make_keys_file(tmp_path, grace_until=FAR_FUTURE):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"},
        {"kid": "k1", "secret": OLD_SECRET, "status": "retired", "grace_until": grace_until},
    ]}))
    return str(p)


def make_app(tmp_path, grace_until=FAR_FUTURE, **overrides):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path, grace_until),
        retry_base_seconds=overrides.get("retry_base_seconds", 5.0),
        max_attempts=overrides.get("max_attempts", 3),
        run_worker=False,  # 测试里手动驱动 worker
    )
    return create_app(settings)


def post(client, external_id, body: bytes, secret=ACTIVE_SECRET, kid="k2", ts=None):
    ts, sig = sign(secret, body, ts)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid={kid},ts={ts},sig={sig}",
    })


@pytest.fixture()
def client(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as c:
        yield c


# ---- 签名校验与密钥轮换 ---------------------------------------------------

def test_valid_signature_accepted(client):
    r = post(client, "A-1", b'{"order": 1}')
    assert r.status_code == 202
    assert r.json()["result"] == "accepted"


def test_bad_signature_rejected_and_audited(client):
    r = client.post("/callbacks", content=b'{"order": 1}', headers={
        "X-Callback-Id": "A-2",
        "X-Signature": f"kid=k2,ts={int(time.time())},sig={'0' * 64}",
    })
    assert r.status_code == 401
    events = client.get("/admin/events", params={"type": "signature_fail"}).json()["events"]
    assert len(events) == 1
    assert events[0]["detail"]["reason"] == "signature_mismatch"


def test_missing_signature_rejected(client):
    r = client.post("/callbacks", content=b"{}", headers={"X-Callback-Id": "A-3"})
    assert r.status_code == 401


def test_retired_key_usable_during_grace_period(client):
    """旧钥匙在过渡期内仍可用。"""
    r = post(client, "A-4", b'{"order": 4}', secret=OLD_SECRET, kid="k1")
    assert r.status_code == 202


def test_retired_key_rejected_after_grace_period(tmp_path):
    """过渡期结束后旧钥匙一律拒绝。"""
    app = make_app(tmp_path, grace_until=PAST)
    with TestClient(app) as c:
        r = post(c, "A-5", b'{"order": 5}', secret=OLD_SECRET, kid="k1")
        assert r.status_code == 401
        assert r.json()["reason"] == "retired_key_grace_expired"


def test_unknown_kid_and_stale_timestamp_rejected(client):
    r = post(client, "A-6", b"{}", kid="nope")
    assert r.status_code == 401
    stale = int(time.time()) - 3600
    r = post(client, "A-6", b"{}", ts=stale)
    assert r.status_code == 401


# ---- 幂等：同编号同内容只处理一次 -----------------------------------------

def test_duplicate_same_content_processed_once(client):
    body = b'{"order": 7}'
    assert post(client, "A-7", body).status_code == 202
    r = post(client, "A-7", body)  # 完全相同的重复投递
    assert r.status_code == 200
    assert r.json()["result"] == "duplicate"

    deliveries = client.get("/admin/deliveries", params={"external_id": "A-7"}).json()["deliveries"]
    assert len(deliveries) == 1  # 只落了一条

    client.app.state.worker.run_once()
    detail = client.get(f"/admin/deliveries/{deliveries[0]['id']}").json()
    assert detail["delivery"]["status"] == "done"
    assert len(detail["outbox"]) == 1
    assert detail["outbox"][0]["status"] == "executed"

    # 处理完成后再重复投递，依然不会二次处理
    r = post(client, "A-7", body)
    assert r.json()["result"] == "duplicate"
    effects = client.get("/admin/events", params={"type": "effect_executed"}).json()["events"]
    assert len(effects) == 1


# ---- 冲突：同编号不同内容冻结，不覆盖 --------------------------------------

def test_conflicting_content_frozen_and_never_overwritten(client):
    v1 = b'{"order": 8, "amount": 100}'
    v2 = b'{"order": 8, "amount": 200}'
    r1 = post(client, "A-8", v1)
    assert r1.json()["result"] == "accepted"
    r2 = post(client, "A-8", v2)
    assert r2.status_code == 202
    assert r2.json()["result"] == "conflict"
    conflict_id = r2.json()["conflict_id"]

    # 两个版本都在，旧内容原样保留，未被新内容覆盖
    detail = client.get(f"/admin/conflicts/{conflict_id}").json()
    assert detail["conflict"]["status"] == "open"
    payloads = sorted(v["payload"] for v in detail["versions"])
    assert payloads == sorted([v1.decode(), v2.decode()])
    assert all(v["frozen"] == 1 for v in detail["versions"])

    # 冻结期间处理层不会动它们
    client.app.state.worker.run_once()
    statuses = {v["status"] for v in client.get(
        f"/admin/conflicts/{conflict_id}").json()["versions"]}
    assert statuses == {"conflicted"}

    events = client.get("/admin/events", params={"type": "conflict_opened"}).json()["events"]
    assert len(events) == 1


def test_third_version_joins_existing_conflict(client):
    post(client, "A-9", b'{"v": 1}')
    post(client, "A-9", b'{"v": 2}')
    r = post(client, "A-9", b'{"v": 3}')
    detail = client.get(f"/admin/conflicts/{r.json()['conflict_id']}").json()
    assert len(detail["versions"]) == 3


# ---- 重试与隔离 ------------------------------------------------------------

def test_retry_backoff_then_quarantine_without_blocking_others(tmp_path):
    app = make_app(tmp_path, retry_base_seconds=5.0, max_attempts=3)
    with TestClient(app) as c:
        post(c, "BAD-1", b'{"force_error": true}')
        post(c, "GOOD-1", b'{"order": 1}')

        clock = [time.time()]
        worker: Worker = c.app.state.worker
        worker.clock = lambda: clock[0]

        # 第 1 次失败 -> 安排 5s 后重试
        worker.run_once()
        d = c.get("/admin/deliveries", params={"external_id": "BAD-1"}).json()["deliveries"][0]
        assert d["status"] == "pending" and d["attempts"] == 1

        # 没到重试时间不会再试
        worker.run_once()
        d = c.get("/admin/deliveries", params={"external_id": "BAD-1"}).json()["deliveries"][0]
        assert d["attempts"] == 1

        # 连续失败达到上限 -> 隔离
        for _ in range(2):
            clock[0] += 60
            worker.run_once()
        d = c.get("/admin/deliveries", params={"external_id": "BAD-1"}).json()["deliveries"][0]
        assert d["status"] == "quarantined" and d["attempts"] == 3

        # 别的编号不受隔离影响，正常完成
        g = c.get("/admin/deliveries", params={"external_id": "GOOD-1"}).json()["deliveries"][0]
        assert g["status"] == "done"

        retries = c.get("/admin/events", params={"type": "retry_scheduled"}).json()["events"]
        delays = sorted(e["detail"]["delay_seconds"] for e in retries)
        assert delays == [5.0, 10.0]  # 指数退避
        quarantined = c.get("/admin/events", params={"type": "quarantined"}).json()["events"]
        assert len(quarantined) == 1


def test_manual_requeue_from_quarantine(tmp_path):
    app = make_app(tmp_path, max_attempts=1)
    with TestClient(app) as c:
        post(c, "BAD-2", b'{"force_error": true}')
        c.app.state.worker.run_once()
        d = c.get("/admin/deliveries", params={"external_id": "BAD-2"}).json()["deliveries"][0]
        assert d["status"] == "quarantined"

        r = c.post(f"/admin/deliveries/{d['id']}/requeue",
                   json={"operator": "ops-li", "note": "下游已恢复"})
        assert r.json()["result"] == "requeued"
        d = c.get(f"/admin/deliveries/{d['id']}").json()["delivery"]
        assert d["status"] == "pending" and d["attempts"] == 0


# ---- 副作用 exactly-once（重启 / 重复投递） --------------------------------

def test_side_effect_not_duplicated_across_restart(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as c:
        post(c, "S-1", b'{"order": 1}')
        worker = c.app.state.worker
        worker._process_due()  # 只处理业务，先不派发

        db = c.app.state.db
        outbox_row = db.query_one("SELECT * FROM outbox")
        key = outbox_row["idempotency_key"]

        # 模拟崩溃：副作用已发到下游，但还没来得及标记 executed
        sink = IdempotentSink(db)
        sink.send(key, outbox_row["effect_type"], json.loads(outbox_row["payload"]))
        assert sink.applied_count(key) == 1

        # “重启”：同一数据库文件上重建 worker/sink，重新派发
        worker2 = Worker(db, c.app.state.settings)
        worker2._dispatch_outbox()
        assert sink.applied_count(key) == 1  # 下游只应用了一次

        row = db.query_one("SELECT * FROM outbox")
        assert row["status"] == "executed"

        # 重复投递同一内容也不会再产生副作用
        post(c, "S-1", b'{"order": 1}')
        worker2.run_once()
        assert sink.applied_count(key) == 1


# ---- 人工选定后可追溯地继续 -------------------------------------------------

def test_manual_resolution_resumes_and_is_audited(client):
    post(client, "R-1", b'{"order": 1, "amount": 100}')
    r = post(client, "R-1", b'{"order": 1, "amount": 200}')
    conflict_id = r.json()["conflict_id"]
    versions = client.get(f"/admin/conflicts/{conflict_id}").json()["versions"]
    chosen = next(v for v in versions if "200" in v["payload"])
    rejected = next(v for v in versions if "100" in v["payload"])

    r = client.post(f"/admin/conflicts/{conflict_id}/resolve",
                    json={"delivery_id": chosen["id"], "operator": "ops-wang",
                          "note": "以金额 200 的为准"})
    assert r.json()["result"] == "resolved"

    # 选定的版本继续处理直至完成，未选中的被 supersede 且内容仍在
    client.app.state.worker.run_once()
    chosen_after = client.get(f"/admin/deliveries/{chosen['id']}").json()["delivery"]
    rejected_after = client.get(f"/admin/deliveries/{rejected['id']}").json()["delivery"]
    assert chosen_after["status"] == "done"
    assert json.loads(chosen_after["checkpoint"])["stage"] == "planned"
    assert rejected_after["status"] == "superseded"
    assert rejected_after["payload"] == rejected["payload"]  # 历史内容保留可查

    # 冲突单已关闭，重复处置被拒绝
    assert client.get(f"/admin/conflicts/{conflict_id}").json()["conflict"]["status"] == "resolved"
    r = client.post(f"/admin/conflicts/{conflict_id}/resolve",
                    json={"delivery_id": chosen["id"], "operator": "ops-wang"})
    assert r.status_code == 409

    # 全程审计可查：签名 -> 接受 -> 冲突 -> 处置 -> 处理 -> 副作用
    events = client.get("/admin/events", params={"external_id": "R-1"}).json()["events"]
    types = {e["type"] for e in events}
    assert {"signature_ok", "accepted", "conflict_opened", "conflict_resolved",
            "delivery_done"} <= types
    resolved = next(e for e in events if e["type"] == "conflict_resolved")
    assert resolved["detail"]["operator"] == "ops-wang"
    effect_events = client.get("/admin/events", params={"type": "effect_executed"}).json()["events"]
    assert len(effect_events) == 1  # 只有被选中的版本产生了副作用

"""业务重放模块端到端测试：筛选预览、批量提交、幂等去重、暂停/继续/取消、
重启续跑、失败单独重试、副作用同套幂等保护、完整审计。"""
from __future__ import annotations

import json
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.handlers import IdempotentSink, TransientError, business_handler
from app.main import create_app
from app.security import sign
from app.worker import Worker

ACTIVE_SECRET = "new-secret"
OLD_SECRET = "old-secret"
FAR_FUTURE = "2099-01-01T00:00:00Z"


def make_keys_file(tmp_path, grace_until=FAR_FUTURE):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"},
        {"kid": "k1", "secret": OLD_SECRET, "status": "retired", "grace_until": grace_until},
    ]}))
    return str(p)


def make_app(tmp_path, **overrides):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path),
        retry_base_seconds=overrides.get("retry_base_seconds", 5.0),
        max_attempts=overrides.get("max_attempts", 3),
        run_worker=False,  # 测试里手动驱动 worker
    )
    return create_app(settings)


def post(client, external_id, body: bytes, secret=ACTIVE_SECRET, kid="k2"):
    ts, sig = sign(secret, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid={kid},ts={ts},sig={sig}",
    })


@pytest.fixture()
def client(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as c:
        yield c


def process_normally(client, external_id, body: bytes):
    """投递并跑完正常处理管线（done，副作用已派发），返回 delivery 行。"""
    post(client, external_id, body)
    client.app.state.worker.run_once()
    rows = client.get("/admin/deliveries", params={"external_id": external_id}).json()["deliveries"]
    assert rows[0]["status"] == "done"
    return rows[0]


def quarantine(client, external_id, body: bytes):
    """投递一个必然失败的报文并跑到隔离（可重放状态）。"""
    post(client, external_id, body)
    clock = [time.time()]
    worker = client.app.state.worker
    worker.clock = lambda: clock[0]
    for _ in range(3):  # max_attempts=3
        worker.run_once()
        clock[0] += 60
    row = client.get("/admin/deliveries", params={"external_id": external_id}).json()["deliveries"][0]
    assert row["status"] == "quarantined"
    return row


def submit(client, **kw):
    payload = {"operator": "ops-li", "reason": "下游补数据", **kw}
    return client.post("/admin/replays", json=payload)


def get_task(client, batch_id, external_id):
    tasks = client.get(f"/admin/replays/{batch_id}").json()["tasks"]
    return next(t for t in tasks if t["external_id"] == external_id)


# ---- 预览：按编号/时间/处理结果筛选 + 影响范围 -------------------------------

def test_preview_filters_by_id_time_and_result(client):
    d1 = process_normally(client, "RP-1", b'{"order": 1}')
    process_normally(client, "RP-2", b'{"order": 2}')
    post(client, "RP-3", b'{"order": 3}')  # 仍在管线中（pending）

    # 按编号筛选：内容 + 影响范围（正常处理产出过的副作用）
    r = client.post("/admin/replays/preview", json={"external_id": "RP-1"})
    assert r.json()["matched"] == 1
    item = r.json()["items"][0]
    assert item["delivery_id"] == d1["id"] and item["replayable"] is True
    assert item["payload"] == '{"order": 1}'
    assert [e["effect_type"] for e in item["prior_effects"]] == ["downstream.notify"]
    assert item["prior_effects"][0]["status"] == "executed"

    # 按处理结果筛选
    r = client.post("/admin/replays/preview", json={"status": "done"})
    assert r.json()["matched"] == 2 and r.json()["replayable"] == 2

    # 按时间筛选（epoch 秒与 ISO-8601 都支持）
    now = time.time()
    r = client.post("/admin/replays/preview",
                    json={"created_from": now - 60, "created_to": now + 60})
    assert r.json()["matched"] == 3
    r = client.post("/admin/replays/preview", json={"created_to": "2000-01-01T00:00:00Z"})
    assert r.json()["matched"] == 0

    # 仍在管线中的版本不可重放
    items = client.post("/admin/replays/preview", json={}).json()["items"]
    item = next(i for i in items if i["external_id"] == "RP-3")
    assert item["replayable"] is False
    assert item["skip_reason"] == "still_in_pipeline"


def test_preview_flags_conflicted_and_superseded_versions(client):
    post(client, "RC-1", b'{"v": 1}')
    r = post(client, "RC-1", b'{"v": 2}')  # 冲突，整组冻结
    conflict_id = r.json()["conflict_id"]
    versions = client.get(f"/admin/conflicts/{conflict_id}").json()["versions"]

    items = client.post("/admin/replays/preview", json={"external_id": "RC-1"}).json()["items"]
    assert all(not i["replayable"] for i in items)
    assert {i["skip_reason"] for i in items} == {"frozen_by_conflict"}

    # 人工选定后：未选中版本 superseded 同样不可重放，选定版本处理完成后可重放
    chosen, rejected = versions[0], versions[1]
    client.post(f"/admin/conflicts/{conflict_id}/resolve",
                json={"delivery_id": chosen["id"], "operator": "ops-wang"})
    client.app.state.worker.run_once()
    items = client.post("/admin/replays/preview", json={"external_id": "RC-1"}).json()["items"]
    by_id = {i["delivery_id"]: i for i in items}
    assert by_id[rejected["id"]]["skip_reason"] == "superseded_version"
    assert by_id[chosen["id"]]["replayable"] is True


# ---- 提交：记录发起人/原始版本/原因/进度 -------------------------------------

def test_submit_records_operator_reason_version_and_progress(client):
    d = process_normally(client, "RS-1", b'{"order": 1}')
    r = submit(client, external_id="RS-1", request_id="req-001")
    assert r.status_code == 201
    batch_id = r.json()["batch_id"]
    assert r.json()["total"] == 1

    detail = client.get(f"/admin/replays/{batch_id}").json()
    batch = detail["batch"]
    assert batch["operator"] == "ops-li"                # 发起人
    assert batch["reason"] == "下游补数据"               # 原因
    assert batch["status"] == "running"
    assert batch["filters"]["external_id"] == "RS-1"    # 筛选条件快照
    assert batch["total"] == 1 and batch["done"] == 0   # 进度

    task = detail["tasks"][0]
    assert task["delivery_id"] == d["id"]               # 原始版本
    assert task["operator"] == "ops-li"
    assert task["reason"] == "下游补数据"
    assert task["status"] == "pending"


def test_submit_requires_operator_and_reason(client):
    process_normally(client, "RS-2", b'{"order": 2}')
    assert client.post("/admin/replays", json={"reason": "x"}).status_code == 422
    assert client.post("/admin/replays", json={"operator": "ops-li"}).status_code == 422
    assert client.post("/admin/replays",
                       json={"operator": "  ", "reason": "x"}).status_code == 422
    assert client.get("/admin/replays").json()["batches"] == []


# ---- 幂等：同一份内容不会生成第二个重放任务 -----------------------------------

def test_duplicate_submit_with_same_request_id_returns_original_batch(client):
    process_normally(client, "RS-3", b'{"order": 3}')
    r1 = submit(client, external_id="RS-3", request_id="req-dup")
    assert r1.status_code == 201
    r2 = submit(client, external_id="RS-3", request_id="req-dup")  # 重复提交（重试/双击）
    assert r2.status_code == 200
    assert r2.json()["result"] == "duplicate"
    assert r2.json()["batch_id"] == r1.json()["batch_id"]

    assert len(client.get("/admin/replays").json()["batches"]) == 1
    tasks = client.get(f"/admin/replays/{r1.json()['batch_id']}").json()["tasks"]
    assert len(tasks) == 1  # 没有生成第二个重放任务


def test_active_replay_blocks_second_task_for_same_content(client):
    process_normally(client, "RS-4", b'{"order": 4}')
    r1 = submit(client, external_id="RS-4")
    assert r1.status_code == 201

    # 同一内容在另一批仍有活动任务 -> 跳过，不生成第二个任务
    r2 = submit(client, external_id="RS-4")
    assert r2.status_code == 422
    assert r2.json()["skipped"][0]["reason"].startswith("active_replay_in_batch:")

    # 第一批执行完后，可以有意识地再次重放（那是新的批次）
    client.app.state.replay_worker.run_once()
    r3 = submit(client, external_id="RS-4")
    assert r3.status_code == 201 and r3.json()["total"] == 1


def test_submit_with_nothing_replayable_creates_no_batch(client):
    post(client, "RS-5", b'{"order": 5}')  # pending，仍在管线中
    r = submit(client, external_id="RS-5")
    assert r.status_code == 422
    assert r.json()["error"] == "no_replayable_deliveries"
    assert client.get("/admin/replays").json()["batches"] == []


# ---- 副作用：与正常处理同一套幂等保护 -----------------------------------------

def test_replay_effects_go_through_same_idempotent_outbox(client):
    d = process_normally(client, "RE-1", b'{"order": 1}')
    db = client.app.state.db
    original_key = db.query_one(
        "SELECT idempotency_key FROM outbox WHERE delivery_id=?", (d["id"],))["idempotency_key"]

    batch_id = submit(client, external_id="RE-1").json()["batch_id"]
    client.app.state.replay_worker.run_once()

    task = get_task(client, batch_id, "RE-1")
    assert task["status"] == "done"
    replay_row = db.query_one("SELECT * FROM outbox WHERE replay_task_id=?", (task["id"],))
    assert replay_row["idempotency_key"] != original_key  # 新键：重放是有意再次产生效果
    assert replay_row["status"] == "pending"

    # 派发走与正常处理同一条 outbox 链路
    client.app.state.worker.run_once()
    replay_row = db.query_one("SELECT * FROM outbox WHERE replay_task_id=?", (task["id"],))
    assert replay_row["status"] == "executed"
    sink = IdempotentSink(db)
    assert sink.applied_count(replay_row["idempotency_key"]) == 1
    assert sink.applied_count(original_key) == 1  # 原效果不受重放影响

    # 重跑两个 worker 都不会重复产生/重复派发
    client.app.state.replay_worker.run_once()
    client.app.state.worker.run_once()
    assert sink.applied_count(replay_row["idempotency_key"]) == 1
    assert db.query_one("SELECT COUNT(*) AS c FROM outbox WHERE replay_task_id=?",
                        (task["id"],))["c"] == 1

    # 批次自动收尾
    assert client.get(f"/admin/replays/{batch_id}").json()["batch"]["status"] == "completed"


def test_replay_effect_not_duplicated_across_restart(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as c:
        process_normally(c, "RE-2", b'{"order": 2}')
        submit(c, external_id="RE-2")
        c.app.state.replay_worker.run_once()  # 重放副作用落 outbox，尚未派发

        db = c.app.state.db
        row = db.query_one("SELECT * FROM outbox WHERE replay_task_id IS NOT NULL")
        key = row["idempotency_key"]

        # 模拟崩溃：副作用已发到下游，但还没来得及标记 executed
        sink = IdempotentSink(db)
        sink.send(key, row["effect_type"], json.loads(row["payload"]))
        assert sink.applied_count(key) == 1

        # “重启”：同一数据库上重建派发器重新派发，下游凭幂等键去重
        worker2 = Worker(db, c.app.state.settings)
        worker2._dispatch_outbox()
        assert sink.applied_count(key) == 1
        assert db.query_one("SELECT status FROM outbox WHERE id=?",
                            (row["id"],))["status"] == "executed"


# ---- 重启续跑 ----------------------------------------------------------------

def test_restart_resumes_unfinished_replay_tasks(tmp_path):
    app = make_app(tmp_path)
    with TestClient(app) as c:
        process_normally(c, "RR-1", b'{"order": 1}')
        batch_id = submit(c, external_id="RR-1").json()["batch_id"]
        task_id = get_task(c, batch_id, "RR-1")["id"]
        # 模拟崩溃：任务执行到一半（processing，已重试 2 次）
        with c.app.state.db.tx() as cur:
            cur.execute("UPDATE replay_tasks SET status='processing', attempts=2 WHERE id=?",
                        (task_id,))

    # 服务重启：同一数据库文件重新装配，启动时 recover 把卡住的任务退回待处理
    app2 = make_app(tmp_path)
    with TestClient(app2) as c2:
        task = get_task(c2, batch_id, "RR-1")
        assert task["status"] == "pending"   # 从上次位置继续，而不是丢失或重来
        assert task["attempts"] == 2
        recovered = c2.get("/admin/events", params={"type": "replay_task_recovered"}).json()["events"]
        assert len(recovered) == 1

        c2.app.state.replay_worker.run_once()
        assert get_task(c2, batch_id, "RR-1")["status"] == "done"
        assert c2.get(f"/admin/replays/{batch_id}").json()["batch"]["status"] == "completed"


# ---- 暂停 / 继续 -------------------------------------------------------------

def test_pause_and_resume(client):
    process_normally(client, "RP-A", b'{"order": 1}')
    process_normally(client, "RP-B", b'{"order": 2}')
    batch_id = submit(client, status="done").json()["batch_id"]

    r = client.post(f"/admin/replays/{batch_id}/pause", json={"operator": "ops-li"})
    assert r.json()["result"] == "paused"
    client.app.state.replay_worker.run_once()
    tasks = client.get(f"/admin/replays/{batch_id}").json()["tasks"]
    assert all(t["status"] == "pending" for t in tasks)  # 暂停期间不执行

    # 暂停中的批次不能重复暂停
    assert client.post(f"/admin/replays/{batch_id}/pause",
                       json={"operator": "ops-li"}).status_code == 409

    r = client.post(f"/admin/replays/{batch_id}/resume", json={"operator": "ops-li"})
    assert r.json()["result"] == "resumed"
    client.app.state.replay_worker.run_once()
    tasks = client.get(f"/admin/replays/{batch_id}").json()["tasks"]
    assert all(t["status"] == "done" for t in tasks)


def test_paused_batch_effects_not_dispatched(client):
    process_normally(client, "RP-C", b'{"order": 3}')
    batch_id = submit(client, external_id="RP-C").json()["batch_id"]
    client.app.state.replay_worker.run_once()  # 任务完成，重放副作用滞留 outbox

    db = client.app.state.db
    assert db.query_one("SELECT COUNT(*) AS c FROM sink_effects")["c"] == 1  # 原效果
    # 模拟「派发前一刻批次被暂停」
    with db.tx() as cur:
        cur.execute("UPDATE replay_batches SET status='paused' WHERE id=?", (batch_id,))
    client.app.state.worker.run_once()
    assert db.query_one("SELECT COUNT(*) AS c FROM sink_effects")["c"] == 1  # 滞留未发

    client.post(f"/admin/replays/{batch_id}/resume", json={"operator": "ops-li"})
    client.app.state.worker.run_once()
    assert db.query_one("SELECT COUNT(*) AS c FROM sink_effects")["c"] == 2  # 恢复后补发


# ---- 取消 --------------------------------------------------------------------

def test_cancel_stops_pending_tasks_and_stuck_effects(tmp_path):
    app = make_app(tmp_path, max_attempts=3)
    with TestClient(app) as c:
        process_normally(c, "RC-GOOD", b'{"order": 1}')
        quarantine(c, "RC-BAD", b'{"force_error": true}')  # 可重放的失败版本
        batch_id = submit(c).json()["batch_id"]            # 无筛选：两个版本都入批
        c.app.state.replay_worker.run_once()
        # GOOD 任务完成（副作用滞留 outbox），BAD 任务失败待重试 -> 批次仍在运行
        assert get_task(c, batch_id, "RC-GOOD")["status"] == "done"
        assert get_task(c, batch_id, "RC-BAD")["status"] == "pending"

        r = c.post(f"/admin/replays/{batch_id}/cancel",
                   json={"operator": "ops-li", "note": "改走线下处理"})
        assert r.json()["result"] == "cancelled"

        detail = c.get(f"/admin/replays/{batch_id}").json()
        assert detail["batch"]["status"] == "cancelled"
        assert detail["batch"]["done"] == 1 and detail["batch"]["cancelled"] == 1
        assert get_task(c, batch_id, "RC-BAD")["status"] == "cancelled"
        assert get_task(c, batch_id, "RC-GOOD")["status"] == "done"  # 已完成的不动

        # 滞留的重放副作用被同事务取消，之后不再派发
        db = c.app.state.db
        stuck = db.query_one("SELECT * FROM outbox WHERE replay_task_id IS NOT NULL")
        assert stuck["status"] == "cancelled"
        c.app.state.worker.run_once()
        assert db.query_one("SELECT COUNT(*) AS c FROM sink_effects")["c"] == 1

        # 取消是终态
        assert c.post(f"/admin/replays/{batch_id}/resume",
                      json={"operator": "ops-li"}).status_code == 409
        assert c.post(f"/admin/replays/{batch_id}/cancel",
                      json={"operator": "ops-li"}).status_code == 409

        events = c.get(f"/admin/replays/{batch_id}/events").json()["events"]
        types = {e["type"] for e in events}
        assert {"replay_batch_created", "replay_batch_cancelled", "effect_cancelled"} <= types


def test_cancel_while_processing_keeps_task_cancelled(tmp_path):
    """处理中的任务被取消：迟到的完成结果不得覆盖 cancelled，不落副作用、不写完成记录。"""
    app = make_app(tmp_path)
    with TestClient(app) as c:
        process_normally(c, "RC-FLY", b'{"order": 1}')
        batch_id = submit(c, external_id="RC-FLY").json()["batch_id"]

        # handler 执行期间（占位事务已提交、收尾事务未开始）运营取消批次
        def cancel_midway(delivery):
            r = c.post(f"/admin/replays/{batch_id}/cancel",
                       json={"operator": "ops-li", "note": "改走线下处理"})
            assert r.status_code == 200
            return business_handler(delivery)

        c.app.state.replay_worker.handler = cancel_midway
        c.app.state.replay_worker.run_once()

        # 任务保持 cancelled：不被迟到的 done 覆盖，批次计数不被污染
        task = get_task(c, batch_id, "RC-FLY")
        assert task["status"] == "cancelled"
        batch = c.get(f"/admin/replays/{batch_id}").json()["batch"]
        assert batch["status"] == "cancelled"
        assert batch["done"] == 0 and batch["cancelled"] == 1

        # 没有写入任何重放副作用，也没有完成记录（仅留一条 discarded 审计）
        db = c.app.state.db
        assert db.query_one("SELECT COUNT(*) AS c FROM outbox WHERE replay_task_id=?",
                            (task["id"],))["c"] == 0
        events = c.get(f"/admin/replays/{batch_id}/events").json()["events"]
        types = [e["type"] for e in events]
        assert "replay_task_done" not in types
        assert "replay_batch_cancelled" in types
        discarded = next(e for e in events
                         if e["type"] == "replay_task_completion_discarded")
        assert discarded["detail"]["task_status"] == "cancelled"

        # 派发器也无可派发：下游只有正常处理那一次效果
        c.app.state.worker.run_once()
        assert db.query_one("SELECT COUNT(*) AS c FROM sink_effects")["c"] == 1


def test_cancel_while_processing_then_handler_fails_keeps_cancelled(tmp_path):
    """处理中的任务被取消后 handler 才失败：不标记 failed、不安排重试、不会复活。"""
    app = make_app(tmp_path, max_attempts=3)
    with TestClient(app) as c:
        process_normally(c, "RC-FLY2", b'{"order": 2}')
        batch_id = submit(c, external_id="RC-FLY2").json()["batch_id"]

        def cancel_then_fail(delivery):
            r = c.post(f"/admin/replays/{batch_id}/cancel", json={"operator": "ops-li"})
            assert r.status_code == 200
            raise TransientError("下游又挂了")

        c.app.state.replay_worker.handler = cancel_then_fail
        c.app.state.replay_worker.run_once()

        # 保持 cancelled：不被 failed 覆盖，也不退回 pending 等待重试
        task = get_task(c, batch_id, "RC-FLY2")
        assert task["status"] == "cancelled"
        assert task["next_retry_at"] is None

        events = c.get(f"/admin/replays/{batch_id}/events").json()["events"]
        types = [e["type"] for e in events]
        assert "replay_task_failed" not in types
        assert "replay_task_retry_scheduled" not in types
        assert "replay_task_completion_discarded" in types

        # 再跑 worker 也不会复活已取消的任务
        c.app.state.replay_worker.run_once()
        assert get_task(c, batch_id, "RC-FLY2")["status"] == "cancelled"


# ---- 失败：单独重试，不阻塞其他编号 -------------------------------------------

def test_failed_task_retried_individually_without_blocking_others(tmp_path):
    app = make_app(tmp_path, max_attempts=2, retry_base_seconds=5.0)
    with TestClient(app) as c:
        process_normally(c, "RF-GOOD", b'{"order": 1}')
        process_normally(c, "RF-BAD", b'{"order": 2}')

        # 重放时 RF-BAD 前两次失败，之后恢复
        calls = {"n": 0}

        def flaky(delivery):
            if delivery["external_id"] == "RF-BAD" and calls["n"] < 2:
                calls["n"] += 1
                raise TransientError("下游又挂了")
            return business_handler(delivery)

        c.app.state.replay_worker.handler = flaky
        clock = [time.time()]
        c.app.state.replay_worker.clock = lambda: clock[0]

        batch_id = submit(c, status="done").json()["batch_id"]
        c.app.state.replay_worker.run_once()
        assert get_task(c, batch_id, "RF-GOOD")["status"] == "done"   # 其他编号不被阻塞
        bad = get_task(c, batch_id, "RF-BAD")
        assert bad["status"] == "pending" and bad["attempts"] == 1    # 安排退避重试

        # 退避期内不会再试
        c.app.state.replay_worker.run_once()
        assert get_task(c, batch_id, "RF-BAD")["attempts"] == 1

        # 到点再试仍失败 -> failed（只影响这一条）
        clock[0] += 60
        c.app.state.replay_worker.run_once()
        bad = get_task(c, batch_id, "RF-BAD")
        assert bad["status"] == "failed" and bad["attempts"] == 2
        batch = c.get(f"/admin/replays/{batch_id}").json()["batch"]
        assert batch["status"] == "completed_with_failures" and batch["failed"] == 1

        # 人工单独重试失败任务：批次重新打开，最终完成
        r = c.post(f"/admin/replays/tasks/{bad['id']}/retry", json={"operator": "ops-li"})
        assert r.json()["result"] == "requeued"
        batch = c.get(f"/admin/replays/{batch_id}").json()["batch"]
        assert batch["status"] == "running" and batch["failed"] == 0
        c.app.state.replay_worker.run_once()
        assert get_task(c, batch_id, "RF-BAD")["status"] == "done"
        assert c.get(f"/admin/replays/{batch_id}").json()["batch"]["status"] == "completed"

        events = c.get(f"/admin/replays/{batch_id}/events").json()["events"]
        types = [e["type"] for e in events]
        assert "replay_task_failed" in types
        assert "replay_task_retried" in types


def test_retry_rejects_non_failed_task(client):
    process_normally(client, "RF-3", b'{"order": 3}')
    batch_id = submit(client, external_id="RF-3").json()["batch_id"]
    task_id = get_task(client, batch_id, "RF-3")["id"]
    assert client.post(f"/admin/replays/tasks/{task_id}/retry",
                       json={"operator": "ops-li"}).status_code == 409


# ---- 审计：一次重放的完整记录 --------------------------------------------------

def test_batch_events_full_audit_trail(client):
    process_normally(client, "RA-1", b'{"order": 1}')
    batch_id = submit(client, external_id="RA-1", request_id="req-audit").json()["batch_id"]
    client.app.state.replay_worker.run_once()
    client.app.state.worker.run_once()

    events = client.get(f"/admin/replays/{batch_id}/events").json()["events"]
    ids = [e["id"] for e in events]
    assert ids == sorted(ids)  # 按时间正序
    types = [e["type"] for e in events]
    assert "replay_batch_created" in types
    assert "replay_task_processing" in types
    assert "replay_task_done" in types
    assert "effect_executed" in types
    assert "replay_batch_completed" in types
    assert all(e["detail"].get("replay_batch_id") == batch_id for e in events)
    created = next(e for e in events if e["type"] == "replay_batch_created")
    assert created["detail"]["operator"] == "ops-li"
    assert created["detail"]["reason"] == "下游补数据"

    # 可按单条任务过滤
    task_id = get_task(client, batch_id, "RA-1")["id"]
    task_events = client.get(f"/admin/replays/{batch_id}/events",
                             params={"task_id": task_id}).json()["events"]
    assert task_events
    assert all(e["detail"].get("replay_task_id") == task_id for e in task_events)
    assert any(e["type"] == "replay_task_done" for e in task_events)


# ---- 老库就地迁移 --------------------------------------------------------------

def test_existing_database_migrated_in_place(tmp_path):
    """老版本库（outbox 无 replay_task_id 列）打开时就地补列，新表照常建出。"""
    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.execute("""CREATE TABLE outbox (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        delivery_id INTEGER NOT NULL,
        effect_type TEXT NOT NULL,
        idempotency_key TEXT NOT NULL UNIQUE,
        payload TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        executed_at REAL,
        created_at REAL NOT NULL)""")
    conn.commit()
    conn.close()

    db = Database(db_path)
    cols = {r["name"] for r in db.query("PRAGMA table_info(outbox)")}
    assert "replay_task_id" in cols
    db.query("SELECT * FROM replay_batches")
    db.query("SELECT * FROM replay_tasks")
    db.close()

"""重放批次级并发配额 + 同一业务编号有序执行的端到端测试：

- 提交批次可指定 max_concurrency（整个批次最多同时处理多少条），额度不足时
  任务被挡下并有清楚的状态（blocked_reason）与审计（replay_task_blocked）；
- 同一 external_id 的多条历史版本按 created_at 先后执行，前一条未进终态
  （done/failed/cancelled）时后一条不能被领取；
- 批次详情展示当前占用（in_flight）、等待数量（waiting）和每条任务的阻塞原因；
- 暂停/取消/重启后配额正确回收（占用实时推导，无计数器可泄漏），任务不永久卡住。
"""
from __future__ import annotations

import json
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.handlers import business_handler
from app.ingest import content_hash_of
from app.main import create_app
from app.security import sign

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
    """投递并跑完正常处理管线（done），返回 delivery 行。"""
    post(client, external_id, body)
    client.app.state.worker.run_once()
    rows = client.get("/admin/deliveries", params={"external_id": external_id}).json()["deliveries"]
    assert rows[0]["status"] == "done"
    return rows[0]


def insert_done_delivery(db, external_id, payload: dict, created_at: float) -> int:
    """直接落库一个 done 状态的历史版本（可精确控制 created_at，用于有序执行测试）。"""
    body = json.dumps(payload, ensure_ascii=False).encode()
    with db.tx() as cur:
        cur.execute(
            """INSERT INTO deliveries
               (external_id, content_hash, payload, status, frozen, attempts,
                created_at, updated_at)
               VALUES (?,?,?,'done',0,0,?,?)""",
            (external_id, content_hash_of(body), body.decode(), created_at, created_at),
        )
        return cur.lastrowid


def submit(client, **kw):
    payload = {"operator": "ops-li", "reason": "下游补数据", **kw}
    return client.post("/admin/replays", json=payload)


def batch_view(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()


def tasks_by_id(client, batch_id):
    return {t["id"]: t for t in batch_view(client, batch_id)["tasks"]}


def batch_events(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}/events").json()["events"]


def set_task_status(client, task_id, status):
    """模拟任务被领取后进程崩溃/另一副本正在处理等中间状态。"""
    with client.app.state.db.tx() as cur:
        cur.execute("UPDATE replay_tasks SET status=? WHERE id=?", (status, task_id))


# ---- 提交：并发配额参数 -------------------------------------------------------

def test_max_concurrency_validation(client):
    process_normally(client, "QV-1", b'{"order": 1}')
    assert submit(client, max_concurrency=0).status_code == 422
    assert submit(client, max_concurrency=-2).status_code == 422
    assert client.get("/admin/replays").json()["batches"] == []

    r = submit(client, max_concurrency=2)
    assert r.status_code == 201
    batch = batch_view(client, r.json()["batch_id"])["batch"]
    assert batch["max_concurrency"] == 2
    assert batch["in_flight"] == 0 and batch["waiting"] == 1

    # 不指定则不限并发
    process_normally(client, "QV-2", b'{"order": 2}')
    r2 = submit(client, external_id="QV-2")
    assert r2.status_code == 201
    batch2 = batch_view(client, r2.json()["batch_id"])["batch"]
    assert batch2["max_concurrency"] is None


# ---- 批次级并发配额 -----------------------------------------------------------

def test_quota_exhausted_blocks_and_is_audited(client):
    for i in range(3):
        process_normally(client, f"Q-{i}", json.dumps({"order": i}).encode())
    batch_id = submit(client, max_concurrency=1).json()["batch_id"]
    worker = client.app.state.replay_worker

    snapshot = {}

    def midway(delivery):
        if not snapshot:
            # 第一条任务处理中（占用唯一配额）时：观察批次详情，并再跑一轮领取
            snapshot["detail"] = batch_view(client, batch_id)
            worker.run_once()
        return business_handler(delivery)

    worker.handler = midway
    worker.run_once()

    # 阻塞瞬间：1 条占用配额，其余 2 条等待且原因明确
    batch = snapshot["detail"]["batch"]
    assert batch["max_concurrency"] == 1
    assert batch["in_flight"] == 1 and batch["waiting"] == 2
    by_ext = {t["external_id"]: t for t in snapshot["detail"]["tasks"]}
    assert by_ext["Q-0"]["status"] == "processing"
    assert by_ext["Q-0"]["blocked_reason"] is None
    assert by_ext["Q-1"]["blocked_reason"] == "quota_exhausted:1/1"
    assert by_ext["Q-2"]["blocked_reason"] == "quota_exhausted:1/1"

    # 额度不足有审计记录（每条被挡任务一条，原因变化才写）
    blocked = [e for e in batch_events(client, batch_id) if e["type"] == "replay_task_blocked"]
    assert len(blocked) == 2
    assert all(e["detail"]["reason"] == "quota_exhausted:1/1" for e in blocked)
    assert all(e["detail"]["max_concurrency"] == 1 for e in blocked)

    # 配额随任务完成不断释放：最终全部完成，没有任务卡住
    final = batch_view(client, batch_id)
    assert final["batch"]["status"] == "completed"
    assert final["batch"]["in_flight"] == 0 and final["batch"]["waiting"] == 0
    assert all(t["status"] == "done" and t["blocked_reason"] is None
               for t in final["tasks"])


def test_quota_released_when_task_fails_into_backoff(tmp_path):
    """任务失败退回退避时释放槽位：同轮下一条立即被领取，不被失败任务堵住。"""
    app = make_app(tmp_path, max_attempts=3, retry_base_seconds=60.0)
    with TestClient(app) as c:
        db = c.app.state.db
        base = time.time() - 1000
        insert_done_delivery(db, "QF-0", {"force_error": True}, base)
        insert_done_delivery(db, "QF-1", {"v": 1}, base + 1)
        batch_id = submit(c, max_concurrency=1).json()["batch_id"]
        worker = c.app.state.replay_worker
        clock = [time.time()]
        worker.clock = lambda: clock[0]

        worker.run_once()
        by_ext = {t["external_id"]: t for t in batch_view(c, batch_id)["tasks"]}
        assert by_ext["QF-0"]["status"] == "pending"   # 退避等待重试（已释放槽位）
        assert by_ext["QF-0"]["attempts"] == 1
        assert by_ext["QF-1"]["status"] == "done"      # 同轮被领取并完成


def test_quota_released_when_restart_recovers_processing_tasks(tmp_path):
    """服务重启：卡在 processing 的任务退回 pending，占用的配额随之释放。"""
    app = make_app(tmp_path)
    with TestClient(app) as c:
        process_normally(c, "QR-1", b'{"order": 1}')
        process_normally(c, "QR-2", b'{"order": 2}')
        batch_id = submit(c, max_concurrency=1).json()["batch_id"]
        t1, t2 = [t["id"] for t in batch_view(c, batch_id)["tasks"]]

        # 模拟崩溃：t1 被领取（processing，占用唯一槽位）后进程死掉
        set_task_status(c, t1, "processing")
        c.app.state.replay_worker.run_once()
        view = tasks_by_id(c, batch_id)
        assert view[t2]["status"] == "pending"
        assert view[t2]["blocked_reason"] == "quota_exhausted:1/1"

    # 重启：recover 把 t1 退回 pending，配额回收，两条任务都能跑完
    app2 = make_app(tmp_path)
    with TestClient(app2) as c2:
        assert batch_view(c2, batch_id)["batch"]["in_flight"] == 0
        c2.app.state.replay_worker.run_once()
        final = batch_view(c2, batch_id)
        assert all(t["status"] == "done" for t in final["tasks"])
        assert final["batch"]["status"] == "completed"


def test_cancel_releases_held_quota_and_clears_blocked_reason(client):
    db = client.app.state.db
    base = time.time() - 1000
    for i in range(3):
        insert_done_delivery(db, f"QC-{i}", {"v": i}, base + i)
    batch_id = submit(client, max_concurrency=1).json()["batch_id"]
    t0 = batch_view(client, batch_id)["tasks"][0]["id"]

    # t0 占用唯一槽位（处理中），其余两条被额度挡下
    set_task_status(client, t0, "processing")
    client.app.state.replay_worker.run_once()
    detail = batch_view(client, batch_id)
    assert detail["batch"]["in_flight"] == 1
    assert [t["blocked_reason"] for t in detail["tasks"][1:]] == ["quota_exhausted:1/1"] * 2

    # 取消：占用与等待清零，阻塞原因随终态清空
    r = client.post(f"/admin/replays/{batch_id}/cancel", json={"operator": "ops-li"})
    assert r.json()["result"] == "cancelled"
    detail = batch_view(client, batch_id)
    assert detail["batch"]["in_flight"] == 0 and detail["batch"]["waiting"] == 0
    assert all(t["status"] == "cancelled" for t in detail["tasks"])
    assert all(t["blocked_reason"] is None for t in detail["tasks"])

    # 取消后 worker 不再领取，任务不会复活也不会卡住
    client.app.state.replay_worker.run_once()
    assert all(t["status"] == "cancelled" for t in batch_view(client, batch_id)["tasks"])
    types = [e["type"] for e in batch_events(client, batch_id)]
    assert "replay_task_blocked" in types
    assert "replay_batch_cancelled" in types


# ---- 同一业务编号的有序执行 ----------------------------------------------------

def test_same_external_id_versions_execute_in_created_order(client):
    db = client.app.state.db
    base = time.time() - 1000
    # 同一编号 3 个历史版本（前两个 created_at 相同，按 delivery_id 决胜）
    ids = [insert_done_delivery(db, "ORD-1", {"v": 0}, base),
           insert_done_delivery(db, "ORD-1", {"v": 1}, base),
           insert_done_delivery(db, "ORD-1", {"v": 2}, base + 1)]
    other = insert_done_delivery(db, "ORD-2", {"v": 0}, base)
    batch_id = submit(client).json()["batch_id"]

    tasks = {t["delivery_id"]: t for t in batch_view(client, batch_id)["tasks"]}
    # 任务落盘时快照了版本时间（有序执行的排序键）
    assert tasks[ids[0]]["delivery_created_at"] == base
    t_v0, t_v1, t_v2 = (tasks[i]["id"] for i in ids)

    snapshot = {}

    def midway(delivery):
        if delivery["external_id"] == "ORD-1" and not snapshot:
            # v0 处理中：v1/v2 必须等各自前序，其他编号不受影响
            snapshot["detail"] = batch_view(client, batch_id)
        return business_handler(delivery)

    client.app.state.replay_worker.handler = midway
    client.app.state.replay_worker.run_once()

    snap = {t["delivery_id"]: t for t in snapshot["detail"]["tasks"]}
    assert snap[ids[1]]["blocked_reason"] == f"waiting_predecessor:{t_v0}"
    assert snap[ids[2]]["blocked_reason"] == f"waiting_predecessor:{t_v1}"
    assert snap[other]["blocked_reason"] is None  # 其他编号不受顺序约束

    # 执行顺序：ORD-1 的三个版本严格按 created_at（再按 delivery_id）先后
    processing = [e for e in batch_events(client, batch_id)
                  if e["type"] == "replay_task_processing" and e["external_id"] == "ORD-1"]
    assert [e["detail"]["replay_task_id"] for e in processing] == [t_v0, t_v1, t_v2]
    assert batch_view(client, batch_id)["batch"]["status"] == "completed"


def test_waiting_predecessor_blocked_and_audited(client):
    db = client.app.state.db
    base = time.time() - 1000
    d0 = insert_done_delivery(db, "ORD-W", {"v": 0}, base)
    d1 = insert_done_delivery(db, "ORD-W", {"v": 1}, base + 1)
    batch_id = submit(client).json()["batch_id"]
    tasks = {t["delivery_id"]: t for t in batch_view(client, batch_id)["tasks"]}
    t0, t1 = tasks[d0]["id"], tasks[d1]["id"]

    # 模拟前序版本正在处理（另一副本/崩溃前领取）：后一条不能被领取
    set_task_status(client, t0, "processing")
    worker = client.app.state.replay_worker
    worker.run_once()
    worker.run_once()  # 原因不变时不重复刷审计

    detail = batch_view(client, batch_id)
    assert detail["batch"]["in_flight"] == 1
    assert detail["batch"]["waiting"] == 1
    view = tasks_by_id(client, batch_id)[t1]
    assert view["status"] == "pending"
    assert view["blocked_reason"] == f"waiting_predecessor:{t0}"

    blocked = [e for e in batch_events(client, batch_id) if e["type"] == "replay_task_blocked"]
    assert len(blocked) == 1
    assert blocked[0]["detail"]["reason"] == f"waiting_predecessor:{t0}"
    assert blocked[0]["detail"]["predecessor_status"] == "processing"

    # 前序进入终态（这里模拟重启回收后跑完）后，后一条放行，批次收尾
    worker.recover()
    worker.run_once()
    final = batch_view(client, batch_id)
    assert all(t["status"] == "done" for t in final["tasks"])
    assert final["batch"]["status"] == "completed"


def test_predecessor_retry_then_terminal_failure_releases_successor(tmp_path):
    """前序失败退避期间（非终态）后一条等待；前序终态失败后后一条放行。"""
    app = make_app(tmp_path, max_attempts=2, retry_base_seconds=5.0)
    with TestClient(app) as c:
        db = c.app.state.db
        base = time.time() - 1000
        d0 = insert_done_delivery(db, "ORD-F", {"v": 0, "force_error": True}, base)
        d1 = insert_done_delivery(db, "ORD-F", {"v": 1}, base + 1)
        batch_id = submit(c).json()["batch_id"]
        tasks = {t["delivery_id"]: t for t in batch_view(c, batch_id)["tasks"]}
        t0, t1 = tasks[d0]["id"], tasks[d1]["id"]

        worker = c.app.state.replay_worker
        clock = [time.time()]
        worker.clock = lambda: clock[0]
        worker.run_once()

        # 前序第一次失败：退避等待重试（非终态），后一条继续等待
        view = tasks_by_id(c, batch_id)
        assert view[t0]["status"] == "pending" and view[t0]["attempts"] == 1
        assert view[t1]["status"] == "pending"
        assert view[t1]["blocked_reason"] == f"waiting_predecessor:{t0}"

        # 前序到点再试仍失败 -> failed（终态）-> 后一条放行执行
        clock[0] += 60
        worker.run_once()
        view = tasks_by_id(c, batch_id)
        assert view[t0]["status"] == "failed"
        assert view[t1]["status"] == "done"
        assert batch_view(c, batch_id)["batch"]["status"] == "completed_with_failures"

        types = [e["type"] for e in batch_events(c, batch_id)]
        assert "replay_task_retry_scheduled" in types
        assert "replay_task_failed" in types
        assert "replay_task_blocked" in types


# ---- 暂停：状态展示与恢复 ------------------------------------------------------

def test_paused_batch_shows_blocked_reason_and_resume_recovers(client):
    process_normally(client, "QP-1", b'{"order": 1}')
    process_normally(client, "QP-2", b'{"order": 2}')
    batch_id = submit(client, max_concurrency=1).json()["batch_id"]

    client.post(f"/admin/replays/{batch_id}/pause", json={"operator": "ops-li"})
    detail = batch_view(client, batch_id)
    assert detail["batch"]["waiting"] == 2
    assert all(t["blocked_reason"] == "batch_paused" for t in detail["tasks"])

    client.app.state.replay_worker.run_once()
    assert all(t["status"] == "pending" for t in batch_view(client, batch_id)["tasks"])

    client.post(f"/admin/replays/{batch_id}/resume", json={"operator": "ops-li"})
    client.app.state.replay_worker.run_once()
    final = batch_view(client, batch_id)
    assert all(t["status"] == "done" for t in final["tasks"])
    assert final["batch"]["status"] == "completed"


# ---- 老库就地迁移 --------------------------------------------------------------

def test_existing_replay_tables_migrated_in_place(tmp_path):
    """老库（replay 表无配额/排序/阻塞列）打开时就地补列，排序键从 deliveries 回填。"""
    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
    CREATE TABLE deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_id TEXT NOT NULL,
        content_hash TEXT NOT NULL,
        payload TEXT NOT NULL,
        status TEXT NOT NULL,
        frozen INTEGER NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL,
        checkpoint TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        UNIQUE (external_id, content_hash));
    CREATE TABLE replay_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        request_id TEXT UNIQUE,
        operator TEXT NOT NULL,
        reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running',
        filters TEXT NOT NULL DEFAULT '{}',
        total INTEGER NOT NULL DEFAULT 0,
        done INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0,
        cancelled INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        finished_at REAL);
    CREATE TABLE replay_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        batch_id INTEGER NOT NULL,
        delivery_id INTEGER NOT NULL,
        external_id TEXT NOT NULL,
        operator TEXT NOT NULL,
        reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL,
        checkpoint TEXT,
        last_error TEXT,
        created_at REAL NOT NULL,
        updated_at REAL NOT NULL,
        finished_at REAL,
        UNIQUE (batch_id, delivery_id));
    """)
    conn.execute("INSERT INTO deliveries (external_id, content_hash, payload, status,"
                 " created_at, updated_at) VALUES ('M-1','h','{}','done',123.5,123.5)")
    conn.execute("INSERT INTO replay_batches (operator, reason, total, created_at, updated_at)"
                 " VALUES ('ops','r',1,100,100)")
    conn.execute("INSERT INTO replay_tasks (batch_id, delivery_id, external_id, operator,"
                 " reason, created_at, updated_at) VALUES (1,1,'M-1','ops','r',100,100)")
    conn.commit()
    conn.close()

    db = Database(db_path)
    batch_cols = {r["name"] for r in db.query("PRAGMA table_info(replay_batches)")}
    task_cols = {r["name"] for r in db.query("PRAGMA table_info(replay_tasks)")}
    assert "max_concurrency" in batch_cols
    assert {"delivery_created_at", "blocked_reason"} <= task_cols
    # 存量任务的排序键从原始版本的落盘时间回填
    row = db.query_one("SELECT delivery_created_at FROM replay_tasks WHERE id=1")
    assert row["delivery_created_at"] == 123.5
    db.close()

"""高风险重放批次审批流程的端到端测试：

- 提交时可标记 risk_level=high 与审批说明 approval_note：批次进入
  pending_approval，未由非发起人批准前 worker 不能领取任务；
- 批准必须由不同于发起人的运营人员显式做出（POST .../approve），批准后进入
  running 才开始执行；拒绝（POST .../reject，必填原因）把未执行任务整体取消；
- 审批超过 approval_deadline 未决由 worker 扫描释放（整体取消、释放占用）；
- 审批状态与操作者在批次详情中展示；审批结果、拒绝原因、超时释放与批准后的
  执行全部写审计；
- request_id 重复提交与重复批准/拒绝都不会产生第二次执行。
"""
from __future__ import annotations

import json
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"
OLD_SECRET = "old-secret"
FAR_FUTURE = "2099-01-01T00:00:00Z"

SUBMITTER = "ops-li"
APPROVER = "ops-wang"


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
        replay_approval_timeout_seconds=overrides.get(
            "replay_approval_timeout_seconds", 3600.0),
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


def submit(client, **kw):
    payload = {"operator": SUBMITTER, "reason": "下游补数据", **kw}
    return client.post("/admin/replays", json=payload)


def submit_high(client, external_id=None, **kw):
    payload = {"risk_level": "high",
               "approval_note": "涉及资金类回调补发，需值班主管确认"}
    if external_id is not None:
        payload["external_id"] = external_id
    payload.update(kw)
    return submit(client, **payload)


def detail(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()


def events(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}/events").json()["events"]


def task_for(client, batch_id, external_id):
    return next(t for t in detail(client, batch_id)["tasks"]
                if t["external_id"] == external_id)


# ---- 提交：高风险批次待审批 ----------------------------------------------------

def test_high_risk_submit_waits_for_approval(client):
    process_normally(client, "AP-1", b'{"order": 1}')
    r = submit_high(client, "AP-1")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "pending_approval"
    assert body["approval_status"] == "pending"
    assert body["approval_deadline"] > time.time()
    batch_id = body["batch_id"]

    d = detail(client, batch_id)
    batch = d["batch"]
    assert batch["status"] == "pending_approval"
    assert batch["risk_level"] == "high"
    assert batch["approval_note"] == "涉及资金类回调补发，需值班主管确认"
    # 审批状态与操作者：发起人已知、审批人尚未出现
    ap = batch["approval"]
    assert ap["status"] == "pending"
    assert ap["submitted_by"] == SUBMITTER
    assert ap["approver"] is None and ap["approved_at"] is None
    assert ap["deadline"] == body["approval_deadline"]
    # 未批准：任务仍是 pending，阻塞原因明确为等待审批
    task = d["tasks"][0]
    assert task["status"] == "pending"
    assert task["blocked_reason"] == "awaiting_approval"
    assert d["batch"]["waiting"] == 1 and d["batch"]["in_flight"] == 0

    # worker 任何轮次都不能领取；被挡下时原因只变化时落一次审计（轮询不刷表）
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-1")["status"] == "pending"
    blocked = [e for e in events(client, batch_id) if e["type"] == "replay_task_blocked"]
    assert len(blocked) == 1
    assert blocked[0]["detail"]["reason"] == "awaiting_approval"
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-1")["status"] == "pending"
    blocked = [e for e in events(client, batch_id) if e["type"] == "replay_task_blocked"]
    assert len(blocked) == 1


def test_normal_risk_submit_runs_immediately(client):
    process_normally(client, "AP-0", b'{"order": 0}')
    r = submit(client, external_id="AP-0")
    assert r.status_code == 201
    assert r.json()["status"] == "running"
    assert r.json()["approval_status"] == "not_required"
    client.app.state.replay_worker.run_once()
    assert task_for(client, r.json()["batch_id"], "AP-0")["status"] == "done"
    d = detail(client, r.json()["batch_id"])["batch"]
    assert d["approval"]["status"] == "not_required"
    assert d["approval"]["approver"] is None


def test_high_risk_requires_approval_note_and_valid_level(client):
    process_normally(client, "AP-V", b'{"order": 1}')
    assert submit(client, external_id="AP-V", risk_level="high").status_code == 422
    assert submit(client, external_id="AP-V", risk_level="high",
                  approval_note="   ").status_code == 422
    assert submit(client, external_id="AP-V", risk_level="weird",
                  approval_note="x").status_code == 422
    assert client.get("/admin/replays").json()["batches"] == []


def test_pending_approval_holds_content_against_other_batches(client):
    """待审批批次占住对应内容：不能通过另开一批绕过审批。"""
    process_normally(client, "AP-HOLD", b'{"order": 1}')
    first = submit_high(client, "AP-HOLD").json()["batch_id"]

    r = submit(client, external_id="AP-HOLD")
    assert r.status_code == 422
    assert r.json()["skipped"][0]["reason"] == f"active_replay_in_batch:{first}"

    # 拒绝后占用释放：可以重新提交一批（新的审批）
    rj = client.post(f"/admin/replays/{first}/reject",
                     json={"operator": APPROVER, "reason": "窗口不对，改期再放"})
    assert rj.status_code == 200
    r2 = submit_high(client, "AP-HOLD")
    assert r2.status_code == 201
    assert r2.json()["status"] == "pending_approval"


# ---- 批准 --------------------------------------------------------------------

def test_claim_attempt_blocked_then_cleared_on_approval(client):
    """worker 领取尝试被审批闸门挡下时落一次 blocked 审计；批准后放行并清掉原因。"""
    process_normally(client, "AP-B", b'{"order": 1}')
    batch_id = submit_high(client, "AP-B").json()["batch_id"]
    task_id = task_for(client, batch_id, "AP-B")["id"]

    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-B")["status"] == "pending"
    blocked = [e for e in events(client, batch_id) if e["type"] == "replay_task_blocked"]
    assert len(blocked) == 1
    assert blocked[0]["detail"]["reason"] == "awaiting_approval"
    assert blocked[0]["detail"]["replay_task_id"] == task_id

    client.post(f"/admin/replays/{batch_id}/approve", json={"operator": APPROVER})
    client.app.state.replay_worker.run_once()
    t = task_for(client, batch_id, "AP-B")
    assert t["status"] == "done" and t["blocked_reason"] is None

def test_approve_by_different_operator_unblocks_execution(client):
    process_normally(client, "AP-2", b'{"order": 2}')
    batch_id = submit_high(client, "AP-2").json()["batch_id"]

    # 发起人本人不能批准自己的批次（职责分离）
    r = client.post(f"/admin/replays/{batch_id}/approve",
                    json={"operator": SUBMITTER, "note": "self"})
    assert r.status_code == 403
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-2")["status"] == "pending"

    # 另一个运营人员明确批准
    r = client.post(f"/admin/replays/{batch_id}/approve",
                    json={"operator": APPROVER, "note": "已电话核实"})
    assert r.status_code == 200
    assert r.json() == {"result": "approved", "batch_id": batch_id, "status": "running"}

    d = detail(client, batch_id)["batch"]
    assert d["status"] == "running"
    assert d["approval"]["status"] == "approved"
    assert d["approval"]["approver"] == APPROVER
    assert d["approval"]["approved_at"] is not None
    assert d["approval"]["rejection_reason"] is None

    # 批准后的执行照常走，并写完整审计（批准 + processing + done + completed）
    client.app.state.replay_worker.run_once()
    client.app.state.worker.run_once()
    assert task_for(client, batch_id, "AP-2")["status"] == "done"
    assert detail(client, batch_id)["batch"]["status"] == "completed"
    types = [e["type"] for e in events(client, batch_id)]
    assert types == [
        "replay_batch_created", "replay_task_blocked",
        "replay_batch_approved",
        "replay_task_processing", "replay_task_done",
        "replay_batch_completed", "effect_executed"]
    blocked = next(e for e in events(client, batch_id)
                   if e["type"] == "replay_task_blocked")
    assert blocked["detail"]["reason"] == "awaiting_approval"
    approved = next(e for e in events(client, batch_id)
                    if e["type"] == "replay_batch_approved")
    assert approved["detail"]["operator"] == APPROVER
    assert approved["detail"]["submitted_by"] == SUBMITTER
    assert approved["detail"]["note"] == "已电话核实"
    created = next(e for e in events(client, batch_id)
                   if e["type"] == "replay_batch_created")
    assert created["detail"]["risk_level"] == "high"
    assert created["detail"]["approval_deadline"] is not None


def test_cannot_approve_normal_batch_or_with_empty_operator(client):
    process_normally(client, "AP-N", b'{"order": 1}')
    batch_id = submit(client, external_id="AP-N").json()["batch_id"]
    # 普通批次无需审批
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 409
    # 审批人不可为空
    process_normally(client, "AP-N2", b'{"order": 2}')
    hid = submit_high(client, "AP-N2").json()["batch_id"]
    assert client.post(f"/admin/replays/{hid}/approve",
                       json={"operator": "  "}).status_code == 422
    # 不存在的批次 404
    assert client.post("/admin/replays/9999/approve",
                       json={"operator": APPROVER}).status_code == 404


def test_duplicate_approval_has_no_second_effect(client):
    """重复批准：只有第一次放行执行，之后的批准被 409 挡下，没有第二次执行。"""
    process_normally(client, "AP-DUP", b'{"order": 1}')
    batch_id = submit_high(client, "AP-DUP", request_id="req-ap-dup").json()["batch_id"]
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 200
    # 再次批准（另一人双击/重试）-> 409，不产生第二条审批事件
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": "ops-zhao"}).status_code == 409
    approvals = [e for e in events(client, batch_id)
                 if e["type"] == "replay_batch_approved"]
    assert len(approvals) == 1 and approvals[0]["detail"]["operator"] == APPROVER

    # request_id 重复提交返回原批次，不生成第二批任务/第二次执行
    dup = submit_high(client, "AP-DUP", request_id="req-ap-dup")
    assert dup.status_code == 200
    assert dup.json()["result"] == "duplicate"
    assert dup.json()["batch_id"] == batch_id
    assert len(client.get("/admin/replays").json()["batches"]) == 1

    client.app.state.replay_worker.run_once()
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-DUP")["status"] == "done"
    assert len(detail(client, batch_id)["tasks"]) == 1
    types = [e["type"] for e in events(client, batch_id)]
    assert types.count("replay_task_done") == 1
    assert types.count("replay_batch_completed") == 1


def test_approve_after_deadline_still_releases_before_worker_sweep(client):
    """截止时间过后但 worker 尚未扫描释放：人工显式批准仍然有效（超时只自动释放）。"""
    process_normally(client, "AP-LATE", b'{"order": 1}')
    batch_id = submit_high(client, "AP-LATE").json()["batch_id"]
    # 把截止时间拨到过去（尚未触发扫描）
    with client.app.state.db.tx() as cur:
        cur.execute("UPDATE replay_batches SET approval_deadline=? WHERE id=?",
                    (time.time() - 1, batch_id))
    d = detail(client, batch_id)["batch"]
    assert d["approval"]["expired_on_time"] is True
    assert d["status"] == "pending_approval"  # 状态本身仍待决

    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 200
    client.app.state.replay_worker.run_once()  # 扫描不会再取消已批准的批次
    assert task_for(client, batch_id, "AP-LATE")["status"] == "done"
    assert detail(client, batch_id)["batch"]["status"] == "completed"
    assert not any(e["type"] == "replay_batch_approval_expired"
                   for e in events(client, batch_id))


# ---- 拒绝 --------------------------------------------------------------------

def test_reject_cancels_tasks_records_reason_and_frees_hold(client):
    process_normally(client, "AP-R1", b'{"order": 1}')
    process_normally(client, "AP-R2", b'{"order": 2}')
    batch_id = submit_high(client, status="done").json()["batch_id"]

    # 拒绝原因必填
    assert client.post(f"/admin/replays/{batch_id}/reject",
                       json={"operator": APPROVER}).status_code == 422
    # 发起人不能拒绝自己的批次
    assert client.post(f"/admin/replays/{batch_id}/reject",
                       json={"operator": SUBMITTER, "reason": "x"}).status_code == 403

    r = client.post(f"/admin/replays/{batch_id}/reject",
                    json={"operator": APPROVER, "reason": "影响面评估不通过",
                          "note": "等下游就绪窗口"})
    assert r.status_code == 200
    assert r.json()["cancelled_tasks"] == 2

    d = detail(client, batch_id)
    assert d["batch"]["status"] == "rejected"
    assert d["batch"]["cancelled"] == 2
    ap = d["batch"]["approval"]
    assert ap["status"] == "rejected"
    assert ap["approver"] == APPROVER
    assert ap["rejection_reason"] == "影响面评估不通过"
    assert all(t["status"] == "cancelled" for t in d["tasks"])
    assert all(t["blocked_reason"] is None for t in d["tasks"])

    # 拒绝是终态：worker 不领取；暂停/继续/再拒绝/再批准都不可用
    client.app.state.replay_worker.run_once()
    assert all(t["status"] == "cancelled" for t in detail(client, batch_id)["tasks"])
    assert client.post(f"/admin/replays/{batch_id}/pause",
                       json={"operator": APPROVER}).status_code == 409
    assert client.post(f"/admin/replays/{batch_id}/reject",
                       json={"operator": "ops-zhao", "reason": "again"}).status_code == 409
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": "ops-zhao"}).status_code == 409

    ev = next(e for e in events(client, batch_id)
              if e["type"] == "replay_batch_rejected")
    assert ev["detail"]["operator"] == APPROVER
    assert ev["detail"]["submitted_by"] == SUBMITTER
    assert ev["detail"]["reason"] == "影响面评估不通过"
    assert ev["detail"]["note"] == "等下游就绪窗口"
    assert ev["detail"]["cancelled_tasks"] == 2

    # 占用释放：同内容可以提交新批次
    r2 = submit(client, external_id="AP-R1")
    assert r2.status_code == 201 and r2.json()["total"] == 1


def test_duplicate_reject_after_approve_is_refused(client):
    """先批准后拒绝（或反之）：条件状态转移只生效一次，不会推翻已生效的决定。"""
    process_normally(client, "AP-RA", b'{"order": 1}')
    batch_id = submit_high(client, "AP-RA").json()["batch_id"]
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 200
    # 批准已生效 -> 拒绝必须被挡下，批次继续跑
    assert client.post(f"/admin/replays/{batch_id}/reject",
                       json={"operator": "ops-zhao", "reason": "late"}).status_code == 409
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "AP-RA")["status"] == "done"
    assert detail(client, batch_id)["batch"]["status"] == "completed"
    types = [e["type"] for e in events(client, batch_id)]
    assert "replay_batch_rejected" not in types


# ---- 审批超时 ----------------------------------------------------------------

def test_approval_timeout_releases_batch_and_is_audited(client):
    process_normally(client, "AP-T1", b'{"order": 1}')
    process_normally(client, "AP-T2", b'{"order": 2}')
    batch_id = submit_high(client, status="done").json()["batch_id"]
    deadline = detail(client, batch_id)["batch"]["approval_deadline"]

    worker = client.app.state.replay_worker
    clock = [deadline - 1]  # 还没到期：worker 不动批次
    worker.clock = lambda: clock[0]
    worker.run_once()
    d = detail(client, batch_id)
    assert d["batch"]["status"] == "pending_approval"
    assert all(t["status"] == "pending" for t in d["tasks"])
    assert not any(e["type"] == "replay_batch_approval_expired"
                   for e in events(client, batch_id))

    # 到点：worker 先扫描释放，再领取——任务绝不可能在超时批次上被执行
    clock[0] = deadline
    worker.run_once()
    d = detail(client, batch_id)
    assert d["batch"]["status"] == "cancelled"
    assert d["batch"]["finished_at"] is not None
    assert d["batch"]["approval"]["status"] == "expired"
    assert all(t["status"] == "cancelled" for t in d["tasks"])
    assert all(t["blocked_reason"] is None for t in d["tasks"])

    ev = next(e for e in events(client, batch_id)
              if e["type"] == "replay_batch_approval_expired")
    assert ev["detail"]["submitted_by"] == SUBMITTER
    assert ev["detail"]["approval_deadline"] == deadline
    assert ev["detail"]["cancelled_tasks"] == 2

    # 重复扫描不产生第二次效果
    clock[0] = deadline + 1000
    worker.run_once()
    expired = [e for e in events(client, batch_id)
               if e["type"] == "replay_batch_approval_expired"]
    assert len(expired) == 1
    assert detail(client, batch_id)["batch"]["cancelled"] == 2

    # 占用释放：同内容可以提交新批次
    assert submit(client, external_id="AP-T1").status_code == 201


def test_timeout_does_not_touch_approved_batch(client):
    """截止时刻人工批准与 worker 扫描竞争：已批准的批次不会被超时取消。"""
    process_normally(client, "AP-TA", b'{"order": 1}')
    batch_id = submit_high(client, "AP-TA").json()["batch_id"]
    deadline = detail(client, batch_id)["batch"]["approval_deadline"]
    worker = client.app.state.replay_worker
    worker.clock = lambda: deadline + 10
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 200
    worker.run_once()  # 扫描只挑 pending 状态，已批准批次照常执行
    assert task_for(client, batch_id, "AP-TA")["status"] == "done"
    assert detail(client, batch_id)["batch"]["status"] == "completed"
    assert not any(e["type"] == "replay_batch_approval_expired"
                   for e in events(client, batch_id))


def test_timeout_setting_applied_at_submit(tmp_path):
    app = make_app(tmp_path, replay_approval_timeout_seconds=120.0)
    with TestClient(app) as c:
        process_normally(c, "AP-TS", b'{"order": 1}')
        before = time.time()
        r = submit_high(c, "AP-TS")
        deadline = r.json()["approval_deadline"]
        assert before + 120.0 - 1 <= deadline <= before + 120.0 + 1


def test_approval_required_even_when_task_done_before_decision(client):
    """即使绕过领取闸门把任务置为 done，outbox 派发同样要求批次已批准/在执行。"""
    from app.handlers import IdempotentSink

    process_normally(client, "AP-DISP", b'{"order": 1}')
    batch_id = submit_high(client, "AP-DISP").json()["batch_id"]
    task_id = task_for(client, batch_id, "AP-DISP")["id"]
    db = client.app.state.db

    # 直接构造一条「任务已完成、重放副作用滞留 outbox，但批次仍待审批」的状态
    with db.tx() as cur:
        cur.execute("UPDATE replay_tasks SET status='done', finished_at=? WHERE id=?",
                    (time.time(), task_id))
        cur.execute(
            """INSERT INTO outbox (delivery_id, replay_task_id, effect_type,
               idempotency_key, payload, created_at)
               SELECT t.delivery_id, t.id, 'downstream.notify',
                      'replay-guard-key', d.payload, ?
               FROM replay_tasks t JOIN deliveries d ON d.id=t.delivery_id
               WHERE t.id=?""",
            (time.time(), task_id))
    client.app.state.worker.run_once()  # 派发器不得放行待审批批次的副作用
    sink = IdempotentSink(db)
    assert sink.applied_count("replay-guard-key") == 0
    assert db.query_one("SELECT status FROM outbox WHERE idempotency_key=?",
                        ("replay-guard-key",))["status"] == "pending"

    client.post(f"/admin/replays/{batch_id}/approve", json={"operator": APPROVER})
    client.app.state.replay_worker.run_once()  # 批准后批次收尾 completed
    client.app.state.worker.run_once()         # 副作用此时才派发
    assert sink.applied_count("replay-guard-key") == 1
    assert db.query_one("SELECT status FROM outbox WHERE idempotency_key=?",
                        ("replay-guard-key",))["status"] == "executed"
    assert detail(client, batch_id)["batch"]["approval"]["status"] == "approved"


# ---- 待审批期间取消 -----------------------------------------------------------

def test_cancel_pending_approval_batch(client):
    process_normally(client, "AP-C", b'{"order": 1}')
    batch_id = submit_high(client, "AP-C").json()["batch_id"]

    r = client.post(f"/admin/replays/{batch_id}/cancel",
                    json={"operator": SUBMITTER, "note": "提单有误，撤回"})
    assert r.status_code == 200
    d = detail(client, batch_id)
    assert d["batch"]["status"] == "cancelled"
    assert d["tasks"][0]["status"] == "cancelled"
    ev = next(e for e in events(client, batch_id)
              if e["type"] == "replay_batch_cancelled")
    assert ev["detail"]["was_awaiting_approval"] is True

    # 终态：再批准/拒绝/超时扫描都不能复活
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": APPROVER}).status_code == 409
    client.app.state.replay_worker.run_once()
    assert detail(client, batch_id)["batch"]["status"] == "cancelled"
    assert not any(e["type"] == "replay_batch_approval_expired"
                   for e in events(client, batch_id))


# ---- 老库迁移 -----------------------------------------------------------------

def test_old_replay_batches_migrated_with_approval_columns(tmp_path):
    """老库（批次无审批列）打开时就地补列，存量批次视为普通风险、无需审批。"""
    import sqlite3
    from app.db import Database

    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
    CREATE TABLE deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_id TEXT NOT NULL, content_hash TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL, frozen INTEGER NOT NULL DEFAULT 0, attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL, checkpoint TEXT, created_at REAL NOT NULL, updated_at REAL NOT NULL,
        UNIQUE (external_id, content_hash));
    CREATE TABLE replay_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT UNIQUE,
        operator TEXT NOT NULL, reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running', filters TEXT NOT NULL DEFAULT '{}',
        max_concurrency INTEGER,
        total INTEGER NOT NULL DEFAULT 0, done INTEGER NOT NULL DEFAULT 0,
        failed INTEGER NOT NULL DEFAULT 0, cancelled INTEGER NOT NULL DEFAULT 0,
        created_at REAL NOT NULL, updated_at REAL NOT NULL, finished_at REAL);
    CREATE TABLE replay_tasks (
        id INTEGER PRIMARY KEY AUTOINCREMENT, batch_id INTEGER NOT NULL,
        delivery_id INTEGER NOT NULL, external_id TEXT NOT NULL, operator TEXT NOT NULL,
        reason TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0, next_retry_at REAL, checkpoint TEXT,
        last_error TEXT, delivery_created_at REAL, blocked_reason TEXT,
        created_at REAL NOT NULL, updated_at REAL NOT NULL, finished_at REAL,
        UNIQUE (batch_id, delivery_id));
    """)
    conn.execute("INSERT INTO deliveries (external_id, content_hash, payload, status,"
                 " created_at, updated_at) VALUES ('M-1','h','{}','done',123.5,123.5)")
    conn.execute("INSERT INTO replay_batches (operator, reason, total, created_at, updated_at)"
                 " VALUES ('ops','r',1,100,100)")
    conn.commit()
    conn.close()

    db = Database(db_path)
    row = db.query_one("SELECT * FROM replay_batches WHERE id=1")
    assert row["risk_level"] == "normal"
    assert row["approval_status"] == "not_required"
    assert row["approval_deadline"] is None and row["approver"] is None
    assert row["approval_note"] is None and row["approval_reason"] is None
    db.close()

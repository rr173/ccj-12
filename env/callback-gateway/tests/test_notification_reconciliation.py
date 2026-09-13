"""通知状态对账与补偿的端到端测试。"""
from __future__ import annotations

import hashlib
import hmac
import json
import time

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "active-secret"
SUBMITTER = "ops-li"
LEAD = "ops-wang"
ADMIN = "ops-admin"
RECON = "/admin/approval-notifications/reconciliation"
ROUTING = "/admin/approval-notifications/routing"


def keys_file(tmp_path):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    return str(p)


def make_client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=keys_file(tmp_path), run_worker=False,
        notif_retry_base_seconds=5.0, notif_retry_cap_seconds=300.0,
        notif_max_attempts=3, notif_deadline_lead_seconds=300.0,
        notif_breaker_window_seconds=60.0,
        notif_breaker_failure_threshold=3,
        notif_breaker_cooldown_seconds=30.0,
        notif_channel_timeout_seconds=10.0,
        receipt_confirm_timeout_seconds=3600.0,
        receipt_confirm_max_retries=2,
        receipt_email_secret="receipt-secret",
        receipt_webhook_secret="receipt-secret")
    app = create_app(settings)
    return TestClient(app)


def post_callback(client, external_id, body=b'{"order":1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    r = client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})
    assert r.status_code == 202, r.text
    client.app.state.worker.run_once()


def setup_task(client, external_id="R-1", *, plan=("email", "inbox"),
               sender=None, channels=("email",)):
    client.post(f"{ROUTING}/versions", json={"operator": ADMIN, "rules": [
        {"event_type": None, "channels": [{"channel": ch} for ch in plan]}]})
    client.post("/admin/replay-policies", json={"operator": "pol", "policy": {
        "rules": [{"name": "h", "risk_level": "high", "mode": "parallel",
                   "nodes": [{"role": "ops-lead", "timeout_seconds": 3600}]}]}})
    contact_body = {"name": LEAD, "operator": ADMIN, "channels": list(channels)}
    if "email" in channels:
        contact_body["email"] = "wang@example.com"
    if "webhook" in channels:
        contact_body["webhook_url"] = "https://hook.example.com/x"
    client.post("/admin/approval-notifications/contacts", json=contact_body)
    post_callback(client, external_id)
    r = client.post("/admin/replays", json={
        "operator": SUBMITTER, "reason": "高风险", "risk_level": "high",
        "approval_note": "需审批", "external_id": external_id})
    assert r.status_code == 201, r.text
    now = time.time()
    client.post("/admin/replay-delegations", json={
        "role": "ops-lead", "delegatee": LEAD, "operator": ADMIN,
        "valid_from": now - 60, "valid_to": now + 3600})
    todos = client.get("/admin/approval-notifications/todos",
                       params={"recipient": LEAD}).json()["todos"]
    assert todos
    task_id = todos[0]["route_task"]["id"]
    if sender is not None:
        client.app.state.notif_worker.senders = {
            "email": sender, "webhook": sender, "inbox": lambda task: None}
        client.app.state.notif_worker.run_once()
    return task_id


def findings(client, job_id):
    return client.get(f"{RECON}/jobs/{job_id}/findings").json()["findings"]


def test_reconciliation_snapshot_pagination_pause_resume_and_restart(tmp_path):
    with make_client(tmp_path) as c:
        task_id = setup_task(c, "R-PAGE")
        db = c.app.state.db
        with db.tx() as cur:
            cur.execute("UPDATE notif_send_tasks SET status='failed', "
                        "last_error='boom', next_retry_at=? WHERE id=?",
                        (time.time() + 3600, task_id))
        r = c.post(f"{RECON}/jobs", json={
            "operator": "ops-audit", "recipient": LEAD, "page_size": 1})
        assert r.status_code == 200, r.text
        job_id = r.json()["job_id"]
        c.post(f"{RECON}/jobs/{job_id}/pause", json={"operator": "ops-audit"})
        # queued 状态下暂停生效：worker 不推进
        c.app.state.notif_worker.run_once()
        job = c.get(f"{RECON}/jobs/{job_id}").json()["job"]
        assert job["status"] == "paused"
        c.post(f"{RECON}/jobs/{job_id}/resume", json={"operator": "ops-audit"})
        # 单页只处理 tasks；多轮依次经过 receipts/reservations
        for _ in range(5):
            c.app.state.notif_worker.run_once()
        job = c.get(f"{RECON}/jobs/{job_id}").json()["job"]
        assert job["status"] == "completed"
        assert job["scanned_count"] >= 1
        rows = findings(c, job_id)
        assert any(f["reason"] == "task_needs_compensation_send" for f in rows)
        # 快照不可变：发布新路由版本后重新查询，finding 仍保留检测时任务快照
        c.post(f"{ROUTING}/versions", json={"operator": ADMIN, "rules": [
            {"event_type": None, "channels": [{"channel": "inbox"}]}]})
        f = next(f for f in rows if f["reason"]
                 == "task_needs_compensation_send")
        assert f["snapshot"]["task"]["id"] == task_id
        # 模拟重启恢复 scanning：worker recover 安全退回 queued，不重复 finding
        with db.tx() as cur:
            cur.execute("UPDATE notif_reconciliation_jobs SET status='scunning'",
                        ) if False else None
            cur.execute("UPDATE notif_reconciliation_jobs SET status='scanning' "
                        "WHERE id=?", (job_id,))
        c.app.state.notif_worker.recover()
        c.app.state.notif_worker.run_once()
        again = findings(c, job_id)
        assert len(again) == len(rows)


def test_compensation_send_plan_is_idempotent_and_never_resends_accepted(tmp_path):
    with make_client(tmp_path) as c:
        task_id = setup_task(c, "R-SEND")
        db = c.app.state.db
        with db.tx() as cur:
            cur.execute("UPDATE notif_send_tasks SET status='quarantined', "
                        "last_error='all channels down' WHERE id=?", (task_id,))
        r = c.post(f"{RECON}/jobs", json={"operator": "auditor", "recipient": LEAD})
        job_id = r.json()["job_id"]
        c.app.state.notif_worker.run_once()
        f = next(f for f in findings(c, job_id)
                 if f["reason"] == "task_needs_compensation_send")
        body = {"operator": "ops-fix"}
        r1 = c.post(f"{RECON}/findings/{f['id']}/compensations/create_send_plan",
                    json=body)
        assert r1.status_code == 200, r1.text
        plan_id = r1.json()["compensation_id"]
        r2 = c.post(f"{RECON}/findings/{f['id']}/compensations/create_send_plan",
                    json=body)
        assert r2.json()["result"] == "idempotent"
        plans = c.get(f"{RECON}/send-plans").json()["plans"]
        assert len([p for p in plans if p["id"] == plan_id]) == 1
        # 计划应用：任务回到 pending、round 增加；随后 inbox 成功
        c.app.state.notif_worker.run_once()
        task = c.get(f"{ROUTING}/tasks/{task_id}").json()["task"]
        assert task["status"] == "sent"
        assert task["sent_channel"] in {"email", "inbox"}
        plan = c.get(f"{RECON}/send-plans", params={"task_id": task_id}
                     ).json()["plans"][0]
        assert plan["status"] == "applied"
        # 已被外部商接受后，同异常不能再次补偿发送
        again = c.post(f"{RECON}/findings/{f['id']}/compensations/create_send_plan",
                       json={"operator": "ops-fix"})
        assert again.json()["result"] == "idempotent"


def test_release_orphan_reservation_and_close_task_keep_history(tmp_path):
    with make_client(tmp_path) as c:
        task_id = setup_task(c, "R-ORPHAN")
        db = c.app.state.db
        now = time.time()
        with db.tx() as cur:
            row = cur.execute("SELECT event_id,recipient FROM notif_send_tasks "
                              "WHERE id=?", (task_id,)).fetchone()
            cur.execute("""INSERT INTO notif_quota_reservations
                (task_id,event_id,recipient,quota_version,rule_id,level,
                 bucket_start,window_seconds,cost,kind,state,generation,
                 created_at,updated_at)
                VALUES (?,?,?,1,'*','normal',?,3600,1,'normal','reserved',1,?,?)""",
                (task_id, row["event_id"], row["recipient"], now, now, now))
            cur.execute("UPDATE notif_send_tasks SET status='cancelled', "
                        "cancelled_reason='manual' WHERE id=?", (task_id,))
        c.post(f"{RECON}/jobs", json={"operator": "auditor", "recipient": LEAD})
        c.app.state.notif_worker.run_once()
        job_id = c.get(f"{RECON}/jobs").json()["jobs"][0]["id"]
        f = next(f for f in findings(c, job_id)
                 if f["reason"] == "orphan_reserved_reservation")
        r = c.post(f"{RECON}/findings/{f['id']}/compensations/release_reservation",
                   json={"operator": "ops-fix"})
        assert r.status_code == 200, r.text
        comp = c.get(f"{RECON}/compensations",
                     params={"finding_id": f["id"]}).json()["compensations"][0]
        assert comp["before"]["reservation"]["state"] == "reserved"
        assert comp["after"]["reservation"]["state"] == "released"
        reservations = c.get(
            "/admin/approval-notifications/quota/reservations",
            params={"task_id": task_id}).json()["reservations"]
        assert reservations[0]["state"] == "released"
        # 重复补偿幂等；审计可按 job 查
        r2 = c.post(f"{RECON}/findings/{f['id']}/compensations/release_reservation",
                    json={"operator": "ops-fix"})
        assert r2.json()["result"] == "idempotent"
        events = c.get(f"{RECON}/jobs/{job_id}/events").json()["events"]
        assert any(e["type"] == "notif_reconciliation_reservation_released"
                   for e in events)


def test_close_task_compensation_is_idempotent_and_preserves_history(tmp_path):
    with make_client(tmp_path) as c:
        task_id = setup_task(c, "R-CLOSE")
        with c.app.state.db.tx() as cur:
            cur.execute("UPDATE notif_send_tasks SET status='quarantined', "
                        "last_error='do not send', next_retry_at=NULL WHERE id=?",
                        (task_id,))
        c.post(f"{RECON}/jobs", json={"operator": "auditor", "recipient": LEAD})
        c.app.state.notif_worker.run_once()
        job_id = c.get(f"{RECON}/jobs").json()["jobs"][0]["id"]
        f = next(f for f in findings(c, job_id)
                 if f["reason"] == "task_needs_compensation_send")
        r = c.post(f"{RECON}/findings/{f['id']}/compensations/close_task",
                   json={"operator": "ops-fix", "note": "业务确认关闭"})
        assert r.status_code == 200, r.text
        task = c.get(f"{ROUTING}/tasks/{task_id}").json()["task"]
        assert task["status"] == "cancelled"
        assert task["cancelled_reason"] == "业务确认关闭"
        r2 = c.post(f"{RECON}/findings/{f['id']}/compensations/close_task",
                    json={"operator": "ops-fix", "note": "业务确认关闭"})
        assert r2.json()["result"] == "idempotent"
        detail = c.get(f"{RECON}/findings/{f['id']}").json()
        assert detail["finding"]["status"] == "resolved"
        assert detail["compensations"][0]["after"]["task"]["status"] == "cancelled"
        assert detail["compensations"][0]["basis_snapshot"]["reason"] == \
            "task_needs_compensation_send"


def test_relink_unmatched_receipt_does_not_rewrite_raw_body(tmp_path):
    with make_client(tmp_path) as c:
        task_id = setup_task(c, "R-RECEIPT",
                             plan=("email", "inbox"),
                             sender=lambda *a: "provider-mid-9")
        msg = c.get("/admin/external-messages",
                    params={"task_id": task_id}).json()["messages"][0]
        raw = json.dumps({"event": "delivered", "message_id": msg["message_id"],
                          "recipient": "wang@example.com"}).encode()
        ts = int(time.time())
        sig = hmac.new(b"receipt-secret", f"{ts}\n".encode() + raw,
                       hashlib.sha256).hexdigest()
        # 先人为让回执匹配不到（不存在的 message id），再由补偿关联已有消息。
        raw_unmatched = json.dumps({
            "event": "delivered", "message_id": "late-provider-mid-9",
            "recipient": "wang@example.com"}).encode()
        sig2 = hmac.new(b"receipt-secret", f"{ts}\n".encode() + raw_unmatched,
                        hashlib.sha256).hexdigest()
        ru = c.post("/receipts/email", content=raw_unmatched, headers={
            "X-Signature": f"kid=email-bootstrap,ts={ts},sig={sig2}"})
        assert ru.status_code == 202, ru.text
        receipt_id = ru.json()["receipt_id"]
        c.post(f"{RECON}/jobs", json={"operator": "auditor", "recipient": LEAD})
        c.app.state.notif_worker.run_once()
        job_id = c.get(f"{RECON}/jobs").json()["jobs"][0]["id"]
        f = next(f for f in findings(c, job_id)
                 if f["entity_type"] == "receipt"
                 and f["receipt_id"] == receipt_id)
        r = c.post(f"{RECON}/findings/{f['id']}/compensations/relink_receipt",
                   json={"operator": "ops-fix", "message_pk": msg["id"]})
        assert r.status_code == 200, r.text
        detail = c.get(f"/admin/receipts/{receipt_id}").json()["receipt"]
        assert detail["raw_body"] == raw_unmatched.decode()
        assert detail["matched"] in {"bound", "applied"}
        assert detail["message_pk"] == msg["id"]
        # 重复补偿幂等：返回已有动作，不会再次关联或驱动状态机
        again = c.post(f"{RECON}/findings/{f['id']}/compensations/relink_receipt",
                       json={"operator": "ops-fix", "message_pk": msg["id"]})
        assert again.status_code == 200
        assert again.json()["result"] == "idempotent"

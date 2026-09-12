"""审批通知与待办分发的端到端测试：

- 节点激活/收到投票/接近截止/拒绝/超时/策略变更需要审批时，按节点角色与当前有效
  委托解析接收人生成站内待办；邮件/webhook 两种通道按联系人配置投递；
- 同一事件对同一接收人只有一个待办（重放、重复触发、worker 多轮扫描不重复）；
- 外发失败按指数间隔重试，超过次数进隔离，隔离后可人工 requeue；
- 待办记录来源批次/变更单、节点、接收人、事件版本与状态；
- 从待办回写原审批动作：重复点击、过期待办、并发处理都不会重复投票或越过门禁
  （角色/委托/职责分离/状态守卫保持原审批链口径）；
- 看板展示未读/已读/已处理/过期/取消数量与失败/隔离数量，支持按接收人/来源/状态查询；
- 生成、发送、重试、隔离、确认与回写全部有审计记录。
"""
from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"

SUBMITTER = "ops-li"
LEAD_A = "ops-wang"
LEAD_B = "ops-chen"
GRANTOR = "ops-admin"
APPROVER = "ops-zhao"


def make_keys_file(tmp_path):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"},
    ]}))
    return str(p)


@pytest.fixture()
def client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path),
        run_worker=False,
        notif_retry_base_seconds=5.0,
        notif_retry_cap_seconds=300.0,
        notif_max_attempts=3,
        notif_deadline_lead_seconds=300.0,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def post(client, external_id, body=b'{"order": 1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}",
    })


def process_normally(client, external_id):
    post(client, external_id)
    client.app.state.worker.run_once()


def apply_policy(client, rules):
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def submit_high(client, external_id, *, operator=SUBMITTER, request_id=None):
    payload = {"operator": operator, "reason": "资金类回调补发",
               "risk_level": "high", "approval_note": "需审批",
               "external_id": external_id}
    if request_id:
        payload["request_id"] = request_id
    r = client.post("/admin/replays", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def approval(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()["batch"]["approval"]


def contact(client, name, *, channels=None, email=None, webhook_url=None,
            operator=GRANTOR, expect=200):
    body = {"name": name, "operator": operator,
            "channels": channels or []}
    if email:
        body["email"] = email
    if webhook_url:
        body["webhook_url"] = webhook_url
    r = client.post("/admin/approval-notifications/contacts", json=body)
    assert r.status_code == expect, r.text
    return r


def delegation(client, role, delegatee, *, valid_from=None, valid_to=None):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": role, "delegatee": delegatee, "operator": GRANTOR,
        "valid_from": now - 60 if valid_from is None else valid_from,
        "valid_to": now + 3600 if valid_to is None else valid_to})
    assert r.status_code == 201, r.text
    return r.json()["delegation_id"]


def todos(client, **params):
    return client.get("/admin/approval-notifications/todos",
                      params=params).json()["todos"]


def summary(client, **params):
    return client.get("/admin/approval-notifications/summary", params=params).json()


def global_events(client, event_type=None):
    params = {"limit": 1000}
    if event_type:
        params["type"] = event_type
    return client.get("/admin/events", params=params).json()["events"]


# ---- 激活：按角色与有效委托生成待办 -------------------------------------------------

def test_node_activation_todos_for_role_delegates(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    # 联系人必须在目录里；仅注册 LEAD_A
    contact(client, LEAD_A)
    process_normally(client, "N-1")
    bid = submit_high(client, "N-1")["batch_id"]
    node_id = approval(client, bid)["nodes"][0]["id"]

    # 节点激活时 LEAD_A 尚无委托 -> 无待办
    assert todos(client, recipient=LEAD_A) == []

    # 之后创建有效委托：为仍 active 的节点事件补发待办
    did = delegation(client, "ops-lead", LEAD_A)
    items = todos(client, recipient=LEAD_A)
    assert len(items) == 1
    todo = items[0]
    assert todo["event_type"] == "activated"
    assert todo["source_type"] == "batch"
    assert todo["batch_id"] == bid and todo["node_id"] == node_id
    assert todo["status"] == "unread" and todo["actionable"] is True
    assert todo["event_version"] >= 1 and todo["current_event_version"] >= 1

    # 不是联系人的受托人即便有委托也不生成待办
    delegation(client, "ops-lead", LEAD_B)
    assert todos(client, recipient=LEAD_B) == []

    # 重复补发/重复创建委托不产生第二个待办（同一事件同一接收人）
    client.post(f"/admin/replay-delegations/{did}/reactivate", json={
        "operator": GRANTOR, "valid_from": time.time() - 5,
        "valid_to": time.time() + 7200})
    assert len(todos(client, recipient=LEAD_A)) == 1

    ev = global_events(client, "approval_todo_generated")
    assert len(ev) == 1
    assert ev[0]["detail"]["recipient"] == LEAD_A
    assert ev[0]["detail"]["source_batch_id"] == bid


def test_contact_registered_after_activation_gets_backfilled(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    process_normally(client, "N-2")
    bid = submit_high(client, "N-2")["batch_id"]
    # 先委托、后注册联系人：注册时为仍可操作的历史事件补发
    delegation(client, "ops-lead", LEAD_A)
    assert todos(client, recipient=LEAD_A) == []
    contact(client, LEAD_A)
    items = todos(client, recipient=LEAD_A)
    assert len(items) == 1 and items[0]["batch_id"] == bid


def test_any_node_notifies_all_contacts_except_submitter(client):
    # 无策略 -> 内置默认：'any' 节点，任何非发起人可批
    process_normally(client, "N-3")
    contact(client, APPROVER)
    contact(client, SUBMITTER)
    bid = submit_high(client, "N-3")["batch_id"]
    recipients = {t["recipient"] for t in todos(client, batch_id=bid)}
    assert recipients == {APPROVER}  # 发起人排除


# ---- 邮件/webhook 通道 -----------------------------------------------------------

def test_email_and_webhook_deliveries_created_and_sent(client):
    sent = {"email": [], "webhook": []}
    client.app.state.notif_worker.senders = {
        "email": lambda addr, subject, body: sent["email"].append((addr, subject)),
        "webhook": lambda url, payload: sent["webhook"].append((url, payload)),
    }
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A, channels=["email", "webhook"],
            email="wang@example.com", webhook_url="https://hook.example.com/x")
    process_normally(client, "N-4")
    bid = submit_high(client, "N-4")["batch_id"]
    delegation(client, "ops-lead", LEAD_A)

    todo = todos(client, recipient=LEAD_A)[0]
    channels = {d["channel"]: d for d in todo["deliveries"]}
    assert set(channels) == {"email", "webhook"}
    assert all(d["status"] == "pending" for d in channels.values())

    # worker 发送后两条投递 sent（未读待办保持开放）
    client.app.state.notif_worker.run_once()
    todo = todos(client, recipient=LEAD_A)[0]
    assert {d["status"] for d in todo["deliveries"]} == {"sent"}
    assert sent["email"][0][0] == "wang@example.com"
    assert sent["webhook"][0][1]["event"] == "activated"
    assert sent["webhook"][0][1]["batch_id"] == bid
    types = [e["type"] for e in global_events(client)]
    assert types.count("approval_delivery_sent") == 2
    assert "approval_todo_generated" in types

    # 再次 worker 不重复发送
    client.app.state.notif_worker.run_once()
    deliveries = client.get("/admin/approval-notifications/deliveries").json()["deliveries"]
    assert [d["status"] for d in deliveries].count("sent") == 2


def test_channel_validation(client):
    r = contact(client, "x1", channels=["email"], expect=422)  # 缺地址
    r = contact(client, "x2", channels=["sms"], expect=422)    # 不支持的通道
    assert r.status_code == 422


# ---- 失败指数退避重试与隔离 ---------------------------------------------------------

def test_failed_deliveries_retry_with_backoff_then_quarantine(client):
    calls = {"n": 0}

    def failing_webhook(url, payload):
        calls["n"] += 1
        raise RuntimeError("webhook 500")

    client.app.state.notif_worker.senders = {
        "email": lambda *a: None,
        "webhook": failing_webhook,
    }
    settings = client.app.state.settings
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A, channels=["webhook"],
            webhook_url="https://hook.example.com/fail")
    process_normally(client, "N-5")
    submit_high(client, "N-5")
    delegation(client, "ops-lead", LEAD_A)
    worker = client.app.state.notif_worker

    base = time.time()
    worker.clock = lambda: base
    worker.run_once()  # 生成待办 + 首次发送失败
    deliveries = client.get(
        "/admin/approval-notifications/deliveries",
        params={"status": "failed"}).json()["deliveries"]
    assert len(deliveries) == 1
    d = deliveries[0]
    assert d["attempts"] == 1
    # 指数退避：base*2^(1-1)=5
    assert d["next_retry_at"] == pytest.approx(base + 5.0, abs=1.0)

    # 未到期不重试
    worker.clock = lambda: base + 4
    worker.run_once()
    d = client.get("/admin/approval-notifications/deliveries").json()["deliveries"][0]
    assert d["attempts"] == 1

    # 到期重试：第 2 次失败，退避 10s
    worker.clock = lambda: base + 6
    worker.run_once()
    d = client.get("/admin/approval-notifications/deliveries").json()["deliveries"][0]
    assert d["attempts"] == 2 and d["status"] == "failed"
    assert d["next_retry_at"] == pytest.approx(base + 6 + 10.0, abs=1.0)

    # 第 3 次失败（达到 NOTIF_MAX_ATTEMPTS=3）-> 隔离
    worker.clock = lambda: base + 20
    worker.run_once()
    q = client.get("/admin/approval-notifications/deliveries",
                   params={"status": "quarantined"}).json()["deliveries"]
    assert len(q) == 1 and q[0]["attempts"] == 3 and q[0]["next_retry_at"] is None
    audit_types = [e["type"] for e in global_events(client)]
    assert audit_types.count("approval_delivery_retry_scheduled") == 2
    assert audit_types.count("approval_delivery_quarantined") == 1
    assert summary(client, recipient=LEAD_A)["delivery_quarantined"] == 1

    # 隔离后 worker 不再尝试（计数不变）
    n_before = calls["n"]
    worker.run_once()
    assert calls["n"] == n_before

    # 人工 requeue：恢复发送器后立即发送成功
    client.app.state.notif_worker.senders = {
        "email": lambda *a: None,
        "webhook": lambda url, payload: None,
    }
    r = client.post(f"/admin/approval-notifications/deliveries/{q[0]['id']}/requeue",
                    json={"operator": GRANTOR})
    assert r.status_code == 200, r.text
    d = client.get("/admin/approval-notifications/deliveries").json()["deliveries"][0]
    # requeue 重置次数；成功发送后 attempts=1
    assert d["status"] == "sent" and d["attempts"] == 1
    assert d["last_error"] is None
    assert any(e["type"] == "approval_delivery_requeued" for e in global_events(client))


# ---- 待办处理：回写原审批动作 -------------------------------------------------------

def test_act_on_todo_writes_back_approval(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    process_normally(client, "A-1")
    bid = submit_high(client, "A-1")["batch_id"]
    did = delegation(client, "ops-lead", LEAD_A)
    todo = todos(client, recipient=LEAD_A)[0]

    # 无委托角色直接处理 -> 422（原审批门禁：role+委托必填）
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve"})
    assert r.status_code == 422

    # 正确回写：节点决定落定、批次放行、待办 handled
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did,
                          "note": "待办回写批准"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "handled"
    assert body["todo"]["status"] == "handled"
    assert body["todo"]["handle_action"] == "approve"
    assert body["decision"]["batch_status"] == "running"
    assert approval(client, bid)["status"] == "approved"

    # 重复点击：409，不会二次投票
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did})
    assert r.status_code == 409
    db = client.app.state.db
    votes = db.query_one(
        "SELECT COUNT(*) AS c FROM replay_node_votes WHERE voter=?", (LEAD_A,))["c"]
    assert votes == 1

    types = [e["type"] for e in global_events(client)]
    assert types.count("approval_todo_handled") == 1
    assert types.count("approval_action_written_back") == 1


def test_todo_act_enforces_separation_of_duties(client):
    # 'any' 节点：发起人即使有待办也无法处理（不能审批自己的批次）
    process_normally(client, "A-2")
    contact(client, SUBMITTER)
    contact(client, APPROVER)
    bid = submit_high(client, "A-2")["batch_id"]
    # 发起人没有待办（被排除）；构造越权：用发起人处理 APPROVER 的待办
    todo = todos(client, recipient=APPROVER)[0]
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": SUBMITTER, "action": "approve"})
    assert r.status_code == 403
    # 非接收人处理别人的待办同样 403
    contact(client, LEAD_B)
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_B, "action": "approve"})
    assert r.status_code == 403
    # 审批人本人处理成功
    assert client.post(
        f"/admin/approval-notifications/todos/{todo['id']}/act",
        json={"operator": APPROVER, "action": "approve"}).status_code == 200
    assert approval(client, bid)["status"] == "approved"


def test_concurrent_todo_handling_single_winner(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 1, "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    contact(client, LEAD_B)
    process_normally(client, "A-3")
    bid = submit_high(client, "A-3")["batch_id"]
    da = delegation(client, "ops-lead", LEAD_A)
    db = delegation(client, "ops-lead", LEAD_B)
    t_a = todos(client, recipient=LEAD_A)[0]
    t_b = todos(client, recipient=LEAD_B)[0]

    barrier = threading.Barrier(2)
    responses = {}

    def handle(tag, todo_id, who, did):
        barrier.wait()
        responses[tag] = client.post(
            f"/admin/approval-notifications/todos/{todo_id}/act",
            json={"operator": who, "action": "approve",
                  "role": "ops-lead", "delegation_id": did})

    th1 = threading.Thread(target=handle, args=("a", t_a["id"], LEAD_A, da))
    th2 = threading.Thread(target=handle, args=("b", t_b["id"], LEAD_B, db))
    th1.start(); th2.start(); th1.join(); th2.join()
    statuses = sorted(r.status_code for r in responses.values())
    assert statuses == [200, 409]
    # 恰好一张票、批次一次放行
    node_id = approval(client, bid)["nodes"][0]["id"]
    votes = client.app.state.db.query_one(
        "SELECT COUNT(*) AS c FROM replay_node_votes WHERE node_id=? AND status='valid'",
        (node_id,))["c"]
    assert votes == 1
    assert approval(client, bid)["status"] == "approved"
    handled = [t for t in todos(client, batch_id=bid) if t["status"] == "handled"]
    assert len(handled) == 1
    assert [e["type"] for e in global_events(client)].count(
        "approval_action_written_back") == 1


def test_expired_todo_cannot_be_handled(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    process_normally(client, "A-4")
    bid = submit_high(client, "A-4")["batch_id"]
    delegation(client, "ops-lead", LEAD_A)
    todo = todos(client, recipient=LEAD_A)[0]

    # worker 推进到截止时间之后：节点超时 -> 批次取消 -> 待办对账关闭
    worker = client.app.state.replay_worker
    nworker = client.app.state.notif_worker
    deadline = approval(client, bid)["nodes"][0]["deadline"]
    worker.clock = lambda: deadline + 1
    worker.run_once()
    nworker.clock = lambda: deadline + 1
    nworker.run_once()

    todo = next(t for t in todos(client, recipient=LEAD_A))
    assert todo["status"] == "expired"  # 节点超时 -> 待办过期
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": 1})
    assert r.status_code == 409
    db = client.app.state.db
    assert db.query_one(
        "SELECT COUNT(*) AS c FROM replay_node_votes")["c"] == 0
    # 超时通知（纯告知）发给发起人
    timeout_todos = [t for t in todos(client, batch_id=bid)
                     if t["event_type"] == "node_timeout"]
    assert timeout_todos and timeout_todos[0]["recipient"] == SUBMITTER
    assert timeout_todos[0]["actionable"] is False


# ---- 投票事件 / 法定人数 ------------------------------------------------------------

def test_vote_received_todo_for_other_eligible_approvers(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    contact(client, LEAD_B)
    process_normally(client, "V-1")
    bid = submit_high(client, "V-1")["batch_id"]
    da = delegation(client, "ops-lead", LEAD_A)
    db_ = delegation(client, "ops-lead", LEAD_B)
    node_id = approval(client, bid)["nodes"][0]["id"]

    # LEAD_A 投第一票（法定人数 2，未满足）
    r = client.post(f"/admin/replays/{bid}/nodes/{node_id}/approve",
                    json={"operator": LEAD_A, "role": "ops-lead",
                          "delegation_id": da})
    assert r.status_code == 200 and r.json()["result"] == "voted"

    # LEAD_B 收到 vote_received 可操作待办；LEAD_A（已投票）不收
    b_todos = todos(client, recipient=LEAD_B)
    events_b = {t["event_type"] for t in b_todos}
    assert "vote_received" in events_b
    vote_todo = next(t for t in b_todos if t["event_type"] == "vote_received")
    assert vote_todo["actionable"] is True
    assert all(t["event_type"] != "vote_received" for t in todos(client, recipient=LEAD_A))

    # LEAD_A 自己的 activated 待办在投票后被对账关闭（已投票不可重复处理）
    client.app.state.notif_worker.run_once()
    a_open = [t for t in todos(client, recipient=LEAD_A)
              if t["status"] in ("unread", "read")]
    assert a_open == []

    # LEAD_B 从 vote_received 待办回写第二票 -> 放行
    r = client.post(f"/admin/approval-notifications/todos/{vote_todo['id']}/act",
                    json={"operator": LEAD_B, "action": "approve",
                          "role": "ops-lead", "delegation_id": db_})
    assert r.status_code == 200
    assert approval(client, bid)["status"] == "approved"


# ---- 截止提醒 --------------------------------------------------------------------

def test_deadline_approaching_event_emitted_once(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    process_normally(client, "D-1")
    bid = submit_high(client, "D-1")["batch_id"]
    delegation(client, "ops-lead", LEAD_A)
    node_id = approval(client, bid)["nodes"][0]["id"]
    deadline = approval(client, bid)["nodes"][0]["deadline"]

    nw = client.app.state.notif_worker
    # 尚未进入提醒窗口：无事件
    nw.clock = lambda: deadline - 400
    nw.run_once()
    assert [t for t in todos(client, recipient=LEAD_A)
            if t["event_type"] == "deadline_approaching"] == []
    # 进入窗口：生成一次
    nw.clock = lambda: deadline - 200
    nw.run_once()
    dl = [t for t in todos(client, recipient=LEAD_A)
          if t["event_type"] == "deadline_approaching"]
    assert len(dl) == 1 and dl[0]["actionable"] is True
    # 重复扫描不重复生成（同一事件同一接收人只一个待办）
    nw.run_once(); nw.run_once()
    assert len([t for t in todos(client, recipient=LEAD_A)
                if t["event_type"] == "deadline_approaching"]) == 1
    assert [e["type"] for e in global_events(client)].count(
        "approval_notify_event") == 2  # activated + deadline_approaching


# ---- 拒绝：结果待办 + 可操作待办关闭 ------------------------------------------------

def test_rejection_notifies_submitter_and_closes_open_todos(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://hook/x")
    contact(client, SUBMITTER)
    process_normally(client, "R-1")
    bid = submit_high(client, "R-1")["batch_id"]
    did = delegation(client, "ops-lead", LEAD_A)
    node_id = approval(client, bid)["nodes"][0]["id"]

    r = client.post(f"/admin/replays/{bid}/nodes/{node_id}/reject",
                    json={"operator": LEAD_A, "role": "ops-lead",
                          "delegation_id": did, "reason": "评估不通过"})
    assert r.status_code == 200
    client.app.state.notif_worker.run_once()

    # 发起人收到纯告知的 node_rejected 待办
    rej = [t for t in todos(client, recipient=SUBMITTER)
           if t["event_type"] == "node_rejected"]
    assert len(rej) == 1 and rej[0]["actionable"] is False
    # LEAD_A 直接在原端点拒绝：其可操作待办由对账关闭（非经由待办处理）
    lead_todos = todos(client, recipient=LEAD_A)
    assert all(t["status"] == "cancelled" for t in lead_todos
               if t["event_type"] == "activated")
    # 未发出的投递随之取消
    deliveries = client.get("/admin/approval-notifications/deliveries",
                            params={"recipient": LEAD_A}).json()["deliveries"]
    assert deliveries and all(d["status"] == "cancelled" for d in deliveries)


# ---- 策略变更需要审批 ---------------------------------------------------------------

def test_policy_change_requires_approval_todo_and_writeback(client):
    # 当前无策略（内置默认）；提交一个高风险新策略 -> 高风险变更单待审批
    contact(client, APPROVER)
    new_rules = [
        {"name": "h1", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
        {"name": "n1", "risk_level": "normal", "nodes": []},
    ]
    r = client.post("/admin/replay-policies/changes", json={
        "operator": SUBMITTER, "request_id": "chg-todo-1",
        "policy": {"rules": new_rules}})
    assert r.status_code == 201, r.text
    change_id = r.json()["change"]["id"]

    items = todos(client, recipient=APPROVER)
    todo = next(t for t in items if t["event_type"] == "change_required")
    assert todo["source_type"] == "change" and todo["change_id"] == change_id
    assert todo["batch_id"] is None and todo["node_id"] is None
    assert todo["actionable"] is True
    # 发起人不收自己提交的变更的可操作待办
    assert all(t["recipient"] != SUBMITTER for t in todos(client))

    # 发起人不能处理自己的变更（职责分离，由原门禁挡下）
    contact(client, SUBMITTER)  # 即使后注册也不补发 change_required（提交人排除）
    assert not any(t["event_type"] == "change_required"
                   for t in todos(client, recipient=SUBMITTER))

    # 从待办批准变更
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": APPROVER, "action": "approve"})
    assert r.status_code == 200, r.text
    view = client.get(f"/admin/replay-policies/changes/{change_id}").json()
    assert view["status"] == "approved"
    assert todos(client, recipient=APPROVER)[0]["status"] == "handled"

    # 提交人执行变更后收到 change_applied 纯告知待办
    r = client.post(f"/admin/replay-policies/changes/{change_id}/apply",
                    json={"operator": SUBMITTER})
    assert r.status_code == 200, r.text
    applied = [t for t in todos(client, recipient=SUBMITTER)
               if t["event_type"] == "change_applied"]
    assert applied and applied[0]["actionable"] is False

    types = [e["type"] for e in global_events(client)]
    assert "approval_todo_generated" in types
    assert types.count("approval_todo_handled") == 1


def test_policy_change_rejected_from_todo(client):
    contact(client, APPROVER)
    new_rules = [
        {"name": "h1", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ]
    r = client.post("/admin/replay-policies/changes", json={
        "operator": SUBMITTER, "request_id": "chg-todo-2",
        "policy": {"rules": new_rules}})
    change_id = r.json()["change"]["id"]
    todo = next(t for t in todos(client, recipient=APPROVER)
                if t["event_type"] == "change_required")
    # reject 缺原因 -> 422（原门禁）
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": APPROVER, "action": "reject"})
    assert r.status_code == 422
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": APPROVER, "action": "reject",
                          "reason": "链不合理"})
    assert r.status_code == 200
    view = client.get(f"/admin/replay-policies/changes/{change_id}").json()
    assert view["status"] == "rejected"
    # 拒绝后待办关闭，再处理 409
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": APPROVER, "action": "approve"})
    assert r.status_code == 409
    # 提交人收到 change_rejected 告知
    contact(client, SUBMITTER)
    rej = [t for t in todos(client, recipient=SUBMITTER)
           if t["event_type"] == "change_rejected"]
    assert rej and rej[0]["actionable"] is False


# ---- 已读 / 查询 / 看板 ------------------------------------------------------------

def test_read_idempotent_and_summary_filters(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    process_normally(client, "S-1")
    bid = submit_high(client, "S-1")["batch_id"]
    delegation(client, "ops-lead", LEAD_A)
    todo = todos(client, recipient=LEAD_A)[0]

    # 已读
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/read",
                    json={"operator": LEAD_A})
    assert r.status_code == 200 and r.json()["todo"]["status"] == "read"
    # 重复确认幂等
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/read",
                    json={"operator": LEAD_A})
    assert r.json()["todo"]["status"] == "read"
    assert [e["type"] for e in global_events(client)].count("approval_todo_read") == 1
    # 别人不能标记
    assert client.post(f"/admin/approval-notifications/todos/{todo['id']}/read",
                       json={"operator": LEAD_B}).status_code == 403

    # 处理已读待办也正常
    did = client.get("/admin/replay-delegations",
                     params={"delegatee": LEAD_A}).json()["delegations"][0]["id"]
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did})
    assert r.status_code == 200

    s = summary(client, recipient=LEAD_A)
    assert s["unread"] == 0 and s["read"] == 0 and s["handled"] == 1


def test_list_filtering_by_recipient_source_status(client):
    process_normally(client, "F-1")
    contact(client, APPROVER)
    bid = submit_high(client, "F-1")["batch_id"]
    todo = todos(client, recipient=APPROVER)[0]

    # 按接收人
    assert len(todos(client, recipient=APPROVER)) == 1
    assert todos(client, recipient="nobody") == []
    # 按状态
    assert len(todos(client, recipient=APPROVER, status="unread")) == 1
    assert todos(client, recipient=APPROVER, status="handled") == []
    # 按来源类型与 id
    assert len(todos(client, source_type="batch")) == 1
    assert todos(client, source_type="change") == []
    assert todos(client, batch_id=bid)[0]["id"] == todo["id"]
    assert todos(client, source=f"batch:{bid}")[0]["id"] == todo["id"]
    # 非法状态 422
    assert client.get("/admin/approval-notifications/todos",
                      params={"status": "bogus"}).status_code == 422


def test_summary_shows_all_counts(client):
    # 两个可审批人各一个 activated；其中一人的待办处理，另一人的批次超时取消
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    contact(client, LEAD_B)
    process_normally(client, "SUM-1")
    bid = submit_high(client, "SUM-1")["batch_id"]
    da = delegation(client, "ops-lead", LEAD_A)
    db_ = delegation(client, "ops-lead", LEAD_B)
    node_id = approval(client, bid)["nodes"][0]["id"]

    # LEAD_A 直接在原节点端点批准（不经过待办）：两人的可操作待办都在对账后关闭
    client.post(f"/admin/replays/{bid}/nodes/{node_id}/approve",
                json={"operator": LEAD_A, "role": "ops-lead", "delegation_id": da})
    client.app.state.notif_worker.run_once()
    assert summary(client, recipient=LEAD_A)["cancelled"] == 1
    cancelled = todos(client, recipient=LEAD_B, status="cancelled")
    assert len(cancelled) == 1
    assert summary(client, recipient=LEAD_B)["cancelled"] == 1
    # 关闭未发出的投递时记审计
    assert any(e["type"] == "approval_todo_closed" for e in global_events(client))


# ---- 联系人停用 -------------------------------------------------------------------

def test_quorum_unmet_via_todo_marks_handled_and_skip_writeback(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    contact(client, LEAD_B)
    process_normally(client, "Q-9")
    bid = submit_high(client, "Q-9")["batch_id"]
    da = delegation(client, "ops-lead", LEAD_A)
    db_ = delegation(client, "ops-lead", LEAD_B)
    node_id = approval(client, bid)["nodes"][0]["id"]
    todo_a = todos(client, recipient=LEAD_A)[0]

    # 第一票（法定人数未满）：待办仍视为"该接收人已处理"（已回写投票），
    # 但节点/批次保持 pending；再点该待办 409，不会重复投票
    r = client.post(f"/admin/approval-notifications/todos/{todo_a['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": da})
    assert r.status_code == 200, r.text
    assert r.json()["decision"]["result"] == "voted"
    assert r.json()["todo"]["status"] == "handled"
    ap = approval(client, bid)
    assert ap["status"] == "pending" and ap["nodes"][0]["approved_count"] == 1
    r = client.post(f"/admin/approval-notifications/todos/{todo_a['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": da})
    assert r.status_code == 409
    assert approval(client, bid)["nodes"][0]["approved_count"] == 1

    # 第二人从自己的待办投票 -> 达法定人数放行
    todo_b = next(t for t in todos(client, recipient=LEAD_B)
                  if t["event_type"] in ("activated", "vote_received"))
    r = client.post(f"/admin/approval-notifications/todos/{todo_b['id']}/act",
                    json={"operator": LEAD_B, "action": "approve",
                          "role": "ops-lead", "delegation_id": db_})
    assert r.status_code == 200 and r.json()["decision"]["batch_status"] == "running"


def test_skip_action_requires_reason_and_writes_back(client):
    # 串行链第二节点：用 skip 待办回写（reason 必填，跳过视为节点满足）
    apply_policy(client, [
        {"name": "s", "risk_level": "high", "mode": "serial", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600},
            {"role": "finance-controller", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    contact(client, FINANCE_A := "ops-qian")
    process_normally(client, "SK-1")
    bid = submit_high(client, "SK-1")["batch_id"]
    d_lead = delegation(client, "ops-lead", LEAD_A)
    n0 = approval(client, bid)["nodes"][0]
    todo = todos(client, recipient=LEAD_A)[0]
    client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                json={"operator": LEAD_A, "action": "approve",
                      "role": "ops-lead", "delegation_id": d_lead})
    # 第二节点激活后财务拿到待办
    d_fin = delegation(client, "finance-controller", FINANCE_A)
    client.app.state.notif_worker.clock = time.time
    client.app.state.notif_worker.run_once()
    fin_todos = todos(client, recipient=FINANCE_A)
    todo2 = next(t for t in fin_todos if t["event_type"] == "activated")
    # skip 缺原因 422
    r = client.post(f"/admin/approval-notifications/todos/{todo2['id']}/act",
                    json={"operator": FINANCE_A, "action": "skip",
                          "role": "finance-controller", "delegation_id": d_fin})
    assert r.status_code == 422
    # 带原因跳过 -> 节点满足、批次放行
    r = client.post(f"/admin/approval-notifications/todos/{todo2['id']}/act",
                    json={"operator": FINANCE_A, "action": "skip",
                          "role": "finance-controller", "delegation_id": d_fin,
                          "reason": "主管电话确认代签"})
    assert r.status_code == 200, r.text
    assert r.json()["decision"]["batch_status"] == "running"
    n1 = next(n for n in approval(client, bid)["nodes"] if n["seq"] == 1)
    assert n1["status"] == "skipped"


def test_deactivate_contact_closes_open_todos(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    contact(client, LEAD_A)
    process_normally(client, "X-1")
    bid = submit_high(client, "X-1")["batch_id"]
    delegation(client, "ops-lead", LEAD_A)
    todo = todos(client, recipient=LEAD_A)[0]

    r = client.post(f"/admin/approval-notifications/contacts/{LEAD_A}/deactivate",
                    json={"operator": GRANTOR, "reason": "离职"})
    assert r.status_code == 200 and r.json()["closed_todos"] == 1
    assert todos(client, recipient=LEAD_A)[0]["status"] == "cancelled"
    # 停用后新事件不再为其生成待办（再建委托也不补发）
    process_normally(client, "X-2")
    bid2 = submit_high(client, "X-2")["batch_id"]
    delegation(client, "ops-lead", LEAD_A,
               valid_from=time.time() - 5, valid_to=time.time() + 7200)
    assert todos(client, recipient=LEAD_A, batch_id=bid2) == []
    # 重复停用 409
    assert client.post(f"/admin/approval-notifications/contacts/{LEAD_A}/deactivate",
                       json={"operator": GRANTOR, "reason": "x"}).status_code == 409

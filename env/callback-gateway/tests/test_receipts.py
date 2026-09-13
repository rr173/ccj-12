"""外部通道回执与送达确认模块的端到端测试。

覆盖：
- email/webhook 发送后登记外部 message_id（外部返回 / 本地占位）；
- 公共签名入口 POST /receipts/{channel}：delivered/bounced/complained/expired；
- 同一 message_id 重复回执幂等（不二次改状态、不二次外部效果）；
- 乱序回执不能把已确认终态改回处理中（首条终态赢）；
- 无法匹配发送任务的回执进待核对队列；人工绑定（不改正文）/忽略；
- 超时扫描：待确认 -> 按策略故障转移重试 -> 超限转人工；
- 人工处置 awaiting_manual（retry/ignore）；
- 安全重放（终态不回退、待核对后匹配可应用，不产生第二次外部效果）；
- 按通道/接收人/时间/状态查询原文、历史、待核对队列；
- 重启 recover / 并发消费不重复改变状态。
"""
from __future__ import annotations

import hashlib
import hmac
import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"
RECEIPT_EMAIL_SECRET = "receipt-email-secret"
RECEIPT_WEBHOOK_SECRET = "receipt-webhook-secret"
SUBMITTER = "ops-li"
LEAD_A = "ops-wang"
GRANTOR = "ops-admin"

ROUTING = "/admin/approval-notifications/routing"


def make_keys_file(tmp_path):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    return str(p)


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path),
        run_worker=False,
        notif_retry_base_seconds=5.0,
        notif_retry_cap_seconds=300.0,
        notif_max_attempts=3,
        notif_deadline_lead_seconds=300.0,
        notif_breaker_window_seconds=60.0,
        notif_breaker_failure_threshold=3,
        notif_breaker_cooldown_seconds=30.0,
        notif_channel_timeout_seconds=10.0,
        receipt_confirm_timeout_seconds=60.0,
        receipt_confirm_max_retries=2,
        receipt_email_secret=RECEIPT_EMAIL_SECRET,
        receipt_webhook_secret=RECEIPT_WEBHOOK_SECRET,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


# ---- 通用装配 helper ---------------------------------------------------------

def post_callback(client, external_id, body=b'{"order": 1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})


def process_normally(client, external_id):
    post_callback(client, external_id)
    client.app.state.worker.run_once()


def apply_policy(client):
    rules = [{"name": "h", "risk_level": "high", "mode": "parallel",
              "nodes": [{"role": "ops-lead", "timeout_seconds": 3600}]}]
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.text


def make_contact(client, name=LEAD_A, *, channels=("email", "webhook"),
                 email="wang@example.com",
                 webhook_url="https://hook.example.com/x"):
    body = {"name": name, "operator": GRANTOR, "channels": list(channels)}
    if email:
        body["email"] = email
    if webhook_url:
        body["webhook_url"] = webhook_url
    r = client.post("/admin/approval-notifications/contacts", json=body)
    assert r.status_code == 200, r.text


def delegation(client, role="ops-lead", delegatee=LEAD_A):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": role, "delegatee": delegatee, "operator": GRANTOR,
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text


def submit_high(client, external_id):
    r = client.post("/admin/replays", json={
        "operator": SUBMITTER, "reason": "资金类回调补发", "risk_level": "high",
        "approval_note": "需审批", "external_id": external_id})
    assert r.status_code == 201, r.text
    return r.json()


def make_route_task(client, external_id, *, plan=("email", "webhook", "inbox"),
                    email_sender=None, webhook_sender=None,
                    channels=("email", "webhook")):
    """发布路由 -> 生成待办 -> 发送一轮，返回 (task, email_calls, webhook_calls)。"""
    calls = {"email": [], "webhook": []}

    def default_email(addr, subject, body):
        calls["email"].append(addr)

    def default_webhook(url, payload):
        calls["webhook"].append(url)

    def wrap(fn, key):
        if fn is None:
            return None

        def w(*args):
            calls[key].append(args[0])
            return fn(*args)

        return w

    client.app.state.notif_worker.senders = {
        "email": wrap(email_sender, "email") or default_email,
        "webhook": wrap(webhook_sender, "webhook") or default_webhook,
        "inbox": lambda task: None}
    client.post(f"{ROUTING}/versions", json={
        "operator": GRANTOR,
        "rules": [{"event_type": None, "channels": [
            {"channel": ch} for ch in plan]}]})
    apply_policy(client)
    make_contact(client, channels=channels)
    process_normally(client, external_id)
    bid = submit_high(client, external_id)["batch_id"]
    delegation(client)
    todos = client.get("/admin/approval-notifications/todos",
                       params={"recipient": LEAD_A, "batch_id": bid}).json()["todos"]
    assert len(todos) == 1
    task = todos[0]["route_task"]
    client.app.state.notif_worker.run_once()
    detail = client.get(f"{ROUTING}/tasks/{task['id']}").json()["task"]
    return detail, calls


def receipt_sign(secret, body, ts=None):
    ts = int(time.time()) if ts is None else ts
    sig = hmac.new(secret.encode(), f"{ts}\n".encode() + body,
                   hashlib.sha256).hexdigest()
    return ts, sig


def send_receipt(client, channel, body: dict, *, secret=None, ts=None,
                 kid=None, bad_sig=False, headers=None):
    raw = json.dumps(body).encode()
    kid = kid or (f"{channel}-bootstrap")
    ts, sig = receipt_sign(secret or
                           (RECEIPT_EMAIL_SECRET if channel == "email"
                            else RECEIPT_WEBHOOK_SECRET), raw, ts)
    if bad_sig:
        sig = "0" * len(sig)
    h = {"X-Signature": f"kid={kid},ts={ts},sig={sig}"}
    if headers:
        h.update(headers)
    return client.post(f"/receipts/{channel}", content=raw, headers=h)


def task_detail(client, task_id):
    return client.get(f"{ROUTING}/tasks/{task_id}").json()["task"]


def latest_message(client, task_id):
    r = client.get("/admin/external-messages",
                   params={"task_id": task_id})
    msgs = r.json()["messages"]
    assert msgs, "expected registered external message"
    return msgs[0]


def set_policy(client, **kw):
    body = {"operator": GRANTOR}
    body.update(kw)
    r = client.put("/admin/receipt-policy", json=body)
    assert r.status_code == 200, r.text
    return r.json()["policy"]


# ---- message_id 登记 ---------------------------------------------------------

def test_message_id_registered_from_provider_and_local(env):
    client = env

    def email_with_id(addr, subject, body):
        return "provider-mid-1001"

    task, calls = make_route_task(client, "RCP-1",
                                  plan=("email", "inbox"),
                                  email_sender=email_with_id)
    assert task["status"] == "sent" and task["sent_channel"] == "email"
    assert task["receipt_status"] == "pending"
    msg = latest_message(client, task["id"])
    assert msg["message_id"] == "provider-mid-1001"
    assert msg["id_source"] == "provider"
    assert msg["status"] == "pending"
    assert msg["confirm_deadline"] > msg["registered_at"]
    assert len(calls["email"]) == 1


def test_local_message_id_when_sender_returns_none(env):
    client = env
    task, _ = make_route_task(client, "RCP-2", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    assert msg["message_id"].startswith("local:email:")
    assert msg["id_source"] == "local"


# ---- 验签 --------------------------------------------------------------------

def test_receipt_requires_valid_signature(env):
    client = env
    task, _ = make_route_task(client, "RCP-3", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "delivered"},
                     bad_sig=True)
    assert r.status_code == 401
    assert r.json()["reason"] == "signature_mismatch"
    # 验签失败不驱动状态机
    assert task_detail(client, task["id"])["receipt_status"] == "pending"


def test_receipt_unknown_channel_404(env):
    client = env
    raw = b"{}"
    ts, sig = receipt_sign(RECEIPT_EMAIL_SECRET, raw)
    r = client.post("/receipts/sms", content=raw, headers={
        "X-Signature": f"kid=k1,ts={ts},sig={sig}"})
    assert r.status_code == 404


# ---- delivered ---------------------------------------------------------------

def test_delivered_receipt_confirms_task(env):
    client = env
    task, _ = make_route_task(client, "RCP-4", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "delivered",
                      "recipient": "wang@example.com"})
    assert r.status_code == 200 and r.json()["result"] == "applied"
    t = task_detail(client, task["id"])
    assert t["receipt_status"] == "delivered"
    m = latest_message(client, task["id"])
    assert m["status"] == "delivered" and m["active_receipt_id"]

    hist = client.get("/admin/external-messages/" + str(m["id"])).json()
    statuses = [(h["from_status"], h["to_status"]) for h in hist["history"]]
    assert ("pending", "delivered") in statuses


# ---- 幂等：重复回执 -----------------------------------------------------------

def test_duplicate_receipt_is_idempotent(env):
    client = env
    task, calls = make_route_task(client, "RCP-5", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    body = {"message_id": msg["message_id"], "event": "delivered",
            "ts": 1757000000}
    first = send_receipt(client, "email", body)
    second = send_receipt(client, "email", body)
    third = send_receipt(client, "email", body)
    assert first.status_code == 200
    assert second.status_code == 200 and second.json()["result"] == "duplicate"
    assert third.json()["receipt_id"] == first.json()["receipt_id"]
    t = task_detail(client, task["id"])
    assert t["receipt_status"] == "delivered"
    assert len(calls["email"]) == 1   # 无第二次外部效果
    # 只有一条回执原文行
    rows = client.get("/admin/receipts",
                      params={"message_id": msg["message_id"]}).json()["receipts"]
    assert len(rows) == 1


def test_concurrent_duplicate_receipts_single_transition(env):
    client = env
    task, _ = make_route_task(client, "RCP-6", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    raw = json.dumps({"message_id": msg["message_id"],
                      "event": "delivered"}).encode()

    results = []

    def fire():
        ts, sig = receipt_sign(RECEIPT_EMAIL_SECRET, raw)
        r = client.post("/receipts/email", content=raw, headers={
            "X-Signature": f"kid=email-bootstrap,ts={ts},sig={sig}"})
        results.append(r.status_code)

    threads = [threading.Thread(target=fire) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert sorted(results) == [200] * 6
    m = latest_message(client, task["id"])
    assert m["status"] == "delivered"
    # 只有一次 pending -> delivered 转移
    hist = client.get(f"/admin/external-messages/{m['id']}").json()["history"]
    transitions = [h for h in hist if h["to_status"] == "delivered"
                   and h["kind"] == "message"]
    assert len(transitions) == 1


# ---- 乱序：终态不可回退 --------------------------------------------------------

def test_out_of_order_terminal_receipts_first_terminal_wins(env):
    client = env
    task, calls = make_route_task(client, "RCP-7", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    # 先 delivered
    r1 = send_receipt(client, "email",
                      {"message_id": msg["message_id"], "event": "delivered"})
    assert r1.status_code == 200
    # 后到 bounced：不得把 delivered 改回处理中/失败
    r2 = send_receipt(client, "email",
                      {"message_id": msg["message_id"], "event": "bounced"})
    assert r2.status_code == 200
    t = task_detail(client, task["id"])
    assert t["receipt_status"] == "delivered"
    m = latest_message(client, task["id"])
    assert m["status"] == "delivered"
    # 迟到终态原文仍落盘可查，并留 terminal_state_kept 历史
    rows = client.get("/admin/receipts",
                      params={"message_id": msg["message_id"]}).json()["receipts"]
    assert {r["event"] for r in rows} == {"delivered", "bounced"}
    hist = client.get(f"/admin/external-messages/{m['id']}").json()["history"]
    assert any(h["reason"] == "terminal_state_kept" for h in hist)
    assert len(calls["email"]) == 1   # 不触发重试


def test_bounced_then_delivered_keeps_bounced(env):
    client = env
    set_policy(client, on_bounced="manual")
    task, _ = make_route_task(client, "RCP-8", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "bounced"})
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "delivered"})
    t = task_detail(client, task["id"])
    assert t["status"] == "awaiting_manual"
    assert t["receipt_status"] == "bounced"
    m = latest_message(client, task["id"])
    assert m["status"] == "bounced"


# ---- 失败回执 -> 故障转移 ------------------------------------------------------

def test_bounced_receipt_failover_to_next_channel(env):
    client = env
    calls = {"email": 0, "webhook": 0}
    task, _ = make_route_task(
        client, "RCP-9", plan=("email", "webhook", "inbox"),
        email_sender=lambda a, s, b: calls.__setitem__("email", calls["email"] + 1),
        webhook_sender=lambda u, p: (
            calls.__setitem__("webhook", calls["webhook"] + 1),
            "wh-mid-9001")[1])
    assert calls == {"email": 1, "webhook": 0}
    msg = latest_message(client, task["id"])
    assert msg["channel"] == "email"
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "bounced"})
    assert r.status_code == 200 and r.json()["disposition"] == "rescheduled"
    t = task_detail(client, task["id"])
    assert t["receipt_status"] == "resending"
    assert t["receipt_retries"] == 1
    # worker 补发下一道（webhook）
    client.app.state.notif_worker.run_once()
    t = task_detail(client, task["id"])
    assert t["sent_channel"] == "webhook"
    assert calls == {"email": 1, "webhook": 1}
    new_msg = latest_message(client, task["id"])
    assert new_msg["channel"] == "webhook"
    assert new_msg["message_id"] == "wh-mid-9001"
    # 旧消息行 superseded
    old = client.get("/admin/external-messages",
                     params={"task_id": task["id"]}).json()["messages"]
    statuses = {m["message_id"]: m["status"] for m in old}
    assert statuses[msg["message_id"]] == "superseded"
    # webhook delivered 后确认
    send_receipt(client, "webhook",
                 {"message_id": "wh-mid-9001", "event": "delivered"})
    assert task_detail(client, task["id"])["receipt_status"] == "delivered"


def test_complained_default_goes_manual(env):
    client = env
    task, calls = make_route_task(client, "RCP-10",
                                  plan=("email", "webhook", "inbox"))
    msg = latest_message(client, task["id"])
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "complained"})
    assert r.json()["disposition"] == "manual"
    t = task_detail(client, task["id"])
    assert t["status"] == "awaiting_manual"
    assert t["receipt_status"] == "complained"
    assert len(calls["email"]) == 1


def test_failure_retries_exhausted_goes_manual(env):
    client = env
    set_policy(client, confirm_max_retries=1)
    task, calls = make_route_task(
        client, "RCP-11", plan=("email", "webhook", "inbox"),
        email_sender=lambda a, s, b: "m1",
        webhook_sender=lambda u, p: "m2")
    m1 = latest_message(client, task["id"])["message_id"]
    r1 = send_receipt(client, "email",
                      {"message_id": m1, "event": "bounced"})
    assert r1.json()["disposition"] == "rescheduled"
    client.app.state.notif_worker.run_once()
    m2 = latest_message(client, task["id"])["message_id"]
    r2 = send_receipt(client, "webhook",
                      {"message_id": m2, "event": "bounced"})
    assert r2.json()["disposition"] == "manual"
    t = task_detail(client, task["id"])
    assert t["status"] == "awaiting_manual"
    assert t["receipt_retries"] == 1
    assert len(calls["email"]) == 1 and len(calls["webhook"]) == 1


# ---- 超时扫描 -----------------------------------------------------------------

def test_timeout_scan_marks_confirmation_then_failover(env):
    client = env
    task, calls = make_route_task(
        client, "RCP-12", plan=("email", "webhook", "inbox"),
        email_sender=lambda a, s, b: "to-1",
        webhook_sender=lambda u, p: "to-2")
    msg = latest_message(client, task["id"])
    # 未超时：不处理
    res = client.app.state.notif_worker.run_once()
    assert task_detail(client, task["id"])["receipt_status"] == "pending"
    # 扫描器使用注入时钟：直接调用 scan 并给定超过 deadline 的时刻
    from app import receipts
    out = receipts.scan_confirmations(
        client.app.state.db, msg["confirm_deadline"] + 1)
    assert out["marked_awaiting_confirmation"] >= 1
    # 默认策略立即沿计划故障转移到 webhook：任务进入 resending 并补发
    t = task_detail(client, task["id"])
    assert t["receipt_status"] == "resending"
    assert t["receipt_retries"] == 1
    # worker 补发 webhook
    client.app.state.notif_worker.run_once()
    t = task_detail(client, task["id"])
    assert t["sent_channel"] == "webhook"
    assert len(calls["webhook"]) == 1
    # 待核对队列里旧消息已被替代（superseded，不再占用待确认）
    q = client.get("/admin/receipt-review-queue").json()
    assert all(m["message_pk"] != msg["id"]
               for m in q["awaiting_confirmation_messages"])


def test_timeout_scan_retry_exhausted_manual(env):
    client = env
    set_policy(client, confirm_max_retries=0)
    task, _ = make_route_task(client, "RCP-13",
                              plan=("email", "webhook", "inbox"))
    msg = latest_message(client, task["id"])
    from app import receipts
    out = receipts.scan_confirmations(
        client.app.state.db, msg["confirm_deadline"] + 1)
    assert out["awaiting_manual"] == 1
    t = task_detail(client, task["id"])
    assert t["status"] == "awaiting_manual"
    assert t["receipt_reason"] == "no_receipt_timeout"
    # 重复扫描不重复转移
    out2 = receipts.scan_confirmations(
        client.app.state.db, msg["confirm_deadline"] + 100)
    assert out2 == {"marked_awaiting_confirmation": 0, "rescheduled": 0,
                    "awaiting_manual": 0, "scanned": 0}


def test_timeout_scan_idempotent_on_concurrent_runs(env):
    client = env
    task, _ = make_route_task(client, "RCP-14",
                              plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    from app import receipts
    db = client.app.state.db
    errors = []

    def fire():
        try:
            receipts.scan_confirmations(db, msg["confirm_deadline"] + 1)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=fire) for _ in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors
    t = task_detail(client, task["id"])
    # email 计划后只有 inbox：故障转移到 inbox（业务通知不丢）；回执状态 resending
    assert t["receipt_retries"] == 1


# ---- 待核对队列：未知回执 -------------------------------------------------------

def test_unmatched_receipt_enters_review_queue(env):
    client = env
    make_route_task(client, "RCP-15", plan=("email", "inbox"))
    r = send_receipt(client, "email",
                     {"message_id": "unknown-mid-xyz", "event": "delivered"})
    assert r.status_code == 202 and r.json()["result"] == "unmatched"
    q = client.get("/admin/receipt-review-queue").json()
    ids = [x["id"] for x in q["unmatched_receipts"]]
    assert r.json()["receipt_id"] in ids

    rows = client.get("/admin/receipts",
                      params={"matched": "unmatched"}).json()["receipts"]
    assert rows[0]["raw_body"]  # 原文保留


def test_unmatched_receipt_delivered_after_send_can_replay_match(env):
    client = env
    # 回执比发送登记先到（乱序跨系统）：先进待核对
    r = send_receipt(client, "email",
                     {"message_id": "early-mid-1", "event": "delivered"})
    assert r.status_code == 202
    rid = r.json()["receipt_id"]
    # 之后发送成功并登记相同 message_id
    task, _ = make_route_task(client, "RCP-16", plan=("email", "inbox"),
                              email_sender=lambda a, s, b: "early-mid-1")
    # 重放：匹配并应用（本地状态推进，无外部效果）
    rr = client.post(f"/admin/receipts/{rid}/replay",
                     json={"operator": GRANTOR})
    assert rr.status_code == 200
    assert rr.json()["result"] == "replayed"
    assert task_detail(client, task["id"])["receipt_status"] == "delivered"


# ---- 人工绑定（不能改写原文） --------------------------------------------------

def test_manual_bind_unknown_receipt_to_task(env):
    client = env
    set_policy(client, on_bounced="manual")
    task, _ = make_route_task(client, "RCP-17",
                              plan=("webhook", "inbox"),
                              email_sender=None,
                              webhook_sender=lambda u, p: "wh-bound-1")
    # 外部回执先到但 message_id 与日志暂对不上：进待核对队列
    r2 = send_receipt(client, "webhook",
                      {"message_id": "ghost-2", "event": "bounced"})
    assert r2.status_code == 202
    rid = r2.json()["receipt_id"]
    b = client.post(f"/admin/receipts/{rid}/bind", json={
        "operator": GRANTOR, "task_id": task["id"],
        "note": "与日志核对一致"})
    # ghost-2 的 message_id 与任务登记行不一致，但通道一致：绑定到最新消息行
    assert b.status_code == 200 and b.json()["result"] == "bound"
    receipt = client.get(f"/admin/receipts/{rid}").json()["receipt"]
    assert receipt["raw_body"]  # 原文不变
    assert '"ghost-2"' in receipt["raw_body"]
    assert receipt["bound_by"] == GRANTOR
    t = task_detail(client, task["id"])
    assert t["status"] == "awaiting_manual"
    assert t["receipt_status"] == "bounced"


def test_bind_requires_channel_match(env):
    client = env
    task, _ = make_route_task(client, "RCP-18",
                              plan=("webhook", "inbox"),
                              webhook_sender=lambda u, p: "w18")
    r = send_receipt(client, "email",
                     {"message_id": "e18", "event": "delivered"})
    rid = r.json()["receipt_id"]
    b = client.post(f"/admin/receipts/{rid}/bind", json={
        "operator": GRANTOR, "task_id": task["id"]})
    assert b.status_code == 422


def test_ignore_unmatched_receipt(env):
    client = env
    make_route_task(client, "RCP-19", plan=("email", "inbox"))
    r = send_receipt(client, "email",
                     {"message_id": "noise", "event": "delivered"})
    rid = r.json()["receipt_id"]
    ir = client.post(f"/admin/receipts/{rid}/ignore",
                     json={"operator": GRANTOR, "reason": "测试流量"})
    assert ir.status_code == 200
    q = client.get("/admin/receipt-review-queue").json()
    assert all(x["id"] != rid for x in q["unmatched_receipts"])
    q2 = client.get("/admin/receipt-review-queue",
                    params={"include_ignored": True}).json()
    assert any(x["id"] == rid for x in q2["unmatched_receipts"])


# ---- 人工处置 awaiting_manual --------------------------------------------------

def test_manual_resolve_retry_starts_new_round(env):
    client = env
    set_policy(client, on_bounced="manual")
    task, calls = make_route_task(client, "RCP-20",
                                  plan=("email", "webhook", "inbox"))
    msg = latest_message(client, task["id"])["message_id"]
    send_receipt(client, "email",
                 {"message_id": msg, "event": "bounced"})
    assert task_detail(client, task["id"])["status"] == "awaiting_manual"
    rr = client.post(f"{ROUTING}/tasks/{task['id']}/receipt-resolve",
                     json={"operator": GRANTOR, "action": "retry",
                           "note": "联系人地址已更正"})
    assert rr.status_code == 200 and rr.json()["result"] == "retry_scheduled"
    client.app.state.notif_worker.run_once()
    t = task_detail(client, task["id"])
    assert t["status"] == "sent" and t["sent_channel"] == "webhook"
    assert len(calls["webhook"]) == 1


def test_manual_resolve_ignore_keeps_terminal(env):
    client = env
    set_policy(client, on_bounced="manual")
    task, calls = make_route_task(client, "RCP-21", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])["message_id"]
    send_receipt(client, "email",
                 {"message_id": msg, "event": "bounced"})
    rr = client.post(f"{ROUTING}/tasks/{task['id']}/receipt-resolve",
                     json={"operator": GRANTOR, "action": "ignore"})
    assert rr.status_code == 200
    t = task_detail(client, task["id"])
    assert t["status"] == "sent" and t["receipt_status"] == "bounced"
    assert len(calls["email"]) == 1


def test_manual_resolve_requires_awaiting_manual(env):
    client = env
    task, _ = make_route_task(client, "RCP-22", plan=("email", "inbox"))
    rr = client.post(f"{ROUTING}/tasks/{task['id']}/receipt-resolve",
                     json={"operator": GRANTOR, "action": "retry"})
    assert rr.status_code == 409


# ---- 安全重放 ------------------------------------------------------------------

def test_replay_terminal_receipt_does_not_change_state(env):
    client = env
    task, calls = make_route_task(client, "RCP-23", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "delivered"})
    rid = client.get("/admin/receipts",
                     params={"message_id": msg["message_id"]}
                     ).json()["receipts"][0]["id"]
    for _ in range(3):
        rr = client.post(f"/admin/receipts/{rid}/replay",
                         json={"operator": GRANTOR})
        assert rr.status_code == 200
        assert rr.json()["kept_status"] == "delivered"
    assert task_detail(client, task["id"])["receipt_status"] == "delivered"
    assert len(calls["email"]) == 1


# ---- 查询：过滤维度 -------------------------------------------------------------

def test_admin_queries_by_channel_recipient_time_status(env):
    client = env
    task, _ = make_route_task(client, "RCP-24", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    before = time.time()
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "delivered"})
    # 通道
    assert client.get("/admin/receipts",
                      params={"channel": "email"}).json()["count"] == 1
    assert client.get("/admin/receipts",
                      params={"channel": "webhook"}).json()["count"] == 0
    # 接收人（消息侧按联系人名；回执行若带邮箱则以报文为准）
    got = client.get("/admin/receipts",
                     params={"recipient": LEAD_A}).json()
    assert got["count"] == 1
    got = client.get("/admin/receipts",
                     params={"recipient": "nobody"}).json()
    assert got["count"] == 0
    # 状态（消息侧）
    got = client.get("/admin/receipts",
                     params={"status": "delivered"}).json()
    assert got["count"] == 1
    # 时间窗
    got = client.get("/admin/receipts",
                     params={"time_from": before - 10}).json()
    assert got["count"] == 1
    got = client.get("/admin/receipts",
                     params={"time_to": before - 10}).json()
    assert got["count"] == 0
    # 历史查询
    hist = client.get("/admin/receipt-history",
                      params={"channel": "email", "status": "delivered"}).json()
    assert hist["count"] >= 1


# ---- 重启恢复 -------------------------------------------------------------------

def test_restart_recover_in_flight_then_receipt_still_works(env):
    client = env
    task, _ = make_route_task(client, "RCP-25", plan=("email", "inbox"))
    # 模拟崩溃：任务被错误置 in_flight，recover 退回 pending；已 sent 的不受影响
    client.app.state.notif_worker.recover()
    msg = latest_message(client, task["id"])
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "delivered"})
    assert task_detail(client, task["id"])["receipt_status"] == "delivered"


# ---- 密钥轮换（回执专用，独立于回调密钥环） --------------------------------------

def test_receipt_key_rotation_and_retire(env):
    client = env
    task, _ = make_route_task(client, "RCP-26", plan=("email", "inbox"))
    msg = latest_message(client, task["id"])
    # 新钥
    r = client.post("/admin/receipt-keys/email", json={
        "operator": GRANTOR, "kid": "rk2", "secret": "new-receipt-secret"})
    assert r.status_code == 200
    grace = r.json()["grace_until"]
    # 旧钥过渡期内仍可用
    ok = send_receipt(client, "email",
                      {"message_id": msg["message_id"], "event": "delivered"},
                      secret=RECEIPT_EMAIL_SECRET, kid="email-bootstrap")
    assert ok.status_code == 200
    keys = client.get("/admin/receipt-keys").json()["keys"]
    by_kid = {k["kid"]: k for k in keys}
    assert by_kid["rk2"]["status"] == "active"
    assert by_kid["email-bootstrap"]["status"] == "retired"
    assert "secret" not in json.dumps(keys)
    # 列表不回显明文
    assert grace > time.time()


def test_retired_key_after_grace_rejected(env):
    client = env
    make_route_task(client, "RCP-27", plan=("email", "inbox"))
    created = client.post("/admin/receipt-keys/email", json={
        "operator": GRANTOR, "kid": "rk2", "secret": "s2"}).json()
    # 轮换后旧钥已自动 retired（默认 24h 过渡）；立即结束过渡期后旧签名必须失效
    old_id = next(k["id"] for k in client.get("/admin/receipt-keys").json()["keys"]
                  if k["kid"] == "email-bootstrap")
    # 已 retired 的密钥仍可再次收紧过渡期（吊销）
    r2 = client.post(f"/admin/receipt-keys/{old_id}/retire", json={
        "operator": GRANTOR, "grace_until": time.time() - 1})
    assert r2.status_code == 200
    r = send_receipt(client, "email",
                     {"message_id": "whatever", "event": "delivered"},
                     secret=RECEIPT_EMAIL_SECRET, kid="email-bootstrap")
    assert r.status_code == 401
    assert r.json()["reason"] == "retired_key_grace_expired"


# ---- 策略管理 -------------------------------------------------------------------

def test_policy_update_validated(env):
    client = env
    p = set_policy(client, confirm_timeout_seconds=30,
                   confirm_max_retries=1, on_expired="manual")
    assert p["confirm_timeout_seconds"] == 30
    assert p["on_expired"] == "manual"
    bad = client.put("/admin/receipt-policy", json={
        "operator": GRANTOR, "on_bounced": "explode"})
    assert bad.status_code == 422


# ---- 多种字段别名与 unknown 事件 --------------------------------------------------

def test_field_aliases_and_unknown_event(env):
    client = env
    task, _ = make_route_task(client, "RCP-28", plan=("webhook", "inbox"),
                              webhook_sender=lambda u, p: "ses-msg-1")
    r = send_receipt(client, "webhook", {
        "messageId": "ses-msg-1", "eventType": "Delivery",
        "mail": {"timestamp": "2026-09-13T10:00:00Z"}})
    assert r.status_code == 200
    assert task_detail(client, task["id"])["receipt_status"] == "delivered"

    task2, _ = make_route_task(client, "RCP-29", plan=("webhook", "inbox"),
                               webhook_sender=lambda u, p: "unk-1")
    r2 = send_receipt(client, "webhook",
                      {"message_id": "unk-1", "event": "opened"})
    # unknown 事件落盘留痕但不驱动状态机
    assert r2.status_code == 200
    assert task_detail(client, task2["id"])["receipt_status"] == "pending"


# ---- 旧链路（未发布路由版本）也登记 message_id ------------------------------------

def test_legacy_delivery_registers_message(env):
    client = env
    # 不发布路由版本 -> 走旧链路
    client.app.state.notif_worker.senders = {
        "email": lambda a, s, b: "legacy-mid-1",
        "webhook": lambda u, p: None,
        "inbox": lambda task: None}
    apply_policy(client)
    make_contact(client, channels=["email"])
    process_normally(client, "RCP-30")
    submit_high(client, "RCP-30")
    delegation(client)
    client.app.state.notif_worker.run_once()
    msgs = client.get("/admin/external-messages",
                      params={"channel": "email"}).json()["messages"]
    assert any(m["message_id"] == "legacy-mid-1" and
               m["source"] == "legacy" for m in msgs)
    mid = next(m["id"] for m in msgs if m["message_id"] == "legacy-mid-1")
    r = send_receipt(client, "email",
                     {"message_id": "legacy-mid-1", "event": "delivered"})
    assert r.status_code == 200
    detail = client.get(f"/admin/external-messages/{mid}").json()["message"]
    assert detail["status"] == "delivered"

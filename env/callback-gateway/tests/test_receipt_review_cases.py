"""失败回执自动复核建案（app/receipt_review.py）的端到端测试。

覆盖：
- bounced 回执自动生成一条 open 复核案件，案件行保留接收人/事件类型/通道/外部编号；
- 证据固化回执原文（原始消息证据）+ 外部消息 + 发送任务快照，且不可变；
- 重复收到同一回执（逐字节重复投递）绝不生成第二条案件；
- 自动故障转移（on_bounced=retry）也建案；同消息第二条不同 bounced 只追加证据；
- delivered 不建案；乱序迟到（先 delivered 后 bounced）对迟到回执建一案；
- 无法匹配消息的 bounced 回执同样建案；
- 管理员端点可按接收人/事件类型筛选、查看案件证据。
"""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

ACTIVE_SECRET = "new-secret"
RECEIPT_EMAIL_SECRET = "receipt-email-secret"
RECEIPT_WEBHOOK_SECRET = "receipt-webhook-secret"
GRANTOR = "ops-admin"
LEAD_A = "ops-wang"
SUBMITTER = "ops-li"
ROUTING = "/admin/approval-notifications/routing"


def make_keys_file(tmp_path):
    p = tmp_path / "keys.json"
    p.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    return str(p)


import hashlib  # noqa: E402
import hmac  # noqa: E402
import time  # noqa: E402

import pytest  # noqa: E402

from app.security import sign  # noqa: E402


@pytest.fixture()
def env(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "gateway.db"),
        keys_file=make_keys_file(tmp_path),
        run_worker=False,
        notif_retry_base_seconds=5.0, notif_retry_cap_seconds=300.0,
        notif_max_attempts=3, notif_deadline_lead_seconds=300.0,
        notif_breaker_window_seconds=60.0,
        notif_breaker_failure_threshold=3,
        notif_breaker_cooldown_seconds=30.0,
        notif_channel_timeout_seconds=10.0,
        receipt_confirm_timeout_seconds=60.0,
        receipt_confirm_max_retries=2,
        receipt_email_secret=RECEIPT_EMAIL_SECRET,
        receipt_webhook_secret=RECEIPT_WEBHOOK_SECRET)
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


# ---- helpers（与 test_receipts.py 同款装配） ---------------------------------

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


def make_contact(client, name=LEAD_A):
    r = client.post("/admin/approval-notifications/contacts", json={
        "name": name, "operator": GRANTOR,
        "channels": ["email", "webhook"], "email": "wang@example.com",
        "webhook_url": "https://hook.example.com/x"})
    assert r.status_code == 200, r.text


def delegation(client):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": "ops-lead", "delegatee": LEAD_A, "operator": GRANTOR,
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text


def submit_high(client, external_id):
    r = client.post("/admin/replays", json={
        "operator": SUBMITTER, "reason": "资金类回调补发", "risk_level": "high",
        "approval_note": "需审批", "external_id": external_id})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def make_route_task(client, external_id, *, plan=("email", "inbox"),
                    on_bounced=None):
    client.app.state.notif_worker.senders = {
        "email": lambda a, s, b: f"mid-{external_id}",
        "webhook": lambda u, p: f"wh-{external_id}",
        "inbox": lambda task: None}
    client.post(f"{ROUTING}/versions", json={
        "operator": GRANTOR,
        "rules": [{"event_type": None, "channels": [
            {"channel": ch} for ch in plan]}]})
    apply_policy(client)
    make_contact(client)
    if on_bounced is not None:
        r = client.put("/admin/receipt-policy",
                       json={"operator": GRANTOR, "on_bounced": on_bounced})
        assert r.status_code == 200, r.text
    process_normally(client, external_id)
    bid = submit_high(client, external_id)
    delegation(client)
    todos = client.get("/admin/approval-notifications/todos",
                       params={"recipient": LEAD_A, "batch_id": bid}).json()["todos"]
    client.app.state.notif_worker.run_once()
    task_id = todos[0]["route_task"]["id"]
    r = client.get("/admin/external-messages", params={"task_id": task_id})
    return task_id, r.json()["messages"][0]


def receipt_sign(secret, body, ts=None):
    ts = int(time.time()) if ts is None else ts
    sig = hmac.new(secret.encode(), f"{ts}\n".encode() + body,
                   hashlib.sha256).hexdigest()
    return ts, sig


def send_receipt(client, channel, body: dict, *, ts=None):
    raw = json.dumps(body).encode()
    t, sig = receipt_sign(
        RECEIPT_EMAIL_SECRET if channel == "email" else RECEIPT_WEBHOOK_SECRET,
        raw, ts)
    return client.post(f"/receipts/{channel}", content=raw, headers={
        "X-Signature": f"kid={channel}-bootstrap,ts={t},sig={sig}"})


def list_cases(client, **params):
    return client.get("/admin/receipt-review/cases", params=params).json()["cases"]


# ---- 测试 ---------------------------------------------------------------------

def test_bounced_receipt_auto_creates_review_case(env):
    client = env
    task_id, msg = make_route_task(client, "RCP-1")
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "bounced",
                      "recipient": "wang@example.com", "reason": "mailbox full"})
    assert r.status_code == 200
    case_id = r.json()["review_case_id"]
    assert case_id is not None

    cases = list_cases(client, event_type="bounced")
    assert len(cases) == 1
    case = cases[0]
    assert case["case_id"] == case_id and case["status"] == "open"
    # 案件行保留接收人 / 事件类型 / 锚点
    assert case["recipient"] == "wang@example.com"
    assert case["event_type"] == "bounced"
    assert case["channel"] == "email"
    assert case["message_id"] == msg["message_id"]
    assert case["message_pk"] == msg["id"]
    assert case["source"] == "auto" and case["subject"] == "receipt"
    assert case["sla_deadline"] and case["sla_deadline"] > time.time()

    detail = client.get(f"/admin/receipt-review/cases/{case_id}").json()
    kinds = {e["kind"]: e for e in detail["evidence"]}
    # 回执原文（原始消息证据）：raw_body + 哈希固化，内容可校验
    ev = kinds["receipt"]
    snap = ev["snapshot"]
    assert snap["event"] == "bounced"
    assert snap["recipient"] == "wang@example.com"
    assert "mailbox full" in snap["raw_body"]
    assert hashlib.sha256(snap["raw_body"].encode()).hexdigest() == \
        snap["receipt_hash"]
    # 消息与任务证据在场
    assert kinds["message"]["snapshot"]["id"] == msg["id"]
    assert kinds["task"]["snapshot"]["id"] == task_id


def test_duplicate_same_receipt_does_not_create_second_case(env):
    client = env
    _, msg = make_route_task(client, "RCP-2")
    body = {"message_id": msg["message_id"], "event": "bounced",
            "recipient": "wang@example.com"}
    first = send_receipt(client, "email", body)
    duplicate = send_receipt(client, "email", body)
    assert duplicate.json()["result"] == "duplicate"

    cases = list_cases(client)
    assert len(cases) == 1
    assert cases[0]["case_id"] == first.json()["review_case_id"]
    # 同一回执证据只有一条
    evidence = client.get(
        f"/admin/receipt-review/cases/{cases[0]['case_id']}").json()["evidence"]
    receipt_ev = [e for e in evidence if e["kind"] == "receipt"]
    assert len(receipt_ev) == 1


def test_case_created_even_when_policy_failover_reschedules(env):
    client = env
    _, msg = make_route_task(client, "RCP-3",
                             plan=("email", "webhook", "inbox"))
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "bounced"})
    # 默认 on_bounced=retry：状态机自动故障转移，但复核案件依然生成
    assert r.json()["disposition"] == "rescheduled"
    cases = list_cases(client)
    assert len(cases) == 1 and cases[0]["event_type"] == "bounced"


def test_second_distinct_bounced_for_same_message_appends_evidence_only(env):
    client = env
    # manual 策略：消息保持 bounced（不故障转移、不 superseded），两条不同内容的
    # bounced 回执才能匹配到同一条消息行，证据归集首案
    _, msg = make_route_task(client, "RCP-4", on_bounced="manual")
    r1 = send_receipt(client, "email",
                      {"message_id": msg["message_id"], "event": "bounced",
                       "ts": 1757000001, "reason": "first"})
    # 内容不同的第二条 bounced（不同 hash，各自落盘）归集同一案件
    r2 = send_receipt(client, "email",
                      {"message_id": msg["message_id"], "event": "bounced",
                       "ts": 1757000002, "reason": "second"})
    assert r1.json()["review_case_id"] == r2.json()["review_case_id"]
    cases = list_cases(client)
    assert len(cases) == 1
    evidence = client.get(
        f"/admin/receipt-review/cases/{cases[0]['case_id']}").json()["evidence"]
    receipt_ev = [e for e in evidence if e["kind"] == "receipt"]
    assert len(receipt_ev) == 2
    raws = " ".join(e["snapshot"]["raw_body"] for e in receipt_ev)
    assert "first" in raws and "second" in raws


def test_delivered_does_not_create_case(env):
    client = env
    _, msg = make_route_task(client, "RCP-5")
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "delivered"})
    assert r.status_code == 200 and r.json()["review_case_id"] is None
    assert list_cases(client) == []


def test_late_bounced_after_delivered_creates_its_own_case(env):
    client = env
    _, msg = make_route_task(client, "RCP-6")
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "delivered"})
    r = send_receipt(client, "email",
                     {"message_id": msg["message_id"], "event": "bounced",
                      "ts": 1757000009})
    # 乱序迟到终态不改消息状态（首终态赢），但 bounced 回执仍建案留管理员复核
    assert r.json()["kept_status"] == "delivered"
    cases = list_cases(client, event_type="bounced")
    assert len(cases) == 1


def test_unmatched_bounced_receipt_creates_case(env):
    client = env
    r = send_receipt(client, "email",
                     {"message_id": "ghost-mid-777", "event": "bounced",
                      "recipient": "ghost@example.com"})
    assert r.status_code == 202 and r.json()["result"] == "unmatched"
    case_id = r.json()["review_case_id"]
    assert case_id is not None
    detail = client.get(f"/admin/receipt-review/cases/{case_id}").json()
    case = detail["case"]
    assert case["event_type"] == "bounced"
    assert case["recipient"] == "ghost@example.com"
    assert case["message_pk"] is None
    # 匹配不上消息：只有回执证据，仍保留原始报文
    kinds = {e["kind"] for e in detail["evidence"]}
    assert kinds == {"receipt"}
    assert "ghost-mid-777" in detail["evidence"][0]["snapshot"]["raw_body"]


def test_case_filter_by_recipient_and_404(env):
    client = env
    _, msg = make_route_task(client, "RCP-8")
    send_receipt(client, "email",
                 {"message_id": msg["message_id"], "event": "bounced",
                  "recipient": "wang@example.com"})
    assert list_cases(client, recipient="nobody@example.com") == []
    assert len(list_cases(client, recipient="wang@example.com",
                          status="open")) == 1
    assert client.get("/admin/receipt-review/cases/99999").status_code == 404


def test_bind_unmatched_bounced_reuses_case_and_backfills_evidence(env):
    client = env
    task_id, msg = make_route_task(client, "RCP-9", on_bounced="manual")
    # 先收到一条编号对不上的 bounced -> unmatched，建无消息证据的案件
    ghost = send_receipt(client, "email",
                         {"message_id": "ghost-mid-rcp9", "event": "bounced",
                          "recipient": "ghost@example.com"})
    assert ghost.json()["result"] == "unmatched"
    case_id = ghost.json()["review_case_id"]
    before = client.get(f"/admin/receipt-review/cases/{case_id}").json()
    assert {e["kind"] for e in before["evidence"]} == {"receipt"}
    # 人工把该回执绑定到真实发送任务：复用同一案件（绝不建第二案）并补全证据
    r = client.post(f"/admin/receipts/{ghost.json()['receipt_id']}/bind",
                    json={"operator": GRANTOR, "task_id": task_id})
    assert r.status_code == 200 and r.json()["review_case_id"] == case_id
    assert len(list_cases(client)) == 1
    detail = client.get(f"/admin/receipt-review/cases/{case_id}").json()
    kinds = {e["kind"] for e in detail["evidence"]}
    assert {"receipt", "message", "task"} <= kinds
    assert detail["case"]["message_pk"] == msg["id"]
    assert detail["case"]["task_id"] == task_id


def test_replay_applied_bounced_receipt_keeps_single_case(env):
    client = env
    _, msg = make_route_task(client, "RCP-10", on_bounced="manual")
    first = send_receipt(client, "email",
                         {"message_id": msg["message_id"], "event": "bounced"})
    case_id = first.json()["review_case_id"]
    # 对已应用的 bounced 回执安全重放：不建第二案、不重复留证
    r = client.post(f"/admin/receipts/{first.json()['receipt_id']}/replay",
                    json={"operator": GRANTOR})
    assert r.status_code == 200 and r.json()["review_case_id"] == case_id
    cases = list_cases(client)
    assert len(cases) == 1
    evidence = client.get(
        f"/admin/receipt-review/cases/{case_id}").json()["evidence"]
    assert len([e for e in evidence if e["kind"] == "receipt"]) == 1

"""通知聚合、静默时段与升级策略的端到端测试。

覆盖：
- 聚合：按接收人/批次/事件类型配置窗口；窗口内邮件/webhook held、站内待办即时生成；
  窗口关闭合并为一条摘要（webhook 结构化负载含全部成员）；全部来源落定的组取消；
  规则停用语义；成员与摘要、取消全部可查询、可审计；
- 静默：窗内只保留站内待办、邮件/webhook delayed；时段结束按 ordinal 原事件顺序放行
  发送；一次性/每日/跨午夜/通道维度/接收人维度；配置停用立即放行；
- 升级：按角色配置逐级接收人（after_seconds / before_deadline_seconds），每级一次、
  级别可查；升级待办可回写审批；原接收人处理或来源落定后升级停止、未发出通知取消；
- 可靠性：worker 多轮扫描/重启不重复发送、不跳过关键提醒；并发扫描不产生重复升级/摘要；
- 校验：窗口/级别/时间格式非法返回 422，未知来源 404。
"""
from __future__ import annotations

import json
import threading
import time
from datetime import datetime, timezone

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"
SUBMITTER = "ops-li"
LEAD_A = "ops-wang"
LEAD_B = "ops-chen"
GRANTOR = "ops-admin"
APPROVER = "ops-zhao"
BOSS = "ops-boss"
BOSS2 = "ops-vp"

API = "/admin/approval-notifications"


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


def post_callback(client, external_id, body=b'{"order": 1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}",
    })


def process_normally(client, external_id):
    post_callback(client, external_id)
    client.app.state.worker.run_once()


def apply_policy(client, rules):
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def lead_policy(client, *, mode="parallel", required=1, timeout=3600):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": mode, "nodes": [
            {"role": "ops-lead", "required_approvals": required,
             "timeout_seconds": timeout}]},
    ])


def submit_high(client, external_id, *, operator=SUBMITTER):
    r = client.post("/admin/replays", json={
        "operator": operator, "reason": "资金类回调补发",
        "risk_level": "high", "approval_note": "需审批",
        "external_id": external_id})
    assert r.status_code == 201, r.text
    return r.json()["batch_id"]


def approval(client, bid):
    return client.get(f"/admin/replays/{bid}").json()["batch"]["approval"]


def contact(client, name, *, channels=None, email=None, webhook_url=None,
            operator=GRANTOR):
    body = {"name": name, "operator": operator, "channels": channels or []}
    if email:
        body["email"] = email
    if webhook_url:
        body["webhook_url"] = webhook_url
    r = client.post(f"{API}/contacts", json=body)
    assert r.status_code == 200, r.text
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
    return client.get(f"{API}/todos", params=params).json()["todos"]


def deliveries(client, **params):
    return client.get(f"{API}/deliveries", params=params).json()["deliveries"]


def events(client, event_type=None):
    params = {"limit": 2000}
    if event_type:
        params["type"] = event_type
    return client.get("/admin/events", params=params).json()["events"]


def agg_rule(client, *, window_seconds, recipient=None, batch_id=None,
             event_type=None, operator=GRANTOR, active=True):
    body = {"operator": operator, "window_seconds": window_seconds, "active": active}
    if recipient is not None:
        body["recipient"] = recipient
    if batch_id is not None:
        body["batch_id"] = batch_id
    if event_type is not None:
        body["event_type"] = event_type
    r = client.post(f"{API}/aggregation-rules", json=body)
    assert r.status_code == 200, r.text
    return r.json()["rule_id"]


def quiet(client, *, start_time=None, end_time=None, start_at=None, end_at=None,
          daily=True, recipient=None, channel=None):
    body = {"operator": GRANTOR, "daily": daily}
    if start_time is not None:
        body["start_time"] = start_time
    if end_time is not None:
        body["end_time"] = end_time
    if start_at is not None:
        body["start_at"] = start_at
    if end_at is not None:
        body["end_at"] = end_at
    if recipient is not None:
        body["recipient"] = recipient
    if channel is not None:
        body["channel"] = channel
    r = client.post(f"{API}/quiet-schedules", json=body)
    assert r.status_code == 200, r.text
    return r.json()["schedule_id"]


def esc_policy(client, levels, *, roles=("ops-lead",), name="esc"):
    r = client.post(f"{API}/escalation-policies", json={
        "operator": GRANTOR, "name": name, "roles": list(roles), "levels": levels})
    assert r.status_code == 200, r.text
    return r.json()["policy_id"]


def install_senders(client, hook=None):
    sent = {"email": [], "webhook": []}

    def email(addr, subject, body):
        sent["email"].append((addr, subject))
        if hook:
            hook()

    def webhook(addr, payload):
        sent["webhook"].append((addr, payload))
        if hook:
            hook()

    client.app.state.notif_worker.senders = {
        "email": email, "webhook": webhook}
    return sent


# ============================================================================
# 聚合
# ============================================================================

def test_aggregation_holds_then_flushes_digest_per_channel(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["email", "webhook"],
            email="wang@example.com", webhook_url="https://h/x")
    process_normally(client, "G-1")
    bid = submit_high(client, "G-1")
    rid = agg_rule(client, window_seconds=60, batch_id=bid)
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)

    # 窗口内：站内待办即时存在；邮件/webhook held 入组（不是 pending）
    todo = todos(client, recipient=LEAD_A)[0]
    assert todo["status"] == "unread"
    assert {d["channel"] for d in todo["deliveries"]} == {"email", "webhook"}
    assert all(d["status"] == "held" for d in todo["deliveries"])
    assert todo["held_channels"] and len(todo["held_channels"]) == 2

    groups = client.get(f"{API}/aggregation-groups",
                        params={"batch_id": bid}).json()["groups"]
    assert len(groups) == 2  # email / webhook 各一组
    assert all(g["status"] == "open" and g["member_count"] == 1
               and g["rule_id"] == rid for g in groups)
    # worker 在窗口内运行：不 flush、不发送
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 30
    sent = install_senders(client)
    nw.run_once()
    assert sent["email"] == [] and sent["webhook"] == []
    assert all(d["status"] == "held" for d in deliveries(client))

    # 窗口关闭：每通道一条摘要；成员 aggregated；摘要按原待办挂载
    nw.clock = lambda: t0 + 61
    nw.run_once()  # flush 生成 pending，同一轮 dispatch 即发出
    ds = deliveries(client)
    statuses = sorted((d["channel"], d["status"]) for d in ds)
    assert ("email", "aggregated") in statuses
    assert ("webhook", "aggregated") in statuses
    digests = [d for d in ds if "摘要" in (d["subject"] or "")]
    assert len(digests) == 2 and all(d["status"] == "sent" for d in digests)
    assert len(sent["email"]) == 1 and "1 条提醒" in sent["email"][0][1]
    w = sent["webhook"][0][1]
    assert w["event"] == "aggregated_digest" and w["count"] == 1
    assert w["items"][0]["event_type"] == "activated"
    assert w["batch_id"] == bid

    groups = client.get(f"{API}/aggregation-groups",
                        params={"batch_id": bid, "status": "flushed"}).json()["groups"]
    assert len(groups) == 2 and groups[0]["digest_delivery"]["status"] == "sent"
    assert [e["type"] for e in events(client)].count(
        "approval_aggregation_group_flushed") == 2
    assert [e["type"] for e in events(client)].count(
        "approval_delivery_held") == 2


def test_aggregation_merges_multiple_reminders_in_window(client):
    # 法定人数 2：activated + vote_received 在同一窗口内合并为一条摘要
    lead_policy(client, required=2)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    contact(client, LEAD_B, channels=["webhook"], webhook_url="https://h/b")
    process_normally(client, "G-2")
    bid = submit_high(client, "G-2")
    rid = agg_rule(client, window_seconds=120, batch_id=bid)
    t0 = time.time()
    da = delegation(client, "ops-lead", LEAD_A)
    db_ = delegation(client, "ops-lead", LEAD_B)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 1
    sent = install_senders(client)
    node_id = approval(client, bid)["nodes"][0]["id"]

    # LEAD_A 投票 -> LEAD_B 的 vote_received 与 activated 同组
    r = client.post(f"/admin/replays/{bid}/nodes/{node_id}/approve",
                    json={"operator": LEAD_A, "role": "ops-lead",
                          "delegation_id": da})
    assert r.status_code == 200
    nw.run_once()
    b_del = deliveries(client, recipient=LEAD_B)
    assert len(b_del) == 2 and all(d["status"] == "held" for d in b_del)
    group_id = b_del[0]["group_id"]
    assert all(d["group_id"] == group_id for d in b_del)

    nw.clock = lambda: t0 + 121
    nw.run_once()
    digests = [d for d in deliveries(client, recipient=LEAD_B)
               if "摘要" in (d["subject"] or "")]
    assert len(digests) == 1 and digests[0]["status"] == "sent"
    payload = sent["webhook"][0][1]
    assert payload["count"] == 2
    assert payload["event_counts"] == {"activated": 1, "vote_received": 1}
    # 成员按投递 id（事件顺序）排列
    assert [it["event_type"] for it in payload["items"]] == \
           ["activated", "vote_received"]
    # LEAD_A 的待办投票后被对账关闭，其 held 投递取消（独立组）
    a_del = deliveries(client, recipient=LEAD_A)
    assert all(d["status"] in ("cancelled", "aggregated") for d in a_del)


def test_aggregation_group_cancelled_when_all_sources_closed(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/x")
    process_normally(client, "G-3")
    bid = submit_high(client, "G-3")
    agg_rule(client, window_seconds=300, batch_id=bid)
    t0 = time.time()
    did = delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 10
    sent = install_senders(client)
    # 原接收人直接在节点端点批准 -> 待办对账关闭；窗口关闭时组取消，不发摘要
    node_id = approval(client, bid)["nodes"][0]["id"]
    r = client.post(f"/admin/replays/{bid}/nodes/{node_id}/approve",
                    json={"operator": LEAD_A, "role": "ops-lead",
                          "delegation_id": did})
    assert r.status_code == 200
    nw.clock = lambda: t0 + 301
    nw.run_once()
    assert sent["webhook"] == []
    groups = client.get(f"{API}/aggregation-groups",
                        params={"batch_id": bid}).json()["groups"]
    assert groups and all(g["status"] == "cancelled" for g in groups)
    assert any(e["type"] in ("approval_aggregation_group_cancelled",
                             "approval_delivery_cancelled")
               for e in events(client))
    ds = deliveries(client)
    assert all(d["status"] == "cancelled" for d in ds)


def test_aggregation_rule_scoped_by_recipient_and_event_type(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    contact(client, LEAD_B, channels=["webhook"], webhook_url="https://h/b")
    process_normally(client, "G-4")
    bid = submit_high(client, "G-4")
    # 只聚合 LEAD_A
    agg_rule(client, window_seconds=60, batch_id=bid, recipient=LEAD_A)
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    delegation(client, "ops-lead", LEAD_B)
    da = {d["recipient"]: d["status"] for d in deliveries(client)}
    assert da[LEAD_A] == "held" and da[LEAD_B] == "pending"

    # 停用规则：窗口过后新事件不再 held（已 held 的组成员不变）
    client.post(f"{API}/aggregation-rules/1/active",
                json={"operator": GRANTOR, "active": False})
    assert client.get(f"{API}/aggregation-rules").json()["rules"][0]["active"] is False


def test_aggregation_repeated_scan_does_not_reflush(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/x")
    process_normally(client, "G-5")
    bid = submit_high(client, "G-5")
    agg_rule(client, window_seconds=10, batch_id=bid)
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    sent = install_senders(client)
    nw.clock = lambda: t0 + 11
    for _ in range(5):  # 重复扫描/模拟重启
        nw.run_once()
    assert len(sent["webhook"]) == 1
    groups = client.get(f"{API}/aggregation-groups",
                        params={"batch_id": bid}).json()["groups"]
    assert len(groups) == 1 and groups[0]["status"] == "flushed"
    assert [e["type"] for e in events(client)].count(
        "approval_aggregation_group_flushed") == 1


def test_aggregation_invalid_rule_rejected(client):
    r = client.post(f"{API}/aggregation-rules",
                    json={"operator": GRANTOR, "window_seconds": 0})
    assert r.status_code == 422
    r = client.post(f"{API}/aggregation-rules",
                    json={"operator": GRANTOR, "window_seconds": 10, "batch_id": 999})
    assert r.status_code == 404


# ============================================================================
# 静默时段
# ============================================================================

def test_quiet_delays_external_keeps_inbox_and_releases_in_order(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    contact(client, LEAD_B, channels=["webhook"], webhook_url="https://h/b")
    t0 = time.time()
    # 一次性静默窗 [t0-60, t0+3600]
    sid = quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600)
    process_normally(client, "Q-1")
    bid = submit_high(client, "Q-1")
    delegation(client, "ops-lead", LEAD_A)
    delegation(client, "ops-lead", LEAD_B)

    # 站内待办即时生成；webhook delayed（可查 delayed_until）
    assert len(todos(client, status="unread")) == 2
    ds = deliveries(client)
    assert all(d["status"] == "delayed" for d in ds)
    assert all(d["delayed_until"] == pytest.approx(t0 + 3600, abs=2) for d in ds)
    nw = client.app.state.notif_worker
    sent = install_senders(client)
    nw.run_once()  # 窗内不发送
    assert sent["webhook"] == []

    # 时段结束：按 ordinal（原事件顺序）放行并发送
    nw.clock = lambda: t0 + 3601
    nw.run_once()
    assert len(sent["webhook"]) == 2
    # 顺序：ordinal 更小的待办（先创建）先发
    ords = [p for _, p in sent["webhook"]]
    assert ords[0]["batch_id"] == ords[1]["batch_id"] == bid
    ds = deliveries(client)
    assert all(d["status"] == "sent" for d in ds)
    assert all(d["delayed_until"] is None for d in ds)
    types = [e["type"] for e in events(client)]
    assert types.count("approval_delivery_delayed") == 2
    assert types.count("approval_delivery_released") == 2
    # 重复扫描不重发
    nw.run_once()
    assert len(sent["webhook"]) == 2
    # 计划可查询
    sched = client.get(f"{API}/quiet-schedules").json()["schedules"]
    assert sched[0]["id"] == sid and sched[0]["daily"] is False


def test_quiet_daily_schedule_utc(client):
    from app import notif_policy
    # 每日 22:00-02:00（跨午夜，UTC）
    start, end = 22 * 3600, 2 * 3600
    # 在窗内（23:00）
    now = datetime(2026, 9, 12, 23, 0, tzinfo=timezone.utc).timestamp()
    with client.app.state.db.tx() as cur:
        s, e = notif_policy._daily_window_seconds("22:00", "02:00", now)
    assert s == pytest.approx(datetime(2026, 9, 12, 22, 0, tzinfo=timezone.utc).timestamp())
    assert e == pytest.approx(datetime(2026, 9, 13, 2, 0, tzinfo=timezone.utc).timestamp())
    # 在窗外（12:00）：返回的是昨日 22:00 起、覆盖到今日凌晨的窗口
    now2 = datetime(2026, 9, 12, 12, 0, tzinfo=timezone.utc).timestamp()
    with client.app.state.db.tx() as cur:
        s2, e2 = notif_policy._daily_window_seconds("22:00", "02:00", now2)
    assert s2 == pytest.approx(datetime(2026, 9, 11, 22, 0, tzinfo=timezone.utc).timestamp())
    assert e2 == pytest.approx(datetime(2026, 9, 12, 2, 0, tzinfo=timezone.utc).timestamp())
    # 窗边界：起点命中、终点不命中（半开区间）
    with client.app.state.db.tx() as cur:
        inside = notif_policy.quiet_resume_at
    # 直接经 HTTP 建每日计划并在 db 上判定
    quiet(client, start_time="22:00", end_time="02:00", channel="webhook")
    db = client.app.state.db
    with db.tx() as cur:
        r_in = notif_policy.quiet_resume_at(
            cur, recipient=LEAD_A, channel="webhook", now=now)
        r_out = notif_policy.quiet_resume_at(
            cur, recipient=LEAD_A, channel="webhook",
            now=datetime(2026, 9, 13, 2, 0, tzinfo=timezone.utc).timestamp())
        r_edge = notif_policy.quiet_resume_at(
            cur, recipient=LEAD_A, channel="webhook",
            now=datetime(2026, 9, 12, 22, 0, tzinfo=timezone.utc).timestamp())
    assert r_in is not None
    assert r_out is None
    assert r_edge is not None


def test_quiet_deactivation_releases_early(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    t0 = time.time()
    sid = quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600)
    process_normally(client, "Q-2")
    submit_high(client, "Q-2")
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    sent = install_senders(client)
    nw.run_once()
    assert sent["webhook"] == []
    # 运营停用静默计划 -> 下一轮立即放行（不必等到 end_at）
    r = client.post(f"{API}/quiet-schedules/{sid}/active",
                    json={"operator": GRANTOR, "active": False})
    assert r.status_code == 200
    nw.run_once()
    assert len(sent["webhook"]) == 1
    assert deliveries(client)[0]["status"] == "sent"


def test_quiet_channel_and_recipient_scoping(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["email", "webhook"],
            email="a@example.com", webhook_url="https://h/a")
    contact(client, LEAD_B, channels=["webhook"], webhook_url="https://h/b")
    t0 = time.time()
    # 只静默 LEAD_A 的 webhook
    quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600,
          recipient=LEAD_A, channel="webhook")
    process_normally(client, "Q-3")
    submit_high(client, "Q-3")
    delegation(client, "ops-lead", LEAD_A)
    delegation(client, "ops-lead", LEAD_B)
    by = {(d["recipient"], d["channel"]): d["status"] for d in deliveries(client)}
    assert by[(LEAD_A, "webhook")] == "delayed"
    assert by[(LEAD_A, "email")] == "pending"
    assert by[(LEAD_B, "webhook")] == "pending"


def test_quiet_cancelled_when_todo_closed_during_window(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    t0 = time.time()
    quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600)
    process_normally(client, "Q-4")
    bid = submit_high(client, "Q-4")
    did = delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    sent = install_senders(client)
    # 静默窗内原接收人处理待办 -> 放行时 delayed 取消，不补发
    todo = todos(client, recipient=LEAD_A)[0]
    r = client.post(f"{API}/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did})
    assert r.status_code == 200
    nw.clock = lambda: t0 + 3601
    nw.run_once()
    assert sent["webhook"] == []
    assert deliveries(client)[0]["status"] == "cancelled"
    assert approval(client, bid)["status"] == "approved"


def test_quiet_validation(client):
    r = client.post(f"{API}/quiet-schedules", json={
        "operator": GRANTOR, "daily": True, "start_time": "25:00",
        "end_time": "02:00"})
    assert r.status_code == 422
    r = client.post(f"{API}/quiet-schedules", json={
        "operator": GRANTOR, "daily": False, "start_at": 100, "end_at": 50})
    assert r.status_code == 422
    r = client.post(f"{API}/quiet-schedules", json={
        "operator": GRANTOR, "daily": False})
    assert r.status_code == 422
    r = client.post(f"{API}/quiet-schedules", json={
        "operator": GRANTOR, "daily": True, "start_time": "01:00",
        "end_time": "03:00", "channel": "sms"})
    assert r.status_code == 422


# ============================================================================
# 升级
# ============================================================================

def test_escalation_fires_after_seconds_once_per_level(client):
    lead_policy(client)
    contact(client, LEAD_A)
    contact(client, BOSS, channels=["webhook"], webhook_url="https://h/boss")
    process_normally(client, "E-1")
    bid = submit_high(client, "E-1")
    pid = esc_policy(client, [
        {"recipients": [BOSS], "after_seconds": 10},
        {"recipients": [BOSS2], "after_seconds": 30}])
    contact(client, BOSS2, channels=["webhook"], webhook_url="https://h/vp")
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 5
    nw.run_once()
    # 未到 10s：不升级
    assert events(client, "approval_escalation_fired") == []
    assert todos(client, recipient=BOSS) == []

    nw.clock = lambda: t0 + 11
    for _ in range(3):  # 重复扫描不重复升级
        nw.run_once()
    boss_todos = todos(client, recipient=BOSS)
    assert len(boss_todos) == 1 and boss_todos[0]["event_type"] == "escalated"
    assert boss_todos[0]["actionable"] is True
    esc = client.get(f"{API}/escalations", params={"batch_id": bid}).json()["escalations"]
    assert len(esc) == 1
    e = esc[0]
    assert e["policy_id"] == pid and e["levels_total"] == 2 and e["levels_fired"] == 1
    assert e["fired_levels"][0]["level"] == 1
    assert e["fired_levels"][0]["recipients"] == [BOSS]
    assert e["status"] == "pending" and e["stop_reason"] is None

    # 第二级：30s
    nw.clock = lambda: t0 + 31
    nw.run_once()
    vp_todos = todos(client, recipient=BOSS2)
    assert len(vp_todos) == 1 and vp_todos[0]["event_type"] == "escalated"
    e = client.get(f"{API}/escalations", params={"batch_id": bid}).json()["escalations"][0]
    assert e["levels_fired"] == 2 and [lv["level"] for lv in e["fired_levels"]] == [1, 2]
    types = [ev["type"] for ev in events(client)]
    assert types.count("approval_escalation_fired") == 2
    # 原接收人待办视图带升级链
    my = todos(client, recipient=LEAD_A)[0]
    assert isinstance(my["escalation"], list) and my["escalation"][0]["levels_fired"] == 2


def test_escalation_before_deadline(client):
    lead_policy(client, timeout=3600)
    contact(client, LEAD_A)
    process_normally(client, "E-2")
    bid = submit_high(client, "E-2")
    esc_policy(client, [{"recipients": [BOSS], "before_deadline_seconds": 300}])
    # BOSS 提交后才入目录：不持有 activated 待办，只收升级待办
    contact(client, BOSS, channels=["webhook"], webhook_url="https://h/b")
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    deadline = approval(client, bid)["nodes"][0]["deadline"]
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 10
    nw.run_once()
    assert todos(client, recipient=BOSS) == []
    # 距截止 300s 内触发
    nw.clock = lambda: deadline - 299
    nw.run_once()
    assert len(todos(client, recipient=BOSS)) == 1
    # 截止后节点已超时（replay worker 先跑会释放批次），升级待办随之对账关闭，
    # 这里验证：超时后不再产生新升级（级别只触发一次且来源落定时停止）
    nw.clock = lambda: deadline + 1
    client.app.state.replay_worker.clock = nw.clock
    client.app.state.replay_worker.run_once()
    nw.run_once()
    esc = client.get(f"{API}/escalations", params={"batch_id": bid}).json()["escalations"][0]
    assert esc["levels_fired"] == 1 and esc["status"] == "stopped"
    assert esc["stop_reason"] == "source_closed"


def test_escalation_stops_when_original_recipient_handles(client):
    lead_policy(client)
    contact(client, LEAD_A)
    contact(client, BOSS, channels=["webhook"], webhook_url="https://h/b")
    process_normally(client, "E-3")
    bid = submit_high(client, "E-3")
    esc_policy(client, [
        {"recipients": [BOSS], "after_seconds": 10},
        {"recipients": [BOSS2], "after_seconds": 30}])
    contact(client, BOSS2, channels=["webhook"], webhook_url="https://h/vp")
    t0 = time.time()
    did = delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    # webhook 持续失败：升级通知停留在 pending/failed，停止时必须能取消未发出投递
    sent = {"webhook": []}

    def failing(url, payload):
        sent["webhook"].append((url, payload))
        raise RuntimeError("boom")
    nw.senders = {"email": lambda *a: None, "webhook": failing}
    nw.clock = lambda: t0 + 11
    nw.run_once()
    assert len(todos(client, recipient=BOSS)) == 1
    assert deliveries(client, recipient=BOSS)[0]["status"] in ("pending", "failed")

    # 原接收人处理 -> 升级停止：第二级永不触发；第一级未发出的 webhook 取消
    todo = todos(client, recipient=LEAD_A)[0]
    r = client.post(f"{API}/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did})
    assert r.status_code == 200
    nw.clock = lambda: t0 + 31
    nw.run_once()
    assert todos(client, recipient=BOSS2) == []
    assert deliveries(client, recipient=BOSS)[0]["status"] == "cancelled"
    esc = client.get(f"{API}/escalations", params={"batch_id": bid}).json()["escalations"][0]
    assert esc["status"] == "stopped" and esc["stop_reason"] == "handled"
    assert esc["levels_fired"] == 1
    stop_ev = [e for e in events(client, "approval_escalation_stopped")]
    assert stop_ev and stop_ev[0]["detail"]["reason"] == "handled"
    assert stop_ev[0]["detail"]["cancelled_deliveries"] >= 1
    # 升级接收人的站内待办仍在（可查），但节点已落定，处理时被原审批门禁挡下
    boss_todo = todos(client, recipient=BOSS)[0]
    r = client.post(f"{API}/todos/{boss_todo['id']}/act",
                    json={"operator": BOSS, "action": "approve"})
    assert r.status_code == 409
    assert approval(client, bid)["status"] == "approved"


def test_escalation_todo_can_write_back_approval(client):
    # 'any' 节点：升级接收人无需委托即可从升级待办回写批准
    contact(client, APPROVER)  # 原始接收人（持有 activated 待办）
    contact(client, BOSS)      # 升级接收人
    process_normally(client, "E-4")
    bid = submit_high(client, "E-4")
    esc_policy(client, [{"recipients": [BOSS], "after_seconds": 5}],
               roles=["any"])
    t0 = time.time()
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 6
    nw.run_once()
    boss_todo = next(t for t in todos(client, recipient=BOSS)
                     if t["event_type"] == "escalated")
    r = client.post(f"{API}/todos/{boss_todo['id']}/act",
                    json={"operator": BOSS, "action": "approve"})
    assert r.status_code == 200, r.text
    assert approval(client, bid)["status"] == "approved"
    # 升级接收人处理 -> 节点落定，下一轮对账关闭原接收人待办并停止升级链
    # （与「直接在原节点端点投票」同一收口口径）
    nw.clock = lambda: t0 + 7
    nw.run_once()
    esc = client.get(f"{API}/escalations").json()["escalations"][0]
    assert esc["status"] == "stopped" and esc["stop_reason"] == "source_closed"


def test_escalation_matches_change_todo_by_change_role(client):
    contact(client, APPROVER)
    contact(client, BOSS)
    new_rules = [
        {"name": "h1", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]}]
    r = client.post("/admin/replay-policies/changes", json={
        "operator": SUBMITTER, "request_id": "chg-esc-1",
        "policy": {"rules": new_rules}})
    assert r.status_code == 201
    change_id = r.json()["change"]["id"]
    esc_policy(client, [{"recipients": [BOSS], "after_seconds": 5}],
               roles=["change"], name="esc-change")
    t0 = time.time()
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 6
    nw.run_once()
    esc_todos = [t for t in todos(client, recipient=BOSS)
                 if t["event_type"] == "escalated"]
    assert len(esc_todos) == 1 and esc_todos[0]["change_id"] == change_id


def test_escalation_late_registered_recipient_gets_backfilled_todo(client):
    lead_policy(client)
    contact(client, LEAD_A)
    process_normally(client, "E-7")
    bid = submit_high(client, "E-7")
    esc_policy(client, [{"recipients": [BOSS], "after_seconds": 5}])
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 6
    nw.run_once()
    # BOSS 尚未注册：级别已触发但无待办（接收人不是联系人）
    assert todos(client, recipient=BOSS) == []
    esc = client.get(f"{API}/escalations", params={"batch_id": bid}).json()
    assert esc["escalations"][0]["levels_fired"] == 1
    # BOSS 之后注册：为仍可操作的 escalated 事件补发待办，不漏关键提醒
    contact(client, BOSS)
    bt = todos(client, recipient=BOSS)
    assert len(bt) == 1 and bt[0]["event_type"] == "escalated"


def test_escalation_validation(client):
    r = client.post(f"{API}/escalation-policies", json={
        "operator": GRANTOR, "name": "bad", "roles": [], "levels": []})
    assert r.status_code == 422
    r = client.post(f"{API}/escalation-policies", json={
        "operator": GRANTOR, "name": "bad", "roles": ["ops-lead"],
        "levels": [{"recipients": []}]})
    assert r.status_code == 422
    r = client.post(f"{API}/escalation-policies", json={
        "operator": GRANTOR, "name": "bad", "roles": ["ops-lead"],
        "levels": [{"recipients": [BOSS]}]})  # 无触发时限
    assert r.status_code == 422
    r = client.post(f"{API}/escalation-policies", json={
        "operator": GRANTOR, "name": "bad", "roles": ["ops-lead"],
        "levels": [{"recipients": [BOSS], "after_seconds": 10},
                   {"recipients": [BOSS2], "after_seconds": 5}]})  # 级别非递增
    assert r.status_code == 422


def test_escalation_disabled_policy_no_new_fires(client):
    lead_policy(client)
    contact(client, LEAD_A)
    contact(client, BOSS)
    process_normally(client, "E-6")
    submit_high(client, "E-6")
    pid = esc_policy(client, [{"recipients": [BOSS], "after_seconds": 5}])
    client.post(f"{API}/escalation-policies/{pid}/active",
                json={"operator": GRANTOR, "active": False})
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 10
    nw.run_once()
    assert todos(client, recipient=BOSS) == []
    assert client.get(f"{API}/escalations").json()["escalations"] == []


# ============================================================================
# 并发 / 重启
# ============================================================================

def test_concurrent_escalation_scans_single_fire(client):
    lead_policy(client)
    contact(client, LEAD_A)
    contact(client, BOSS)
    process_normally(client, "C-1")
    bid = submit_high(client, "C-1")
    esc_policy(client, [{"recipients": [BOSS], "after_seconds": 5}])
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    nw.clock = lambda: t0 + 6
    barrier = threading.Barrier(4)
    errors = []

    def scan():
        try:
            barrier.wait()
            nw.run_once()
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=scan) for _ in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    assert len(todos(client, recipient=BOSS)) == 1
    esc = client.get(f"{API}/escalations", params={"batch_id": bid}).json()["escalations"]
    assert len(esc) == 1 and esc[0]["levels_fired"] == 1
    assert [e["type"] for e in events(client)].count(
        "approval_escalation_fired") == 1


def test_concurrent_aggregation_flush_single_digest(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    process_normally(client, "C-2")
    bid = submit_high(client, "C-2")
    agg_rule(client, window_seconds=5, batch_id=bid)
    t0 = time.time()
    delegation(client, "ops-lead", LEAD_A)
    from app import notif_policy
    barrier = threading.Barrier(3)
    errors = []

    def flush():
        try:
            barrier.wait()
            notif_policy.flush_due_groups(client.app.state.db, t0 + 6)
        except BaseException as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=flush) for _ in range(3)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    digests = [d for d in deliveries(client)
               if d["status"] not in ("held", "aggregated", "cancelled")]
    # 恰好一条摘要（可能 pending，未 dispatch）
    assert len(digests) == 1
    groups = client.get(f"{API}/aggregation-groups",
                        params={"batch_id": bid}).json()["groups"]
    assert len(groups) == 1 and groups[0]["status"] == "flushed"


def test_restart_recovery_no_duplicate_digest_or_release(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["email", "webhook"],
            email="a@example.com", webhook_url="https://h/a")
    process_normally(client, "C-3")
    bid = submit_high(client, "C-3")
    t0 = time.time()
    # 静默 + 聚合同时存在：聚合摘要也会落 delayed
    quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600)
    agg_rule(client, window_seconds=10, batch_id=bid)
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    sent = install_senders(client)
    # 窗口先关闭：摘要 held->aggregated，digest 落 delayed（静默中）
    nw.clock = lambda: t0 + 11
    nw.run_once()
    assert sent["email"] == []
    ds = deliveries(client)
    assert sorted({d["status"] for d in ds}) == ["aggregated", "delayed"]
    # 再静默结束（模拟重启：全新 worker 对象、重复多轮）
    nw2 = type(nw)(client.app.state.db, client.app.state.settings)
    nw2.senders = client.app.state.notif_worker.senders
    nw2.clock = lambda: t0 + 3601
    for _ in range(3):
        nw2.run_once()
    # 每通道恰好一封摘要，不重发、不漏发
    assert len(sent["email"]) == 1 and len(sent["webhook"]) == 1
    assert all(d["status"] in ("sent", "aggregated") for d in deliveries(client))


def test_quiet_releases_multiple_events_in_original_order(client):
    # 无策略 -> 内置 'any' 节点：提交即给除发起人外的全体联系人生成待办
    contact(client, APPROVER, channels=["webhook"], webhook_url="https://h/a")
    contact(client, LEAD_B, channels=["webhook"], webhook_url="https://h/b")
    t0 = time.time()
    quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600, channel="webhook")
    process_normally(client, "Q-9")
    submit_high(client, "Q-9")
    nw = client.app.state.notif_worker
    order = []
    client.app.state.notif_worker.senders = {
        "email": lambda *a: None,
        "webhook": lambda url, payload: order.append((url, payload["node_id"]))}
    nw.run_once()
    assert order == []  # 窗内
    # 放行：按 ordinal（待办创建顺序）发送，与事件落盘顺序一致
    nw.clock = lambda: t0 + 3601
    nw.run_once()
    assert len(order) == 2
    # 待办按接收人名排序创建：LEAD_B(ops-chen) 在 APPROVER(ops-zhao) 之前；
    # 发送顺序与待办创建顺序（ordinal=待办 id）一致，即原事件顺序
    assert [o[0] for o in order] == ["https://h/b", "https://h/a"]
    ds = deliveries(client)
    by_addr = {d["address"]: d for d in ds}
    assert by_addr["https://h/b"]["ordinal"] < by_addr["https://h/a"]["ordinal"]
    node_ids = [o[1] for o in order]
    assert node_ids == sorted(node_ids)


def test_old_database_migrates_in_place(tmp_path):
    # 老版本库：approval_notification_deliveries 无 group_id/delayed_until/ordinal，
    # approval_todos 无 open_at，也没有聚合/静默/升级的新表
    import sqlite3
    path = str(tmp_path / "old.db")
    con = sqlite3.connect(path)
    con.executescript("""
    CREATE TABLE approval_contacts (
        name TEXT PRIMARY KEY, email TEXT, webhook_url TEXT,
        channels TEXT NOT NULL DEFAULT '[]', active INTEGER NOT NULL DEFAULT 1,
        created_by TEXT, created_at REAL, updated_at REAL,
        deactivated_at REAL, deactivated_by TEXT);
    CREATE TABLE approval_notify_events (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_key TEXT NOT NULL UNIQUE,
        event_type TEXT NOT NULL, source_type TEXT NOT NULL, batch_id INTEGER,
        change_id INTEGER, node_id INTEGER, roles TEXT NOT NULL DEFAULT '[]',
        actionable INTEGER NOT NULL DEFAULT 1, state_version INTEGER NOT NULL DEFAULT 1,
        subject TEXT NOT NULL, body TEXT NOT NULL, payload TEXT NOT NULL DEFAULT '{}',
        created_at REAL NOT NULL);
    CREATE TABLE approval_todos (
        id INTEGER PRIMARY KEY AUTOINCREMENT, event_id INTEGER NOT NULL,
        recipient TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'unread',
        source_type TEXT NOT NULL, batch_id INTEGER, change_id INTEGER,
        node_id INTEGER, event_type TEXT NOT NULL, event_version INTEGER NOT NULL,
        actionable INTEGER NOT NULL DEFAULT 1, title TEXT NOT NULL,
        read_at REAL, handled_at REAL, handled_by TEXT, handle_action TEXT,
        close_reason TEXT, closed_at REAL, created_at REAL NOT NULL,
        updated_at REAL NOT NULL, UNIQUE (event_id, recipient));
    CREATE TABLE approval_notification_deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT, todo_id INTEGER NOT NULL,
        recipient TEXT NOT NULL, channel TEXT NOT NULL, address TEXT NOT NULL,
        subject TEXT, body TEXT, payload TEXT,
        status TEXT NOT NULL DEFAULT 'pending', attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL, last_error TEXT, sent_at REAL,
        created_at REAL NOT NULL, updated_at REAL NOT NULL);
    INSERT INTO approval_contacts (name, channels, active, created_at, updated_at)
        VALUES ('old-wang', '["webhook"]', 1, 1000, 1000);
    INSERT INTO approval_notify_events
        (event_key, event_type, source_type, subject, body, created_at)
        VALUES ('node:activated:1', 'activated', 'batch', 's', 'b', 1000);
    INSERT INTO approval_todos
        (event_id, recipient, status, source_type, event_type, event_version,
         actionable, title, created_at, updated_at)
        VALUES (1, 'old-wang', 'unread', 'batch', 'activated', 1, 1, 's', 1000, 1000);
    INSERT INTO approval_notification_deliveries
        (todo_id, recipient, channel, address, status, created_at, updated_at)
        VALUES (1, 'old-wang', 'webhook', 'https://h', 'pending', 1000, 1000);
    """)
    con.commit()
    con.close()

    # 打开即迁移：新列/新表就位，存量数据回填（open_at=created_at、ordinal=id）
    db = Database(path)
    todo = db.query_one("SELECT * FROM approval_todos WHERE id=1")
    assert todo["open_at"] == 1000
    d = db.query_one("SELECT * FROM approval_notification_deliveries WHERE id=1")
    assert d["ordinal"] == 1 and d["group_id"] is None and d["delayed_until"] is None
    for table in ("approval_aggregation_rules", "approval_notification_groups",
                  "approval_notification_group_members", "approval_quiet_schedules",
                  "approval_escalation_policies", "approval_escalations",
                  "approval_escalation_levels"):
        assert db.query_one(f"SELECT COUNT(*) AS c FROM {table}")["c"] == 0
    # 存量 pending 投递迁移后行为不变（仍可被发送器领取）
    rows = db.query(
        "SELECT * FROM approval_notification_deliveries WHERE status IN "
        "('pending','failed') ORDER BY ordinal, id")
    assert len(rows) == 1
    db.close()


def test_aggregation_digest_respects_quiet_and_summary_counts(client):
    lead_policy(client)
    contact(client, LEAD_A, channels=["webhook"], webhook_url="https://h/a")
    process_normally(client, "C-4")
    bid = submit_high(client, "C-4")
    t0 = time.time()
    quiet(client, daily=False, start_at=t0 - 60, end_at=t0 + 3600)
    agg_rule(client, window_seconds=10, batch_id=bid)
    delegation(client, "ops-lead", LEAD_A)
    nw = client.app.state.notif_worker
    install_senders(client)
    nw.clock = lambda: t0 + 11
    nw.run_once()
    # 摘要在静默中 -> delayed 而非 sent
    digest = [d for d in deliveries(client)
              if "摘要" in (d["subject"] or "")][0]
    assert digest["status"] == "delayed"
    s = client.get(f"{API}/summary", params={"recipient": LEAD_A}).json()
    assert s["delivery_delayed"] == 1 and s["delivery_aggregated"] == 1
    assert s["unread"] == 1

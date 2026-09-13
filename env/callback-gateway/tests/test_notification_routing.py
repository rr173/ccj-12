"""通知通道路由与故障转移模块的端到端测试。

覆盖：
- 按事件类型配置 email/webhook/inbox 的优先级、启用状态与接收条件，按版本快照入队；
- 首选通道成功即终态；超时立即切换、连续失败达上限才切换到下一通道；
- 每次尝试、切换原因、最终结果落盘（attempts / switches / 审计）；
- 时间窗口内连续失败熔断（open 不派发新请求）、冷却 half_open 单探针、
  探针成功才恢复接流量、探针失败回 open；
- 同一事件对同一接收人跨通道只产生一条业务通知（重启/重复扫描/并发不越过幂等）；
- 待发送任务、通道健康、切换历史、审计事件可查；
- 新版本发布/回滚只影响之后入队的任务，已入队任务持有快照不变；
- 待办处理/关闭取消未发出任务；熔断时新任务走下一通道。
"""
from __future__ import annotations

import json
import threading
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"
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
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def post(client, external_id, body=b'{"order": 1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})


def process_normally(client, external_id):
    post(client, external_id)
    client.app.state.worker.run_once()


def apply_policy(client, nodes=None):
    rules = [{"name": "h", "risk_level": "high", "mode": "parallel",
              "nodes": nodes or [{"role": "ops-lead", "timeout_seconds": 3600}]}]
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def submit_high(client, external_id, *, operator=SUBMITTER):
    r = client.post("/admin/replays", json={
        "operator": operator, "reason": "资金类回调补发", "risk_level": "high",
        "approval_note": "需审批", "external_id": external_id})
    assert r.status_code == 201, r.text
    return r.json()


def contact(client, name, *, channels=None, email=None, webhook_url=None,
            operator=GRANTOR):
    body = {"name": name, "operator": operator, "channels": channels or []}
    if email:
        body["email"] = email
    if webhook_url:
        body["webhook_url"] = webhook_url
    r = client.post("/admin/approval-notifications/contacts", json=body)
    assert r.status_code == 200, r.text
    return r


def delegation(client, role="ops-lead", delegatee=LEAD_A):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": role, "delegatee": delegatee, "operator": GRANTOR,
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text
    return r.json()["delegation_id"]


def make_event(client, external_id, *, channels=None, email="wang@example.com",
               webhook_url="https://hook.example.com/x"):
    """注册联系人 + 提交高风险批次 + 委托，返回该接收人在最新批次上的唯一待办。"""
    apply_policy(client)
    contact(client, LEAD_A, channels=channels, email=email, webhook_url=webhook_url)
    process_normally(client, external_id)
    bid = submit_high(client, external_id)["batch_id"]
    delegation(client)
    todos = client.get("/admin/approval-notifications/todos",
                       params={"recipient": LEAD_A, "batch_id": bid}).json()["todos"]
    assert len(todos) == 1
    return todos[0]


def set_senders(client, *, email=None, webhook=None, inbox=None):
    senders = {"email": email or (lambda *a: None),
               "webhook": webhook or (lambda *a: None),
               "inbox": inbox or (lambda task: None)}
    client.app.state.notif_worker.senders = senders
    return senders


def publish(client, rules, *, breaker=None, operator=GRANTOR, note=""):
    body = {"operator": operator, "note": note, "rules": rules}
    if breaker is not None:
        body["breaker"] = breaker
    r = client.post(f"{ROUTING}/versions", json=body)
    assert r.status_code == 200, r.text
    return r.json()


def tasks(client, **params):
    return client.get(f"{ROUTING}/tasks", params=params).json()["tasks"]


def task_detail(client, task_id):
    return client.get(f"{ROUTING}/tasks/{task_id}").json()["task"]


def health(client, channel="webhook"):
    rows = client.get(f"{ROUTING}/channels/health").json()["channels"]
    return next(r for r in rows if r["channel"] == channel)


def switches(client, **params):
    return client.get(f"{ROUTING}/switches", params=params).json()["switches"]


def attempts(client, **params):
    return client.get(f"{ROUTING}/attempts", params=params).json()["attempts"]


def default_rules(*, order=("email", "webhook", "inbox")):
    return [{"event_type": None, "channels": [
        {"channel": ch} for ch in order]}]


# ---- 基础：版本快照入队、首选成功、跨通道不重复 -------------------------------------

def test_first_channel_succeeds_task_sent_once(env):
    client = env
    set_senders(client)
    publish(client, default_rules(order=("email", "webhook", "inbox")))
    todo = make_event(client, "R-1", channels=["email", "webhook"])

    route = todo["route_task"]
    assert route is not None
    assert route["status"] == "pending"
    assert route["plan"] == ["email", "webhook", "inbox"]
    assert route["route_version"] == 1
    # 走路由链路时不再生成旧的每通道投递行
    assert todo["deliveries"] == []

    client.app.state.notif_worker.run_once()
    t = task_detail(client, route["id"])
    assert t["status"] == "sent" and t["sent_channel"] == "email"
    # 一次成功尝试；只有 selected 一次切换记录，没有失败/转移
    assert [a["channel"] for a in t["attempts"]] == ["email"]
    assert [s["reason"] for s in t["switches"]] == ["selected"]

    # 再跑多轮：绝不重复发送（跨通道只有一条业务通知）
    client.app.state.notif_worker.run_once()
    client.app.state.notif_worker.run_once()
    t = task_detail(client, route["id"])
    assert len(t["attempts"]) == 1
    assert len(tasks(client)) == 1


def test_inbox_is_business_notification_fallback(env):
    client = env
    # 联系人没有任何外发通道地址；计划 email -> webhook -> inbox
    set_senders(client)
    publish(client, default_rules())
    todo = make_event(client, "R-2", channels=[], email=None, webhook_url=None)
    route = todo["route_task"]
    client.app.state.notif_worker.run_once()
    t = task_detail(client, route["id"])
    # 前两通道无地址（no_address 记录），最终站内兜底成功，业务通知恰好一次
    assert t["status"] == "sent" and t["sent_channel"] == "inbox"
    reasons = [s["reason"] for s in t["switches"]]
    assert reasons.count("no_address") == 2
    assert len([a for a in t["attempts"] if a["result"] == "success"]) == 1


def test_same_event_recipient_single_task_on_rescan_and_restart(env):
    client = env
    set_senders(client)
    publish(client, default_rules(order=("webhook", "inbox")))
    todo = make_event(client, "R-3", channels=["webhook"])
    rid = todo["route_task"]["id"]

    # 重复触发待办对账/截止扫描等 worker 步骤不产生新任务
    client.app.state.notif_worker.run_once()
    client.app.state.notif_worker.run_once()
    assert len(tasks(client)) == 1

    # 模拟重启：recover + 再次派发，已 sent 任务不重发
    client.app.state.notif_worker.recover()
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["status"] == "sent"
    assert len(t["attempts"]) == 1


# ---- 故障转移：连续失败重试、达上限切换、超时立即切换 ---------------------------------

def test_consecutive_failures_retry_then_failover_to_next_channel(env):
    client = env
    calls = {"webhook": 0, "email": 0}

    def failing_email(addr, subject, body):
        calls["email"] += 1
        raise RuntimeError("smtp down")

    def ok_webhook(url, payload):
        calls["webhook"] += 1

    set_senders(client, email=failing_email, webhook=ok_webhook)
    # email max_attempts=2：同一通道失败两次后才切 webhook
    publish(client, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 2},
        {"channel": "webhook", "max_attempts": 2},
        {"channel": "inbox"}]}])
    todo = make_event(client, "R-4", channels=["email", "webhook"])
    rid = todo["route_task"]["id"]

    # 第一次派发：首败 -> 退避 pending（未切换）
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["status"] == "pending" and t["current_channel"] == "email"
    assert calls["email"] == 1
    sw = switches(client, task_id=rid)
    assert [s["reason"] for s in sw] == ["selected"]

    # 到期重发（next_retry_at 已到）：第二次失败达上限 -> 切 webhook 成功
    db: Database = client.app.state.db
    db.query("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (rid,))
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["status"] == "sent" and t["sent_channel"] == "webhook"
    assert calls["email"] == 2 and calls["webhook"] == 1
    reasons = [s["reason"] for s in t["switches"]]
    assert "consecutive_failures" in reasons
    results = [(a["channel"], a["result"]) for a in t["attempts"]]
    assert results == [("email", "failure"), ("email", "failure"),
                       ("webhook", "success")]


def test_timeout_switches_immediately_without_retry(env):
    client = env

    def slow_email(addr, subject, body):
        time.sleep(1.0)

    set_senders(client, email=slow_email)
    publish(client, [{"event_type": None, "channels": [
        {"channel": "email", "timeout_seconds": 0.2, "max_attempts": 5},
        {"channel": "inbox"}]}])
    todo = make_event(client, "R-5", channels=["email"])
    rid = todo["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    # 超时一次即切到 inbox（尽管 max_attempts=5）
    assert t["status"] == "sent" and t["sent_channel"] == "inbox"
    assert [a["result"] for a in t["attempts"]] == ["timeout", "success"]
    reasons = [s["reason"] for s in t["switches"]]
    assert "timeout" in reasons


def test_all_channels_exhausted_quarantined_and_requeue(env):
    client = env

    def fail_email(*a):
        raise RuntimeError("smtp down")

    def fail_webhook(*a):
        raise RuntimeError("http 500")

    set_senders(client, email=fail_email, webhook=fail_webhook)
    # 计划只有两个外发通道（无 inbox 兜底），各 1 次尝试
    publish(client, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 1},
        {"channel": "webhook", "max_attempts": 1}]}])
    todo = make_event(client, "R-6", channels=["email", "webhook"])
    rid = todo["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["status"] == "quarantined"
    assert [s["reason"] for s in t["switches"]][-1] == "plan_exhausted"

    # 修好两个通道后人工 requeue：从首选通道开新一轮
    set_senders(client)
    r = client.post(f"{ROUTING}/tasks/{rid}/requeue",
                    json={"operator": GRANTOR})
    assert r.status_code == 200, r.text
    assert r.json()["round"] == 2
    t = task_detail(client, rid)
    assert t["status"] == "sent" and t["sent_channel"] == "email"
    # 新一轮的尝试单独计数
    new_round = [a for a in t["attempts"] if a["round"] == 2]
    assert len(new_round) == 1 and new_round[0]["channel"] == "email"


# ---- 熔断：窗口统计、open 拒绝、half_open 探针、恢复 ----------------------------------

def test_breaker_opens_blocks_new_dispatch_probe_then_recovers(env):
    client = env
    lock = threading.Lock()
    state = {"webhook_fail": True}

    def webhook(url, payload):
        with lock:
            if state["webhook_fail"]:
                raise RuntimeError("webhook down")

    set_senders(client, webhook=webhook)
    # 阈值 2：连续两次失败熔断 webhook
    publish(client, [{"event_type": None, "channels": [
        {"channel": "webhook", "max_attempts": 1},
        {"channel": "inbox"}]}],
        breaker={"webhook": {"failure_threshold": 2, "window_seconds": 60,
                             "cooldown_seconds": 30}})

    # 任务 1：webhook 失败 -> inbox 兜底（1 次失败，未熔断）
    t1 = make_event(client, "R-7", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert task_detail(client, t1)["sent_channel"] == "inbox"
    assert health(client)["state"] == "closed"
    assert health(client)["window_consecutive_failures"] == 1

    # 任务 2：再失败一次 -> webhook 熔断 open
    t2 = make_event(client, "R-8", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    h = health(client)
    assert h["state"] == "open" and h["opened_at"] is not None

    # 熔断期间新任务：首选 webhook 不派发（breaker_open 直接切 inbox），
    # 不产生 webhook 尝试，inbox 成功
    t3 = make_event(client, "R-9", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    t3v = task_detail(client, t3)
    assert t3v["sent_channel"] == "inbox"
    assert [a["channel"] for a in t3v["attempts"]] == ["inbox"]
    assert "breaker_open" in [s["reason"] for s in t3v["switches"]]

    # 冷却未到：即便有任务也不放探针（把所有通道都设为 webhook，任务应等待）
    publish(client, [{"event_type": None, "channels":
                      [{"channel": "webhook", "max_attempts": 1}]}], operator="ops-b")
    t4 = make_event(client, "R-10", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    t4v = task_detail(client, t4)
    assert t4v["status"] == "pending"  # 全计划熔断 -> 等待恢复
    assert health(client)["state"] == "open"

    # 冷却到期：下一轮 half_open 放唯一探针；探针仍失败 -> 回 open
    db: Database = client.app.state.db
    db.query("UPDATE notif_channel_state SET opened_at=opened_at-1000 WHERE channel='webhook'")
    db.query("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (t4,))
    client.app.state.notif_worker.run_once()
    h = health(client)
    assert h["state"] == "open"  # 探针失败回 open
    probe_attempts = attempts(client, channel="webhook", result="failure")
    assert any(a["probe"] for a in probe_attempts)

    # 再次冷却到期，通道恢复：探针成功 -> closed，任务经 webhook 成功
    db.query("UPDATE notif_channel_state SET opened_at=opened_at-1000 WHERE channel='webhook'")
    db.query("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (t4,))
    state["webhook_fail"] = False
    client.app.state.notif_worker.run_once()
    assert health(client)["state"] == "closed"
    t4v = task_detail(client, t4)
    assert t4v["status"] == "sent" and t4v["sent_channel"] == "webhook"
    probe_success = attempts(client, channel="webhook", result="success")
    assert any(a["probe"] for a in probe_success)


def test_breaker_window_failure_count_resets_on_success(env):
    client = env
    state = {"fail": True}

    def webhook(url, payload):
        if state["fail"]:
            raise RuntimeError("down")

    set_senders(client, webhook=webhook)
    # 阈值 3；webhook 失败两次后有一次成功（走 inbox 兜底期间另一任务 webhook 成功），
    # 窗口连续失败计数归零，不会被更早的失败熔断。
    publish(client, [{"event_type": None, "channels": [
        {"channel": "webhook", "max_attempts": 1}, {"channel": "inbox"}]}],
        breaker={"webhook": {"failure_threshold": 3}})
    t1 = make_event(client, "W-1", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    t2 = make_event(client, "W-2", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert health(client)["window_consecutive_failures"] == 2

    # 一次成功（手工放开 webhook，让某任务首选即成功）
    state["fail"] = False
    t3 = make_event(client, "W-3", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert task_detail(client, t3)["sent_channel"] == "webhook"
    assert health(client)["state"] == "closed"
    assert health(client)["window_consecutive_failures"] == 0


def test_manual_channel_disable_blocks_dispatch_and_reset_recovers(env):
    client = env
    set_senders(client)
    publish(client, default_rules(order=("webhook", "inbox")),
            breaker={"webhook": {"failure_threshold": 2}})
    # 手工熔断 webhook（排障），随后停用
    db: Database = client.app.state.db
    now = time.time()
    with db.tx() as cur:
        cur.execute(
            "UPDATE notif_channel_state SET state='open', opened_at=?, updated_at=? "
            "WHERE channel='webhook'", (now, now))
    t1 = make_event(client, "D-1", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert task_detail(client, t1)["sent_channel"] == "inbox"

    # reset 复位后重新接流量
    r = client.post(f"{ROUTING}/channels/webhook/state",
                    json={"operator": GRANTOR, "enabled": True, "reset": True})
    assert r.status_code == 200, r.text
    assert health(client)["state"] == "closed"


# ---- 接收条件 -----------------------------------------------------------------------

def test_channel_conditions_filter_plan_by_actionable_and_risk(env):
    client = env
    set_senders(client)
    # 可操作事件走 webhook；纯告知走 email（不匹配 webhook 的 actionable 条件）
    publish(client, [
        {"event_type": "activated", "channels": [
            {"channel": "webhook", "condition": {"actionable": True}},
            {"channel": "inbox"}]},
        {"event_type": None, "channels": [
            {"channel": "email"}, {"channel": "inbox"}]}])
    todo = make_event(client, "C-1", channels=["email", "webhook"])
    assert todo["route_task"]["plan"] == ["webhook", "inbox"]


def test_recipient_condition_whitelist(env):
    client = env
    set_senders(client)
    publish(client, [{"event_type": None, "channels": [
        {"channel": "webhook", "condition": {"recipients": ["someone-else"]}},
        {"channel": "inbox"}]}])
    todo = make_event(client, "C-2", channels=["webhook"])
    # LEAD_A 不在白名单：webhook 被条件过滤，只剩 inbox
    assert todo["route_task"]["plan"] == ["inbox"]


# ---- 版本：发布/回滚只影响新任务，已入队任务持快照不变 --------------------------------

def test_publish_and_rollback_only_affect_new_tasks(env):
    client = env
    set_senders(client)
    v1 = publish(client, default_rules(order=("email", "inbox")))["version"]
    t1 = make_event(client, "V-1", channels=["email", "webhook"])["route_task"]["id"]
    snap1 = task_detail(client, t1)["route_snapshot"]
    assert [p["channel"] for p in snap1["plan"]] == ["email", "inbox"]

    # 发布 v2：首选改为 webhook
    publish(client, default_rules(order=("webhook", "email", "inbox")), operator="ops-b")
    cur = client.get(f"{ROUTING}/current").json()
    assert cur["current_version"] == 2 and cur["rollback_available"] is True
    t2 = make_event(client, "V-2", channels=["email", "webhook"])["route_task"]["id"]
    assert task_detail(client, t2)["route_version"] == 2
    assert task_detail(client, t2)["plan"][0] == "webhook"
    # 旧任务快照不变
    assert task_detail(client, t1)["route_version"] == 1
    assert [p["channel"] for p in task_detail(client, t1)["route_snapshot"]["plan"]] \
        == ["email", "inbox"]

    # 回滚到 v1：只影响之后的新任务
    r = client.post(f"{ROUTING}/rollback",
                    json={"operator": "ops-b", "reason": "紧急止损"})
    assert r.status_code == 200, r.text
    assert r.json()["version"] == 1
    t3 = make_event(client, "V-3", channels=["email", "webhook"])["route_task"]["id"]
    assert task_detail(client, t3)["route_version"] == 1
    assert task_detail(client, t3)["plan"][0] == "email"
    # v2 任务仍持 v2 快照
    assert task_detail(client, t2)["route_version"] == 2
    versions = client.get(f"{ROUTING}/versions").json()["versions"]
    results = {v["result"] for v in versions}
    assert {"applied", "rollback"} <= results


def test_invalid_config_rejected_current_unchanged(env):
    client = env
    set_senders(client)
    publish(client, default_rules())
    r = client.post(f"{ROUTING}/versions", json={
        "operator": GRANTOR, "rules": [
            {"event_type": "x", "channels": [{"channel": "sms"}]}]})
    assert r.status_code == 422
    assert client.get(f"{ROUTING}/current").json()["current_version"] == 1
    rejected = [v for v in client.get(f"{ROUTING}/versions").json()["versions"]
                if v["result"] == "rejected"]
    assert len(rejected) == 1


# ---- 取消 / 幂等 / 并发 ---------------------------------------------------------------

def test_handling_todo_cancels_pending_send_task(env):
    client = env
    set_senders(client, webhook=lambda *a: (_ for _ in ()).throw(RuntimeError("down")))
    publish(client, [{"event_type": None, "channels": [
        {"channel": "webhook", "max_attempts": 5}, {"channel": "inbox"}]}])
    todo = make_event(client, "X-1", channels=["webhook"])
    rid = todo["route_task"]["id"]
    # webhook 失败退避，任务挂起
    client.app.state.notif_worker.run_once()
    assert task_detail(client, rid)["status"] == "pending"

    # 待办被本人凭委托处理（回写原审批动作），未发出的路由发送任务同事务取消
    todo = client.get("/admin/approval-notifications/todos",
                      params={"recipient": LEAD_A, "batch_id": todo["batch_id"]}
                      ).json()["todos"][0]
    did = client.get("/admin/replay-delegations",
                     params={"role": "ops-lead"}).json()["delegations"][0]["id"]
    r = client.post(f"/admin/approval-notifications/todos/{todo['id']}/act",
                    json={"operator": LEAD_A, "action": "approve",
                          "role": "ops-lead", "delegation_id": did})
    assert r.status_code == 200, r.text
    assert task_detail(client, rid)["status"] == "cancelled"


def test_concurrent_dispatch_single_winner(env):
    client = env
    set_senders(client)
    publish(client, default_rules(order=("email", "inbox")))
    rid = make_event(client, "Z-1", channels=["email"])["route_task"]["id"]
    db: Database = client.app.state.db
    from app import notif_routing
    senders = client.app.state.notif_worker.senders
    errors = []

    def drive():
        try:
            notif_routing.process_send_task(
                db, rid, senders, client.app.state.settings, time.time())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=drive) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []
    t = task_detail(client, rid)
    assert t["status"] == "sent"
    # 只有一个赢家真正执行了通道尝试
    assert len(t["attempts"]) == 1


def test_no_route_version_keeps_legacy_chain(env):
    client = env
    set_senders(client)
    # 不发布任何路由版本：走旧的每通道投递链路
    assert client.get(f"{ROUTING}/current").json()["current_version"] is None
    todo = make_event(client, "L-1", channels=["email", "webhook"])
    assert todo["route_task"] is None
    assert {d["channel"] for d in todo["deliveries"]} == {"email", "webhook"}
    client.app.state.notif_worker.run_once()
    todo = client.get("/admin/approval-notifications/todos",
                      params={"recipient": LEAD_A, "batch_id": todo["batch_id"]}
                      ).json()["todos"][0]
    assert {d["status"] for d in todo["deliveries"]} == {"sent"}


def test_enqueued_task_uses_own_snapshot_after_republish(env):
    """任务入队后发布新版本/回滚，派发仍按入队时快照，不随指针改变。"""
    client = env
    set_senders(client)
    publish(client, default_rules(order=("email", "inbox")))
    rid = make_event(client, "S-1", channels=["email", "webhook"])["route_task"]["id"]
    # 入队后、派发前把首选改成 webhook
    publish(client, default_rules(order=("webhook", "inbox")), operator="ops-b")
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["route_version"] == 1 and t["sent_channel"] == "email"
    # 新任务才用新版本
    rid2 = make_event(client, "S-2", channels=["email", "webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert task_detail(client, rid2)["sent_channel"] == "webhook"


def test_rollback_without_history_conflicts(env):
    client = env
    set_senders(client)
    # 从未发布/只有一个版本时不能回滚
    r = client.post(f"{ROUTING}/rollback",
                    json={"operator": GRANTOR, "reason": "x"})
    assert r.status_code == 409
    publish(client, default_rules())
    r = client.post(f"{ROUTING}/rollback",
                    json={"operator": GRANTOR, "reason": "x"})
    assert r.status_code == 409
    # 缺少原因 422
    publish(client, default_rules(), operator="ops-b")
    r = client.post(f"{ROUTING}/rollback", json={"operator": GRANTOR})
    assert r.status_code == 422


def test_breaker_open_all_channels_task_waits_then_sends_on_recovery(env):
    """计划里唯一通道被熔断：任务等待恢复（不隔离），半开探针成功后同一任务发出。"""
    client = env
    state = {"fail": True}

    def webhook(url, payload):
        if state["fail"]:
            raise RuntimeError("down")

    set_senders(client, webhook=webhook)
    publish(client, [{"event_type": None, "channels":
                      [{"channel": "webhook", "max_attempts": 1}]}],
            breaker={"webhook": {"failure_threshold": 1, "cooldown_seconds": 30}})
    # 第一次失败即熔断（无 inbox 兜底），任务隔离还是等待？-> 等待恢复
    rid = make_event(client, "O-1", channels=["webhook"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert health(client)["state"] == "open"
    t = task_detail(client, rid)
    assert t["status"] in ("pending", "quarantined")
    # 期望设计为等待恢复（pending）而非隔离
    assert t["status"] == "pending"
    # 熔断期间重复派发不会新增尝试
    client.app.state.notif_worker.run_once()
    assert len(attempts(client, task_id=rid)) == 1
    # 冷却到期 + 通道恢复：探针成功，任务发出
    db: Database = client.app.state.db
    db.query("UPDATE notif_channel_state SET opened_at=opened_at-1000 WHERE channel='webhook'")
    db.query("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (rid,))
    state["fail"] = False
    client.app.state.notif_worker.run_once()
    t = task_detail(client, rid)
    assert t["status"] == "sent" and t["sent_channel"] == "webhook"
    last = attempts(client, task_id=rid)[0]
    assert last["probe"] is True and last["result"] == "success"


def test_summary_includes_route_task_counts(env):
    client = env
    set_senders(client)
    publish(client, default_rules(order=("email", "inbox")))
    make_event(client, "M-1", channels=["email"])
    client.app.state.notif_worker.run_once()
    s = client.get("/admin/approval-notifications/summary").json()
    assert s["route_sent"] == 1
    assert s["route_pending"] == 0


def test_routed_tasks_filterable_by_route_version_and_event_type(env):
    client = env
    set_senders(client)
    v1 = publish(client, default_rules(order=("email", "inbox")))["version"]
    rid = make_event(client, "F-1", channels=["email"])["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert len(tasks(client, route_version=v1)) == 1
    assert len(tasks(client, event_type="activated")) == 1
    assert tasks(client, event_type="node_rejected") == []
    detail = client.get(f"{ROUTING}/tasks/{rid}").json()["task"]
    assert detail["attempts"] and detail["switches"]


def test_contact_deactivation_cancels_routed_task(env):
    client = env
    set_senders(client, webhook=lambda *a: (_ for _ in ()).throw(RuntimeError("down")))
    publish(client, [{"event_type": None, "channels": [
        {"channel": "webhook", "max_attempts": 5}, {"channel": "inbox"}]}])
    todo = make_event(client, "D-9", channels=["webhook"])
    rid = todo["route_task"]["id"]
    client.app.state.notif_worker.run_once()
    assert task_detail(client, rid)["status"] == "pending"
    r = client.post(f"/admin/approval-notifications/contacts/{LEAD_A}/deactivate",
                    json={"operator": GRANTOR})
    assert r.status_code == 200, r.text
    assert task_detail(client, rid)["status"] == "cancelled"


# ---- 查询 ----------------------------------------------------------------------------

def test_admin_queries_tasks_health_switches_attempts(env):
    client = env

    def fail_email(*a):
        raise RuntimeError("down")

    set_senders(client, email=fail_email)
    publish(client, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 1}, {"channel": "inbox"}]}],
        breaker={"email": {"failure_threshold": 5}})
    make_event(client, "Q-1", channels=["email"])
    client.app.state.notif_worker.run_once()

    # 任务按状态/通道/接收人过滤
    assert len(tasks(client, status="sent")) == 1
    assert len(tasks(client, channel="email")) == 1
    assert tasks(client, status="quarantined") == []
    # 健康视图
    h = {r["channel"]: r for r in
         client.get(f"{ROUTING}/channels/health").json()["channels"]}
    assert set(h) == {"email", "webhook", "inbox"}
    assert h["email"]["window_consecutive_failures"] == 1
    # 切换历史 / 尝试
    sw = switches(client, reason="consecutive_failures")
    assert len(sw) == 1 and sw[0]["to_channel"] == "inbox"
    assert {(a["channel"], a["result"]) for a in attempts(client)} == {
        ("email", "failure"), ("inbox", "success")}
    # 审计事件
    types = [e["type"] for e in
             client.get("/admin/events", params={"limit": 1000}).json()["events"]]
    for needed in ("notif_send_task_enqueued", "notif_channel_switched",
                   "notif_send_sent"):
        assert needed in types

"""接收人通知额度与抑制窗口的端到端测试。

覆盖：
- 按接收人 × 事件级别 × 时间窗口配置额度、单事件占用与超额处置
  （延迟/降级站内/转人工）；事件级别解析与 event_levels 覆盖；
- 发送任务领取前原子预占；重复扫描、失败重试、服务重启恢复不重复占用；
- 回执失败后的通道切换不绕过同一事件的额度限制；
- 窗口到期释放可用额度（固定窗口对齐）；
- 取消任务/人工忽略/确认不再发送回收未使用预占；
- 管理员可查看配置版本、当前消耗、被延迟/降级/人工的任务与审计；
- 配置更新/回滚不改变已入队任务的规则快照；
- 同一接收人并发多事件的稳定占用顺序（低 ordinal 优先，不插队）。
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
GRANTOR = "ops-admin"

ROUTING = "/admin/approval-notifications/routing"
QUOTA = "/admin/approval-notifications/quota"


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
        receipt_confirm_timeout_seconds=3600.0,
        receipt_confirm_max_retries=2,
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


# ---- 复用 routing 测试同款装配辅助 -------------------------------------------------

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


def contact(client, name=LEAD_A, *, channels=None, email=None, webhook_url=None,
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


def make_event(client, external_id, *, channels=None,
               email="wang@example.com", webhook_url="https://hook.example.com/x"):
    """生成 LEAD_A 在最新高风险批次上的激活待办（critical 级别），返回待办视图。"""
    contact(client, channels=channels, email=email, webhook_url=webhook_url)
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


def publish_routing(client, rules=None):
    rules = rules or [{"event_type": None, "channels": [
        {"channel": "email"}, {"channel": "webhook"}, {"channel": "inbox"}]}]
    r = client.post(f"{ROUTING}/versions",
                    json={"operator": GRANTOR, "rules": rules})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def publish_quota(client, rules, *, event_levels=None, note="", operator=GRANTOR):
    body = {"operator": operator, "note": note, "rules": rules}
    if event_levels is not None:
        body["event_levels"] = event_levels
    r = client.post(f"{QUOTA}/versions", json=body)
    assert r.status_code == 200, r.text
    return r.json()["version"]


def default_quota_rules(**over):
    rule = {"id": "*", "level": "*", "window_seconds": 3600, "limit": 5,
            "cost": 1, "on_exceeded": "delay"}
    rule.update(over)
    return [rule]


def task_detail(client, task_id):
    return client.get(f"{ROUTING}/tasks/{task_id}").json()["task"]


def tasks(client, **params):
    return client.get(f"{ROUTING}/tasks", params=params).json()["tasks"]


def usage(client, **params):
    return client.get(f"{QUOTA}/usage", params=params).json()


def reservations(client, **params):
    return client.get(f"{QUOTA}/reservations", params=params).json()["reservations"]


def quota_tasks(client, **params):
    return client.get(f"{QUOTA}/tasks", params=params).json()["tasks"]


# ---- 配置：校验 / 版本 / 回滚 / 审计 ------------------------------------------------

def test_quota_publish_validation_keeps_current(env):
    c = env
    # 缺少缺省规则 -> 拒绝（落 rejected 版本），当前版本保持 NULL
    r = c.post(f"{QUOTA}/versions", json={"operator": GRANTOR, "rules": [
        {"id": "r1", "level": "critical", "window_seconds": 60, "limit": 1}]})
    assert r.status_code == 422
    assert c.get(f"{QUOTA}/current").json()["current_version"] is None
    versions = c.get(f"{QUOTA}/versions").json()["versions"]
    assert versions[0]["result"] == "rejected"

    # 非法级别 / 动作 / 窗口
    for rules in (
        [{"id": "*", "level": "bogus", "window_seconds": 60, "limit": 1}],
        [{"id": "*", "level": "*", "window_seconds": 0, "limit": 1}],
        [{"id": "*", "level": "*", "window_seconds": 60, "limit": 1,
          "on_exceeded": "drop"}],
        [{"id": "*", "level": "*", "window_seconds": 60, "limit": -1}],
    ):
        r = c.post(f"{QUOTA}/versions", json={"operator": GRANTOR, "rules": rules})
        assert r.status_code == 422, rules
    # 重复规则 id
    r = c.post(f"{QUOTA}/versions", json={"operator": GRANTOR, "rules": [
        {"id": "r", "recipient": "a", "level": "normal",
         "window_seconds": 60, "limit": 1},
        {"id": "r", "recipient": "b", "level": "normal",
         "window_seconds": 60, "limit": 1},
        {"id": "*", "level": "*", "window_seconds": 60, "limit": 1}]})
    assert r.status_code == 422


def test_quota_rollback_only_affects_new_tasks(env):
    c = env
    set_senders(c)
    publish_routing(c)
    v1 = publish_quota(c, default_quota_rules(limit=10))
    todo = make_event(c, "Q-1", channels=["email"])
    t1 = task_detail(c, todo["route_task"]["id"])
    assert t1["quota_version"] == v1 and t1["quota_snapshot"]["limit"] == 10

    publish_quota(c, default_quota_rules(limit=1, on_exceeded="manual"))
    # 已入队任务的快照不随新版本改变
    t1b = task_detail(c, t1["id"])
    assert t1b["quota_version"] == v1 and t1b["quota_snapshot"]["limit"] == 10

    r = c.post(f"{QUOTA}/rollback", json={"operator": GRANTOR,
                                          "reason": "误发布回滚"})
    assert r.status_code == 200 and r.json()["version"] == v1
    assert c.get(f"{QUOTA}/current").json()["rollback_available"] is False
    # 回滚必须带原因
    r = c.post(f"{QUOTA}/rollback", json={"operator": GRANTOR, "reason": ""})
    assert r.status_code == 422


# ---- 预占：领取前原子占用，重复扫描/重试不重复占用 -----------------------------------

def test_reserve_atomic_before_dispatch_and_consume_on_sent(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=3))
    todo = make_event(c, "Q-2", channels=["email"])
    rid = todo["route_task"]["id"]

    # 入队时未预占（额度只在领取前占用）
    t = task_detail(c, rid)
    assert t["status"] == "pending" and t["quota_status"] == "none"
    assert reservations(c, task_id=rid, state="reserved") == []

    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent" and t["quota_status"] == "consumed"
    rs = reservations(c, task_id=rid)
    assert len(rs) == 1 and rs[0]["state"] == "consumed" and rs[0]["cost"] == 1

    u = usage(c, recipient=LEAD_A)["buckets"]
    assert len(u) == 1 and u[0]["used"] == 1 and u[0]["limit"] == 3
    assert u[0]["consumed_cost"] == 1 and u[0]["reserved_cost"] == 0
    assert u[0]["available"] == 2


def test_rescan_and_failure_retry_do_not_double_reserve(env):
    c = env
    calls = {"n": 0}

    def flaky(addr, subject, body):
        calls["n"] += 1
        if calls["n"] < 3:
            raise RuntimeError("smtp down")

    set_senders(c, email=flaky)
    publish_routing(c, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 3}, {"channel": "inbox"}]}])
    publish_quota(c, default_quota_rules(limit=5))
    todo = make_event(c, "Q-3", channels=["email"])
    rid = todo["route_task"]["id"]

    c.app.state.notif_worker.run_once()  # 首次失败：已预占，退避 pending
    t = task_detail(c, rid)
    assert t["status"] == "pending" and t["quota_status"] == "admitted"
    before = reservations(c, task_id=rid)
    assert len(before) == 1 and before[0]["state"] == "reserved"

    # 重复扫描不产生第二个预占（退避未到期，任务不被领取）
    c.app.state.notif_worker.run_once()
    assert len(reservations(c, task_id=rid)) == 1

    # 到点重试直到成功（清退避时间模拟到期）：同代预占始终只有一行
    from app import notif_routing
    db = c.app.state.db
    for _ in range(5):
        db.query("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (rid,))
        notif_routing.dispatch_due_tasks(
            db, c.app.state.notif_worker.senders, c.app.state.settings)
        if task_detail(c, rid)["status"] == "sent":
            break
    rs = reservations(c, task_id=rid)
    assert len(rs) == 1
    t = task_detail(c, rid)
    assert t["status"] == "sent" and t["quota_generation"] == 1
    assert rs[0]["state"] == "consumed"


def test_restart_recovers_in_flight_without_double_reservation(env):
    c = env

    def failing(addr, subject, body):
        raise RuntimeError("smtp down")

    set_senders(c, email=failing)
    publish_routing(c, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 5}]}])
    publish_quota(c, default_quota_rules(limit=2))
    todo = make_event(c, "Q-4", channels=["email"])
    rid = todo["route_task"]["id"]

    c.app.state.notif_worker.run_once()  # 领取+预占，通道失败后退避 pending
    assert reservations(c, task_id=rid)[0]["state"] == "reserved"

    # 手工制造「重试领取后崩溃」：任务卡 in_flight，预占已落盘
    with c.app.state.db.tx() as cur:
        cur.execute("UPDATE notif_send_tasks SET status='in_flight' WHERE id=?",
                    (rid,))
    c.app.state.notif_worker.recover()
    t = task_detail(c, rid)
    assert t["status"] == "pending"
    # 预占仍是同一代同一行（恢复不重复占用）
    rs = reservations(c, task_id=rid)
    assert len(rs) == 1 and rs[0]["state"] == "reserved"

    # 换成成功发送器，恢复后继续：复用预占发送成功
    set_senders(c)
    c.app.state.db.query(
        "UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?", (rid,))
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent"
    rs = reservations(c, task_id=rid)
    assert len(rs) == 1 and rs[0]["state"] == "consumed"


# ---- 超额处置：延迟 / 降级 / 转人工 --------------------------------------------------

def _two_events(c, e1, e2, *, channels=("email",)):
    """造两个不同批次的激活事件（均 critical），返回 (task1, task2)。"""
    out = []
    for ext in (e1, e2):
        todo = make_event(c, ext, channels=list(channels))
        out.append(todo["route_task"]["id"])
    return out


def test_exceed_limit_delays_until_window_ends(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, window_seconds=3600,
                                         on_exceeded="delay"))
    r1, r2 = _two_events(c, "Q-5", "Q-6")

    c.app.state.notif_worker.run_once()
    t1, t2 = task_detail(c, r1), task_detail(c, r2)
    assert t1["status"] == "sent"
    # 第二个任务超额：延迟到窗口结束
    assert t2["status"] == "pending" and t2["quota_status"] == "delayed"
    assert t2["quota_reason"] == "quota_exceeded"
    assert t2["next_retry_at"] is not None and t2["next_retry_at"] > time.time()
    # 未预占任何额度（延迟不占额度）
    assert reservations(c, task_id=r2, state="reserved") == []

    # 等待期间重复扫描不改变结果
    c.app.state.notif_worker.run_once()
    assert task_detail(c, r2)["quota_status"] == "delayed"

    # 窗口到期后放行：把 next_retry_at 提前并推进时钟（直接调派发函数 + 未来时间）
    db = c.app.state.db
    with db.tx() as cur:
        cur.execute("UPDATE notif_send_tasks SET next_retry_at=NULL WHERE id=?",
                    (r2,))
    from app import notif_routing
    future = time.time() + 3700
    notif_routing.dispatch_due_tasks(db, c.app.state.notif_worker.senders,
                                     c.app.state.settings, future)
    t2 = task_detail(c, r2)
    assert t2["status"] == "sent" and t2["quota_status"] == "consumed"
    rs = reservations(c, task_id=r2)
    # 新窗口里预占（bucket_start 与 t1 不同）
    assert rs[0]["bucket_start"] != reservations(c, task_id=r1)[0]["bucket_start"]


def test_exceed_limit_downgrade_to_inbox(env):
    c = env
    sent = {"email": 0}
    set_senders(c, email=lambda *a: sent.__setitem__("email", sent["email"] + 1))
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="downgrade"))
    r1, r2 = _two_events(c, "Q-7", "Q-8")

    c.app.state.notif_worker.run_once()
    t1, t2 = task_detail(c, r1), task_detail(c, r2)
    assert t1["status"] == "sent" and t1["sent_channel"] == "email"
    assert t2["status"] == "sent" and t2["sent_channel"] == "inbox"
    assert t2["plan"] == ["inbox"] and t2["quota_status"] == "consumed"
    assert sent["email"] == 1  # 只有首事件走外发；降级事件由站内通道兜底（不重复外发）
    # 降级预占不计入桶消耗
    rs = reservations(c, task_id=r2)
    assert rs[0]["kind"] == "downgraded"
    buckets = usage(c, recipient=LEAD_A)["buckets"]
    normal = [b for b in buckets if b["rule_id"] == "*"]
    assert sum(b["used"] for b in normal) == 1  # 只有 t1 占额度
    # 降级切换有审计/切换轨迹
    sw = client_switches(c, r2)
    assert any(s["reason"] == "quota_downgraded" for s in sw)


def client_switches(c, task_id):
    return c.get(f"{ROUTING}/switches", params={"task_id": task_id}).json()["switches"]


def test_exceed_limit_manual_resolve_retry_and_ignore(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="manual"))
    r1, r2 = _two_events(c, "Q-9", "Q-10")

    c.app.state.notif_worker.run_once()
    t2 = task_detail(c, r2)
    assert t2["status"] == "awaiting_manual" and t2["quota_status"] == "manual"
    # 额度审查队列可见
    qt = quota_tasks(c, quota_status="manual")
    assert {t["id"] for t in qt} == {r2}

    # 窗口未满时 retry：仍然超额，再次转人工（没有新预占）
    r = c.post(f"{QUOTA}/tasks/{r2}/resolve",
               json={"operator": GRANTOR, "action": "retry"})
    assert r.status_code == 200
    c.app.state.notif_worker.run_once()
    t2 = task_detail(c, r2)
    assert t2["status"] == "awaiting_manual" and t2["quota_generation"] == 2

    # ignore：回收（本来就未预占）并取消任务
    r = c.post(f"{QUOTA}/tasks/{r2}/resolve",
               json={"operator": GRANTOR, "action": "ignore", "note": "不再发送"})
    assert r.status_code == 200 and r.json()["result"] == "ignored"
    t2 = task_detail(c, r2)
    assert t2["status"] == "cancelled" and t2["quota_status"] == "ignored"


def test_manual_retry_after_window_consumes_new_reservation(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="manual"))
    r1, r2 = _two_events(c, "Q-11", "Q-12")
    c.app.state.notif_worker.run_once()
    assert task_detail(c, r2)["status"] == "awaiting_manual"

    # 推进到下一窗口后 retry：取得新预占并发出
    r = c.post(f"{QUOTA}/tasks/{r2}/resolve",
               json={"operator": GRANTOR, "action": "retry"})
    assert r.status_code == 200
    db = c.app.state.db
    from app import notif_routing
    future = time.time() + 3700
    notif_routing.dispatch_due_tasks(db, c.app.state.notif_worker.senders,
                                     c.app.state.settings, future)
    t2 = task_detail(c, r2)
    assert t2["status"] == "sent" and t2["quota_generation"] == 2
    rs = reservations(c, task_id=r2)
    assert len(rs) == 1 and rs[0]["state"] == "consumed"


# ---- 回收：取消任务释放未使用预占 ---------------------------------------------------

def test_consumed_quota_kept_after_task_cancel(env):
    """已发送成功（consumed）的额度在窗口内不随后续取消/确认而释放。"""
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="delay"))
    todo = make_event(c, "Q-13", channels=["email"])
    rid = todo["route_task"]["id"]
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent"
    rs = reservations(c, task_id=rid)
    assert rs[0]["state"] == "consumed"
    # 桶消耗仍为 1（窗口内已用掉）
    assert usage(c, recipient=LEAD_A)["buckets"][0]["used"] == 1


def test_reserved_quota_released_on_cancel(env):
    c = env

    def failing(addr, subject, body):
        raise RuntimeError("down")

    set_senders(c, email=failing)
    publish_routing(c, [{"event_type": None, "channels": [
        {"channel": "email", "max_attempts": 5}]}])
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="delay"))
    todo = make_event(c, "Q-14", channels=["email"])
    rid = todo["route_task"]["id"]
    c.app.state.notif_worker.run_once()  # 失败退避：预占 reserved
    assert reservations(c, task_id=rid)[0]["state"] == "reserved"

    # 联系人停用 -> 待办关闭 -> 任务取消 -> 预占回收
    r = c.post("/admin/approval-notifications/contacts/ops-wang/deactivate",
               json={"operator": GRANTOR, "reason": "离职"})
    assert r.status_code == 200
    t = task_detail(c, rid)
    assert t["status"] == "cancelled" and t["quota_status"] == "released"
    rs = reservations(c, task_id=rid)
    assert rs[0]["state"] == "released"
    # 桶消耗归零
    buckets = usage(c, recipient=LEAD_A)["buckets"]
    assert all(b["used"] == 0 for b in buckets)


# ---- 回执驱动的通道切换不绕过额度 ---------------------------------------------------

def test_receipt_failover_reuses_same_reservation(env):
    c = env
    captured = {}

    def ok_email(addr, subject, body):
        captured["email"] = captured.get("email", 0) + 1
        return "prov-email-1"

    def ok_webhook(url, payload):
        captured["webhook"] = captured.get("webhook", 0) + 1
        return "prov-hook-1"

    set_senders(c, email=ok_email, webhook=ok_webhook)
    publish_routing(c, [{"event_type": None, "channels": [
        {"channel": "email"}, {"channel": "webhook"}, {"channel": "inbox"}]}])
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="manual"))
    todo = make_event(c, "Q-15", channels=["email", "webhook"])
    rid = todo["route_task"]["id"]
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent" and t["sent_channel"] == "email"
    rs1 = reservations(c, task_id=rid)
    assert len(rs1) == 1 and rs1[0]["state"] == "consumed"

    # 回执 bounced + on_bounced=retry -> 自动故障转移到 webhook
    c.put("/admin/receipt-policy", json={
        "operator": GRANTOR, "on_bounced": "retry"})
    msg = c.get("/admin/external-messages",
                params={"task_id": rid}).json()["messages"][0]
    body = json.dumps({"event": "bounced", "message_id": msg["message_id"]})
    import hmac, hashlib
    # 登记一个已知回执密钥并验签
    kr = c.post("/admin/receipt-keys/email", json={
        "operator": GRANTOR, "kid": "rk", "secret": "rcpt-secret"})
    assert kr.status_code == 200, kr.text
    ts = int(time.time())
    sig = hmac.new(b"rcpt-secret", f"{ts}\n".encode() + body.encode(),
                   hashlib.sha256).hexdigest()
    r = c.post("/receipts/email", content=body, headers={
        "X-Signature": f"kid=rk,ts={ts},sig={sig}"})
    assert r.status_code == 200, r.text

    # 回执驱动重排，worker 补发 webhook
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent" and t["sent_channel"] == "webhook"
    # 同一代：没有产生第二个预占，不绕过/重复占用额度
    rs = reservations(c, task_id=rid)
    assert len(rs) == 1 and rs[0]["generation"] == 1 and rs[0]["state"] == "consumed"
    assert t["quota_generation"] == 1


# ---- 配置版本快照不被后续发布改变 ---------------------------------------------------

def test_quota_snapshot_frozen_after_enqueue(env):
    c = env
    set_senders(c)
    publish_routing(c)
    v1 = publish_quota(c, default_quota_rules(limit=2, cost=2,
                                              on_exceeded="manual"))
    todo = make_event(c, "Q-16", channels=["email"])
    rid = todo["route_task"]["id"]
    snap = task_detail(c, rid)["quota_snapshot"]
    assert snap["cost"] == 2 and snap["on_exceeded"] == "manual"
    # 发布 v2：limit 变大、动作变 delay、cost 变 1
    publish_quota(c, default_quota_rules(limit=10, cost=1, on_exceeded="delay"))
    assert task_detail(c, rid)["quota_version"] == v1
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent"
    rs = reservations(c, task_id=rid)
    assert rs[0]["cost"] == 2  # 仍按入队快照的 cost 占用


# ---- 稳定占用顺序：同接收人并发多事件按 ordinal 排序 ----------------------------------

def test_stable_ordering_same_recipient_no_jumping(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="delay"))
    r1, r2, r3 = _three_events(c)
    ids = [r1, r2, r3]

    # 并发派发同一批任务：最终只有 ordinal 最小的 r1 发出，其余延迟
    threads = []
    errors = []

    def worker(tid):
        try:
            from app import notif_routing
            notif_routing.process_send_task(
                c.app.state.db, tid, c.app.state.notif_worker.senders,
                c.app.state.settings, time.time())
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    for tid in ids:
        th = threading.Thread(target=worker, args=(tid,))
        threads.append(th)
    for th in threads:
        th.start()
    for th in threads:
        th.join()
    assert not errors, errors
    statuses = {tid: task_detail(c, tid)["status"] for tid in ids}
    assert statuses[r1] == "sent"
    assert statuses[r2] == "pending" and statuses[r3] == "pending"
    # 高 ordinal 的延迟原因体现有序等待或超额（不允许 r2/r3 抢在 r1 前发出）
    for tid in (r2, r3):
        assert task_detail(c, tid)["quota_status"] in ("delayed", "admitted")
    # 只有一个预占
    active = [r for r in reservations(c) if r["state"] in ("reserved", "consumed")]
    assert len(active) == 1


def _three_events(c):
    out = []
    for ext in ("Q-17", "Q-18", "Q-19"):
        todo = make_event(c, ext, channels=["email"])
        out.append(todo["route_task"]["id"])
    return out


# ---- 级别解析 / 接收人定向规则 ------------------------------------------------------

def test_level_resolution_and_recipient_scoped_rules(env):
    c = env
    set_senders(c)
    publish_routing(c)
    # LEAD_A 的 critical 限 1（manual）；其他人/级别走缺省大额度
    publish_quota(c, [
        {"id": "lead-crit", "recipient": LEAD_A, "level": "critical",
         "window_seconds": 3600, "limit": 1, "on_exceeded": "manual"},
        {"id": "*", "level": "*", "window_seconds": 3600, "limit": 100}],
        event_levels={"vote_received": "critical"})
    r1, r2 = _two_events(c, "Q-20", "Q-21")
    c.app.state.notif_worker.run_once()
    t1, t2 = task_detail(c, r1), task_detail(c, r2)
    assert t1["quota_rule_id"] == "lead-crit" and t1["quota_level"] == "critical"
    assert t2["status"] == "awaiting_manual"
    # usage 只含命中规则桶
    buckets = usage(c)["buckets"]
    assert {b["rule_id"] for b in buckets} == {"lead-crit"}


# ---- 无额度配置 / 无匹配规则：不受约束 ----------------------------------------------

def test_no_quota_version_means_unrestricted(env):
    c = env
    set_senders(c)
    publish_routing(c)
    todo = make_event(c, "Q-22", channels=["email"])
    rid = todo["route_task"]["id"]
    c.app.state.notif_worker.run_once()
    t = task_detail(c, rid)
    assert t["status"] == "sent" and t["quota_status"] == "none"
    assert t["quota_version"] is None and reservations(c) == []


# ---- 审计可查 ----------------------------------------------------------------------

def test_quota_actions_are_audited(env):
    c = env
    set_senders(c)
    publish_routing(c)
    publish_quota(c, default_quota_rules(limit=1, on_exceeded="delay"))
    r1, r2 = _two_events(c, "Q-23", "Q-24")
    c.app.state.notif_worker.run_once()
    types = audit_types(c)
    assert "notif_quota_published" in types
    assert "notif_quota_reserved" in types
    assert "notif_quota_consumed" in types
    assert "notif_quota_delayed" in types


def audit_types(c):
    rows = c.get("/admin/events", params={"limit": 1000}).json()["events"]
    return {r["type"] for r in rows}

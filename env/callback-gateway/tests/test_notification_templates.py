"""通知内容模板编排模块的端到端测试。

覆盖：
- 草稿创建/编辑/校验/发布；校验失败落 rejected 且当前指针不动；重复发布幂等；
- 变量定义与正文占位符校验（未声明占位符、敏感变量未脱敏、必填未使用、语言间不一致）；
- 渲染预览（多通道、错误回显、不留失败记录、不发送）；
- 版本化路由入队渲染：按接收人语言选择、明确回退顺序、模板版本/语言/变量快照/最终正文
  固化进发送任务；发布新版本不改写已入队任务；发送器只读固化正文；
- 变量缺失、类型不符、正文超长、敏感未脱敏 -> render_failed 终态不发送，失败原因可查，
  修复后手动 retry-render 才能再派发；
- 敏感变量脱敏（正文与快照均不含原文）；
- 旧链路（未发布路由）投递落渲染后正文并固化模板版本/语言/哈希；
- 通配模板解析（event,* / *,channel / *,*）；
- 并发/重复：重复发布、重复渲染、worker 多轮不产生第二份发送效果。
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
TPL = "/admin/approval-notifications/templates"
NOTIF = "/admin/approval-notifications"


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
        notif_max_attempts=3,
        notif_breaker_failure_threshold=3,
        notif_breaker_cooldown_seconds=30.0,
        notif_channel_timeout_seconds=10.0,
        notif_default_language="zh",
        notif_email_subject_max=200,
        notif_email_body_max=80,          # 收紧长度上限以测超长阻断
        notif_webhook_body_max=20000,
        notif_inbox_body_max=5000)
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def post(client, external_id, body=b'{"order": 1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})


def process_normally(client, external_id):
    # 主 Worker 处理回调落盘（重放批次引用要求内容已处理）；通知派发由 notif_worker 负责
    post(client, external_id)
    client.app.state.worker.run_once()


def apply_policy(client):
    rules = [{"name": "h", "risk_level": "high", "mode": "parallel",
              "nodes": [{"role": "ops-lead", "timeout_seconds": 3600}]}]
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.text


def contact(client, name, *, channels=None, email=None, webhook_url=None,
            language=None):
    body = {"name": name, "operator": GRANTOR, "channels": channels or []}
    if email:
        body["email"] = email
    if webhook_url:
        body["webhook_url"] = webhook_url
    if language:
        body["language"] = language
    r = client.post(f"{NOTIF}/contacts", json=body)
    assert r.status_code == 200, r.text
    return r


def delegation(client, role="ops-lead", delegatee=LEAD_A):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": role, "delegatee": delegatee, "operator": GRANTOR,
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text
    return r.json()["delegation_id"]


def make_event(client, external_id, *, language=None,
               channels=("email", "webhook"),
               email="wang@example.com", webhook_url="https://hook.example.com/x"):
    """注册联系人 + 提交高风险批次 + 委托，返回该接收人在最新批次上的唯一待办。"""
    apply_policy(client)
    contact(client, LEAD_A, channels=list(channels), email=email,
            webhook_url=webhook_url, language=language)
    process_normally(client, external_id)
    bid = client.post("/admin/replays", json={
        "operator": SUBMITTER, "reason": "资金类回调补发", "risk_level": "high",
        "approval_note": "需审批", "external_id": external_id}
    ).json()["batch_id"]
    delegation(client)
    todos = client.get(f"{NOTIF}/todos",
                       params={"recipient": LEAD_A, "batch_id": bid}).json()["todos"]
    assert len(todos) == 1
    return todos[0]


def set_senders(client, *, email=None, webhook=None):
    sent = []

    def _email(addr, subject, body):
        sent.append(("email", addr, subject, body))

    def _webhook(addr, payload):
        sent.append(("webhook", addr, payload))

    # 传入的自定义发送器也必须是 fn(addr,subject,body)/fn(addr,payload)；
    # 缺省用会记录调用参数的包装器
    client.app.state.notif_worker.senders = {
        "email": email if email is not None else _email,
        "webhook": webhook if webhook is not None else _webhook,
        "inbox": lambda task: None}
    return sent


def publish_routing(client, order=("email", "webhook", "inbox")):
    r = client.post(f"{ROUTING}/versions",
                    json={"operator": GRANTOR,
                          "rules": [{"event_type": None,
                                     "channels": [{"channel": c} for c in order]}]})
    assert r.status_code == 200, r.text


def draft(client, *, event_type="activated", channel="email", variables=None,
          texts=None, fallback=None, status_expected=200):
    body = {"event_type": event_type, "channel": channel, "operator": GRANTOR,
            "upsert": True,
            "variables": variables or [],
            "texts": texts or [
                {"language": "zh", "subject": "节点激活 {submitted_by}",
                 "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by} 提交"},
                {"language": "en", "subject": "Node active {submitted_by}",
                 "body": "Batch {batch_id} node {node_seq} by {submitted_by}"}],
            "fallback_languages": fallback or []}
    r = client.post(f"{TPL}/drafts", json=body)
    assert r.status_code == status_expected, r.text
    return r


def publish_template(client, event_type="activated", channel="email",
                     expected=200):
    r = client.post(f"{TPL}/drafts/{event_type}/{channel}/publish",
                    json={"operator": GRANTOR, "reason": "go"})
    assert r.status_code == expected, r.text
    return r


def update_draft(client, *, event_type="activated", channel="email", **fields):
    r = client.put(f"{TPL}/drafts/{event_type}/{channel}",
                   json={"operator": GRANTOR, **fields})
    assert r.status_code == 200, r.text
    return r


def task_detail(client, task_id):
    return client.get(f"{ROUTING}/tasks/{task_id}").json()["task"]


def failures(client, **params):
    return client.get(f"{TPL}/render-failures", params=params).json()["failures"]


STANDARD_VARS = [
    {"name": "batch_id", "type": "int"},
    {"name": "node_seq", "type": "int"},
    {"name": "submitted_by", "type": "string"}]


# ---- 草稿 / 校验 / 发布 ----------------------------------------------------------

def test_draft_validate_publish_and_current_pointer(env):
    client = env
    r = draft(client, variables=STANDARD_VARS)
    assert r.json()["created"] is True
    # 不带 upsert 的重复创建：幂等返回既有草稿，不产生第二份
    rep = client.post(f"{TPL}/drafts", json={
        "event_type": "activated", "channel": "email", "operator": GRANTOR,
        "variables": STANDARD_VARS,
        "texts": [{"language": "zh", "subject": "x {submitted_by}",
                   "body": "{batch_id} {node_seq} {submitted_by}"},
                  {"language": "en", "subject": "x {submitted_by}",
                   "body": "{batch_id} {node_seq} {submitted_by}"}]})
    assert rep.json()["created"] is False
    drafts = client.get(f"{TPL}/drafts").json()["versions"]
    assert len(drafts) == 1

    v = client.post(f"{TPL}/drafts/activated/email/validate").json()
    assert v["valid"] is True and v["errors"] == []

    out = publish_template(client).json()
    assert out["result"] == "published" and out["version"] == 1
    current = client.get(f"{TPL}/current").json()["current"]
    assert {(c["event_type"], c["channel"], c["version"]) for c in current} == \
        {("activated", "email", 1)}

    # 发布不改变草稿（草稿保留，可继续编辑后再发布新版本）；published 版本不可变
    draft_after = client.get(f"{TPL}/drafts/activated/email")
    assert draft_after.status_code == 200
    versions = client.get(f"{TPL}/versions",
                          params={"event_type": "activated", "channel": "email"}
                          ).json()["versions"]
    assert {(v["status"], v["version"]) for v in versions} == \
        {("draft", None), ("published", 1)}


def test_publish_rejected_on_undeclared_placeholder_and_unknown_filter(env):
    client = env
    # 正文含未声明占位符 {deadline}
    draft(client, variables=STANDARD_VARS, texts=[
        {"language": "zh", "subject": "S {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 截止 {deadline}"},
        {"language": "en", "subject": "S {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} due {deadline}"}])
    r = publish_template(client, expected=422).json()
    assert r["detail"][0]["code"] == "render_error"
    # rejected 留痕，但当前指针没有
    rejected = client.get(f"{TPL}/versions",
                          params={"status": "rejected"}).json()["versions"]
    assert len(rejected) == 1 and rejected[0]["rejection_reason"]
    assert client.get(f"{TPL}/current").json()["current"] == []

    # 非法占位符语法 {name|upper}（未知过滤器）在 validate 中报错
    update_draft(client, texts=[
        {"language": "zh", "subject": "S {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by|upper}"},
        {"language": "en", "subject": "S {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by|upper}"}])
    v = client.post(f"{TPL}/drafts/activated/email/validate").json()
    assert v["valid"] is False and v["errors"][0]["code"] == "render_error"


def test_publish_rejected_when_sensitive_not_masked(env):
    client = env
    variables = [{"name": "batch_id", "type": "int"},
                 {"name": "token", "type": "string", "sensitive": True}]
    draft(client, variables=variables, texts=[
        {"language": "zh", "subject": "验证码 {token}",
         "body": "批次 {batch_id}：您的验证码 {token}"},
        {"language": "en", "subject": "Code {token}",
         "body": "Batch {batch_id}: code {token}"}])
    r = publish_template(client, expected=422).json()
    assert r["detail"][0]["code"] == "sensitive_unmasked"

    # 改为 |mask 后可发布
    update_draft(client, variables=variables, texts=[
        {"language": "zh", "subject": "验证码 {token|mask}",
         "body": "批次 {batch_id}：您的验证码 {token|mask}"},
        {"language": "en", "subject": "Code {token|mask}",
         "body": "Batch {batch_id}: code {token|mask}"}])
    assert publish_template(client).json()["result"] == "published"


def test_required_variable_unused_and_language_placeholder_mismatch(env):
    client = env
    # 必填变量声明了却没在正文使用
    variables = STANDARD_VARS + [{"name": "unused", "type": "string"}]
    draft(client, variables=variables)
    v = client.post(f"{TPL}/drafts/activated/email/validate").json()
    assert v["errors"] and v["errors"][0]["code"] == "render_error"

    # 语言间占位符集合不一致
    update_draft(client, variables=STANDARD_VARS, texts=[
        {"language": "zh", "subject": "S {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}"},
        {"language": "en", "subject": "S {submitted_by}",
         "body": "Batch {batch_id} by {submitted_by}"}])  # 缺 node_seq
    v = client.post(f"{TPL}/drafts/activated/email/validate").json()
    assert v["errors"][0]["code"] == "render_error"


def test_duplicate_publish_is_idempotent(env):
    client = env
    draft(client, variables=STANDARD_VARS)
    first = publish_template(client).json()
    # 发布后同键再建同内容草稿并发布 -> unchanged，不产生版本 2
    draft(client, variables=STANDARD_VARS)
    second = publish_template(client).json()
    assert second["result"] == "unchanged" and second["version"] == first["version"]
    versions = client.get(f"{TPL}/versions",
                          params={"status": "published"}).json()["versions"]
    assert [v["version"] for v in versions] == [1]


def test_preview_is_read_only(env):
    client = env
    draft(client, variables=STANDARD_VARS)
    r = client.post(f"{TPL}/drafts/activated/email/preview", json={
        "language": "en",
        "variables": {"batch_id": 7, "node_seq": 0, "submitted_by": SUBMITTER}})
    assert r.status_code == 200, r.text
    pv = r.json()["previews"][0]
    assert pv["ok"] is True and pv["language"] == "en"
    assert pv["body"] == f"Batch 7 node 0 by {SUBMITTER}"
    assert pv["content_sha256"]
    # 预览不留失败记录、不产生任务
    assert failures(client) == []
    assert client.get(f"{ROUTING}/tasks").json()["count"] == 0

    # 变量缺失在预览里逐通道回显错误（不 422、不落库）
    r = client.post(f"{TPL}/drafts/activated/email/preview",
                    json={"variables": {"batch_id": 7}}).json()
    assert r["previews"][0]["ok"] is False
    assert r["previews"][0]["error"]["code"] == "missing_variable"
    assert failures(client) == []


# ---- 路由链路：语言选择、回退、固化 ------------------------------------------------

def test_routed_task_renders_by_recipient_language_and_freezes_snapshot(env):
    client = env
    set_senders(client)
    publish_routing(client)
    draft(client, variables=STANDARD_VARS)
    publish_template(client)

    todo = make_event(client, "R-1", language="en")
    task = task_detail(client, todo["route_task"]["id"])
    assert task["render_status"] == "rendered"
    snap = task["content_snapshot"]["email"]
    assert snap["language"] == "en"
    assert snap["template_version_id"] is not None
    assert snap["language_chain"][0] == "en"
    assert snap["body"] == f"Batch 1 node 0 by {SUBMITTER}"
    assert snap["variables_snapshot"] == {
        "batch_id": 1, "node_seq": 0, "submitted_by": SUBMITTER}
    assert task["plan"][0:2] == ["email", "webhook"]
    # webhook 通道未配置模板：该通道静态正文（通配解析未命中）
    assert "webhook" not in task["content_snapshot"] or \
        task["content_snapshot"]["webhook"]["template_version_id"] is None

    # 发布新版本（英文正文变化）不影响已入队任务：继续编辑原草稿再发布
    update_draft(client, variables=STANDARD_VARS, texts=[
        {"language": "zh", "subject": "新标题 {submitted_by}",
         "body": "新版批次 {batch_id} 节点 {node_seq} 由 {submitted_by}"},
        {"language": "en", "subject": "New title {submitted_by}",
         "body": "NEW Batch {batch_id} node {node_seq} by {submitted_by}"}])
    publish_template(client)
    client.app.state.notif_worker.run_once()
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "sent" and task["sent_channel"] == "email"
    snap = task["content_snapshot"]["email"]
    assert snap["body"] == f"Batch 1 node 0 by {SUBMITTER}"  # 仍是入队时的 v1
    assert task["content_sha256"] == snap["content_sha256"]


def test_language_fallback_chain_explicit_order(env):
    client = env
    set_senders(client)
    publish_routing(client)
    # 模板只提供 en；接收人偏好 fr；声明回退 en
    draft(client, variables=STANDARD_VARS, fallback=["en"], texts=[
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}"}])
    publish_template(client)
    todo = make_event(client, "R-2", language="fr")
    snap = task_detail(client, todo["route_task"]["id"])["content_snapshot"]["email"]
    assert snap["language"] == "en"
    assert snap["language_chain"] == ["fr", "en", "zh"]


def test_missing_language_blocks_and_is_queryable(env):
    client = env
    set_senders(client)
    publish_routing(client)
    # 只有 en；接收人 fr，模板无回退，系统缺省 zh 也不提供 -> missing_language。
    for ch in ("email", "webhook"):
        draft(client, channel=ch, variables=STANDARD_VARS, texts=[
            {"language": "en", "subject": "Node {submitted_by}",
             "body": f"Batch {{batch_id}} node {{node_seq}} by {{submitted_by}} [{ch}]"}])
        publish_template(client, channel=ch)
    todo = make_event(client, "R-3", language="fr")
    task = task_detail(client, todo["route_task"]["id"])
    # email/webhook 均无语言版本被剔除；inbox 未配置模板，任务仍可走 inbox 兜底发送，
    # 但失败记录可查
    fl = failures(client, reason_code="missing_language")
    assert {f["channel"] for f in fl} == {"email", "webhook"}
    assert fl[0]["language_chain"] == ["fr", "zh"]
    client.app.state.notif_worker.run_once()
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "sent" and task["sent_channel"] == "inbox"
    # 外发通道一次都没发
    assert not [a for a in task["attempts"] if a["channel"] in ("email", "webhook")]


def test_missing_variable_blocks_all_channels_render_failed(env):
    client = env
    set_senders(client)
    publish_routing(client, order=("email", "webhook"))  # 无 inbox 兜底
    # 通配通道模板同时覆盖 email/webhook；缺变量时两通道全阻断
    variables = STANDARD_VARS + [
        {"name": "approval_link", "type": "string", "required": True}]
    draft(client, channel="*", variables=variables, texts=[
        {"language": "zh", "subject": "节点 {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；链接 {approval_link}"},
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; {approval_link}"}])
    publish_template(client, channel="*")
    todo = make_event(client, "R-4", channels=["email", "webhook"])
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "render_failed"
    assert task["render_failure_reason"][0]["code"] == "missing_variable"
    # worker 多轮也不会派发
    for _ in range(3):
        client.app.state.notif_worker.run_once()
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "render_failed" and task["attempts"] == []
    fl = failures(client)
    assert len(fl) == 2 and {f["channel"] for f in fl} == {"email", "webhook"}

    # 修复：发布带默认值的新版本（同样通配通道），再手动重试渲染
    draft(client, channel="*",
          variables=STANDARD_VARS + [
        {"name": "approval_link", "type": "string", "required": False,
         "default": "https://app.example.com/approve"}], texts=[
        {"language": "zh", "subject": "节点 {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；链接 {approval_link}"},
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; {approval_link}"}])
    publish_template(client, channel="*")
    r = client.post(f"{TPL}/tasks/{task['id']}/retry-render",
                    json={"operator": GRANTOR})
    assert r.status_code == 200, r.text
    assert r.json()["result"] == "rebuilt"
    client.app.state.notif_worker.run_once()
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "sent" and task["sent_channel"] == "email"
    assert "https://app.example.com/approve" in task["content_snapshot"]["email"]["body"]
    # 失败记录已解除
    assert failures(client) == []
    assert len(failures(client, resolved=True)) == 2


def test_type_mismatch_and_body_too_long_block(env):
    client = env
    set_senders(client)
    publish_routing(client, order=("email", "webhook"))
    # risk_level 是字符串却声明 int -> 类型不符（通配通道模板，email/webhook 全阻断）
    variables = [{"name": "batch_id", "type": "int"},
                 {"name": "risk_level", "type": "int"}]
    draft(client, channel="*", variables=variables, texts=[
        {"language": "zh", "subject": "批次 {batch_id}",
         "body": "风险 {risk_level} 批次 {batch_id}"},
        {"language": "en", "subject": "Batch {batch_id}",
         "body": "Risk {risk_level} batch {batch_id}"}])
    publish_template(client, channel="*")
    todo = make_event(client, "R-5")
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "render_failed"
    assert task["render_failure_reason"][0]["code"] == "type_mismatch"
    fl = failures(client, reason_code="type_mismatch")
    assert {f["channel"] for f in fl} == {"email", "webhook"}

    # 超长：改正类型后正文仍超 80 字 -> body_too_long
    update_draft(client, channel="*", variables=[
        {"name": "batch_id", "type": "int"},
        {"name": "risk_level", "type": "string"}], texts=[
        {"language": "zh", "subject": "批次 {batch_id}",
         "body": "风险等级 " + "很长" * 100 + " {risk_level} 批次 {batch_id}"},
        {"language": "en", "subject": "Batch {batch_id}",
         "body": "risk " + "x" * 100 + " {risk_level} batch {batch_id}"}])
    publish_template(client, channel="*")
    r = client.post(f"{TPL}/tasks/{task['id']}/retry-render",
                    json={"operator": GRANTOR})
    assert r.json()["result"] == "failed"
    assert {f["code"] for f in r.json()["failures"]} == {"body_too_long"}
    task = task_detail(client, todo["route_task"]["id"])
    assert task["status"] == "render_failed"


def test_sensitive_value_masked_in_body_and_snapshot(env):
    client = env
    sent = set_senders(client)
    publish_routing(client)
    variables = STANDARD_VARS + [
        {"name": "token", "type": "string", "sensitive": True,
         "default": "ABCDEFGH123456"}]
    draft(client, variables=variables, texts=[
        {"language": "zh", "subject": "验证码 {token|mask}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；验证码 {token|mask}"},
        {"language": "en", "subject": "Code {token|mask}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; code {token|mask}"}])
    publish_template(client)
    todo = make_event(client, "R-6")
    client.app.state.notif_worker.run_once()
    body = next(item[3] for item in sent if item[0] == "email")
    assert "ABCDEFGH123456" not in body
    assert "****3456" in body
    task = task_detail(client, todo["route_task"]["id"])
    snap = task["content_snapshot"]["email"]
    assert snap["variables_snapshot"]["token"] == "****3456"
    assert "ABCDEFGH123456" not in json.dumps(snap, ensure_ascii=False)


def test_repeat_render_and_repeated_dispatch_send_once(env):
    client = env
    sent = set_senders(client)
    publish_routing(client)
    draft(client, variables=STANDARD_VARS)
    publish_template(client)
    todo = make_event(client, "R-7", language="en")
    tid = todo["route_task"]["id"]
    # 对非 render_failed 任务重试渲染：409
    r = client.post(f"{TPL}/tasks/{tid}/retry-render", json={"operator": GRANTOR})
    assert r.status_code == 409
    client.app.state.notif_worker.run_once()
    client.app.state.notif_worker.run_once()
    client.app.state.notif_worker.run_once()
    emails = [s for s in sent if s[0] == "email"]
    assert len(emails) == 1
    task = task_detail(client, tid)
    assert len(task["attempts"]) == 1
    # 模拟重启恢复后再派发：仍不重发
    client.app.state.notif_worker.recover()
    client.app.state.notif_worker.run_once()
    assert len([s for s in sent if s[0] == "email"]) == 1


# ---- 旧链路 --------------------------------------------------------------------

def test_legacy_deliveries_render_and_freeze_template_content(env):
    client = env
    sent = set_senders(client)
    # 不发布路由版本 -> 旧的每通道投递链路
    draft(client, variables=STANDARD_VARS)
    publish_template(client)
    todo = make_event(client, "R-8", channels=["email"])
    d = client.get(f"{NOTIF}/deliveries",
                   params={"recipient": LEAD_A}).json()["deliveries"]
    assert len(d) == 1
    row = d[0]
    assert row["template_version_id"] is not None
    assert row["template_language"] == "zh"
    assert row["body"] == f"批次 1 节点 0 由 {SUBMITTER} 提交"
    assert row["content_sha256"]
    # 发布新版本不改已入队投递；发送读落盘正文
    draft(client, variables=STANDARD_VARS, texts=[
        {"language": "zh", "subject": "新 {submitted_by}",
         "body": "改写后的正文 {batch_id}/{node_seq}/{submitted_by}"},
        {"language": "en", "subject": "New {submitted_by}",
         "body": "CHANGED {batch_id}/{node_seq}/{submitted_by}"}])
    publish_template(client)
    client.app.state.notif_worker.run_once()
    emails = [s for s in sent if s[0] == "email"]
    assert len(emails) == 1 and emails[0][3] == f"批次 1 节点 0 由 {SUBMITTER} 提交"


def test_legacy_render_failure_creates_no_delivery_but_queryable(env):
    client = env
    set_senders(client)
    variables = STANDARD_VARS + [{"name": "approval_link", "type": "string"}]
    draft(client, variables=variables, texts=[
        {"language": "zh", "subject": "节点 {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；{approval_link}"},
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; {approval_link}"}])
    publish_template(client)
    todo = make_event(client, "R-9", channels=["email"])
    assert todo["deliveries"] == []
    fl = failures(client)
    assert len(fl) == 1 and fl[0]["entity_type"] == "delivery"
    assert fl[0]["reason_code"] == "missing_variable"
    client.app.state.notif_worker.run_once()
    # 没有投递就没有任何外发
    assert client.get(f"{NOTIF}/deliveries",
                      params={"recipient": LEAD_A}).json()["deliveries"] == []


# ---- 通配模板 -------------------------------------------------------------------

def test_wildcard_template_resolution_order(env):
    client = env
    set_senders(client)
    publish_routing(client)
    # ('*','*') 双通配模板：所有事件类型 × 所有通道命中
    draft(client, event_type="*", channel="*", variables=[
        {"name": "submitted_by", "type": "string"}], texts=[
        {"language": "zh", "subject": "通知 {submitted_by}",
         "body": "通用模板正文：{submitted_by}"},
        {"language": "en", "subject": "Notice {submitted_by}",
         "body": "Generic body: {submitted_by}"}])
    publish_template(client, event_type="*", channel="*")
    todo = make_event(client, "R-10", language="en")
    snap = task_detail(client, todo["route_task"]["id"])["content_snapshot"]
    assert snap["email"]["body"] == f"Generic body: {SUBMITTER}"
    assert snap["webhook"]["body"] == f"Generic body: {SUBMITTER}"


def test_precise_template_overrides_wildcard(env):
    client = env
    set_senders(client)
    publish_routing(client)
    draft(client, event_type="*", channel="*", variables=[
        {"name": "submitted_by", "type": "string"}], texts=[
        {"language": "zh", "subject": "通用", "body": "通用 {submitted_by}"},
        {"language": "en", "subject": "Generic", "body": "generic {submitted_by}"}])
    publish_template(client, event_type="*", channel="*")
    draft(client, event_type="activated", channel="email",
          variables=STANDARD_VARS)
    publish_template(client)
    todo = make_event(client, "R-11", language="en")
    snap = task_detail(client, todo["route_task"]["id"])["content_snapshot"]
    # 精确键优先
    assert snap["email"]["body"] == f"Batch 1 node 0 by {SUBMITTER}"
    # webhook 落到双通配
    assert snap["webhook"]["body"] == f"generic {SUBMITTER}"


# ---- 管理员查询：版本 / 预览 / 快照 / 审计 -----------------------------------------

def test_admin_views_versions_snapshot_and_audit(env):
    client = env
    set_senders(client)
    publish_routing(client)
    draft(client, variables=STANDARD_VARS)
    published = publish_template(client).json()
    todo = make_event(client, "R-12", language="en")
    tid = todo["route_task"]["id"]
    client.app.state.notif_worker.run_once()

    # 版本历史含完整内容
    detail = client.get(f"{TPL}/versions/{published['template_version_id']}").json()
    assert detail["version"]["status"] == "published"
    assert {t["language"] for t in detail["version"]["texts"]} == {"zh", "en"}

    # 已发布版本也可预览
    r = client.post(
        f"{TPL}/versions/{published['template_version_id']}/preview",
        json={"language": "zh",
              "variables": {"batch_id": 9, "node_seq": 2, "submitted_by": "x"}})
    assert r.json()["previews"][0]["body"] == "批次 9 节点 2 由 x 提交"

    # 发送任务详情内嵌内容快照
    task = task_detail(client, tid)
    assert task["content_snapshot"]["email"]["template_version_id"] == \
        published["template_version_id"]
    assert task["template_snapshot"]["languages"] == ["en", "zh"]

    # 审计链：发布、入队、发送均可按类型查到
    db = client.app.state.db
    types = {r["type"] for r in db.query(
        "SELECT DISTINCT type FROM events WHERE type LIKE 'notif_template%' "
        "OR type='notif_send_task_enqueued' OR type='notif_send_sent'")}
    assert "notif_template_published" in types
    assert "notif_send_task_enqueued" in types
    assert "notif_send_sent" in types


# ---- 并发：发布 / 重启恢复 ---------------------------------------------------------

def test_concurrent_publish_single_winner(env):
    """并发发布不同内容：版本号严格单调、指针只指向一个版本，不丢审计。"""
    client = env

    def make_defn(marker):
        return {
            "event_type": "activated", "channel": "email", "operator": GRANTOR,
            "variables": STANDARD_VARS,
            "texts": [
                {"language": "zh", "subject": f"{marker} {{submitted_by}}",
                 "body": f"{marker} 批次 {{batch_id}} 节点 {{node_seq}} 由 {{submitted_by}}"},
                {"language": "en", "subject": f"{marker} {{submitted_by}}",
                 "body": f"{marker} batch {{batch_id}} node {{node_seq}} by {{submitted_by}}"}],
            "fallback_languages": []}

    # 先建草稿
    r = client.post(f"{TPL}/drafts", json=make_defn("v0"))
    assert r.status_code == 200, r.text
    results = []
    barrier = threading.Barrier(5)

    def worker(i):
        # 每个线程先整体替换草稿再发布；单连接写事务串行，只有一个版本号能赢每一轮
        barrier.wait()
        try:
            client.put(f"{TPL}/drafts/activated/email",
                       json={"operator": GRANTOR, **{
                           k: v for k, v in make_defn(f"v{i}").items()
                           if k in ("variables", "texts", "fallback_languages")}})
            r = client.post(f"{TPL}/drafts/activated/email/publish",
                            json={"operator": GRANTOR, "reason": f"c{i}"})
            results.append((i, r.status_code, r.json() if r.status_code == 200 else None))
        except Exception as exc:  # noqa: BLE001
            results.append((i, "exc", str(exc)))

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 每次成功发布要么推进新版本，要么因内容指纹不同必然产生新版本；版本号无空洞竞争破坏
    published = client.get(f"{TPL}/versions",
                           params={"status": "published",
                                   "event_type": "activated",
                                   "channel": "email"}).json()["versions"]
    versions = sorted(v["version"] for v in published)
    assert versions == list(range(1, len(versions) + 1))
    # 指针与某一个已发布版本一致
    current = client.get(f"{TPL}/current").json()["current"]
    assert len(current) == 1
    assert current[0]["version"] == versions[-1]


def test_render_failed_task_survives_restart_recover_and_then_retries(env):
    """render_failed 终态在服务重启/recover 后保持不变，不被自动派发；修复后可手动重试。"""
    client = env
    set_senders(client)
    publish_routing(client, order=("email", "webhook"))
    variables = STANDARD_VARS + [
        {"name": "approval_link", "type": "string", "required": True}]
    draft(client, channel="*", variables=variables, texts=[
        {"language": "zh", "subject": "节点 {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；{approval_link}"},
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; {approval_link}"}])
    publish_template(client, channel="*")
    todo = make_event(client, "R-20", channels=["email", "webhook"])
    tid = todo["route_task"]["id"]
    assert task_detail(client, tid)["status"] == "render_failed"

    # 模拟服务重启：recover 只回收 in_flight，不碰 render_failed；再跑 worker 也不发
    client.app.state.notif_worker.recover()
    client.app.state.notif_worker.run_once()
    assert task_detail(client, tid)["status"] == "render_failed"

    # 修好模板（给默认值）后手动重试渲染，再由 worker 派发
    draft(client, channel="*",
          variables=STANDARD_VARS + [
              {"name": "approval_link", "type": "string", "required": False,
               "default": "https://app/approve"}], texts=[
        {"language": "zh", "subject": "节点 {submitted_by}",
         "body": "批次 {batch_id} 节点 {node_seq} 由 {submitted_by}；{approval_link}"},
        {"language": "en", "subject": "Node {submitted_by}",
         "body": "Batch {batch_id} node {node_seq} by {submitted_by}; {approval_link}"}])
    publish_template(client, channel="*")
    r = client.post(f"{TPL}/tasks/{tid}/retry-render", json={"operator": GRANTOR})
    assert r.json()["result"] == "rebuilt"
    client.app.state.notif_worker.run_once()
    assert task_detail(client, tid)["status"] == "sent"

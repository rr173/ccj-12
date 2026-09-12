"""可配置多级审批策略的端到端测试：

- 运营可维护策略版本（提交即生效新版本、失败保留当前、每次变更留痕）；
- 提交批次时按风险等级与批次规模匹配规则，生成串行/并行审批节点；
- 每个节点记录指定角色、实际审批人、截止时间；审批人不能是发起人，
  也不能重复承担同一批次的多个节点；
- 任一节点拒绝/超时即终止批次；所有节点批准（或跳过）后 worker 才能执行；
- 批次保存提交时的策略快照，策略更新不改变已提交批次；
- 详情展示当前节点、剩余节点与超时状态；批准/拒绝/跳过/超时/策略变更都可审计；
- 重复操作与并发决定不会让批次越过门禁或产生第二次执行。
"""
from __future__ import annotations

import json
import sqlite3
import time

import pytest
from fastapi.testclient import TestClient

from app.config import Settings
from app.db import Database
from app.main import create_app
from app.security import sign

ACTIVE_SECRET = "new-secret"
OLD_SECRET = "old-secret"
FAR_FUTURE = "2099-01-01T00:00:00Z"

SUBMITTER = "ops-li"
LEAD = "ops-wang"
FINANCE = "ops-zhao"
SECURITY = "ops-sun"

# 两级串行：先运营主管、后财务控制员
SERIAL_RULES = [
    {"name": "high-serial", "risk_level": "high", "mode": "serial", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 1800},
        {"role": "finance-controller", "timeout_seconds": 3600},
    ]},
    {"name": "normal-none", "risk_level": "normal", "nodes": []},
]

# 两级并行：运营主管与安全同时待决
PARALLEL_RULES = [
    {"name": "high-parallel", "risk_level": "high", "mode": "parallel", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 1800},
        {"role": "security", "timeout_seconds": 3600},
    ]},
    {"name": "normal-none", "risk_level": "normal", "nodes": []},
]


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
    """投递并跑完正常处理管线（done），返回 delivery 行。"""
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
               "approval_note": "涉及资金类回调补发，需多级审批"}
    if external_id is not None:
        payload["external_id"] = external_id
    payload.update(kw)
    return submit(client, **payload)


def apply_policy(client, rules, operator="ops-policy"):
    r = client.post("/admin/replay-policies",
                    json={"operator": operator, "policy": {"rules": rules}})
    assert r.status_code == 200, r.json()
    return r.json()["version"]


def detail(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()


def approval(client, batch_id):
    return detail(client, batch_id)["batch"]["approval"]


def node_by_seq(client, batch_id, seq):
    return next(n for n in approval(client, batch_id)["nodes"] if n["seq"] == seq)


def events(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}/events").json()["events"]


def event_types(client, batch_id):
    return [e["type"] for e in events(client, batch_id)]


def task_for(client, batch_id, external_id):
    return next(t for t in detail(client, batch_id)["tasks"]
                if t["external_id"] == external_id)


def approve_node(client, batch_id, node_id, operator, role, note=""):
    return client.post(f"/admin/replays/{batch_id}/nodes/{node_id}/approve",
                       json={"operator": operator, "role": role, "note": note})


# ---- 策略版本管理 ---------------------------------------------------------------

def test_policy_submit_validate_and_version_history(client):
    # 无已生效策略时：当前为内置默认策略（高风险单审批人、普通直跑）
    cur = client.get("/admin/replay-policies/current").json()
    assert cur["version"] is None and cur["source"] == "builtin_default"
    assert cur["policy"]["rules"][0]["nodes"][0]["role"] == "any"

    # 非法策略：422 + 具体原因，当前策略不变，失败也落版本记录与审计
    bad = client.post("/admin/replay-policies", json={
        "operator": "ops-policy", "policy": {"rules": [
            {"risk_level": "high", "mode": "sideways",
             "nodes": [{"role": "", "timeout_seconds": -5}]},
            "not-an-object",
        ]}})
    assert bad.status_code == 422
    reasons = bad.json()["reasons"]
    assert any("mode" in r for r in reasons)
    assert any("role" in r for r in reasons)
    assert any("timeout_seconds" in r for r in reasons)
    assert any("must be an object" in r for r in reasons)
    assert client.get("/admin/replay-policies/current").json()["version"] is None

    # 缺 operator / 空规则 / 非 JSON 同样被拒绝
    assert client.post("/admin/replay-policies",
                       json={"policy": {"rules": SERIAL_RULES}}).status_code == 422
    assert client.post("/admin/replay-policies",
                       json={"operator": "ops-policy", "policy": {"rules": []}}
                       ).status_code == 422
    assert client.post("/admin/replay-policies",
                       content=b"not json").status_code == 422

    # 合法策略整份生效，版本号单调递增
    v1 = apply_policy(client, SERIAL_RULES)
    assert v1 == 1
    v2 = apply_policy(client, PARALLEL_RULES, operator="ops-policy-2")
    assert v2 == 2
    cur = client.get("/admin/replay-policies/current").json()
    assert cur["version"] == 2 and cur["source"] == "applied"
    assert cur["operator"] == "ops-policy-2"
    assert cur["policy"]["rules"][0]["mode"] == "parallel"

    # 每次变更/尝试都可查：applied + rejected 记录齐全
    versions = client.get("/admin/replay-policies/versions").json()["versions"]
    assert [v["result"] for v in versions[:2]] == ["applied", "applied"]
    rejected = [v for v in versions if v["result"] == "rejected"]
    assert len(rejected) == 4
    invalid_policy = next(v for v in rejected
                          if v["reason"] and any("mode" in r for r in v["reason"]))
    assert invalid_policy["operator"] == "ops-policy"
    assert any(v["reason"] == ["rules_must_be_non_empty_list"] for v in rejected)
    assert any(v["reason"] == ["body_must_be_valid_json"] for v in rejected)

    # 策略变更写审计事件
    all_events = client.get("/admin/events").json()["events"]
    applied = [e for e in all_events if e["type"] == "replay_policy_applied"]
    rejected_ev = [e for e in all_events if e["type"] == "replay_policy_rejected"]
    assert [e["detail"]["version"] for e in applied] == [2, 1]  # 新的在前
    assert len(rejected_ev) == 4


def test_policy_size_validation_rules(client):
    # min_size/max_size 合法性
    r = client.post("/admin/replay-policies", json={
        "operator": "ops-policy", "policy": {"rules": [
            {"risk_level": "high", "min_size": 10, "max_size": 2, "nodes": []}]}})
    assert r.status_code == 422
    assert any("min_size" in x for x in r.json()["reasons"])
    r = client.post("/admin/replay-policies", json={
        "operator": "ops-policy", "policy": {"rules": [
            {"risk_level": "weird", "nodes": []}]}})
    assert r.status_code == 422
    assert any("risk_level" in x for x in r.json()["reasons"])


# ---- 串行多级审批 ---------------------------------------------------------------

def test_serial_chain_full_flow_with_roles_and_audit(client):
    apply_policy(client, SERIAL_RULES)
    process_normally(client, "MS-1", b'{"order": 1}')
    r = submit_high(client, "MS-1")
    assert r.status_code == 201
    body = r.json()
    assert body["status"] == "pending_approval"
    assert body["policy_version"] == 1
    assert body["approval_nodes"] == 2
    batch_id = body["batch_id"]

    # 节点链：首节点 active（带截止时间），第二节点 waiting（尚未起算截止时间）
    ap = approval(client, batch_id)
    n0, n1 = ap["nodes"]
    assert (n0["seq"], n0["role"], n0["status"]) == (0, "ops-lead", "active")
    assert n0["deadline"] is not None and n0["activated_at"] is not None
    assert (n1["seq"], n1["role"], n1["status"]) == (1, "finance-controller", "waiting")
    assert n1["deadline"] is None and n1["activated_at"] is None
    # 详情展示当前节点与剩余节点
    assert ap["current_node_ids"] == [n0["id"]]
    assert ap["remaining_node_ids"] == [n0["id"], n1["id"]]
    assert ap["policy_version"] == 1
    # 批次保存提交时的策略快照
    snap = detail(client, batch_id)["batch"]["policy_snapshot"]
    assert snap["policy_version"] == 1 and snap["rule_name"] == "high-serial"
    assert snap["mode"] == "serial" and len(snap["nodes"]) == 2

    # 全部节点满足前 worker 不能领取
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MS-1")["status"] == "pending"
    assert task_for(client, batch_id, "MS-1")["blocked_reason"] == "awaiting_approval"

    # 尚未轮到的节点不能接受决定
    assert approve_node(client, batch_id, n1["id"], FINANCE,
                        "finance-controller").status_code == 409
    # 发起人不能审批自己的批次
    assert approve_node(client, batch_id, n0["id"], SUBMITTER,
                        "ops-lead").status_code == 403
    # 指定角色的节点：不带角色 422，角色不符 403
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/approve",
                       json={"operator": LEAD}).status_code == 422
    assert approve_node(client, batch_id, n0["id"], LEAD,
                        "finance-controller").status_code == 403

    # 第一级：ops-lead 角色批准 -> 第二节点激活并起算截止时间，批次仍待决
    r = approve_node(client, batch_id, n0["id"], LEAD, "ops-lead", note="现场已核对")
    assert r.status_code == 200
    assert r.json()["batch_status"] == "pending_approval"
    assert r.json()["remaining_node_ids"] == [n1["id"]]
    ap = approval(client, batch_id)
    n0v, n1v = ap["nodes"]
    assert n0v["status"] == "approved"
    assert n0v["decided_by"] == LEAD and n0v["decided_role"] == "ops-lead"
    assert n0v["decided_at"] is not None
    assert n1v["status"] == "active" and n1v["deadline"] is not None
    assert ap["current_node_ids"] == [n1["id"]]
    assert ap["remaining_node_ids"] == [n1["id"]]
    assert ap["deadline"] == n1v["deadline"]  # 批次截止时间跟随当前节点

    # 批次仍未放行
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MS-1")["status"] == "pending"

    # 同一审批人不能重复承担同一批次的多个节点
    assert approve_node(client, batch_id, n1["id"], LEAD,
                        "finance-controller").status_code == 403

    # 第二级：财务控制员批准 -> 全部节点满足，批次进入 running
    r = approve_node(client, batch_id, n1["id"], FINANCE, "finance-controller")
    assert r.status_code == 200
    assert r.json()["batch_status"] == "running"
    assert r.json()["remaining_node_ids"] == []
    ap = approval(client, batch_id)
    assert ap["status"] == "approved" and ap["approver"] == FINANCE
    assert ap["current_node_ids"] == [] and ap["remaining_node_ids"] == []

    # worker 现在可以执行；完整审计链：节点批准 x2 + 节点激活 + 批次放行 + 执行
    client.app.state.replay_worker.run_once()
    client.app.state.worker.run_once()
    assert task_for(client, batch_id, "MS-1")["status"] == "done"
    assert detail(client, batch_id)["batch"]["status"] == "completed"
    types = event_types(client, batch_id)
    assert types == [
        "replay_batch_created", "replay_task_blocked",  # 阻塞原因不变，轮询不重复刷
        "replay_approval_node_approved", "replay_approval_node_activated",
        "replay_approval_node_approved", "replay_batch_approved",
        "replay_task_processing", "replay_task_done",
        "replay_batch_completed", "effect_executed"]
    first = next(e for e in events(client, batch_id)
                 if e["type"] == "replay_approval_node_approved")
    assert first["detail"]["node_id"] == n0["id"]
    assert first["detail"]["role"] == "ops-lead"
    assert first["detail"]["operator"] == LEAD
    activated = next(e for e in events(client, batch_id)
                     if e["type"] == "replay_approval_node_activated")
    assert activated["detail"]["node_id"] == n1["id"]
    assert activated["detail"]["deadline"] is not None


def test_serial_second_node_deadline_starts_at_activation(client):
    """串行链后一节点的截止时间从激活时起算，超时同样终止批次。"""
    apply_policy(client, [
        {"name": "high-serial", "risk_level": "high", "mode": "serial", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 1800},
            {"role": "finance-controller", "timeout_seconds": 100},
        ]},
        {"name": "normal-none", "risk_level": "normal", "nodes": []},
    ])
    process_normally(client, "MS-T", b'{"order": 1}')
    batch_id = submit_high(client, "MS-T").json()["batch_id"]
    n0 = node_by_seq(client, batch_id, 0)
    approve_node(client, batch_id, n0["id"], LEAD, "ops-lead")
    n1 = node_by_seq(client, batch_id, 1)
    assert n1["status"] == "active"
    assert time.time() + 99 <= n1["deadline"] <= time.time() + 101

    # 第二级超时：worker 扫描释放整个批次
    worker = client.app.state.replay_worker
    worker.clock = lambda: n1["deadline"] + 1
    worker.run_once()
    d = detail(client, batch_id)
    assert d["batch"]["status"] == "cancelled"
    assert d["batch"]["approval"]["status"] == "expired"
    nodes = d["batch"]["approval"]["nodes"]
    assert nodes[0]["status"] == "approved"
    assert nodes[1]["status"] == "expired"
    assert all(t["status"] == "cancelled" for t in d["tasks"])
    expired = next(e for e in events(client, batch_id)
                   if e["type"] == "replay_approval_node_expired")
    assert expired["detail"]["node_id"] == n1["id"]
    assert expired["detail"]["role"] == "finance-controller"


# ---- 并行多级审批 ---------------------------------------------------------------

def test_parallel_nodes_all_must_approve(client):
    apply_policy(client, PARALLEL_RULES)
    process_normally(client, "MP-1", b'{"order": 1}')
    batch_id = submit_high(client, "MP-1").json()["batch_id"]

    # 并行：两个节点同时 active，各自带截止时间
    ap = approval(client, batch_id)
    n0, n1 = ap["nodes"]
    assert {n0["status"], n1["status"]} == {"active"}
    assert n0["deadline"] is not None and n1["deadline"] is not None
    assert ap["current_node_ids"] == [n0["id"], n1["id"]]
    assert ap["remaining_node_ids"] == [n0["id"], n1["id"]]
    assert ap["deadline"] == min(n0["deadline"], n1["deadline"])

    # 批次级兼容入口在多个节点待决时无法推断决定对象
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": LEAD, "role": "ops-lead"}).status_code == 422

    # 只批准一个：批次仍待决，worker 不能领取
    r = approve_node(client, batch_id, n0["id"], LEAD, "ops-lead")
    assert r.json()["batch_status"] == "pending_approval"
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MP-1")["status"] == "pending"
    ap = approval(client, batch_id)
    assert ap["current_node_ids"] == [n1["id"]]
    assert ap["deadline"] == n1["deadline"]  # 批次截止时间收敛到剩余节点

    # 同一人不能承担第二个节点；另一角色的人批准后全部满足
    assert approve_node(client, batch_id, n1["id"], LEAD, "security").status_code == 403
    r = approve_node(client, batch_id, n1["id"], SECURITY, "security")
    assert r.json()["batch_status"] == "running"
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MP-1")["status"] == "done"


def test_parallel_any_rejection_terminates_batch(client):
    apply_policy(client, PARALLEL_RULES)
    process_normally(client, "MP-R", b'{"order": 1}')
    batch_id = submit_high(client, "MP-R").json()["batch_id"]
    n0, n1 = approval(client, batch_id)["nodes"]

    # 一个节点批准后再被另一个节点拒绝：整个批次立即终止
    approve_node(client, batch_id, n0["id"], LEAD, "ops-lead")
    r = client.post(f"/admin/replays/{batch_id}/nodes/{n1['id']}/reject",
                    json={"operator": SECURITY, "role": "security",
                          "reason": "安全评估不通过", "note": "先冻结"})
    assert r.status_code == 200
    assert r.json()["cancelled_tasks"] == 1

    d = detail(client, batch_id)
    assert d["batch"]["status"] == "rejected"
    ap = d["batch"]["approval"]
    assert ap["status"] == "rejected"
    assert ap["rejection_reason"] == "安全评估不通过"
    nodes = ap["nodes"]
    assert nodes[0]["status"] == "approved"      # 已决定的记录保留
    assert nodes[1]["status"] == "rejected"
    assert nodes[1]["decided_by"] == SECURITY
    assert nodes[1]["decision_reason"] == "安全评估不通过"
    assert all(t["status"] == "cancelled" for t in d["tasks"])

    # 终态：worker 不领取，任何节点/批次决定都不再接受
    client.app.state.replay_worker.run_once()
    assert all(t["status"] == "cancelled" for t in detail(client, batch_id)["tasks"])
    assert approve_node(client, batch_id, n0["id"], FINANCE,
                        "ops-lead").status_code == 409
    types = event_types(client, batch_id)
    assert "replay_approval_node_rejected" in types
    assert "replay_batch_rejected" in types
    rejected = next(e for e in events(client, batch_id)
                    if e["type"] == "replay_approval_node_rejected")
    assert rejected["detail"]["reason"] == "安全评估不通过"

    # 占用释放：同内容可重新提交（走新的审批）
    assert submit_high(client, "MP-R").status_code == 201


def test_parallel_earliest_node_timeout_cancels_batch(client):
    """并行节点各自有截止时间：最早到期的节点超时即终止整个批次。"""
    apply_policy(client, [
        {"name": "high-parallel", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 100},
            {"role": "security", "timeout_seconds": 3600},
        ]},
        {"name": "normal-none", "risk_level": "normal", "nodes": []},
    ])
    process_normally(client, "MP-T", b'{"order": 1}')
    batch_id = submit_high(client, "MP-T").json()["batch_id"]
    n0, n1 = approval(client, batch_id)["nodes"]
    assert n0["deadline"] < n1["deadline"]

    worker = client.app.state.replay_worker
    worker.clock = lambda: n0["deadline"] + 1  # 只到第一个节点的截止
    worker.run_once()
    d = detail(client, batch_id)
    assert d["batch"]["status"] == "cancelled"
    assert d["batch"]["approval"]["status"] == "expired"
    nodes = d["batch"]["approval"]["nodes"]
    assert nodes[0]["status"] == "expired"
    assert nodes[1]["status"] == "cancelled"   # 未到期节点随批次关闭
    assert all(t["status"] == "cancelled" for t in d["tasks"])
    types = event_types(client, batch_id)
    assert "replay_approval_node_expired" in types
    assert "replay_batch_approval_expired" in types

    # 重复扫描不产生第二次效果
    worker.run_once()
    assert event_types(client, batch_id).count("replay_batch_approval_expired") == 1


# ---- 跳过 -----------------------------------------------------------------------

def test_skip_node_with_reason_counts_as_satisfied(client):
    apply_policy(client, SERIAL_RULES)
    process_normally(client, "MSK-1", b'{"order": 1}')
    batch_id = submit_high(client, "MSK-1").json()["batch_id"]
    n0 = node_by_seq(client, batch_id, 0)

    # 跳过原因必填
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/skip",
                       json={"operator": LEAD, "role": "ops-lead"}
                       ).status_code == 422
    # 发起人不能跳过自己的批次节点
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/skip",
                       json={"operator": SUBMITTER, "role": "ops-lead",
                             "reason": "x"}).status_code == 403

    # 有理由跳过：节点视为满足，串行链推进到下一节点
    r = client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/skip",
                    json={"operator": LEAD, "role": "ops-lead",
                          "reason": "主管休假，值班经理代签已电话确认"})
    assert r.status_code == 200
    assert r.json()["result"] == "skipped"
    assert r.json()["batch_status"] == "pending_approval"
    n0v = node_by_seq(client, batch_id, 0)
    assert n0v["status"] == "skipped"
    assert n0v["decided_by"] == LEAD
    assert n0v["decision_reason"] == "主管休假，值班经理代签已电话确认"
    n1 = node_by_seq(client, batch_id, 1)
    assert n1["status"] == "active"

    # 跳过的人不能再承担下一节点；另一人批准后批次放行
    assert approve_node(client, batch_id, n1["id"], LEAD,
                        "finance-controller").status_code == 403
    approve_node(client, batch_id, n1["id"], FINANCE, "finance-controller")
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MSK-1")["status"] == "done"

    skipped = next(e for e in events(client, batch_id)
                   if e["type"] == "replay_approval_node_skipped")
    assert skipped["detail"]["operator"] == LEAD
    assert skipped["detail"]["reason"] == "主管休假，值班经理代签已电话确认"
    assert skipped["detail"]["node_id"] == n0["id"]


# ---- 策略快照：更新不影响已提交批次 ----------------------------------------------

def test_policy_update_does_not_change_submitted_batches(client):
    apply_policy(client, SERIAL_RULES)  # v1：串行两级
    process_normally(client, "PS-1", b'{"order": 1}')
    process_normally(client, "PS-2", b'{"order": 2}')
    batch_a = submit_high(client, "PS-1").json()["batch_id"]

    # 策略升级：高风险改为单节点并行（ CTO 一言堂 ）
    v2 = apply_policy(client, [
        {"name": "high-single", "risk_level": "high", "mode": "serial",
         "nodes": [{"role": "cto", "timeout_seconds": 7200}]},
        {"name": "normal-none", "risk_level": "normal", "nodes": []},
    ])
    assert v2 == 2

    # 新批次按 v2 生成节点
    batch_b = submit_high(client, "PS-2").json()["batch_id"]
    ap_b = approval(client, batch_b)
    assert ap_b["policy_version"] == 2
    assert len(ap_b["nodes"]) == 1 and ap_b["nodes"][0]["role"] == "cto"

    # 已提交批次保持 v1 的节点链与快照：两个节点、串行、原角色
    ap_a = approval(client, batch_a)
    assert ap_a["policy_version"] == 1
    assert [n["role"] for n in ap_a["nodes"]] == ["ops-lead", "finance-controller"]
    snap = detail(client, batch_a)["batch"]["policy_snapshot"]
    assert snap["policy_version"] == 1 and snap["rule_name"] == "high-serial"

    # 已提交批次仍按 v1 的链审批：cto 角色对它无效，ops-lead 有效
    n0 = ap_a["nodes"][0]
    assert approve_node(client, batch_a, n0["id"], FINANCE, "cto").status_code == 403
    assert approve_node(client, batch_a, n0["id"], LEAD, "ops-lead").status_code == 200
    n1 = node_by_seq(client, batch_a, 1)
    assert n1["status"] == "active" and n1["role"] == "finance-controller"


# ---- 规模匹配与无规则兜底 ---------------------------------------------------------

def test_size_based_rules_and_normal_default_allow(client):
    apply_policy(client, [
        {"name": "big-batch", "risk_level": "any", "min_size": 2, "mode": "serial",
         "nodes": [{"role": "ops-lead", "timeout_seconds": 1800}]},
        {"name": "high-small", "risk_level": "high", "mode": "serial",
         "nodes": [{"role": "ops-lead", "timeout_seconds": 1800}]},
        # 普通小批次无规则匹配 -> 直接运行
    ])
    process_normally(client, "SZ-1", b'{"order": 1}')
    process_normally(client, "SZ-2", b'{"order": 2}')

    # 普通但规模大（2 条 >= min_size）：命中 any 规则，需要一级审批
    r = submit(client, status="done")
    assert r.status_code == 201
    assert r.json()["status"] == "pending_approval"
    assert r.json()["approval_nodes"] == 1
    big_batch = r.json()["batch_id"]
    snap = detail(client, big_batch)["batch"]["policy_snapshot"]
    assert snap["rule_name"] == "big-batch" and snap["batch_size"] == 2

    # 普通且规模小：无匹配规则 -> 直接运行
    r = submit(client, external_id="SZ-1")
    # SZ-1 被上一批占住，先拒绝上一批释放占用
    n0 = node_by_seq(client, big_batch, 0)
    client.post(f"/admin/replays/{big_batch}/nodes/{n0['id']}/reject",
                json={"operator": LEAD, "role": "ops-lead", "reason": "改期"})
    r = submit(client, external_id="SZ-1")
    assert r.status_code == 201
    assert r.json()["status"] == "running"
    assert r.json()["approval_nodes"] == 0


def test_high_risk_without_matching_rule_fails_closed(client):
    apply_policy(client, [
        {"name": "normal-none", "risk_level": "normal", "nodes": []},
    ])
    process_normally(client, "NH-1", b'{"order": 1}')
    # 已生效策略不覆盖高风险：拒绝提交，而不是静默降低审批要求
    r = submit_high(client, "NH-1")
    assert r.status_code == 422
    assert r.json()["error"] == "no_applicable_policy"
    assert client.get("/admin/replays").json()["batches"] == []
    # 普通批次不受影响
    assert submit(client, external_id="NH-1").status_code == 201


# ---- 超时状态展示 -----------------------------------------------------------------

def test_detail_shows_timeout_state(client):
    apply_policy(client, SERIAL_RULES)
    process_normally(client, "MT-1", b'{"order": 1}')
    batch_id = submit_high(client, "MT-1").json()["batch_id"]
    n0 = node_by_seq(client, batch_id, 0)

    ap = approval(client, batch_id)
    assert ap["expired_on_time"] is False
    assert ap["nodes"][0]["expired_on_time"] is False

    # 把首节点截止拨到过去（worker 尚未扫描）：详情提示已超时，但状态仍待决
    past = time.time() - 1
    with client.app.state.db.tx() as cur:
        cur.execute("UPDATE replay_approval_nodes SET deadline=? WHERE id=?",
                    (past, n0["id"]))
        cur.execute("UPDATE replay_batches SET approval_deadline=? WHERE id=?",
                    (past, batch_id))
    ap = approval(client, batch_id)
    assert ap["expired_on_time"] is True
    assert ap["nodes"][0]["expired_on_time"] is True
    assert ap["status"] == "pending"
    assert ap["current_node_ids"] == [n0["id"]]


# ---- 重复操作与并发决定 -------------------------------------------------------------

def test_duplicate_and_racing_decisions_have_no_second_effect(client):
    apply_policy(client, PARALLEL_RULES)
    process_normally(client, "MD-1", b'{"order": 1}')
    batch_id = submit_high(client, "MD-1", request_id="req-md-1").json()["batch_id"]
    n0, n1 = approval(client, batch_id)["nodes"]

    # 同一节点重复批准：第二次 409，只有一条节点批准事件
    assert approve_node(client, batch_id, n0["id"], LEAD, "ops-lead").status_code == 200
    assert approve_node(client, batch_id, n0["id"], FINANCE,
                        "ops-lead").status_code == 409
    assert approve_node(client, batch_id, n0["id"], LEAD, "ops-lead").status_code == 409
    assert event_types(client, batch_id).count("replay_approval_node_approved") == 1

    # 同一节点批准后再拒绝：409，批次不被推翻
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/reject",
                       json={"operator": SECURITY, "role": "ops-lead",
                             "reason": "late"}).status_code == 409
    assert detail(client, batch_id)["batch"]["status"] == "pending_approval"

    # 全部节点满足后：任何进一步决定都被 409 挡下
    assert approve_node(client, batch_id, n1["id"], SECURITY,
                        "security").status_code == 200
    assert detail(client, batch_id)["batch"]["status"] == "running"
    assert approve_node(client, batch_id, n1["id"], FINANCE,
                        "security").status_code == 409
    assert client.post(f"/admin/replays/{batch_id}/approve",
                       json={"operator": FINANCE}).status_code == 409
    assert event_types(client, batch_id).count("replay_batch_approved") == 1

    # request_id 重复提交返回原批次：不生成第二批任务与第二套节点
    dup = submit_high(client, "MD-1", request_id="req-md-1")
    assert dup.status_code == 200
    assert dup.json()["result"] == "duplicate"
    assert dup.json()["batch_id"] == batch_id
    assert len(client.get("/admin/replays").json()["batches"]) == 1
    db = client.app.state.db
    assert db.query_one("SELECT COUNT(*) AS c FROM replay_approval_nodes "
                        "WHERE batch_id=?", (batch_id,))["c"] == 2
    assert db.query_one("SELECT COUNT(*) AS c FROM replay_approval_nodes")["c"] == 2

    # 执行只发生一次
    client.app.state.replay_worker.run_once()
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "MD-1")["status"] == "done"
    assert event_types(client, batch_id).count("replay_task_done") == 1


def test_concurrent_decision_on_same_node_settles_once(client):
    """两人同时决定同一节点（写事务串行 + 条件更新）：只有一笔生效。"""
    apply_policy(client, SERIAL_RULES)
    process_normally(client, "MC-1", b'{"order": 1}')
    batch_id = submit_high(client, "MC-1").json()["batch_id"]
    n0 = node_by_seq(client, batch_id, 0)

    from app.replay import decide_node
    db = client.app.state.db
    r1 = decide_node(db, batch_id, n0["id"], "approve", LEAD, "ops-lead")
    assert r1["result"] == "approved"
    # 第二个决定到达时节点已非 active：409，不产生第二条决定事件
    with pytest.raises(Exception) as excinfo:
        decide_node(db, batch_id, n0["id"], "approve", FINANCE, "ops-lead")
    assert "409" in str(excinfo.value)
    assert event_types(client, batch_id).count("replay_approval_node_approved") == 1
    assert node_by_seq(client, batch_id, 0)["decided_by"] == LEAD


def test_node_endpoints_validate_input(client):
    apply_policy(client, SERIAL_RULES)
    process_normally(client, "MV-1", b'{"order": 1}')
    batch_id = submit_high(client, "MV-1").json()["batch_id"]
    n0 = node_by_seq(client, batch_id, 0)
    # 不存在的批次/节点
    assert client.post("/admin/replays/9999/nodes/1/approve",
                       json={"operator": LEAD, "role": "ops-lead"}).status_code == 404
    assert client.post(f"/admin/replays/{batch_id}/nodes/9999/approve",
                       json={"operator": LEAD, "role": "ops-lead"}).status_code == 404
    # 空审批人
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/approve",
                       json={"operator": "  ", "role": "ops-lead"}).status_code == 422
    # 空拒绝原因
    assert client.post(f"/admin/replays/{batch_id}/nodes/{n0['id']}/reject",
                       json={"operator": LEAD, "role": "ops-lead", "reason": " "}
                       ).status_code == 422


# ---- 老库迁移 -----------------------------------------------------------------

def test_old_pending_batch_migrated_with_default_node(tmp_path):
    """老库（上一版：有审批列、无策略列与节点表）中仍待决的批次：
    打开时补策略列并合成内置默认节点，之后可照常批准/拒绝/超时。"""
    db_path = str(tmp_path / "old.db")
    conn = sqlite3.connect(db_path)
    conn.executescript("""
    CREATE TABLE deliveries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        external_id TEXT NOT NULL, content_hash TEXT NOT NULL, payload TEXT NOT NULL,
        status TEXT NOT NULL, frozen INTEGER NOT NULL DEFAULT 0,
        attempts INTEGER NOT NULL DEFAULT 0,
        next_retry_at REAL, checkpoint TEXT, created_at REAL NOT NULL,
        updated_at REAL NOT NULL, UNIQUE (external_id, content_hash));
    CREATE TABLE replay_batches (
        id INTEGER PRIMARY KEY AUTOINCREMENT, request_id TEXT UNIQUE,
        operator TEXT NOT NULL, reason TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'running', filters TEXT NOT NULL DEFAULT '{}',
        max_concurrency INTEGER,
        risk_level TEXT NOT NULL DEFAULT 'normal', approval_note TEXT,
        approval_status TEXT NOT NULL DEFAULT 'not_required', approver TEXT,
        approval_reason TEXT, approved_at REAL, approval_deadline REAL,
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
                 " created_at, updated_at) VALUES ('OLD-1','h','{}','done',100,100)")
    conn.execute(
        "INSERT INTO replay_batches (operator, reason, status, risk_level,"
        " approval_status, approval_deadline, total, created_at, updated_at)"
        " VALUES ('ops-li','r','pending_approval','high','pending',3700,1,100,100)")
    conn.execute("INSERT INTO replay_tasks (batch_id, delivery_id, external_id,"
                 " operator, reason, created_at, updated_at)"
                 " VALUES (1,1,'OLD-1','ops-li','r',100,100)")
    conn.commit()
    conn.close()

    db = Database(db_path)
    # 补了策略列；待决批次合成了一个内置默认节点（沿用原截止时间）
    batch_cols = {r["name"] for r in db.query("PRAGMA table_info(replay_batches)")}
    assert {"policy_version", "policy_snapshot"} <= batch_cols
    node = db.query_one("SELECT * FROM replay_approval_nodes WHERE batch_id=1")
    assert node["seq"] == 0 and node["role"] == "any"
    assert node["status"] == "active"
    assert node["deadline"] == 3700
    db.close()

    # 通过完整应用走完审批：批次级兼容入口批准 -> 放行执行
    settings = Settings(database_path=db_path, keys_file=make_keys_file(tmp_path),
                        run_worker=False)
    app = create_app(settings)
    with TestClient(app) as c:
        ap = c.get("/admin/replays/1").json()["batch"]["approval"]
        assert ap["current_node_ids"] == [node["id"]]
        assert ap["nodes"][0]["role"] == "any"
        r = c.post("/admin/replays/1/approve", json={"operator": "ops-wang"})
        assert r.status_code == 200
        assert r.json() == {"result": "approved", "batch_id": 1, "status": "running"}
        c.app.state.replay_worker.run_once()
        d = c.get("/admin/replays/1").json()
        assert d["tasks"][0]["status"] == "done"
        assert d["batch"]["status"] == "completed"

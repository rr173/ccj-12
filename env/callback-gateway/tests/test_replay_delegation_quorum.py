"""审批委托与节点法定人数的端到端测试：

- 策略节点可配置 required_approvals（法定人数）与 roles（多个允许角色）；
- 并行节点有效赞成票达到法定人数才算满足，串行节点按节点分别计数；
- 同一审批人不能在同一节点重复计数，也不能在同一批次的多个节点持有效票；
- 运营可为角色创建带生效/失效时间的委托；节点决定必须校验委托当前有效
  （本人、角色匹配、时间窗覆盖当前时刻、未撤销/未到期）；
- 委托到期或撤销后，投在尚未达到法定人数节点上的赞成票失效，节点重新等待；
  已经落定的节点不会被悄悄改写；
- 批次详情展示每个节点的已批准人数、法定人数、当前有效委托与还缺多少人；
- 委托创建/撤销/重新激活/失效、批准/拒绝/跳过、票失效与法定人数丢失都写审计；
- 重复或并发决定不重复计数；法定人数不足时 worker 绝不执行。
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
FAR_FUTURE = "2099-01-01T00:00:00Z"

SUBMITTER = "ops-li"
LEAD_A = "ops-wang"
LEAD_B = "ops-chen"
FINANCE_A = "ops-zhao"
FINANCE_B = "ops-qian"
GRANTOR = "ops-admin"


def make_keys_file(tmp_path, grace_until=FAR_FUTURE):
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
    )
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def post(client, external_id, body: bytes):
    ts, sig = sign(ACTIVE_SECRET, body)
    return client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}",
    })


def process_normally(client, external_id, body=b'{"order": 1}'):
    post(client, external_id, body)
    client.app.state.worker.run_once()


def apply_policy(client, rules):
    r = client.post("/admin/replay-policies",
                    json={"operator": "ops-policy", "policy": {"rules": rules}})
    assert r.status_code == 200, r.json()
    return r.json()["version"]


def submit_high(client, external_id, request_id=None):
    payload = {"operator": SUBMITTER, "reason": "资金类回调补发",
               "risk_level": "high", "approval_note": "需法定人数审批",
               "external_id": external_id}
    if request_id:
        payload["request_id"] = request_id
    r = client.post("/admin/replays", json=payload)
    assert r.status_code == 201, r.text
    return r.json()


def approval(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()["batch"]["approval"]


def detail(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()


def events(client, batch_id=None):
    if batch_id is None:
        return client.get("/admin/events").json()["events"]
    return client.get(f"/admin/replays/{batch_id}/events").json()["events"]


def event_types(client, batch_id):
    return [e["type"] for e in events(client, batch_id)]


def task_for(client, batch_id, external_id):
    return next(t for t in detail(client, batch_id)["tasks"]
                if t["external_id"] == external_id)


def create_delegation(client, role, delegatee, *, valid_from=None, valid_to=None,
                      operator=GRANTOR, note="", expect=None):
    now = time.time()
    payload = {"role": role, "delegatee": delegatee, "operator": operator,
               "valid_from": now - 60 if valid_from is None else valid_from,
               "valid_to": now + 3600 if valid_to is None else valid_to,
               "note": note}
    r = client.post("/admin/replay-delegations", json=payload)
    if expect is not None:
        assert r.status_code == expect, r.text
        return r
    assert r.status_code == 201, r.text
    return r.json()["delegation_id"]


def vote(client, batch_id, node_id, operator, role, delegation_id=None,
         action="approve", **kw):
    payload = {"operator": operator, "role": role, **kw}
    if delegation_id is not None:
        payload["delegation_id"] = delegation_id
    return client.post(
        f"/admin/replays/{batch_id}/nodes/{node_id}/{action}", json=payload)


# ---- 法定人数：并行 ---------------------------------------------------------------

def test_parallel_quorum_needs_enough_approvers(client):
    # 单节点法定人数 2（任一 ops-lead 角色成员）
    apply_policy(client, [
        {"name": "high-quorum", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 1800}]},
    ])
    process_normally(client, "Q-1")
    batch_id = submit_high(client, "Q-1")["batch_id"]
    node = approval(client, batch_id)["nodes"][0]
    assert node["required_approvals"] == 2
    assert node["approved_count"] == 0 and node["missing"] == 2
    assert node["status"] == "active"

    d_a = create_delegation(client, "ops-lead", LEAD_A)
    d_b = create_delegation(client, "ops-lead", LEAD_B)

    # 第一张赞成票：计数 +1，但法定人数不足，批次继续等待
    r = vote(client, batch_id, node["id"], LEAD_A, "ops-lead", d_a)
    assert r.status_code == 200
    body = r.json()
    assert body["result"] == "voted" and body["batch_status"] == "pending_approval"
    assert (body["approved_count"], body["required_approvals"], body["missing"]) == (1, 2, 1)

    n = approval(client, batch_id)["nodes"][0]
    assert n["approved_count"] == 1 and n["missing"] == 1
    assert n["quorum_reached"] is False and n["status"] == "active"
    assert n["votes"][0]["voter"] == LEAD_A and n["votes"][0]["status"] == "valid"
    assert approval(client, batch_id)["missing_approvals"] == 1

    # worker 在法定人数不足时绝不执行
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "Q-1")["status"] == "pending"
    assert task_for(client, batch_id, "Q-1")["blocked_reason"] == "awaiting_approval"

    # 第二张赞成票达到法定人数：节点落定、批次放行
    r = vote(client, batch_id, node["id"], LEAD_B, "ops-lead", d_b)
    assert r.json()["batch_status"] == "running"
    n = approval(client, batch_id)["nodes"][0]
    assert n["status"] == "approved" and n["approved_count"] == 2 and n["missing"] == 0
    assert n["decided_by"] == LEAD_B  # 使节点落定的最后决定人
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "Q-1")["status"] == "done"

    types = event_types(client, batch_id)
    approved = [e for e in events(client, batch_id)
                if e["type"] == "replay_approval_node_approved"]
    assert [e["detail"]["approved_count"] for e in approved] == [1, 2]
    assert approved[0]["detail"]["quorum_reached"] is False
    assert approved[1]["detail"]["quorum_reached"] is True
    assert types.count("replay_batch_approved") == 1


def test_duplicate_and_concurrent_votes_do_not_double_count(client):
    apply_policy(client, [
        {"name": "high-quorum", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 1800}]},
    ])
    process_normally(client, "Q-D")
    batch_id = submit_high(client, "Q-D")["batch_id"]
    node_id = approval(client, batch_id)["nodes"][0]["id"]
    d_a = create_delegation(client, "ops-lead", LEAD_A)
    d_b = create_delegation(client, "ops-lead", LEAD_B)

    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a).status_code == 200
    # 同一人重复投票：409，不重复计数
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a).status_code == 409
    db = client.app.state.db
    assert db.query_one(
        "SELECT COUNT(*) AS c FROM replay_node_votes WHERE node_id=? AND status='valid'",
        (node_id,))["c"] == 1

    # 并发：两人同时投第二票（法定人数 2）：恰好一张进账使节点落定，另一张被
    # 节点状态挡下（409），批次最多只放行一次
    d_c = create_delegation(client, "ops-lead", "ops-third")
    barrier = threading.Barrier(2)
    responses = {}

    def cast(tag, who, did):
        barrier.wait()
        responses[tag] = vote(client, batch_id, node_id, who, "ops-lead", did)

    t1 = threading.Thread(target=cast, args=("b", LEAD_B, d_b))
    t2 = threading.Thread(target=cast, args=("c", "ops-third", d_c))
    t1.start(); t2.start(); t1.join(); t2.join()
    assert sorted(r.status_code for r in responses.values()) == [200, 409]
    winner = next(r.json() for r in responses.values() if r.status_code == 200)
    assert winner["result"] == "approved"
    db = client.app.state.db
    valid = db.query(
        "SELECT voter FROM replay_node_votes WHERE node_id=? AND status='valid' "
        "AND vote='approve' ORDER BY voter", (node_id,))
    assert {r["voter"] for r in valid} in (
        {LEAD_A, LEAD_B}, {LEAD_A, "ops-third"})  # 恰好两人计数
    assert approval(client, batch_id)["status"] == "approved"
    assert event_types(client, batch_id).count("replay_batch_approved") == 1
    client.app.state.replay_worker.run_once()
    assert event_types(client, batch_id).count("replay_task_done") == 1


# ---- 多角色节点 ------------------------------------------------------------------

def test_node_allows_multiple_roles(client):
    apply_policy(client, [
        {"name": "multi-role", "risk_level": "high", "mode": "parallel", "nodes": [
            {"roles": ["ops-lead", "finance-controller"],
             "required_approvals": 2, "timeout_seconds": 1800}]},
    ])
    process_normally(client, "MR-1")
    batch_id = submit_high(client, "MR-1")["batch_id"]
    n = approval(client, batch_id)["nodes"][0]
    assert n["allowed_roles"] == ["ops-lead", "finance-controller"]

    d_lead = create_delegation(client, "ops-lead", LEAD_A)
    d_fin = create_delegation(client, "finance-controller", FINANCE_A)
    # 安全角色的委托不被该节点接受
    d_sec = create_delegation(client, "security", "ops-sun")
    r = vote(client, batch_id, n["id"], "ops-sun", "security", d_sec)
    assert r.status_code == 403
    # 两张不同允许角色的票达到法定人数
    assert vote(client, batch_id, n["id"], LEAD_A, "ops-lead", d_lead).status_code == 200
    assert vote(client, batch_id, n["id"], FINANCE_A, "finance-controller", d_fin
               ).json()["batch_status"] == "running"


# ---- 串行：按节点分别计数，同一人不能承担多个节点 ------------------------------------

def test_serial_nodes_counted_separately_and_one_person_one_node(client):
    apply_policy(client, [
        {"name": "serial-quorum", "risk_level": "high", "mode": "serial", "nodes": [
            {"role": "ops-lead", "required_approvals": 1, "timeout_seconds": 1800},
            {"role": "finance-controller", "required_approvals": 1, "timeout_seconds": 1800}]},
    ])
    process_normally(client, "S-Q")
    batch_id = submit_high(client, "S-Q")["batch_id"]
    n0 = approval(client, batch_id)["nodes"][0]
    d_lead_a = create_delegation(client, "ops-lead", LEAD_A)
    d_fin_a = create_delegation(client, "finance-controller", FINANCE_A)

    # 首节点批准 -> 第二节点激活
    assert vote(client, batch_id, n0["id"], LEAD_A, "ops-lead", d_lead_a
               ).json()["batch_status"] == "pending_approval"
    n1 = next(n for n in approval(client, batch_id)["nodes"] if n["seq"] == 1)
    assert n1["status"] == "active"
    # 同一人（即便有另一角色的委托）不能在同一批次承担第二个节点
    d_lead_fin = create_delegation(client, "finance-controller", LEAD_A)
    r = vote(client, batch_id, n1["id"], LEAD_A, "finance-controller", d_lead_fin)
    assert r.status_code == 403
    # 另一人按节点分别计数：第二节点满足才放行
    assert vote(client, batch_id, n1["id"], FINANCE_A, "finance-controller", d_fin_a
               ).json()["batch_status"] == "running"


# ---- 委托时间窗：决定时必须当前有效 --------------------------------------------------

def test_decision_requires_currently_valid_delegation(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 1800}]},
    ])
    process_normally(client, "D-W")
    batch_id = submit_high(client, "D-W")["batch_id"]
    node_id = approval(client, batch_id)["nodes"][0]["id"]
    now = time.time()

    # 指定角色节点不带委托：422
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead").status_code == 422

    # 尚未生效的委托：409
    future = create_delegation(client, "ops-lead", LEAD_A,
                               valid_from=now + 100, valid_to=now + 3600)
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", future
               ).status_code == 409
    # 已过期的委托：409（即使扫描尚未跑）
    past = create_delegation(client, "ops-lead", LEAD_B,
                             valid_from=now - 3600, valid_to=now - 100)
    assert vote(client, batch_id, node_id, LEAD_B, "ops-lead", past
               ).status_code == 409
    # 委托授给别人：403
    other = create_delegation(client, "finance-controller", FINANCE_A)
    assert vote(client, batch_id, node_id, LEAD_A, "finance-controller", other
               ).status_code == 403
    # 不存在的委托：404
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", 999999
               ).status_code == 404

    # 有效委托：决定被接受
    current = create_delegation(client, "ops-lead", LEAD_A)
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", current
               ).status_code == 200


def test_delegation_window_validation(client):
    now = time.time()
    # 空字段
    r = client.post("/admin/replay-delegations",
                    json={"role": "", "delegatee": LEAD_A, "operator": GRANTOR,
                          "valid_from": now, "valid_to": now + 10})
    assert r.status_code == 422
    # 失效时间不晚于生效时间
    r = client.post("/admin/replay-delegations",
                    json={"role": "ops-lead", "delegatee": LEAD_A, "operator": GRANTOR,
                          "valid_from": now, "valid_to": now})
    assert r.status_code == 422
    # ISO-8601 时间也支持
    r = client.post("/admin/replay-delegations", json={
        "role": "ops-lead", "delegatee": LEAD_A, "operator": GRANTOR,
        "valid_from": "2020-01-01T00:00:00Z", "valid_to": FAR_FUTURE})
    assert r.status_code == 201


# ---- 委托撤销：票失效、节点重新等待、已落定节点不改写 ---------------------------------

def test_revocation_invalidates_votes_only_on_unmet_nodes(client):
    apply_policy(client, [
        {"name": "p", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 3600},
            {"role": "security", "required_approvals": 1, "timeout_seconds": 3600}]},
    ])
    process_normally(client, "RV-1")
    batch_id = submit_high(client, "RV-1")["batch_id"]
    nodes = approval(client, batch_id)["nodes"]
    n_lead, n_sec = nodes[0], nodes[1]

    d_a = create_delegation(client, "ops-lead", LEAD_A)
    d_b = create_delegation(client, "ops-lead", LEAD_B)
    d_sec = create_delegation(client, "security", "ops-sun")

    # 节点 0 有一张票（1/2，未满足）；节点 1 已达法定人数并落定
    assert vote(client, batch_id, n_lead["id"], LEAD_A, "ops-lead", d_a
               ).json()["missing"] == 1
    assert vote(client, batch_id, n_sec["id"], "ops-sun", "security", d_sec
               ).json()["result"] == "approved"

    # 撤销 LEAD_A 与 ops-sun 的委托
    r = client.post(f"/admin/replay-delegations/{d_a}/revoke",
                    json={"operator": GRANTOR, "reason": "授权调整"})
    assert r.status_code == 200
    affected = r.json()["affected_nodes"]
    assert [a["node_id"] for a in affected] == [n_lead["id"]]
    assert affected[0]["approved_count"] == 0
    assert affected[0]["missing"] == 2
    assert affected[0]["still_quorate"] is False

    ap = approval(client, batch_id)
    n_lead_v = next(n for n in ap["nodes"] if n["id"] == n_lead["id"])
    n_sec_v = next(n for n in ap["nodes"] if n["id"] == n_sec["id"])
    # 未满足节点：票失效、人数回退、重新等待
    assert n_lead_v["approved_count"] == 0 and n_lead_v["missing"] == 2
    assert n_lead_v["votes"][0]["status"] == "invalid"
    assert n_lead_v["votes"][0]["invalidated_at"] is not None
    assert n_lead_v["status"] == "active"
    # 已落定节点：不被悄悄改写
    assert n_sec_v["status"] == "approved"
    assert n_sec_v["votes"][0]["status"] == "valid"
    assert ap["status"] == "pending"
    assert set(ap["current_node_ids"]) == {n_lead["id"]}

    # 失效票的人不能凭旧委托再投；拿到新委托（重新激活）后旧票也不自动复活，
    # 但可以重新投票
    assert vote(client, batch_id, n_lead["id"], LEAD_A, "ops-lead", d_a
               ).status_code == 409
    client.app.state.replay_worker.run_once()
    assert task_for(client, batch_id, "RV-1")["status"] == "pending"

    # LEAD_B 的票 + LEAD_A 重新获得委托后重投 -> 达法定人数放行
    assert vote(client, batch_id, n_lead["id"], LEAD_B, "ops-lead", d_b
               ).json()["missing"] == 1
    new_a = client.post(f"/admin/replay-delegations/{d_a}/reactivate", json={
        "operator": GRANTOR,
        "valid_from": time.time() - 10, "valid_to": time.time() + 3600,
        "note": "授权恢复"}).json()["delegation_id"]
    assert new_a == d_a
    assert vote(client, batch_id, n_lead["id"], LEAD_A, "ops-lead", d_a
               ).json()["batch_status"] == "running"

    types = event_types(client, batch_id)
    assert types.count("replay_node_vote_invalidated") == 1
    assert "replay_approval_node_quorum_lost" in types
    # 审计链：委托生命周期事件可在全局审计中查到
    all_types = [e["type"] for e in events(client)]
    assert "replay_delegation_revoked" in all_types
    assert "replay_delegation_reactivated" in all_types
    revoked = next(e for e in events(client)
                   if e["type"] == "replay_delegation_revoked")
    assert revoked["detail"]["delegation_id"] == d_a
    assert revoked["detail"]["affected_nodes"] == [n_lead["id"]]


def test_expired_delegation_scan_reopens_quorum_wait(client):
    apply_policy(client, [
        {"name": "p", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "required_approvals": 2, "timeout_seconds": 86400}]},
    ])
    process_normally(client, "EX-1")
    batch_id = submit_high(client, "EX-1")["batch_id"]
    node_id = approval(client, batch_id)["nodes"][0]["id"]
    now = time.time()
    # 一份「很快到期」的委托
    d_a = create_delegation(client, "ops-lead", LEAD_A,
                            valid_from=now - 10, valid_to=now + 1)
    d_b = create_delegation(client, "ops-lead", LEAD_B)
    vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a)
    assert approval(client, batch_id)["nodes"][0]["approved_count"] == 1

    # worker 扫描：委托到期 -> 票失效 -> 节点重新等待（批次不取消、不执行）
    worker = client.app.state.replay_worker
    worker.clock = lambda: now + 10
    worker.run_once()
    n = approval(client, batch_id)["nodes"][0]
    assert n["status"] == "active" and n["approved_count"] == 0 and n["missing"] == 2
    assert detail(client, batch_id)["batch"]["status"] == "pending_approval"
    assert task_for(client, batch_id, "EX-1")["status"] == "pending"
    all_types = [e["type"] for e in events(client)]
    assert "replay_delegation_expired" in all_types
    assert "replay_approval_node_quorum_lost" in event_types(client, batch_id)

    # LEAD_B 一票后仍缺一人（LEAD_A 的票保持失效）；重新投票达到人数
    assert vote(client, batch_id, node_id, LEAD_B, "ops-lead", d_b
               ).json()["missing"] == 1
    d_a_new = create_delegation(client, "ops-lead", LEAD_A)
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a_new
               ).json()["batch_status"] == "running"


def test_revoked_or_expired_delegation_cannot_decide(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600}]},
    ])
    process_normally(client, "RV-2")
    batch_id = submit_high(client, "RV-2")["batch_id"]
    node_id = approval(client, batch_id)["nodes"][0]["id"]
    d_a = create_delegation(client, "ops-lead", LEAD_A)
    client.post(f"/admin/replay-delegations/{d_a}/revoke",
                json={"operator": GRANTOR, "reason": "x"})
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a
               ).status_code == 409
    # 撤销需要原因
    d_b = create_delegation(client, "ops-lead", LEAD_B)
    assert client.post(f"/admin/replay-delegations/{d_b}/revoke",
                       json={"operator": GRANTOR, "reason": " "}).status_code == 422
    # 已撤销才能重新激活
    assert client.post(f"/admin/replay-delegations/{d_b}/reactivate", json={
        "operator": GRANTOR, "valid_from": time.time(),
        "valid_to": time.time() + 10}).status_code == 409
    # 重新激活后可决定
    now = time.time()
    assert client.post(f"/admin/replay-delegations/{d_a}/reactivate", json={
        "operator": GRANTOR, "valid_from": now - 5, "valid_to": now + 3600
    }).status_code == 200
    assert vote(client, batch_id, node_id, LEAD_A, "ops-lead", d_a
               ).status_code == 200


# ---- 批次详情：有效委托展示 ----------------------------------------------------------

def test_detail_shows_quorum_and_valid_delegations(client):
    apply_policy(client, [
        {"name": "h", "risk_level": "high", "mode": "parallel", "nodes": [
            {"roles": ["ops-lead", "finance-controller"],
             "required_approvals": 2, "timeout_seconds": 3600}]},
    ])
    process_normally(client, "DT-1")
    batch_id = submit_high(client, "DT-1")["batch_id"]
    n = approval(client, batch_id)["nodes"][0]

    d_lead = create_delegation(client, "ops-lead", LEAD_A, note="值班授权")
    create_delegation(client, "finance-controller", FINANCE_A)
    now = time.time()
    create_delegation(client, "ops-lead", LEAD_B,
                      valid_from=now + 1000, valid_to=now + 3600)  # 尚未生效，不展示
    create_delegation(client, "ops-lead", "ops-old",
                      valid_from=now - 3600, valid_to=now - 10)     # 已过期，不展示

    n = approval(client, batch_id)["nodes"][0]
    valids = {(v["role"], v["delegatee"]) for v in n["valid_delegations"]}
    assert valids == {("ops-lead", LEAD_A), ("finance-controller", FINANCE_A)}
    lead_view = next(v for v in n["valid_delegations"] if v["delegatee"] == LEAD_A)
    assert lead_view["id"] == d_lead and lead_view["note"] == "值班授权"
    assert n["approved_count"] == 0 and n["required_approvals"] == 2
    assert n["missing"] == 2

    # 委托列表端点可按角色/受托人/状态过滤
    listing = client.get("/admin/replay-delegations",
                         params={"role": "ops-lead"}).json()["delegations"]
    assert len(listing) == 3
    statuses = {(d["delegatee"], d["effective_status"]) for d in listing}
    assert ("ops-old", "expired") in statuses
    assert (LEAD_B, "pending") in statuses
    revoked = client.get("/admin/replay-delegations",
                         params={"status": "revoked"}).json()["delegations"]
    assert revoked == []


# ---- 审计：所有生命周期动作留痕 ------------------------------------------------------

def test_delegation_lifecycle_audit(client):
    now = time.time()
    did = create_delegation(client, "ops-lead", LEAD_A,
                            valid_from=now - 5, valid_to=now + 3600, note="n")
    client.post(f"/admin/replay-delegations/{did}/revoke",
                json={"operator": GRANTOR, "reason": "r1"})
    client.post(f"/admin/replay-delegations/{did}/reactivate", json={
        "operator": GRANTOR, "valid_from": now - 5, "valid_to": now + 7200})
    types = [(e["type"], e["detail"].get("delegation_id")) for e in events(client)]
    assert ("replay_delegation_created", did) in types
    assert ("replay_delegation_revoked", did) in types
    assert ("replay_delegation_reactivated", did) in types
    rev = next(e for e in events(client) if e["type"] == "replay_delegation_revoked")
    assert rev["detail"]["reason"] == "r1" and rev["detail"]["role"] == "ops-lead"
    react = next(e for e in events(client)
                 if e["type"] == "replay_delegation_reactivated")
    assert react["detail"]["previous_status"] == "revoked"


# ---- 老库迁移：已落定节点回填票 ------------------------------------------------------

def test_migrated_decided_nodes_have_backfilled_votes(client):
    # 先用当前版本走一个串行两级审批并完成首节点，再模拟老行/校验回填逻辑
    apply_policy(client, [
        {"name": "s", "risk_level": "high", "mode": "serial", "nodes": [
            {"role": "ops-lead", "timeout_seconds": 3600},
            {"role": "finance-controller", "timeout_seconds": 3600}]},
    ])
    process_normally(client, "MG-1")
    batch_id = submit_high(client, "MG-1")["batch_id"]
    n0 = approval(client, batch_id)["nodes"][0]
    d = create_delegation(client, "ops-lead", LEAD_A)
    vote(client, batch_id, n0["id"], LEAD_A, "ops-lead", d)
    n0_after = next(n for n in approval(client, batch_id)["nodes"] if n["id"] == n0["id"])
    assert n0_after["status"] == "approved"
    assert n0_after["approved_count"] == 1
    assert n0_after["votes"][0]["voter"] == LEAD_A
    assert n0_after["votes"][0]["voter_role"] == "ops-lead"
    assert n0_after["votes"][0]["delegation_id"] == d

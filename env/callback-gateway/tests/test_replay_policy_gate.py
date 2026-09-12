"""策略变更的影响预览与审批门禁的端到端测试：

- 影响预览只读：带策略版本与生成时间，不改变线上配置；按当前批次规模与风险等级
  给出受影响范围、预计命中比例（以历史批次为样本）、审批节点变化与不兼容规则；
- 高风险变更必须由不同于提交人的运营审批后才能执行；审批期间旧稳定版本继续服务；
- 拒绝、超时、重复提交、并发审批都只是变更单状态转移，不产生部分生效；
- 基线被其他变更推进过的变更单拒绝执行（stale），不会叠加到新版线上状态上；
- 发布完成后可查变更前后版本、影响预览、审批决定与审计记录；
- 已提交的重放批次继续持有原策略快照，不受变更影响。
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
APPROVER = "ops-wang"

# 稳定策略：高风险单节点 ops-lead
STABLE_RULES = [
    {"name": "h-stable", "risk_level": "high", "mode": "serial", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 3600}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 高风险变更：high 改为并行双节点
NEW_HIGH_RULES = [
    {"name": "h-new", "risk_level": "high", "mode": "parallel", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 1800},
        {"role": "security", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 标准变更：只给 normal 大批（>=3）加审批（增强，不触及 high）
NORMAL_ONLY_RULES = [
    {"name": "h-stable", "risk_level": "high", "mode": "serial", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 3600}]},
    {"name": "n-big", "risk_level": "normal", "min_size": 3, "mode": "serial",
     "nodes": [{"role": "ops-lead", "timeout_seconds": 600}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 高风险缺口：只覆盖 >=5，小规模 high 无规则（fail closed）
HIGH_GAP_RULES = [
    {"name": "h-big", "risk_level": "high", "min_size": 5, "mode": "serial",
     "nodes": [{"role": "ops-lead", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 被遮蔽的无效规则：h-dup 的整个区间都被 h-all 先匹配
SHADOWED_RULES = [
    {"name": "h-all", "risk_level": "high", "mode": "serial",
     "nodes": [{"role": "ops-lead", "timeout_seconds": 1800}]},
    {"name": "h-dup", "risk_level": "high", "min_size": 3, "mode": "serial",
     "nodes": [{"role": "security", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 候选策略（灰度发布用）：高风险并行双节点
CANDIDATE_RULES = [
    {"name": "h-cand", "risk_level": "high", "mode": "parallel", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 1800},
        {"role": "security", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]


def make_client(tmp_path, ttl=3600.0):
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    settings = Settings(database_path=str(tmp_path / "gateway.db"),
                        keys_file=str(keys), run_worker=False,
                        replay_policy_change_ttl_seconds=ttl)
    return TestClient(create_app(settings))


@pytest.fixture()
def client(tmp_path):
    with make_client(tmp_path) as c:
        yield c


def apply_policy(client, rules, operator="ops-policy"):
    r = client.post("/admin/replay-policies",
                    json={"operator": operator, "policy": {"rules": rules}})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def post_done(client, external_id, body=b'{"order":1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    r = client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})
    assert r.status_code in (200, 202), r.text
    client.app.state.worker.run_once()


def submit_batch(client, external_id, risk_level="high", request_id=None, **kw):
    payload = {"operator": SUBMITTER, "reason": "变更验证",
               "risk_level": risk_level, "external_id": external_id}
    if risk_level == "high":
        payload["approval_note"] = "涉及资金类回调补发"
    if request_id is not None:
        payload["request_id"] = request_id
    payload.update(kw)
    return client.post("/admin/replays", json=payload)


def create_change(client, rules=None, operator=SUBMITTER, **kw):
    payload = {"operator": operator}
    if rules is not None:
        payload["policy"] = {"rules": rules}
    payload.update(kw)
    return client.post("/admin/replay-policies/changes", json=payload)


def get_change(client, change_id):
    return client.get(f"/admin/replay-policies/changes/{change_id}").json()


def approve_change(client, change_id, operator=APPROVER):
    return client.post(f"/admin/replay-policies/changes/{change_id}/approve",
                       json={"operator": operator})


def apply_change(client, change_id, operator=SUBMITTER):
    return client.post(f"/admin/replay-policies/changes/{change_id}/apply",
                       json={"operator": operator})


def current_version(client):
    return client.get("/admin/replay-policies/current").json()["version"]


def versions(client):
    return client.get("/admin/replay-policies/versions").json()["versions"]


# ---- 影响预览 ------------------------------------------------------------------

def test_preview_is_read_only_and_carries_version_and_time(client):
    v1 = apply_policy(client, STABLE_RULES)
    before_versions = len(versions(client))

    r = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": NEW_HIGH_RULES}})
    assert r.status_code == 200, r.text
    p = r.json()
    # 预览必须带策略版本与生成时间
    assert p["generated_at"] > 0
    assert p["policy_version"] == v1 + 1 and p["version_status"] == "expected"
    assert p["base_version"] == v1
    assert p["base_stable_versions"] == {"high": v1, "normal": v1}
    # 高风险变更分类
    assert p["change_type"] == "apply_policy"
    assert p["risk_class"] == "high" and p["requires_approval"] is True
    # 受影响范围 + 审批节点变化（high 全规模区间：单节点串行 -> 双节点并行）
    scope = [s for s in p["affected_scope"] if s["risk_level"] == "high"]
    assert len(scope) == 1
    assert scope[0]["before_rule"] == "h-stable" and scope[0]["after_rule"] == "h-new"
    assert scope[0]["change"] == "approval_chain_modified"
    assert [n["role"] for n in scope[0]["approval_nodes"]["before"]] == ["ops-lead"]
    assert [n["role"] for n in scope[0]["approval_nodes"]["after"]] == \
        ["ops-lead", "security"]
    # 无历史批次：命中比例无样本
    assert p["estimated_hit"]["basis"] == "no_history"
    assert p["estimated_hit"]["estimated_hit_ratio"] is None

    # 预览不改变线上配置：当前版本、版本历史、稳定指针都不变
    assert current_version(client) == v1
    assert len(versions(client)) == before_versions
    rollout = client.get("/admin/replay-policies/current").json()["rollout"]
    assert rollout["risk_levels"]["high"]["stable_version"] == v1


def test_preview_estimated_hit_ratio_uses_batch_history(client):
    apply_policy(client, STABLE_RULES)
    for ext in ("e1", "e2", "e3"):
        post_done(client, ext)
    # 两个高风险批 + 一个普通批（当前策略下的规模分布）
    assert submit_batch(client, "e1").status_code == 201
    assert submit_batch(client, "e2").status_code == 201
    assert submit_batch(client, "e3", risk_level="normal").status_code == 201

    p = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": NEW_HIGH_RULES}}).json()
    hit = p["estimated_hit"]
    assert hit["basis"] == "replay_batches_history"
    assert hit["sampled_batches"] == 3
    # high 规则变化影响两个 high 批；normal 规则不变
    assert hit["affected_batches"] == 2
    assert hit["estimated_hit_ratio"] == pytest.approx(2 / 3)
    assert hit["by_risk_level"]["high"]["estimated_hit_ratio"] == 1.0
    assert hit["by_risk_level"]["normal"]["estimated_hit_ratio"] == 0.0


def test_preview_reports_incompatible_rules(client):
    apply_policy(client, STABLE_RULES)
    # 高风险 fail-closed 缺口：规模 1-4 无规则
    p = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": HIGH_GAP_RULES}}).json()
    gaps = [i for i in p["incompatible_rules"] if i["type"] == "high_risk_uncovered"]
    assert len(gaps) == 1
    assert gaps[0]["min_size"] == 1 and gaps[0]["max_size"] == 4
    assert p["risk_class"] == "high"
    # 被前序规则完全遮蔽的无效规则
    p = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": SHADOWED_RULES}}).json()
    shadowed = [i for i in p["incompatible_rules"] if i["type"] == "shadowed_rule"]
    assert [i["rule"] for i in shadowed] == ["h-dup"]


def test_preview_validation(client):
    apply_policy(client, STABLE_RULES)
    # policy 与 candidate_version 必须二选一
    assert client.post("/admin/replay-policies/preview", json={}).status_code == 422
    r = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": NORMAL_ONLY_RULES},
                          "candidate_version": 1})
    assert r.status_code == 422
    # 无效策略：与直接提交相同的校验错误
    r = client.post("/admin/replay-policies/preview",
                    json={"policy": {"rules": []}})
    assert r.status_code == 422
    assert r.json()["detail"]["error"] == "invalid_replay_policy"
    # 候选版本不存在
    assert client.post("/admin/replay-policies/preview",
                       json={"candidate_version": 999, "risk_level": "high",
                             "rollout_percent": 50}).status_code == 404


# ---- 审批门禁：高风险变更 -------------------------------------------------------

def test_high_risk_change_requires_approval_by_different_operator(client):
    v1 = apply_policy(client, STABLE_RULES)
    r = create_change(client, NEW_HIGH_RULES)
    assert r.status_code == 201, r.text
    change = r.json()["change"]
    cid = change["id"]
    assert change["status"] == "pending"
    assert change["risk_class"] == "high" and change["requires_approval"] is True
    # 预览随单固化：版本与生成时间
    assert change["preview"]["policy_version"] == v1 + 1
    assert change["preview"]["generated_at"] > 0

    # 未批准不能执行；提交人不能自批
    assert apply_change(client, cid).status_code == 409
    assert approve_change(client, cid, operator=SUBMITTER).status_code == 409

    # 审批期间旧稳定版本继续服务：新提交的高风险批仍按 v1 解析
    post_done(client, "e1")
    b = submit_batch(client, "e1").json()
    assert b["policy_version"] == v1

    # 不同运营批准 -> 执行 -> 整份生效为新版本
    r = approve_change(client, cid)
    assert r.status_code == 200, r.text
    assert r.json()["change"]["status"] == "approved"
    assert current_version(client) == v1  # 批准后、执行前线上仍未变
    r = apply_change(client, cid)
    assert r.status_code == 200, r.text
    applied = r.json()["change"]
    assert applied["status"] == "applied"
    assert applied["result"]["applied_version"] == v1 + 1
    assert current_version(client) == v1 + 1

    # 发布后查询：变更前后版本、影响预览、审批决定、审计记录
    view = get_change(client, cid)
    assert view["base_version"] == v1
    assert view["result"]["applied_version"] == v1 + 1
    assert view["preview"]["affected_scope"]
    assert view["approval"]["decision"] == "approved"
    assert view["approval"]["decided_by"] == APPROVER
    audit_types = [a["type"] for a in view["audits"]]
    assert audit_types == ["replay_policy_change_applied",
                           "replay_policy_change_approved",
                           "replay_policy_change_submitted"]

    # 已提交的批次保持 v1 快照；新提交的批次走 v2
    old = client.get(f"/admin/replays/{b['batch_id']}").json()["batch"]
    assert old["policy_version"] == v1
    assert old["policy_snapshot"]["rule_name"] == "h-stable"
    post_done(client, "e2")
    b2 = submit_batch(client, "e2").json()
    assert b2["policy_version"] == v1 + 1
    assert b2["approval_nodes"] == 2  # 新策略的并行双节点


def test_standard_change_applies_without_approval(client):
    v1 = apply_policy(client, STABLE_RULES)
    r = create_change(client, NORMAL_ONLY_RULES)
    assert r.status_code == 201, r.text
    change = r.json()["change"]
    cid = change["id"]
    assert change["risk_class"] == "standard"
    assert change["requires_approval"] is False
    # 标准变更不需要（也不接受）审批；提交人直接执行
    assert approve_change(client, cid).status_code == 409
    r = apply_change(client, cid)
    assert r.status_code == 200, r.text
    assert r.json()["change"]["result"]["applied_version"] == v1 + 1
    assert current_version(client) == v1 + 1


def test_weakening_change_is_classified_high_risk(client):
    apply_policy(client, NEW_HIGH_RULES)  # 当前：high 并行双节点
    # 回退为单节点 = 削弱审批强度 -> 高风险变更
    r = create_change(client, STABLE_RULES)
    change = r.json()["change"]
    assert change["risk_class"] == "high"
    scope = [s for s in change["preview"]["affected_scope"]
             if s["risk_level"] == "high"]
    assert scope and scope[0]["weakens_approval"] is True


# ---- 拒绝 / 超时 / 重复提交 / 并发：不产生部分生效 --------------------------------

def test_reject_produces_no_effect(client):
    v1 = apply_policy(client, STABLE_RULES)
    cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
    # 拒绝原因必填；提交人不能自否
    r = client.post(f"/admin/replay-policies/changes/{cid}/reject",
                    json={"operator": APPROVER})
    assert r.status_code == 422
    assert client.post(f"/admin/replay-policies/changes/{cid}/reject",
                       json={"operator": SUBMITTER, "reason": "x"}).status_code == 409
    r = client.post(f"/admin/replay-policies/changes/{cid}/reject",
                    json={"operator": APPROVER, "reason": "节点链风险未评估"})
    assert r.status_code == 200, r.text
    assert r.json()["change"]["status"] == "rejected"
    # 拒绝后不能批准、不能执行；线上配置与版本历史零变化
    assert approve_change(client, cid).status_code == 409
    assert apply_change(client, cid).status_code == 409
    assert current_version(client) == v1
    assert len(versions(client)) == 1
    view = get_change(client, cid)
    assert view["approval"]["decision"] == "rejected"
    assert view["approval"]["reason"] == "节点链风险未评估"
    assert "replay_policy_change_rejected" in [a["type"] for a in view["audits"]]


def test_expired_change_cannot_be_decided_or_applied(tmp_path):
    with make_client(tmp_path, ttl=0.2) as client:
        v1 = apply_policy(client, STABLE_RULES)
        cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
        time.sleep(0.3)
        # 超时后：批准与执行都被拒，变更单落为 expired（状态与审计真正提交），线上零变化
        assert approve_change(client, cid).status_code == 409
        assert apply_change(client, cid).status_code == 409
        view = get_change(client, cid)
        assert view["status"] == "expired"
        assert "replay_policy_change_expired" in [a["type"] for a in view["audits"]]
        assert current_version(client) == v1
        assert len(versions(client)) == 1


def test_worker_sweep_expires_due_changes(tmp_path):
    with make_client(tmp_path, ttl=0.2) as client:
        apply_policy(client, STABLE_RULES)
        cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
        time.sleep(0.3)
        client.app.state.replay_worker.run_once()  # 扫描落 expired + 审计
        view = get_change(client, cid)
        assert view["status"] == "expired"
        assert "replay_policy_change_expired" in [a["type"] for a in view["audits"]]
        # 重复扫描不会产生第二次效果
        client.app.state.replay_worker.run_once()
        assert [a["type"] for a in get_change(client, cid)["audits"]].count(
            "replay_policy_change_expired") == 1


def test_duplicate_submission_is_idempotent(client):
    apply_policy(client, STABLE_RULES)
    r1 = create_change(client, NEW_HIGH_RULES, request_id="chg-1")
    assert r1.status_code == 201
    r2 = create_change(client, NEW_HIGH_RULES, request_id="chg-1")
    assert r2.status_code == 200 and r2.json()["result"] == "duplicate"
    cid = r1.json()["change"]["id"]
    assert r2.json()["change"]["id"] == cid
    # 只有一张变更单；执行也只能生效一次
    changes = client.get("/admin/replay-policies/changes").json()["changes"]
    assert len(changes) == 1
    assert approve_change(client, cid).status_code == 200
    assert apply_change(client, cid).status_code == 200
    assert apply_change(client, cid).status_code == 409
    assert len([v for v in versions(client) if v["result"] == "applied"]) == 2


def test_concurrent_approval_has_single_winner(client):
    apply_policy(client, STABLE_RULES)
    cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
    results = []

    def decide(operator):
        results.append(approve_change(client, cid, operator=operator).status_code)

    threads = [threading.Thread(target=decide, args=(f"ops-a{i}",))
               for i in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    # 并发审批只有一个生效，其余 409；不会产生第二个决定
    assert sorted(results) == [200, 409, 409, 409, 409, 409]
    view = get_change(client, cid)
    assert view["status"] == "approved"
    assert [a["type"] for a in view["audits"]].count(
        "replay_policy_change_approved") == 1
    # 批准后的执行同样只能生效一次
    assert apply_change(client, cid).status_code == 200
    assert apply_change(client, cid, operator=APPROVER).status_code == 409


def test_stale_change_is_rejected_at_apply(client):
    v1 = apply_policy(client, STABLE_RULES)
    cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
    assert approve_change(client, cid).status_code == 200
    # 预览之后另一份策略整份生效，基线被推进 -> 拒绝执行（不产生叠加效果）
    v2 = apply_policy(client, NORMAL_ONLY_RULES, "ops-other")
    r = apply_change(client, cid)
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "stale_change"
    assert current_version(client) == v2
    assert get_change(client, cid)["status"] == "approved"  # 未落定，可重新评估


# ---- 发布候选版本的门禁 ----------------------------------------------------------

def test_publish_release_change_flow(client):
    v1 = apply_policy(client, CANDIDATE_RULES, "ops-p1")
    v2 = apply_policy(client, STABLE_RULES, "ops-p2")  # 稳定=v2
    r = create_change(client, None, candidate_version=v1, risk_level="high",
                      rollout_percent=100)
    assert r.status_code == 201, r.text
    change = r.json()["change"]
    cid = change["id"]
    assert change["change_type"] == "publish_release"
    assert change["requires_approval"] is True  # 触及 high 等级
    preview = change["preview"]
    assert preview["policy_version"] == v1 and preview["version_status"] == "applied"
    assert preview["release"] == {"risk_level": "high", "min_size": None,
                                  "max_size": None, "rollout_percent": 100}
    # 审批期间没有发布单：新批次仍走稳定版本
    post_done(client, "e1")
    assert submit_batch(client, "e1").json()["policy_version"] == v2
    # 他人批准 -> 执行 -> 发布单创建，新批次分流到候选
    assert approve_change(client, cid).status_code == 200
    r = apply_change(client, cid)
    assert r.status_code == 200, r.text
    release_id = r.json()["change"]["result"]["release_id"]
    assert release_id is not None
    rel = client.get(f"/admin/replay-policies/releases/{release_id}").json()
    assert rel["status"] == "candidate" and rel["candidate_version"] == v1
    post_done(client, "e2")
    b = submit_batch(client, "e2").json()
    assert b["policy_lane"] == "candidate" and b["policy_version"] == v1


def test_standard_release_change_applies_directly(client):
    v1 = apply_policy(client, CANDIDATE_RULES, "ops-p1")
    apply_policy(client, STABLE_RULES, "ops-p2")
    # normal 等级：候选与稳定的 normal 规则一致（无受影响范围）-> 标准变更
    r = create_change(client, None, candidate_version=v1, risk_level="normal",
                      rollout_percent=50)
    change = r.json()["change"]
    assert change["risk_class"] == "standard"
    r = apply_change(client, change["id"])
    assert r.status_code == 200, r.text
    assert r.json()["change"]["result"]["release_id"] is not None


def test_release_preview_estimates_candidate_hit_ratio(client):
    v1 = apply_policy(client, CANDIDATE_RULES, "ops-p1")
    apply_policy(client, STABLE_RULES, "ops-p2")
    for ext in ("e1", "e2", "e3", "e4"):
        post_done(client, ext)
    # 两个 high 小批（闸门内）+ 两个 normal 批
    assert submit_batch(client, "e1").status_code == 201
    assert submit_batch(client, "e2").status_code == 201
    assert submit_batch(client, "e3", risk_level="normal").status_code == 201
    assert submit_batch(client, "e4", risk_level="normal").status_code == 201

    p = client.post("/admin/replay-policies/preview",
                    json={"candidate_version": v1, "risk_level": "high",
                          "rollout_percent": 50, "min_size": 1, "max_size": 5}
                    ).json()
    assert p["change_type"] == "publish_release"
    assert p["policy_version"] == v1
    hit = p["estimated_hit"]
    assert hit["sampled_batches"] == 4 and hit["in_gate_batches"] == 2
    assert hit["in_gate_ratio"] == 0.5
    # 闸门内 50% 分流：预计命中比例 = 0.5 * 0.5
    assert hit["estimated_hit_ratio"] == 0.25
    # 发布前线上无发布单
    assert client.get("/admin/replay-policies/releases").json()["releases"] == []


# ---- 已提交批次不受变更影响 -------------------------------------------------------

def test_submitted_batches_keep_original_snapshot(client):
    v1 = apply_policy(client, STABLE_RULES)
    post_done(client, "e1")
    b = submit_batch(client, "e1").json()
    batch_id = b["batch_id"]
    assert b["policy_version"] == v1

    # 高风险变更完成审批并生效
    cid = create_change(client, NEW_HIGH_RULES).json()["change"]["id"]
    assert approve_change(client, cid).status_code == 200
    assert apply_change(client, cid).status_code == 200
    assert current_version(client) == v1 + 1

    # 已提交批次仍持有原策略快照：单节点串行链不变，可照旧完成审批
    view = client.get(f"/admin/replays/{batch_id}").json()["batch"]
    assert view["policy_version"] == v1
    assert view["policy_snapshot"]["rule_name"] == "h-stable"
    nodes = view["approval"]["nodes"]
    assert len(nodes) == 1 and nodes[0]["role"] == "ops-lead"
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": "ops-lead", "delegatee": APPROVER, "operator": "ops-grant",
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text
    r = client.post(f"/admin/replays/{batch_id}/nodes/{nodes[0]['id']}/approve",
                    json={"operator": APPROVER, "role": "ops-lead",
                          "delegation_id": r.json()["delegation_id"]})
    assert r.status_code == 200, r.text
    assert client.get(f"/admin/replays/{batch_id}").json()["batch"][
        "approval_status"] == "approved"

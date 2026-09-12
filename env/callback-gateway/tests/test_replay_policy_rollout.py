"""审批策略灰度发布与回滚的端到端测试：

- 同一风险等级可维护多个候选策略版本，按批次规模闸门与指定百分比分流；
- 每个批次记录命中的策略版本、分流规则（闸门/百分比/bucket）与发布批次序号；
- 分流结果对同一批次恒定：重复提交（request_id）与服务重启后保持不变；
- 候选发布前可用 evaluate 试运行校验；发布后可暂停/恢复/转正/回滚；
- 回滚不改变已生成审批节点的批次，只影响之后的新提交；
- 策略历史/详情展示稳定版本、候选版本、分流命中、暂停与回滚原因；
- 发布/暂停/恢复/转正/回滚/被整份切换取代/批次分流命中全部写审计；
- 并发发布与批次提交串行化：没有「无版本」或跨版本快照。
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
LEAD = "ops-wang"
SECURITY = "ops-sun"

# 稳定策略：高风险单节点 ops-lead
STABLE_RULES = [
    {"name": "h-stable", "risk_level": "high", "mode": "serial", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 3600}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 候选策略：高风险并行双节点（ops-lead + security）
CANDIDATE_RULES = [
    {"name": "h-cand", "risk_level": "high", "mode": "parallel", "nodes": [
        {"role": "ops-lead", "timeout_seconds": 1800},
        {"role": "security", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]
# 仅覆盖大批（>=5）的策略
BIG_ONLY_RULES = [
    {"name": "h-big", "risk_level": "high", "min_size": 5, "mode": "serial",
     "nodes": [{"role": "ops-lead", "timeout_seconds": 1800}]},
    {"name": "n-none", "risk_level": "normal", "nodes": []},
]


@pytest.fixture()
def client(tmp_path):
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    settings = Settings(database_path=str(tmp_path / "gateway.db"),
                        keys_file=str(keys), run_worker=False)
    app = create_app(settings)
    with TestClient(app) as c:
        yield c


def apply_policy(client, rules, operator="ops-policy"):
    r = client.post("/admin/replay-policies",
                    json={"operator": operator, "policy": {"rules": rules}})
    assert r.status_code == 200, r.text
    return r.json()["version"]


def setup_versions(client):
    """v1=候选内容, v2=稳定内容（最后整份生效 -> 两等级稳定指针都是 v2）。"""
    v1 = apply_policy(client, CANDIDATE_RULES, "ops-p1")
    v2 = apply_policy(client, STABLE_RULES, "ops-p2")
    return v1, v2


def publish(client, **kw):
    payload = {"operator": "ops-rel", "risk_level": "high",
               "candidate_version": kw.pop("version"),
               "rollout_percent": kw.pop("percent", 100)}
    payload.update(kw)
    return client.post("/admin/replay-policies/releases", json=payload)


def post_done(client, external_id, body=b'{"order":1}'):
    ts, sig = sign(ACTIVE_SECRET, body)
    r = client.post("/callbacks", content=body, headers={
        "X-Callback-Id": external_id,
        "X-Signature": f"kid=k2,ts={ts},sig={sig}"})
    assert r.status_code in (200, 202), r.text
    client.app.state.worker.run_once()


def submit_high(client, external_id, request_id=None, **kw):
    payload = {"operator": SUBMITTER, "reason": "灰度验证", "risk_level": "high",
               "approval_note": "涉及资金类回调补发", "external_id": external_id}
    if request_id is not None:
        payload["request_id"] = request_id
    payload.update(kw)
    return client.post("/admin/replays", json=payload)


def detail(client, batch_id):
    return client.get(f"/admin/replays/{batch_id}").json()


def approval(client, batch_id):
    return detail(client, batch_id)["batch"]["approval"]


def delegate(client, role, delegatee):
    now = time.time()
    r = client.post("/admin/replay-delegations", json={
        "role": role, "delegatee": delegatee, "operator": "ops-grant",
        "valid_from": now - 60, "valid_to": now + 3600})
    assert r.status_code == 201, r.text
    return r.json()["delegation_id"]


def approve_node(client, batch_id, node_id, operator, role):
    return client.post(f"/admin/replays/{batch_id}/nodes/{node_id}/approve",
                       json={"operator": operator, "role": role,
                             "delegation_id": delegate(client, role, operator)})


# ---- 发布与试运行 ----------------------------------------------------------------

def test_publish_candidate_and_current_view(client):
    v1, v2 = setup_versions(client)
    r = publish(client, version=v1, percent=30, min_size=1, max_size=4,
                note="小批先试")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["result"] == "published"
    assert body["rollout_seq"] == 1 and body["candidate_version"] == v1
    assert body["stable_version"] == v2 and body["status"] == "candidate"

    # 当前视图：稳定版本（含上一稳定版本）+ 开放中的候选发布
    cur = client.get("/admin/replay-policies/current").json()["rollout"]["risk_levels"]
    high = cur["high"]
    assert high["stable_version"] == v2 and high["stable_source"] == "applied"
    rel = high["open_release"]
    assert rel["id"] == body["release_id"] and rel["rollout_percent"] == 30
    assert rel["min_size"] == 1 and rel["max_size"] == 4
    assert rel["candidate_rule_name"] == "h-cand"
    assert rel["hits"] == {"candidate_hits": 0, "recorded_batches": 0,
                           "last_hit_at": None}
    assert cur["normal"]["stable_version"] == v2
    assert cur["normal"]["open_release"] is None


def test_publish_validation(client):
    v1, v2 = setup_versions(client)
    # 候选版本不能等于当前稳定版本
    assert publish(client, version=v2, percent=10).status_code == 422
    # 百分比/闸门/等级/版本号合法性
    assert publish(client, version=v1, percent=0).status_code == 422
    assert publish(client, version=v1, percent=101).status_code == 422
    assert publish(client, version=v1, percent=10, min_size=9, max_size=2
                   ).status_code == 422
    r = publish(client, version=v1, percent=10, risk_level="urgent")
    assert r.status_code == 422
    assert publish(client, version=999, percent=10).status_code == 404
    # 候选策略规则不覆盖声明的规模闸门 -> 422（明确错误码）。
    # 整份切到 v2（覆盖全部规模）为稳定，再把只覆盖 >=5 的 v3 以闸门 [1,4] 发布：
    # 规则区间与闸门无交集 -> 拒绝
    v3 = apply_policy(client, BIG_ONLY_RULES, "ops-p3")  # 稳定=v3
    apply_policy(client, STABLE_RULES, "ops-p4")        # 稳定回到 v2
    r = publish(client, version=v3, percent=100, min_size=1, max_size=4)
    assert r.status_code == 422
    assert r.json()["error"] == "candidate_policy_does_not_cover_gate"
    # 同一风险等级至多一条未结束发布
    assert publish(client, version=v1, percent=10).status_code == 200
    assert publish(client, version=v1, percent=20).status_code == 409


def test_evaluate_dry_run_is_read_only_and_previews_rules(client):
    v1, v2 = setup_versions(client)
    publish(client, version=v1, percent=100, min_size=1, max_size=4)
    # 闸门内：命中候选版本 v1（并行双节点）
    e = client.post("/admin/replay-policies/evaluate",
                    json={"risk_level": "high", "total": 2,
                          "request_id": "eval-1"}).json()
    assert e["would_hit_lane"] == "candidate"
    assert e["would_use_version"] == v1
    assert e["would_route_to_candidate"] is True
    assert e["routing_reason"] == "matched_candidate"
    assert [n["role"] for n in e["candidate_rule"]["nodes"]] == ["ops-lead", "security"]
    assert [n["role"] for n in e["stable_rule"]["nodes"]] == ["ops-lead"]
    # 闸门外：走稳定版本 v2
    e = client.post("/admin/replay-policies/evaluate",
                    json={"risk_level": "high", "total": 6,
                          "request_id": "eval-1"}).json()
    assert e["would_hit_lane"] == "stable" and e["would_use_version"] == v2
    assert e["routing_reason"] == "size_outside_gate"
    # 试运行不落任何批次/发布/审计
    assert client.get("/admin/replays").json()["batches"] == []
    types = {x["type"] for x in client.get("/admin/events").json()["events"]}
    assert "replay_policy_batch_routed" not in types
    # 非法入参
    assert client.post("/admin/replay-policies/evaluate",
                       json={"risk_level": "high", "total": 0}).status_code == 422


# ---- 分流：规模闸门 + 百分比 + 确定性 -----------------------------------------------

def test_routing_by_size_gate_and_percent(client):
    v1, v2 = setup_versions(client)
    publish(client, version=v1, percent=30, min_size=1, max_size=4)
    # 200 个不同分流键在闸门内：候选占比约 30%（稳定哈希，容差放宽）
    cand = 0
    for i in range(200):
        e = client.post("/admin/replay-policies/evaluate",
                        json={"risk_level": "high", "total": 3,
                              "request_id": f"k{i}"}).json()
        cand += e["would_hit_lane"] == "candidate"
    assert 40 <= cand <= 80
    # 同一分流键结果恒定
    def lane(rid, total):
        return client.post("/admin/replay-policies/evaluate",
                           json={"risk_level": "high", "total": total,
                                 "request_id": rid}).json()["would_hit_lane"]
    first = [lane("same-key", 3) for _ in range(5)]
    assert len(set(first)) == 1
    # 规模超闸门：始终稳定（与桶无关）
    assert all(lane(f"g{i}", 6) == "stable" for i in range(50))


def test_batch_records_hit_version_rule_and_release(client):
    v1, v2 = setup_versions(client)
    rel = publish(client, version=v1, percent=100).json()["release_id"]
    post_done(client, "GR-1")
    r = submit_high(client, "GR-1", request_id="req-gr-1")
    assert r.status_code == 201
    body = r.json()
    assert body["policy_version"] == v1 and body["policy_lane"] == "candidate"
    assert body["rollout_id"] == rel and body["rollout_seq"] == 1
    assert body["candidate_version"] == v1
    assert 0 <= body["routing_bucket"] <= 99
    assert body["approval_nodes"] == 2  # 候选策略并行双节点

    # 批次表/快照/详情都固化分流命中
    b = detail(client, body["batch_id"])["batch"]
    assert b["policy_version"] == v1 and b["policy_lane"] == "candidate"
    assert b["rollout_id"] == rel and b["rollout_seq"] == 1
    snap = b["policy_snapshot"]
    assert snap["rule_name"] == "h-cand" and snap["mode"] == "parallel"
    routing = snap["routing"]
    assert routing["lane"] == "candidate"
    assert routing["policy_version"] == v1 and routing["stable_version"] == v2
    assert routing["candidate_version"] == v1 and routing["release_id"] == rel
    assert routing["rollout_seq"] == 1 and routing["rollout_percent"] == 100
    assert routing["bucket"] == body["routing_bucket"]
    assert routing["routing_key"] == "req:req-gr-1"
    ap = b["approval"]
    assert ap["policy_lane"] == "candidate" and ap["rollout_id"] == rel
    assert ap["rollout_seq"] == 1
    assert ap["policy_routing"]["rule_name"] == "h-cand"

    # 发布单命中统计 +1
    got = client.get(f"/admin/replay-policies/releases/{rel}").json()
    assert got["hits"]["candidate_hits"] == 1
    assert got["hits"]["recorded_batches"] == 1
    assert got["hits"]["last_hit_at"] is not None

    # 分流命中审计
    ev = client.get(f"/admin/replays/{body['batch_id']}/events").json()["events"]
    routed = next(e for e in ev if e["type"] == "replay_policy_batch_routed")
    assert routed["detail"]["lane"] == "candidate"
    assert routed["detail"]["policy_version"] == v1
    assert routed["detail"]["release_id"] == rel
    assert routed["detail"]["rollout_seq"] == 1


def test_routing_is_deterministic_across_duplicate_and_restart(tmp_path):
    keys = tmp_path / "keys.json"
    keys.write_text(json.dumps({"keys": [
        {"kid": "k2", "secret": ACTIVE_SECRET, "status": "active"}]}))
    db_path = str(tmp_path / "gateway.db")
    settings = Settings(database_path=db_path, keys_file=str(keys), run_worker=False)

    def app():
        return create_app(Settings(database_path=db_path, keys_file=str(keys),
                                   run_worker=False))

    with TestClient(app()) as c:
        v1, v2 = setup_versions(c)
        c.post("/admin/replay-policies/releases", json={
            "operator": "ops-rel", "risk_level": "high",
            "candidate_version": v1, "rollout_percent": 100})
        post_done(c, "RD-1")
        first = submit_high(c, "RD-1", request_id="req-rd-1").json()
        # 重复提交：返回原批次与同样的车道/版本/发布单
        dup = submit_high(c, "RD-1", request_id="req-rd-1").json()
        assert dup["result"] == "duplicate" and dup["batch_id"] == first["batch_id"]
        assert dup["policy_version"] == first["policy_version"]
        assert dup["policy_lane"] == first["policy_lane"]
        assert dup["rollout_id"] == first["rollout_id"]
        assert dup["rollout_seq"] == first["rollout_seq"]

    # 重启后：试运行同一分流键给出同一车道；已落盘批次的归属不变
    with TestClient(app()) as c:
        e = c.post("/admin/replay-policies/evaluate",
                   json={"risk_level": "high", "total": 1,
                         "request_id": "req-rd-1"}).json()
        assert e["would_hit_lane"] == "candidate"
        assert e["would_use_version"] == first["policy_version"]
        b = detail(c, first["batch_id"])["batch"]
        assert b["policy_version"] == first["policy_version"]
        assert b["policy_lane"] == "candidate"


def test_routing_without_request_id_uses_content_set_hash(client):
    v1, v2 = setup_versions(client)
    publish(client, version=v1, percent=100)
    post_done(client, "RC-1")
    first = submit_high(client, "RC-1").json()
    # 第一次提交占住内容；拒绝/取消后用同内容再提交（无 request_id）：
    # 分流键是投递 id 集合，哈希相同 -> 同车道
    ap = approval(client, first["batch_id"])
    client.post(f"/admin/replays/{first['batch_id']}/nodes/{ap['nodes'][0]['id']}/reject",
                json={"operator": LEAD, "role": "ops-lead",
                      "delegation_id": delegate(client, "ops-lead", LEAD),
                      "reason": "先释放"})
    second = submit_high(client, "RC-1").json()
    assert second["batch_id"] != first["batch_id"]
    assert second["policy_lane"] == "candidate"
    assert second["policy_version"] == v1
    s1 = detail(client, first["batch_id"])["batch"]["policy_snapshot"]["routing"]
    s2 = detail(client, second["batch_id"])["batch"]["policy_snapshot"]["routing"]
    assert s1["routing_key"] == s2["routing_key"]
    assert s1["bucket"] == s2["bucket"]


# ---- 暂停 / 恢复 / 转正 / 回滚 ----------------------------------------------------

def test_pause_and_resume_change_only_new_submissions(client):
    v1, v2 = setup_versions(client)
    rel = publish(client, version=v1, percent=100).json()["release_id"]
    post_done(client, "PR-1")
    post_done(client, "PR-2")
    before = submit_high(client, "PR-1", request_id="req-pr-1").json()
    assert before["policy_lane"] == "candidate"

    # 暂停必须带原因
    assert client.post(f"/admin/replay-policies/releases/{rel}/pause",
                       json={"operator": "ops-rel"}).status_code == 422
    r = client.post(f"/admin/replay-policies/releases/{rel}/pause",
                    json={"operator": "ops-rel", "reason": "候选审批链异常率升高"})
    assert r.status_code == 200 and r.json()["status"] == "paused"
    row = client.get(f"/admin/replay-policies/releases/{rel}").json()
    assert row["pause_reason"] == "候选审批链异常率升高"
    assert row["paused_by"] == "ops-rel"
    # 暂停后新提交全走稳定版本，但仍记录当前发布批次序号
    after = submit_high(client, "PR-2", request_id="req-pr-2").json()
    assert after["policy_lane"] == "stable" and after["policy_version"] == v2
    assert after["rollout_id"] is None and after["rollout_seq"] == 1
    assert after["approval_nodes"] == 1
    # 已命中候选的批次不受影响：仍是双节点候选链
    assert len(approval(client, before["batch_id"])["nodes"]) == 2

    # 恢复：同分流键重新命中候选
    r = client.post(f"/admin/replay-policies/releases/{rel}/resume",
                    json={"operator": "ops-rel"})
    assert r.status_code == 200 and r.json()["status"] == "candidate"
    post_done(client, "PR-3")
    again = submit_high(client, "PR-3", request_id="req-pr-3").json()
    assert again["policy_lane"] == "candidate" and again["policy_version"] == v1
    # 暂停/恢复审计
    types = [e["type"] for e in client.get("/admin/events").json()["events"]]
    assert types.count("replay_policy_release_paused") == 1
    assert types.count("replay_policy_release_resumed") == 1


def test_promote_then_rollback_restores_previous_stable(client):
    v1, v2 = setup_versions(client)  # 稳定=v2
    rel = publish(client, version=v1, percent=100).json()["release_id"]

    r = client.post(f"/admin/replay-policies/releases/{rel}/promote",
                    json={"operator": "ops-rel"})
    assert r.status_code == 200
    assert r.json()["stable_version"] == v1
    assert r.json()["previous_stable_version"] == v2
    cur = client.get("/admin/replay-policies/current").json()["rollout"]["risk_levels"]
    assert cur["high"]["stable_version"] == v1
    assert cur["high"]["previous_stable_version"] == v2
    # 转正后没有开放中的发布
    assert cur["high"]["open_release"] is None

    # 转正后回滚（原因必填）：稳定指针恢复为 v2
    assert client.post(f"/admin/replay-policies/releases/{rel}/rollback",
                       json={"operator": "ops-rel"}).status_code == 422
    r = client.post(f"/admin/replay-policies/releases/{rel}/rollback",
                    json={"operator": "ops-rel", "reason": "转正后发现漏审角色"})
    assert r.status_code == 200
    assert r.json()["restored_stable_version"] == v2
    cur = client.get("/admin/replay-policies/current").json()["rollout"]["risk_levels"]
    assert cur["high"]["stable_version"] == v2
    row = client.get(f"/admin/replay-policies/releases/{rel}").json()
    assert row["status"] == "rolled_back"
    assert row["rollback_reason"] == "转正后发现漏审角色"
    # 回滚后的新提交走 v2
    post_done(client, "PB-1")
    b = submit_high(client, "PB-1", request_id="req-pb-1").json()
    assert b["policy_version"] == v2 and b["policy_lane"] == "stable"
    assert b["approval_nodes"] == 1
    # 发布/转正/回滚审计齐全
    types = [e["type"] for e in client.get("/admin/events").json()["events"]]
    for t in ("replay_policy_release_published", "replay_policy_release_promoted",
              "replay_policy_release_rolled_back"):
        assert types.count(t) == 1


def test_rollback_during_gray_keeps_existing_candidate_batches(client):
    v1, v2 = setup_versions(client)
    rel = publish(client, version=v1, percent=100).json()["release_id"]
    post_done(client, "RB-1")
    post_done(client, "RB-2")
    existing = submit_high(client, "RB-1", request_id="req-rb-1").json()
    assert existing["policy_version"] == v1 and existing["approval_nodes"] == 2

    # 灰度中回滚（稳定版本从未改变，restored 仍为 v2）
    r = client.post(f"/admin/replay-policies/releases/{rel}/rollback",
                    json={"operator": "ops-rel", "reason": "紧急止损"})
    assert r.status_code == 200
    assert r.json()["restored_stable_version"] == v2
    # 已生成审批节点的批次不变：候选双节点链照常审批与放行
    ap = approval(client, existing["batch_id"])
    assert [n["role"] for n in ap["nodes"]] == ["ops-lead", "security"]
    approve_node(client, existing["batch_id"], ap["nodes"][0]["id"], LEAD, "ops-lead")
    approve_node(client, existing["batch_id"], ap["nodes"][1]["id"],
                 SECURITY, "security")
    client.app.state.replay_worker.run_once()
    client.app.state.worker.run_once()
    d = detail(client, existing["batch_id"])
    assert d["batch"]["status"] == "completed"
    assert d["batch"]["policy_version"] == v1
    assert d["tasks"][0]["status"] == "done"
    # 回滚后新提交走稳定版本
    after = submit_high(client, "RB-2", request_id="req-rb-2").json()
    assert after["policy_version"] == v2 and after["policy_lane"] == "stable"
    assert after["approval_nodes"] == 1


def test_lifecycle_state_guards(client):
    v1, v2 = setup_versions(client)
    rel = publish(client, version=v1, percent=100).json()["release_id"]
    url = f"/admin/replay-policies/releases/{rel}"
    # candidate 不能直接 resume
    assert client.post(f"{url}/resume", json={"operator": "o"}).status_code == 409
    client.post(f"{url}/pause", json={"operator": "o", "reason": "x"})
    # paused 不能再暂停 / 转正后不能再暂停
    assert client.post(f"{url}/pause", json={"operator": "o", "reason": "y"}
                       ).status_code == 409
    client.post(f"{url}/promote", json={"operator": "o"})
    assert client.post(f"{url}/pause", json={"operator": "o", "reason": "z"}
                       ).status_code == 409
    client.post(f"{url}/rollback", json={"operator": "o", "reason": "r"})
    # 终态后所有动作 409
    assert client.post(f"{url}/pause", json={"operator": "o", "reason": "z"}
                       ).status_code == 409
    assert client.post(f"{url}/resume", json={"operator": "o"}).status_code == 409
    assert client.post(f"{url}/promote", json={"operator": "o"}).status_code == 409
    assert client.post(f"{url}/rollback", json={"operator": "o", "reason": "r2"}
                       ).status_code == 409
    assert client.get("/admin/replay-policies/releases/9999").status_code == 404


# ---- 整份策略切换与灰度的关系 ------------------------------------------------------

def test_full_policy_apply_supersedes_open_release(client):
    v1, v2 = setup_versions(client)
    rel = publish(client, version=v1, percent=50).json()["release_id"]
    # 整份提交新策略：两等级稳定指针切到 v3，开放中的灰度被 superseded
    v3 = apply_policy(client, CANDIDATE_RULES, "ops-p3")
    row = client.get(f"/admin/replay-policies/releases/{rel}").json()
    assert row["status"] == "superseded" and row["superseded_by"] == "ops-p3"
    cur = client.get("/admin/replay-policies/current").json()["rollout"]["risk_levels"]
    assert cur["high"]["stable_version"] == v3
    assert cur["high"]["open_release"] is None
    # 被取代的发布单不能再操作
    assert client.post(f"/admin/replay-policies/releases/{rel}/pause",
                       json={"operator": "o", "reason": "x"}).status_code == 409
    # 取代事件有审计
    ev = [e for e in client.get("/admin/events").json()["events"]
          if e["type"] == "replay_policy_release_superseded"]
    assert len(ev) == 1 and ev[0]["detail"]["superseded_by_version"] == v3
    # 灰度序号继续单调递增（下一条仍是该等级第 2 次发布）
    r = publish(client, version=v2, percent=10)
    assert r.status_code == 200 and r.json()["rollout_seq"] == 2


def test_builtin_default_stable_when_no_policy(client):
    # 从未提交过策略：稳定指针不存在 -> 内置默认（高风险单 any 审批）
    cur = client.get("/admin/replay-policies/current").json()["rollout"]["risk_levels"]
    assert cur["high"]["stable_version"] is None
    assert cur["high"]["stable_source"] == "builtin_default"
    e = client.post("/admin/replay-policies/evaluate",
                    json={"risk_level": "high", "total": 1}).json()
    assert e["stable_version"] is None
    assert e["stable_rule"]["nodes"][0]["role"] == "any"


# ---- 并发：发布与批次提交 ----------------------------------------------------------

def test_concurrent_publish_and_submit_have_consistent_snapshots(client):
    v1, v2 = setup_versions(client)
    for i in range(30):
        post_done(client, f"CC-{i}")
    errors: list[str] = []

    def submit_many(start):
        try:
            for i in range(start, start + 15):
                r = submit_high(client, f"CC-{i}", request_id=f"req-cc-{i}")
                assert r.status_code == 201, r.text
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

    def publish_pause_rollback():
        try:
            for k in range(10):
                r = publish(client, version=v1, percent=100)
                if r.status_code != 200:
                    continue
                rid = r.json()["release_id"]
                client.post(f"/admin/replay-policies/releases/{rid}/pause",
                            json={"operator": "o", "reason": "churn"})
                client.post(f"/admin/replay-policies/releases/{rid}/rollback",
                            json={"operator": "o", "reason": "churn done"})
        except Exception as exc:  # noqa: BLE001
            errors.append(str(exc))

    threads = [threading.Thread(target=submit_many, args=(0,)),
               threading.Thread(target=submit_many, args=(15,)),
               threading.Thread(target=publish_pause_rollback)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert errors == []

    # 每个批次：列上的版本/车道与快照一致，节点链来自同一版本，没有无版本的高风险批
    db = client.app.state.db
    rows = db.query(
        "SELECT id, policy_version, policy_lane, rollout_id, rollout_seq,"
        " policy_snapshot FROM replay_batches WHERE request_id LIKE 'req-cc-%'")
    assert len(rows) == 30
    for row in rows:
        snap = json.loads(row["policy_snapshot"])
        assert row["policy_version"] in (v1, v2)
        assert snap["policy_version"] == row["policy_version"]
        assert snap["routing"]["lane"] == row["policy_lane"]
        if row["policy_lane"] == "candidate":
            assert row["rollout_id"] is not None and row["rollout_seq"] is not None
            assert snap["routing"]["candidate_version"] == row["policy_version"]
        nodes = db.query(
            "SELECT role FROM replay_approval_nodes WHERE batch_id=? ORDER BY seq",
            (row["id"],))
        assert [n["role"] for n in nodes] == [n["role"] for n in snap["nodes"]]
        # v1 候选 = 双节点；v2 稳定 = 单节点——不存在跨版本混搭
        assert len(nodes) == (2 if row["policy_version"] == v1 else 1)

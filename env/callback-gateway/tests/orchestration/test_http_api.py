"""编排模块 HTTP 端到端：图发布、回调入口、实例视图、重关联、跳过、终止、期限。"""
from __future__ import annotations

import json

from fastapi.testclient import TestClient

from app.config import Settings
from app.main import create_app

from conftest import standard_spec


def make_client(tmp_path):
    settings = Settings(
        database_path=str(tmp_path / "api.db"),
        keys_file=str(tmp_path / "keys.json"),
        run_worker=False,
    )
    tmp_path.joinpath("keys.json").write_text(json.dumps({"keys": [
        {"kid": "k1", "secret": "s1", "status": "active"}]}))
    app = create_app(settings)
    return TestClient(app), app


def test_publish_and_callback_flow(tmp_path):
    client, app = make_client(tmp_path)
    with client:
        # 发布
        r = client.post("/admin/orchestration/graphs/order_flow",
                        json={"spec": standard_spec(), "operator": "ops"})
        assert r.status_code == 200, r.text
        version_id = r.json()["graph_version_id"]

        # 非法图被拒绝（环）
        bad = {"key_path": "order_id", "nodes": [
            {"code": "a", "key_path": "event", "depends_on": ["b"]},
            {"code": "b", "key_path": "event", "depends_on": ["a"]}]}
        r = client.post("/admin/orchestration/graphs/bad_flow", json={"spec": bad})
        assert r.status_code == 422

        # 未发布的流程回调 404
        r = client.post("/orchestration/callbacks/nope",
                        json={"order_id": "x", "event": "paid"})
        assert r.status_code == 404

        # 乱序：ship 先到（非根无实例 -> orphan 等待重关联）
        r = client.post("/orchestration/callbacks/order_flow",
                        json={"order_id": "o1", "event": "ship"},
                        headers={"X-Callback-Uid": "ship-1"})
        assert r.status_code == 202 and r.json()["result"] == "orphan"

        # paid 根节点到达，自动创建实例
        r = client.post("/orchestration/callbacks/order_flow",
                        json={"order_id": "o1", "event": "paid"})
        assert r.status_code == 202
        instance_key = r.json()["instance_key"]

        # 把先到的 ship 重关联到实例
        orphans = client.get("/admin/orchestration/callbacks/orphans").json()["callbacks"]
        ship_orphan = next(c for c in orphans if c["callback_uid"] == "ship-1")
        r = client.post(f"/admin/orchestration/callbacks/{ship_orphan['id']}/reassociate",
                        json={"instance_key": instance_key, "node": "ship"})
        assert r.status_code == 200 and r.json()["result"] == "reassociated"

        # 两条分支（risk_b 由管理员跳过）
        client.post("/orchestration/callbacks/order_flow",
                    json={"order_id": "o1", "event": "risk_a"})
        client.post("/orchestration/callbacks/order_flow",
                    json={"order_id": "o1", "event": "notify"})
        r = client.post(f"/admin/orchestration/instances/{instance_key}/skip",
                        json={"node": "risk_b", "reason": "waived"})
        assert r.status_code == 200

        # 实例视图：全部节点完成/跳过，实例完成
        view = client.get(f"/admin/orchestration/instances/{instance_key}").json()
        assert view["instance"]["status"] == "completed"
        by_node = {n["node"]: n for n in view["nodes"]}
        assert by_node["ship"]["status"] == "COMPLETED"
        assert by_node["risk_b"]["status"] == "SKIPPED"
        # 采用版本与依赖路径可查
        assert by_node["paid"]["adopted_callback_id"]
        assert by_node["ship"]["dependency_paths"]
        # 审计按 seq 有序
        seqs = [a["seq"] for a in view["audit"]]
        assert seqs == sorted(seqs)

        # 图版本历史
        versions = client.get(
            "/admin/orchestration/graphs/order_flow/versions").json()["versions"]
        assert any(v["id"] == version_id and v["current"] for v in versions)


def test_terminate_and_late_and_missing_events(tmp_path):
    client, app = make_client(tmp_path)
    with client:
        client.post("/admin/orchestration/graphs/order_flow",
                    json={"spec": standard_spec()})
        r = client.post("/orchestration/callbacks/order_flow",
                        json={"order_id": "o9", "event": "paid"})
        key = r.json()["instance_key"]
        client.post(f"/admin/orchestration/instances/{key}/terminate",
                    json={"reason": "fraud"})
        # 迟到回调只记 late
        r = client.post("/orchestration/callbacks/order_flow",
                        json={"order_id": "o9", "event": "risk_a"})
        assert r.status_code == 200 and r.json()["result"] == "late"

        # 缺失事件异常可查询
        rows = client.get("/admin/orchestration/missing-events").json()["missing_events"]
        assert isinstance(rows, list)
        # 必需节点不能跳过：另开实例验证 409
        r = client.post("/orchestration/callbacks/order_flow",
                        json={"order_id": "o10", "event": "paid"})
        key2 = r.json()["instance_key"]
        r = client.post(f"/admin/orchestration/instances/{key2}/skip",
                        json={"node": "paid", "reason": "x"})
        assert r.status_code == 409


def test_graph_version_pinning_and_rollback(tmp_path):
    client, app = make_client(tmp_path)
    with client:
        client.post("/admin/orchestration/graphs/order_flow",
                    json={"spec": standard_spec()})
        client.post("/orchestration/callbacks/order_flow",
                    json={"order_id": "p1", "event": "paid"})
        # 发布 v2（多一个可选节点）
        spec2 = standard_spec()
        spec2["nodes"].append({"code": "extra", "occurrences": 1, "key_path": "event",
                               "required": False, "depends_on": ["paid"]})
        client.post("/admin/orchestration/graphs/order_flow", json={"spec": spec2})
        # 回滚到 v1
        r = client.post("/admin/orchestration/graphs/order_flow/rollback",
                        json={"version": 1, "reason": "fix"})
        assert r.status_code == 200
        # 新实例仍是 v1 的 5 个节点
        client.post("/orchestration/callbacks/order_flow",
                    json={"order_id": "p2", "event": "paid"})
        row = app.state.db.query_one(
            "SELECT instance_key FROM orch_instances WHERE correlation_key='p2'")
        view = client.get(f"/admin/orchestration/instances/{row['instance_key']}").json()
        assert {n["node"] for n in view["nodes"]} == {
            "paid", "risk_a", "risk_b", "ship", "notify"}

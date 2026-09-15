"""图发布校验：环、不可达、重复定义、无效取值位置、occurrences/skip 非法全部拒绝。"""
from __future__ import annotations

import pytest

from app.orchestration.engine import NotFoundError, OrchestrationError

from conftest import standard_spec


def _spec(nodes, key_path="order_id"):
    return {"key_path": key_path, "nodes": nodes}


def test_publish_valid_graph_returns_version(env):
    r = env.publish()
    assert r["result"] == "published"
    assert r["version"] == 1
    r2 = env.publish()
    assert r2["version"] == 2  # 同流程内单调递增


def test_publish_rejects_cycle(env):
    spec = _spec([
        {"code": "a", "key_path": "event", "depends_on": ["b"]},
        {"code": "b", "key_path": "event", "depends_on": ["a"]},
    ])
    with pytest.raises(OrchestrationError) as exc:
        env.publish(spec=spec)
    assert "环" in str(exc.value)
    # 被拒绝后没有当前指针
    with pytest.raises(NotFoundError):
        env.send("o1", "a")
    rejected = env.engine.list_graph_versions("order_flow")
    assert rejected[-1]["result"] == "rejected"


def test_publish_rejects_unreachable_node(env):
    spec = _spec([
        {"code": "a", "key_path": "event"},
        {"code": "lonely", "key_path": "event"},  # 第二个独立根是可达的
        {"code": "ghost", "key_path": "event",
         "depends_on": [{"code": "lonely", "join": "ANY"}]},
    ])
    # 多根 DAG 合法：a 与 lonely->ghost 是两条独立启动分支
    assert env.publish(spec=spec)["version"] == 1


def test_publish_rejects_duplicate_node_definitions(env):
    spec = _spec([
        {"code": "a", "key_path": "event"},
        {"code": "a", "key_path": "event"},
    ])
    with pytest.raises(OrchestrationError) as exc:
        env.publish(spec=spec)
    assert "重复事件定义" in str(exc.value)


def test_publish_rejects_invalid_key_paths(env):
    for bad in ["", ".a", "a.", "a..b", "1bad", "a[-1]", "a.b[]", "a.[x]"]:
        spec = standard_spec()
        spec["key_path"] = bad if bad else "order_id"
        if not bad:
            spec["key_path"] = ""
        with pytest.raises(OrchestrationError):
            env.publish(spec=spec)
    # 节点级非法 key_path 同样拒绝
    spec = standard_spec()
    spec["nodes"][0]["key_path"] = "event[bad]"
    with pytest.raises(OrchestrationError):
        env.publish(spec=spec)


def test_publish_rejects_unknown_dependency_and_self_loop(env):
    spec = _spec([{"code": "a", "key_path": "event", "depends_on": ["nope"]}])
    with pytest.raises(OrchestrationError) as exc:
        env.publish(spec=spec)
    assert "未定义" in str(exc.value)

    spec = _spec([{"code": "a", "key_path": "event", "depends_on": ["a"]}])
    with pytest.raises(OrchestrationError):
        env.publish(spec=spec)


def test_publish_rejects_bad_occurrences_and_join(env):
    for bad_occ in (0, -1, 1.5, True):
        with pytest.raises(OrchestrationError):
            env.publish(spec=_spec(
                [{"code": "a", "key_path": "event", "occurrences": bad_occ}]))
    with pytest.raises(OrchestrationError):
        env.publish(spec=_spec(
            [{"code": "a", "key_path": "event", "join": "WHENEVER"}]))
    with pytest.raises(OrchestrationError):
        env.publish(spec=_spec(
            [{"code": "a", "key_path": "event", "wait_seconds": 0}]))


def test_rollback_only_affects_new_instances(env):
    env.publish()  # v1
    env.send("o1", "paid")
    key_v1 = env.instance_key("o1")
    v1_before = env.view(key_v1)["instance"]["graph_version"]

    env.publish(spec=standard_spec(ship={"wait_seconds": 60}))  # v2
    env.engine.rollback_graph("order_flow", 1, operator="ops", reason="hotfix")
    env.send("o2", "paid")
    key_new = env.instance_key("o2")
    # 旧实例固定 v1（回滚不影响既有实例）
    assert env.view(key_v1)["instance"]["graph_version"] == v1_before
    # 新实例用回滚后的指针（指向 v1 的不可变快照）
    assert env.view(key_new)["instance"]["graph_version"] == 1

    versions = env.engine.list_graph_versions("order_flow")
    assert versions[0]["current"] is True

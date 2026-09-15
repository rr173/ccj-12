"""期限扫描、缺失事件异常、重关联、跳过、终止、迟到回调、并发单有效结果。"""
from __future__ import annotations

import threading

import pytest

from app.orchestration.engine import ConflictStateError, NotFoundError

from conftest import standard_spec


def _spec_with_deadlines(**waits):
    spec = standard_spec()
    for node in spec["nodes"]:
        if node["code"] in waits:
            node["wait_seconds"] = waits[node["code"]]
    return spec


def _status(view, code):
    return next(n for n in view["nodes"] if n["node"] == code)["status"]


def test_deadline_generates_missing_prerequisite_event(env):
    # paid 无前置，期限 100；risk_a 依赖 paid
    env.publish(spec=_spec_with_deadlines(paid=50, risk_a=100))
    env.start("d1")
    env.send("d1", "risk_a")  # 只到 risk_a（前置 paid 缺失）
    key = env.instance_key("d1")
    assert env.engine.scan_deadlines() == []
    env.clock.advance(101)
    events = env.engine.scan_deadlines()
    assert len(events) == 2
    kinds = {e["node"]: e["kind"] for e in events}
    assert kinds["paid"] == "missing_callback"
    assert kinds["risk_a"] == "missing_prerequisite"
    # 异常可查询，含受阻分支与最后期限
    rows = env.engine.list_missing_events(status="open")
    by_node = {r["node_code"]: r for r in rows}
    ra = by_node["risk_a"]
    assert ra["detail_json"]["missing_prerequisites"] == ["paid"]
    assert ra["blocked_paths_json"]  # 受阻分支非空
    # 重复扫描不产生第二条（每实例×节点同时只有一条 open）
    assert env.engine.scan_deadlines(now=env.clock() + 1000) == []


def test_missing_event_resolves_when_callback_arrives(env):
    env.publish(spec=_spec_with_deadlines(paid=100))
    env.start("d2")
    env.clock.advance(101)
    events = env.engine.scan_deadlines()
    assert len(events) == 1 and events[0]["kind"] == "missing_callback"
    # 回调到达：paid 完成，open 异常同事务关闭
    env.send("d2", "paid")
    rows = env.engine.list_missing_events()
    paid_row = [r for r in rows if r["node_code"] == "paid"][0]
    assert paid_row["status"] == "resolved"
    assert paid_row["resolve_reason"] == "node_completed"


def test_missing_prerequisite_resolves_after_skip_then_cascade(env):
    env.publish(spec=_spec_with_deadlines(risk_b=100))
    env.start("d3")
    env.send("d3", "ship")
    env.send("d3", "paid")
    env.send("d3", "risk_a")
    env.clock.advance(101)
    events = env.engine.scan_deadlines()
    assert events and events[0]["node"] == "risk_b"
    # 跳过可选的 risk_b -> ship（ALL）释放、异常关闭
    key = env.instance_key("d3")
    env.engine.skip_node(key, "risk_b", reason="manual waiver")
    rows = {r["node_code"]: r for r in env.engine.list_missing_events()}
    assert rows["risk_b"]["status"] == "resolved"
    v = env.view(key)
    assert _status(v, "risk_b") == "SKIPPED"
    assert _status(v, "ship") == "COMPLETED"


def test_required_node_cannot_be_skipped(env):
    env.publish()
    env.send("d4", "paid")
    key = env.instance_key("d4")
    with pytest.raises(ConflictStateError):
        env.engine.skip_node(key, "paid", reason="nope")
    with pytest.raises(ConflictStateError):
        env.engine.skip_node(key, "ship", reason="nope")


def test_wrong_correlation_key_becomes_orphan_then_reassociated(env):
    env.publish()
    # 正常实例（正确关联键）
    env.start("d5")
    env.send("d5", "ship")
    env.send("d5", "paid")
    env.send("d5", "risk_a")
    key = env.instance_key("d5")
    # 一条关联键写错的 risk_b（进 orphan 队列，原文保留）
    r = env.engine.ingest("order_flow", env.cb("WRONG-KEY", "risk_b", extra=1),
                          callback_uid="cb-wrong")
    assert r["result"] == "orphan"
    orphans = env.engine.list_orphan_callbacks()
    assert len(orphans) == 1 and orphans[0]["correlation_key"] == "WRONG-KEY"

    # 重新关联到正确实例的 risk_b 节点；ship 同事务释放
    out = env.engine.reassociate_callback(orphans[0]["id"], key, "risk_b",
                                          operator="bob", note="key typo")
    assert out["result"] == "reassociated"
    v = env.view(key)
    assert _status(v, "risk_b") == "COMPLETED"
    assert _status(v, "ship") == "COMPLETED"
    # 归属历史：原 orphan 行保留，新行 bound_from 指向它
    original = env.db.query_one(
        "SELECT * FROM orch_callbacks WHERE callback_uid=?", ("cb-wrong",))
    bound = env.db.query_one(
        "SELECT * FROM orch_callbacks WHERE bound_from_callback_id=?",
        (original["id"],))
    assert bound is not None
    assert bound["ownership"] == "bound"
    assert bound["node_code"] == "risk_b"
    # 原行仍可查（未删除/未改写）
    assert original["ownership"] == "orphan"
    # 重复关联同一 orphan 被拒（只有一个有效结果）
    with pytest.raises(ConflictStateError):
        env.engine.reassociate_callback(original["id"], key, "risk_b")


def test_terminate_then_late_callbacks_recorded_only_as_late(env):
    env.publish()
    env.send("d6", "paid")
    key = env.instance_key("d6")
    env.engine.terminate_instance(key, reason="fraud")
    v = env.view(key)
    assert v["instance"]["status"] == "terminated"
    assert {n["node"]: n["status"] for n in v["nodes"]}["risk_a"] == "TERMINATED"
    # 迟到回调只记 late，不恢复处理
    r = env.send("d6", "risk_a")
    assert r["result"] == "late"
    late_rows = env.db.query(
        "SELECT ownership, late_reason FROM orch_callbacks WHERE ownership='late'")
    assert len(late_rows) == 1 and late_rows[0]["late_reason"] == "instance_terminated"
    v = env.view(key)
    assert _status(v, "risk_a") == "TERMINATED"  # 未被复活
    # open 异常在终止时作废
    env.engine.scan_deadlines()
    assert all(m["status"] in ("voided", "resolved")
               for m in env.engine.list_missing_events())
    # 终止幂等
    assert env.engine.terminate_instance(key, reason="again")["result"] == "noop"


def test_orphan_with_missing_correlation_key(env):
    env.publish()
    payload = '{"event": "paid", "nested": {"x": 1}}'  # 缺 order_id
    r = env.engine.ingest("order_flow", payload, callback_uid="nokey-1")
    assert r["result"] == "orphan" and r["reason"] == "missing_correlation_key"


def test_reassociate_opens_conflict_on_extra_version(env):
    env.publish()
    env.send("d7", "paid")
    key = env.instance_key("d7")
    # 已完成实例 + 一条非根节点事件但关联键错误 -> orphan（不会自动建新实例）
    r = env.engine.ingest("order_flow", env.cb("BAD", "risk_b", v=2),
                          callback_uid="bad-paid")
    assert r["result"] == "orphan"
    # 直接重关联到已完成的 paid 节点 -> 完成后冲突（内容不同的新版本）
    out = env.engine.reassociate_callback(r["callback_id"], key, "paid")
    assert out["conflict"] == "open"
    v = env.view(key)
    assert env.node(v, "paid")["conflict"] == "open"
    assert _status(v, "paid") == "COMPLETED"  # 已完成不回退


def test_deadline_scan_concurrent_with_callback_has_single_outcome(env):
    """期限扫描与回调到达并发：只有一个有效结果（异常 open 或回调释放）。"""
    env.publish(spec=_spec_with_deadlines(paid=100))
    env.start("d8")
    env.clock.advance(101)
    barrier = threading.Barrier(2)
    outcomes = []

    def scan():
        barrier.wait()
        outcomes.append(("scan", env.engine.scan_deadlines()))

    def callback():
        barrier.wait()
        outcomes.append(("cb", env.send("d8", "paid")))

    t1 = threading.Thread(target=scan)
    t2 = threading.Thread(target=callback)
    t1.start(); t2.start(); t1.join(); t2.join()

    rows = env.engine.list_missing_events()
    paid = [r for r in rows if r["node_code"] == "paid"]
    # 要么没有异常（回调先赢），要么异常已被回调解决；绝不可能同时 open + 已完成
    v = env.view(env.instance_key("d8"))
    assert _status(v, "paid") == "COMPLETED"
    assert all(r["status"] != "open" for r in paid)
    # 业务只执行一次
    assert len([c for c in env.handler.calls if c[1] == "paid"]) == 1


def test_audit_is_ordered_per_instance(env):
    env.publish()
    env.send("d9", "paid")
    env.send("d9", "risk_a")
    key = env.instance_key("d9")
    v = env.view(key)
    seqs = [a["seq"] for a in v["audit"]]
    assert seqs == sorted(seqs) and len(seqs) == len(set(seqs))
    types = [a["event_type"] for a in v["audit"]]
    assert types[0] == "instance_created"
    assert "node_completed" in types
    # 图版本变化不写实例 seq，但出现在全局审计
    env.publish()
    global_events = env.engine.global_audit(event_type="graph_published")
    assert global_events and global_events[0]["seq"] is None


def test_reassociate_to_unknown_instance_or_node_rejected(env):
    env.publish()
    # 缺关联键的回调必然进 orphan（不会自动建实例）
    r = env.engine.ingest("order_flow",
                          '{"event": "paid", "nested": {"x": 1}}',
                          callback_uid="x1")
    assert r["result"] == "orphan"
    with pytest.raises(NotFoundError):
        env.engine.reassociate_callback(r["callback_id"], "nope", "paid")

"""核心编排：乱序持久化等待、原子级联释放、ALL/ANY 汇合只释放一次、幂等、版本固定。"""
from __future__ import annotations

from conftest import standard_spec


def _status(view, code):
    return next(n for n in view["nodes"] if n["node"] == code)["status"]


def test_callbacks_persist_waiting_until_prerequisites_met(env):
    env.publish()
    env.start("o1")  # 流程启动：实例固定当前图版本
    # ship 在 paid/risk_a/risk_b 之前到达：持久化等待，不执行业务
    env.send("o1", "ship")
    key = env.instance_key("o1")
    v = env.view(key)
    assert _status(v, "ship") == "WAITING"
    ship = env.node(v, "ship")
    assert ship["category"] == "not_arrived" or ship["version_count"] == 1
    assert ship["version_count"] == 1  # 回调已落盘等待（未到达=从未收到；此处收到但前置未满足）
    assert ship["category"] == "waiting"

    # 补齐前置：paid -> risk_a -> risk_b，ship 在同一事务原子释放并完成
    env.send("o1", "paid")
    env.send("o1", "risk_a")
    env.send("o1", "risk_b")
    v = env.view(key)
    assert _status(v, "paid") == "COMPLETED"
    assert _status(v, "ship") == "COMPLETED"
    # 每个节点业务处理器恰好执行一次
    paid_calls = [c for c in env.handler.calls if c[1] == "ship"]
    assert len(paid_calls) == 1


def test_atomic_release_of_all_newly_ready_successors(env):
    """前置完成后同一事务释放所有刚刚就绪的后继（risk_a/risk_b 同时开放）。"""
    env.publish()
    env.send("o2", "paid")
    key = env.instance_key("o2")
    v = env.view(key)
    # paid 完成的同一事务里，risk_a/risk_b 不会被误执行（数据未到，仅前置就绪）
    assert _status(v, "risk_a") == "WAITING"
    assert _status(v, "risk_b") == "WAITING"
    # 汇合点回调先到（持久化等待）；两条分支齐后同事务释放
    env.send("o2", "notify")
    env.send("o2", "ship")
    env.send("o2", "risk_a")
    env.send("o2", "risk_b")
    v = env.view(key)
    assert _status(v, "risk_a") == "COMPLETED"
    assert _status(v, "risk_b") == "COMPLETED"
    assert _status(v, "ship") == "COMPLETED"   # ALL 汇合：两分支齐
    assert _status(v, "notify") == "COMPLETED"  # ANY 汇合：第一条分支即释放


def test_any_join_releases_once(env):
    """任一汇合按 ANY 规则只释放一次（第二个分支到达不重复执行）。"""
    env.publish()
    env.start("o3")
    env.send("o3", "notify")  # 汇合点回调先到
    env.send("o3", "paid")
    env.send("o3", "risk_a")
    key = env.instance_key("o3")
    v = env.view(key)
    assert _status(v, "notify") == "COMPLETED"
    notify_calls = [c for c in env.handler.calls if c[1] == "notify"]
    assert len(notify_calls) == 1

    env.send("o3", "risk_b")
    v = env.view(key)
    assert _status(v, "notify") == "COMPLETED"
    notify_calls = [c for c in env.handler.calls if c[1] == "notify"]
    assert len(notify_calls) == 1  # 仍然只有一次


def test_all_join_waits_for_every_branch(env):
    env.publish()
    env.start("o4")
    env.send("o4", "notify")
    env.send("o4", "ship")   # ALL 汇合点回调先到，等待两分支
    env.send("o4", "paid")
    env.send("o4", "risk_a")
    key = env.instance_key("o4")
    v = env.view(key)
    # ANY 节点 notify 已释放；ALL 节点 ship 仍等 risk_b
    assert _status(v, "notify") == "COMPLETED"
    assert _status(v, "ship") == "WAITING"
    unfinished = env.node(v, "ship")["unfinished_prerequisites"]
    assert unfinished == ["risk_b"]

    env.send("o4", "risk_b")
    v = env.view(key)
    assert _status(v, "ship") == "COMPLETED"


def test_duplicate_callback_same_correlation_key_is_idempotent(env):
    env.publish()
    env.start("o5")
    env.send("o5", "ship")  # ship 回调先到，节点保持 WAITING，可继续收同节点投递
    r1 = env.send("o5", "paid", amount=100)
    r2 = env.send("o5", "paid", amount=100)  # 内容一致
    assert r1["result"] == "received"
    assert r2["result"] == "duplicate"
    key = env.instance_key("o5")
    v = env.view(key)
    paid = env.node(v, "paid")
    assert paid["version_count"] == 1  # 不同版本只有一份
    paid_calls = [c for c in env.handler.calls if c[1] == "paid"]
    assert len(paid_calls) == 1

    # 显式 callback_uid 重复投递也幂等（uid 去重优先，内容不同也不产生第二版本）
    r3 = env.send("o5", "notify", uid="cb-uid-1", amount=100)
    r4 = env.send("o5", "notify", uid="cb-uid-1", amount=999)
    assert r3["result"] == "received"
    assert r4["result"] == "duplicate"
    v = env.view(key)
    assert env.node(v, "notify")["version_count"] == 1


def test_conflict_blocks_graph_until_selected_and_then_advances(env):
    """同节点内容不同的版本：超额版本打开冲突；未选定前不推进，选定结果冻结。"""
    env.publish()
    env.send("o6", "paid", score=1)
    # paid 已完成后，到达一份不同内容 -> 完成后冲突（效果不撤回、业务不二次执行）
    env.send("o6", "paid", score=2)
    key = env.instance_key("o6")
    v = env.view(key)
    paid = env.node(v, "paid")
    assert paid["conflict"] == "open"
    assert paid["status"] == "COMPLETED"
    candidates = [ver["callback_id"] for ver in paid["versions"]
                  if "duplicate_of" not in ver]
    assert len(candidates) == 2
    paid_calls = [c for c in env.handler.calls if c[1] == "paid"]
    assert len(paid_calls) == 1  # 冲突绝不触发第二次业务处理

    # 多次数节点：occurrences=2 时必须恰好两份，第三份打开冲突且节点不完成
    spec = standard_spec()
    spec["nodes"][0]["occurrences"] = 2  # paid 需要两份不同内容
    env.publish(process_type="dual", spec=spec)
    env.send("d1", "paid", part=1, process_type="dual")
    v = env.view(env.instance_key("d1", "dual"), "dual")
    assert _status(v, "paid") == "WAITING"  # 还缺一份
    env.send("d1", "paid", part=2, process_type="dual")
    v = env.view(env.instance_key("d1", "dual"), "dual")
    assert _status(v, "paid") == "COMPLETED"  # 恰好两份，完成
    env.send("d1", "paid", part=3, process_type="dual")
    v = env.view(env.instance_key("d1", "dual"), "dual")
    paid = env.node(v, "paid")
    assert paid["conflict"] == "open"
    assert _status(v, "risk_a") == "WAITING"  # 未选定前后继不能推进（这里 paid 已完成，
    # 后继本来就在等数据，关键断言为冲突 open；见跳过/选定流程）

    # 人工选定第二份，冲突解决（结果冻结）
    chosen = [x for x in paid["versions"] if "duplicate_of" not in x][1]["callback_id"]
    env.engine.resolve_conflict(env.instance_key("d1", "dual"), "paid", chosen,
                                operator="alice")
    v = env.view(env.instance_key("d1", "dual"), "dual")
    assert env.node(v, "paid")["conflict"] == "resolved"
    assert env.node(v, "paid")["adopted_callback_id"] == chosen

    # 不能改选（沿用现有冲突处置结果）
    import pytest
    from app.orchestration.engine import ConflictStateError
    other = [x for x in paid["versions"] if "duplicate_of" not in x][0]["callback_id"]
    with pytest.raises(ConflictStateError):
        env.engine.resolve_conflict(env.instance_key("d1", "dual"), "paid", other,
                                    operator="bob")


def test_new_instance_pins_graph_version_at_creation(env):
    env.publish()
    env.send("o7", "paid")
    old_key = env.instance_key("o7")
    # 发布 v2：新增一个可选节点（结构变化仅作版本识别）
    spec2 = standard_spec()
    spec2["nodes"].append({"code": "extra", "occurrences": 1, "key_path": "event",
                           "required": False, "skippable": True,
                           "depends_on": ["paid"]})
    env.publish(spec=spec2)
    env.send("o8", "paid")
    new_key = env.instance_key("o8")
    old_nodes = {n["node"] for n in env.view(old_key)["nodes"]}
    new_nodes = {n["node"] for n in env.view(new_key)["nodes"]}
    assert "extra" not in old_nodes
    assert "extra" in new_nodes


def test_instance_completes_when_all_nodes_done_or_skipped(env):
    env.publish()
    env.start("o9")
    env.send("o9", "notify")
    env.send("o9", "ship")
    env.send("o9", "paid")
    env.send("o9", "risk_a")
    # 跳过 risk_b（可选），ship（ALL）仍应满足并完成
    env.engine.skip_node(env.instance_key("o9"), "risk_b", reason="not needed")
    v = env.view(env.instance_key("o9"))
    assert v["instance"]["status"] == "completed"


def test_status_categories(env):
    """管理员五档：已完成/处理中/等待/阻塞/未到达。"""
    env.publish()
    env.start("o10")
    env.send("o10", "ship")
    key = env.instance_key("o10")
    v = env.view(key)
    cats = {n["node"]: n["category"] for n in v["nodes"]}
    assert cats["ship"] == "waiting"       # 收到回调但等前置
    assert cats["paid"] == "not_arrived"   # 从未收到

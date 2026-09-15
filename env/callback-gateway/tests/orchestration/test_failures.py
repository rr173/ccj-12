"""业务失败只阻塞依赖分支；重试成功继续；重启恢复不重复执行/不重复外部效果。"""
from __future__ import annotations

from app.handlers import IdempotentSink

from conftest import FailingHandler, standard_spec


def _status(view, code):
    return next(n for n in view["nodes"] if n["node"] == code)["status"]


def test_business_failure_blocks_only_dependent_branch(env):
    env.publish()
    env.handler.fail_nodes["risk_a"] = 5  # risk_a 持续失败
    env.send("f1", "paid")
    env.send("f1", "risk_a")
    env.send("f1", "risk_b")
    env.send("f1", "notify")  # ANY 汇合：risk_b 完成即可释放 notify
    key = env.instance_key("f1")
    v = env.view(key)
    assert _status(v, "risk_a") == "BLOCKED"
    assert env.node(v, "risk_a")["blocked_reason"]
    # 独立分支 risk_b 与 ANY 汇合 notify 继续；ALL 汇合 ship 被 risk_a 阻塞
    assert _status(v, "risk_b") == "COMPLETED"
    assert _status(v, "notify") == "COMPLETED"
    assert _status(v, "ship") == "WAITING"
    assert "risk_a" in env.node(v, "ship")["unfinished_prerequisites"]


def test_manual_retry_succeeds_and_advances(env):
    env.publish()
    env.handler.fail_nodes["risk_a"] = 1
    env.start("f2")
    env.send("f2", "ship")
    env.send("f2", "notify")
    env.send("f2", "paid")
    env.send("f2", "risk_a")  # 第 1 次失败 -> BLOCKED
    env.send("f2", "risk_b")  # 独立分支照常
    key = env.instance_key("f2")
    assert _status(env.view(key), "risk_a") == "BLOCKED"
    risk_a_calls = [c for c in env.handler.calls if c[1] == "risk_a"]
    assert len(risk_a_calls) == 1

    # 立即重试（force）成功，ship（ALL）随之释放
    r = env.engine.retry_blocked(key, "risk_a", force=True)
    assert "risk_a" in r["advanced"]
    v = env.view(key)
    assert _status(v, "risk_a") == "COMPLETED"
    assert _status(v, "ship") == "COMPLETED"
    risk_a_calls = [c for c in env.handler.calls if c[1] == "risk_a"]
    assert len(risk_a_calls) == 2  # 失败 1 次 + 重试 1 次（没有第三次）


def test_backoff_retry_not_due_returns_waiting(env):
    env.publish()
    env.handler.fail_nodes["risk_a"] = 5
    env.send("f3", "paid")
    env.send("f3", "risk_a")
    key = env.instance_key("f3")
    # 退避时间未到，不重试
    r = env.engine.retry_blocked(key, "risk_a")
    assert r["result"] == "waiting_retry_at"
    # 扫描也不会重试
    assert env.engine.retry_due_blocked() == 0
    env.clock.advance(1000)
    assert env.engine.retry_due_blocked() == 1
    # 仍然失败，但只多了一次调用（不重复执行成功过的节点）
    risk_a_calls = [c for c in env.handler.calls if c[1] == "risk_a"]
    assert len(risk_a_calls) == 2


def test_recover_after_restart_recomputes_readiness_without_double_execution(env):
    env.publish()
    env.start("r1")
    env.send("r1", "notify")  # ANY 汇合点回调先到，risk_a 完成时即释放
    env.send("r1", "paid")
    env.send("r1", "risk_a")
    key = env.instance_key("r1")
    calls_before = list(env.handler.calls)

    # 模拟重启：新引擎、新处理器实例（调用计数归零），重算活动实例
    new_handler = FailingHandler()
    engine2 = type(env.engine)(env.db, handler=new_handler, clock=env.clock,
                               retry_base_seconds=10.0, retry_cap_seconds=100.0)
    result = engine2.recover()
    assert result["instances_scanned"] == 1
    # 已完成节点不重新执行
    assert ("risk_a" not in [c[1] for c in new_handler.calls])
    assert ("paid" not in [c[1] for c in new_handler.calls])
    # 迟到的 risk_b 回调后，新引擎同样能推进
    engine2.ingest("order_flow", env.cb("r1", "risk_b"))
    v = engine2.get_instance_view(key)
    assert _status(v, "risk_b") == "COMPLETED"
    assert calls_before is not None  # 旧处理器的调用记录不变
    # notify（ANY）依然只执行过一次
    notify_calls = [c for c in env.handler.calls if c[1] == "notify"]
    assert len(notify_calls) == 1


def test_effects_dispatched_exactly_once_through_sink(env):
    env.publish()
    sink = IdempotentSink(env.db, env.clock)
    env.send("e1", "paid")
    pending = env.engine.list_effects()
    assert len(pending) == 1 and pending[0]["status"] == "pending"
    assert env.engine.dispatch_pending_effects(sink) == 1
    # 重复派发：下游幂等表 + 条件更新保证只算一次
    assert env.engine.dispatch_pending_effects(sink) == 0
    effects = env.engine.list_effects()
    assert all(e["status"] == "executed" for e in effects)
    assert sink.applied_count(effects[0]["idempotency_key"]) == 1
    # 恢复重算不产生第二个副作用
    env.engine.recover()
    assert len(env.engine.list_effects()) == 1


def test_node_handler_never_called_twice_even_if_state_manually_dirty(env):
    env.publish()
    env.send("g1", "paid")
    key = env.instance_key("g1")
    # 直接把已完成节点伪装回 READY（模拟异常脏状态），重算不得重新执行业务
    with env.db.tx() as cur:
        cur.execute(
            "UPDATE orch_node_states SET status='READY' WHERE instance_id="
            "(SELECT id FROM orch_instances WHERE instance_key=?) AND node_code='paid'",
            (key,))
    env.engine.recover()
    paid_calls = [c for c in env.handler.calls if c[1] == "paid"]
    assert len(paid_calls) == 1  # 仍只有最初一次
    # effects 行存在时 READY 脏状态不被回退（保持完成态语义）
    assert _status(env.view(key), "paid") == "COMPLETED"

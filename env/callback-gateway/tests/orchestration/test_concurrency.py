"""并发压力：扫描/回调/人工处置并发时只有一个有效结果；业务不重复执行。"""
from __future__ import annotations

import threading

from conftest import standard_spec


def _spec(deadlines):
    spec = standard_spec()
    for node in spec["nodes"]:
        if node["code"] in deadlines:
            node["wait_seconds"] = deadlines[node["code"]]
    return spec


def _status(view, code):
    return next(n for n in view["nodes"] if n["node"] == code)["status"]


def test_concurrent_callbacks_scans_and_manual_actions_single_outcome(env):
    spec = _spec({"paid": 50, "risk_a": 1000, "risk_b": 1000,
                  "ship": 1000, "notify": 1000})
    env.publish(spec=spec)
    n = 12
    for i in range(n):
        env.start(f"c{i}")
    env.clock.advance(60)

    errors = []

    def worker_scan():
        try:
            for _ in range(5):
                env.engine.scan_deadlines()
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def worker_paid(i):
        try:
            env.send(f"c{i}", "paid", seq=i)
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    def worker_skip(i):
        try:
            key = env.instance_key(f"c{i}")
            try:
                env.engine.skip_node(key, "risk_a", reason="concurrent")
            except Exception:  # noqa: BLE001
                pass
        except Exception as exc:  # noqa: BLE001
            errors.append(exc)

    threads = [threading.Thread(target=worker_scan) for _ in range(4)]
    threads += [threading.Thread(target=worker_paid, args=(i,)) for i in range(n)]
    threads += [threading.Thread(target=worker_skip, args=(i,)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    for i in range(n):
        key = env.instance_key(f"c{i}")
        v = env.view(key)
        # paid 完成恰好一次
        inst_id = env.db.query_one(
            "SELECT id FROM orch_instances WHERE instance_key=?", (key,))["id"]
        paid_calls = [c for c in env.handler.calls if c == (inst_id, "paid")]
        assert len(paid_calls) == 1
        assert _status(v, "paid") == "COMPLETED"
        # 每实例×节点至多一条 open 缺失异常
        open_rows = [m for m in env.engine.list_missing_events(status="open")
                     if m["instance_key"] == key and m["node_code"] == "paid"]
        assert len(open_rows) <= 1


def test_concurrent_terminate_and_callback_only_late_or_processed(env):
    env.publish()
    n = 8
    for i in range(n):
        env.start(f"t{i}")
    barrier = threading.Barrier(2 * n)

    def terminate(i):
        barrier.wait()
        env.engine.terminate_instance(env.instance_key(f"t{i}"), reason="x")

    def late_cb(i):
        barrier.wait()
        env.send(f"t{i}", "paid")

    ts = []
    for i in range(n):
        ts.append(threading.Thread(target=terminate, args=(i,)))
        ts.append(threading.Thread(target=late_cb, args=(i,)))
    for t in ts:
        t.start()
    for t in ts:
        t.join()

    for i in range(n):
        key = env.instance_key(f"t{i}")
        v = env.view(key)
        # 每个实例要么终止（回调变 late），要么回调先生效完成；绝不出现 late 后又被处理
        paid_status = _status(v, "paid")
        assert paid_status in ("TERMINATED", "COMPLETED")
        if paid_status == "TERMINATED":
            late = env.db.query(
                "SELECT COUNT(*) AS c FROM orch_callbacks WHERE instance_id="
                "(SELECT id FROM orch_instances WHERE instance_key=?) "
                "AND ownership='late'", (key,))[0]["c"]
            assert late == 1
        # 业务对每个 paid 节点至多执行一次
        paid_effects = env.db.query(
            "SELECT COUNT(*) AS c FROM orch_effects WHERE instance_id="
            "(SELECT id FROM orch_instances WHERE instance_key=?) AND node_code='paid'",
            (key,))[0]["c"]
        assert paid_effects <= 1


def test_concurrent_reassociate_and_deadline_single_winner(env):
    env.publish(spec=_spec({"risk_b": 30}))
    env.start("r0")
    env.send("r0", "ship")
    env.send("r0", "paid")
    env.send("r0", "risk_a")
    # 错误关联键的 risk_b 进 orphan
    r = env.engine.ingest("order_flow", env.cb("WRONG", "risk_b"),
                          callback_uid="w1")
    assert r["result"] == "orphan"
    orphan_id = r["callback_id"]
    key = env.instance_key("r0")
    env.clock.advance(31)
    outcomes = []

    def scan():
        outcomes.append(("scan", env.engine.scan_deadlines()))

    def relink():
        try:
            outcomes.append(("relink",
                             env.engine.reassociate_callback(orphan_id, key, "risk_b")))
        except Exception as exc:  # noqa: BLE001
            outcomes.append(("relink_error", str(exc)))

    t1 = threading.Thread(target=scan)
    t2 = threading.Thread(target=relink)
    t1.start(); t2.start(); t1.join(); t2.join()

    v = env.view(key)
    # 重关联赢则 risk_b 完成、异常 resolved；扫描赢则 open 异常一条，二者互斥
    open_missing = [m for m in v["missing_events"]
                    if m["node_code"] == "risk_b" and m["status"] == "open"]
    if _status(v, "risk_b") == "COMPLETED":
        assert not open_missing
    else:
        assert len(open_missing) == 1

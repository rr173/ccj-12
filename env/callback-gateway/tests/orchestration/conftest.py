"""编排模块测试夹具：内存库 + 可控时钟 + 可注入失败的业务处理器。"""
from __future__ import annotations

import json

import pytest

from app.db import Database
from app.orchestration.engine import (
    OrchestrationEngine, BusinessFailure, default_business_handler,
)


class FakeClock:
    def __init__(self, t: float = 1000.0):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float):
        self.t += seconds
        return self.t


class FailingHandler:
    """按节点配置失败次数：fail_nodes={code: 剩余失败次数}，失败耗尽后成功。"""

    def __init__(self, fail_nodes: dict | None = None,
                 record: list | None = None):
        self.fail_nodes = dict(fail_nodes or {})
        self.record = record if record is not None else []
        self.calls = []

    def __call__(self, instance, node_code, payloads):
        self.calls.append((instance["id"], node_code))
        self.record.append(node_code)
        remaining = self.fail_nodes.get(node_code, 0)
        if remaining > 0:
            self.fail_nodes[node_code] = remaining - 1
            raise BusinessFailure(f"injected failure at {node_code}")
        return default_business_handler(instance, node_code, payloads)


# 标准测试图：paid -> (risk_a 可选, risk_b 可选) -> ship（ALL 汇合）
# notify 是 ANY 汇合（risk_a/risk_b 任一完成即释放）
# 关联键取 order_id；节点识别取 event 字段（节点级 key_path 覆盖）
def standard_spec(**overrides):
    def n(code, **kw):
        return {"code": code, "occurrences": 1, "key_path": "event", **kw}

    nodes = [
        n("paid", required=True),
        n("risk_a", skippable=True, depends_on=["paid"], **overrides.get("risk_a", {})),
        n("risk_b", skippable=True, depends_on=["paid"], **overrides.get("risk_b", {})),
        n("ship", required=True, join="ALL", depends_on=["risk_a", "risk_b"],
          **overrides.get("ship", {})),
        n("notify", required=False, join="ANY",
          depends_on=[{"code": "risk_a"}, {"code": "risk_b"}],
          **overrides.get("notify", {})),
    ]
    return {"key_path": "order_id", "nodes": nodes}


def cb(order_id, event, **extra):
    return json.dumps({"order_id": order_id, "event": event, **extra},
                      ensure_ascii=False)


@pytest.fixture()
def env(tmp_path):
    """构建引擎环境包：db/engine/clock/spec/发布助手/回调助手。"""
    db = Database(str(tmp_path / "orch.db"))
    clock = FakeClock()
    handler = FailingHandler()
    engine = OrchestrationEngine(db, handler=handler, clock=clock,
                                 retry_base_seconds=10.0, retry_cap_seconds=100.0)

    def publish(process_type="order_flow", spec=None, operator="ops", note=None):
        return engine.publish_graph(process_type, spec or standard_spec(),
                                    operator=operator, note=note)

    def send(order_id, event, process_type="order_flow", uid=None, **extra):
        return engine.ingest(process_type, cb(order_id, event, **extra),
                             callback_uid=uid)

    def start(order_id, process_type="order_flow", operator="system"):
        """显式创建实例（模拟流程启动注册），之后乱序回调都挂到该实例。"""
        return engine.start_instance(process_type, str(order_id), operator=operator)

    def _instance_key(order_id, process_type="order_flow"):
        # 不限 status：实例可能在最后一次回调的同事务内完成
        row = db.query_one(
            "SELECT instance_key FROM orch_instances WHERE process_type=? "
            "AND correlation_key=? ORDER BY id DESC LIMIT 1",
            (process_type, str(order_id)))
        return row["instance_key"] if row else None

    def view(order_or_key, process_type="order_flow"):
        if isinstance(order_or_key, str) and len(order_or_key) == 32:
            key = order_or_key  # uuid hex
        else:
            key = _instance_key(order_or_key, process_type)
        return engine.get_instance_view(key)

    def node(view_, code):
        return next(n for n in view_["nodes"] if n["node"] == code)

    class Bag:
        pass

    bag = Bag()
    bag.db = db
    bag.engine = engine
    bag.clock = clock
    bag.handler = handler
    bag.publish = publish
    bag.send = send
    bag.start = start
    bag.view = view
    bag.instance_key = _instance_key
    bag.node = node
    bag.cb = cb
    yield bag
    db.close()

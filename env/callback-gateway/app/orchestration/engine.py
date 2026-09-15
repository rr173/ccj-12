"""编排引擎：回调入位、就绪级联、汇合、冲突、失败隔离、重启恢复、人工处置。

事务边界：每个公共方法整体运行在一个 BEGIN IMMEDIATE 写事务里（全库写串行），
因此「回调入口 / 期限扫描 / 重关联 / 跳过 / 终止 / 重启恢复」并发时只有一个有效结果。

实例与回调归属：
- 实例可由根节点回调自动创建，也可经 start_instance 显式开启（实例固定当时图版本）；
- 没有活动实例时，非根节点回调无法判断是「乱序首达」还是「关联键写错」，
  一律落 orphan 待核对队列，由管理员重关联（保留原始归属历史）；
- 实例终止后的回调一律记 late，绝不恢复处理。

冲突语义（同一节点收到内容不同的版本）：
- 次数未收齐（occurrences）期间出现超额版本：conflict=open，未人工选定前不推进；
- 节点已完成后才到达不同版本：登记为完成后冲突（conflict=open），已产生的外部效果
  不撤回、业务绝不第二次执行；人工选定一次即冻结（不能改选），再到新版本沿用该结果。

级联规则（_advance_locked，在单个事务内迭代到不动点）：
- 节点「前置满足」：ALL 汇合要求全部前驱 COMPLETED/SKIPPED；ANY 汇合要求任一前驱
  COMPLETED/SKIPPED（前驱被终止的实例不存在——终止会结束整个实例）；
- 节点「数据就绪」：收到的**内容不同**版本数 == occurrences（同内容重复投递幂等），
  数量超额即冲突（conflict=open），未选定前该节点及其后继都不能推进；
- 前置满足且数据就绪且无冲突：WAITING -> READY，同事务内立即执行业务处理器；
  本轮新满足的所有后继在同一事务内释放（原子释放，不跨事务半开）；
- 业务处理器抛异常：节点 BLOCKED，只阻塞依赖它的分支，其他独立分支继续级联；
- COMPLETED 节点的处理器永不第二次调用（状态条件转移 + effects 唯一键双保险）。
"""
from __future__ import annotations

import hashlib
import json
import logging
import sqlite3
import threading
import time
import uuid
from typing import Any, Callable

from ..db import Database
from . import store
from .model import (
    GraphSpec, NodeSpec, build_graph, graph_from_json,
)

log = logging.getLogger("gateway.orchestration")

# ---- 节点状态 --------------------------------------------------------------
WAITING = "WAITING"
READY = "READY"
PROCESSING = "PROCESSING"
COMPLETED = "COMPLETED"
SKIPPED = "SKIPPED"
BLOCKED = "BLOCKED"
TERMINATED = "TERMINATED"

TERMINAL_STATUSES = (COMPLETED, SKIPPED, TERMINATED)
# 实例完成时每个节点必须落在的状态
DONE_STATUSES = (COMPLETED, SKIPPED)


class OrchestrationError(Exception):
    """编排操作错误（对外稳定 reason 字符串见各子类/消息）。"""


class NotFoundError(OrchestrationError):
    pass


class ConflictStateError(OrchestrationError):
    """操作与当前冲突/状态不符（如重复选定、对必需节点跳过）。"""


class BusinessFailure(Exception):
    """业务处理器可重试失败（由注册的 handler 抛出）。"""


# 业务处理器签名：(instance_row, node_code, payloads: list[dict]) ->
#   {"effects": [{"type": str, "payload": dict}], "checkpoint": dict}
BusinessHandler = Callable[[sqlite3.Row, str, list[dict]], dict]


def default_business_handler(instance, node_code: str, payloads: list[dict]) -> dict:
    """缺省处理器：节点每完成一次产出一个 orch.node_completed 外部效果。"""
    return {
        "checkpoint": {"stage": "completed", "handler": "default/v1"},
        "effects": [{
            "type": "orch.node_completed",
            "payload": {
                "instance_id": instance["instance_key"],
                "process_type": instance["process_type"],
                "node": node_code,
                "documents": payloads,
            },
        }],
    }


def content_hash(payload_text: str) -> str:
    return hashlib.sha256(payload_text.encode("utf-8")).hexdigest()


def canonical_hash(payload: Any) -> str:
    return hashlib.sha256(
        json.dumps(payload, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        .encode("utf-8")
    ).hexdigest()


class OrchestrationEngine:
    def __init__(self, db: Database, handler: BusinessHandler | None = None,
                 clock=time.time, retry_base_seconds: float = 5.0,
                 retry_cap_seconds: float = 300.0):
        self.db = db
        self.handler = handler or default_business_handler
        self.clock = clock
        self.retry_base_seconds = retry_base_seconds
        self.retry_cap_seconds = retry_cap_seconds
        self._graph_cache: dict[int, GraphSpec] = {}
        self._cache_lock = threading.Lock()
        store.init_orch_schema(db)

    # ======================================================================
    # 图发布 / 回滚
    # ======================================================================

    def publish_graph(self, process_type: str, spec: dict, *,
                      operator: str = "ops", note: str | None = None) -> dict:
        """发布新图版本。校验失败整体拒绝（不落指针，留 rejected 记录）。

        返回 {"result":"published","process_type","graph_version_id","version"}。
        """
        now = self.clock()
        raw_text = json.dumps(spec, ensure_ascii=False, sort_keys=True)
        # 先在事务外完成纯函数校验（不占写锁），失败落 rejected 留痕后再抛出
        try:
            preview = build_graph(process_type, 0, spec)
        except Exception as exc:  # noqa: BLE001
            errors = getattr(exc, "errors", [str(exc)])
            with self.db.tx() as cur:
                cur.execute(
                    """INSERT INTO orch_graph_versions
                       (process_type, version, result, spec_json, reason,
                        created_by, note, created_at)
                       VALUES (?, NULL, 'rejected', ?, ?, ?, ?, ?)""",
                    (process_type, raw_text,
                     json.dumps(errors, ensure_ascii=False),
                     operator, note, now),
                )
                store.audit(cur, "graph_publish_rejected", None, None, None,
                            {"process_type": process_type, "operator": operator,
                             "errors": errors}, ts=now)
            raise OrchestrationError(json.dumps(errors, ensure_ascii=False)) from exc

        with self.db.tx() as cur:
            row = cur.execute(
                "SELECT COALESCE(MAX(version),0)+1 AS v FROM orch_graph_versions "
                "WHERE process_type=?", (process_type,)).fetchone()
            next_version = row["v"]
            graph = build_graph(process_type, next_version, spec,
                                created_at=now, created_by=operator, note=note)
            cur.execute(
                """INSERT INTO orch_graph_versions
                   (process_type, version, result, spec_json, created_by, note, created_at)
                   VALUES (?,?, 'applied', ?, ?, ?, ?)""",
                (process_type, next_version, graph.to_json(), operator, note, now),
            )
            version_id = cur.lastrowid
            prev = cur.execute(
                "SELECT graph_version FROM orch_graph_current WHERE process_type=?",
                (process_type,)).fetchone()
            cur.execute(
                """INSERT INTO orch_graph_current (process_type, graph_version, updated_by,
                    updated_at, reason) VALUES (?,?,?,?,?)
                   ON CONFLICT(process_type) DO UPDATE SET
                    graph_version=excluded.graph_version, updated_by=excluded.updated_by,
                    updated_at=excluded.updated_at, reason=excluded.reason""",
                (process_type, version_id, operator, now,
                 f"publish v{next_version}" + (f": {note}" if note else "")),
            )
            store.audit(cur, "graph_published", None, None, None,
                        {"process_type": process_type, "operator": operator,
                         "graph_version_id": version_id, "version": next_version,
                         "previous_version_id": prev["graph_version"] if prev else None,
                         "nodes": list(graph.nodes)}, ts=now)
        with self._cache_lock:
            self._graph_cache.pop(version_id, None)
        return {"result": "published", "process_type": process_type,
                "graph_version_id": version_id, "version": next_version}

    def rollback_graph(self, process_type: str, target_version: int, *,
                       operator: str = "ops", reason: str = "") -> dict:
        """把当前指针回滚到该流程的某个已发布版本；只影响之后新建的实例。

        已发布版本快照不可变（不新增副本）；回滚动作本身落审计。
        """
        now = self.clock()
        with self.db.tx() as cur:
            target = cur.execute(
                """SELECT id, version FROM orch_graph_versions
                   WHERE process_type=? AND version=? AND result='applied'
                   ORDER BY id LIMIT 1""",
                (process_type, target_version)).fetchone()
            if target is None:
                raise NotFoundError(
                    f"流程 {process_type!r} 不存在已发布版本 v{target_version}")
            current = cur.execute(
                "SELECT graph_version FROM orch_graph_current WHERE process_type=?",
                (process_type,)).fetchone()
            if current is None:
                raise NotFoundError(f"流程 {process_type!r} 尚无当前生效版本")
            cur.execute(
                "UPDATE orch_graph_current SET graph_version=?, updated_by=?, updated_at=?, "
                "reason=? WHERE process_type=?",
                (target["id"], operator, now,
                 f"rollback -> v{target_version}: {reason}", process_type),
            )
            store.audit(cur, "graph_rolled_back", None, None, None,
                        {"process_type": process_type, "operator": operator,
                         "target_version": target_version,
                         "target_version_id": target["id"],
                         "previous_version_id": current["graph_version"],
                         "reason": reason}, ts=now)
        return {"result": "rolled_back", "process_type": process_type,
                "version": target_version, "graph_version_id": target["id"]}

    def list_graph_versions(self, process_type: str) -> list[dict]:
        rows = self.db.query(
            """SELECT id, version, result, reason, created_by, note, created_at
               FROM orch_graph_versions WHERE process_type=? ORDER BY id""",
            (process_type,))
        current = self.db.query_one(
            "SELECT graph_version FROM orch_graph_current WHERE process_type=?",
            (process_type,))
        cur_id = current["graph_version"] if current else None
        return [{**dict(r), "current": r["id"] == cur_id} for r in rows]

    def _load_graph(self, cur, version_id: int) -> GraphSpec:
        with self._cache_lock:
            cached = self._graph_cache.get(version_id)
        if cached is not None:
            return cached
        row = cur.execute(
            "SELECT * FROM orch_graph_versions WHERE id=?", (version_id,)).fetchone()
        if row is None:
            raise NotFoundError(f"图版本 id={version_id} 不存在")
        graph = graph_from_json(row["process_type"], row["version"], row["spec_json"],
                                created_at=row["created_at"],
                                created_by=row["created_by"], note=row["note"])
        with self._cache_lock:
            self._graph_cache[version_id] = graph
        return graph

    def _current_graph_id(self, cur, process_type: str) -> int:
        row = cur.execute(
            "SELECT graph_version FROM orch_graph_current WHERE process_type=?",
            (process_type,)).fetchone()
        if row is None:
            raise NotFoundError(f"流程 {process_type!r} 尚未发布事件依赖图")
        return row["graph_version"]

    # ======================================================================
    # 实例
    # ======================================================================

    def _get_instance(self, cur, instance_key: str):
        row = cur.execute(
            "SELECT * FROM orch_instances WHERE instance_key=?", (instance_key,),
        ).fetchone()
        if row is None:
            raise NotFoundError(f"流程实例 {instance_key!r} 不存在")
        return row

    def _create_instance_locked(self, cur, process_type: str, correlation_key: str,
                                graph_version_id: int, now: float,
                                created_by: str = "system"):
        graph = self._load_graph(cur, graph_version_id)
        instance_key = uuid.uuid4().hex
        cur.execute(
            """INSERT INTO orch_instances
               (instance_key, process_type, correlation_key, graph_version_id,
                status, created_by, created_at, updated_at)
               VALUES (?,?,?,?,'active',?,?,?)""",
            (instance_key, process_type, str(correlation_key), graph_version_id,
             created_by, now, now),
        )
        instance_id = cur.lastrowid
        for node in graph.nodes.values():
            deadline = now + node.wait_seconds if node.wait_seconds is not None else None
            cur.execute(
                """INSERT INTO orch_node_states
                   (instance_id, node_code, status, deadline, created_at, updated_at)
                   VALUES (?,?,'WAITING',?,?,?)""",
                (instance_id, node.code, deadline, now, now),
            )
        store.audit(cur, "instance_created", instance_id, None, None,
                    {"instance_key": instance_key, "process_type": process_type,
                     "correlation_key": str(correlation_key),
                     "graph_version_id": graph_version_id,
                     "graph_version": graph.version, "created_by": created_by}, ts=now)
        return cur.execute("SELECT * FROM orch_instances WHERE id=?",
                           (instance_id,)).fetchone()

    def start_instance(self, process_type: str, correlation_key: str, *,
                       operator: str = "system") -> dict:
        """显式创建流程实例（固定创建时生效的图版本）。

        之后同关联键的回调（无论乱序与否）都挂载到本实例；关联键写错的回调因匹配
        不到任何活动实例而进入待核对队列，由管理员重关联。
        """
        now = self.clock()
        with self.db.tx() as cur:
            graph_version_id = self._current_graph_id(cur, process_type)
            existing = cur.execute(
                """SELECT * FROM orch_instances WHERE process_type=? AND correlation_key=?
                   AND status='active'""",
                (process_type, str(correlation_key))).fetchone()
            if existing is not None:
                return {"result": "exists", "instance_id": existing["id"],
                        "instance_key": existing["instance_key"]}
            instance = self._create_instance_locked(
                cur, process_type, correlation_key, graph_version_id, now,
                created_by=operator)
            return {"result": "created", "instance_id": instance["id"],
                    "instance_key": instance["instance_key"]}

    # ======================================================================
    # 回调入口
    # ======================================================================

    def ingest(self, process_type: str, payload_text: str, *,
               callback_uid: str | None = None, received_at: float | None = None,
               create_instance: bool = True) -> dict:
        """接收一份回调；自动识别关联键与流程实例，必要时创建实例（固定当前图版本）。

        - 完全重复（callback_uid 或同实例同节点同内容）-> duplicate，幂等无副作用；
        - 取不到关联键/无活动实例且不允许创建 -> orphan，进待核对队列；
        - 节点收齐后出现内容不同的新版本 -> conflict_opened，未选定前不推进；
        - 正常入位后在同一事务内做就绪级联与业务处理。
        """
        now = received_at if received_at is not None else self.clock()
        try:
            payload = json.loads(payload_text)
        except (TypeError, json.JSONDecodeError) as exc:
            raise OrchestrationError(f"回调内容不是合法 JSON：{exc}") from exc
        p_hash = canonical_hash(payload)
        uid = callback_uid or f"auto:{process_type}:{p_hash}"

        with self.db.tx() as cur:
            existing = cur.execute(
                "SELECT * FROM orch_callbacks WHERE callback_uid=?", (uid,)).fetchone()
            if existing is not None:
                store.audit(cur, "callback_duplicate", existing["instance_id"],
                            existing["node_code"], existing["id"],
                            {"callback_uid": uid, "reason": "uid"}, ts=now)
                return {"result": "duplicate", "callback_id": existing["id"],
                        "instance_id": existing["instance_id"],
                        "reason": "callback_uid"}

            graph_version_id = self._current_graph_id(cur, process_type)
            graph = self._load_graph(cur, graph_version_id)
            correlation_key = graph.extract_correlation_key(payload)

            if correlation_key is None or correlation_key == "":
                return self._store_orphan(
                    cur, process_type, None, uid, payload_text, p_hash, now,
                    reason="missing_correlation_key")

            # 用「当前图」识别节点（新实例将固定当前版本；旧实例在找到后改用其固定版本）
            matched_current = self._match_node(graph, payload)

            instance = cur.execute(
                """SELECT * FROM orch_instances WHERE process_type=? AND correlation_key=?
                   ORDER BY (status='active') DESC, id DESC LIMIT 1""",
                (process_type, str(correlation_key))).fetchone()

            if instance is not None and instance["status"] == "terminated":
                # 终止后迟到回调只能记录为 late 而不能恢复处理（节点无法识别也记 late）
                term_graph = self._load_graph(cur, instance["graph_version_id"])
                node_code = self._match_node(term_graph, payload)
                return self._store_late(
                    cur, process_type, str(correlation_key), uid, payload_text, p_hash,
                    now, instance["id"], node_code, reason="instance_terminated")

            if instance is None or instance["status"] != "active":
                # 乱序首达与「关联键错误」的区分：根节点回调开启新流程，自动建实例；
                # 非根节点回调没有活动实例可挂载——它要么是关联键写错（真正的实例存在
                # 于另一个关联键下），进待核对队列由管理员重关联；乱序首达的实例必然
                # 先由其根节点回调创建。
                if not create_instance or matched_current is None:
                    return self._store_orphan(
                        cur, process_type, str(correlation_key), uid, payload_text,
                        p_hash, now, reason="no_node_key_match")
                if matched_current not in graph.roots:
                    return self._store_orphan(
                        cur, process_type, str(correlation_key), uid, payload_text,
                        p_hash, now, reason="no_active_instance")
                instance = self._create_instance_locked(
                    cur, process_type, correlation_key, graph_version_id, now)

            # 用实例自己固定的图版本识别节点（后续发布不改变本实例的映射）
            inst_graph = (graph if instance["graph_version_id"] == graph_version_id
                          else self._load_graph(cur, instance["graph_version_id"]))

            node_code = self._match_node(inst_graph, payload)
            if node_code is None:
                return self._store_orphan(
                    cur, process_type, str(correlation_key), uid, payload_text, p_hash,
                    now, reason="no_node_key_match", instance_id=instance["id"])

            state = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
                (instance["id"], node_code)).fetchone()
            if state["status"] == TERMINATED:
                return self._store_late(
                    cur, process_type, str(correlation_key), uid, payload_text, p_hash,
                    now, instance["id"], node_code, reason="instance_terminated")

            versions = json.loads(state["versions_json"])
            same = [v for v in versions if v["hash"] == p_hash]
            if state["status"] == COMPLETED and not same:
                # 已完成节点收到内容不同的新版本：登记并打开冲突，沿用现有处置结果
                # （已产生的外部效果不撤回；人工选定后冻结，业务绝不第二次执行）。
                return self._open_post_completion_conflict(
                    cur, instance, inst_graph, state, node_code, uid, process_type,
                    correlation_key, payload_text, p_hash, now)
            if state["status"] in DONE_STATUSES:
                return self._store_late(
                    cur, process_type, str(correlation_key), uid, payload_text, p_hash,
                    now, instance["id"], node_code,
                    reason=f"node_{state['status'].lower()}")
            if same:
                cur.execute(
                    """INSERT INTO orch_callbacks
                       (callback_uid, process_type, correlation_key, payload_hash, payload,
                        received_at, ownership, instance_id, node_code)
                       VALUES (?,?,?,?,?,?,'received',?,?)""",
                    (uid, process_type, str(correlation_key), p_hash, payload_text,
                     now, instance["id"], node_code),
                )
                cb_id = cur.lastrowid
                # 同一关联键重复回调保持幂等：内容一致只追加一条重复记录，不增加版本数
                versions.append({"callback_id": cb_id, "hash": p_hash, "received_at": now,
                                 "duplicate_of": same[0]["callback_id"]})
                cur.execute(
                    "UPDATE orch_node_states SET versions_json=?, updated_at=? WHERE id=?",
                    (json.dumps(versions, ensure_ascii=False), now, state["id"]),
                )
                store.audit(cur, "callback_duplicate", instance["id"], node_code, cb_id,
                            {"callback_uid": uid,
                             "duplicate_of": same[0]["callback_id"]}, ts=now)
                result = {"result": "duplicate", "callback_id": cb_id,
                          "instance_id": instance["id"], "instance_key":
                          instance["instance_key"], "node": node_code,
                          "reason": "same_content"}
                self._advance_locked(cur, instance["id"], now)
                return result

            cur.execute(
                """INSERT INTO orch_callbacks
                   (callback_uid, process_type, correlation_key, payload_hash, payload,
                    received_at, ownership, instance_id, node_code)
                   VALUES (?,?,?,?,?,?,'received',?,?)""",
                (uid, process_type, str(correlation_key), p_hash, payload_text, now,
                 instance["id"], node_code),
            )
            cb_id = cur.lastrowid
            versions.append({"callback_id": cb_id, "hash": p_hash, "received_at": now})
            node_spec = inst_graph.nodes[node_code]
            distinct_count = len([v for v in versions if "duplicate_of" not in v])
            conflict = state["conflict"]
            if distinct_count > node_spec.occurrences:
                conflict = "open"
                store.audit(cur, "conflict_opened", instance["id"], node_code, cb_id,
                            {"versions": distinct_count,
                             "occurrences": node_spec.occurrences,
                             "new_callback_id": cb_id}, ts=now)

            cur.execute(
                """UPDATE orch_node_states SET versions_json=?, conflict=?, updated_at=?
                   WHERE id=?""",
                (json.dumps(versions, ensure_ascii=False), conflict, now, state["id"]),
            )
            # 若此前已人工解决（收到替代版本重新打开冲突），清除旧选定
            if conflict == "open":
                cur.execute(
                    "UPDATE orch_node_states SET selected_callback_id=NULL WHERE id=?",
                    (state["id"],))
            store.audit(cur, "callback_received", instance["id"], node_code, cb_id,
                        {"callback_uid": uid, "hash": p_hash,
                         "version_count": distinct_count,
                         "occurrences": node_spec.occurrences,
                         "conflict": conflict}, ts=now)
            # 该节点对应缺失事件异常若存在，是否解除由推进结果决定
            advanced = self._advance_locked(cur, instance["id"], now)
            return {"result": "received", "callback_id": cb_id,
                    "instance_id": instance["id"], "instance_key": instance["instance_key"],
                    "node": node_code, "version_count": distinct_count,
                    "conflict": conflict, "advanced": advanced}

    def _open_post_completion_conflict(self, cur, instance, graph, state, node_code,
                                       uid, process_type, correlation_key,
                                       payload_text, p_hash, now) -> dict:
        """已完成节点收到内容不同的新版本：登记为收到的回调并打开冲突。

        已产生的外部效果不撤回、业务不第二次执行；冲突沿用「未选定不推进」语义——
        节点本身已完成，但其一致性冲突保持 open，直到人工选定（结果随后冻结）。
        """
        cur.execute(
            """INSERT INTO orch_callbacks
               (callback_uid, process_type, correlation_key, payload_hash, payload,
                received_at, ownership, instance_id, node_code)
               VALUES (?,?,?,?,?,?,'received',?,?)""",
            (uid, process_type, str(correlation_key), p_hash, payload_text, now,
             instance["id"], node_code),
        )
        cb_id = cur.lastrowid
        versions = json.loads(state["versions_json"])
        versions.append({"callback_id": cb_id, "hash": p_hash, "received_at": now})
        node_spec = graph.nodes[node_code]
        # 已有处置结果（resolved）时沿用：不再重新打开冲突，新回调进入候选列表
        conflict = "open" if state["conflict"] != "resolved" else "resolved"
        cur.execute(
            """UPDATE orch_node_states SET versions_json=?, conflict=?, updated_at=?
               WHERE id=?""",
            (json.dumps(versions, ensure_ascii=False), conflict, now, state["id"]),
        )
        store.audit(cur, "conflict_opened", instance["id"], node_code, cb_id,
                    {"versions": len(versions), "occurrences": node_spec.occurrences,
                     "new_callback_id": cb_id, "post_completion": True,
                     "frozen_resolution": conflict == "resolved"}, ts=now)
        return {"result": "received", "callback_id": cb_id,
                "instance_id": instance["id"], "instance_key": instance["instance_key"],
                "node": node_code, "version_count": len(versions),
                "conflict": conflict, "advanced": [],
                "note": "post_completion_conflict"}

    @staticmethod
    def _match_node(graph: GraphSpec, payload: Any) -> str | None:
        """按节点自己的 key_path（缺省用图级 key_path）从内容取值并与节点 code 匹配。

        约定：取值位置取出的值等于节点 code（或节点 code 列表）即识别为该节点；
        取不到任何节点键时返回 None（orphan/no_node_key_match）。
        """
        for code, node in graph.nodes.items():
            value = __class__._extract_node_key(graph, node, payload)
            if value is None:
                continue
            values = value if isinstance(value, list) else [value]
            if code in [str(v) for v in values if v is not None]:
                return code
        return None

    @staticmethod
    def _extract_node_key(graph: GraphSpec, node: NodeSpec, payload: Any):
        from .model import extract_key
        return extract_key(payload, graph.node_key_segments(node))

    def _store_orphan(self, cur, process_type, correlation_key, uid, payload_text,
                      p_hash, now, reason, instance_id=None):
        cur.execute(
            """INSERT INTO orch_callbacks
               (callback_uid, process_type, correlation_key, payload_hash, payload,
                received_at, ownership, instance_id)
               VALUES (?,?,?,?,?,?,'orphan',?)""",
            (uid, process_type,
             None if correlation_key is None else str(correlation_key),
             p_hash, payload_text, now, instance_id),
        )
        cb_id = cur.lastrowid
        store.audit(cur, "callback_orphaned", instance_id, None, cb_id,
                    {"callback_uid": uid, "process_type": process_type,
                     "correlation_key": correlation_key, "reason": reason}, ts=now)
        return {"result": "orphan", "callback_id": cb_id, "reason": reason}

    def _store_late(self, cur, process_type, correlation_key, uid, payload_text,
                    p_hash, now, instance_id, node_code, reason):
        cur.execute(
            """INSERT INTO orch_callbacks
               (callback_uid, process_type, correlation_key, payload_hash, payload,
                received_at, ownership, instance_id, node_code, late_reason)
               VALUES (?,?,?,?,?,?,'late',?,?,?)""",
            (uid, process_type, str(correlation_key), p_hash, payload_text, now,
             instance_id, node_code, reason),
        )
        cb_id = cur.lastrowid
        store.audit(cur, "callback_late", instance_id, node_code, cb_id,
                    {"callback_uid": uid, "reason": reason}, ts=now)
        return {"result": "late", "callback_id": cb_id, "instance_id": instance_id,
                "node": node_code, "reason": reason}

    # ======================================================================
    # 就绪判定与级联
    # ======================================================================

    def _prerequisites_met(self, graph: GraphSpec, states: dict[str, sqlite3.Row],
                           node: NodeSpec) -> tuple[bool, list[str]]:
        """返回 (是否满足, 未完成前驱列表)。ANY 汇合只需任一前驱 DONE。"""
        if not node.depends_on:
            return True, []
        unfinished = [d for d in node.depends_on
                      if states[d]["status"] not in DONE_STATUSES]
        if node.join == "ANY":
            return (len(unfinished) < len(node.depends_on)), unfinished
        return (not unfinished), unfinished

    def _data_ready(self, node: NodeSpec, state: sqlite3.Row) -> tuple[bool, bool]:
        """返回 (数据是否就绪, 是否存在未解决冲突)。"""
        versions = json.loads(state["versions_json"])
        distinct = [v for v in versions if "duplicate_of" not in v]
        if state["conflict"] == "open":
            return False, True
        if state["conflict"] == "resolved":
            return state["selected_callback_id"] is not None, False
        return len(distinct) >= node.occurrences, False

    def _advance_locked(self, cur, instance_id: int, now: float) -> list[str]:
        """在当前事务内把实例推进到不动点；返回本轮新完成的节点 code 列表。

        一个事务同时完成「释放所有刚就绪后继 + 业务处理 + 写副作用」，
        业务失败只把该节点置 BLOCKED，级联继续沿其他独立分支推进。
        """
        instance = cur.execute("SELECT * FROM orch_instances WHERE id=?",
                               (instance_id,)).fetchone()
        if instance["status"] != "active":
            return []
        graph = self._load_graph(cur, instance["graph_version_id"])
        progressed: list[str] = []
        # 防御：崩溃可能把节点留在 READY/PROCESSING。已有副作用行=业务曾执行成功，
        # 补成 COMPLETED（不重新调用处理器）；无副作用行=未执行，退回 WAITING 重算。
        cur.execute(
            """UPDATE orch_node_states SET status='COMPLETED',
                   completed_at=COALESCE(completed_at, ?), updated_at=?
               WHERE instance_id=? AND status IN ('READY','PROCESSING')
                 AND EXISTS (SELECT 1 FROM orch_effects e
                             WHERE e.instance_id=orch_node_states.instance_id
                               AND e.node_code=orch_node_states.node_code)""",
            (now, now, instance_id))
        cur.execute(
            """UPDATE orch_node_states SET status='WAITING', updated_at=?
               WHERE instance_id=? AND status IN ('READY','PROCESSING')
                 AND NOT EXISTS (SELECT 1 FROM orch_effects e
                                 WHERE e.instance_id=orch_node_states.instance_id
                                   AND e.node_code=orch_node_states.node_code)""",
            (now, instance_id))

        for _ in range(len(graph.nodes) + 1):
            rows = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=?",
                (instance_id,)).fetchall()
            states = {r["node_code"]: r for r in rows}
            released_this_round: list[str] = []
            for code, state in states.items():
                if state["status"] != WAITING:
                    continue
                node = graph.nodes[code]
                pre_met, unfinished = self._prerequisites_met(graph, states, node)
                if not pre_met:
                    continue
                data_ok, has_conflict = self._data_ready(node, state)
                if has_conflict:
                    # 未选定冲突版本前不能推进依赖图
                    continue
                if not data_ok:
                    continue
                released_this_round.append(code)
                cur.execute(
                    """UPDATE orch_node_states SET status='READY', released_at=?,
                       blocked_reason=NULL, updated_at=? WHERE id=? AND status='WAITING'""",
                    (now, now, state["id"]),
                )
                store.audit(cur, "node_released", instance_id, code, None,
                            {"join": node.join,
                             "unfinished_prerequisites": unfinished}, ts=now)
            if not released_this_round:
                break
            # 原子释放后，在同一事务内逐个处理（失败只隔离本分支）
            for code in released_this_round:
                self._process_ready_node(cur, instance, graph, code, now)
                progressed.append(code)
        else:  # 理论不可达（DAG 有限）
            raise RuntimeError("advance did not reach fixed point")

        self._resolve_missing_after_advance(cur, instance, graph, now)
        self._maybe_complete_instance(cur, instance_id, now)
        return progressed

    def _selected_payloads(self, cur, graph: GraphSpec, node: NodeSpec, state) -> list[dict]:
        versions = json.loads(state["versions_json"])
        if state["selected_callback_id"]:
            chosen = [state["selected_callback_id"]]
        else:
            distinct = [v["callback_id"] for v in versions if "duplicate_of" not in v]
            chosen = distinct[: node.occurrences]
        payloads = []
        for cb_id in chosen:
            row = cur.execute("SELECT payload FROM orch_callbacks WHERE id=?",
                              (cb_id,)).fetchone()
            payloads.append(json.loads(row["payload"]))
        return payloads

    def _process_ready_node(self, cur, instance, graph: GraphSpec, code: str,
                            now: float) -> None:
        """调用业务处理器并同事务落副作用；失败 -> BLOCKED（只阻塞本分支）。

        崩溃恢复关键：节点已有副作用行说明业务此前已执行过（卡在 READY/PROCESSING
        的脏状态），此时绝不再次调用处理器，只把状态补成 COMPLETED。
        """
        node = graph.nodes[code]
        state = cur.execute(
            "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
            (instance["id"], code)).fetchone()
        existing_effect = cur.execute(
            "SELECT COUNT(*) AS c FROM orch_effects WHERE instance_id=? AND node_code=?",
            (instance["id"], code)).fetchone()["c"]
        if existing_effect > 0:
            cur.execute(
                """UPDATE orch_node_states SET status='COMPLETED', blocked_reason=NULL,
                   completed_at=COALESCE(completed_at, ?), updated_at=?
                   WHERE id=? AND status NOT IN ('COMPLETED','SKIPPED','TERMINATED')""",
                (now, now, state["id"]),
            )
            store.audit(cur, "node_recovered_completed", instance["id"], code,
                        state["selected_callback_id"],
                        {"existing_effects": existing_effect}, ts=now)
            return
        # 条件转移兜底：任何情况下节点业务不会执行两次
        changed = cur.execute(
            """UPDATE orch_node_states SET status='PROCESSING', attempts=attempts+1,
               next_retry_at=NULL, updated_at=?
               WHERE id=? AND status IN ('READY','BLOCKED')""",
            (now, state["id"])).rowcount
        if changed == 0:
            return
        store.audit(cur, "node_processing", instance["id"], code, None,
                    {"attempts": state["attempts"] + 1}, ts=now)
        payloads = self._selected_payloads(cur, graph, node, state)
        try:
            result = self.handler(instance, code, payloads)
        except Exception as exc:  # noqa: BLE001 - 业务失败统一隔离到分支
            attempts = state["attempts"] + 1
            delay = min(self.retry_base_seconds * (2 ** max(attempts - 1, 0)),
                        self.retry_cap_seconds)
            cur.execute(
                """UPDATE orch_node_states SET status='BLOCKED', blocked_reason=?,
                   attempts=?, next_retry_at=?, updated_at=? WHERE id=?""",
                (f"{type(exc).__name__}: {exc}", attempts, now + delay, now,
                 state["id"]),
            )
            store.audit(cur, "node_blocked", instance["id"], code, None,
                        {"error": str(exc), "error_type": type(exc).__name__,
                         "attempts": attempts, "retry_after_seconds": delay}, ts=now)
            return

        effects = result.get("effects", []) if isinstance(result, dict) else []
        for eff in effects:
            eff_type = eff["type"]
            eff_payload = eff.get("payload", {})
            key = self._effect_key(instance["id"], code, eff_type, eff_payload)
            # 唯一键 + OR IGNORE：重启重算/重试不重复产生外部效果
            cur.execute(
                """INSERT OR IGNORE INTO orch_effects
                   (instance_id, node_code, idempotency_key, effect_type, payload,
                    created_at) VALUES (?,?,?,?,?,?)""",
                (instance["id"], code, key, eff_type,
                 json.dumps(eff_payload, ensure_ascii=False, sort_keys=True), now),
            )
        cur.execute(
            """UPDATE orch_node_states SET status='COMPLETED', blocked_reason=NULL,
               completed_at=?, updated_at=? WHERE id=?""",
            (now, now, state["id"]),
        )
        store.audit(cur, "node_completed", instance["id"], code,
                    state["selected_callback_id"],
                    {"effects": len(effects),
                     "checkpoint": result.get("checkpoint") if isinstance(result, dict)
                     else None}, ts=now)

    @staticmethod
    def _effect_key(instance_id: int, code: str, eff_type: str, payload: dict) -> str:
        canonical = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        return hashlib.sha256(
            f"orch:{instance_id}:{code}:{eff_type}:{canonical}".encode()).hexdigest()

    def _resolve_missing_after_advance(self, cur, instance, graph: GraphSpec,
                                       now: float) -> None:
        """推进后把已经不再缺失（回调到达且节点就绪/完成，或前置已补齐）的异常关闭。"""
        rows = cur.execute(
            "SELECT * FROM orch_missing_events WHERE instance_id=? AND status='open'",
            (instance["id"],)).fetchall()
        states = {r["node_code"]: r for r in cur.execute(
            "SELECT * FROM orch_node_states WHERE instance_id=?",
            (instance["id"],)).fetchall()}
        for ev in rows:
            state = states.get(ev["node_code"])
            if state is None:
                continue
            node = graph.nodes[ev["node_code"]]
            if state["status"] in (COMPLETED, SKIPPED, TERMINATED):
                self._close_missing(cur, ev, "resolved", "node_" + state["status"].lower(),
                                    "system", now)
                continue
            pre_met, _ = self._prerequisites_met(graph, states, node)
            data_ok, has_conflict = self._data_ready(node, state)
            if ev["kind"] == "missing_prerequisite" and pre_met:
                self._close_missing(cur, ev, "resolved", "prerequisite_completed",
                                    "system", now)
            elif ev["kind"] == "missing_callback" and data_ok and not has_conflict:
                self._close_missing(cur, ev, "resolved", "callback_arrived",
                                    "system", now)

    def _close_missing(self, cur, ev, status: str, reason: str, operator: str,
                       now: float) -> None:
        cur.execute(
            """UPDATE orch_missing_events SET status=?, resolved_by=?, resolved_at=?,
               resolve_reason=?, updated_at=? WHERE id=? AND status='open'""",
            (status, operator, now, reason, now, ev["id"]),
        )
        store.audit(cur, "missing_event_resolved", ev["instance_id"], ev["node_code"],
                    None, {"missing_event_id": ev["id"], "status": status,
                           "reason": reason, "operator": operator}, ts=now)

    def _maybe_complete_instance(self, cur, instance_id: int, now: float) -> None:
        pending = cur.execute(
            """SELECT COUNT(*) AS c FROM orch_node_states
               WHERE instance_id=? AND status NOT IN ('COMPLETED','SKIPPED','TERMINATED')""",
            (instance_id,)).fetchone()["c"]
        if pending == 0:
            cur.execute(
                """UPDATE orch_instances SET status='completed', completed_at=?, updated_at=?
                   WHERE id=? AND status='active'""",
                (now, now, instance_id),
            )
            store.audit(cur, "instance_completed", instance_id, None, None, {}, ts=now)

    # ======================================================================
    # 重试 / 恢复
    # ======================================================================

    def retry_blocked(self, instance_key: str, node_code: str, *,
                      operator: str = "ops", force: bool = False) -> dict:
        """人工/重试驱动：重试 BLOCKED 节点（重新计算就绪并重新执行业务处理器）。

        自动重试受 next_retry_at 退避时间约束（force=True 为人工立即重试）；
        节点仍然 COMPLETED 不重复执行——_process_ready_node 的条件转移保证。
        """
        now = self.clock()
        with self.db.tx() as cur:
            instance = self._get_instance(cur, instance_key)
            state = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
                (instance["id"], node_code)).fetchone()
            if state is None:
                raise NotFoundError(f"节点 {node_code!r} 不存在")
            if state["status"] != BLOCKED:
                return {"result": "noop", "node": node_code, "status": state["status"]}
            if not force and state["next_retry_at"] is not None \
                    and state["next_retry_at"] > now:
                return {"result": "waiting_retry_at", "node": node_code,
                        "next_retry_at": state["next_retry_at"]}
            # 退回 WAITING 由统一的就绪重算决定是否重新释放（前置仍未完成则继续等待）
            cur.execute(
                """UPDATE orch_node_states SET status='WAITING', blocked_reason=NULL,
                   next_retry_at=NULL, updated_at=? WHERE id=? AND status='BLOCKED'""",
                (now, state["id"]),
            )
            store.audit(cur, "node_retry_requested", instance["id"], node_code, None,
                        {"operator": operator, "force": force}, ts=now)
            advanced = self._advance_locked(cur, instance["id"], now)
            return {"result": "retried", "node": node_code, "advanced": advanced}

    def retry_due_blocked(self) -> int:
        """扫描所有到期可重试的 BLOCKED 节点，逐实例重算（定时/重启恢复入口）。"""
        now = self.clock()
        rows = self.db.query(
            """SELECT s.instance_id, s.node_code FROM orch_node_states s
               JOIN orch_instances i ON i.id=s.instance_id
               WHERE s.status='BLOCKED' AND i.status='active'
                 AND (s.next_retry_at IS NULL OR s.next_retry_at <= ?)""",
            (now,))
        touched = 0
        for r in rows:
            with self.db.tx() as cur:
                inst = cur.execute("SELECT * FROM orch_instances WHERE id=?",
                                   (r["instance_id"],)).fetchone()
                state = cur.execute(
                    "SELECT id FROM orch_node_states WHERE instance_id=? AND node_code=?",
                    (r["instance_id"], r["node_code"])).fetchone()
                cur.execute(
                    """UPDATE orch_node_states SET status='WAITING', blocked_reason=NULL,
                       next_retry_at=NULL, updated_at=? WHERE id=? AND status='BLOCKED'""",
                    (now, state["id"]),
                )
                store.audit(cur, "node_retry_scheduled", r["instance_id"],
                            r["node_code"], None, {"trigger": "due_scan"}, ts=now)
                self._advance_locked(cur, r["instance_id"], now)
            touched += 1
        return touched

    def recover(self) -> dict:
        """服务重启后调用：重算全部活动实例的就绪状态（不重复执行已完成节点）。

        同时把崩溃残留在 PROCESSING/READY 的节点安全退回 WAITING（_advance_locked
        内完成；已有 effects 行的节点不会回退，副作用表唯一键是最后一道防线）。
        """
        rows = self.db.query(
            "SELECT id, instance_key FROM orch_instances WHERE status='active' ORDER BY id")
        advanced_total: list[str] = []
        for r in rows:
            with self.db.tx() as cur:
                advanced_total.extend(self._advance_locked(cur, r["id"], self.clock()))
        return {"instances_scanned": len(rows), "nodes_advanced": advanced_total}

    # ======================================================================
    # 冲突人工选定
    # ======================================================================

    def resolve_conflict(self, instance_key: str, node_code: str, callback_id: int, *,
                         operator: str = "ops", note: str | None = None) -> dict:
        """选定冲突版本；未选定前节点不推进，选定后同事务级联。"""
        now = self.clock()
        with self.db.tx() as cur:
            instance = self._get_instance(cur, instance_key)
            state = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
                (instance["id"], node_code)).fetchone()
            if state is None:
                raise NotFoundError(f"节点 {node_code!r} 不存在")
            versions = json.loads(state["versions_json"])
            valid_ids = {v["callback_id"] for v in versions}
            if callback_id not in valid_ids:
                raise ConflictStateError(
                    f"回调 {callback_id} 不属于节点 {node_code!r} 的候选版本 {sorted(valid_ids)}")
            cb = cur.execute("SELECT * FROM orch_callbacks WHERE id=?",
                             (callback_id,)).fetchone()
            if state["conflict"] == "resolved" and state["selected_callback_id"]:
                # 同一节点收到内容不同的版本时沿用现有冲突处置结果：
                # 已选定后不允许改选（处置结果不可变，保证外部效果不矛盾）
                if state["selected_callback_id"] != callback_id:
                    raise ConflictStateError(
                        "冲突已有处置结果并已冻结，不能改选其他版本")
                return {"result": "noop", "node": node_code,
                        "selected_callback_id": callback_id}
            cur.execute(
                """UPDATE orch_node_states SET conflict='resolved',
                   selected_callback_id=?, updated_at=? WHERE id=?""",
                (callback_id, now, state["id"]),
            )
            store.audit(cur, "conflict_resolved", instance["id"], node_code,
                        callback_id,
                        {"operator": operator, "selected_callback_id": callback_id,
                         "selected_hash": cb["payload_hash"], "note": note,
                         "candidate_ids": sorted(valid_ids)}, ts=now)
            advanced = self._advance_locked(cur, instance["id"], now)
            return {"result": "resolved", "node": node_code,
                    "selected_callback_id": callback_id, "advanced": advanced}

    # ======================================================================
    # 人工处置：重关联 / 跳过 / 终止
    # ======================================================================

    def reassociate_callback(self, callback_id: int, instance_key: str, node_code: str,
                             *, operator: str = "ops", note: str | None = None) -> dict:
        """把一条已存在但关联键错误的 orphan 回调重新关联到指定实例节点。

        原始归属历史保留：新增一条 bound 回调行指向原行（bound_from_callback_id），
        原 orphan 行绝不删除/改写；与期限扫描并发时由写事务串行保证只有一个结果。
        """
        now = self.clock()
        with self.db.tx() as cur:
            original = cur.execute("SELECT * FROM orch_callbacks WHERE id=?",
                                   (callback_id,)).fetchone()
            if original is None:
                raise NotFoundError(f"回调 {callback_id} 不存在")
            if original["ownership"] != "orphan":
                raise ConflictStateError(
                    f"回调 {callback_id} 归属为 {original['ownership']}，"
                    "只有 orphan 待核对回调可以重新关联")
            already_bound = cur.execute(
                "SELECT id FROM orch_callbacks WHERE bound_from_callback_id=? LIMIT 1",
                (callback_id,)).fetchone()
            if already_bound is not None:
                # 并发/重复重关联只有一个有效结果：第一次绑定已生效
                raise ConflictStateError(
                    f"回调 {callback_id} 已被重新关联（bound 行 id={already_bound['id']}），"
                    "不能重复关联")
            instance = self._get_instance(cur, instance_key)
            if instance["status"] != "active":
                raise ConflictStateError(
                    f"实例 {instance_key} 已 {instance['status']}，不能再关联回调")
            graph = self._load_graph(cur, instance["graph_version_id"])
            if node_code not in graph.nodes:
                raise NotFoundError(
                    f"图版本中没有节点 {node_code!r}")
            state = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
                (instance["id"], node_code)).fetchone()
            if state["status"] in (SKIPPED, TERMINATED):
                raise ConflictStateError(
                    f"节点 {node_code} 已 {state['status']}，不能再关联回调")
            post_completion = state["status"] == COMPLETED

            p_hash = original["payload_hash"]
            cur.execute(
                """INSERT INTO orch_callbacks
                   (callback_uid, process_type, correlation_key, payload_hash, payload,
                    received_at, ownership, instance_id, node_code,
                    bound_from_callback_id, bound_by, bound_at, bound_note)
                   VALUES (?,?,?,?,?,?,'bound',?,?,?,?,?,?)""",
                (f"bound:{original['callback_uid']}:{instance['id']}:{node_code}",
                 instance["process_type"], instance["correlation_key"], p_hash,
                 original["payload"], now, instance["id"], node_code,
                 original["id"], operator, now, note),
            )
            new_cb_id = cur.lastrowid

            versions = json.loads(state["versions_json"])
            same = [v for v in versions if v["hash"] == p_hash]
            conflict = state["conflict"]
            if same:
                versions.append({"callback_id": new_cb_id, "hash": p_hash,
                                 "received_at": now, "duplicate_of":
                                 same[0]["callback_id"]})
            else:
                versions.append({"callback_id": new_cb_id, "hash": p_hash,
                                 "received_at": now})
                node = graph.nodes[node_code]
                distinct = len([v for v in versions if "duplicate_of" not in v])
                if distinct > node.occurrences:
                    # 已冻结（resolved）的处置结果不重新打开；其余超额情况打开冲突。
                    # 已完成节点的超额版本同样登记冲突，但已产生效果不撤回、不二次执行。
                    if conflict != "resolved":
                        conflict = "open"
                        cur.execute(
                            "UPDATE orch_node_states SET selected_callback_id=NULL "
                            "WHERE id=?", (state["id"],))
                    store.audit(cur, "conflict_opened", instance["id"], node_code,
                                new_cb_id, {"versions": distinct,
                                            "occurrences": node.occurrences,
                                            "source": "reassociate",
                                            "post_completion": post_completion}, ts=now)
            cur.execute(
                """UPDATE orch_node_states SET versions_json=?, conflict=?, updated_at=?
                   WHERE id=?""",
                (json.dumps(versions, ensure_ascii=False), conflict, now, state["id"]),
            )
            store.audit(cur, "callback_reassociated", instance["id"], node_code,
                        new_cb_id,
                        {"operator": operator, "original_callback_id": original["id"],
                         "original_ownership": "orphan",
                         "original_correlation_key": original["correlation_key"],
                         "target_correlation_key": instance["correlation_key"],
                         "note": note, "conflict": conflict,
                         "post_completion": post_completion}, ts=now)
            advanced = ([] if post_completion
                        else self._advance_locked(cur, instance["id"], now))
            return {"result": "reassociated", "callback_id": new_cb_id,
                    "original_callback_id": original["id"],
                    "instance_id": instance["id"], "node": node_code,
                    "conflict": conflict, "advanced": advanced}

    def skip_node(self, instance_key: str, node_code: str, *,
                  operator: str = "ops", reason: str) -> dict:
        """明确跳过一个允许跳过（required=false）的节点；必需节点拒绝。"""
        now = self.clock()
        with self.db.tx() as cur:
            instance = self._get_instance(cur, instance_key)
            if instance["status"] != "active":
                raise ConflictStateError(
                    f"实例 {instance_key} 已 {instance['status']}，不能跳过节点")
            graph = self._load_graph(cur, instance["graph_version_id"])
            if node_code not in graph.nodes:
                raise NotFoundError(f"图版本中没有节点 {node_code!r}")
            node = graph.nodes[node_code]
            if node.required:
                raise ConflictStateError(
                    f"节点 {node_code!r} 在图中标记为必需（required），不能跳过")
            state = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? AND node_code=?",
                (instance["id"], node_code)).fetchone()
            if state["status"] in (COMPLETED, SKIPPED):
                return {"result": "noop", "node": node_code, "status": state["status"]}
            if state["status"] == TERMINATED:
                raise ConflictStateError("节点已随实例终止")
            cur.execute(
                """UPDATE orch_node_states SET status='SKIPPED', skipped_at=?, skipped_by=?,
                   skip_reason=?, blocked_reason=NULL, updated_at=? WHERE id=?""",
                (now, operator, reason, now, state["id"]),
            )
            store.audit(cur, "node_skipped", instance["id"], node_code, None,
                        {"operator": operator, "reason": reason}, ts=now)
            advanced = self._advance_locked(cur, instance["id"], now)
            return {"result": "skipped", "node": node_code, "advanced": advanced}

    def terminate_instance(self, instance_key: str, *, operator: str = "ops",
                           reason: str) -> dict:
        """终止整个实例：未完成节点全部 TERMINATED，开放异常作废，迟到回调只记 late。"""
        now = self.clock()
        with self.db.tx() as cur:
            instance = self._get_instance(cur, instance_key)
            if instance["status"] != "active":
                return {"result": "noop", "status": instance["status"]}
            cur.execute(
                """UPDATE orch_instances SET status='terminated', terminated_at=?,
                   terminate_reason=?, updated_at=? WHERE id=?""",
                (now, reason, now, instance["id"]),
            )
            terminated_nodes = [r["node_code"] for r in cur.execute(
                """SELECT node_code FROM orch_node_states
                   WHERE instance_id=? AND status NOT IN ('COMPLETED','SKIPPED')""",
                (instance["id"],)).fetchall()]
            cur.execute(
                """UPDATE orch_node_states SET status='TERMINATED', updated_at=?
                   WHERE instance_id=? AND status NOT IN ('COMPLETED','SKIPPED')""",
                (now, instance["id"]),
            )
            for code in terminated_nodes:
                store.audit(cur, "node_terminated", instance["id"], code, None,
                            {"operator": operator, "reason": reason}, ts=now)
            voided = cur.execute(
                """SELECT id, node_code FROM orch_missing_events
                   WHERE instance_id=? AND status='open'""",
                (instance["id"],)).fetchall()
            cur.execute(
                """UPDATE orch_missing_events SET status='voided', resolved_by=?,
                   resolved_at=?, resolve_reason='instance_terminated', updated_at=?
                   WHERE instance_id=? AND status='open'""",
                (operator, now, now, instance["id"]),
            )
            for ev in voided:
                store.audit(cur, "missing_event_voided", instance["id"],
                            ev["node_code"], None,
                            {"missing_event_id": ev["id"], "operator": operator}, ts=now)
            store.audit(cur, "instance_terminated", instance["id"], None, None,
                        {"operator": operator, "reason": reason,
                         "terminated_nodes": terminated_nodes}, ts=now)
            return {"result": "terminated", "instance_id": instance["id"],
                    "terminated_nodes": terminated_nodes}

    # ======================================================================
    # 期限扫描：缺失事件异常
    # ======================================================================

    def scan_deadlines(self, now: float | None = None) -> list[dict]:
        """扫描全部活动实例到期节点；缺前置或缺回调生成 open 缺失事件异常。

        与回调入口/重关联/跳过并发时，写事务串行 + open 异常部分唯一索引保证
        同一节点只有一个有效结果（竞态丢失方什么也不生成）。
        """
        now = self.clock() if now is None else now
        due = self.db.query(
            """SELECT s.*, i.instance_key, i.status AS inst_status, i.graph_version_id
               FROM orch_node_states s JOIN orch_instances i ON i.id=s.instance_id
               WHERE i.status='active' AND s.status NOT IN
                     ('COMPLETED','SKIPPED','TERMINATED')
                 AND s.deadline IS NOT NULL AND s.deadline <= ?
               ORDER BY s.deadline, s.id""",
            (now,))
        created: list[dict] = []
        for row in due:
            with self.db.tx() as cur:
                ev = self._scan_one_deadline(cur, row, now)
                if ev:
                    created.append(ev)
        return created

    def _scan_one_deadline(self, cur, row, now: float) -> dict | None:
        # 复查（取出后可能已被回调/人工处置推进）
        state = cur.execute(
            "SELECT * FROM orch_node_states WHERE id=?", (row["id"],)).fetchone()
        instance = cur.execute("SELECT * FROM orch_instances WHERE id=?",
                               (state["instance_id"],)).fetchone()
        if instance["status"] != "active" or state["status"] in (
                COMPLETED, SKIPPED, TERMINATED):
            return None
        existing = cur.execute(
            "SELECT id FROM orch_missing_events WHERE instance_id=? AND node_code=? "
            "AND status='open'", (instance["id"], state["node_code"])).fetchone()
        if existing:
            return None
        graph = self._load_graph(cur, instance["graph_version_id"])
        node = graph.nodes[state["node_code"]]
        states = {r["node_code"]: r for r in cur.execute(
            "SELECT * FROM orch_node_states WHERE instance_id=?",
            (instance["id"],)).fetchall()}
        pre_met, unfinished = self._prerequisites_met(graph, states, node)
        data_ok, has_conflict = self._data_ready(node, state)

        if not pre_met:
            kind, reason_code = "missing_prerequisite", "deadline_passed"
            missing = unfinished
        elif not data_ok or has_conflict:
            kind, reason_code = "missing_callback", "deadline_passed"
            missing = []
        else:
            # 数据与前置都已满足但节点还未推进（异常时序），直接触发推进
            self._advance_locked(cur, instance["id"], now)
            return None

        blocked_paths = self._blocked_paths(graph, state["node_code"])
        detail = {
            "deadline": state["deadline"], "now": now,
            "overdue_seconds": now - state["deadline"],
            "missing_prerequisites": missing,
            "conflict": state["conflict"],
            "versions_received": len(json.loads(state["versions_json"])),
            "occurrences": node.occurrences,
        }
        cur.execute(
            """INSERT INTO orch_missing_events
               (instance_id, node_code, kind, reason_code, detail_json,
                blocked_paths_json, deadline, status, created_at, updated_at)
               VALUES (?,?,?,?,?,?,?,'open',?,?)""",
            (instance["id"], state["node_code"], kind, reason_code,
             json.dumps(detail, ensure_ascii=False, sort_keys=True),
             json.dumps(blocked_paths, ensure_ascii=False), state["deadline"], now, now),
        )
        ev_id = cur.lastrowid
        cur.execute(
            "UPDATE orch_node_states SET deadline_checked_at=?, updated_at=? WHERE id=?",
            (now, now, state["id"]),
        )
        store.audit(cur, "missing_event_opened", instance["id"], state["node_code"],
                    None, {"missing_event_id": ev_id, "kind": kind,
                           "reason_code": reason_code, "detail": detail,
                           "blocked_paths": blocked_paths}, ts=now)
        return {"missing_event_id": ev_id, "instance_id": instance["id"],
                "instance_key": instance["instance_key"],
                "node": state["node_code"], "kind": kind,
                "blocked_paths": blocked_paths, "deadline": state["deadline"]}

    def _blocked_paths(self, graph: GraphSpec, start: str) -> list[list[str]]:
        """从受阻节点出发的全部后继依赖路径（管理视图里的「受阻分支」）。"""
        paths: list[list[str]] = []

        def walk(code: str, path: list[str]) -> None:
            succs = graph.successors.get(code, ())
            if not succs:
                if len(path) > 1:
                    paths.append(path[1:])
                return
            for s in succs:
                walk(s, path + [s])

        walk(start, [start])
        return paths

    # ======================================================================
    # 查询
    # ======================================================================

    def get_instance_view(self, instance_key: str) -> dict:
        with self.db.tx() as cur:
            instance = self._get_instance(cur, instance_key)
            graph = self._load_graph(cur, instance["graph_version_id"])
            node_rows = cur.execute(
                "SELECT * FROM orch_node_states WHERE instance_id=? ORDER BY id",
                (instance["id"],)).fetchall()
            nodes = []
            states = {r["node_code"]: r for r in node_rows}
            for r in node_rows:
                spec = graph.nodes[r["node_code"]]
                versions = json.loads(r["versions_json"])
                pre_met, unfinished = self._prerequisites_met(graph, states, spec)
                adopted = None
                if r["selected_callback_id"]:
                    adopted = r["selected_callback_id"]
                elif r["status"] in (COMPLETED,):
                    adopted = [v["callback_id"] for v in versions
                               if "duplicate_of" not in v][: spec.occurrences]
                nodes.append({
                    "node": r["node_code"],
                    "category": self._node_category(spec, r, pre_met, unfinished, states),
                    "status": r["status"],
                    "join": spec.join,
                    "occurrences": spec.occurrences,
                    "required": spec.required,
                    "depends_on": list(spec.depends_on),
                    "versions": versions,
                    "version_count": len([v for v in versions
                                          if "duplicate_of" not in v]),
                    "conflict": r["conflict"],
                    "adopted_callback_id": adopted,
                    "blocked_reason": r["blocked_reason"],
                    "unfinished_prerequisites": unfinished,
                    "dependency_paths": self._paths_from_roots(graph, r["node_code"]),
                    "deadline": r["deadline"],
                    "released_at": r["released_at"],
                    "completed_at": r["completed_at"],
                    "attempts": r["attempts"],
                })
            missing = [dict(r) for r in cur.execute(
                "SELECT * FROM orch_missing_events WHERE instance_id=? ORDER BY id",
                (instance["id"],)).fetchall()]
            for m in missing:
                m["detail_json"] = json.loads(m["detail_json"])
                m["blocked_paths_json"] = json.loads(m["blocked_paths_json"])
            audit_rows = [dict(r) for r in cur.execute(
                "SELECT * FROM orch_audit WHERE instance_id=? ORDER BY seq, id",
                (instance["id"],)).fetchall()]
            for a in audit_rows:
                a["detail_json"] = json.loads(a["detail_json"])
            return {
                "instance": {
                    "instance_key": instance["instance_key"],
                    "process_type": instance["process_type"],
                    "correlation_key": instance["correlation_key"],
                    "status": instance["status"],
                    "graph_version_id": instance["graph_version_id"],
                    "graph_version": graph.version,
                    "created_at": instance["created_at"],
                    "completed_at": instance["completed_at"],
                    "terminated_at": instance["terminated_at"],
                    "terminate_reason": instance["terminate_reason"],
                },
                "nodes": nodes,
                "missing_events": missing,
                "audit": audit_rows,
            }

    @staticmethod
    def _node_category(spec: NodeSpec, state, pre_met: bool, unfinished: list[str],
                       states: dict[str, sqlite3.Row]) -> str:
        """五档管理视图：completed / processing / waiting / blocked / not_arrived。

        - COMPLETED/SKIPPED -> completed；BLOCKED -> blocked；PROCESSING/READY -> processing
        - WAITING 中已收到任一版本 -> waiting（处理中/等待前置或冲突选定）
        - WAITING 且无版本 -> not_arrived（未到达）
        """
        if state["status"] in (COMPLETED, SKIPPED):
            return "completed"
        if state["status"] == BLOCKED:
            return "blocked"
        if state["status"] in (PROCESSING, READY):
            return "processing"
        has_version = bool(json.loads(state["versions_json"]))
        return "waiting" if has_version else "not_arrived"

    def _paths_from_roots(self, graph: GraphSpec, target: str) -> list[list[str]]:
        """从前驱根到目标节点的全部依赖路径（展示「为什么被阻塞」）。"""
        results: list[list[str]] = []

        def backtrack(code: str, chain: list[str]) -> None:
            deps = graph.nodes[code].depends_on
            if not deps:
                results.append(list(reversed(chain)))
                return
            for d in deps:
                backtrack(d, chain + [d])

        backtrack(target, [target])
        return results

    def list_missing_events(self, *, status: str | None = None,
                            process_type: str | None = None) -> list[dict]:
        sql = ("""SELECT m.*, i.instance_key, i.process_type FROM orch_missing_events m
                  JOIN orch_instances i ON i.id=m.instance_id WHERE 1=1""")
        params: list = []
        if status:
            sql += " AND m.status=?"
            params.append(status)
        if process_type:
            sql += " AND i.process_type=?"
            params.append(process_type)
        sql += " ORDER BY m.id"
        rows = self.db.query(sql, tuple(params))
        out = []
        for r in rows:
            d = dict(r)
            d["detail_json"] = json.loads(d["detail_json"])
            d["blocked_paths_json"] = json.loads(d["blocked_paths_json"])
            out.append(d)
        return out

    def list_orphan_callbacks(self) -> list[dict]:
        return [dict(r) for r in self.db.query(
            "SELECT * FROM orch_callbacks WHERE ownership='orphan' ORDER BY id")]

    def list_effects(self, instance_key: str | None = None) -> list[dict]:
        if instance_key:
            rows = self.db.query(
                """SELECT e.* FROM orch_effects e JOIN orch_instances i ON i.id=e.instance_id
                   WHERE i.instance_key=? ORDER BY e.id""", (instance_key,))
        else:
            rows = self.db.query("SELECT * FROM orch_effects ORDER BY id")
        return [dict(r) for r in rows]

    def global_audit(self, *, event_type: str | None = None, limit: int = 200) -> list[dict]:
        sql = ("""SELECT a.*, i.instance_key FROM orch_audit a
                  LEFT JOIN orch_instances i ON i.id=a.instance_id WHERE 1=1""")
        params: list = []
        if event_type:
            sql += " AND a.event_type=?"
            params.append(event_type)
        sql += " ORDER BY a.id DESC LIMIT ?"
        params.append(limit)
        rows = self.db.query(sql, tuple(params))
        out = []
        for r in rows:
            d = dict(r)
            d["detail_json"] = json.loads(d["detail_json"])
            out.append(d)
        return out

    # ======================================================================
    # 副作用派发（由 worker 调用；与既有网关 outbox 同一幂等思路）
    # ======================================================================

    def dispatch_pending_effects(self, sink) -> int:
        """把 pending 效果派发给幂等下游 sink；返回尝试派发条数。

        sink 接口：already_applied(key)->bool / send(key, type, payload)->None，
        需要自身幂等（参考 handlers.IdempotentSink）。
        """
        rows = self.db.query(
            """SELECT e.* FROM orch_effects e JOIN orch_instances i ON i.id=e.instance_id
               WHERE e.status='pending' ORDER BY e.id LIMIT 100""")
        dispatched = 0
        for row in rows:
            now = self.clock()
            key = row["idempotency_key"]
            try:
                if not sink.already_applied(key):
                    sink.send(key, row["effect_type"], json.loads(row["payload"]))
            except Exception as exc:  # noqa: BLE001
                with self.db.tx() as cur:
                    cur.execute(
                        "UPDATE orch_effects SET attempts=attempts+1, status='failed', "
                        "last_error=? WHERE id=? AND status='pending'",
                        (str(exc), row["id"]))
                    store.audit(cur, "effect_failed", row["instance_id"],
                                row["node_code"], None,
                                {"effect_id": row["id"], "error": str(exc)}, ts=now)
                continue
            with self.db.tx() as cur:
                changed = cur.execute(
                    "UPDATE orch_effects SET status='executed', executed_at=? "
                    "WHERE id=? AND status='pending'", (now, row["id"])).rowcount
                if changed:
                    store.audit(cur, "effect_executed", row["instance_id"],
                                row["node_code"], None,
                                {"effect_id": row["id"], "idempotency_key": key,
                                 "effect_type": row["effect_type"]}, ts=now)
                    dispatched += 1
        return dispatched

    def retry_failed_effects(self) -> int:
        """把 failed 效果重新放回 pending（外部系统恢复后人工/定时触发）。"""
        now = self.clock()
        with self.db.tx() as cur:
            n = cur.execute(
                "UPDATE orch_effects SET status='pending', last_error=NULL "
                "WHERE status='failed'").rowcount
            if n:
                store.audit(cur, "effects_retried", None, None, None,
                            {"count": n}, ts=now)
        return n

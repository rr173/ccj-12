"""事件依赖图：声明、校验与内存模型。

图声明（JSON 可序列化）：

    {
      "key_path": "data.order_id",          # 关联键在回调内容里的取值位置（全图统一）
      "nodes": [
        {"code": "paid",     "occurrences": 1, "required": true},
        {"code": "risk_a",   "occurrences": 1, "skippable": true, "wait_seconds": 3600},
        {"code": "risk_b",   "occurrences": 1, "skippable": true},
        {"code": "ship",     "occurrences": 1, "required": true,
         "wait_seconds": 7200,
         "join": "ALL",                       # 汇合点：ALL=等待全部分支，ANY=任一分支
         "depends_on": [
           {"code": "risk_a"},
           {"code": "risk_b", "join": "ANY"}  # 边上的 ANY 等价于把汇合点标为 ANY
         ]}
      ]
    }

规则：
- 节点 code 全图唯一；depends_on 只能引用已定义节点；不允许自环/环（DAG）；
  从无前驱的根节点出发不可达的节点一律拒绝（孤立定义无意义，避免静默失配）；
- key_path 必须是合法取值位置（点分段，支持 a.b[0].c），发布时只校验语法；
- occurrences>=1：该节点允许出现多少份**内容不同**的回调版本，恰好收齐才算数据就绪；
- required（默认 true）的节点不能被人工跳过；skippable=true 是 required=false 的别名；
- wait_seconds：相对实例创建时间的等待期限，到期前置仍未完成时生成缺失事件异常。
"""
from __future__ import annotations

import json
import re
from dataclasses import dataclass, field
from typing import Any

# ---- 取值位置（key path） --------------------------------------------------

_SEGMENT_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
# 点分段，数组下标写在段末：items[0]、items[0][1]
_TOKEN_RE = re.compile(r"^([A-Za-z_][A-Za-z0-9_]*)((?:\[\d+\])*)$")


class KeyPathError(ValueError):
    """无效取值位置。"""


def validate_key_path(path: str) -> list[str]:
    """校验取值位置语法，返回规范化段列表（数字下标段原样保留字符串）。

    合法："a"、"data.order_id"、"items[0].id"；非法：空串、".a"、"a."、
    "a..b"、"[]"、"a[x]"、"a[0-]"。
    """
    if not isinstance(path, str) or not path.strip():
        raise KeyPathError("key_path 必须是非空字符串")
    if path.startswith(".") or path.endswith(".") or ".." in path:
        raise KeyPathError(f"非法取值位置：{path!r}")
    segments: list[str] = []
    for raw in path.split("."):
        m = _TOKEN_RE.match(raw)
        if not m:
            raise KeyPathError(f"非法取值位置段：{raw!r}（位于 {path!r}）")
        name, indexes = m.group(1), m.group(2)
        segments.append(name)
        if indexes:
            for idx in re.findall(r"\[(\d+)\]", indexes):
                segments.append(idx)
    return segments


def extract_key(payload: Any, segments: list[str]) -> Any:
    """按段列表从解析后的回调内容中取值；任一段缺失/类型不符返回 None（失配）。"""
    cur: Any = payload
    for seg in segments:
        if seg.isdigit() and not (seg and seg[0] == "_"):
            if not isinstance(cur, list):
                return None
            i = int(seg)
            if i >= len(cur):
                return None
            cur = cur[i]
        else:
            if not isinstance(cur, dict) or seg not in cur:
                return None
            cur = cur[seg]
    return cur


# ---- 图模型 ---------------------------------------------------------------

JOIN_ALL = "ALL"
JOIN_ANY = "ANY"
VALID_JOINS = (JOIN_ALL, JOIN_ANY)


class GraphValidationError(ValueError):
    """图发布校验失败；errors 列出全部问题（一次报全，便于运营修正）。"""

    def __init__(self, errors: list[str]):
        self.errors = errors
        super().__init__("; ".join(errors))


@dataclass(frozen=True)
class NodeSpec:
    code: str
    occurrences: int = 1
    required: bool = True
    wait_seconds: float | None = None
    join: str = JOIN_ALL
    # 前驱节点 code（保持声明顺序，依赖路径展示用）
    depends_on: tuple[str, ...] = ()
    # 该节点自己的取值位置覆盖；None=沿用图的 key_path
    key_path: tuple[str, ...] | None = None
    # 原始 key_path 文本（发布快照展示用）
    key_path_raw: str | None = None

    def to_dict(self) -> dict:
        return {
            "code": self.code,
            "occurrences": self.occurrences,
            "required": self.required,
            "wait_seconds": self.wait_seconds,
            "join": self.join,
            "depends_on": list(self.depends_on),
            "key_path": self.key_path_raw,
        }


@dataclass(frozen=True)
class GraphSpec:
    process_type: str
    version: int
    key_path: tuple[str, ...]
    key_path_raw: str
    nodes: dict[str, NodeSpec] = field(default_factory=dict)
    # 后继邻接（级联释放用）
    successors: dict[str, tuple[str, ...]] = field(default_factory=dict)
    roots: tuple[str, ...] = ()
    created_at: float = 0.0
    created_by: str = ""
    note: str | None = None

    def to_json(self) -> str:
        return json.dumps(
            {
                "process_type": self.process_type,
                "version": self.version,
                "key_path": self.key_path_raw,
                "nodes": [n.to_dict() for n in self.nodes.values()],
                "created_at": self.created_at,
                "created_by": self.created_by,
                "note": self.note,
            },
            ensure_ascii=False,
            sort_keys=True,
        )

    def extract_correlation_key(self, payload: Any) -> Any:
        return extract_key(payload, list(self.key_path))

    def node_key_segments(self, node: NodeSpec) -> list[str]:
        return list(node.key_path if node.key_path is not None else self.key_path)


def graph_from_json(process_type: str, version: int, raw: str | dict,
                    created_at: float = 0.0, created_by: str = "",
                    note: str | None = None) -> GraphSpec:
    """从发布时固化的 JSON 重建图（信任已落盘内容，但仍跑一遍构造）。"""
    data = json.loads(raw) if isinstance(raw, str) else raw
    return build_graph(
        process_type, version, data, created_at=created_at,
        created_by=created_by, note=note,
    )


def build_graph(process_type: str, version: int, spec: dict, *,
                created_at: float = 0.0, created_by: str = "",
                note: str | None = None) -> GraphSpec:
    """校验并构造图；失败抛 GraphValidationError（errors 收集全部问题）。"""
    errors: list[str] = []
    if not isinstance(process_type, str) or not process_type.strip():
        errors.append("process_type 必须是非空字符串")
    if not isinstance(spec, dict):
        raise GraphValidationError(["图声明必须是对象"])

    # key_path
    key_segments: tuple[str, ...] = ()
    key_raw = spec.get("key_path")
    if not isinstance(key_raw, str) or not key_raw.strip():
        errors.append("图级 key_path 必须是非空字符串（声明关联键的取值位置）")
    else:
        try:
            key_segments = tuple(validate_key_path(key_raw))
        except KeyPathError as exc:
            errors.append(str(exc))

    raw_nodes = spec.get("nodes")
    if not isinstance(raw_nodes, list) or not raw_nodes:
        errors.append("nodes 必须是非空数组")
        raw_nodes = []

    nodes: dict[str, NodeSpec] = {}
    order: list[str] = []
    for i, rn in enumerate(raw_nodes):
        if not isinstance(rn, dict):
            errors.append(f"nodes[{i}] 必须是对象")
            continue
        code = rn.get("code")
        where = f"nodes[{i}]" + (f"({code})" if code else "")
        if not isinstance(code, str) or not code.strip():
            errors.append(f"{where}: code 必须是非空字符串")
            continue
        if code in nodes:
            errors.append(f"重复事件定义：节点 {code!r} 出现多次")
            continue

        occurrences = rn.get("occurrences", 1)
        if not isinstance(occurrences, int) or isinstance(occurrences, bool) or occurrences < 1:
            errors.append(f"{where}: occurrences 必须是 >=1 的整数")
            occurrences = 1

        # required 缺省 true；显式 skippable=true 可置 false
        required = rn.get("required", True)
        if "skippable" in rn and "required" not in rn:
            required = not bool(rn["skippable"])
        if not isinstance(required, bool):
            errors.append(f"{where}: required/skippable 必须是布尔值")
            required = True

        wait = rn.get("wait_seconds")
        if wait is not None:
            if not isinstance(wait, (int, float)) or isinstance(wait, bool) or wait <= 0:
                errors.append(f"{where}: wait_seconds 必须是正数")
                wait = None
            else:
                wait = float(wait)

        join_explicit = "join" in rn
        join = str(rn.get("join", JOIN_ALL)).upper()
        if join not in VALID_JOINS:
            errors.append(f"{where}: join 必须是 ALL 或 ANY")
            join = JOIN_ALL

        deps_raw = rn.get("depends_on", [])
        deps: list[str] = []
        edge_any = False
        if not isinstance(deps_raw, list):
            errors.append(f"{where}: depends_on 必须是数组")
            deps_raw = []
        for j, dep in enumerate(deps_raw):
            dep_code = dep["code"] if isinstance(dep, dict) else dep
            edge_join = None
            if isinstance(dep, dict):
                edge_join = str(dep.get("join", "")).upper() or None
                if edge_join not in (None, JOIN_ALL, JOIN_ANY):
                    errors.append(f"{where}.depends_on[{j}]: 边上的 join 必须是 ALL/ANY")
                    edge_join = None
            if not isinstance(dep_code, str) or not dep_code.strip():
                errors.append(f"{where}.depends_on[{j}]: 前驱 code 必须是非空字符串")
                continue
            if dep_code == code:
                errors.append(f"节点 {code!r} 不能依赖自己（自环）")
                continue
            if dep_code in deps:
                errors.append(f"{where}: 前驱 {dep_code!r} 重复声明")
                continue
            deps.append(dep_code)
            if edge_join == JOIN_ANY:
                edge_any = True  # 边简写：未显式声明节点 join 时把汇合点标为 ANY
        if edge_any and not join_explicit:
            join = JOIN_ANY

        node_key_raw = rn.get("key_path")
        node_key_segments: tuple[str, ...] | None = None
        if node_key_raw is not None:
            if not isinstance(node_key_raw, str) or not node_key_raw.strip():
                errors.append(f"{where}: 节点级 key_path 必须是非空字符串")
            else:
                try:
                    node_key_segments = tuple(validate_key_path(node_key_raw))
                except KeyPathError as exc:
                    errors.append(str(exc))
                    node_key_raw = None

        nodes[code] = NodeSpec(
            code=code, occurrences=occurrences, required=required,
            wait_seconds=wait, join=join, depends_on=tuple(deps),
            key_path=node_key_segments,
            key_path_raw=node_key_raw if node_key_segments is not None else None,
        )
        order.append(code)

    # 前驱引用必须存在
    for code, node in list(nodes.items()):
        for dep in node.depends_on:
            if dep not in nodes:
                errors.append(f"节点 {code!r} 依赖了未定义的节点 {dep!r}")

    if not errors:
        # 环检测（DFS 三色）
        WHITE, GRAY, BLACK = 0, 1, 2
        color = {c: WHITE for c in nodes}
        cycle: list[str] = []

        def dfs(u: str, stack: list[str]) -> bool:
            color[u] = GRAY
            stack.append(u)
            for v in nodes[u].depends_on:
                if color[v] == GRAY:
                    start = stack.index(v)
                    cycle.extend(stack[start:] + [v])
                    return True
                if color[v] == WHITE and dfs(v, stack):
                    return True
            stack.pop()
            color[u] = BLACK
            return False

        for c in order:
            if color[c] == WHITE and dfs(c, []):
                errors.append("依赖图存在环：" + " -> ".join(cycle))
                break

        # 可达性：从所有无前驱根节点正向 BFS
        successors: dict[str, list[str]] = {c: [] for c in nodes}
        roots = [c for c in order if not nodes[c].depends_on]
        if not roots and nodes:
            errors.append("图中没有无前驱的根节点（所有节点都处于环中）")
        seen: set[str] = set()
        frontier = list(roots)
        while frontier:
            u = frontier.pop()
            if u in seen:
                continue
            seen.add(u)
            for v, node in nodes.items():
                if u in node.depends_on and v not in seen:
                    successors[u].append(v)
                    frontier.append(v)
        unreachable = [c for c in order if c not in seen]
        if unreachable:
            errors.append("存在从根节点不可达的节点：" + ", ".join(unreachable))

    if errors:
        raise GraphValidationError(errors)

    successors = {c: tuple(
        [d for d in order if c in nodes[d].depends_on]) for c in order}
    return GraphSpec(
        process_type=process_type,
        version=version,
        key_path=key_segments,
        key_path_raw=key_raw,
        nodes=nodes,
        successors=successors,
        roots=tuple(c for c in order if not nodes[c].depends_on),
        created_at=created_at,
        created_by=created_by,
        note=note,
    )

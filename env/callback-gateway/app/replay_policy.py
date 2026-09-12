"""重放批次的多级审批策略：版本化管理 + 按风险等级/批次规模生成审批节点链。

- 运营通过管理端点提交策略（`POST /admin/replay-policies`）：校验通过整份生效
  （版本号单调递增），校验失败保留当前策略；每次提交（无论成败）都落
  `replay_policy_versions` 记录并写审计事件，可查每一次变更历史；
- 策略 = 一组按序匹配的规则。每条规则按风险等级（risk_level: high/normal/any）
  与批次规模（min_size/max_size，任务条数）匹配，声明一串审批节点：
  mode=serial 时节点串行（上一节点满足后才激活下一节点，各自起算截止时间），
  mode=parallel 时节点并行（同时待决，全部满足才放行）；
- 每个节点可配置：允许承担该节点的角色（role 单角色，或 roles 角色列表；
  'any' 表示任何非发起人）与法定人数 required_approvals（有效赞成票达到该数
  节点才满足，默认 1）。串行节点按节点分别计数、逐节点满足；并行节点各自达到
  法定人数才算满足。审批人凭本人当前有效的角色委托（见 delegation.py）承担
  指定角色，或在直接持角色场景下不带委托承担；
- 提交重放批次时按「当前生效策略 + 批次风险等级 + 批次规模」解析出节点链，
  节点行与策略快照随批次一次性落盘——之后策略再更新也不影响已提交的批次；
- 没有任何已生效策略时使用内置默认策略（与引入策略功能前的行为一致）：
  高风险批次需一名非发起人审批（时限 REPLAY_APPROVAL_TIMEOUT_SECONDS），
  普通批次直接运行；
- 已生效策略下，高风险批次若没有任何规则匹配 -> 拒绝提交（fail closed，
  宁可提交失败也不静默降低审批要求）；普通批次无匹配规则则直接运行。
"""
from __future__ import annotations

import json
import sqlite3
import time

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from . import audit
from .db import Database

# 拒绝记录里保存的原始报文上限，避免超大提交撑爆审计表
_MAX_STORED_SUBMISSION = 4000

RISK_LEVELS = ("high", "normal", "any")
MODES = ("serial", "parallel")

# 节点角色的通配值：任何非发起人运营人员都可承担该节点
ROLE_ANY = "any"


def builtin_default_rules(approval_timeout_seconds: float) -> list[dict]:
    """内置默认策略（没有任何已生效策略版本时生效）：行为与策略功能引入前一致。"""
    return [
        {"name": "builtin-high-risk-single-approval", "risk_level": "high",
         "min_size": None, "max_size": None, "mode": "serial",
         "nodes": [{"role": ROLE_ANY, "required_approvals": 1,
                    "timeout_seconds": float(approval_timeout_seconds)}]},
        {"name": "builtin-normal-no-approval", "risk_level": "normal",
         "min_size": None, "max_size": None, "mode": "serial", "nodes": []},
    ]


def validate_policy(doc) -> tuple[list[dict] | None, list[str]]:
    """校验策略文档，返回 (规范化规则列表, 错误列表)；有错时规则列表为 None。"""
    errors: list[str] = []
    if not isinstance(doc, dict):
        return None, ["policy_must_be_object"]
    rules = doc.get("rules")
    if not isinstance(rules, list) or not rules:
        return None, ["rules_must_be_non_empty_list"]

    normalized: list[dict] = []
    for i, rule in enumerate(rules):
        if not isinstance(rule, dict):
            errors.append(f"rules[{i}]: must be an object")
            continue
        name = rule.get("name")
        if name is not None and (not isinstance(name, str) or not name.strip()):
            errors.append(f"rules[{i}].name: must be a non-empty string when present")
        risk = rule.get("risk_level", "any")
        if risk not in RISK_LEVELS:
            errors.append(f"rules[{i}].risk_level: must be one of {','.join(RISK_LEVELS)}")
        min_size, max_size = rule.get("min_size"), rule.get("max_size")
        for label, value in (("min_size", min_size), ("max_size", max_size)):
            if value is not None and (
                    isinstance(value, bool) or not isinstance(value, int) or value < 1):
                errors.append(f"rules[{i}].{label}: must be an integer >= 1")
        if isinstance(min_size, int) and not isinstance(min_size, bool) \
                and isinstance(max_size, int) and not isinstance(max_size, bool) \
                and min_size >= 1 and max_size >= 1 and min_size > max_size:
            errors.append(f"rules[{i}]: min_size must be <= max_size")
        mode = rule.get("mode", "serial")
        if mode not in MODES:
            errors.append(f"rules[{i}].mode: must be one of {','.join(MODES)}")
        nodes = rule.get("nodes")
        if not isinstance(nodes, list):
            errors.append(f"rules[{i}].nodes: must be a list (empty = no approval needed)")
            continue
        norm_nodes = []
        for j, node in enumerate(nodes):
            if not isinstance(node, dict):
                errors.append(f"rules[{i}].nodes[{j}]: must be an object")
                continue
            # 允许承担该节点的角色：role 单角色，或 roles 角色列表（至少一个）；
            # role 缺省时取 roles[0]，两者都给时取并集（role 不得与 roles 矛盾）
            role = node.get("role")
            roles = node.get("roles")
            allowed: list[str] = []
            if isinstance(roles, list) and roles:
                for r in roles:
                    if not isinstance(r, str) or not r.strip():
                        errors.append(
                            f"rules[{i}].nodes[{j}].roles: each role must be a non-empty string")
                    elif r.strip() not in allowed:
                        allowed.append(r.strip())
            elif roles is not None:
                errors.append(
                    f"rules[{i}].nodes[{j}].roles: must be a non-empty list when present")
            if role is not None:
                if not isinstance(role, str) or not role.strip():
                    errors.append(f"rules[{i}].nodes[{j}].role: must be a non-empty string")
                elif role.strip() not in allowed:
                    allowed.insert(0, role.strip())
            if not allowed:
                errors.append(
                    f"rules[{i}].nodes[{j}]: role or roles must designate at least one role")
            primary = (role.strip() if isinstance(role, str) and role.strip()
                       else (allowed[0] if allowed else ""))
            timeout = node.get("timeout_seconds")
            if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) \
                    or timeout <= 0:
                errors.append(
                    f"rules[{i}].nodes[{j}].timeout_seconds: must be a positive number")
                timeout = 0
            required = node.get("required_approvals", 1)
            if isinstance(required, bool) or not isinstance(required, int) \
                    or required < 1:
                errors.append(
                    f"rules[{i}].nodes[{j}].required_approvals: must be an integer >= 1")
                required = 1
            norm_nodes.append({"role": primary, "roles": allowed,
                               "required_approvals": required,
                               "timeout_seconds": float(timeout)})
        normalized.append({
            "name": name.strip() if isinstance(name, str) and name.strip() else f"rule-{i}",
            "risk_level": risk,
            "min_size": min_size if isinstance(min_size, int)
                        and not isinstance(min_size, bool) else None,
            "max_size": max_size if isinstance(max_size, int)
                        and not isinstance(max_size, bool) else None,
            "mode": mode,
            "nodes": norm_nodes,
        })
    if errors:
        return None, errors
    return normalized, []


def match_rule(rules: list[dict], risk_level: str, total: int) -> dict | None:
    """按序匹配：第一条「风险等级相符且批次规模落在区间内」的规则生效。"""
    for rule in rules:
        if rule["risk_level"] not in ("any", risk_level):
            continue
        if rule["min_size"] is not None and total < rule["min_size"]:
            continue
        if rule["max_size"] is not None and total > rule["max_size"]:
            continue
        return rule
    return None


class ReplayPolicyStore:
    """replay_policy_versions 表的读写；版本号在写事务内分配，并发提交不会重号。"""

    def __init__(self, db: Database):
        self._db = db

    def record_applied(self, operator: str, rules: list[dict]) -> tuple[int, float]:
        """在一个事务里分配新版本号、写入 applied 记录、整份切换各风险等级稳定指针
        （并关闭未结束的灰度发布）并落审计事件，返回 (版本号, 时间)。

        灰度分流见 replay_rollout.py：稳定指针与发布单的更新和策略版本写入同事务，
        并发的批次提交只会看到完整的旧状态或完整的新状态，不会读到半成品。"""
        now = time.time()
        from . import replay_rollout
        with self._db.tx() as cur:
            row = cur.execute("SELECT MAX(version) AS v FROM replay_policy_versions").fetchone()
            version = (row["v"] or 0) + 1
            cur.execute(
                """INSERT INTO replay_policy_versions
                   (version, result, policy, operator, reason, created_at)
                   VALUES (?,?,?,?,NULL,?)""",
                (version, "applied",
                 json.dumps({"rules": rules}, ensure_ascii=False, sort_keys=True),
                 operator, now),
            )
            audit.record(cur, "replay_policy_applied", None, None,
                         {"version": version, "operator": operator,
                          "rules": len(rules)}, ts=now)
            replay_rollout.apply_full_policy(cur, version, rules, operator, now)
            return version, now

    def record_rejected(self, operator: str, raw_policy: str, reasons: list[str]) -> None:
        now = time.time()
        with self._db.tx() as cur:
            cur.execute(
                """INSERT INTO replay_policy_versions
                   (version, result, policy, operator, reason, created_at)
                   VALUES (NULL,'rejected',?,?,?,?)""",
                (raw_policy[:_MAX_STORED_SUBMISSION], operator,
                 json.dumps(reasons, ensure_ascii=False), now),
            )
            audit.record(cur, "replay_policy_rejected", None, None,
                         {"operator": operator, "reasons": reasons}, ts=now)

    def current_applied(self) -> sqlite3.Row | None:
        return self._db.query_one(
            "SELECT * FROM replay_policy_versions WHERE result='applied' "
            "ORDER BY version DESC LIMIT 1")

    def history(self, limit: int) -> list[sqlite3.Row]:
        return self._db.query(
            "SELECT * FROM replay_policy_versions ORDER BY id DESC LIMIT ?", (limit,))


def submit_policy(db: Database, raw_body: bytes) -> tuple[int, dict]:
    """处理一次策略提交，返回 (HTTP 状态码, 响应体)。任何结果都落版本记录与审计。"""
    store = ReplayPolicyStore(db)
    try:
        submitted = json.loads(raw_body)
    except (UnicodeDecodeError, json.JSONDecodeError):
        store.record_rejected("unknown", _safe_text(raw_body), ["body_must_be_valid_json"])
        return 422, {"error": "invalid_replay_policy", "reasons": ["body_must_be_valid_json"]}
    operator = submitted.get("operator") if isinstance(submitted, dict) else None
    if not isinstance(operator, str) or not operator.strip():
        store.record_rejected("unknown", _safe_text(raw_body),
                              ["operator_must_be_non_empty_string"])
        return 422, {"error": "invalid_replay_policy",
                     "reasons": ["operator_must_be_non_empty_string"]}
    operator = operator.strip()
    # 策略主体：{"operator": ..., "policy": {"rules": [...]}}，也接受直接给 "rules"
    policy_doc = submitted.get("policy")
    if policy_doc is None and "rules" in submitted:
        policy_doc = {"rules": submitted["rules"]}
    rules, errors = validate_policy(policy_doc)
    if errors:
        store.record_rejected(operator, json.dumps(submitted, ensure_ascii=False), errors)
        return 422, {"error": "invalid_replay_policy", "reasons": errors}
    version, applied_at = store.record_applied(operator, rules)
    return 200, {"result": "applied", "version": version, "applied_at": applied_at}


def current_policy(db: Database, approval_timeout_seconds: float) -> dict:
    """当前生效策略视图：已生效版本，或内置默认策略（version=None）。

    附带灰度视图 rollout：每个风险等级的稳定版本、上一稳定版本与未结束的候选发布。
    """
    from . import replay_rollout
    row = ReplayPolicyStore(db).current_applied()
    if row is None:
        body = {"version": None, "source": "builtin_default",
                "policy": {"rules": builtin_default_rules(approval_timeout_seconds)}}
    else:
        body = {"version": row["version"], "source": "applied",
                "operator": row["operator"], "applied_at": row["created_at"],
                "policy": json.loads(row["policy"])}
    with db.tx() as cur:
        body["rollout"] = replay_rollout.current_rollout_view(cur, approval_timeout_seconds)
    return body


def policy_history(db: Database, limit: int) -> dict:
    """每次策略变更/尝试的记录（新的在前，含被拒绝的提交及原因）。"""
    versions = []
    for row in ReplayPolicyStore(db).history(limit):
        policy = row["policy"]
        if policy:
            try:
                policy = json.loads(policy)
            except json.JSONDecodeError:
                pass  # 失败提交的原始报文不是 JSON，原样保留便于排查
        versions.append({
            "id": row["id"], "version": row["version"], "result": row["result"],
            "operator": row["operator"],
            "reason": json.loads(row["reason"]) if row["reason"] else None,
            "policy": policy, "created_at": row["created_at"],
        })
    return {"versions": versions}


def _safe_text(raw_body: bytes) -> str:
    return raw_body.decode("utf-8", errors="replace")


def create_policy_router(db: Database, approval_timeout_seconds: float) -> APIRouter:
    router = APIRouter(prefix="/admin/replay-policies", tags=["replay-policies"])

    @router.post("")
    async def submit_policy_endpoint(request: Request):
        """提交新审批策略：校验通过整份生效（新版本号），失败保留当前策略；
        两种结果都落版本记录与审计。已提交的批次不受影响（它们持有自己的快照）。"""
        status, payload = submit_policy(db, await request.body())
        return JSONResponse(status_code=status, content=payload)

    @router.get("/current")
    def current_policy_endpoint():
        """当前生效的审批策略（无已生效版本时返回内置默认策略）。"""
        return current_policy(db, approval_timeout_seconds)

    @router.get("/versions")
    def policy_versions_endpoint(limit: int = Query(100, le=1000)):
        """每次策略变更/尝试的记录：版本、操作者、时间、结果（含被拒绝的提交）。"""
        return policy_history(db, limit)

    return router

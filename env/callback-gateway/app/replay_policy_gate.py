"""策略变更的影响预览与审批门禁。

在整份策略生效（replay_policy.py）与灰度发布/回滚（replay_rollout.py）之上，
为「提交新策略」与「发布候选版本」两类变更提供上线前的影响预览与审批门禁：

- 影响预览（POST /admin/replay-policies/preview，只读；或随变更单提交时生成并固化）：
  按当前生效的稳定策略与近期重放批次的风险等级/规模分布，计算
  · 受影响范围：以新旧规则的规模边界把批次规模切成区间，逐区间对比命中规则；
  · 预计命中比例：以 replay_batches 历史批次为样本，估算命中规则会变化的批次占比
    （发布候选时另按规模闸门 × 分流百分比估算进入候选车道的比例）；
  · 审批节点变化：每个受影响区间前后命中规则的节点链（角色/法定人数/时限/模式）；
  · 不兼容规则：高风险 fail-closed 缺口（无规则匹配的规模区间）、被前序规则完全
    遮蔽的无效规则、候选策略不覆盖灰度闸门。
  预览结果带策略版本（候选版本号，或新策略预计获得的版本号）与生成时间，
  只读不落数据，不改变任何线上配置；
- 变更单（replay_policy_changes）：提交即固化预览与基线（当前 applied 版本 +
  各等级稳定指针）。高风险变更（触及 high 等级、削弱审批强度、或引入高风险无规则
  缺口）必须由不同于提交人的运营 approve 后才能 apply；标准变更由提交人直接 apply。
  pending/approved 期间旧稳定版本继续服务——变更单不触碰任何线上指针；
- 生效原子性：拒绝、超时（expires_at，worker 扫描 + 决定/执行时惰性判定）、
  重复提交（request_id 幂等键）、并发审批（写事务串行 + 条件状态转移）都只是变更单
  的状态转移；apply 把「整份策略生效 / 候选灰度发布」与变更单落定放在同一事务，
  且执行前校验基线未被其他变更推进（stale 拒绝）——要么全部生效，要么全部不生效；
- 可追溯：变更单保存变更前后版本、影响预览与审批决定，每次状态转移写 events 审计，
  详情端点连同审计轨迹一起返回。已提交的重放批次持有自己的策略快照，不受变更影响。
"""
from __future__ import annotations

import json
import sqlite3
import time

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import audit
from . import notifications as notif
from .db import Database
from .replay_policy import apply_policy_tx, match_rule, validate_policy
from .replay_rollout import (
    TRACKED_LEVELS,
    _load_applied_version,
    _size_overlap,
    _stable_row,
    _validate_gates,
    load_version_rules,
    publish_release_tx,
)

CHANGE_APPLY_POLICY = "apply_policy"
CHANGE_PUBLISH_RELEASE = "publish_release"

STATUS_PENDING = "pending"
STATUS_APPROVED = "approved"
STATUS_APPLIED = "applied"
STATUS_REJECTED = "rejected"
STATUS_EXPIRED = "expired"
OPEN_STATUSES = (STATUS_PENDING, STATUS_APPROVED)
CHANGE_STATUSES = (STATUS_PENDING, STATUS_APPROVED, STATUS_APPLIED,
                   STATUS_REJECTED, STATUS_EXPIRED)

RISK_HIGH = "high"
RISK_STANDARD = "standard"


# ---- 请求模型 ----------------------------------------------------------------

class PreviewRequest(BaseModel):
    """影响预览入参：policy（新策略文档）与 candidate_version（已 applied 的候选
    版本，配合 risk_level/rollout_percent/min_size/max_size 预演灰度发布）二选一。"""
    policy: dict | None = None
    candidate_version: int | None = None
    risk_level: str | None = None
    rollout_percent: int | None = None
    min_size: int | None = None
    max_size: int | None = None


class ChangeSubmitRequest(PreviewRequest):
    operator: str
    request_id: str | None = None    # 提交幂等键：重复提交返回原变更单
    note: str = ""


class ChangeDecisionRequest(BaseModel):
    operator: str
    reason: str = ""                 # 拒绝必填
    note: str = ""


# ---- 纯函数：规则对比 / 区间切分 ----------------------------------------------

def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return str(value).strip()


def _node_chain(rule: dict | None) -> list[dict] | None:
    """规则命中的审批节点链（规范化视图，内置默认策略与落库策略可直接比较）。"""
    if rule is None:
        return None
    return [{"seq": i, "role": n["role"],
             "roles": n.get("roles") or [n["role"]],
             "required_approvals": n.get("required_approvals", 1),
             "timeout_seconds": n["timeout_seconds"]}
            for i, n in enumerate(rule["nodes"])]


def _rule_fingerprint(rule: dict | None) -> str | None:
    if rule is None:
        return None
    return json.dumps({
        "name": rule["name"], "risk_level": rule["risk_level"],
        "min_size": rule["min_size"], "max_size": rule["max_size"],
        "mode": rule["mode"], "nodes": _node_chain(rule),
    }, sort_keys=True)


def _rules_differ(a: dict | None, b: dict | None) -> bool:
    return _rule_fingerprint(a) != _rule_fingerprint(b)


def _weakens(old: dict | None, new: dict | None) -> bool:
    """审批强度是否被削弱：从需要审批变成无需审批、节点变少、法定人数变少
    或审批时限变长。原本无需审批（无规则/空节点链）的变化不算削弱。"""
    old_nodes = old["nodes"] if old is not None else []
    new_nodes = new["nodes"] if new is not None else []
    if not old_nodes:
        return False
    if not new_nodes:
        return True
    if len(new_nodes) < len(old_nodes):
        return True
    if sum(n.get("required_approvals", 1) for n in new_nodes) < \
            sum(n.get("required_approvals", 1) for n in old_nodes):
        return True
    old_timeout = max((n["timeout_seconds"] for n in old_nodes), default=0.0)
    new_timeout = max((n["timeout_seconds"] for n in new_nodes), default=0.0)
    return new_timeout > old_timeout


def _boundary_sizes(rule_sets: list[list[dict]], level: str) -> list[int]:
    """新旧规则在该风险等级下的全部规模边界（探针起点）：匹配结果只在边界处变化。"""
    starts = {1}
    for rules in rule_sets:
        for r in rules:
            if r["risk_level"] not in ("any", level):
                continue
            if r["min_size"] is not None:
                starts.add(r["min_size"])
            if r["max_size"] is not None:
                starts.add(r["max_size"] + 1)
    return sorted(starts)


def _change_kind(old: dict | None, new: dict | None) -> str:
    if old is None:
        return "rule_added"
    if new is None:
        return "rule_removed"
    if old["mode"] != new["mode"] or _node_chain(old) != _node_chain(new):
        return "approval_chain_modified"
    return "rule_replaced"  # 节点链一致，仅规则身份/区间变化


def _affected_scope(old_rules: list[dict], new_rules: list[dict], level: str,
                    gate: tuple[int | None, int | None] | None) -> list[dict]:
    """逐规模区间对比新旧命中规则，输出受影响范围（相邻同签名区间合并）。"""
    starts = _boundary_sizes([old_rules, new_rules], level)
    out: list[dict] = []
    for i, lo in enumerate(starts):
        hi = starts[i + 1] - 1 if i + 1 < len(starts) else None
        if gate is not None and not _size_overlap(lo, hi, gate[0], gate[1]):
            continue
        old_rule = match_rule(old_rules, level, lo)
        new_rule = match_rule(new_rules, level, lo)
        if not _rules_differ(old_rule, new_rule):
            continue
        entry = {
            "risk_level": level, "min_size": lo, "max_size": hi,
            "before_rule": old_rule["name"] if old_rule else None,
            "after_rule": new_rule["name"] if new_rule else None,
            "change": _change_kind(old_rule, new_rule),
            "approval_nodes": {"before": _node_chain(old_rule),
                               "after": _node_chain(new_rule)},
            "weakens_approval": _weakens(old_rule, new_rule),
        }
        if out and out[-1]["max_size"] is not None \
                and out[-1]["max_size"] + 1 == lo \
                and out[-1]["before_rule"] == entry["before_rule"] \
                and out[-1]["after_rule"] == entry["after_rule"] \
                and out[-1]["change"] == entry["change"] \
                and out[-1]["weakens_approval"] == entry["weakens_approval"] \
                and out[-1]["approval_nodes"] == entry["approval_nodes"]:
            out[-1]["max_size"] = hi
        else:
            out.append(entry)
    return out


def _uncovered_high_intervals(new_rules: list[dict],
                              gate: tuple[int | None, int | None] | None) -> list[dict]:
    """新策略下高风险无规则匹配的规模区间：这些批次提交将被 fail closed 拒绝。"""
    starts = _boundary_sizes([new_rules], "high")
    out: list[dict] = []
    for i, lo in enumerate(starts):
        hi = starts[i + 1] - 1 if i + 1 < len(starts) else None
        if gate is not None and not _size_overlap(lo, hi, gate[0], gate[1]):
            continue
        if match_rule(new_rules, "high", lo) is not None:
            continue
        if out and out[-1]["max_size"] is not None and out[-1]["max_size"] + 1 == lo:
            out[-1]["max_size"] = hi
        else:
            out.append({"type": "high_risk_uncovered", "risk_level": "high",
                        "min_size": lo, "max_size": hi,
                        "detail": "high risk batches of this size will be "
                                  "rejected (fail closed)"})
    return out


def _shadowed_rules(rules: list[dict]) -> list[dict]:
    """被前序规则完全遮蔽的无效规则：其整个规模区间上总有更早的规则先匹配。"""
    out = []
    for i, rule in enumerate(rules):
        if i == 0:
            continue
        levels = [level for level in TRACKED_LEVELS
                  if rule["risk_level"] in ("any", level)]
        lo = rule["min_size"] or 1
        hi = rule["max_size"]
        # 探针：区间起点 + 前序规则落在区间内的边界（匹配结果只在边界处变化）
        probes = {lo}
        for prev in rules[:i]:
            for b in (prev["min_size"],
                      prev["max_size"] + 1 if prev["max_size"] is not None else None):
                if b is not None and b > lo and (hi is None or b <= hi):
                    probes.add(b)
        shadowed = True
        for level in levels:
            for p in probes:
                first = next((j for j, r in enumerate(rules)
                              if r["risk_level"] in ("any", level)
                              and (r["min_size"] is None or p >= r["min_size"])
                              and (r["max_size"] is None or p <= r["max_size"])), None)
                if first == i:
                    shadowed = False
                    break
            if not shadowed:
                break
        if shadowed:
            out.append({"type": "shadowed_rule", "rule": rule["name"],
                        "rule_index": i,
                        "detail": "rule can never match: earlier rules cover "
                                  "its entire range"})
    return out


# ---- 影响预览 ------------------------------------------------------------------

def _estimated_hit(cur: sqlite3.Cursor, change_type: str,
                   old_rules_by_level: dict, new_rules_by_level: dict,
                   release: dict | None) -> dict:
    """预计命中比例：以 replay_batches 历史批次（风险等级 × 规模分布）为样本。

    整份策略：命中规则会变化的批次占比；发布候选：落入规模闸门的批次占比 ×
    分流百分比 = 预计进入候选车道的比例。无历史样本时比例为 None（basis=no_history）。
    """
    rows = cur.execute(
        "SELECT risk_level, total, COUNT(*) AS c FROM replay_batches "
        "WHERE risk_level IN ('high','normal') GROUP BY risk_level, total").fetchall()
    sampled = sum(r["c"] for r in rows)
    basis = "replay_batches_history" if sampled else "no_history"
    if change_type == CHANGE_PUBLISH_RELEASE:
        level = release["risk_level"]
        in_gate = sum(r["c"] for r in rows
                      if r["risk_level"] == level
                      and (release["min_size"] is None or r["total"] >= release["min_size"])
                      and (release["max_size"] is None or r["total"] <= release["max_size"]))
        in_gate_ratio = (in_gate / sampled) if sampled else None
        hit_ratio = (in_gate_ratio * release["rollout_percent"] / 100
                     if in_gate_ratio is not None else None)
        return {"basis": basis, "sampled_batches": sampled,
                "in_gate_batches": in_gate, "in_gate_ratio": in_gate_ratio,
                "rollout_percent": release["rollout_percent"],
                "estimated_hit_ratio": hit_ratio,
                "estimated_hit_batches": (hit_ratio * sampled
                                          if hit_ratio is not None else None)}
    affected = 0
    per_level = {level: {"sampled_batches": 0, "affected_batches": 0}
                 for level in TRACKED_LEVELS}
    for r in rows:
        level = r["risk_level"]
        old_rule = match_rule(old_rules_by_level[level], level, r["total"])
        new_rule = match_rule(new_rules_by_level[level], level, r["total"])
        per_level[level]["sampled_batches"] += r["c"]
        if _rules_differ(old_rule, new_rule):
            affected += r["c"]
            per_level[level]["affected_batches"] += r["c"]
    for level in TRACKED_LEVELS:
        n = per_level[level]["sampled_batches"]
        per_level[level]["estimated_hit_ratio"] = (
            per_level[level]["affected_batches"] / n) if n else None
    return {"basis": basis, "sampled_batches": sampled,
            "affected_batches": affected,
            "estimated_hit_ratio": (affected / sampled) if sampled else None,
            "by_risk_level": per_level}


def build_preview(cur: sqlite3.Cursor, *, change_type: str, new_rules: list[dict],
                  candidate_version: int | None, release: dict | None,
                  approval_timeout: float, now: float) -> dict:
    """计算影响预览。只读：不写任何数据、不改变线上配置。

    结果带策略版本（候选版本号；新策略为预计获得的版本号，version_status=expected）
    与生成时间；基线（base_version + 各等级稳定指针）随预览固化，供执行前校验。
    """
    base_version = cur.execute(
        "SELECT MAX(version) AS v FROM replay_policy_versions "
        "WHERE result='applied'").fetchone()["v"]
    stable_versions: dict[str, int | None] = {}
    old_rules_by_level: dict[str, list[dict]] = {}
    for level in TRACKED_LEVELS:
        stable = _stable_row(cur, level)
        stable_versions[level] = stable["policy_version"] if stable is not None else None
        old_rules_by_level[level] = load_version_rules(cur, stable_versions[level],
                                                       approval_timeout)
    if change_type == CHANGE_PUBLISH_RELEASE:
        levels = [release["risk_level"]]
        gate: tuple[int | None, int | None] | None = \
            (release["min_size"], release["max_size"])
        new_rules_by_level = {release["risk_level"]: new_rules}
    else:
        levels = list(TRACKED_LEVELS)
        gate = None
        new_rules_by_level = {level: new_rules for level in levels}

    affected: list[dict] = []
    incompatible: list[dict] = []
    for level in levels:
        affected.extend(_affected_scope(old_rules_by_level[level],
                                        new_rules_by_level[level], level, gate))
        if level == "high":
            incompatible.extend(
                _uncovered_high_intervals(new_rules_by_level[level], gate))
    incompatible.extend(_shadowed_rules(new_rules))
    if change_type == CHANGE_PUBLISH_RELEASE:
        covering = [r for r in new_rules
                    if r["risk_level"] in ("any", release["risk_level"])
                    and _size_overlap(r["min_size"], r["max_size"],
                                      release["min_size"], release["max_size"])]
        if not covering:
            incompatible.append({
                "type": "candidate_policy_does_not_cover_gate",
                "risk_level": release["risk_level"],
                "min_size": release["min_size"], "max_size": release["max_size"],
                "detail": "candidate policy has no rule covering the declared "
                          "size gate; routed batches would fail closed"})
        if candidate_version == stable_versions[release["risk_level"]]:
            incompatible.append({
                "type": "candidate_is_stable",
                "risk_level": release["risk_level"],
                "detail": "candidate_version is already the stable version "
                          "for this risk level"})

    risk_high = (any(a["risk_level"] == "high" for a in affected)
                 or any(a["weakens_approval"] for a in affected)
                 or any(i["type"] == "high_risk_uncovered" for i in incompatible))
    max_version = cur.execute(
        "SELECT MAX(version) AS v FROM replay_policy_versions").fetchone()["v"] or 0
    return {
        "generated_at": now,
        "change_type": change_type,
        # 预览针对的策略版本：候选发布为已 applied 的候选版本；新策略为预计版本号
        "policy_version": (candidate_version if candidate_version is not None
                           else max_version + 1),
        "version_status": "applied" if candidate_version is not None else "expected",
        "base_version": base_version,
        "base_stable_versions": stable_versions,
        "risk_class": RISK_HIGH if risk_high else RISK_STANDARD,
        "requires_approval": risk_high,
        "affected_scope": affected,
        "estimated_hit": _estimated_hit(cur, change_type, old_rules_by_level,
                                        new_rules_by_level, release),
        "incompatible_rules": incompatible,
        "release": release,
    }


def _resolve_spec(cur: sqlite3.Cursor, req: PreviewRequest):
    """校验并解析变更目标，返回 (change_type, 新规则, 候选版本, 发布参数)。"""
    has_policy = req.policy is not None
    has_candidate = req.candidate_version is not None
    if has_policy == has_candidate:
        raise HTTPException(
            422, "exactly one of policy or candidate_version must be given")
    if has_policy:
        if any(v is not None for v in (req.risk_level, req.rollout_percent,
                                       req.min_size, req.max_size)):
            raise HTTPException(
                422, "risk_level/rollout_percent/min_size/max_size only apply to "
                     "candidate_version (publish_release) changes")
        rules, errors = validate_policy(req.policy)
        if errors:
            raise HTTPException(422, {"error": "invalid_replay_policy",
                                      "reasons": errors})
        return CHANGE_APPLY_POLICY, rules, None, None
    if req.risk_level is None or req.rollout_percent is None:
        raise HTTPException(
            422, "risk_level and rollout_percent are required for "
                 "candidate_version (publish_release) changes")
    _validate_gates(req.risk_level, req.candidate_version, req.rollout_percent,
                    req.min_size, req.max_size)
    row = _load_applied_version(cur, req.candidate_version)  # 404：版本不存在/未生效
    rules = json.loads(row["policy"])["rules"]
    release = {"risk_level": req.risk_level, "min_size": req.min_size,
               "max_size": req.max_size, "rollout_percent": req.rollout_percent}
    return CHANGE_PUBLISH_RELEASE, rules, req.candidate_version, release


def preview_impact(db: Database, req: PreviewRequest, approval_timeout: float) -> dict:
    """只读影响预览：不落任何数据、不改变线上配置，供提交/发布前核对。"""
    with db.tx() as cur:  # 与一次配置切换串行，看到完整的前后状态之一
        change_type, rules, candidate_version, release = _resolve_spec(cur, req)
        return build_preview(cur, change_type=change_type, new_rules=rules,
                             candidate_version=candidate_version, release=release,
                             approval_timeout=approval_timeout, now=time.time())


# ---- 变更单：提交 / 审批 / 执行 -------------------------------------------------

def _load_change_or_404(cur: sqlite3.Cursor, change_id: int) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM replay_policy_changes WHERE id=?",
                      (change_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "policy change not found")
    return row


def _change_view(cur: sqlite3.Cursor, row: sqlite3.Row, now: float) -> dict:
    status = row["status"]
    if status in OPEN_STATUSES and row["expires_at"] <= now:
        # 展示层惰性过期（决定/执行路径会在事务内真正落状态并写审计）
        status = STATUS_EXPIRED
    return {
        "id": row["id"], "request_id": row["request_id"],
        "change_type": row["change_type"], "status": status,
        "operator": row["operator"], "note": row["note"],
        "risk_class": row["risk_class"],
        "requires_approval": bool(row["requires_approval"]),
        "base_version": row["base_version"],
        "base_stable_versions": json.loads(row["base_stable_versions"]),
        "candidate_version": row["candidate_version"],
        "release": ({"risk_level": row["risk_level"], "min_size": row["min_size"],
                     "max_size": row["max_size"],
                     "rollout_percent": row["rollout_percent"]}
                    if row["change_type"] == CHANGE_PUBLISH_RELEASE else None),
        "preview": json.loads(row["preview"]),
        "approval": {"decision": row["decision"], "decided_by": row["decided_by"],
                     "decided_at": row["decided_at"],
                     "reason": row["decision_reason"],
                     "note": row["decision_note"]},
        "result": {"applied_version": row["applied_version"],
                   "release_id": row["release_id"],
                   "applied_by": row["applied_by"],
                   "applied_at": row["applied_at"]},
        "expires_at": row["expires_at"],
        "created_at": row["created_at"], "updated_at": row["updated_at"],
    }


def create_change(db: Database, req: ChangeSubmitRequest, approval_timeout: float,
                  ttl: float, now: float) -> tuple[int, dict]:
    """提交策略变更单：校验 + 生成并固化影响预览，状态 pending（不影响线上）。

    request_id 幂等：重复提交返回原变更单，不生成第二张。
    """
    operator = _require(req.operator, "operator")
    note = (req.note or "").strip()
    request_id = (req.request_id.strip()
                  if req.request_id and req.request_id.strip() else None)
    with db.tx() as cur:
        if request_id is not None:
            existing = cur.execute(
                "SELECT * FROM replay_policy_changes WHERE request_id=?",
                (request_id,)).fetchone()
            if existing is not None:
                return 200, {"result": "duplicate",
                             "change": _change_view(cur, existing, now)}
        change_type, rules, candidate_version, release = _resolve_spec(cur, req)
        preview = build_preview(cur, change_type=change_type, new_rules=rules,
                                candidate_version=candidate_version,
                                release=release, approval_timeout=approval_timeout,
                                now=now)
        try:
            cur.execute(
                """INSERT INTO replay_policy_changes
                   (request_id, change_type, status, operator, note, policy,
                    candidate_version, risk_level, min_size, max_size, rollout_percent,
                    base_version, base_stable_versions, risk_class, requires_approval,
                    preview, expires_at, created_at, updated_at)
                   VALUES (?,?,'pending',?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (request_id, change_type, operator, note or None,
                 json.dumps({"rules": rules}, ensure_ascii=False, sort_keys=True)
                 if change_type == CHANGE_APPLY_POLICY else None,
                 candidate_version,
                 release["risk_level"] if release else None,
                 release["min_size"] if release else None,
                 release["max_size"] if release else None,
                 release["rollout_percent"] if release else None,
                 preview["base_version"],
                 json.dumps(preview["base_stable_versions"], sort_keys=True),
                 preview["risk_class"],
                 1 if preview["requires_approval"] else 0,
                 json.dumps(preview, ensure_ascii=False, sort_keys=True),
                 now + ttl, now, now))
        except sqlite3.IntegrityError:
            # 并发下 request_id 撞唯一键：返回已存在的那一张
            if request_id is not None:
                existing = cur.execute(
                    "SELECT * FROM replay_policy_changes WHERE request_id=?",
                    (request_id,)).fetchone()
                if existing is not None:
                    return 200, {"result": "duplicate",
                                 "change": _change_view(cur, existing, now)}
            raise
        change_id = cur.lastrowid
        audit.record(cur, "replay_policy_change_submitted", None, None, {
            "change_id": change_id, "change_type": change_type,
            "operator": operator, "request_id": request_id,
            "risk_class": preview["risk_class"],
            "requires_approval": preview["requires_approval"],
            "base_version": preview["base_version"],
            "candidate_version": candidate_version,
            "expires_at": now + ttl}, ts=now)
        row = _load_change_or_404(cur, change_id)
        # 审批通知：需要审批的变更单在同事务内给联系人目录中的可审批人生成待办
        if row["requires_approval"]:
            notif.emit_change_required_tx(cur, row, now)
        return 201, {"result": "created", "change": _change_view(cur, row, now)}


def _expire_if_due(db: Database, change_id: int, now: float) -> None:
    """惰性过期：到期的未决变更落为 expired 并写审计（独立事务，先提交——
    若与决定/执行放在同一事务，随后的 409 回滚会把过期状态与审计一起丢掉）。"""
    with db.tx() as cur:
        row = cur.execute(
            "SELECT * FROM replay_policy_changes WHERE id=?", (change_id,)).fetchone()
        if row is None or row["status"] not in OPEN_STATUSES \
                or row["expires_at"] > now:
            return
        cur.execute(
            """UPDATE replay_policy_changes SET status='expired', updated_at=?
               WHERE id=? AND status IN ('pending','approved')""",
            (now, change_id))
        audit.record(cur, "replay_policy_change_expired", None, None, {
            "change_id": row["id"], "change_type": row["change_type"],
            "operator": row["operator"], "previous_status": row["status"],
            "expires_at": row["expires_at"]}, ts=now)
        # 审批通知：惰性超时同样告知提交人
        notif.emit_change_decided_tx(cur, row, "expired", "", "", now)


def decide_change_tx(cur: sqlite3.Cursor, change_id: int, action: str,
                     operator: str, reason: str, note: str, now: float) -> dict:
    """decide_change 的事务内实现（供 HTTP 端点与待办回写在同一事务内调用）。

    审批人必须不同于提交人；并发决定由写事务串行 + 条件状态转移保证只有一个生效。
    """
    row = _load_change_or_404(cur, change_id)
    if action == "reject" and not (reason or "").strip():
        raise HTTPException(422, "reason is required to reject a change")
    # 惰性过期：到期的未决变更按 expired 落定（调用方 _expire_if_due 已在独立
    # 事务提交过期；走到这里仍开放才继续）
    if row["status"] not in OPEN_STATUSES or row["expires_at"] <= now:
        raise HTTPException(409, "change has expired or is no longer open")
    if action == "approve" and row["status"] != STATUS_PENDING:
        raise HTTPException(
            409, f"change is {row['status']}, only pending changes can be approved")
    if action == "reject" and row["status"] not in OPEN_STATUSES:
        raise HTTPException(
            409, f"change is {row['status']}, cannot be rejected")
    if operator == row["operator"]:
        raise HTTPException(
            409, "deciding operator must differ from the change submitter")
    if action == "approve" and not row["requires_approval"]:
        raise HTTPException(
            409, "change does not require approval; the submitter can "
                 "apply it directly")
    new_status = STATUS_APPROVED if action == "approve" else STATUS_REJECTED
    guard = "status='pending'" if action == "approve" \
        else "status IN ('pending','approved')"
    changed = cur.execute(
        f"""UPDATE replay_policy_changes
            SET status=?, decision=?, decided_by=?, decided_at=?,
                decision_reason=?, decision_note=?, updated_at=?
            WHERE id=? AND {guard}""",
        (new_status, new_status, operator, now,
         reason or None, (note or "").strip() or None, now, change_id),
    ).rowcount
    if not changed:  # 并发下已被另一个决定落定
        raise HTTPException(409, "change was concurrently decided")
    audit.record(cur,
                 "replay_policy_change_approved" if action == "approve"
                 else "replay_policy_change_rejected",
                 None, None, {
                     "change_id": change_id, "change_type": row["change_type"],
                     "operator": row["operator"], "decided_by": operator,
                     "risk_class": row["risk_class"],
                     "reason": reason or None,
                     "note": (note or "").strip() or None}, ts=now)
    # 审批通知：批准/拒绝告知提交人（纯告知）
    notif.emit_change_decided_tx(cur, row, action, operator, reason or None, now)
    return {"result": new_status,
            "change": _change_view(cur, _load_change_or_404(cur, change_id), now)}


def decide_change(db: Database, change_id: int, action: str, operator: str,
                  reason: str, note: str, now: float) -> dict:
    """审批决定：approve（仅高风险变更需要）/ reject（否决，原因必填）。

    审批人必须不同于提交人；并发决定由写事务串行 + 条件状态转移保证只有一个生效。
    """
    operator = _require(operator, "operator")
    reason = (reason or "").strip()
    if action == "reject" and not reason:
        raise HTTPException(422, "reason is required to reject a change")
    _expire_if_due(db, change_id, now)
    with db.tx() as cur:
        return decide_change_tx(cur, change_id, action, operator, reason, note, now)


def apply_change(db: Database, change_id: int, operator: str,
                 approval_timeout: float, now: float) -> dict:
    """执行变更：高风险变更须已 approved，标准变更 pending 即可（提交人自行执行）。

    「整份策略生效 / 候选灰度发布」与变更单落定在同一事务：任一校验失败整体回滚，
    不会产生部分生效。执行前校验预览基线（applied 版本 + 各等级稳定指针）未被
    其他变更推进，被推进则拒绝（stale）——需重新预览生成新变更单。
    """
    operator = _require(operator, "operator")
    _expire_if_due(db, change_id, now)
    with db.tx() as cur:
        row = _load_change_or_404(cur, change_id)
        expected = STATUS_APPROVED if row["requires_approval"] else STATUS_PENDING
        if row["status"] != expected:
            if row["requires_approval"] and row["status"] == STATUS_PENDING:
                raise HTTPException(
                    409, "high-risk change must be approved by a different "
                         "operator before apply")
            raise HTTPException(
                409, f"change is {row['status']}, cannot be applied")
        # 基线校验：预览之后线上策略被推进过 -> 预览失效，拒绝执行
        base_now = cur.execute(
            "SELECT MAX(version) AS v FROM replay_policy_versions "
            "WHERE result='applied'").fetchone()["v"]
        stable_now: dict[str, int | None] = {}
        for level in TRACKED_LEVELS:
            stable = _stable_row(cur, level)
            stable_now[level] = stable["policy_version"] if stable is not None else None
        stale = []
        if base_now != row["base_version"]:
            stale.append({"field": "base_version",
                          "at_preview": row["base_version"], "now": base_now})
        saved_stable = json.loads(row["base_stable_versions"])
        if stable_now != saved_stable:
            stale.append({"field": "stable_versions",
                          "at_preview": saved_stable, "now": stable_now})
        if stale:
            raise HTTPException(409, {
                "error": "stale_change",
                "detail": "online policy moved since the preview; "
                          "create a new change to re-preview",
                "stale": stale})
        if row["change_type"] == CHANGE_APPLY_POLICY:
            rules = json.loads(row["policy"])["rules"]
            applied_version = apply_policy_tx(cur, operator, rules, now)
            release_id = None
        else:
            body = publish_release_tx(
                cur, operator=operator, risk_level=row["risk_level"],
                candidate_version=row["candidate_version"],
                rollout_percent=row["rollout_percent"],
                min_size=row["min_size"], max_size=row["max_size"],
                note=row["note"] or "", now=now)
            if isinstance(body, JSONResponse):
                # 闸门覆盖此刻不再满足：抛错回滚整个事务，变更单保持原状
                raise HTTPException(body.status_code,
                                    json.loads(body.body.decode()))
            applied_version = None
            release_id = body["release_id"]
        changed = cur.execute(
            """UPDATE replay_policy_changes
               SET status='applied', applied_version=?, release_id=?,
                   applied_by=?, applied_at=?, updated_at=?
               WHERE id=? AND status=?""",
            (applied_version, release_id, operator, now, now,
             change_id, expected)).rowcount
        if not changed:  # 并发下已被决定/执行
            raise HTTPException(409, "change was concurrently decided")
        audit.record(cur, "replay_policy_change_applied", None, None, {
            "change_id": change_id, "change_type": row["change_type"],
            "operator": row["operator"], "applied_by": operator,
            "risk_class": row["risk_class"],
            "base_version": row["base_version"],
            "applied_version": applied_version, "release_id": release_id,
            "candidate_version": row["candidate_version"],
            "approved_by": (row["decided_by"]
                            if row["decision"] == STATUS_APPROVED else None)}, ts=now)
        # 审批通知：变更已执行，告知提交人（纯告知）
        applied_row = _load_change_or_404(cur, change_id)
        notif.emit_change_decided_tx(cur, applied_row, "applied", operator, "", now)
        return {"result": "applied",
                "change": _change_view(cur, _load_change_or_404(cur, change_id), now)}


def expire_changes(db: Database, now: float) -> int:
    """审批超时扫描：到期的未决/待执行变更置为 expired（幂等，与人工决定互斥）。

    由 replay worker 每轮调用；只改变更单状态，不触碰任何线上配置。
    """
    due = db.query(
        "SELECT * FROM replay_policy_changes "
        "WHERE status IN ('pending','approved') AND expires_at <= ?", (now,))
    expired = 0
    for row in due:
        with db.tx() as cur:
            changed = cur.execute(
                """UPDATE replay_policy_changes SET status='expired', updated_at=?
                   WHERE id=? AND status IN ('pending','approved')
                     AND expires_at <= ?""",
                (now, row["id"], now)).rowcount
            if not changed:  # 并发下已被人工决定/执行
                continue
            audit.record(cur, "replay_policy_change_expired", None, None, {
                "change_id": row["id"], "change_type": row["change_type"],
                "operator": row["operator"], "previous_status": row["status"],
                "expires_at": row["expires_at"]}, ts=now)
            # 审批通知：超时告知提交人（纯告知，无可操作待办）
            notif.emit_change_decided_tx(cur, row, "expired", "", "", now)
            expired += 1
    return expired


# ---- 查询 ----------------------------------------------------------------------

def list_changes(db: Database, status_filter: str | None, limit: int,
                 now: float) -> dict:
    if status_filter is not None and status_filter not in CHANGE_STATUSES:
        raise HTTPException(422, f"invalid status: {status_filter!r} "
                                 f"(expect one of {','.join(CHANGE_STATUSES)})")
    sql, params = "SELECT * FROM replay_policy_changes", []
    if status_filter:
        sql += " WHERE status=?"
        params.append(status_filter)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    with db.tx() as cur:  # 视图与列表保持同一快照
        return {"changes": [_change_view(cur, r, now) for r in rows]}


def _change_audits(cur: sqlite3.Cursor, change_id: int) -> list[dict]:
    """该变更单的审计轨迹（提交/审批/拒绝/超时/执行），新的在前。"""
    rows = cur.execute(
        "SELECT ts, type, detail FROM events "
        "WHERE type LIKE 'replay_policy_change_%' ORDER BY id DESC LIMIT 200",
    ).fetchall()
    out = []
    for r in rows:
        detail = json.loads(r["detail"])
        if detail.get("change_id") == change_id:
            out.append({"ts": r["ts"], "type": r["type"], "detail": detail})
    return out


def get_change(db: Database, change_id: int, now: float) -> dict:
    """变更单详情：变更前后版本、固化的影响预览、审批决定与审计轨迹。"""
    with db.tx() as cur:
        row = _load_change_or_404(cur, change_id)
        view = _change_view(cur, row, now)
        view["audits"] = _change_audits(cur, change_id)
        return view


def create_policy_gate_router(db: Database, approval_timeout: float,
                              change_ttl: float) -> APIRouter:
    router = APIRouter(prefix="/admin/replay-policies",
                       tags=["replay-policy-gate"])

    @router.post("/preview")
    def preview_endpoint(req: PreviewRequest):
        """影响预览（只读）：受影响范围、预计命中比例、审批节点变化、不兼容规则；
        结果带策略版本与生成时间，不改变线上配置。"""
        return preview_impact(db, req, approval_timeout)

    @router.post("/changes")
    def create_endpoint(req: ChangeSubmitRequest):
        """提交策略变更单：固化影响预览与基线，状态 pending（不影响线上）；
        request_id 重复提交返回原变更单。"""
        status, body = create_change(db, req, approval_timeout, change_ttl,
                                     time.time())
        return JSONResponse(status_code=status, content=body)

    @router.get("/changes")
    def list_endpoint(status: str | None = None,
                      limit: int = Query(100, le=1000)):
        """策略变更单列表（可按状态过滤），含预览摘要与审批决定。"""
        return list_changes(db, status, limit, time.time())

    @router.get("/changes/{change_id}")
    def get_endpoint(change_id: int):
        """变更单详情：变更前后版本、影响预览、审批决定与审计轨迹。"""
        return get_change(db, change_id, time.time())

    @router.post("/changes/{change_id}/approve")
    def approve_endpoint(change_id: int, req: ChangeDecisionRequest):
        """批准高风险变更（审批人必须不同于提交人）；批准期间旧稳定版本继续服务。"""
        return decide_change(db, change_id, "approve", req.operator, req.reason,
                             req.note, time.time())

    @router.post("/changes/{change_id}/reject")
    def reject_endpoint(change_id: int, req: ChangeDecisionRequest):
        """拒绝变更（原因必填）：变更单关闭，不产生任何线上效果。"""
        return decide_change(db, change_id, "reject", req.operator, req.reason,
                             req.note, time.time())

    @router.post("/changes/{change_id}/apply")
    def apply_endpoint(change_id: int, req: ChangeDecisionRequest):
        """执行变更：高风险须已批准；配置变更与变更单落定同一事务，不产生部分生效。"""
        return apply_change(db, change_id, req.operator, approval_timeout,
                            time.time())

    return router

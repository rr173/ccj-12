"""审批策略的灰度发布与回滚。

在「整份策略立即生效」（replay_policy.py）之上，运营可以为**同一风险等级**维护
多个候选策略版本，按**批次规模闸门**与**分流百分比**把新提交的重放批次逐步引流到
候选版本；候选可暂停、恢复、转正（promote）与回滚（rollback）。

- 稳定指针 `replay_policy_stable`：每个风险等级（high/normal）一行，记录当前稳定
  策略版本（NULL = 内置默认策略）与上一稳定版本（回滚目标）。整份策略提交时其规则
  覆盖到的等级在这里整份切换（行为与灰度功能引入前一致），同时自动关闭这些等级上
  尚未结束的灰度发布（superseded）；
- 灰度发布单 `replay_policy_releases`：同一风险等级至多一条未结束
  （candidate/paused）的发布（部分唯一索引 + 写事务串行兜底并发发布）；带批次规模
  闸门（min_size/max_size）、分流百分比（rollout_percent 1-100）与该等级单调递增的
  发布批次序号（rollout_seq）；
- 分流确定性：批次是否命中候选只取决于 (风险等级, 稳定分流键, 闸门, 百分比)——
  分流键优先取提交幂等键 request_id，否则取本批选中投递 id 的有序集合哈希，
  bucket = sha256(risk_level|key) % 100，bucket < percent 即命中。结果与提交时刻、
  服务重启无关；重复提交（同 request_id）必然得到同一车道；命中结果随批次落盘
  （policy_lane/rollout_id/rollout_seq + 快照里的闸门/百分比/bucket），之后发布单
  暂停、改百分比或回滚都不改变已落盘批次，只影响之后的新提交；
- 生命周期：发布（candidate）-> 暂停（paused，分流立即停止，新批次全走稳定版本，
  原因必填）-> 恢复（candidate）-> 转正（promoted，该等级稳定指针整份切到候选
  版本）；任意阶段可回滚到上一稳定版本（原因必填）——回滚只改稳定指针/关闭发布单，
  **已经生成审批节点的批次持有自己的策略快照，永不改变**；
- 试运行校验：POST .../evaluate 在不落任何数据的前提下预演分流（车道、bucket、
  命中的版本/规则/节点链），候选版本发布前可用它核对不同批次规模下的行为；
- 全程审计：发布/暂停/恢复/转正/回滚/被整份切换取代，以及每个批次的分流命中
  （replay_policy_batch_routed）都写 events；并发发布与批次提交在 BEGIN IMMEDIATE
  写事务里串行，批次解析到的版本与落盘快照必然来自同一份发布状态，不会出现无版本
  或跨版本快照。
"""
from __future__ import annotations

import hashlib
import json
import sqlite3
import time

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from . import audit
from .db import Database
from .replay_policy import builtin_default_rules, match_rule

TRACKED_LEVELS = ("high", "normal")

RELEASE_CANDIDATE = "candidate"
RELEASE_PAUSED = "paused"
RELEASE_PROMOTED = "promoted"
RELEASE_ROLLED_BACK = "rolled_back"
RELEASE_SUPERSEDED = "superseded"
OPEN_STATUSES = (RELEASE_CANDIDATE, RELEASE_PAUSED)
RELEASE_STATUSES = (RELEASE_CANDIDATE, RELEASE_PAUSED, RELEASE_PROMOTED,
                    RELEASE_ROLLED_BACK, RELEASE_SUPERSEDED)

LANE_STABLE = "stable"
LANE_CANDIDATE = "candidate"


# ---- 请求模型 ----------------------------------------------------------------

class ReleasePublishRequest(BaseModel):
    operator: str                    # 发布人（必填）
    risk_level: str                  # 灰度针对的风险等级：high | normal
    candidate_version: int           # 候选策略版本号（必须是已 applied 的版本）
    rollout_percent: int             # 命中规模闸门的批次分流到候选的百分比（1-100）
    min_size: int | None = None      # 批次规模闸门下限（任务条数，NULL=不限）
    max_size: int | None = None      # 批次规模闸门上限
    note: str = ""


class ReleaseOperatorRequest(BaseModel):
    operator: str
    reason: str = ""                 # 暂停/回滚必填；恢复可选
    note: str = ""


class EvaluateRequest(BaseModel):
    """试运行校验：不落数据，预演给定风险等级/批次规模/分流键下会命中哪个版本与规则。"""
    risk_level: str
    total: int                       # 模拟批次规模（任务条数）
    request_id: str | None = None    # 给定分流键（缺省用固定模拟键，便于反复核对）


# ---- 纯函数：校验 / 分流哈希 --------------------------------------------------

def _require(value: str | None, field: str) -> str:
    if not value or not str(value).strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return str(value).strip()


def routing_bucket(risk_level: str, routing_key: str) -> int:
    """稳定分桶：只取决于风险等级与分流键，返回 0-99；进程重启、重复提交结果恒定。"""
    digest = hashlib.sha256(f"{risk_level}|{routing_key}".encode()).hexdigest()
    return int(digest[:12], 16) % 100


def batch_routing_key(request_id: str | None, delivery_ids: list[int]) -> str:
    """批次的分流键：优先提交幂等键（重复提交同键 -> 同车道）；否则取本批选中投递
    id 的有序集合（同内容集合的再次提交哈希相同，与服务进程/时间无关）。"""
    if request_id:
        return f"req:{request_id.strip()}"
    return "ids:" + ",".join(str(i) for i in sorted(delivery_ids))


def _size_overlap(a_min: int | None, a_max: int | None,
                  b_min: int | None, b_max: int | None) -> bool:
    """两个 [min,max] 整数区间（None 端 = 无限）是否有交集。"""
    lo = max(a_min or 1, b_min or 1)
    uppers = [x for x in (a_max, b_max) if x is not None]
    hi = min(uppers) if uppers else None
    return hi is None or lo <= hi


def _first_covering_rule_name(rules: list[dict], risk_level: str) -> str | None:
    return next((r["name"] for r in rules
                 if r["risk_level"] in ("any", risk_level)), None)


def _validate_gates(risk_level, candidate_version, rollout_percent, min_size, max_size):
    if risk_level not in TRACKED_LEVELS:
        raise HTTPException(422, f"invalid risk_level: {risk_level!r} "
                                 f"(expect one of {','.join(TRACKED_LEVELS)})")
    if isinstance(candidate_version, bool) or not isinstance(candidate_version, int) \
            or candidate_version < 1:
        raise HTTPException(422, "candidate_version must be an integer >= 1")
    if isinstance(rollout_percent, bool) or not isinstance(rollout_percent, int) \
            or not 1 <= rollout_percent <= 100:
        raise HTTPException(422, "rollout_percent must be an integer in [1, 100]")
    for field, value in (("min_size", min_size), ("max_size", max_size)):
        if value is not None and (
                isinstance(value, bool) or not isinstance(value, int) or value < 1):
            raise HTTPException(422, f"{field} must be an integer >= 1 when present")
    if min_size is not None and max_size is not None and min_size > max_size:
        raise HTTPException(422, "min_size must be <= max_size")


# ---- 版本/规则读取（必须在写事务内调用以拿到一致快照） --------------------------

def load_version_rules(cur: sqlite3.Cursor, version: int | None,
                       approval_timeout: float) -> list[dict]:
    """取某策略版本的规范化规则；version=None（无稳定指针）用内置默认策略。"""
    if version is None:
        return builtin_default_rules(approval_timeout)
    row = cur.execute(
        "SELECT policy FROM replay_policy_versions "
        "WHERE version=? AND result='applied' ORDER BY id DESC LIMIT 1",
        (version,)).fetchone()
    if row is None:
        # 数据库被外部破坏才会走到：引用了不存在的 applied 版本。fail closed。
        raise HTTPException(409, f"policy version {version} is not available")
    return json.loads(row["policy"])["rules"]


def _load_applied_version(cur: sqlite3.Cursor, version: int) -> sqlite3.Row:
    row = cur.execute(
        "SELECT * FROM replay_policy_versions WHERE version=? AND result='applied' "
        "ORDER BY id DESC LIMIT 1", (version,)).fetchone()
    if row is None:
        raise HTTPException(404, f"applied policy version {version} not found")
    return row


def _stable_row(cur: sqlite3.Cursor, risk_level: str) -> sqlite3.Row | None:
    return cur.execute(
        "SELECT * FROM replay_policy_stable WHERE risk_level=?", (risk_level,)).fetchone()


def _open_release(cur: sqlite3.Cursor, risk_level: str) -> sqlite3.Row | None:
    """该等级当前未结束（candidate/paused）的灰度发布；至多一条。"""
    return cur.execute(
        "SELECT * FROM replay_policy_releases WHERE risk_level=? "
        "AND status IN ('candidate','paused') ORDER BY id DESC LIMIT 1",
        (risk_level,)).fetchone()


# ---- 分流解析（批次提交事务内调用） --------------------------------------------

def resolve_route(cur: sqlite3.Cursor, risk_level: str, total: int, routing_key: str,
                  approval_timeout: float) -> dict:
    """在批次写事务内解析本批应走哪个策略版本（与发布状态变更串行，快照一致）。

    返回车道（stable/candidate）、命中的发布单/批次序号、规模闸门判定、分桶、
    稳定/候选版本与解析出的规则。只负责「选版本」，不做 fail-closed 判定——
    规则是否匹配该批由调用方用 match_rule 决定（高风险无匹配仍拒绝提交）。
    """
    stable = _stable_row(cur, risk_level)
    stable_version = stable["policy_version"] if stable is not None else None
    release = _open_release(cur, risk_level)
    bucket = routing_bucket(risk_level, routing_key)

    release_view = None
    if release is not None:
        size_in_gate = (release["min_size"] is None or total >= release["min_size"]) and \
                       (release["max_size"] is None or total <= release["max_size"])
        release_view = {
            "release_id": release["id"], "rollout_seq": release["rollout_seq"],
            "candidate_version": release["candidate_version"],
            "status": release["status"],
            "min_size": release["min_size"], "max_size": release["max_size"],
            "rollout_percent": release["rollout_percent"],
            "size_in_gate": size_in_gate, "bucket": bucket,
        }

    # 只有 candidate（非暂停）、规模在闸门内且落入百分比桶的批次才走候选版本；
    # paused 发布单立即停止分流（新批次全走稳定版本），发布单与序号仍随批次留痕。
    routed = (release is not None and release["status"] == RELEASE_CANDIDATE
              and release_view["size_in_gate"]
              and bucket < release["rollout_percent"])
    if routed:
        version = release["candidate_version"]
        lane = LANE_CANDIDATE
        release_id, rollout_seq = release["id"], release["rollout_seq"]
    else:
        version = stable_version
        lane = LANE_STABLE
        release_id = None
        # 稳定车道也记录「当前发布批次」序号（没有任何发布单时为 None）
        rollout_seq = release["rollout_seq"] if release is not None else None
    rules = load_version_rules(cur, version, approval_timeout)
    return {
        "lane": lane, "release_id": release_id, "rollout_seq": rollout_seq,
        "policy_version": version, "rules": rules, "bucket": bucket,
        "routing_key": routing_key, "stable_version": stable_version,
        "release": release_view,
    }


def route_snapshot(route: dict, risk_level: str, total: int,
                   rule_name: str | None, mode: str) -> dict:
    """随批次落盘的分流快照：命中版本/车道/发布单/分流规则，事后可完整追溯。"""
    rel = route["release"]
    return {
        "risk_level": risk_level, "batch_size": total,
        "lane": route["lane"],
        "policy_version": route["policy_version"],
        "stable_version": route["stable_version"],
        "release_id": route["release_id"],
        "rollout_seq": route["rollout_seq"],
        "candidate_version": rel["candidate_version"] if rel else None,
        "release_status": rel["status"] if rel else None,
        "routing_key": route["routing_key"], "bucket": route["bucket"],
        "min_size": rel["min_size"] if rel else None,
        "max_size": rel["max_size"] if rel else None,
        "rollout_percent": rel["rollout_percent"] if rel else None,
        "size_in_gate": rel["size_in_gate"] if rel else None,
        "rule_name": rule_name, "mode": mode,
    }


# ---- 生命周期：发布 / 暂停 / 恢复 / 转正 / 回滚 --------------------------------

def publish_release(db: Database, req: ReleasePublishRequest,
                    now: float) -> dict | JSONResponse:
    """发布候选策略灰度：校验候选版本与规模闸门覆盖，分配该等级的发布批次序号。

    不改变稳定版本；同等级已有未结束发布（candidate/paused）时拒绝（409）。
    """
    operator = _require(req.operator, "operator")
    _validate_gates(req.risk_level, req.candidate_version, req.rollout_percent,
                    req.min_size, req.max_size)
    note = (req.note or "").strip()
    with db.tx() as cur:
        candidate = _load_applied_version(cur, req.candidate_version)
        candidate_rules = json.loads(candidate["policy"])["rules"]
        stable = _stable_row(cur, req.risk_level)
        stable_version = stable["policy_version"] if stable is not None else None
        if req.candidate_version == stable_version:
            raise HTTPException(
                422, "candidate_version is already the stable version for "
                     f"risk_level={req.risk_level}")
        if _open_release(cur, req.risk_level) is not None:
            raise HTTPException(
                409, f"an unfinished release already exists for risk_level="
                     f"{req.risk_level}; pause/rollback/promote it first")
        # 闸门覆盖校验：候选策略必须有规则覆盖该风险等级且规模区间与闸门相交——
        # 否则被闸门引入候选车道的批次将无规则匹配（高风险 fail closed 直接拒绝提交）。
        covering = [r for r in candidate_rules
                    if r["risk_level"] in ("any", req.risk_level)
                    and _size_overlap(r["min_size"], r["max_size"],
                                      req.min_size, req.max_size)]
        if not covering:
            return JSONResponse(status_code=422, content={
                "error": "candidate_policy_does_not_cover_gate",
                "detail": ("candidate policy has no rule matching risk_level="
                           f"{req.risk_level} within the declared size gate "
                           f"[{req.min_size}, {req.max_size}]"),
                "risk_level": req.risk_level,
                "candidate_version": req.candidate_version,
                "min_size": req.min_size, "max_size": req.max_size})
        candidate_rule_name = covering[0]["name"]
        row = cur.execute(
            "SELECT COALESCE(MAX(rollout_seq),0) AS s FROM replay_policy_releases "
            "WHERE risk_level=?", (req.risk_level,)).fetchone()
        seq = row["s"] + 1
        try:
            cur.execute(
                """INSERT INTO replay_policy_releases
                   (risk_level, rollout_seq, candidate_version, candidate_rule_name,
                    stable_version_at_publish, min_size, max_size, rollout_percent,
                    status, operator, note, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,'candidate',?,?,?,?)""",
                (req.risk_level, seq, req.candidate_version, candidate_rule_name,
                 stable_version, req.min_size, req.max_size, req.rollout_percent,
                 operator, note or None, now, now))
        except sqlite3.IntegrityError:
            # 并发发布撞同等级「至多一条未结束」部分唯一索引
            raise HTTPException(409, "an unfinished release already exists for "
                                    f"risk_level={req.risk_level}")
        release_id = cur.lastrowid
        audit.record(cur, "replay_policy_release_published", None, None, {
            "release_id": release_id, "risk_level": req.risk_level,
            "rollout_seq": seq, "candidate_version": req.candidate_version,
            "candidate_rule_name": candidate_rule_name,
            "stable_version": stable_version,
            "min_size": req.min_size, "max_size": req.max_size,
            "rollout_percent": req.rollout_percent,
            "operator": operator, "note": note or None}, ts=now)
        body = {"result": "published", "release_id": release_id,
                "risk_level": req.risk_level, "rollout_seq": seq,
                "candidate_version": req.candidate_version,
                "candidate_rule_name": candidate_rule_name,
                "stable_version": stable_version,
                "min_size": req.min_size, "max_size": req.max_size,
                "rollout_percent": req.rollout_percent,
                "status": RELEASE_CANDIDATE, "published_at": now}
        return body


def _load_release_or_404(cur: sqlite3.Cursor, release_id: int) -> sqlite3.Row:
    row = cur.execute("SELECT * FROM replay_policy_releases WHERE id=?",
                      (release_id,)).fetchone()
    if row is None:
        raise HTTPException(404, "policy release not found")
    return row


def pause_release(db: Database, release_id: int, operator: str, reason: str,
                  now: float) -> dict:
    """暂停灰度：发布单 candidate -> paused，立即停止分流（之后新提交全走稳定版本）。

    已命中候选并生成审批节点的批次不受影响（它们持有候选版本快照）。
    """
    operator = _require(operator, "operator")
    reason = (reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason is required to pause a release")
    with db.tx() as cur:
        rel = _load_release_or_404(cur, release_id)
        if rel["status"] != RELEASE_CANDIDATE:
            raise HTTPException(
                409, f"release is {rel['status']}, only candidate releases can pause")
        cur.execute(
            """UPDATE replay_policy_releases SET status='paused', paused_at=?,
               paused_by=?, pause_reason=?, updated_at=? WHERE id=?""",
            (now, operator, reason, now, release_id))
        audit.record(cur, "replay_policy_release_paused", None, None, {
            "release_id": release_id, "risk_level": rel["risk_level"],
            "rollout_seq": rel["rollout_seq"],
            "candidate_version": rel["candidate_version"],
            "stable_version": rel["stable_version_at_publish"],
            "operator": operator, "reason": reason}, ts=now)
    return {"result": "paused", "release_id": release_id, "status": RELEASE_PAUSED,
            "paused_at": now, "reason": reason}


def resume_release(db: Database, release_id: int, operator: str, note: str,
                   now: float) -> dict:
    """恢复灰度：paused -> candidate，按原闸门/百分比继续分流（百分比未改，
    同一分流键暂停前后命中结果一致——暂停只是暂时不放行新批次）。"""
    operator = _require(operator, "operator")
    with db.tx() as cur:
        rel = _load_release_or_404(cur, release_id)
        if rel["status"] != RELEASE_PAUSED:
            raise HTTPException(
                409, f"release is {rel['status']}, only paused releases can resume")
        cur.execute(
            "UPDATE replay_policy_releases SET status='candidate', updated_at=? WHERE id=?",
            (now, release_id))
        audit.record(cur, "replay_policy_release_resumed", None, None, {
            "release_id": release_id, "risk_level": rel["risk_level"],
            "rollout_seq": rel["rollout_seq"],
            "candidate_version": rel["candidate_version"],
            "rollout_percent": rel["rollout_percent"],
            "operator": operator, "note": (note or "").strip() or None,
            "previous_pause_reason": rel["pause_reason"]}, ts=now)
    return {"result": "resumed", "release_id": release_id,
            "status": RELEASE_CANDIDATE, "resumed_at": now}


def _set_stable(cur: sqlite3.Cursor, risk_level: str, version: int | None,
                rule_name: str | None, operator: str, now: float,
                reason: str | None) -> sqlite3.Row:
    """切换某等级的稳定指针（prev 记录上一稳定版本，供回滚）。"""
    row = _stable_row(cur, risk_level)
    if row is None:
        cur.execute(
            """INSERT INTO replay_policy_stable
               (risk_level, policy_version, rule_name, prev_policy_version,
                updated_by, updated_at, reason)
               VALUES (?,?,?,NULL,?,?,?)""",
            (risk_level, version, rule_name, operator, now, reason))
    else:
        prev = row["prev_policy_version"]
        if row["policy_version"] != version:
            prev = row["policy_version"]
        cur.execute(
            """UPDATE replay_policy_stable SET policy_version=?, rule_name=?,
               prev_policy_version=?, updated_by=?, updated_at=?, reason=?
               WHERE risk_level=?""",
            (version, rule_name, prev, operator, now, reason, risk_level))
    return _stable_row(cur, risk_level)


def promote_release(db: Database, release_id: int, operator: str, note: str,
                    now: float, approval_timeout: float) -> dict:
    """候选转正：该等级稳定指针整份切到候选版本（prev 记原稳定版本）。

    转正后所有新提交都走候选版本；之后可用回滚把稳定指针恢复为原稳定版本。
    已提交批次仍持有各自快照，不受影响。candidate/paused 状态都允许转正。
    """
    operator = _require(operator, "operator")
    with db.tx() as cur:
        rel = _load_release_or_404(cur, release_id)
        if rel["status"] not in OPEN_STATUSES:
            raise HTTPException(
                409, f"release is {rel['status']}, only open releases can promote")
        stable = _stable_row(cur, rel["risk_level"])
        current_stable = stable["policy_version"] if stable is not None else None
        if current_stable != rel["stable_version_at_publish"]:
            # 发布期间该等级稳定指针被整份策略切换过（发布单理应已被 superseded）
            raise HTTPException(
                409, {"error": "stable_changed_since_publish",
                      "stable_at_publish": rel["stable_version_at_publish"],
                      "stable_now": current_stable})
        rules = load_version_rules(cur, rel["candidate_version"], approval_timeout)
        rule_name = _first_covering_rule_name(rules, rel["risk_level"])
        _set_stable(cur, rel["risk_level"], rel["candidate_version"], rule_name,
                    operator, now, f"promote rollout seq={rel['rollout_seq']}")
        cur.execute(
            """UPDATE replay_policy_releases SET status='promoted', promoted_at=?,
               promoted_by=?, updated_at=? WHERE id=?""",
            (now, operator, now, release_id))
        audit.record(cur, "replay_policy_release_promoted", None, None, {
            "release_id": release_id, "risk_level": rel["risk_level"],
            "rollout_seq": rel["rollout_seq"],
            "candidate_version": rel["candidate_version"],
            "from_stable_version": rel["stable_version_at_publish"],
            "operator": operator, "note": (note or "").strip() or None}, ts=now)
    return {"result": "promoted", "release_id": release_id,
            "risk_level": rel["risk_level"], "rollout_seq": rel["rollout_seq"],
            "stable_version": rel["candidate_version"],
            "previous_stable_version": rel["stable_version_at_publish"],
            "promoted_at": now}


def rollback_release(db: Database, release_id: int, operator: str, reason: str,
                     now: float, approval_timeout: float) -> dict:
    """回滚灰度发布到上一稳定版本（原因必填，只影响之后的新提交）。

    - 发布中（candidate/paused）：关闭发布单，停止分流；稳定指针本就指向上一稳定
      版本（校验未被整份切换改动），之后新提交全部走稳定版本；
    - 已转正（promoted）：把该等级稳定指针恢复为转正前的稳定版本（可能是内置默认，
      version=None），发布单标记 rolled_back。
    已经生成审批节点的批次持有自己的版本快照，永不改变。
    """
    operator = _require(operator, "operator")
    reason = (reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason is required to rollback a release")
    with db.tx() as cur:
        rel = _load_release_or_404(cur, release_id)
        if rel["status"] not in (RELEASE_CANDIDATE, RELEASE_PAUSED, RELEASE_PROMOTED):
            raise HTTPException(
                409, f"release is {rel['status']}, cannot rollback")
        stable = _stable_row(cur, rel["risk_level"])
        current_stable = stable["policy_version"] if stable is not None else None
        restored_to: int | None
        if rel["status"] == RELEASE_PROMOTED:
            if current_stable != rel["candidate_version"]:
                # 转正后稳定指针又被更新的转正/整份切换推进：本发布单不再代表当前稳定
                raise HTTPException(
                    409, {"error": "stable_has_moved_on",
                          "candidate_version": rel["candidate_version"],
                          "stable_now": current_stable})
            restored_to = stable["prev_policy_version"] if stable is not None else None
            rules = load_version_rules(cur, restored_to, approval_timeout)
            _set_stable(cur, rel["risk_level"], restored_to,
                        _first_covering_rule_name(rules, rel["risk_level"]),
                        operator, now,
                        f"rollback release seq={rel['rollout_seq']}: {reason}")
        else:
            if current_stable != rel["stable_version_at_publish"]:
                raise HTTPException(
                    409, {"error": "stable_changed_since_publish",
                          "stable_at_publish": rel["stable_version_at_publish"],
                          "stable_now": current_stable})
            restored_to = current_stable  # 灰度中：稳定版本从未改变
        cur.execute(
            """UPDATE replay_policy_releases SET status='rolled_back', rolled_back_at=?,
               rolled_back_by=?, rollback_reason=?, updated_at=? WHERE id=?""",
            (now, operator, reason, now, release_id))
        audit.record(cur, "replay_policy_release_rolled_back", None, None, {
            "release_id": release_id, "risk_level": rel["risk_level"],
            "rollout_seq": rel["rollout_seq"],
            "candidate_version": rel["candidate_version"],
            "restored_stable_version": restored_to,
            "previous_status": rel["status"],
            "operator": operator, "reason": reason}, ts=now)
    return {"result": "rolled_back", "release_id": release_id,
            "risk_level": rel["risk_level"], "rollout_seq": rel["rollout_seq"],
            "restored_stable_version": restored_to,
            "rolled_back_at": now, "reason": reason}


def apply_full_policy(cur: sqlite3.Cursor, version: int, rules: list[dict],
                      operator: str, now: float) -> None:
    """整份策略 applied 时（replay_policy.record_applied 同事务调用）：

    - 两个风险等级的稳定指针都指向新版本（保持灰度功能引入前的解析语义：版本是否
      覆盖某等级由匹配规则决定，无覆盖的等级仍 fail closed / 直跑）；
    - 这些等级上尚未结束的灰度发布单整份关闭为 superseded（候选版本不可能等于
      新版本号，故无歧义），写审计。
    """
    for level in TRACKED_LEVELS:
        _set_stable(cur, level, version, _first_covering_rule_name(rules, level),
                    operator, now, "full_policy_apply")
    open_rows = cur.execute(
        "SELECT * FROM replay_policy_releases WHERE status IN ('candidate','paused')"
    ).fetchall()
    for rel in open_rows:
        cur.execute(
            """UPDATE replay_policy_releases SET status='superseded', superseded_at=?,
               superseded_by=?, updated_at=? WHERE id=?""",
            (now, operator, now, rel["id"]))
        audit.record(cur, "replay_policy_release_superseded", None, None, {
            "release_id": rel["id"], "risk_level": rel["risk_level"],
            "rollout_seq": rel["rollout_seq"],
            "candidate_version": rel["candidate_version"],
            "superseded_by_version": version,
            "operator": operator}, ts=now)


# ---- 试运行校验 / 查询视图 -----------------------------------------------------

def _rule_preview(rules: list[dict], risk_level: str, total: int) -> dict | None:
    rule = match_rule(rules, risk_level, total)
    if rule is None:
        return None
    return {
        "rule_name": rule["name"], "mode": rule["mode"],
        "min_size": rule["min_size"], "max_size": rule["max_size"],
        "nodes": [{"seq": i, "role": n["role"],
                   "roles": n["roles"] if "roles" in n else [n["role"]],
                   "required_approvals": n.get("required_approvals", 1),
                   "timeout_seconds": n["timeout_seconds"]}
                  for i, n in enumerate(rule["nodes"])],
    }


def evaluate(db: Database, req: EvaluateRequest, approval_timeout: float) -> dict:
    """只读试运行：预演当前发布状态下给定批次会如何分流，以及稳定/候选版本各自
    会命中什么规则与节点链。不落任何数据、不写审计，供发布前核对。"""
    if req.risk_level not in TRACKED_LEVELS:
        raise HTTPException(422, f"invalid risk_level: {req.risk_level!r}")
    if isinstance(req.total, bool) or not isinstance(req.total, int) or req.total < 1:
        raise HTTPException(422, "total must be an integer >= 1")
    key = f"req:{req.request_id.strip()}" if req.request_id else \
        f"eval:{req.risk_level}:{req.total}"
    with db.tx() as cur:  # 读一致性：与一次发布切换串行，看到完整的前后状态之一
        route = resolve_route(cur, req.risk_level, req.total, key, approval_timeout)
        stable_rules = load_version_rules(cur, route["stable_version"],
                                          approval_timeout)
        stable_preview = _rule_preview(stable_rules, req.risk_level, req.total)
        rel = route["release"]
        candidate_preview = None
        if rel is not None:
            cand_rules = load_version_rules(cur, rel["candidate_version"],
                                            approval_timeout)
            candidate_preview = _rule_preview(cand_rules, req.risk_level, req.total)
    would_route = route["lane"] == LANE_CANDIDATE
    return {
        "risk_level": req.risk_level, "total": req.total, "routing_key": key,
        "bucket": route["bucket"],
        "stable_version": route["stable_version"],
        "release": rel,
        "would_hit_lane": route["lane"],
        "would_route_to_candidate": would_route,
        "would_use_version": route["policy_version"],
        "routing_reason": (
            "matched_candidate" if would_route else
            ("release_paused" if rel and rel["status"] == RELEASE_PAUSED else
             "size_outside_gate" if rel and not rel["size_in_gate"] else
             "bucket_outside_percent" if rel else
             "no_open_release")),
        "stable_rule": stable_preview,
        "candidate_rule": candidate_preview,
    }


def _release_hit_stats(cur: sqlite3.Cursor, release_id: int) -> dict:
    """该发布单的分流命中统计：命中候选车道的批次数与最近一次命中时间。"""
    row = cur.execute(
        """SELECT COUNT(*) AS hits, MAX(created_at) AS last_hit_at
           FROM replay_batches WHERE rollout_id=? AND policy_lane='candidate'""",
        (release_id,)).fetchone()
    total_row = cur.execute(
        "SELECT COUNT(*) AS c FROM replay_batches WHERE rollout_id=?",
        (release_id,)).fetchone()
    return {"candidate_hits": row["hits"],
            "recorded_batches": total_row["c"],
            "last_hit_at": row["last_hit_at"]}


def _release_view(cur: sqlite3.Cursor, rel: sqlite3.Row, with_hits: bool = True) -> dict:
    out = {k: rel[k] for k in rel.keys()}
    if with_hits:
        out["hits"] = _release_hit_stats(cur, rel["id"])
    return out


def list_releases(db: Database, risk_level: str | None, status_filter: str | None,
                  limit: int) -> dict:
    if risk_level is not None and risk_level not in TRACKED_LEVELS:
        raise HTTPException(422, f"invalid risk_level: {risk_level!r}")
    if status_filter is not None and status_filter not in RELEASE_STATUSES:
        raise HTTPException(422, f"invalid status: {status_filter!r} "
                                 f"(expect one of {','.join(RELEASE_STATUSES)})")
    sql, params = "SELECT * FROM replay_policy_releases WHERE 1=1", []
    if risk_level:
        sql += " AND risk_level=?"
        params.append(risk_level)
    if status_filter:
        sql += " AND status=?"
        params.append(status_filter)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    with db.tx() as cur:  # 统计与列表保持同一快照
        return {"releases": [_release_view(cur, r) for r in rows]}


def get_release(db: Database, release_id: int) -> dict:
    with db.tx() as cur:
        rel = _load_release_or_404(cur, release_id)
        return _release_view(cur, rel)


def current_rollout_view(cur: sqlite3.Cursor, approval_timeout: float) -> dict:
    """/replay-policies/current 的灰度视图：每等级稳定版本 + 未结束发布单（含命中）。"""
    levels = {}
    for level in TRACKED_LEVELS:
        stable = _stable_row(cur, level)
        open_rel = _open_release(cur, level)
        levels[level] = {
            "stable_version": stable["policy_version"] if stable is not None else None,
            "stable_source": ("applied" if stable is not None and
                              stable["policy_version"] is not None else "builtin_default"),
            "stable_rule_name": stable["rule_name"] if stable is not None else None,
            "previous_stable_version": (stable["prev_policy_version"]
                                        if stable is not None else None),
            "updated_by": stable["updated_by"] if stable is not None else None,
            "updated_at": stable["updated_at"] if stable is not None else None,
            "stable_reason": stable["reason"] if stable is not None else None,
            "open_release": (_release_view(cur, open_rel)
                             if open_rel is not None else None),
        }
    return {"risk_levels": levels}


def record_batch_route_audit(cur: sqlite3.Cursor, batch_id: int, risk_level: str,
                             route: dict, rule_name: str | None, now: float) -> None:
    """批次分流命中审计：与批次/节点落盘在同一事务，发布与批次的轨迹可互相核对。"""
    rel = route["release"]
    audit.record(cur, "replay_policy_batch_routed", None, None, {
        "replay_batch_id": batch_id,
        "risk_level": risk_level,
        "lane": route["lane"], "policy_version": route["policy_version"],
        "stable_version": route["stable_version"],
        "release_id": route["release_id"], "rollout_seq": route["rollout_seq"],
        "candidate_version": rel["candidate_version"] if rel else None,
        "release_status": rel["status"] if rel else None,
        "routing_key": route["routing_key"], "bucket": route["bucket"],
        "min_size": rel["min_size"] if rel else None,
        "max_size": rel["max_size"] if rel else None,
        "rollout_percent": rel["rollout_percent"] if rel else None,
        "size_in_gate": rel["size_in_gate"] if rel else None,
        "rule_name": rule_name}, ts=now)


def create_rollout_router(db: Database, approval_timeout: float) -> APIRouter:
    router = APIRouter(prefix="/admin/replay-policies", tags=["replay-policy-rollout"])

    @router.post("/releases")
    def publish_endpoint(req: ReleasePublishRequest):
        """为风险等级发布候选策略灰度（规模闸门 + 分流百分比）；稳定版本不变。"""
        return publish_release(db, req, time.time())

    @router.get("/releases")
    def list_endpoints(risk_level: str | None = None, status: str | None = None,
                       limit: int = Query(100, le=1000)):
        """灰度发布单列表（可按风险等级/状态过滤），含每单的分流命中与暂停/回滚原因。"""
        return list_releases(db, risk_level, status, limit)

    @router.get("/releases/{release_id}")
    def get_endpoint(release_id: int):
        """单条灰度发布单详情：闸门、百分比、状态、暂停/回滚原因、分流命中统计。"""
        return get_release(db, release_id)

    @router.post("/releases/{release_id}/pause")
    def pause_endpoint(release_id: int, req: ReleaseOperatorRequest):
        """暂停灰度（原因必填）：停止向候选分流，之后新提交全走稳定版本。"""
        return pause_release(db, release_id, req.operator, req.reason, time.time())

    @router.post("/releases/{release_id}/resume")
    def resume_endpoint(release_id: int, req: ReleaseOperatorRequest):
        """恢复已暂停的灰度：按原闸门/百分比继续分流。"""
        return resume_release(db, release_id, req.operator, req.note or req.reason,
                              time.time())

    @router.post("/releases/{release_id}/promote")
    def promote_endpoint(release_id: int, req: ReleaseOperatorRequest):
        """候选转正：该风险等级的稳定版本整份切到候选版本（记录上一稳定版本）。"""
        return promote_release(db, release_id, req.operator, req.note, time.time(),
                               approval_timeout)

    @router.post("/releases/{release_id}/rollback")
    def rollback_endpoint(release_id: int, req: ReleaseOperatorRequest):
        """回滚到上一稳定版本（原因必填）：只影响之后的新提交，已生成节点的批次不变。"""
        return rollback_release(db, release_id, req.operator, req.reason, time.time(),
                                approval_timeout)

    @router.post("/evaluate")
    def evaluate_endpoint(req: EvaluateRequest):
        """试运行校验：只读预演分流（车道/桶/命中版本与规则节点链），不落数据。"""
        return evaluate(db, req, approval_timeout)

    return router

"""接收人通知额度与抑制窗口（recipient notification quota & suppression window）。

在既有通知路由（``notif_send_tasks``）、发送任务领取与外部回执确认链路之上提供
**按接收人 × 事件级别 × 时间窗口的可配置发送额度**，以及超额后的三种处置：
延迟发送（``delay``）、降级为站内通知（``downgrade``）、转人工（``manual``）。

配置（``notif_quota_versions`` / ``notif_quota_current``）
- POST /quota/versions 整份提交：按接收人（NULL/'*'=全体）与事件级别
  （info|normal|critical，'*' 或缺省规则匹配任意级别）给出窗口秒数、额度、单事件
  占用 cost 与超额动作；校验通过整份生效（版本号单调递增），失败保留当前版本；
- POST /quota/rollback 回滚到上一生效版本（原因必填），只影响之后入队的任务；
- 发送任务在**入队时**固化命中规则快照（``notif_send_tasks.quota_snapshot`` 与
  quota_version/rule_id/level），之后发布/回滚都不改变它——已入队任务始终按入队时
  的规则占用额度。

事件级别
- 内置映射：纯告知结果类（approved/rejected/...）= info；投票动态/超时/撤回/变更
  结果/升级 = normal；可操作的激活、待审批、临期提醒 = critical；
- 配置里可给 event_levels 覆盖任意事件类型的级别。

预占与生命周期（``notif_quota_reservations``）
- 发送任务领取（pending/failed -> in_flight）**前**在同一写事务里原子预占：桶
  （规则版本×规则×接收人×窗口起点）内活跃预占成本之和 + 本事件 cost 不超过额度才
  领取成功；重复扫描、失败重试、服务重启恢复都复用同一代预占，不重复占用；
- 固定窗口（自锚点对齐）：窗口到期后旧桶行不再被任何查询计入消耗，等同释放可用
  额度，无需后台清理；延迟到窗口之后的任务在新窗口里自然取得预占；
- 同一接收人多个事件并发时，worker 按 (ordinal,id) 顺序派发，且低序号同桶任务尚在
  等待时高序号任务不允许「插队」预占（稳定占用顺序）；
- 回执失败/超时后的通道切换（receipts 故障转移、manual retry 之外的自动重试）沿用
  任务自己的预占（generation 不变），不能借切换通道绕过同一事件的额度限制；
- 发送成功预占转 consumed；取消任务、确认不会再发送（人工忽略）时未使用预占回收
  （reserved -> released）；服务重启后 in_flight 任务退回 pending，其预占保留可复用，
  孤儿预占（任务已终结）在启动恢复时对账回收；
- 人工 requeue / 处置 awaiting_manual 时选择重试：generation+1 并回收旧预占，按
  当前窗口重新预占（这是一次新的发送意图，而失败退避重试与回执自动通道切换不是）。

超额处置（在领取事务内决策，绝不产生外部效果）
- delay：任务留 pending，排到本窗口结束后重试（quota_status=delayed）；
- downgrade：把本轮工作计划改为仅 inbox（站内待办本就存在），记录 downgraded 预占
  （不计入桶消耗），inbox 成功后任务 sent；
- manual：任务转 awaiting_manual，等待管理员处置（retry 重新预占 / ignore 回收）。
"""
from __future__ import annotations

import json
import logging
import sqlite3
import time

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from . import audit
from . import notifications as notif
from . import notif_routing
from .config import Settings
from .db import Database

log = logging.getLogger("gateway.notifications.quota")

# ---- 常量 -------------------------------------------------------------------

LEVEL_INFO = "info"
LEVEL_NORMAL = "normal"
LEVEL_CRITICAL = "critical"
LEVELS = (LEVEL_INFO, LEVEL_NORMAL, LEVEL_CRITICAL)
RULE_MATCH_ALL = "*"

ACTION_DELAY = "delay"
ACTION_DOWNGRADE = "downgrade"
ACTION_MANUAL = "manual"
ON_EXCEEDED_ACTIONS = (ACTION_DELAY, ACTION_DOWNGRADE, ACTION_MANUAL)

# 发送任务额度处置状态（notif_send_tasks.quota_status）
QS_NONE = "none"
QS_ADMITTED = "admitted"
QS_DELAYED = "delayed"
QS_DOWNGRADED = "downgraded"
QS_MANUAL = "manual"
QS_IGNORED = "ignored"
QS_CONSUMED = "consumed"
QS_RELEASED = "released"

# 预占状态
RS_RESERVED = "reserved"
RS_CONSUMED = "consumed"
RS_RELEASED = "released"
KIND_NORMAL = "normal"
KIND_DOWNGRADED = "downgraded"

# 转人工任务状态复用 notif_routing/receipts 的 awaiting_manual
TASK_AWAITING_MANUAL = "awaiting_manual"

# 事件类型 -> 级别的内置映射（配置 event_levels 可覆盖）
DEFAULT_LEVEL_MAP = {
    # 纯告知结果：低
    "batch_approved": LEVEL_INFO,
    "node_rejected": LEVEL_INFO,
    "node_timeout": LEVEL_INFO,
    "batch_cancelled": LEVEL_INFO,
    "change_approved": LEVEL_INFO,
    "change_rejected": LEVEL_INFO,
    "change_expired": LEVEL_INFO,
    "change_applied": LEVEL_INFO,
    # 动态/过程：中
    "vote_received": LEVEL_NORMAL,
    "change_required": LEVEL_NORMAL,
    "escalated": LEVEL_NORMAL,
    # 需要人动手的待办：高
    "activated": LEVEL_CRITICAL,
    "deadline_approaching": LEVEL_CRITICAL,
}

# 转人工 / 忽略原因前缀（写任务 quota_reason 与审计）
REASON_QUOTA_EXCEEDED = "quota_exceeded"
REASON_QUOTA_MANUAL = "quota_manual"


# ---- 请求模型 ----------------------------------------------------------------

class QuotaRuleSpec(BaseModel):
    id: str | None = None                    # 规则标识（版本内唯一；缺省规则用 '*'）
    recipient: str | None = None             # NULL/空/'*'=全体接收人
    level: str | None = None                 # info|normal|critical；NULL/'*'=任意级别
    window_seconds: float
    limit: int                               # 窗口内可占用总额度（>=0）
    cost: int = 1                            # 单个事件占用额度（>=1；0 级规则请配大额度）
    on_exceeded: str = ACTION_DELAY          # delay|downgrade|manual
    active: bool = True


class QuotaConfigRequest(BaseModel):
    operator: str
    note: str = ""
    rules: list[QuotaRuleSpec]
    event_levels: dict[str, str] | None = None  # 事件类型 -> 级别（覆盖内置映射）


class RollbackRequest(BaseModel):
    operator: str
    reason: str


class QuotaResolveRequest(BaseModel):
    operator: str
    action: str                              # retry | ignore
    note: str | None = None


# ============================================================================
# 配置校验 / 发布 / 回滚
# ============================================================================

def _validate_config(config: dict) -> tuple[list[dict], dict]:
    """校验并规范化额度配置，返回 (rules, event_levels)。失败抛 HTTPException(422)。"""
    if not isinstance(config, dict) or not isinstance(config.get("rules"), list):
        raise HTTPException(422, "config must be an object with a non-empty 'rules' list")
    if not config["rules"]:
        raise HTTPException(422, "rules must be a non-empty list")
    norm_rules: list[dict] = []
    seen_ids: set[str] = set()
    has_default = False
    for i, rule in enumerate(config["rules"]):
        if not isinstance(rule, dict):
            raise HTTPException(422, f"rule {i}: must be an object")
        rid = rule.get("id")
        level = rule.get("level")
        if level in ("", RULE_MATCH_ALL):
            level = None
        # 无 id：级别通配规则即缺省规则 '*'；否则要求显式 id
        if rid in (None, "", RULE_MATCH_ALL):
            if level is not None:
                raise HTTPException(
                    422, f"rule {i}: an id is required for level-specific rules "
                         "(only the default level='*' rule may use id '*')")
            rid = RULE_MATCH_ALL
            has_default = True
        else:
            rid = str(rid)
        if rid in seen_ids:
            raise HTTPException(422, f"duplicate rule id {rid!r}")
        seen_ids.add(rid)
        if level is not None and level not in LEVELS:
            raise HTTPException(422, f"rule {rid!r}: level must be one of "
                                     f"{','.join(LEVELS)} (or '*' for default)")
        recipient = rule.get("recipient")
        if recipient in ("", RULE_MATCH_ALL):
            recipient = None
        elif recipient is not None:
            recipient = str(recipient)
        try:
            window = float(rule.get("window_seconds"))
        except (TypeError, ValueError):
            raise HTTPException(422, f"rule {rid!r}: window_seconds must be a number > 0")
        if window <= 0:
            raise HTTPException(422, f"rule {rid!r}: window_seconds must be > 0")
        try:
            limit = int(rule.get("limit"))
        except (TypeError, ValueError):
            raise HTTPException(422, f"rule {rid!r}: limit must be an integer >= 0")
        if limit < 0:
            raise HTTPException(422, f"rule {rid!r}: limit must be >= 0")
        cost = int(rule.get("cost", 1))
        if cost < 1:
            raise HTTPException(422, f"rule {rid!r}: cost must be >= 1")
        on_exceeded = str(rule.get("on_exceeded", ACTION_DELAY))
        if on_exceeded not in ON_EXCEEDED_ACTIONS:
            raise HTTPException(422, f"rule {rid!r}: on_exceeded must be one of "
                                     f"{','.join(ON_EXCEEDED_ACTIONS)}")
        norm_rules.append({"id": rid, "recipient": recipient, "level": level,
                           "window_seconds": window, "limit": limit, "cost": cost,
                           "on_exceeded": on_exceeded,
                           "active": bool(rule.get("active", True))})
    if not has_default:
        raise HTTPException(422, "a default rule (level='*', id='*') is required")
    # 事件级别覆盖
    norm_levels: dict = {}
    for etype, lvl in (config.get("event_levels") or {}).items():
        if lvl not in LEVELS:
            raise HTTPException(422, f"event_levels.{etype}: level must be one of "
                                     f"{','.join(LEVELS)}")
        norm_levels[str(etype)] = lvl
    return norm_rules, norm_levels


def publish_quota(db: Database, req: QuotaConfigRequest) -> dict:
    """整份发布额度配置：校验通过 -> 新版本落盘 + 推进当前指针（同一事务）。

    被拒绝的提交也落一条 rejected 版本记录（含原因），当前版本保持不变。
    """
    operator = notif._require(req.operator, "operator")
    raw = {"rules": [r.model_dump() for r in req.rules],
           "event_levels": req.event_levels}
    now = time.time()
    try:
        norm_rules, norm_levels = _validate_config(raw)
    except HTTPException as exc:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO notif_quota_versions
                   (version, result, config_json, operator, reason, created_at)
                   VALUES (NULL,'rejected',?,?,?,?)""",
                (json.dumps(raw, ensure_ascii=False), operator,
                 json.dumps(exc.detail, ensure_ascii=False), now))
            audit.record(cur, "notif_quota_rejected", None, None,
                         {"operator": operator, "reason": exc.detail}, ts=now)
        raise
    config = {"rules": norm_rules, "event_levels": norm_levels}
    with db.tx() as cur:
        row = cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 AS v FROM notif_quota_versions "
            "WHERE result IN ('applied','rollback')").fetchone()
        version = int(row["v"])
        cur.execute(
            """INSERT INTO notif_quota_versions
               (version, result, config_json, operator, reason, created_at)
               VALUES (?, 'applied', ?, ?, NULL, ?)""",
            (version, json.dumps(config, ensure_ascii=False, sort_keys=True),
             operator, now))
        prev = cur.execute(
            "SELECT quota_version FROM notif_quota_current WHERE id=1").fetchone()
        prev_version = prev["quota_version"] if prev is not None else None
        cur.execute(
            """UPDATE notif_quota_current SET quota_version=?, updated_by=?,
               updated_at=?, reason=? WHERE id=1""",
            (version, operator, now, req.note or None))
        audit.record(cur, "notif_quota_published", None, None, {
            "version": version, "operator": operator, "note": req.note,
            "prev_version": prev_version,
            "rules": [{"id": r["id"], "recipient": r["recipient"],
                       "level": r["level"] or "*", "limit": r["limit"],
                       "window_seconds": r["window_seconds"],
                       "on_exceeded": r["on_exceeded"]} for r in norm_rules]}, ts=now)
    return {"result": "applied", "version": version}


def rollback_quota(db: Database, req: RollbackRequest) -> dict:
    """回滚到上一生效版本（原因必填）。只推进指针，只影响之后入队的任务。"""
    operator = notif._require(req.operator, "operator")
    reason = (req.reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason must be non-empty for rollback")
    now = time.time()
    with db.tx() as cur:
        current = cur.execute(
            "SELECT quota_version FROM notif_quota_current WHERE id=1").fetchone()
        cur_version = current["quota_version"] if current else None
        if cur_version is None:
            raise HTTPException(409, "no quota version is currently applied")
        prev = cur.execute(
            """SELECT version FROM notif_quota_versions
               WHERE result IN ('applied','rollback') AND version < ?
               ORDER BY id DESC LIMIT 1""", (cur_version,)).fetchone()
        if prev is None or prev["version"] is None:
            raise HTTPException(409, "no previous quota version to roll back to")
        target = int(prev["version"])
        target_row = cur.execute(
            "SELECT config_json FROM notif_quota_versions WHERE version=? "
            "ORDER BY id DESC LIMIT 1", (target,)).fetchone()
        cur.execute(
            """INSERT INTO notif_quota_versions
               (version, result, config_json, operator, reason, created_at)
               VALUES (?, 'rollback', ?, ?, ?, ?)""",
            (target, target_row["config_json"], operator, reason, now))
        cur.execute(
            """UPDATE notif_quota_current SET quota_version=?, updated_by=?,
               updated_at=?, reason=? WHERE id=1""",
            (target, operator, now, reason))
        audit.record(cur, "notif_quota_rolled_back", None, None, {
            "from_version": cur_version, "version": target,
            "operator": operator, "reason": reason}, ts=now)
    return {"result": "rolled_back", "version": target}


# ============================================================================
# 规则解析 / 级别
# ============================================================================

def current_version(db: Database) -> int | None:
    row = db.query_one("SELECT quota_version FROM notif_quota_current WHERE id=1")
    return row["quota_version"] if row is not None else None


def _load_version(cur: sqlite3.Cursor, version: int) -> dict:
    row = cur.execute(
        "SELECT config_json FROM notif_quota_versions WHERE version=? "
        "AND result IN ('applied','rollback') ORDER BY id DESC LIMIT 1",
        (version,)).fetchone()
    return json.loads(row["config_json"]) if row else {"rules": [], "event_levels": {}}


def resolve_level(event_type: str, event_levels: dict | None = None) -> str:
    """事件类型 -> 级别：配置覆盖优先，否则内置映射，未列出的按 normal。"""
    overrides = event_levels or {}
    if event_type in overrides:
        return overrides[event_type]
    return DEFAULT_LEVEL_MAP.get(event_type, LEVEL_NORMAL)


def _rule_matches(rule: dict, *, recipient: str, level: str) -> bool:
    if rule["recipient"] is not None and rule["recipient"] != recipient:
        return False
    if rule["level"] is not None and rule["level"] != level:
        return False
    return True


def resolve_rule(config: dict, *, recipient: str, level: str) -> dict | None:
    """按规则顺序解析：第一条 active 且接收人/级别匹配的规则赢；无匹配返回 None。"""
    for rule in config.get("rules") or []:
        if not rule.get("active", True):
            continue
        if _rule_matches(rule, recipient=recipient, level=level):
            return rule
    return None


def snapshot_for_enqueue(cur: sqlite3.Cursor, *, recipient: str,
                         event_type: str) -> dict | None:
    """入队侧：解析当前生效配置下该事件的额度规则快照。

    返回 None 表示未发布额度配置或无匹配规则（该任务领取时不受额度闸门约束）。
    """
    pointer = cur.execute(
        "SELECT quota_version FROM notif_quota_current WHERE id=1").fetchone()
    version = pointer["quota_version"] if pointer is not None else None
    if version is None:
        return None
    config = _load_version(cur, version)
    level = resolve_level(event_type, config.get("event_levels"))
    rule = resolve_rule(config, recipient=recipient, level=level)
    if rule is None:
        return None
    return {"quota_version": version, "rule_id": rule["id"], "level": level,
            "snapshot": {"window_seconds": rule["window_seconds"],
                         "limit": rule["limit"], "cost": rule["cost"],
                         "on_exceeded": rule["on_exceeded"],
                         "rule_id": rule["id"], "level": level,
                         "quota_version": version}}


# ============================================================================
# 预占（领取事务内，原子）
# ============================================================================

def bucket_start(now: float, window_seconds: float, anchor: float = 0.0) -> float:
    """固定窗口起点：自 Unix 纪元对齐（window 整除 epoch），与配置版本无关，
    延迟到窗口结束后自然落入下一窗口。"""
    return anchor + ((now - anchor) // window_seconds) * window_seconds


def _bucket_used(cur: sqlite3.Cursor, *, version: int, rule_id: str,
                 recipient: str, bstart: float) -> int:
    """桶内当前消耗：当前窗口里活跃预占（reserved，含在途/退避/延迟到窗口内的任务）
    与已消耗（consumed，发送成功的占用）的 normal 成本之和。

    只统计同一窗口起点的行——窗口到期后任务落入新桶，旧桶行不再计入，等同额度随窗口
    到期释放。downgraded 预占不占外发额度；released（取消/忽略）不占额度。
    """
    row = cur.execute(
        """SELECT COALESCE(SUM(cost),0) AS used FROM notif_quota_reservations
           WHERE quota_version=? AND rule_id=? AND recipient=? AND bucket_start=?
             AND state IN ('reserved','consumed') AND kind='normal'""",
        (version, rule_id, recipient, bstart)).fetchone()
    return int(row["used"])


def _insert_reservation(cur, *, task, version: int, rule_id: str, level: str,
                        bstart: float, window: float, cost: int, kind: str,
                        generation: int, reason: str | None, now: float) -> int:
    cur.execute(
        """INSERT INTO notif_quota_reservations
           (task_id, event_id, recipient, quota_version, rule_id, level,
            bucket_start, window_seconds, cost, kind, state, generation, reason,
            created_at, updated_at)
           VALUES (?,?,?,?,?,?,?,?,?,?, 'reserved', ?, ?, ?, ?)""",
        (task["id"], task["event_id"], task["recipient"], version, rule_id, level,
         bstart, window, cost, kind, generation, reason, now, now))
    return cur.lastrowid


def admit_or_defer_tx(cur: sqlite3.Cursor, task: sqlite3.Row,
                      now: float) -> dict:
    """领取事务内的额度闸门（任务已被条件 UPDATE 置 in_flight 后调用，同一事务）。

    返回处置 dict：
    - {"decision":"admit"}：预占成功，调用方继续正常通道派发；
    - {"decision":"delay","retry_at":...}：超额且动作 delay，任务回 pending 排到窗口后；
    - {"decision":"downgrade"}：超额且动作 downgrade，工作计划改为仅 inbox；
    - {"decision":"manual"}：超额且动作 manual，任务转 awaiting_manual；
    - {"decision":"none"}：任务无额度规则（发布前入队/无匹配），不受约束。

    幂等：该任务当前代已有活跃预占时直接 admit（重复扫描/失败重试/重启恢复复用）。
    """
    version = task["quota_version"]
    rule_id = task["quota_rule_id"]
    generation = int(task["quota_generation"] or 1)
    if version is None or not rule_id or not task["quota_snapshot"]:
        return {"decision": "none"}
    snap = json.loads(task["quota_snapshot"])
    window = float(snap["window_seconds"])
    limit = int(snap["limit"])
    cost = int(snap["cost"])
    level = task["quota_level"] or snap.get("level") or LEVEL_NORMAL
    bstart = bucket_start(now, window)

    # 该任务此前已有过预占（任意代/任意窗口，含 consumed）：这是同一事件的失败退避重试、
    # 回执驱动的通道切换或恢复后的再领取——直接放行，绝不重新过闸门、不二次占用，也不
    # 能借通道切换绕过同一事件的额度限制（它本来就占着这一事件的额度）。新代预占只由
    # 人工 requeue/重试（bump_generation_tx 同时回收旧预占）产生，而那种路径旧预占已
    # released，查不到历史活跃/消耗行，才会走到下面的全新预占。
    prior = cur.execute(
        """SELECT * FROM notif_quota_reservations
           WHERE task_id=? AND state IN ('reserved','consumed')
           ORDER BY id DESC LIMIT 1""",
        (task["id"],)).fetchone()
    if prior is not None and prior["generation"] == generation:
        if prior["state"] == RS_RESERVED and prior["bucket_start"] != bstart:
            cur.execute(
                """UPDATE notif_quota_reservations SET bucket_start=?, updated_at=?
                   WHERE id=?""", (bstart, now, prior["id"]))
        if prior["kind"] == KIND_DOWNGRADED:
            return {"decision": "downgrade", "reservation_id": prior["id"],
                    "reused": True}
        return {"decision": "admit", "reservation_id": prior["id"], "reused": True}

    # 稳定占用顺序：同一接收人 × 同一规则、更低 ordinal 且从未预占过的同窗口等待任务
    # （pending/failed）仍在前面时，本任务不允许插队，随同延迟到它之后再判。
    # 能走到这里说明本任务自己没有任何 reserved/consumed 预占行。
    blocker = cur.execute(
        """SELECT t.id, t.next_retry_at FROM notif_send_tasks t
           WHERE t.id<>? AND t.recipient=? AND t.quota_version=?
             AND t.quota_rule_id=? AND t.ordinal < ?
             AND t.status IN ('pending','failed')
             AND NOT EXISTS (
                 SELECT 1 FROM notif_quota_reservations r
                 WHERE r.task_id=t.id
                   AND r.state IN ('reserved','consumed'))
           ORDER BY t.ordinal, t.id LIMIT 1""",
        (task["id"], task["recipient"], version, rule_id,
         task["ordinal"])).fetchone()
    if blocker is not None:
        retry_at = blocker["next_retry_at"] or (now + 1.0)
        if retry_at <= now:
            retry_at = now + 1.0
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', next_retry_at=?,
               quota_status=?, quota_reason='ordered_behind_lower_ordinal',
               updated_at=? WHERE id=?""",
            (retry_at, QS_DELAYED, now, task["id"]))
        audit.record(cur, "notif_quota_order_wait", None, None, {
            "send_task_id": task["id"], "todo_id": task["todo_id"],
            "notify_event_id": task["event_id"], "recipient": task["recipient"],
            "quota_version": version, "rule_id": rule_id, "level": level,
            "blocker_task_id": blocker["id"], "next_retry_at": retry_at}, ts=now)
        return {"decision": "delay", "retry_at": retry_at, "ordered": True}

    used = _bucket_used(cur, version=version, rule_id=rule_id,
                        recipient=task["recipient"], bstart=bstart)
    if used + cost > limit:
        return _apply_exceeded(cur, task=task, snap=snap, version=version,
                               rule_id=rule_id, level=level, now=now,
                               generation=generation,
                               used=used, window=window, bstart=bstart)

    rid = _insert_reservation(cur, task=task, version=version, rule_id=rule_id,
                              level=level, bstart=bstart, window=window, cost=cost,
                              kind=KIND_NORMAL, generation=generation,
                              reason=None, now=now)
    cur.execute(
        "UPDATE notif_send_tasks SET quota_status=?, quota_reason=NULL WHERE id=?",
        (QS_ADMITTED, task["id"]))
    audit.record(cur, "notif_quota_reserved", None, None, {
        "send_task_id": task["id"], "todo_id": task["todo_id"],
        "notify_event_id": task["event_id"], "recipient": task["recipient"],
        "quota_version": version, "rule_id": rule_id, "level": level,
        "reservation_id": rid, "bucket_start": bstart,
        "window_seconds": window, "cost": cost, "used_after": used + cost,
        "limit": limit, "generation": generation}, ts=now)
    return {"decision": "admit", "reservation_id": rid, "used": used + cost}


def _apply_exceeded(cur, *, task, snap: dict, version: int, rule_id: str,
                    level: str, now: float, generation: int,
                    used: int, window: float, bstart: float) -> dict:
    """桶内额度不足：按入队时快照里的 on_exceeded 处置（delay/downgrade/manual）。"""
    action = snap.get("on_exceeded", ACTION_DELAY)
    base_detail = {
        "send_task_id": task["id"], "todo_id": task["todo_id"],
        "notify_event_id": task["event_id"], "recipient": task["recipient"],
        "quota_version": version, "rule_id": rule_id, "level": level,
        "bucket_start": bstart, "window_seconds": window,
        "used": used, "limit": snap["limit"], "cost": snap["cost"],
        "on_exceeded": action, "generation": generation}
    if action == ACTION_DELAY:
        retry_at = bstart + window
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', next_retry_at=?,
               quota_status=?, quota_reason=?, updated_at=? WHERE id=?""",
            (retry_at, QS_DELAYED, REASON_QUOTA_EXCEEDED, now, task["id"]))
        audit.record(cur, "notif_quota_delayed", None, None,
                     {**base_detail, "retry_at": retry_at,
                      "window_ends_at": retry_at}, ts=now)
        return {"decision": "delay", "retry_at": retry_at}

    if action == ACTION_DOWNGRADE:
        # 工作计划改为仅 inbox（站内待办始终存在），记录不占桶消耗的降级预占
        cur.execute(
            """UPDATE notif_send_tasks SET plan_json=?, attempt_index=0,
               current_channel=NULL, quota_status=?, quota_reason=?,
               updated_at=? WHERE id=?""",
            (json.dumps([{"channel": notif_routing.CHANNEL_INBOX, "address": None,
                          "timeout_seconds": None, "max_attempts": 1,
                          "condition": None, "downgraded_by_quota": True}],
                        ensure_ascii=False),
             QS_DOWNGRADED, REASON_QUOTA_EXCEEDED, now, task["id"]))
        rid = _insert_reservation(
            cur, task=task, version=version, rule_id=rule_id, level=level,
            bstart=bstart, window=window, cost=int(snap["cost"]),
            kind=KIND_DOWNGRADED, generation=generation,
            reason=REASON_QUOTA_EXCEEDED, now=now)
        cur.execute(
            """INSERT INTO notif_channel_switches
               (task_id,event_id,recipient,from_channel,to_channel,reason,
                detail,created_at)
               VALUES (?,?,?,?,?,?,?,?)""",
            (task["id"], task["event_id"], task["recipient"], None,
             notif_routing.CHANNEL_INBOX, "quota_downgraded",
             json.dumps(base_detail, ensure_ascii=False, sort_keys=True), now))
        audit.record(cur, "notif_quota_downgraded", None, None,
                     {**base_detail, "reservation_id": rid}, ts=now)
        return {"decision": "downgrade", "reservation_id": rid}

    # manual：转人工等待处置（retry 重新预占 / ignore 回收）
    cur.execute(
        """UPDATE notif_send_tasks SET status=?, next_retry_at=NULL,
           quota_status=?, quota_reason=?, updated_at=? WHERE id=?""",
        (TASK_AWAITING_MANUAL, QS_MANUAL, REASON_QUOTA_MANUAL, now, task["id"]))
    audit.record(cur, "notif_quota_awaiting_manual", None, None,
                 base_detail, ts=now)
    return {"decision": "manual"}


# ============================================================================
# 预占生命周期：成功 / 回收 / 代际更替 / 重启对账
# ============================================================================

def mark_consumed_tx(cur: sqlite3.Cursor, task, now: float) -> None:
    """发送成功：当前代活跃预占转 consumed（额度真正被用掉，窗口内不释放）。

    任务没有额度规则（发布前入队/无匹配）时什么都不做，quota_status 保持 none。"""
    if task["quota_version"] is None or not task["quota_rule_id"]:
        return
    gen = int(task["quota_generation"] or 1)
    row = cur.execute(
        """UPDATE notif_quota_reservations SET state='consumed', updated_at=?
           WHERE task_id=? AND generation=? AND state='reserved'""",
        (now, task["id"], gen)).rowcount
    if row:
        cur.execute(
            "UPDATE notif_send_tasks SET quota_status=? WHERE id=?",
            (QS_CONSUMED, task["id"]))
        r = cur.execute(
            "SELECT * FROM notif_quota_reservations WHERE task_id=? "
            "AND generation=? ORDER BY id DESC LIMIT 1",
            (task["id"], gen)).fetchone()
        audit.record(cur, "notif_quota_consumed", None, None, {
            "send_task_id": task["id"], "recipient": task["recipient"],
            "quota_version": r["quota_version"], "rule_id": r["rule_id"],
            "level": r["level"], "kind": r["kind"], "cost": r["cost"],
            "bucket_start": r["bucket_start"], "generation": gen}, ts=now)


def release_for_task_tx(cur: sqlite3.Cursor, task, reason: str, now: float,
                        *, generation: int | None = None) -> int:
    """回收任务未使用的当前代预占（取消任务 / 人工忽略 / 确认不再发送）。

    已 consumed（发送成功）的预占不回收；返回回收行数。"""
    gen = int(task["quota_generation"] or 1) if generation is None else generation
    rows = cur.execute(
        """SELECT id FROM notif_quota_reservations
           WHERE task_id=? AND generation=? AND state='reserved'""",
        (task["id"], gen)).fetchall()
    for r in rows:
        cur.execute(
            "UPDATE notif_quota_reservations SET state='released', reason=?, "
            "updated_at=? WHERE id=?", (reason, now, r["id"]))
    if rows:
        cur.execute(
            "UPDATE notif_send_tasks SET quota_status=? WHERE id=?",
            (QS_RELEASED, task["id"]))
        audit.record(cur, "notif_quota_released", None, None, {
            "send_task_id": task["id"], "recipient": task["recipient"],
            "quota_version": task["quota_version"],
            "rule_id": task["quota_rule_id"], "reason": reason,
            "released": len(rows), "generation": gen}, ts=now)
    return len(rows)


def bump_generation_tx(cur: sqlite3.Cursor, task, reason: str, now: float) -> int:
    """人工发起新发送轮次（requeue / manual retry）：回收旧代预占并把代际 +1。

    下一轮领取时按当时窗口重新预占。失败退避重试与回执自动通道切换不走这里——
    它们 generation 不变、复用同一预占。"""
    release_for_task_tx(cur, task, reason, now)
    new_gen = int(task["quota_generation"] or 1) + 1
    cur.execute(
        """UPDATE notif_send_tasks SET quota_generation=?, quota_status='none',
           quota_reason=NULL WHERE id=?""", (new_gen, task["id"]))
    return new_gen


def reconcile_on_recover(db: Database, now: float | None = None) -> dict:
    """启动恢复对账：in_flight 任务已由 notif_routing.recover 退回 pending，其活跃
    预占保留（下轮领取复用，不重复占用）；若任务已终结（防御性，正常不会出现）则
    回收其孤儿预占。返回统计。"""
    now = time.time() if now is None else now
    with db.tx() as cur:
        return reconcile_on_recover_tx(cur, now)


def reconcile_on_recover_tx(cur: sqlite3.Cursor, now: float) -> dict:
    """reconcile_on_recover 的事务内版本（供 recover_in_flight_tasks 同事务调用）。"""
    released = 0
    orphans = cur.execute(
        """SELECT r.id AS rid, r.task_id AS task_id, r.generation AS gen
           FROM notif_quota_reservations r
           JOIN notif_send_tasks t ON t.id = r.task_id
           WHERE r.state='reserved'
             AND t.status IN ('sent','cancelled','quarantined',
                              'awaiting_manual')
             AND t.quota_generation = r.generation""").fetchall()
    for o in orphans:
        cur.execute(
            "UPDATE notif_quota_reservations SET state='released', "
            "reason='recover_orphan', updated_at=? WHERE id=?", (now, o["rid"]))
        released += 1
    if released:
        audit.record(cur, "notif_quota_recover_reconciled", None, None,
                     {"released_orphan_reservations": released}, ts=now)
    return {"released_orphan_reservations": released}


# ============================================================================
# 人工处置额度转人工任务
# ============================================================================

def resolve_quota_manual_task(db: Database, task_id: int,
                              req: QuotaResolveRequest) -> dict:
    """处置因超额转 awaiting_manual 的发送任务。

    - retry：回收旧预占、generation+1，任务回 pending 立即按当前窗口重新预占
      （仍可能再次超额转人工/延迟/降级）；
    - ignore：确认不再发送，回收未使用预占，任务置 cancelled（轨迹保留）。
    """
    operator = notif._require(req.operator, "operator")
    if req.action not in ("retry", "ignore"):
        raise HTTPException(422, "action must be 'retry' or 'ignore'")
    now = time.time()
    with db.tx() as cur:
        task = cur.execute("SELECT * FROM notif_send_tasks WHERE id=?",
                           (task_id,)).fetchone()
        if task is None:
            raise HTTPException(404, "notification send task not found")
        if task["status"] != TASK_AWAITING_MANUAL:
            raise HTTPException(
                409, f"task is {task['status']}, not awaiting_manual")
        if task["quota_status"] != QS_MANUAL:
            raise HTTPException(
                409, "task is awaiting manual review for a non-quota reason; "
                     "use the receipt review resolution endpoint")
        if req.action == "ignore":
            cur.execute(
                """UPDATE notif_send_tasks SET status='cancelled',
                   cancelled_reason='quota_manual_ignored', next_retry_at=NULL,
                   quota_status=?, updated_at=? WHERE id=?""",
                (QS_IGNORED, now, task_id))
            n = release_for_task_tx(cur, task, "quota_manual_ignored", now)
            audit.record(cur, "notif_quota_manual_resolved", None, None, {
                "send_task_id": task_id, "operator": operator, "action": "ignore",
                "note": req.note, "released_reservations": n}, ts=now)
            return {"result": "ignored", "task_id": task_id,
                    "released_reservations": n}
        new_gen = bump_generation_tx(cur, task, "quota_manual_retry", now)
        cur.execute(
            """UPDATE notif_send_tasks SET status='pending', next_retry_at=NULL,
               current_channel=NULL, updated_at=? WHERE id=?""", (now, task_id))
        audit.record(cur, "notif_quota_manual_resolved", None, None, {
            "send_task_id": task_id, "operator": operator, "action": "retry",
            "note": req.note, "new_generation": new_gen}, ts=now)
        return {"result": "retry_scheduled", "task_id": task_id,
                "generation": new_gen}


# ============================================================================
# 查询：版本 / 当前消耗 / 预占 / 受影响任务
# ============================================================================

def get_current_quota(db: Database) -> dict:
    with db.tx() as cur:
        pointer = cur.execute(
            "SELECT * FROM notif_quota_current WHERE id=1").fetchone()
        version = pointer["quota_version"] if pointer else None
        out = {"current_version": version, "updated_by": None,
               "updated_at": None, "reason": None, "config": None,
               "rollback_available": False}
        if pointer is not None:
            out.update(updated_by=pointer["updated_by"],
                       updated_at=pointer["updated_at"], reason=pointer["reason"])
        if version is not None:
            out["config"] = _load_version(cur, version)
            prev = cur.execute(
                """SELECT version FROM notif_quota_versions
                   WHERE result IN ('applied','rollback') AND version < ?
                   ORDER BY id DESC LIMIT 1""", (version,)).fetchone()
            out["rollback_available"] = prev is not None
        return out


def list_quota_versions(db: Database, limit: int = 100) -> dict:
    rows = db.query(
        "SELECT * FROM notif_quota_versions ORDER BY id DESC LIMIT ?", (limit,))
    return {"versions": [{"id": r["id"], "version": r["version"],
                          "result": r["result"], "operator": r["operator"],
                          "reason": r["reason"], "created_at": r["created_at"],
                          "has_config": r["config_json"] is not None}
                         for r in rows]}


def get_quota_version(db: Database, version: int) -> dict:
    row = db.query_one(
        "SELECT * FROM notif_quota_versions WHERE version=? "
        "AND result IN ('applied','rollback') ORDER BY id DESC LIMIT 1",
        (version,))
    if row is None:
        raise HTTPException(404, "quota version not found")
    return {"version": row["version"], "config": json.loads(row["config_json"]),
            "operator": row["operator"], "created_at": row["created_at"]}


def quota_usage(db: Database, *, version: int | None = None,
                recipient: str | None = None, now: float | None = None) -> dict:
    """当前消耗视图：每条规则 × 接收人 × 当前窗口的占用/额度/窗口边界与预占明细。"""
    now = time.time() if now is None else now
    with db.tx() as cur:
        cur_version = current_version(db)
        ver = version or cur_version
        if ver is None:
            return {"current_version": None, "buckets": []}
        config = _load_version(cur, ver)
        rules = config.get("rules") or []
        sql = ("SELECT rule_id, recipient, bucket_start, window_seconds, kind, "
               "state, SUM(cost) AS cost, COUNT(*) AS n "
               "FROM notif_quota_reservations "
               "WHERE quota_version=? AND state IN ('reserved','consumed')")
        params: list = [ver]
        if recipient:
            sql += " AND recipient=?"
            params.append(recipient)
        sql += " GROUP BY rule_id, recipient, bucket_start, window_seconds, kind, state"
        agg = cur.execute(sql, tuple(params)).fetchall()
        groups: dict[tuple, dict] = {}
        for r in agg:
            key = (r["rule_id"], r["recipient"], r["bucket_start"])
            g = groups.setdefault(key, {
                "rule_id": r["rule_id"], "recipient": r["recipient"],
                "bucket_start": r["bucket_start"],
                "window_seconds": r["window_seconds"],
                "window_ends_at": r["bucket_start"] + r["window_seconds"],
                "used": 0, "reserved_cost": 0, "consumed_cost": 0,
                "reserved_count": 0, "consumed_count": 0,
                "downgraded_cost": 0, "downgraded_count": 0,
                "limit": None, "on_exceeded": None,
                "level": None, "active": None})
            if r["kind"] == KIND_DOWNGRADED:
                g["downgraded_cost"] += int(r["cost"])
                g["downgraded_count"] += int(r["n"])
            elif r["state"] == RS_CONSUMED:
                g["consumed_cost"] += int(r["cost"])
                g["consumed_count"] += int(r["n"])
                g["used"] += int(r["cost"])
            else:
                g["reserved_cost"] += int(r["cost"])
                g["reserved_count"] += int(r["n"])
                g["used"] += int(r["cost"])
        rule_by_id = {r["id"]: r for r in rules}
        out = []
        for g in groups.values():
            rule = rule_by_id.get(g["rule_id"])
            if rule is not None:
                g.update(limit=rule["limit"], on_exceeded=rule["on_exceeded"],
                         level=rule["level"] or "*", active=rule["active"])
            g["window_current"] = g["bucket_start"] <= now < g["window_ends_at"]
            g["available"] = (max(rule["limit"] - g["used"], 0)
                              if rule is not None else None)
            out.append(g)
        out.sort(key=lambda g: (g["recipient"], g["rule_id"], g["bucket_start"]))
        return {"current_version": cur_version, "viewed_version": ver,
                "now": now, "buckets": out}


def list_reservations(db: Database, *, task_id: int | None = None,
                      recipient: str | None = None, version: int | None = None,
                      rule_id: str | None = None, state: str | None = None,
                      limit: int = 100) -> dict:
    sql, params = "SELECT * FROM notif_quota_reservations WHERE 1=1", []
    if task_id is not None:
        sql += " AND task_id=?"
        params.append(task_id)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if version is not None:
        sql += " AND quota_version=?"
        params.append(version)
    if rule_id:
        sql += " AND rule_id=?"
        params.append(rule_id)
    if state:
        sql += " AND state=?"
        params.append(state)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"reservations": [_reservation_view(r) for r in rows],
            "count": len(rows)}


def _reservation_view(r: sqlite3.Row) -> dict:
    return {"id": r["id"], "task_id": r["task_id"], "event_id": r["event_id"],
            "recipient": r["recipient"], "quota_version": r["quota_version"],
            "rule_id": r["rule_id"], "level": r["level"],
            "bucket_start": r["bucket_start"],
            "window_ends_at": r["bucket_start"] + r["window_seconds"],
            "window_seconds": r["window_seconds"], "cost": r["cost"],
            "kind": r["kind"], "state": r["state"], "generation": r["generation"],
            "reason": r["reason"], "created_at": r["created_at"],
            "updated_at": r["updated_at"]}


def list_affected_tasks(db: Database, *, quota_status: str | None = None,
                        recipient: str | None = None, version: int | None = None,
                        limit: int = 100) -> dict:
    """被延迟/降级/转人工（及已消耗/回收）的发送任务查询。"""
    sql, params = "SELECT * FROM notif_send_tasks WHERE quota_status<>?", ["none"]
    if quota_status:
        sql += " AND quota_status=?"
        params.append(quota_status)
    if recipient:
        sql += " AND recipient=?"
        params.append(recipient)
    if version is not None:
        sql += " AND quota_version=?"
        params.append(version)
    sql += " ORDER BY id DESC LIMIT ?"
    params.append(limit)
    rows = db.query(sql, tuple(params))
    return {"tasks": [{
        "id": r["id"], "todo_id": r["todo_id"], "event_id": r["event_id"],
        "recipient": r["recipient"], "event_type": r["event_type"],
        "status": r["status"], "quota_version": r["quota_version"],
        "quota_rule_id": r["quota_rule_id"], "quota_level": r["quota_level"],
        "quota_status": r["quota_status"], "quota_reason": r["quota_reason"],
        "quota_generation": r["quota_generation"],
        "next_retry_at": r["next_retry_at"],
        "sent_channel": r["sent_channel"], "created_at": r["created_at"]}
        for r in rows], "count": len(rows)}


# ============================================================================
# 路由
# ============================================================================

def create_quota_router(db: Database, settings: Settings,
                        senders_provider=None) -> APIRouter:
    router = APIRouter(prefix="/admin/approval-notifications/quota",
                       tags=["approval-notification-quota"])

    @router.get("/current")
    def current():
        """当前生效额度配置版本、快照与是否可回滚。"""
        return get_current_quota(db)

    @router.post("/versions")
    def publish(req: QuotaConfigRequest):
        """发布新额度配置（校验失败保留当前版本并落 rejected 记录）。"""
        return publish_quota(db, req)

    @router.get("/versions")
    def versions(limit: int = Query(100, le=1000)):
        """额度配置版本历史（applied/rollback/rejected）。"""
        return list_quota_versions(db, limit)

    @router.get("/versions/{version}")
    def version_detail(version: int):
        return get_quota_version(db, version)

    @router.post("/rollback")
    def rollback(req: RollbackRequest):
        """回滚到上一生效额度版本（原因必填）；只影响之后入队的任务。"""
        return rollback_quota(db, req)

    @router.get("/usage")
    def usage(recipient: str | None = None, version: int | None = None):
        """当前消耗：规则×接收人×窗口的占用/额度/窗口边界。"""
        return quota_usage(db, version=version, recipient=recipient)

    @router.get("/reservations")
    def reservations(task_id: int | None = None, recipient: str | None = None,
                     version: int | None = None, rule_id: str | None = None,
                     state: str | None = None, limit: int = Query(100, le=1000)):
        """预占/消耗/回收明细。"""
        return list_reservations(
            db, task_id=task_id, recipient=recipient, version=version,
            rule_id=rule_id, state=state, limit=limit)

    @router.get("/tasks")
    def tasks(quota_status: str | None = None, recipient: str | None = None,
              version: int | None = None, limit: int = Query(100, le=1000)):
        """被延迟/降级/转人工/已消耗的发送任务。"""
        return list_affected_tasks(
            db, quota_status=quota_status, recipient=recipient, version=version,
            limit=limit)

    @router.post("/tasks/{task_id}/resolve")
    def resolve(task_id: int, req: QuotaResolveRequest):
        """处置额度超额转人工的任务：retry（重新预占）/ ignore（回收并取消）。

        retry 只把任务排回队列，由下一轮 worker 领取并按当前窗口重新预占（不在 HTTP
        请求里做通道 IO，且立即自动重判会再次命中本窗口超额，失去人工处置意义）。"""
        return resolve_quota_manual_task(db, task_id, req)

    return router

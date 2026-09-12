"""业务重放模块：可追溯的历史回调重放。

流程：筛选预览（preview）-> 提交批次（submit）-> [高风险须审批] ->
后台重放 worker 逐条执行 -> 副作用走与正常处理完全相同的 outbox 幂等派发链路。

- 每条重放任务记录发起人、原始版本（delivery_id）、原因和进度（状态/次数/checkpoint）；
- 同一份内容不会生成第二个重放任务：批内 UNIQUE(batch_id, delivery_id) 去重；
  提交带 request_id 时重复提交返回原批次；跨批存在活动任务（pending/processing，
  含待审批批次占住的任务）的投递会被跳过（已完成的批次不阻塞以后再次重放
  ——那是有意为之的新批次）；
- 多级审批策略：审批要求不再硬编码为「高风险一次他人批准」，而是由可版本化
  维护的审批策略（见 replay_policy.py）按风险等级与批次规模匹配规则，为每个
  提交的批次生成一串审批节点（串行逐节点激活 / 并行同时待决）。每个节点记录
  指定角色、实际审批人与截止时间；审批人不能是批次发起人，也不能重复承担
  同一批次的多个节点；任一节点拒绝或超时即终止整个批次（未执行任务整体
  取消），所有节点批准（或被有理由地跳过）后批次才进入 running，worker
  方可领取。批次落盘时保存所采用策略的快照（版本 + 规则 + 节点规格），
  之后的策略更新只影响新提交的批次；
- 高风险审批：提交时可用 risk_level=high（并填 approval_note）标记高风险批次，
  批次进入 pending_approval 而不是 running，worker 在批准前不能领取其任何任务
  （跨批活动检查同时占住对应内容，防止绕过审批另开一批）；批准必须由不同于
  发起人的运营人员显式做出（POST .../approve），批准后批次才进入 running；
  拒绝（POST .../reject，必填拒绝原因）或超时（approval_deadline 到期由 worker
  扫描释放）把未执行任务整体置为终态 cancelled。批准/拒绝都是条件状态转移，
  重复批准/拒绝不会产生第二次效果；request_id 重复提交也不会产生第二次执行；
- 批次级并发配额：提交时可指定 max_concurrency（整个批次最多同时处理多少条），
  占用量 = 本批 processing 中的任务数，领取时在占位事务里实时推导——任务离开
  processing（完成/失败/取消/重启回收）槽位即释放，不存在需要单独回收的计数器，
  暂停/取消/重启后配额天然正确，任务不会永久卡住；
- 同一编号有序执行：同批次同 external_id 的多条历史版本按版本落盘时间
  （delivery_created_at 快照，再按 delivery_id 决胜）先后执行，前一条未进终态
  （done/failed/cancelled）时后一条不能被 worker 领取；
- 被配额/顺序挡住的任务：worker 在原因变化时写 replay_task_blocked 审计（轮询不
  重复刷），批次详情实时展示当前占用、等待数量和每条任务的阻塞原因；
- 批次可暂停/继续/取消；取消时未执行的任务与滞留的待派发副作用同事务取消；
  正在处理的任务同样标记 cancelled，其迟到的完成/失败结果落库时被条件更新
  挡下（保持 cancelled，不落副作用、不写完成记录），审计留 discarded 事件；
- 重启后 recover() 把卡在 processing 的任务退回 pending，从上次位置（attempts/
  checkpoint/next_retry_at 都落库）继续，占用的配额随之释放；
- 失败按指数退避单独重试，超限标记 failed，可人工单条重试，不阻塞其他编号；
- 重放副作用的幂等键以 replay:{task_id} 为作用域：同一任务重试/重启不会重复派发，
  下游仍按幂等键去重（与正常处理同一套 outbox + sink 保护）；新批次新任务才会
  真正再次产生外部效果——这正是重放的目的。
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import sqlite3
import time
from datetime import datetime

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from . import audit
from .config import Settings
from .db import Database
from . import delegation as delegation_mod
from .handlers import business_handler
from .replay_policy import ROLE_ANY, match_rule, resolve_rules

log = logging.getLogger("gateway.replay")


def replay_effect_key(task_id: int, effect_type: str, effect_payload: dict) -> str:
    """重放副作用幂等键：以重放任务为作用域，同一任务重算结果恒定。

    任务重试/服务重启 -> 键相同 -> INSERT OR IGNORE 去重，不会重复派发；
    下游 sink 也按该键去重，与正常处理同一套 exactly-once 保护。
    """
    canonical = json.dumps(effect_payload, ensure_ascii=False, sort_keys=True)
    return hashlib.sha256(f"replay:{task_id}:{effect_type}:{canonical}".encode()).hexdigest()


# ---- 筛选与预览 ------------------------------------------------------------

class ReplayFilter(BaseModel):
    """历史回调筛选条件：编号、时间范围、处理结果，或直接指定版本号列表。"""
    external_id: str | None = None        # 业务编号
    status: str | None = None             # 处理结果：done|quarantined|pending|conflicted|superseded
    created_from: float | str | None = None  # 落盘时间起（epoch 秒或 ISO-8601）
    created_to: float | str | None = None    # 落盘时间止
    delivery_ids: list[int] | None = None    # 直接指定内容版本


class PreviewRequest(ReplayFilter):
    pass


class SubmitRequest(ReplayFilter):
    operator: str                  # 发起人（必填，落每条任务与审计）
    reason: str                    # 重放原因（必填）
    request_id: str | None = None  # 提交幂等键：重复提交返回原批次
    # 批次级并发配额：整个批次最多同时处理多少条任务；NULL 表示不限
    max_concurrency: int | None = Field(default=None, ge=1)
    # 风险等级：high 为高风险，必须由非发起人批准后才能进入 running；normal 直接运行
    risk_level: str = "normal"
    # 审批说明：高风险批次必填（为什么要做这次高风险重放，供审批人判断与事后追溯）
    approval_note: str | None = None


class BatchActionRequest(BaseModel):
    operator: str
    note: str = ""


class ApprovalRequest(BaseModel):
    """高风险批次的批准/拒绝请求：审批人必须是不同于发起人的运营人员。"""
    operator: str                  # 审批人（必填，须不同于批次发起人）
    delegation_id: int | None = None  # 承担指定角色所用的有效委托（any 节点可省）
    note: str = ""                 # 批准备注（可选）


class RejectionRequest(BaseModel):
    operator: str                  # 审批人（必填，须不同于批次发起人）
    reason: str                    # 拒绝原因（必填，落批次与审计）
    delegation_id: int | None = None
    note: str = ""


class NodeDecisionRequest(BaseModel):
    """单个审批节点的决定请求（批准/拒绝/跳过共用）。

    审批人承担指定角色时须携带本人当前有效的委托 id（delegation_id）；'any' 节点
    无需委托。role 为审批人实际承担的角色（节点允许多角色时用以指明哪一个）。
    """
    operator: str                  # 审批人（必填，须不同于发起人，且未在本批其他节点持有效票）
    role: str | None = None        # 审批人承担的角色；节点指定非 any 角色时必填且必须被允许
    delegation_id: int | None = None  # 审批委托 id：承担指定角色时必填且决定时须仍有效
    note: str = ""


class NodeRejectionRequest(BaseModel):
    operator: str
    reason: str                    # 拒绝原因（必填）；任一节点拒绝即终止整个批次
    role: str | None = None
    delegation_id: int | None = None
    note: str = ""


class NodeSkipRequest(BaseModel):
    operator: str
    reason: str                    # 跳过原因（必填，留痕可追溯）；跳过视为该节点已满足
    role: str | None = None
    delegation_id: int | None = None
    note: str = ""


# 批次/审批状态
BATCH_RUNNING = "running"
BATCH_PAUSED = "paused"
BATCH_PENDING_APPROVAL = "pending_approval"
BATCH_REJECTED = "rejected"
BATCH_CANCELLED = "cancelled"
RISK_LEVELS = ("normal", "high")
APPROVAL_NOT_REQUIRED = "not_required"
APPROVAL_PENDING = "pending"
APPROVAL_APPROVED = "approved"
APPROVAL_REJECTED = "rejected"
APPROVAL_EXPIRED = "expired"

# 审批节点状态
NODE_WAITING = "waiting"      # 串行链中尚未轮到
NODE_ACTIVE = "active"        # 待决中（可批准/拒绝/跳过，超时会被释放）
NODE_APPROVED = "approved"
NODE_REJECTED = "rejected"
NODE_SKIPPED = "skipped"      # 有理由跳过，视为已满足
NODE_EXPIRED = "expired"      # 超时未决（批次随之取消）
NODE_CANCELLED = "cancelled"  # 批次被终止/撤回时随之关闭的待决节点
NODE_UNDECIDED = (NODE_WAITING, NODE_ACTIVE)


def node_allowed_roles(spec_or_node) -> list[str]:
    """节点允许承担的角色列表：策略节点取 roles（兼容只有 role 的老规则），
    已落盘节点取 allowed_roles JSON（兼容只有 role 列的老库行）。"""
    keys = spec_or_node.keys()
    if "allowed_roles" in keys and spec_or_node["allowed_roles"]:
        raw = spec_or_node["allowed_roles"]
        if isinstance(raw, str):
            try:
                parsed = json.loads(raw)
                if isinstance(parsed, list) and parsed:
                    return [str(r) for r in parsed]
            except (json.JSONDecodeError, TypeError):
                pass
        elif isinstance(raw, list) and raw:
            return [str(r) for r in raw]
    roles = spec_or_node["roles"] if "roles" in keys and spec_or_node["roles"] else None
    if roles:
        return [str(r) for r in roles]
    return [spec_or_node["role"]]


def node_required(spec_or_node) -> int:
    """节点法定人数：缺省 1（老策略节点/老库行）。"""
    value = spec_or_node["required_approvals"] if "required_approvals" in spec_or_node.keys() \
        else 1
    return int(value) if value is not None else 1


def _require_operator(operator: str, field: str = "operator"):
    if not operator or not operator.strip():
        raise HTTPException(422, f"{field} must be non-empty")
    return operator.strip()


def _parse_time(value) -> float | None:
    """时间筛选支持 epoch 秒（数字）或 ISO-8601 字符串。"""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    text = str(value).strip()
    try:
        return float(text)
    except ValueError:
        pass
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp()
    except ValueError:
        raise HTTPException(422, f"invalid time filter: {value!r} "
                                 "(expect epoch seconds or ISO-8601)")


def _select_deliveries(db: Database, f: ReplayFilter):
    """预览与提交共用同一套筛选，保证「看到的就是将要提交的」。"""
    sql, params = "SELECT * FROM deliveries WHERE 1=1", []
    if f.external_id:
        sql += " AND external_id=?"
        params.append(f.external_id)
    if f.status:
        sql += " AND status=?"
        params.append(f.status)
    t_from, t_to = _parse_time(f.created_from), _parse_time(f.created_to)
    if t_from is not None:
        sql += " AND created_at>=?"
        params.append(t_from)
    if t_to is not None:
        sql += " AND created_at<=?"
        params.append(t_to)
    if f.delivery_ids:
        sql += f" AND id IN ({','.join('?' * len(f.delivery_ids))})"
        params.extend(f.delivery_ids)
    sql += " ORDER BY id"
    return db.query(sql, tuple(params))


def _skip_reason(delivery) -> str | None:
    """哪些版本不允许重放（返回 None 表示可重放）。

    只允许重放已脱离正常管线的版本（done/quarantined）：仍在管线中的、
    冲突冻结中的、人工未选中的版本重放出去会产生不该有的外部效果。
    """
    status = delivery["status"]
    if status == "superseded":
        return "superseded_version"
    if delivery["frozen"] or status == "conflicted":
        return "frozen_by_conflict"
    if status in ("pending", "processing"):
        return "still_in_pipeline"
    return None


def _active_replay_by_delivery(db: Database) -> dict[int, int]:
    """当前有活动重放任务（pending/processing）的 delivery -> batch_id。"""
    rows = db.query(
        "SELECT delivery_id, batch_id FROM replay_tasks WHERE status IN ('pending','processing')")
    return {r["delivery_id"]: r["batch_id"] for r in rows}


def _preview_item(db: Database, delivery, active: dict[int, int]) -> dict:
    skip = _skip_reason(delivery)
    if skip is None and delivery["id"] in active:
        skip = f"active_replay_in_batch:{active[delivery['id']]}"
    # 影响范围：该版本正常处理时产出过的外部副作用（重放将按同内容重新产出，
    # 走同一 outbox 幂等链路）；未处理过的版本则没有历史副作用可参照
    prior = db.query(
        "SELECT effect_type, payload, status FROM outbox "
        "WHERE delivery_id=? AND replay_task_id IS NULL ORDER BY id",
        (delivery["id"],),
    )
    return {
        "delivery_id": delivery["id"],
        "external_id": delivery["external_id"],
        "status": delivery["status"],
        "frozen": delivery["frozen"],
        "content_hash": delivery["content_hash"],
        "created_at": delivery["created_at"],
        "payload": delivery["payload"],
        "replayable": skip is None,
        "skip_reason": skip,
        "prior_effects": [{"effect_type": p["effect_type"], "status": p["status"],
                           "payload": json.loads(p["payload"])} for p in prior],
    }


def preview(db: Database, req: PreviewRequest) -> dict:
    """预览：将要重放的内容 + 影响范围（历史副作用），不落任何数据。"""
    rows = _select_deliveries(db, req)
    active = _active_replay_by_delivery(db)
    items = [_preview_item(db, d, active) for d in rows]
    return {"matched": len(items),
            "replayable": sum(1 for i in items if i["replayable"]),
            "items": items}


# ---- 提交批次 --------------------------------------------------------------

def submit(db: Database, req: SubmitRequest, approval_timeout: float) -> tuple[int, dict]:
    """一次性提交一批重放任务：批次 + 全部任务 + 审批节点链在一个事务里落盘。

    审批节点链由当前生效策略按风险等级与批次规模解析生成（无已生效策略时
    用内置默认策略）；解析结果作为策略快照随批次保存，之后的策略更新不影响
    本批。需要审批的批次落为 pending_approval：任务照常落盘并占住对应内容
    （其他批次不能再提交同一投递），但 worker 在所有节点满足前不领取。
    """
    operator = _require_operator(req.operator)
    if not req.reason.strip():
        raise HTTPException(422, "reason must be non-empty")
    if req.risk_level not in RISK_LEVELS:
        raise HTTPException(422, f"invalid risk_level: {req.risk_level!r} "
                                 f"(expect one of {','.join(RISK_LEVELS)})")
    approval_note = (req.approval_note or "").strip()
    high_risk = req.risk_level == "high"
    if high_risk and not approval_note:
        raise HTTPException(422, "approval_note is required for high risk batches")

    # 提交幂等：同一 request_id 重复提交（网络重试/双击）返回原批次
    if req.request_id:
        existing = db.query_one(
            "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
        if existing is not None:
            return 200, {"result": "duplicate", "batch_id": existing["id"],
                         "total": existing["total"],
                         "status": existing["status"],
                         "approval_status": existing["approval_status"]}

    rows = _select_deliveries(db, req)
    active = _active_replay_by_delivery(db)
    eligible, skipped = [], []
    for d in rows:
        reason = _skip_reason(d)
        if reason is None and d["id"] in active:
            reason = f"active_replay_in_batch:{active[d['id']]}"
        if reason is not None:
            skipped.append({"delivery_id": d["id"], "external_id": d["external_id"],
                            "reason": reason})
        else:
            eligible.append(d)

    if not eligible:
        return 422, {"error": "no_replayable_deliveries",
                     "matched": len(rows), "skipped": skipped}

    now = time.time()
    total = len(eligible)
    # 按当前生效策略（无已生效版本时为内置默认策略）解析本批的审批节点链：
    # 规则按风险等级与批次规模匹配，节点串行或并行；解析结果作为快照随批次
    # 落盘，之后策略更新不影响本批。已生效策略下高风险批次无规则匹配时
    # 拒绝提交（fail closed，不静默降低审批要求）。
    policy_version, rules = resolve_rules(db, approval_timeout)
    rule = match_rule(rules, req.risk_level, total)
    if rule is None:
        if high_risk:
            return 422, {"error": "no_applicable_policy",
                         "detail": "no approval policy rule matches this batch; "
                                   "ask a policy maintainer to cover it",
                         "risk_level": req.risk_level, "total": total}
        node_specs, mode, rule_name = [], "serial", None
    else:
        node_specs, mode, rule_name = rule["nodes"], rule["mode"], rule["name"]

    if node_specs:
        batch_status = BATCH_PENDING_APPROVAL
        approval_status = APPROVAL_PENDING
        # 批次截止时间 = 活动节点中最早的截止（串行：首节点；并行：全体同时激活取最小）
        first_timeouts = ([node_specs[0]["timeout_seconds"]] if mode == "serial"
                          else [n["timeout_seconds"] for n in node_specs])
        deadline = now + min(first_timeouts)
    else:
        batch_status = BATCH_RUNNING
        approval_status = APPROVAL_NOT_REQUIRED
        deadline = None
    snapshot = {"policy_version": policy_version, "rule_name": rule_name, "mode": mode,
                "risk_level": req.risk_level, "batch_size": total,
                "nodes": [{"seq": i, "role": n["role"],
                           "allowed_roles": node_allowed_roles(n),
                           "required_approvals": node_required(n),
                           "timeout_seconds": n["timeout_seconds"]}
                          for i, n in enumerate(node_specs)]}
    filters = req.model_dump(exclude={"operator", "reason", "request_id", "max_concurrency",
                                      "risk_level", "approval_note"})
    filters_json = json.dumps(filters, ensure_ascii=False, default=str)
    try:
        with db.tx() as cur:
            cur.execute(
                """INSERT INTO replay_batches
                   (request_id, operator, reason, status, filters, max_concurrency,
                    risk_level, approval_note, approval_status, approval_deadline,
                    policy_version, policy_snapshot,
                    total, created_at, updated_at)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                (req.request_id, operator, req.reason, batch_status, filters_json,
                 req.max_concurrency, req.risk_level,
                 approval_note or None, approval_status, deadline,
                 policy_version, json.dumps(snapshot, ensure_ascii=False, sort_keys=True),
                 len(eligible), now, now),
            )
            batch_id = cur.lastrowid
            for d in eligible:
                # INSERT OR IGNORE + UNIQUE(batch_id, delivery_id)：
                # 同一份内容重复加入也不会生成第二个重放任务；
                # delivery_created_at 快照原始版本的落盘时间，作为同编号有序执行的排序键
                cur.execute(
                    """INSERT OR IGNORE INTO replay_tasks
                       (batch_id, delivery_id, external_id, operator, reason,
                        status, delivery_created_at, created_at, updated_at)
                       VALUES (?,?,?,?,?,'pending',?,?,?)""",
                    (batch_id, d["id"], d["external_id"], operator, req.reason,
                     d["created_at"], now, now),
                )
            # 审批节点链随批次一次性落盘（来自策略快照，之后不随策略变更而改变）：
            # 串行只激活首节点，并行全部激活；激活时起算各自截止时间。
            # 每个节点带允许角色列表与法定人数（有效赞成票达到才满足）。
            for i, spec in enumerate(node_specs):
                activated = mode == "parallel" or i == 0
                cur.execute(
                    """INSERT INTO replay_approval_nodes
                       (batch_id, seq, role, allowed_roles, required_approvals,
                        status, timeout_seconds, activated_at, deadline, created_at)
                       VALUES (?,?,?,?,?,?,?,?,?,?)""",
                    (batch_id, i, spec["role"],
                     json.dumps(node_allowed_roles(spec), ensure_ascii=False),
                     node_required(spec),
                     NODE_ACTIVE if activated else NODE_WAITING,
                     spec["timeout_seconds"],
                     now if activated else None,
                     now + spec["timeout_seconds"] if activated else None,
                     now),
                )
            audit.record(cur, "replay_batch_created", None, None, {
                "replay_batch_id": batch_id, "operator": operator,
                "reason": req.reason, "request_id": req.request_id,
                "max_concurrency": req.max_concurrency,
                "risk_level": req.risk_level, "approval_note": approval_note or None,
                "status": batch_status, "approval_status": approval_status,
                "approval_deadline": deadline,
                "policy_version": policy_version, "rule_name": rule_name,
                "approval_mode": mode if node_specs else None,
                "approval_nodes": len(node_specs),
                "filters": filters, "total": len(eligible), "skipped": skipped}, ts=now)
    except sqlite3.IntegrityError:
        # 并发下 request_id 撞唯一键：返回已存在的那一批
        if req.request_id:
            existing = db.query_one(
                "SELECT * FROM replay_batches WHERE request_id=?", (req.request_id,))
            if existing is not None:
                return 200, {"result": "duplicate", "batch_id": existing["id"],
                             "total": existing["total"],
                             "status": existing["status"],
                             "approval_status": existing["approval_status"]}
        raise

    body = {"result": "created", "batch_id": batch_id,
            "total": len(eligible), "skipped": skipped,
            "status": batch_status, "risk_level": req.risk_level,
            "approval_status": approval_status,
            "policy_version": policy_version,
            "approval_nodes": len(node_specs)}
    if deadline is not None:
        body["approval_deadline"] = deadline
    return 201, body


# ---- 批次控制：暂停 / 继续 / 取消 --------------------------------------------

def _get_batch_or_404(db: Database, batch_id: int):
    row = db.query_one("SELECT * FROM replay_batches WHERE id=?", (batch_id,))
    if row is None:
        raise HTTPException(404, "replay batch not found")
    return row


# ---- 多级审批：节点决定（批准/拒绝/跳过）、批次级兼容入口、超时释放 ---------------

def _batch_pending_guard(batch) -> None:
    """审批决定的共同前置：批次仍处于待决状态（否则决定不再被接受）。"""
    if batch["approval_status"] != APPROVAL_PENDING \
            or batch["status"] != BATCH_PENDING_APPROVAL:
        raise HTTPException(
            409, f"batch approval is {batch['approval_status']}, "
                 f"batch is {batch['status']}, decision no longer accepted")


def _terminate_batch_rejected(cur: sqlite3.Cursor, batch, operator: str,
                              reason: str, note: str, now: float) -> dict:
    """任一节点拒绝 -> 整个批次终止：未执行任务整体取消，其余待决节点关闭。

    拒绝不可撤销；占住的投递随之释放（之后可以重新提交新批次）。
    """
    batch_id = batch["id"]
    cancelled_tasks = cur.execute(
        """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
           finished_at=?, updated_at=?
           WHERE batch_id=? AND status IN ('pending','processing')""",
        (now, now, batch_id),
    ).rowcount
    cur.execute(
        """UPDATE replay_approval_nodes SET status='cancelled'
           WHERE batch_id=? AND status IN ('waiting','active')""",
        (batch_id,),
    )
    cur.execute(
        """UPDATE replay_batches
           SET status='rejected', approval_status='rejected', approver=?,
               approval_reason=?, approved_at=NULL, cancelled=cancelled+?,
               updated_at=?, finished_at=?
           WHERE id=? AND status='pending_approval' AND approval_status='pending'""",
        (operator, reason, cancelled_tasks, now, now, batch_id),
    )
    audit.record(cur, "replay_batch_rejected", None, None,
                 {"replay_batch_id": batch_id, "operator": operator,
                  "submitted_by": batch["operator"], "reason": reason,
                  "note": note, "risk_level": batch["risk_level"],
                  "cancelled_tasks": cancelled_tasks}, ts=now)
    return {"batch_status": BATCH_REJECTED, "approval_status": APPROVAL_REJECTED,
            "cancelled_tasks": cancelled_tasks, "remaining_node_ids": []}


def _advance_chain(cur: sqlite3.Cursor, batch, operator: str, note: str,
                   now: float) -> dict:
    """一票落定后推进审批链：

    - 所有待决节点（waiting/active）都已消失 -> 全部节点满足，批次进入 running
      （条件更新保证并发决定最多放行一次）；
    - 串行：若上一节点刚满足且下一节点仍 waiting，激活下一节点（起算其截止时间）；
    - 并行：批次截止时间收敛为剩余活动节点中最早的截止。

    法定人数节点达到人数时已由 decide_node 落定为 approved，故这里 remaining 为空
    即代表全部满足，不再按「节点有 decided_by」推断。"""
    batch_id = batch["id"]
    remaining = cur.execute(
        """SELECT * FROM replay_approval_nodes
           WHERE batch_id=? AND status IN ('waiting','active') ORDER BY seq""",
        (batch_id,),
    ).fetchall()
    if not remaining:
        cur.execute(
            """UPDATE replay_batches
               SET status='running', approval_status='approved', approver=?,
                   approved_at=?, approval_reason=NULL, updated_at=?
               WHERE id=? AND status='pending_approval' AND approval_status='pending'""",
            (operator, now, now, batch_id),
        )
        audit.record(cur, "replay_batch_approved", None, None,
                     {"replay_batch_id": batch_id, "operator": operator,
                      "submitted_by": batch["operator"], "note": note,
                      "risk_level": batch["risk_level"]}, ts=now)
        return {"batch_status": BATCH_RUNNING, "approval_status": APPROVAL_APPROVED,
                "remaining_node_ids": []}
    waiting = [n for n in remaining if n["status"] == NODE_WAITING]
    active = [n for n in remaining if n["status"] == NODE_ACTIVE]
    if waiting and not active:
        # 串行链：上一节点满足后激活下一节点，从此时起算其截止时间
        nxt = waiting[0]
        node_deadline = now + nxt["timeout_seconds"]
        cur.execute(
            """UPDATE replay_approval_nodes SET status='active', activated_at=?,
               deadline=? WHERE id=? AND status='waiting'""",
            (now, node_deadline, nxt["id"]),
        )
        cur.execute(
            "UPDATE replay_batches SET approval_deadline=?, updated_at=? WHERE id=?",
            (node_deadline, now, batch_id),
        )
        audit.record(cur, "replay_approval_node_activated", None, None,
                     {"replay_batch_id": batch_id, "node_id": nxt["id"],
                      "seq": nxt["seq"], "role": nxt["role"],
                      "allowed_roles": node_allowed_roles(nxt),
                      "required_approvals": node_required(nxt),
                      "deadline": node_deadline}, ts=now)
    elif active:
        # 并行：批次截止时间收敛为剩余活动节点中最早的截止
        earliest = min(n["deadline"] for n in active if n["deadline"] is not None)
        cur.execute(
            "UPDATE replay_batches SET approval_deadline=?, updated_at=? WHERE id=?",
            (earliest, now, batch_id),
        )
    return {"batch_status": BATCH_PENDING_APPROVAL,
            "approval_status": APPROVAL_PENDING,
            "remaining_node_ids": [n["id"] for n in remaining]}


def _node_approved_count(cur: sqlite3.Cursor, node_id: int) -> int:
    """节点当前有效赞成票数。"""
    return cur.execute(
        "SELECT COUNT(*) AS c FROM replay_node_votes "
        "WHERE node_id=? AND status='valid' AND vote='approve'",
        (node_id,),
    ).fetchone()["c"]


def decide_node(db: Database, batch_id: int, node_id: int, action: str,
                operator: str, role: str | None = None,
                delegation_id: int | None = None,
                reason: str = "", note: str = "") -> dict:
    """对单个审批节点做出决定（approve / reject / skip）。

    approve 是投一张赞成票：有效赞成票达到节点法定人数（required_approvals）节点才
    落定为 approved；reject/skip 一投即让节点落定（拒绝还会终止整个批次）。

    所有检查与状态转移在同一个写事务里完成（BEGIN IMMEDIATE 串行化并发决定）：
    节点仍 active 才接受决定；审批人不能是批次发起人；同一节点同一人只能有一张
    有效票（部分唯一索引，重复/并发决定得到 409，不重复计数）；同一批次同一人
    不能在多个节点持有效票（不能承担多个节点）；承担指定角色须凭本人当前有效的
    委托（角色匹配且时间窗覆盖当前时刻，到期/撤销即不可用），'any' 节点无需委托；
    委托在节点满足前失效会令其赞成票失效，节点法定人数不足要重新等待。
    """
    operator = _require_operator(operator, "operator")
    reason = (reason or "").strip()
    note = (note or "").strip()
    if action not in ("approve", "reject", "skip"):
        raise HTTPException(422, f"unknown action: {action!r}")
    if action in ("reject", "skip") and not reason:
        raise HTTPException(422, f"reason must be non-empty when deciding {action}")
    now = time.time()
    with db.tx() as cur:
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (batch_id,)).fetchone()
        if batch is None:
            raise HTTPException(404, "replay batch not found")
        node = cur.execute(
            "SELECT * FROM replay_approval_nodes WHERE id=? AND batch_id=?",
            (node_id, batch_id),
        ).fetchone()
        if node is None:
            raise HTTPException(404, "approval node not found")
        _batch_pending_guard(batch)
        if operator == batch["operator"]:
            raise HTTPException(403, "approver must be different from the batch operator")
        if node["status"] != NODE_ACTIVE:
            raise HTTPException(
                409, f"node is {node['status']}, decision no longer accepted")
        allowed_roles = node_allowed_roles(node)
        designated = [r for r in allowed_roles if r != ROLE_ANY]
        decided_role = (role or "").strip()
        used_delegation = None
        if not designated:
            # 'any' 节点：任何非发起人都可承担，不需要委托；声明角色仅作记录
            decided_role = decided_role or ROLE_ANY
        else:
            if not decided_role:
                raise HTTPException(
                    422, f"role is required: this node may be acted on by roles "
                         f"{','.join(designated)}")
            if decided_role not in designated:
                raise HTTPException(
                    403, f"role {decided_role!r} is not allowed for this node "
                         f"(allowed: {','.join(designated)})")
            # 指定角色：必须凭本人当前有效的委托承担（决定时再校验一次时间窗，
            # 即使到期扫描尚未跑，过期/撤销的委托也在这里被挡住）
            used_delegation = delegation_mod.load_for_decision(
                cur, delegation_id, operator, designated, now)
        # 同一审批人不能在同一批次的多个节点持有效票（不能承担多个节点）。
        # 已失效的票不占位——委托失效后该人可以在别的节点投票，也可重新投本节点。
        other = cur.execute(
            """SELECT node_id FROM replay_node_votes
               WHERE batch_id=? AND voter=? AND status='valid' AND node_id<>? LIMIT 1""",
            (batch_id, operator, node_id),
        ).fetchone()
        if other is not None:
            raise HTTPException(403, "operator already has an effective vote on node "
                                     f"{other['node_id']} of this batch")
        # 同一节点同一人只保留一张有效票（部分唯一索引兜底并发；这里给明确 409）
        dup = cur.execute(
            "SELECT id FROM replay_node_votes WHERE node_id=? AND voter=? AND status='valid'",
            (node_id, operator),
        ).fetchone()
        if dup is not None:
            raise HTTPException(409, "operator has already voted on this node")

        # 落票。INSERT 的部分唯一索引是防并发重复计数的最后一道闸：两个并发决定
        # 串行化后第二个会撞唯一键，整个事务回滚，不产生第二次计数。
        vote_row = {
            "vote": action, "voter_role": decided_role,
            "delegation_id": used_delegation["id"] if used_delegation is not None else None,
            "reason": reason or None, "note": note or None,
        }
        try:
            cur.execute(
                """INSERT INTO replay_node_votes
                   (node_id, batch_id, vote, voter, voter_role, delegation_id,
                    status, reason, note, created_at, invalidated_at)
                   VALUES (?,?,?,?,?,?,'valid',?,?,?,NULL)""",
                (node_id, batch_id, vote_row["vote"], operator,
                 vote_row["voter_role"], vote_row["delegation_id"],
                 vote_row["reason"], vote_row["note"], now),
            )
        except sqlite3.IntegrityError:
            raise HTTPException(409, "duplicate concurrent decision on this node")

        if action == "reject":
            # 任一节点拒绝即终止整个批次（未执行任务整体取消，其余待决节点关闭）
            cur.execute(
                """UPDATE replay_approval_nodes
                   SET status='rejected', decided_by=?, decided_role=?,
                       decision_reason=?, decision_note=?, decided_at=?
                   WHERE id=? AND status='active'""",
                (operator, decided_role, reason, note or None, now, node_id),
            )
            audit.record(cur, "replay_approval_node_rejected", None, None,
                         {"replay_batch_id": batch_id, "node_id": node_id,
                          "seq": node["seq"], "role": node["role"],
                          "allowed_roles": allowed_roles,
                          "operator": operator, "decided_role": decided_role,
                          "delegation_id": vote_row["delegation_id"],
                          "reason": reason, "note": note or None}, ts=now)
            result = _terminate_batch_rejected(cur, batch, operator, reason, note, now)
            return {"result": NODE_REJECTED, "batch_id": batch_id, "node_id": node_id,
                    **result}

        if action == "skip":
            # 有理由跳过：节点视为已满足（管理覆盖），立即落定，不参与法定人数计数
            cur.execute(
                """UPDATE replay_approval_nodes
                   SET status='skipped', decided_by=?, decided_role=?,
                       decision_reason=?, decision_note=?, decided_at=?
                   WHERE id=? AND status='active'""",
                (operator, decided_role, reason, note or None, now, node_id),
            )
            audit.record(cur, "replay_approval_node_skipped", None, None,
                         {"replay_batch_id": batch_id, "node_id": node_id,
                          "seq": node["seq"], "role": node["role"],
                          "allowed_roles": allowed_roles,
                          "required_approvals": node_required(node),
                          "operator": operator, "decided_role": decided_role,
                          "delegation_id": vote_row["delegation_id"],
                          "reason": reason, "note": note or None}, ts=now)
            result = _advance_chain(cur, batch, operator, note, now)
            return {"result": NODE_SKIPPED, "batch_id": batch_id, "node_id": node_id,
                    **result}

        # approve：计有效赞成票；达到法定人数节点才落定为 approved（串行/并行都按
        # 节点分别计数）。未达到时节点保持 active，重新等待更多人批准。
        approved_count = _node_approved_count(cur, node_id)
        required = node_required(node)
        quorate = approved_count >= required
        if quorate:
            changed = cur.execute(
                """UPDATE replay_approval_nodes
                   SET status='approved', decided_by=?, decided_role=?,
                       decision_note=?, decided_at=?
                   WHERE id=? AND status='active'""",
                (operator, decided_role, note or None, now, node_id),
            ).rowcount
            if not changed:  # 并发下节点已被拒绝/超时/取消
                raise HTTPException(409, "node is no longer awaiting decision")
        audit.record(cur, "replay_approval_node_approved", None, None,
                     {"replay_batch_id": batch_id, "node_id": node_id,
                      "seq": node["seq"], "role": node["role"],
                      "allowed_roles": allowed_roles,
                      "required_approvals": required,
                      "operator": operator, "decided_role": decided_role,
                      "delegation_id": vote_row["delegation_id"],
                      "approved_count": approved_count,
                      "missing": max(0, required - approved_count),
                      "quorum_reached": quorate,
                      "note": note or None}, ts=now)
        if not quorate:
            # 法定人数不足：批次继续等待；批次截止时间不变（仍是该节点的截止）
            remaining = cur.execute(
                """SELECT id FROM replay_approval_nodes
                   WHERE batch_id=? AND status IN ('waiting','active') ORDER BY seq""",
                (batch_id,),
            ).fetchall()
            cur.execute("UPDATE replay_batches SET updated_at=? WHERE id=?",
                        (now, batch_id))
            return {"result": "voted", "batch_id": batch_id, "node_id": node_id,
                    "batch_status": BATCH_PENDING_APPROVAL,
                    "approval_status": APPROVAL_PENDING,
                    "approved_count": approved_count,
                    "required_approvals": required,
                    "missing": required - approved_count,
                    "remaining_node_ids": [r["id"] for r in remaining]}
        result = _advance_chain(cur, batch, operator, note, now)
    return {"result": NODE_APPROVED, "batch_id": batch_id, "node_id": node_id,
            "approved_count": approved_count,
            "required_approvals": required, "missing": 0, **result}


def _single_active_node(db: Database, batch_id: int):
    """批次级（兼容）审批入口的定位：恰好一个节点待决时才能推断决定对象。"""
    nodes = db.query(
        "SELECT * FROM replay_approval_nodes WHERE batch_id=? AND status='active' "
        "ORDER BY seq", (batch_id,))
    if not nodes:
        raise HTTPException(409, "batch has no active approval node")
    if len(nodes) > 1:
        raise HTTPException(422, "multiple approval nodes are active; "
                                 "use the node-level endpoints")
    return nodes[0]


def approve(db: Database, batch_id: int, operator: str, note: str = "",
            delegation_id: int | None = None) -> dict:
    """批准（批次级兼容入口）：定位当前唯一待决节点并投赞成票。

    单节点且法定人数 1 的批次一票即进入 running；多级链上节点满足后若仍有后续
    节点，批次保持 pending_approval 直到全部节点满足。
    """
    operator = _require_operator(operator, "operator")
    batch = _get_batch_or_404(db, batch_id)
    _batch_pending_guard(batch)
    node = _single_active_node(db, batch_id)
    result = decide_node(db, batch_id, node["id"], "approve", operator,
                         delegation_id=delegation_id, note=note)
    return {"result": "approved", "batch_id": batch_id,
            "status": result["batch_status"]}


def reject(db: Database, batch_id: int, operator: str, reason: str,
           note: str = "", delegation_id: int | None = None) -> dict:
    """拒绝（批次级兼容入口）：拒绝当前唯一待决节点，整个批次随之终止。"""
    operator = _require_operator(operator, "operator")
    reason = (reason or "").strip()
    if not reason:
        raise HTTPException(422, "reason must be non-empty when rejecting")
    batch = _get_batch_or_404(db, batch_id)
    _batch_pending_guard(batch)
    node = _single_active_node(db, batch_id)
    result = decide_node(db, batch_id, node["id"], "reject", operator,
                         delegation_id=delegation_id, reason=reason, note=note)
    return {"result": "rejected", "batch_id": batch_id,
            "cancelled_tasks": result["cancelled_tasks"]}


def expire_approvals(db: Database, now: float) -> int:
    """审批超时释放：任一活动节点超过其截止时间仍待决，整个批次取消。

    由 replay worker 每轮在领取任务前调用（也因此可被手动 run_once 触发）。
    批次与节点的条件更新保证与人工决定互斥：谁先提交谁生效，超时不会作用到
    已批准/已决定的批次上；重复扫描不会产生第二次效果。
    """
    due = db.query(
        """SELECT n.id AS node_id, n.batch_id AS batch_id, n.seq AS seq,
                  n.role AS role, n.deadline AS deadline
           FROM replay_approval_nodes n
           JOIN replay_batches b ON b.id = n.batch_id
           WHERE n.status='active' AND n.deadline IS NOT NULL AND n.deadline <= ?
             AND b.status='pending_approval' AND b.approval_status='pending'""",
        (now,),
    )
    for node in due:
        with db.tx() as cur:
            changed = cur.execute(
                """UPDATE replay_batches
                   SET status='cancelled', approval_status='expired',
                       updated_at=?, finished_at=?
                   WHERE id=? AND status='pending_approval'
                     AND approval_status='pending'""",
                (now, now, node["batch_id"]),
            ).rowcount
            if not changed:  # 并发下批次已被人工批准/拒绝/取消
                continue
            cur.execute(
                """UPDATE replay_approval_nodes SET status='expired', decided_at=?
                   WHERE id=? AND status='active'""",
                (now, node["node_id"]),
            )
            cur.execute(
                """UPDATE replay_approval_nodes SET status='cancelled'
                   WHERE batch_id=? AND status IN ('waiting','active')""",
                (node["batch_id"],),
            )
            cancelled_tasks = cur.execute(
                """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
                   finished_at=?, updated_at=?
                   WHERE batch_id=? AND status IN ('pending','processing')""",
                (now, now, node["batch_id"]),
            ).rowcount
            cur.execute(
                "UPDATE replay_batches SET cancelled=cancelled+?, updated_at=? WHERE id=?",
                (cancelled_tasks, now, node["batch_id"]),
            )
            batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                                (node["batch_id"],)).fetchone()
            audit.record(cur, "replay_approval_node_expired", None, None,
                         {"replay_batch_id": node["batch_id"],
                          "node_id": node["node_id"], "seq": node["seq"],
                          "role": node["role"], "deadline": node["deadline"]}, ts=now)
            audit.record(cur, "replay_batch_approval_expired", None, None,
                         {"replay_batch_id": node["batch_id"],
                          "submitted_by": batch["operator"],
                          "risk_level": batch["risk_level"],
                          "approval_deadline": batch["approval_deadline"],
                          "expired_node_id": node["node_id"],
                          "cancelled_tasks": cancelled_tasks}, ts=now)
    return len(due)


def pause(db: Database, batch_id: int, operator: str) -> dict:
    """暂停：未执行的任务不再被 worker 拉取，滞留的重放副作用暂停派发。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] != "running":
            raise HTTPException(409, f"batch is {batch['status']}, cannot pause")
        cur.execute("UPDATE replay_batches SET status='paused', updated_at=? WHERE id=?",
                    (now, batch_id))
        audit.record(cur, "replay_batch_paused", None, None,
                     {"replay_batch_id": batch_id, "operator": operator}, ts=now)
    return {"result": "paused", "batch_id": batch_id}

def resume(db: Database, batch_id: int, operator: str) -> dict:
    """继续：从暂停处恢复，未完成的任务按各自位置继续执行。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] != "paused":
            raise HTTPException(409, f"batch is {batch['status']}, cannot resume")
        cur.execute("UPDATE replay_batches SET status='running', updated_at=? WHERE id=?",
                    (now, batch_id))
        audit.record(cur, "replay_batch_resumed", None, None,
                     {"replay_batch_id": batch_id, "operator": operator}, ts=now)
    return {"result": "resumed", "batch_id": batch_id}


def cancel(db: Database, batch_id: int, operator: str, note: str = "") -> dict:
    """取消：未执行的任务标记 cancelled；正在处理的任务一并标记，其迟到的
    完成/失败结果落库时会被 worker 的条件更新挡下（保持 cancelled，不再写
    完成记录或派发副作用）；已完成任务滞留的待派发副作用同事务取消
    （已派发的外部效果无法撤回，审计里保留完整轨迹）。"""
    now = time.time()
    with db.tx() as cur:
        batch = _get_batch_or_404(db, batch_id)
        if batch["status"] not in ("running", "paused", "pending_approval"):
            raise HTTPException(409, f"batch is {batch['status']}, cannot cancel")
        awaiting_approval = batch["status"] == "pending_approval"
        cancelled_tasks = cur.execute(
            """UPDATE replay_tasks SET status='cancelled', blocked_reason=NULL,
               finished_at=?, updated_at=?
               WHERE batch_id=? AND status IN ('pending','processing')""",
            (now, now, batch_id),
        ).rowcount
        # 待决的审批节点一并关闭（已决定的节点记录保留可查）
        cur.execute(
            """UPDATE replay_approval_nodes SET status='cancelled'
               WHERE batch_id=? AND status IN ('waiting','active')""",
            (batch_id,),
        )
        # 滞留的重放副作用按任务分组取消并记审计（内容保留可查）
        stuck = cur.execute(
            """SELECT t.id AS task_id, t.external_id, t.delivery_id, COUNT(o.id) AS c
               FROM outbox o JOIN replay_tasks t ON t.id = o.replay_task_id
               WHERE t.batch_id=? AND o.status='pending'
               GROUP BY t.id""",
            (batch_id,),
        ).fetchall()
        cur.execute(
            """UPDATE outbox SET status='cancelled'
               WHERE status='pending' AND replay_task_id IN
                     (SELECT id FROM replay_tasks WHERE batch_id=?)""",
            (batch_id,),
        )
        for row in stuck:
            audit.record(cur, "effect_cancelled", row["external_id"], row["delivery_id"],
                         {"replay_batch_id": batch_id, "replay_task_id": row["task_id"],
                          "cancelled": row["c"], "reason": "replay_batch_cancelled"}, ts=now)
        # 条件更新与并发的批准/拒绝/超时互斥（写事务串行 + 状态守卫）；
        # 待审批期间取消时保留 pending 审批轨迹，另在审计里标注撤回
        expected_status = "pending_approval" if awaiting_approval else batch["status"]
        changed = cur.execute(
            """UPDATE replay_batches SET status='cancelled', cancelled=cancelled+?,
               updated_at=?, finished_at=? WHERE id=? AND status=?""",
            (cancelled_tasks, now, now, batch_id, expected_status),
        ).rowcount
        if not changed:  # 并发下批次已被批准/拒绝/超时
            raise HTTPException(409, "batch is no longer in the state read at cancel time")
        audit.record(cur, "replay_batch_cancelled", None, None,
                     {"replay_batch_id": batch_id, "operator": operator, "note": note,
                      "was_awaiting_approval": awaiting_approval,
                      "cancelled_tasks": cancelled_tasks,
                      "done": batch["done"], "failed": batch["failed"]}, ts=now)
    return {"result": "cancelled", "batch_id": batch_id,
            "cancelled_tasks": cancelled_tasks}


def retry_task(db: Database, task_id: int, operator: str) -> dict:
    """失败任务单独重试：重置计数放回队列，只影响这一条，不阻塞其他编号。"""
    now = time.time()
    with db.tx() as cur:
        task = cur.execute("SELECT * FROM replay_tasks WHERE id=?", (task_id,)).fetchone()
        if task is None:
            raise HTTPException(404, "replay task not found")
        if task["status"] != "failed":
            raise HTTPException(409, f"task is {task['status']}, not failed")
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (task["batch_id"],)).fetchone()
        if batch["status"] == "cancelled":
            raise HTTPException(409, "batch is cancelled")
        cur.execute(
            """UPDATE replay_tasks SET status='pending', attempts=0, next_retry_at=NULL,
               last_error=NULL, blocked_reason=NULL, finished_at=NULL, updated_at=? WHERE id=?""",
            (now, task_id),
        )
        # 批次若已因失败收尾，重新打开继续跑
        cur.execute(
            """UPDATE replay_batches SET failed=failed-1, updated_at=?,
               status=CASE WHEN status='completed_with_failures' THEN 'running' ELSE status END,
               finished_at=CASE WHEN status='completed_with_failures' THEN NULL ELSE finished_at END
               WHERE id=?""",
            (now, task["batch_id"]),
        )
        audit.record(cur, "replay_task_retried", task["external_id"], task["delivery_id"],
                     {"replay_batch_id": task["batch_id"], "replay_task_id": task_id,
                      "operator": operator}, ts=now)
    return {"result": "requeued", "task_id": task_id, "batch_id": task["batch_id"]}


# ---- 查询 ------------------------------------------------------------------

# 重放任务的终态：前序版本进入其中之一，同编号的后一条才允许被领取
TERMINAL_TASK_STATUSES = ("done", "failed", "cancelled")


def _version_key(task) -> tuple:
    """同编号历史版本的执行顺序：按落盘时间，再按 delivery_id 决胜（稳定且确定）。"""
    return (task["delivery_created_at"], task["delivery_id"])


def _nearest_open_predecessor(tasks, task):
    """同批次同编号、版本更早且未进终态的最近一条任务；没有则 None（可执行）。"""
    if task["delivery_created_at"] is None:
        return None  # 老库遗留行没有版本时间快照，不参与有序约束
    earlier = [t for t in tasks
               if t["external_id"] == task["external_id"]
               and t["id"] != task["id"]
               and t["delivery_created_at"] is not None
               and _version_key(t) < _version_key(task)
               and t["status"] not in TERMINAL_TASK_STATUSES]
    if not earlier:
        return None
    return max(earlier, key=_version_key)


def _live_blocked_reason(batch, task, predecessor, in_flight: int,
                         now: float) -> str | None:
    """批次详情里每条 pending 任务「为什么还没被执行」的实时原因（None 表示可执行）。

    与 worker 领取闸门的判定顺序一致：批次状态 -> 退避等待 -> 前序版本 -> 并发配额。
    """
    if task["status"] != "pending":
        return None
    if batch["status"] == "pending_approval":
        return "awaiting_approval"
    if batch["status"] == "rejected":
        return "batch_rejected"
    if batch["status"] == "paused":
        return "batch_paused"
    if batch["status"] == "cancelled":
        return "batch_cancelled"
    if batch["status"] != "running":
        return f"batch_not_running:{batch['status']}"
    if task["next_retry_at"] is not None and task["next_retry_at"] > now:
        return "retry_backoff"
    if predecessor is not None:
        return f"waiting_predecessor:{predecessor['id']}"
    maxc = batch["max_concurrency"]
    if maxc is not None and in_flight >= maxc:
        return f"quota_exhausted:{in_flight}/{maxc}"
    return None


def _vote_row(r) -> dict:
    return {
        "vote": r["vote"], "voter": r["voter"], "voter_role": r["voter_role"],
        "delegation_id": r["delegation_id"], "status": r["status"],
        "reason": r["reason"], "note": r["note"], "created_at": r["created_at"],
        "invalidated_at": r["invalidated_at"],
    }


def _node_votes(db: Database, node_id: int) -> list[dict]:
    return [_vote_row(r) for r in db.query(
        "SELECT * FROM replay_node_votes WHERE node_id=? ORDER BY id", (node_id,))]


def _current_delegations(db: Database, allowed_roles: list[str],
                         now: float) -> list[dict]:
    """节点允许角色当前有效的委托（生效/失效时间窗覆盖 now 且未撤销）。"""
    designated = [r for r in allowed_roles if r != ROLE_ANY]
    if not designated:
        return []  # 'any' 节点任何非发起人都可承担，没有「角色委托」概念
    marks = ",".join("?" * len(designated))
    rows = db.query(
        f"""SELECT * FROM replay_delegations
            WHERE status='active' AND role IN ({marks})
              AND valid_from<=? AND valid_to>=?
            ORDER BY role, id""",
        (*designated, now, now),
    )
    return [{"id": r["id"], "role": r["role"], "delegatee": r["delegatee"],
             "delegator": r["delegator"], "valid_from": r["valid_from"],
             "valid_to": r["valid_to"], "note": r["note"]} for r in rows]


def _node_view(db: Database, node, now: float) -> dict:
    """单个审批节点的展示：允许角色、法定人数、有效赞成人数与还缺人数、
    每张票（含已失效票及失效原因）、当前有效委托、实际落定人与截止时间。"""
    allowed_roles = node_allowed_roles(node)
    required = node_required(node)
    votes = _node_votes(db, node["id"])
    approved_count = sum(1 for v in votes
                         if v["status"] == "valid" and v["vote"] == "approve")
    return {
        "id": node["id"],
        "seq": node["seq"],
        "role": node["role"],                    # 主指定角色（'any' 表示任何非发起人）
        "allowed_roles": allowed_roles,          # 允许承担该节点的全部角色
        "required_approvals": required,          # 法定人数
        "approved_count": approved_count,        # 当前有效赞成人数
        "missing": max(0, required - approved_count)
                   if node["status"] == NODE_ACTIVE else 0,  # 还缺多少人（待决时）
        "quorum_reached": approved_count >= required,
        "status": node["status"],
        "decided_by": node["decided_by"],        # 节点落定（达法定人数/拒绝/跳过）的决定人
        "decided_role": node["decided_role"],
        "decision_reason": node["decision_reason"],
        "decision_note": node["decision_note"],
        "timeout_seconds": node["timeout_seconds"],
        "activated_at": node["activated_at"],
        "deadline": node["deadline"],
        "decided_at": node["decided_at"],
        "votes": votes,
        # 该节点允许角色当前有效的委托（决定时凭它承担角色）
        "valid_delegations": _current_delegations(db, allowed_roles, now),
        "expired_on_time": (node["status"] == NODE_ACTIVE
                            and node["deadline"] is not None
                            and node["deadline"] <= now),
    }


def _approval_view(db: Database, row, now: float | None = None) -> dict:
    """批次当前审批状态与操作者：发起人、审批人、决定时间/原因、策略版本、
    各审批节点（允许角色/法定人数/有效赞成人数/有效委托/还缺多少人/每张票）、
    当前待决节点、剩余节点与超时状态。

    approved/可执行的前提是 approval_status='approved'；expired_on_time 只用于
    详情提示——状态转移以 worker 下一轮扫描（或手动 run_once）为准。
    """
    now = time.time() if now is None else now
    nodes = db.query(
        "SELECT * FROM replay_approval_nodes WHERE batch_id=? ORDER BY seq",
        (row["id"],),
    )
    node_views = []
    for n in nodes:
        votes = [_vote_row(r) for r in db.query(
            "SELECT * FROM replay_node_votes WHERE node_id=? ORDER BY id", (n["id"],))]
        allowed_roles = node_allowed_roles(n)
        required = node_required(n)
        approved_count = sum(1 for v in votes
                             if v["status"] == "valid" and v["vote"] == "approve")
        node_views.append({
            "id": n["id"],
            "seq": n["seq"],
            "role": n["role"],
            "allowed_roles": allowed_roles,
            "required_approvals": required,
            "approved_count": approved_count,
            "missing": max(0, required - approved_count)
                       if n["status"] == NODE_ACTIVE else 0,
            "quorum_reached": approved_count >= required,
            "status": n["status"],
            "decided_by": n["decided_by"],
            "decided_role": n["decided_role"],
            "decision_reason": n["decision_reason"],
            "decision_note": n["decision_note"],
            "timeout_seconds": n["timeout_seconds"],
            "activated_at": n["activated_at"],
            "deadline": n["deadline"],
            "decided_at": n["decided_at"],
            "votes": votes,
            "valid_delegations": _current_delegations(db, allowed_roles, now),
            "expired_on_time": (n["status"] == NODE_ACTIVE
                                and n["deadline"] is not None
                                and n["deadline"] <= now),
        })
    view = {
        "risk_level": row["risk_level"],
        "approval_note": row["approval_note"],
        "status": row["approval_status"],
        "submitted_by": row["operator"],
        "approver": row["approver"],
        "approved_at": row["approved_at"],
        "rejection_reason": row["approval_reason"],
        "deadline": row["approval_deadline"],
        "expired_on_time": (
            row["status"] == BATCH_PENDING_APPROVAL
            and row["approval_status"] == APPROVAL_PENDING
            and row["approval_deadline"] is not None
            and row["approval_deadline"] <= now),
        "policy_version": row["policy_version"],
        "nodes": node_views,
        # 当前待决节点 / 剩余节点（待决 + 尚未轮到的串行节点）
        "current_node_ids": [n["id"] for n in nodes if n["status"] == NODE_ACTIVE],
        "remaining_node_ids": [n["id"] for n in nodes if n["status"] in NODE_UNDECIDED],
        # 批次级汇总：还缺多少张有效赞成票（仅统计待决节点）
        "missing_approvals": sum(v["missing"] for v in node_views),
    }
    return view


def _batch_view(row) -> dict:
    out = {k: row[k] for k in row.keys()}
    out["filters"] = json.loads(out["filters"])
    out["policy_snapshot"] = (json.loads(out["policy_snapshot"])
                              if out["policy_snapshot"] else None)
    return out


def batch_detail(db: Database, batch_id: int) -> dict:
    """批次详情：进度计数、并发占用/等待数量、审批状态/操作者、每条任务状态与
    实时阻塞原因。"""
    batch = _get_batch_or_404(db, batch_id)
    tasks = db.query("SELECT * FROM replay_tasks WHERE batch_id=? ORDER BY id", (batch_id,))
    now = time.time()
    in_flight = sum(1 for t in tasks if t["status"] == "processing")
    waiting = sum(1 for t in tasks if t["status"] == "pending")
    views = []
    for t in tasks:
        v = {k: t[k] for k in t.keys()}
        # 实时计算的阻塞原因覆盖库里 worker 维护的最近值（可能滞后一个轮询周期）
        v["blocked_reason"] = _live_blocked_reason(
            batch, t, _nearest_open_predecessor(tasks, t), in_flight, now)
        views.append(v)
    out = _batch_view(batch)
    out["in_flight"] = in_flight  # 当前占用：正在处理的任务数（并发配额的占用量）
    out["waiting"] = waiting      # 等待数量：尚未进入执行的任务数
    out["approval"] = _approval_view(db, batch, now)  # 当前审批状态、节点与操作者
    return {"batch": out, "tasks": views}


def batch_events(db: Database, batch_id: int, task_id: int | None, limit: int) -> dict:
    """一次重放的完整审计记录：该批次（可选单条任务）的全部事件，按时间正序。"""
    _get_batch_or_404(db, batch_id)
    sql = "SELECT * FROM events WHERE json_extract(detail,'$.replay_batch_id')=?"
    params: list = [batch_id]
    if task_id is not None:
        sql += " AND json_extract(detail,'$.replay_task_id')=?"
        params.append(task_id)
    sql += " ORDER BY id LIMIT ?"
    params.append(limit)
    events = []
    for r in db.query(sql, tuple(params)):
        d = {k: r[k] for k in r.keys()}
        d["detail"] = json.loads(d["detail"])
        events.append(d)
    return {"events": events}


# ---- 重放 worker ------------------------------------------------------------

class ReplayWorker:
    """逐条执行重放任务；与主 worker 相同的重试/退避语义，任务级隔离不互相阻塞。

    每轮先扫审批超时（到期未决的高风险批次整体取消），再走领取闸门——
    _process_one 的占位事务统一复核：批次在跑且审批已通过（或无需审批） ->
    同编号前序版本已进终态 -> 批次并发配额未满，满足才占位执行；
    被挡下的任务记录阻塞原因（状态展示 + 审计），下一轮换到槽位/前序终态/
    审批通过后自动放行。
    """

    def __init__(self, db: Database, settings: Settings, handler=business_handler,
                 clock=time.time):
        self.db = db
        self.settings = settings
        self.handler = handler
        self.clock = clock
        self._stop = asyncio.Event()

    async def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception:
                log.exception("replay worker iteration failed")
            try:
                await asyncio.wait_for(self._stop.wait(), self.settings.worker_poll_interval)
            except asyncio.TimeoutError:
                pass

    def stop(self):
        self._stop.set()

    def recover(self) -> int:
        """启动恢复：上次崩溃时卡在 processing 的任务退回 pending，从上次位置继续
        （attempts/checkpoint/next_retry_at 都在库里；副作用靠幂等键去重不会重发）。
        这些任务占用的批次并发配额随状态离开 processing 自动释放，无需额外回收。"""
        now = self.clock()
        with self.db.tx() as cur:
            rows = cur.execute(
                "SELECT * FROM replay_tasks WHERE status='processing'").fetchall()
            for t in rows:
                cur.execute(
                    "UPDATE replay_tasks SET status='pending', updated_at=? WHERE id=?",
                    (now, t["id"]),
                )
                audit.record(cur, "replay_task_recovered", t["external_id"],
                             t["delivery_id"],
                             {"replay_batch_id": t["batch_id"], "replay_task_id": t["id"],
                              "attempts": t["attempts"]}, ts=now)
        return len(rows)

    def run_once(self):
        now = self.clock()
        # 先处理委托到期：到期委托投在未满足节点上的赞成票失效（节点重新等待
        # 法定人数），必须先于审批超时与领取，保证本轮门禁基于最新有效票数
        delegation_mod.expire_delegations(self.db, now)
        # 再处理审批超时：到期仍未满足节点法定人数的批次整体取消（释放占住的任务），
        # 必须先于领取，保证超时批次的任务本轮绝不会被领取
        expire_approvals(self.db, now)
        # 待审批批次的 pending 任务也取出交给领取闸门：闸门会以 awaiting_approval
        # 挡下（原因变化才写审计，轮询不刷表）；running 且审批通过/无需审批的才领取
        rows = self.db.query(
            """SELECT t.* FROM replay_tasks t
               JOIN replay_batches b ON b.id = t.batch_id
               WHERE t.status='pending'
                 AND b.status IN ('running','pending_approval')
                 AND (t.next_retry_at IS NULL OR t.next_retry_at <= ?)
               ORDER BY t.id LIMIT 100""",
            (now,),
        )
        for row in rows:
            self._process_one(row)

    @staticmethod
    def _open_predecessor(cur: sqlite3.Cursor, task):
        """同批次同编号、版本更早且未进终态的最近一条任务；没有则 None（可领取）。"""
        if task["delivery_created_at"] is None:
            return None  # 老库遗留行没有版本时间快照，不参与有序约束
        return cur.execute(
            """SELECT id, status FROM replay_tasks
               WHERE batch_id=? AND external_id=? AND id<>?
                 AND delivery_created_at IS NOT NULL
                 AND (delivery_created_at < ?
                      OR (delivery_created_at = ? AND delivery_id < ?))
                 AND status NOT IN ('done','failed','cancelled')
               ORDER BY delivery_created_at DESC, delivery_id DESC LIMIT 1""",
            (task["batch_id"], task["external_id"], task["id"],
             task["delivery_created_at"], task["delivery_created_at"],
             task["delivery_id"]),
        ).fetchone()

    def _mark_blocked(self, cur: sqlite3.Cursor, task, reason: str, now: float,
                      **detail):
        """记录任务被领取闸门挡下的原因：仅在原因变化时更新并写审计，
        轮询期间原因不变不重复刷事件；被领取（或取消/重试）时该字段清空。"""
        if task["blocked_reason"] == reason:
            return
        cur.execute(
            "UPDATE replay_tasks SET blocked_reason=?, updated_at=? WHERE id=?",
            (reason, now, task["id"]),
        )
        audit.record(cur, "replay_task_blocked", task["external_id"],
                     task["delivery_id"],
                     {"replay_batch_id": task["batch_id"],
                      "replay_task_id": task["id"],
                      "reason": reason,
                      "previous_reason": task["blocked_reason"],
                      **detail}, ts=now)

    def _process_one(self, task):
        now = self.clock()
        task_id = task["id"]
        # 领取闸门：占位事务里原子复核——拉取之后批次可能已被暂停/取消，或任务被
        # 人工改动；同编号前序版本未进终态、批次并发配额已满时都不得领取。
        # BEGIN IMMEDIATE 串行化所有写事务，检查与占位之间没有竞态。
        with self.db.tx() as cur:
            current = cur.execute(
                """SELECT t.*, b.status AS batch_status,
                          b.max_concurrency AS batch_max_concurrency,
                          b.approval_status AS batch_approval_status
                   FROM replay_tasks t JOIN replay_batches b ON b.id = t.batch_id
                   WHERE t.id=?""",
                (task_id,),
            ).fetchone()
            if current is None or current["status"] != "pending":
                return
            # 审批闸门：待审批（含已过截止点但尚未被扫描释放）的高风险批次不得领取，
            # 原因变化时记一次 blocked 审计；running 只可能来自普通批次（not_required）
            # 或已被非发起人明确批准（approved）的高风险批次；其余批次状态不再处理
            if current["batch_status"] == BATCH_PENDING_APPROVAL:
                self._mark_blocked(cur, current, "awaiting_approval", now)
                return
            if current["batch_status"] != "running":
                return
            if current["batch_approval_status"] not in (
                    APPROVAL_NOT_REQUIRED, APPROVAL_APPROVED):
                self._mark_blocked(cur, current, "awaiting_approval", now)
                return
            # 同一编号有序执行：存在版本更早且未进终态的前序任务 -> 不可领取
            predecessor = self._open_predecessor(cur, current)
            if predecessor is not None:
                self._mark_blocked(
                    cur, current, f"waiting_predecessor:{predecessor['id']}", now,
                    predecessor_task_id=predecessor["id"],
                    predecessor_status=predecessor["status"])
                return
            # 批次级并发配额：占用 = 本批 processing 中的任务数（实时推导，
            # 任务离开 processing 即释放，不存在需要单独回收的计数器）
            maxc = current["batch_max_concurrency"]
            if maxc is not None:
                in_flight = cur.execute(
                    "SELECT COUNT(*) AS c FROM replay_tasks "
                    "WHERE batch_id=? AND status='processing'",
                    (current["batch_id"],),
                ).fetchone()["c"]
                if in_flight >= maxc:
                    self._mark_blocked(
                        cur, current, f"quota_exhausted:{in_flight}/{maxc}", now,
                        in_flight=in_flight, max_concurrency=maxc)
                    return
            claimed = cur.execute(
                """UPDATE replay_tasks SET status='processing', attempts=attempts+1,
                   blocked_reason=NULL, updated_at=?
                   WHERE id=? AND status='pending'""",
                (now, task_id),
            ).rowcount
            if not claimed:
                return
            attempt = current["attempts"] + 1
            audit.record(cur, "replay_task_processing", current["external_id"],
                         current["delivery_id"],
                         {"replay_batch_id": current["batch_id"],
                          "replay_task_id": task_id,
                          "attempt": attempt}, ts=now)

        delivery = self.db.query_one(
            "SELECT * FROM deliveries WHERE id=?", (task["delivery_id"],))
        try:
            result = self.handler(delivery)
        except Exception as exc:  # noqa: BLE001 - 任何业务异常都走重试/失败
            self._handle_failure(task, attempt, exc, self.clock())
            return

        now = self.clock()
        with self.db.tx() as cur:
            # 条件更新落终态：handler 执行期间批次可能已被取消（任务 processing->
            # cancelled）。更新不到说明任务已不属于本次执行——保持 cancelled，
            # 丢弃迟到的结果：不落副作用、不写完成记录、不计进度
            finalized = cur.execute(
                """UPDATE replay_tasks SET status='done', checkpoint=?, next_retry_at=NULL,
                   finished_at=?, updated_at=? WHERE id=? AND status='processing'""",
                (json.dumps(result.get("checkpoint") or {}, ensure_ascii=False),
                 now, now, task_id),
            ).rowcount
            if not finalized:
                current = cur.execute("SELECT status FROM replay_tasks WHERE id=?",
                                      (task_id,)).fetchone()
                audit.record(cur, "replay_task_completion_discarded",
                             task["external_id"], task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "task_status": current["status"] if current else None},
                             ts=now)
                return
            for effect in result.get("effects", []):
                key = replay_effect_key(task_id, effect["type"], effect["payload"])
                cur.execute(
                    """INSERT OR IGNORE INTO outbox
                       (delivery_id, replay_task_id, effect_type, idempotency_key,
                        payload, created_at)
                       VALUES (?,?,?,?,?,?)""",
                    (task["delivery_id"], task_id, effect["type"], key,
                     json.dumps(effect["payload"], ensure_ascii=False, sort_keys=True), now),
                )
            audit.record(cur, "replay_task_done", task["external_id"], task["delivery_id"],
                         {"replay_batch_id": task["batch_id"], "replay_task_id": task_id,
                          "attempt": attempt,
                          "effects": len(result.get("effects", [])),
                          "checkpoint": result.get("checkpoint")}, ts=now)
            self._bump_and_maybe_finish(cur, task["batch_id"], "done", now)

    def _handle_failure(self, task, attempt: int, exc: Exception, now: float):
        task_id = task["id"]
        with self.db.tx() as cur:
            # 与成功收尾同一守卫：处理期间被取消的任务保持 cancelled，
            # 不标记失败、不安排重试（否则会把已取消的任务复活回队列）
            still_processing = cur.execute(
                "SELECT 1 AS x FROM replay_tasks WHERE id=? AND status='processing'",
                (task_id,),
            ).fetchone()
            if still_processing is None:
                audit.record(cur, "replay_task_completion_discarded",
                             task["external_id"], task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "error": str(exc)}, ts=now)
                return
            if attempt >= self.settings.max_attempts:
                # 连续失败 -> 标记 failed，只影响这一条，其他编号照常执行
                cur.execute(
                    """UPDATE replay_tasks SET status='failed', next_retry_at=NULL,
                       last_error=?, finished_at=?, updated_at=? WHERE id=?""",
                    (str(exc), now, now, task_id),
                )
                audit.record(cur, "replay_task_failed", task["external_id"],
                             task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id,
                              "attempts": attempt, "error": str(exc)}, ts=now)
                self._bump_and_maybe_finish(cur, task["batch_id"], "failed", now)
            else:
                delay = min(
                    self.settings.retry_base_seconds * (2 ** (attempt - 1)),
                    self.settings.retry_cap_seconds,
                )
                cur.execute(
                    """UPDATE replay_tasks SET status='pending', next_retry_at=?,
                       last_error=?, updated_at=? WHERE id=?""",
                    (now + delay, str(exc), now, task_id),
                )
                audit.record(cur, "replay_task_retry_scheduled", task["external_id"],
                             task["delivery_id"],
                             {"replay_batch_id": task["batch_id"],
                              "replay_task_id": task_id, "attempt": attempt,
                              "delay_seconds": delay, "error": str(exc)}, ts=now)

    def _bump_and_maybe_finish(self, cur: sqlite3.Cursor, batch_id: int,
                               counter: str, now: float):
        """进度计数与任务终态同事务更新；批次内无未完成任务时收尾。"""
        cur.execute(
            f"UPDATE replay_batches SET {counter}={counter}+1, updated_at=? WHERE id=?",
            (now, batch_id),
        )
        remaining = cur.execute(
            "SELECT COUNT(*) AS c FROM replay_tasks WHERE batch_id=? "
            "AND status IN ('pending','processing')",
            (batch_id,),
        ).fetchone()["c"]
        if remaining:
            return
        batch = cur.execute("SELECT * FROM replay_batches WHERE id=?",
                            (batch_id,)).fetchone()
        if batch["status"] != "running":
            return
        final = "completed" if batch["failed"] == 0 else "completed_with_failures"
        cur.execute(
            "UPDATE replay_batches SET status=?, finished_at=?, updated_at=? WHERE id=?",
            (final, now, now, batch_id),
        )
        audit.record(cur, "replay_batch_completed", None, None,
                     {"replay_batch_id": batch_id, "result": final,
                      "total": batch["total"], "done": batch["done"],
                      "failed": batch["failed"], "cancelled": batch["cancelled"]}, ts=now)


# ---- 路由 ------------------------------------------------------------------

def create_replay_router(db: Database, settings: Settings) -> APIRouter:
    router = APIRouter(prefix="/admin/replays", tags=["replays"])

    @router.post("/preview")
    def preview_endpoint(req: PreviewRequest):
        """预览将要重放的内容和影响范围（历史副作用），不落任何数据。"""
        return preview(db, req)

    @router.post("")
    def submit_endpoint(req: SubmitRequest):
        """一次性提交一批重放任务（批次 + 任务单事务落盘）。

        高风险批次（risk_level=high 且带 approval_note）进入 pending_approval，
        待非发起人批准后才运行。
        """
        status, body = submit(db, req, settings.replay_approval_timeout_seconds)
        return JSONResponse(status_code=status, content=body)

    @router.get("")
    def list_batches(status: str | None = None, limit: int = Query(100, le=1000)):
        sql, params = "SELECT * FROM replay_batches", []
        if status:
            sql += " WHERE status=?"
            params.append(status)
        sql += " ORDER BY id DESC LIMIT ?"
        params.append(limit)
        return {"batches": [_batch_view(r) for r in db.query(sql, tuple(params))]}

    @router.get("/{batch_id}")
    def get_batch(batch_id: int):
        """批次详情：发起人、原因、筛选快照、审批状态与操作者、进度计数
        + 每条任务的状态。"""
        return batch_detail(db, batch_id)

    @router.get("/{batch_id}/events")
    def get_batch_events(batch_id: int, task_id: int | None = None,
                         limit: int = Query(500, le=2000)):
        """该批次（可选单条任务）的完整审计记录，按时间正序。"""
        return batch_events(db, batch_id, task_id, limit)

    @router.post("/{batch_id}/approve")
    def approve_endpoint(batch_id: int, req: ApprovalRequest):
        """批准（批次级兼容入口）：向当前唯一待决节点投赞成票；全部节点满足后批次进入 running。"""
        return approve(db, batch_id, req.operator, req.note, req.delegation_id)

    @router.post("/{batch_id}/reject")
    def reject_endpoint(batch_id: int, req: RejectionRequest):
        """拒绝（批次级兼容入口，拒绝原因必填）：拒绝当前唯一待决节点，批次整体终止。"""
        return reject(db, batch_id, req.operator, req.reason, req.note,
                      req.delegation_id)

    @router.post("/{batch_id}/nodes/{node_id}/approve")
    def approve_node_endpoint(batch_id: int, node_id: int, req: NodeDecisionRequest):
        """对指定审批节点投赞成票：审批人须非发起人、未在本批其他节点持有效票，
        承担指定角色须凭本人当前有效的委托（delegation_id）；有效赞成票达到
        节点法定人数后节点才满足。"""
        return decide_node(db, batch_id, node_id, "approve", req.operator,
                           req.role, req.delegation_id, note=req.note)

    @router.post("/{batch_id}/nodes/{node_id}/reject")
    def reject_node_endpoint(batch_id: int, node_id: int, req: NodeRejectionRequest):
        """拒绝指定审批节点（原因必填）：任一节点拒绝即终止整个批次。"""
        return decide_node(db, batch_id, node_id, "reject", req.operator,
                           req.role, req.delegation_id, req.reason, req.note)

    @router.post("/{batch_id}/nodes/{node_id}/skip")
    def skip_node_endpoint(batch_id: int, node_id: int, req: NodeSkipRequest):
        """跳过指定审批节点（原因必填，留痕可追溯）：该节点视为已满足，审批链继续推进。"""
        return decide_node(db, batch_id, node_id, "skip", req.operator,
                           req.role, req.delegation_id, req.reason, req.note)

    @router.post("/{batch_id}/pause")
    def pause_endpoint(batch_id: int, req: BatchActionRequest):
        return pause(db, batch_id, req.operator)

    @router.post("/{batch_id}/resume")
    def resume_endpoint(batch_id: int, req: BatchActionRequest):
        return resume(db, batch_id, req.operator)

    @router.post("/{batch_id}/cancel")
    def cancel_endpoint(batch_id: int, req: BatchActionRequest):
        return cancel(db, batch_id, req.operator, req.note)

    @router.post("/tasks/{task_id}/retry")
    def retry_task_endpoint(task_id: int, req: BatchActionRequest):
        """失败任务单独重试（只影响这一条）。"""
        return retry_task(db, task_id, req.operator)

    return router

"""编排模块 HTTP 端点。

运营：
- POST /admin/orchestration/graphs/{process_type}           发布事件依赖图（新版本）
- POST /admin/orchestration/graphs/{process_type}/rollback   回滚当前指针
- GET  /admin/orchestration/graphs/{process_type}/versions

回调入口：
- POST /orchestration/callbacks/{process_type}    提交回调（自动建实例/入位/级联）

管理员：
- GET  /admin/orchestration/instances/{key}        五档节点视图+采用版本+阻塞原因+依赖路径
- POST /admin/orchestration/instances/{key}/nodes/{node}/resolve   选定冲突版本
- POST /admin/orchestration/instances/{key}/nodes/{node}/retry     重试阻塞节点
- POST /admin/orchestration/instances/{key}/skip   跳过可跳过节点
- POST /admin/orchestration/instances/{key}/terminate  终止实例
- GET  /admin/orchestration/missing-events         缺失事件异常查询
- GET  /admin/orchestration/callbacks/orphans      待核对（关联键错误）回调
- POST /admin/orchestration/callbacks/{id}/reassociate  重新关联（保留归属历史）
- GET  /admin/orchestration/effects、/audit
"""
from __future__ import annotations

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from ..db import Database
from .engine import (
    ConflictStateError, NotFoundError, OrchestrationEngine, OrchestrationError,
)


class PublishBody(BaseModel):
    spec: dict
    operator: str = "ops"
    note: str | None = None


class RollbackBody(BaseModel):
    version: int
    operator: str = "ops"
    reason: str = ""


class StartInstanceBody(BaseModel):
    process_type: str
    correlation_key: str
    operator: str = "system"


class ReassociateBody(BaseModel):
    instance_key: str
    node: str
    operator: str = "ops"
    note: str | None = None


class SkipBody(BaseModel):
    node: str
    operator: str = "ops"
    reason: str


class TerminateBody(BaseModel):
    operator: str = "ops"
    reason: str


class RetryBody(BaseModel):
    operator: str = "ops"
    force: bool = False


class ResolveBody(BaseModel):
    callback_id: int
    operator: str = "ops"
    note: str | None = None


def _error(exc: Exception, status: int = 400):
    return JSONResponse(status_code=status,
                        content={"error": type(exc).__name__, "detail": str(exc)})


def create_orchestration_router(db: Database, engine: OrchestrationEngine) -> APIRouter:
    router = APIRouter(tags=["orchestration"])

    # ---- 回调入口（与既有 /callbacks 平级；真实部署在网关层统一验签）---------

    @router.post("/orchestration/callbacks/{process_type}")
    async def receive(process_type: str, request: Request):
        body = (await request.body()).decode("utf-8")
        uid = request.headers.get("x-callback-uid")
        try:
            result = engine.ingest(process_type, body, callback_uid=uid)
        except NotFoundError as exc:
            return _error(exc, 404)
        except OrchestrationError as exc:
            return _error(exc, 400)
        code = {"received": 202, "duplicate": 200, "orphan": 202, "late": 200}
        return JSONResponse(status_code=code[result["result"]], content=result)

    # ---- 图发布/回滚 -------------------------------------------------------

    @router.post("/admin/orchestration/graphs/{process_type}")
    def publish(process_type: str, body: PublishBody):
        try:
            return engine.publish_graph(process_type, body.spec,
                                        operator=body.operator, note=body.note)
        except OrchestrationError as exc:
            # GraphValidationError 的 detail 是 JSON 数组字符串
            return _error(exc, 422)

    @router.post("/admin/orchestration/graphs/{process_type}/rollback")
    def rollback(process_type: str, body: RollbackBody):
        try:
            return engine.rollback_graph(process_type, body.version,
                                         operator=body.operator, reason=body.reason)
        except NotFoundError as exc:
            return _error(exc, 404)

    @router.get("/admin/orchestration/graphs/{process_type}/versions")
    def versions(process_type: str):
        return {"process_type": process_type,
                "versions": engine.list_graph_versions(process_type)}

    @router.post("/admin/orchestration/instances")
    def start_instance(body: StartInstanceBody):
        """显式开启流程实例（固定当前图版本）；之后同关联键的乱序回调都挂载到它。"""
        try:
            return engine.start_instance(body.process_type, body.correlation_key,
                                         operator=body.operator)
        except NotFoundError as exc:
            return _error(exc, 404)

    # ---- 实例视图与人工处置 -------------------------------------------------

    @router.get("/admin/orchestration/instances/{instance_key}")
    def instance_view(instance_key: str):
        try:
            return engine.get_instance_view(instance_key)
        except NotFoundError as exc:
            return _error(exc, 404)

    @router.post("/admin/orchestration/instances/{instance_key}/nodes/{node}/resolve")
    def resolve(instance_key: str, node: str, body: ResolveBody):
        try:
            return engine.resolve_conflict(
                instance_key, node, body.callback_id,
                operator=body.operator, note=body.note)
        except NotFoundError as exc:
            return _error(exc, 404)
        except ConflictStateError as exc:
            return _error(exc, 409)

    @router.post("/admin/orchestration/instances/{instance_key}/nodes/{node}/retry")
    def retry_node(instance_key: str, node: str, body: RetryBody):
        try:
            return engine.retry_blocked(instance_key, node, operator=body.operator,
                                        force=body.force)
        except NotFoundError as exc:
            return _error(exc, 404)

    @router.post("/admin/orchestration/instances/{instance_key}/skip")
    def skip(instance_key: str, body: SkipBody):
        try:
            return engine.skip_node(instance_key, body.node, operator=body.operator,
                                    reason=body.reason)
        except NotFoundError as exc:
            return _error(exc, 404)
        except ConflictStateError as exc:
            return _error(exc, 409)

    @router.post("/admin/orchestration/instances/{instance_key}/terminate")
    def terminate(instance_key: str, body: TerminateBody):
        return engine.terminate_instance(instance_key, operator=body.operator,
                                         reason=body.reason)

    # ---- 缺失事件异常 / orphan / 副作用 / 审计 -------------------------------

    @router.get("/admin/orchestration/missing-events")
    def missing_events(status: str | None = None, process_type: str | None = None):
        return {"missing_events":
                engine.list_missing_events(status=status, process_type=process_type)}

    @router.get("/admin/orchestration/callbacks/orphans")
    def orphans():
        return {"callbacks": engine.list_orphan_callbacks()}

    @router.post("/admin/orchestration/callbacks/{callback_id}/reassociate")
    def reassociate(callback_id: int, body: ReassociateBody):
        try:
            return engine.reassociate_callback(
                callback_id, body.instance_key, body.node,
                operator=body.operator, note=body.note)
        except NotFoundError as exc:
            return _error(exc, 404)
        except ConflictStateError as exc:
            return _error(exc, 409)

    @router.get("/admin/orchestration/effects")
    def effects(instance_key: str | None = None):
        return {"effects": engine.list_effects(instance_key)}

    @router.get("/admin/orchestration/audit")
    def audit(event_type: str | None = None, limit: int = 200):
        return {"events": engine.global_audit(event_type=event_type, limit=limit)}

    return router

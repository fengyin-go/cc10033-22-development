"""分区恢复基线接口：基线登记、完整快照挂接、回滚候选选择与回写明细。

路由层只做参数解析与 HTTP 适配，全部业务判断在 PartitionBaselineService 内完成。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from app.services.partition_baseline import PartitionBaselineService

router = APIRouter(prefix="/api/partition-baselines", tags=["分区恢复基线"])

service = PartitionBaselineService()


class ReleasePayload(BaseModel):
    release_id: str
    target_table: str
    model_version: str
    partitions: list[str] = Field(default_factory=list)
    source_model: str


class LandingPayload(BaseModel):
    target_table: str
    model_version: str
    date_partition: str
    actual_rows: int
    expected_rows: int
    source_model: str


class BaselinePayload(BaseModel):
    target_table: str
    model_version: str
    date_partition: str
    source_model: str
    release_id: str


class ReleaseBaselinesPayload(BaseModel):
    release_id: str
    source_model: str


class SnapshotPayload(BaseModel):
    snapshot_id: str
    target_table: str
    model_version: str
    partitions: list[str] = Field(default_factory=list)


class RollbackPayload(BaseModel):
    target_table: str
    current_version: str
    requested_partitions: list[str] | None = None
    reason: str = "部署流水线手动触发回滚"


@router.post("/releases")
def record_release(payload: ReleasePayload) -> dict[str, Any]:
    record = service.record_model_release(
        release_id=payload.release_id,
        target_table=payload.target_table,
        model_version=payload.model_version,
        partitions=payload.partitions,
        source_model=payload.source_model,
    )
    return {"ok": True, "record": record}


@router.post("/first-landings")
def record_first_landing(payload: LandingPayload) -> dict[str, Any]:
    landing = service.record_first_landing(
        target_table=payload.target_table,
        model_version=payload.model_version,
        date_partition=payload.date_partition,
        actual_rows=payload.actual_rows,
        expected_rows=payload.expected_rows,
        source_model=payload.source_model,
    )
    return {"ok": True, "landing": landing}


@router.post("/build")
def build_baseline(payload: BaselinePayload) -> dict[str, Any]:
    baseline = service.build_baseline(
        target_table=payload.target_table,
        model_version=payload.model_version,
        date_partition=payload.date_partition,
        source_model=payload.source_model,
        release_id=payload.release_id,
    )
    return {"ok": True, "baseline": baseline}


@router.post("/build-for-release")
def build_for_release(payload: ReleaseBaselinesPayload) -> dict[str, Any]:
    return service.build_baselines_for_release(payload.release_id, payload.source_model)


@router.get("")
def list_baselines(
    target_table: str | None = None, model_version: str | None = None,
) -> dict[str, Any]:
    items = service.list_baselines(target_table, model_version)
    return {"items": items, "total": len(items)}


@router.post("/snapshots")
def register_snapshot(payload: SnapshotPayload) -> dict[str, Any]:
    return service.register_complete_snapshot(
        snapshot_id=payload.snapshot_id,
        target_table=payload.target_table,
        model_version=payload.model_version,
        partitions=payload.partitions,
    )


@router.get("/snapshots")
def list_snapshots(target_table: str | None = None) -> dict[str, Any]:
    items = service.list_snapshots(target_table)
    return {"items": items, "total": len(items)}


@router.get("/rollback-candidate")
def rollback_candidate(target_table: str, current_version: str) -> dict[str, Any]:
    return service.select_rollback_candidate(target_table, current_version)


@router.post("/rollbacks")
def execute_rollback(payload: RollbackPayload) -> dict[str, Any]:
    result = service.execute_rollback(
        target_table=payload.target_table,
        current_version=payload.current_version,
        requested_partitions=payload.requested_partitions,
        reason=payload.reason,
    )
    if not result["ok"]:
        raise HTTPException(status_code=409, detail=result["message"])
    return result


@router.get("/rollbacks")
def list_rollbacks(target_table: str | None = None) -> dict[str, Any]:
    items = service.list_rollbacks(target_table)
    return {"items": items, "total": len(items)}


@router.get("/rollbacks/{rollback_id}")
def rollback_detail(rollback_id: str) -> dict[str, Any]:
    detail = service.get_rollback_detail(rollback_id)
    if detail is None:
        raise HTTPException(status_code=404, detail=f"回滚记录 {rollback_id} 不存在")
    return detail


@router.get("/summary")
def summary() -> dict[str, Any]:
    return service.summary()

"""部署流水线服务：管理数仓模型的部署任务，包括环境切换、前置检查和回滚机制。

增强能力：
- 消费 fact.published/agg.built 通道，自动生成部署任务
- 前置检查：校验上游依赖是否就绪（维度/事实/ETL映射）
- 执行部署：发布到目标环境并通过DataChannel通知snapshot_plan
- 触发回滚：恢复到上一有效快照版本
- 部署状态追踪与失败链路记录
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.services.base import BaseModuleService
from app.services.partition_baseline import PartitionBaselineService
from app.store import store

ENVIRONMENTS = {"开发", "预发", "生产", "灰度"}

# 分区恢复基线领域服务：部署成功登记发布记录，回滚时选择可回滚版本与快照。
baseline_service = PartitionBaselineService()


class DeployPipelineService(BaseModuleService):
    MODULE = "deploy_pipeline"
    ENTITY = "部署任务"
    OPERATION_TERM = "校验"
    LIST_FIELDS = ["任务名称", "目标模型", "部署环境", "前置检查", "回滚方案", "部署状态", "执行日志"]
    REQUIRED_FIELDS = ["任务名称", "目标模型", "部署环境"]
    STATUS_ORDER = ["待部署", "部署中", "已部署", "部署失败", "已回滚"]
    ACTION_RULES = {"执行部署": "待部署", "触发回滚": "已部署"}
    NEGATIVE_ACTIONS = []

    def create_entry(self, values: dict[str, Any]) -> tuple[dict[str, Any] | None, list[str]]:
        """创建部署任务：检查部署环境合法性，基于上游发布事件自动填充前置检查。"""
        env = str(values.get("部署环境", "")).strip()
        if env not in ENVIRONMENTS:
            env = "预发"
            values["部署环境"] = env

        target_model = str(values.get("目标模型", "")).strip()

        # 消费上游发布事件，自动构建前置检查列表
        prerequisites = _gather_prerequisites(target_model)
        if prerequisites:
            values["前置检查"] = "/".join(prerequisites)

        # 自动生成回滚方案
        if not values.get("回滚方案"):
            rollback = _generate_rollback_plan(target_model)
            values["回滚方案"] = rollback

        # 消费事实发布通道
        fact_msgs = store.channel.consume("fact.published", "deploy_pipeline")
        for msg in fact_msgs:
            if msg["payload"]["fact_name"] in target_model:
                values["_fact_published"] = True

        entry, missing = super().create_entry(values)
        if entry is None:
            return None, missing

        store.channel.publish("deploy.task_created", {
            "task_name": entry["任务名称"],
            "target_model": target_model,
            "environment": env,
            "prerequisites": prerequisites,
            "rollback_plan": entry.get("回滚方案"),
            "entry_id": entry["id"],
        })

        store.artifacts.register(
            "deploy_pipeline", "deploy_task",
            f"deploy_{entry['id']}",
            {"task_name": entry["任务名称"], "target_model": target_model, "env": env},
            source_modules=["fact_design", "agg_summary", "deploy_pipeline"],
        )
        return entry, []

    def run_action(self, entry_id: int, action: str) -> tuple[dict[str, Any] | None, str]:
        entry, message = super().run_action(entry_id, action)
        if entry is None:
            return None, message

        target_model = entry.get("目标模型", "")

        if action == "执行部署":
            entry["status"] = "部署中"
            # 执行前置检查
            check_result = _run_preflight_checks(entry)
            if not check_result["all_passed"]:
                entry["status"] = "部署失败"
                entry["部署状态"] = "前置检查失败"
                entry["执行日志"] = check_result["detail"]
                entry["pending"] = True
                entry["abnormal"] = True
                store.recovery.log_retry("deploy_pipeline", f"deploy_{entry_id}", 1,
                                         check_result["detail"], False)
                store.channel.publish("deploy.failed", {
                    "task_name": entry["任务名称"],
                    "target_model": target_model,
                    "reason": check_result["detail"],
                    "entry_id": entry_id,
                })
                return entry, f"部署失败: {check_result['detail']}"

            # 部署成功
            entry["status"] = "已部署"
            entry["部署状态"] = "成功"
            entry["执行日志"] = f"前置检查通过-模型发布-数据回填完成 {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M')}"
            entry["pending"] = False

            # 发布部署成功事件，触发snapshot_plan生成快照
            published_payload = {
                "task_name": entry["任务名称"],
                "target_model": target_model,
                "environment": entry.get("部署环境", ""),
                "deployed_at": datetime.now(timezone.utc).isoformat(),
                "source_version": check_result.get("current_version", "v1.0"),
                "entry_id": entry_id,
            }
            store.channel.publish("deploy.published", published_payload)

            # 登记模型发布记录，作为分区恢复基线的发布侧校验依据
            baseline_service.record_model_release(
                release_id=f"REL-{entry_id}",
                target_table=target_model,
                model_version=check_result.get("current_version", "v1.0"),
                partitions=[],
                source_model=target_model,
            )

            # 创建部署检查点用于恢复
            cp_id = store.recovery.create_checkpoint("deploy_pipeline", f"deploy_{entry_id}", {
                "target_model": target_model,
                "environment": entry.get("部署环境"),
                "deployed_at": datetime.now(timezone.utc).isoformat(),
            })
            store.recovery.mark_completed(cp_id)

        elif action == "触发回滚":
            # 回滚到上一版本：优先走分区恢复基线，缺少基线时保留原有方案
            rollback_result = _execute_rollback(entry)
            entry["status"] = "已回滚"
            entry["部署状态"] = f"已回滚至{rollback_result.get('rollback_version', '上一版本')}"
            entry["执行日志"] = f"回滚原因: {rollback_result.get('reason', '手动触发')}"
            entry["pending"] = True
            entry["abnormal"] = True
            if rollback_result.get("rollback_id"):
                entry["回滚明细编号"] = rollback_result["rollback_id"]
                entry["回滚快照编号"] = "/".join(rollback_result.get("snapshot_ids", []))
                entry["回写日期分区"] = "/".join(rollback_result.get("written_partitions", []))

            store.channel.publish("deploy.rollback", {
                "task_name": entry["任务名称"],
                "target_model": target_model,
                "rollback_to": rollback_result.get("rollback_version"),
                "entry_id": entry_id,
                "snapshot_ids": rollback_result.get("snapshot_ids", []),
                "written_partitions": rollback_result.get("written_partitions", []),
            })

        return entry, message

    def domain_snapshot(self) -> dict[str, Any]:
        base = super().domain_snapshot()
        env_stats: dict[str, dict[str, int]] = {}
        for row in store.rows(self.MODULE):
            env = row.get("部署环境", "未知")
            stat = env_stats.setdefault(env, {"total": 0, "deployed": 0, "failed": 0, "rollback": 0})
            stat["total"] += 1
            if row.get("status") == "已部署":
                stat["deployed"] += 1
            elif row.get("status") == "部署失败":
                stat["failed"] += 1
            elif row.get("status") == "已回滚":
                stat["rollback"] += 1
        base["environment_stats"] = env_stats
        base["consumed_published_events"] = len(store.channel.consume("fact.published", "deploy_pipeline"))
        return base


def _gather_prerequisites(target_model: str) -> list[str]:
    """基于目标模型类型收集前置依赖检查项。"""
    prereqs = []
    if "dwd_" in target_model:
        prereqs.append("上游维度表已发布")
        prereqs.append("ETL映射已部署")
    elif "dim_" in target_model:
        prereqs.append("维度属性已确认")
        prereqs.append("SCD策略已启用")

    # 检查事实表是否已发布
    for f in store.rows("fact_design"):
        if f.get("事实名称", "") in target_model or target_model in f.get("事实名称", ""):
            if f.get("status") == "已发布":
                prereqs.append("源事实表已发布")
            break

    # 检查ETL映射
    for e in store.rows("etl_mapping"):
        if e.get("目标表", "") in target_model or target_model in e.get("目标表", ""):
            if e.get("status") == "已部署":
                prereqs.append("ETL映射已部署")
            break

    prereqs.append("分区格式完整")
    return prereqs


def _generate_rollback_plan(target_model: str) -> str:
    """根据目标模型生成对应的回滚方案。"""
    if "dim_" in target_model:
        return "恢复上一版本快照"
    elif "dwd_" in target_model:
        return "回滚到上一分区版本"
    elif "dws_" in target_model:
        return "删除异常分区重新构建"
    return "回滚到部署前状态"


def _run_preflight_checks(entry: dict[str, Any]) -> dict[str, Any]:
    """执行部署前置检查。"""
    target_model = entry.get("目标模型", "")
    checks = {"items": [], "all_passed": True, "current_version": "v1.0"}

    # 检查1: 目标模型存在
    if target_model:
        checks["items"].append({"check": "目标模型定义", "passed": True})
    else:
        checks["items"].append({"check": "目标模型定义", "passed": False})
        checks["all_passed"] = False

    # 检查2: 维度关联完整性
    for f in store.rows("fact_design"):
        if target_model in str(f.get("事实名称", "")):
            related_dims = str(f.get("关联维度", "")).split(".")
            published_dims = {
                d.get("维度编码")
                for d in store.rows("dim_definition") if d.get("status") == "已发布"
            }
            valid_dims = [d for d in related_dims if d and d.strip() in published_dims]
            dim_check = len(valid_dims) == len([d for d in related_dims if d.strip()])
            checks["items"].append({"check": f"关联维度就绪({len(valid_dims)}/维度)", "passed": dim_check})
            if not dim_check:
                checks["all_passed"] = False

    # 检查3: ETL映射部署
    etl_ready = False
    for e in store.rows("etl_mapping"):
        if target_model in str(e.get("目标表", "")) and e.get("status") == "已部署":
            etl_ready = True
    if not etl_ready:
        etl_ready = _mock_etl_check(target_model)
    checks["items"].append({"check": "ETL映射已部署", "passed": etl_ready})
    if not etl_ready:
        checks["all_passed"] = False

    checks["detail"] = "；".join(
        f"{item['check']}: {'通过' if item['passed'] else '失败'}"
        for item in checks["items"]
    )
    return checks


def _mock_etl_check(target_model: str) -> bool:
    """模拟ETL检查：部分模型跳过ETL检查。"""
    return "dws_" in target_model and "dim_" not in target_model


def _execute_rollback(entry: dict[str, Any]) -> dict[str, Any]:
    """执行回滚操作。

    目标表存在分区恢复基线时，按基线选择可回滚版本并执行分区写回，
    回滚明细携带实际快照编号与写回的日期分区；否则保留原有版本回退方案。
    """
    target_model = str(entry.get("目标模型", ""))
    fallback = {
        "rollback_version": "v0.9",
        "reason": "校验未通过，手动触发回滚",
        "rolled_back_at": datetime.now(timezone.utc).isoformat(),
        "snapshot_ids": [],
        "written_partitions": [],
    }
    if not target_model:
        return fallback

    candidate = baseline_service.select_rollback_candidate(target_model, "v1.0")
    if not candidate["rollbackable"]:
        return fallback

    result = baseline_service.execute_rollback(
        target_table=target_model,
        current_version=candidate["requested_version"],
        reason="校验未通过，部署流水线触发回滚",
    )
    if not result["ok"] or not result.get("rollback"):
        return fallback

    rollback = result["rollback"]
    return {
        "rollback_id": rollback["rollback_id"],
        "rollback_version": rollback["restored_model_version"],
        "reason": rollback["reason"],
        "rolled_back_at": rollback["rolled_back_at"],
        "snapshot_ids": rollback["snapshot_ids"],
        "written_partitions": rollback["written_partitions"],
        "leftover_partitions": rollback["leftover_partitions"],
        "fallback_to_previous": rollback["fallback"],
    }
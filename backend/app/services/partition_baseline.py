"""分区恢复基线服务：为部署流水线建立可按日期分区回滚的数据基线。

领域规则（与部署发布会通知的快照计划配套）：

1. 基线按「目标表 + 模型版本 + 日期分区」三元组识别。
2. 只有模型发布记录覆盖、且目标表首落分区数据完整（实际行数达到预期行数）
   的分区才具备回滚资格；首轮只写了一半的数据不具备回滚资格。
3. 其他模型在同名分区上的数据不能充当本模型的基线（按源模型隔离）。
4. 快照计划登记完整快照后，部署流水线才可按快照标明的表和分区回写；
   新快照尚缺某个日期分区时，该分区不在可回写范围内。
5. 新模型第一份快照的采集与回滚时序冲突时，沿用上一版仍完整的基线。
6. 回滚明细必须列出实际快照编号和写回的日期分区，并区分实际回写的旧分区
   与刚发布版本留下的快照状态。
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from app.store import store


class PartitionBaselineService:
    """分区恢复基线的登记、快照挂接与回写编排，全部状态流转集中在本服务。"""

    def __init__(self, data_store: Any | None = None) -> None:
        self._store = data_store or store

    # ------------------------------------------------------------------
    # 1. 模型发布记录 / 首落分区数据登记
    # ------------------------------------------------------------------

    def record_model_release(
        self,
        release_id: str,
        target_table: str,
        model_version: str,
        partitions: list[str],
        source_model: str,
    ) -> dict[str, Any]:
        """登记模型发布记录：标明本次发布写入的目标表、版本与日期分区。"""
        registry = self._store.partition_baselines
        record = registry.record_release(
            release_id=release_id,
            target_table=target_table,
            model_version=model_version,
            partitions=list(partitions),
        )
        record["source_model"] = source_model
        return record

    def record_first_landing(
        self,
        target_table: str,
        model_version: str,
        date_partition: str,
        *,
        actual_rows: int,
        expected_rows: int,
        source_model: str,
    ) -> dict[str, Any]:
        """登记目标表某分区首次落下的数据，作为基线完整性校验依据。"""
        return self._store.partition_baselines.record_first_landing(
            target_table=target_table,
            model_version=model_version,
            date_partition=date_partition,
            actual_rows=actual_rows,
            expected_rows=expected_rows,
            source_model=source_model,
        )

    # ------------------------------------------------------------------
    # 2. 建立分区恢复基线
    # ------------------------------------------------------------------

    def build_baseline(
        self,
        target_table: str,
        model_version: str,
        date_partition: str,
        *,
        source_model: str,
        release_id: str,
    ) -> dict[str, Any]:
        """用模型发布记录和目标表首落分区校验，建立单分区恢复基线。

        不具备回滚资格的两类情形：
        - 发布记录不存在，或该发布属于其他模型 / 未覆盖此日期分区；
        - 首落分区数据缺失，或实际行数未达到预期（首轮只写了一半）。
        不合规分区仍会留痕，但 eligible=False，不能被回滚选用。
        """
        registry = self._store.partition_baselines
        reasons: list[str] = []

        release = registry.get_release(release_id)
        if release is None:
            reasons.append(f"模型发布记录 {release_id} 不存在")
        else:
            if release["model_version"] != model_version:
                reasons.append(
                    f"发布记录版本 {release['model_version']} 与基线版本 {model_version} 不一致"
                )
            if not _same_table(release["target_table"], target_table):
                reasons.append(
                    f"发布记录目标表 {release['target_table']} 与基线目标表 {target_table} 不一致"
                )
            if date_partition not in release["partitions"]:
                reasons.append(f"发布记录未覆盖日期分区 {date_partition}")
            if release.get("source_model") and release.get("source_model") != source_model:
                reasons.append(
                    f"分区由其他模型 {release.get('source_model')} 写入，"
                    f"不能充当 {source_model} 的回滚基线"
                )

        landing = registry.get_first_landing(target_table, model_version, date_partition)
        if landing is None:
            reasons.append("目标表缺少该日期分区的首落分区数据")
        else:
            if landing["source_model"] != source_model:
                reasons.append(
                    f"首落分区来自其他模型 {landing['source_model']}，同名分区不具备回滚资格"
                )
            if not landing["complete"]:
                reasons.append(
                    f"首轮数据不完整：实际 {landing['actual_rows']} 行 / "
                    f"预期 {landing['expected_rows']} 行"
                )

        eligible = not reasons
        baseline = registry.register_baseline(
            target_table=target_table,
            model_version=model_version,
            date_partition=date_partition,
            metadata={
                "eligible": eligible,
                "source_release": release_id,
                "actual_rows": landing["actual_rows"] if landing else 0,
                "expected_rows": landing["expected_rows"] if landing else 0,
                "reason": "" if eligible else "；".join(reasons),
            },
        )
        # 已存在的基线保持原校验结论，新校验仅补充原因，避免重复登记翻转状态
        if baseline["reason"] == "" and reasons:
            baseline["reason"] = "；".join(reasons)
        return baseline

    def build_baselines_for_release(self, release_id: str, source_model: str) -> dict[str, Any]:
        """按一条模型发布记录批量建立其覆盖分区的恢复基线。"""
        registry = self._store.partition_baselines
        release = registry.get_release(release_id)
        if release is None:
            return {"ok": False, "message": f"模型发布记录 {release_id} 不存在", "baselines": []}

        baselines = [
            self.build_baseline(
                target_table=release["target_table"],
                model_version=release["model_version"],
                date_partition=partition,
                source_model=source_model,
                release_id=release_id,
            )
            for partition in release["partitions"]
        ]
        eligible = [b for b in baselines if b["eligible"]]
        return {
            "ok": True,
            "release_id": release_id,
            "target_table": release["target_table"],
            "model_version": release["model_version"],
            "total": len(baselines),
            "eligible": len(eligible),
            "rejected": len(baselines) - len(eligible),
            "baselines": baselines,
        }

    # ------------------------------------------------------------------
    # 3. 完整快照登记（快照计划侧调用）
    # ------------------------------------------------------------------

    def register_complete_snapshot(
        self,
        snapshot_id: str,
        target_table: str,
        model_version: str,
        partitions: list[str],
    ) -> dict[str, Any]:
        """快照计划采集完整后登记快照，挂接其覆盖的可回滚基线。

        返回缺失分区：已登记基线中、该版本尚未被本快照覆盖的日期分区。
        """
        registry = self._store.partition_baselines
        snapshot = registry.register_snapshot(
            snapshot_id=snapshot_id,
            target_table=target_table,
            model_version=model_version,
            partitions=list(partitions),
        )
        ready = registry.eligible_baselines(target_table, model_version)
        return {
            "ok": True,
            "snapshot_id": snapshot_id,
            "target_table": target_table,
            "model_version": model_version,
            "partitions": list(partitions),
            "missing_partitions": snapshot["missing_partitions"],
            "ready_baselines": len(ready),
            "registered_at": snapshot["registered_at"],
        }

    # ------------------------------------------------------------------
    # 4. 回滚候选版本选择
    # ------------------------------------------------------------------

    def select_rollback_candidate(
        self, target_table: str, current_version: str,
    ) -> dict[str, Any]:
        """为部署流水线选择可回滚版本及其快照基线。

        选择顺序：
        1. 当前版本下已挂接完整快照的基线；
        2. 当前版本快照缺分区（新模型首快照时序冲突）时，回退到上一版完整基线；
        3. 没有任何合格基线时明确返回不可回滚。
        """
        registry = self._store.partition_baselines
        current = registry.eligible_baselines(target_table, current_version)
        if current:
            versions = sorted({b["model_version"] for b in current})
            return {
                "rollbackable": True,
                "fallback": False,
                "target_table": target_table,
                "requested_version": current_version,
                "selected_version": current_version,
                "candidate_versions": versions,
                "baselines": current,
                "reason": "当前版本存在完整快照基线",
            }

        # 上一版仍留有完整基线：按版本号排序取紧邻的旧版本
        all_eligible = registry.eligible_baselines(target_table)
        older = [b for b in all_eligible if b["model_version"] != current_version]
        if older:
            previous_version = sorted({b["model_version"] for b in older})[-1]
            previous = [b for b in older if b["model_version"] == previous_version]
            return {
                "rollbackable": True,
                "fallback": True,
                "target_table": target_table,
                "requested_version": current_version,
                "selected_version": previous_version,
                "candidate_versions": [previous_version],
                "baselines": previous,
                "reason": "新版本首快照采集与回滚时序冲突，沿用在上一版仍完整的基线",
            }

        return {
            "rollbackable": False,
            "fallback": False,
            "target_table": target_table,
            "requested_version": current_version,
            "selected_version": None,
            "candidate_versions": [],
            "baselines": [],
            "reason": "没有具备回滚资格且挂接完整快照的分区基线",
        }

    # ------------------------------------------------------------------
    # 5. 执行回滚写回
    # ------------------------------------------------------------------

    def execute_rollback(
        self,
        target_table: str,
        current_version: str,
        *,
        requested_partitions: list[str] | None = None,
        reason: str = "部署流水线手动触发回滚",
    ) -> dict[str, Any]:
        """按选定基线执行分区回写，返回实际快照编号与写回的日期分区。

        只回写快照标明覆盖、且调用方请求（若指定）的分区；新快照尚缺的分区
        不会被写回，而会在 leftover_partitions 中保留刚发布版本的快照状态。
        """
        registry = self._store.partition_baselines
        candidate = self.select_rollback_candidate(target_table, current_version)
        if not candidate["rollbackable"]:
            return {"ok": False, "message": candidate["reason"], "rollback": None}

        selected_version = candidate["selected_version"]
        baselines = candidate["baselines"]
        requested = set(requested_partitions) if requested_partitions else None

        written_partitions: list[str] = []
        snapshot_ids: set[str] = set()
        skipped_partitions: list[str] = []
        for baseline in baselines:
            partition = baseline["date_partition"]
            if requested is not None and partition not in requested:
                skipped_partitions.append(partition)
                continue
            written_partitions.append(partition)
            snapshot_ids.add(baseline["snapshot_id"])

        if not written_partitions:
            return {
                "ok": False,
                "message": "请求回滚的日期分区均无完整快照基线，未执行任何写回",
                "rollback": None,
            }

        # 刚发布版本在该表上仍登记着的分区快照状态（含缺分区的半成品快照）
        current_snapshots = [
            s for s in registry.list_snapshots(target_table)
            if s["model_version"] == current_version
        ]
        current_snapshot_partitions = sorted({
            p for s in current_snapshots for p in s["partitions"]
        })
        # 当前版本已登记基线的全部分区：含新快照尚缺、未被写回的日期分区
        current_registered_partitions = sorted({
            b["date_partition"] for b in registry.list_baselines(target_table, current_version)
        })
        current_state_partitions = sorted(
            set(current_snapshot_partitions) | set(current_registered_partitions)
        )
        leftover_partitions = [
            p for p in current_state_partitions if p not in written_partitions
        ]

        rollback = registry.add_rollback_record({
            "target_table": target_table,
            "requested_version": current_version,
            "restored_model_version": selected_version,
            "fallback": candidate["fallback"],
            "snapshot_ids": sorted(snapshot_ids),
            "written_partitions": sorted(written_partitions),
            "skipped_partitions": sorted(skipped_partitions),
            "leftover_partitions": leftover_partitions,
            "current_version_snapshot_ids": [s["snapshot_id"] for s in current_snapshots],
            "reason": reason,
            "rolled_back_at": datetime.now(timezone.utc).isoformat(),
        })
        return {"ok": True, "message": "分区回写完成", "rollback": rollback}

    def get_rollback_detail(self, rollback_id: str) -> dict[str, Any] | None:
        """重新进入回滚详情：核对返回的模型版本和持久化的日期分区。"""
        return self._store.partition_baselines.get_rollback_record(rollback_id)

    # ------------------------------------------------------------------
    # 6. 查询视图
    # ------------------------------------------------------------------

    def list_baselines(
        self, target_table: str | None = None, model_version: str | None = None,
    ) -> list[dict[str, Any]]:
        return self._store.partition_baselines.list_baselines(target_table, model_version)

    def list_snapshots(self, target_table: str | None = None) -> list[dict[str, Any]]:
        snapshots = self._store.partition_baselines.list_snapshots(target_table)
        # 对外视图去掉内部集合字段
        return [
            {k: v for k, v in snapshot.items() if k != "covered"}
            for snapshot in snapshots
        ]

    def list_rollbacks(self, target_table: str | None = None) -> list[dict[str, Any]]:
        return self._store.partition_baselines.list_rollback_records(target_table)

    def summary(self) -> dict[str, Any]:
        return self._store.partition_baselines.baseline_summary()


def _same_table(table_a: str, table_b: str) -> bool:
    """两张目标表是否指向同一对象，兼容 dim_* 这类带通配符的登记。"""
    if table_a == table_b:
        return True
    if table_a.endswith("*"):
        return table_b.startswith(table_a[:-1])
    if table_b.endswith("*"):
        return table_a.startswith(table_b[:-1])
    return False

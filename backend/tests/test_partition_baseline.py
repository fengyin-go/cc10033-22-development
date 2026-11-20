"""分区恢复基线业务测试。

覆盖需求中的全部场景：
- 基线按「目标表 + 模型版本 + 日期分区」识别；
- 模型发布记录 + 目标表首落分区校验通过才具备回滚资格；
- 其他模型同名分区、首轮只写一半的数据不具备回滚资格；
- 快照计划登记完整快照后部署流水线才能按标明的表和分区回写；
- 回滚明细列出实际快照编号和写回的日期分区；
- 新模型第一份快照采集与回滚时序冲突时，沿用上一版完整基线；
- 回退后区分实际回写的旧分区与刚发布版本留下的快照状态；
- 同表切换模型版本、新快照尚缺一个日期分区时回滚，重进回滚详情核对版本与持久化分区。
"""
from __future__ import annotations

import pytest

from app.services.partition_baseline import PartitionBaselineService
from app.store import Store

TABLE = "dwd_sales_detail"
MODEL = "sales_model"
PARTITIONS = ["20261117", "20261118", "20261119"]


@pytest.fixture
def svc(store: Store) -> PartitionBaselineService:
    return PartitionBaselineService(store)


@pytest.fixture
def store() -> Store:
    return Store()


def _release_v1(svc: PartitionBaselineService, release_id: str = "REL-V1",
                version: str = "v1.0", partitions=None) -> None:
    svc.record_model_release(
        release_id=release_id,
        target_table=TABLE,
        model_version=version,
        partitions=list(PARTITIONS if partitions is None else partitions),
        source_model=MODEL,
    )


def _land_v1(svc: PartitionBaselineService, version: str = "v1.0",
             partitions=None, *, actual: int = 100, expected: int = 100) -> None:
    for partition in (PARTITIONS if partitions is None else partitions):
        svc.record_first_landing(
            target_table=TABLE,
            model_version=version,
            date_partition=partition,
            actual_rows=actual,
            expected_rows=expected,
            source_model=MODEL,
        )


# ======================================================================
# Section A: 基线建立与回滚资格校验
# ======================================================================


class TestBaselineBuild:
    """发布记录与首落分区双重校验决定基线资格。"""

    def test_eligible_baseline_after_full_validation(self, store: Store, svc: PartitionBaselineService) -> None:
        """发布记录覆盖且首落分区完整时，基线具备回滚资格。"""
        _release_v1(svc)
        _land_v1(svc)
        result = svc.build_baselines_for_release("REL-V1", MODEL)

        assert result["ok"] is True
        assert result["total"] == 3
        assert result["eligible"] == 3
        assert result["rejected"] == 0
        for baseline in result["baselines"]:
            assert baseline["eligible"] is True
            assert baseline["target_table"] == TABLE
            assert baseline["model_version"] == "v1.0"
            assert baseline["date_partition"] in PARTITIONS
            assert baseline["reason"] == ""

    def test_baseline_identified_by_table_version_partition(self, store: Store, svc: PartitionBaselineService) -> None:
        """同表不同版本/分区是不同基线，重复登记幂等不翻转状态。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)

        again = svc.build_baseline(TABLE, "v1.0", PARTITIONS[0],
                                   source_model=MODEL, release_id="REL-V1")
        assert again["eligible"] is True
        assert len(svc.list_baselines(TABLE)) == 3

        # 同表新版本是独立基线
        _release_v1(svc, release_id="REL-V2", version="v2.0",
                    partitions=[PARTITIONS[0]])
        _land_v1(svc, version="v2.0", partitions=[PARTITIONS[0]])
        svc.build_baseline(TABLE, "v2.0", PARTITIONS[0],
                           source_model=MODEL, release_id="REL-V2")
        assert len(svc.list_baselines(TABLE)) == 4

    def test_half_written_first_landing_not_eligible(self, store: Store, svc: PartitionBaselineService) -> None:
        """首轮只写了一半（实际行数未达预期）的分区不具备回滚资格。"""
        _release_v1(svc)
        # 两个完整分区 + 一个只写了一半的分区
        _land_v1(svc, partitions=PARTITIONS[:2])
        svc.record_first_landing(
            TABLE, "v1.0", PARTITIONS[2],
            actual_rows=40, expected_rows=100, source_model=MODEL,
        )
        result = svc.build_baselines_for_release("REL-V1", MODEL)

        assert result["eligible"] == 2
        assert result["rejected"] == 1
        bad = next(b for b in result["baselines"] if b["date_partition"] == PARTITIONS[2])
        assert bad["eligible"] is False
        assert "首轮数据不完整" in bad["reason"]

    def test_other_model_same_partition_not_eligible(self, store: Store, svc: PartitionBaselineService) -> None:
        """其他模型在同名分区上的数据不能充当本模型基线。"""
        _release_v1(svc)
        # 首落分区来自另一个模型
        svc.record_first_landing(
            TABLE, "v1.0", PARTITIONS[0],
            actual_rows=100, expected_rows=100, source_model="other_model",
        )
        baseline = svc.build_baseline(
            TABLE, "v1.0", PARTITIONS[0], source_model=MODEL, release_id="REL-V1",
        )
        assert baseline["eligible"] is False
        assert "其他模型" in baseline["reason"]

    def test_missing_release_record_rejected(self, store: Store, svc: PartitionBaselineService) -> None:
        """没有模型发布记录时不能建立可回滚基线。"""
        _land_v1(svc, partitions=[PARTITIONS[0]])
        baseline = svc.build_baseline(
            TABLE, "v1.0", PARTITIONS[0], source_model=MODEL, release_id="REL-GHOST",
        )
        assert baseline["eligible"] is False
        assert "发布记录" in baseline["reason"]

    def test_release_not_covering_partition_rejected(self, store: Store, svc: PartitionBaselineService) -> None:
        """发布记录未覆盖的日期分区不在回滚基线上。"""
        _release_v1(svc, partitions=PARTITIONS[:1])
        _land_v1(svc, partitions=[PARTITIONS[1]])
        baseline = svc.build_baseline(
            TABLE, "v1.0", PARTITIONS[1], source_model=MODEL, release_id="REL-V1",
        )
        assert baseline["eligible"] is False
        assert "未覆盖日期分区" in baseline["reason"]

    def test_missing_first_landing_rejected(self, store: Store, svc: PartitionBaselineService) -> None:
        """目标表缺少首落分区数据时基线不可回滚。"""
        _release_v1(svc)
        baseline = svc.build_baseline(
            TABLE, "v1.0", PARTITIONS[0], source_model=MODEL, release_id="REL-V1",
        )
        assert baseline["eligible"] is False
        assert "首落分区数据" in baseline["reason"]


# ======================================================================
# Section B: 完整快照登记与回写门控
# ======================================================================


class TestSnapshotGate:
    """快照计划登记完整快照后，部署流水线才可回写。"""

    def test_baseline_not_rollbackable_before_snapshot(self, store: Store, svc: PartitionBaselineService) -> None:
        """仅有基线、未登记完整快照时没有可回滚候选。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)

        candidate = svc.select_rollback_candidate(TABLE, "v1.0")
        assert candidate["rollbackable"] is False

    def test_complete_snapshot_enables_rollback(self, store: Store, svc: PartitionBaselineService) -> None:
        """完整快照登记后，基线挂上快照编号并可回滚。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)

        reg = svc.register_complete_snapshot(
            "SNAP-V1-FULL", TABLE, "v1.0", PARTITIONS,
        )
        assert reg["missing_partitions"] == []
        candidate = svc.select_rollback_candidate(TABLE, "v1.0")
        assert candidate["rollbackable"] is True
        for baseline in candidate["baselines"]:
            assert baseline["snapshot_id"] == "SNAP-V1-FULL"
            assert baseline["snapshot_complete"] is True

    def test_partial_snapshot_reports_missing_partition(self, store: Store, svc: PartitionBaselineService) -> None:
        """新快照尚缺一个日期分区时，缺分区被明确标出且不可回写。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)

        reg = svc.register_complete_snapshot(
            "SNAP-V1-PARTIAL", TABLE, "v1.0", PARTITIONS[:2],
        )
        assert reg["missing_partitions"] == [PARTITIONS[2]]

        candidate = svc.select_rollback_candidate(TABLE, "v1.0")
        # 已被快照覆盖的两个分区仍可回滚，缺的那个不在候选内
        covered = {b["date_partition"] for b in candidate["baselines"]}
        assert covered == set(PARTITIONS[:2])
        assert PARTITIONS[2] not in covered


# ======================================================================
# Section C: 回滚执行与明细
# ======================================================================


class TestRollbackExecution:
    """回滚明细必须列出实际快照编号与写回的日期分区。"""

    def test_rollback_writes_only_snapshot_partitions(self, store: Store, svc: PartitionBaselineService) -> None:
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)
        svc.register_complete_snapshot("SNAP-V1-FULL", TABLE, "v1.0", PARTITIONS)

        result = svc.execute_rollback(TABLE, "v1.0")
        assert result["ok"] is True
        rollback = result["rollback"]
        assert rollback["snapshot_ids"] == ["SNAP-V1-FULL"]
        assert rollback["written_partitions"] == sorted(PARTITIONS)
        assert rollback["restored_model_version"] == "v1.0"

    def test_rollback_detail_persists_snapshot_and_partitions(self, store: Store, svc: PartitionBaselineService) -> None:
        """重新进入回滚详情，能核对返回的模型版本和持久化的日期分区。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)
        svc.register_complete_snapshot("SNAP-V1-FULL", TABLE, "v1.0", PARTITIONS)

        created = svc.execute_rollback(TABLE, "v1.0")["rollback"]
        detail = svc.get_rollback_detail(created["rollback_id"])
        assert detail is not None
        assert detail["restored_model_version"] == "v1.0"
        assert detail["snapshot_ids"] == ["SNAP-V1-FULL"]
        assert detail["written_partitions"] == sorted(PARTITIONS)

    def test_requested_partitions_filter_writeback(self, store: Store, svc: PartitionBaselineService) -> None:
        """只回写指定日期分区，其余分区跳过并在明细中留痕。"""
        _release_v1(svc)
        _land_v1(svc)
        svc.build_baselines_for_release("REL-V1", MODEL)
        svc.register_complete_snapshot("SNAP-V1-FULL", TABLE, "v1.0", PARTITIONS)

        result = svc.execute_rollback(TABLE, "v1.0",
                                      requested_partitions=[PARTITIONS[0]])
        rollback = result["rollback"]
        assert rollback["written_partitions"] == [PARTITIONS[0]]
        assert set(rollback["skipped_partitions"]) == set(PARTITIONS[1:])

    def test_rollback_without_any_baseline_fails(self, store: Store, svc: PartitionBaselineService) -> None:
        result = svc.execute_rollback(TABLE, "v9.9")
        assert result["ok"] is False
        assert result["rollback"] is None


# ======================================================================
# Section D: 版本切换、时序冲突与新旧分区状态区分
# ======================================================================


class TestVersionSwitchAndFallback:
    """同表切换模型版本、首快照时序冲突、回退后新旧分区状态区分。"""

    def _prepare_v1(self, svc: PartitionBaselineService) -> None:
        _release_v1(svc, release_id="REL-V1", version="v1.0")
        _land_v1(svc, version="v1.0")
        svc.build_baselines_for_release("REL-V1", MODEL)
        svc.register_complete_snapshot("SNAP-V1-FULL", TABLE, "v1.0", PARTITIONS)

    def test_new_model_first_snapshot_conflict_falls_back_to_previous(
        self, store: Store, svc: PartitionBaselineService,
    ) -> None:
        """新模型第一份快照采集与回滚时序冲突：v2 无完整快照，沿用 v1 完整基线。"""
        self._prepare_v1(svc)

        # v2 发布并落地，但快照还在采集中（未登记完整快照）
        svc.record_model_release(
            "REL-V2", TABLE, "v2.0", PARTITIONS, source_model=MODEL,
        )
        _land_v1(svc, version="v2.0")
        svc.build_baselines_for_release("REL-V2", MODEL)

        candidate = svc.select_rollback_candidate(TABLE, "v2.0")
        assert candidate["rollbackable"] is True
        assert candidate["fallback"] is True
        assert candidate["selected_version"] == "v1.0"

        result = svc.execute_rollback(TABLE, "v2.0")
        rollback = result["rollback"]
        assert rollback["restored_model_version"] == "v1.0"
        assert rollback["fallback"] is True
        assert rollback["snapshot_ids"] == ["SNAP-V1-FULL"]

    def test_rollback_distinguishes_old_partitions_and_new_snapshot_state(
        self, store: Store, svc: PartitionBaselineService,
    ) -> None:
        """回退后明细同时保留实际回写分区与刚发布版本留下的快照状态。"""
        self._prepare_v1(svc)

        # v2 快照只覆盖两个分区，缺一个日期分区
        svc.record_model_release("REL-V2", TABLE, "v2.0", PARTITIONS, source_model=MODEL)
        _land_v1(svc, version="v2.0")
        svc.build_baselines_for_release("REL-V2", MODEL)
        svc.register_complete_snapshot(
            "SNAP-V2-PARTIAL", TABLE, "v2.0", PARTITIONS[:2],
        )

        result = svc.execute_rollback(TABLE, "v2.0")
        rollback = result["rollback"]
        # 实际回写来自 v2 快照已覆盖的分区
        assert rollback["restored_model_version"] == "v2.0"
        assert rollback["snapshot_ids"] == ["SNAP-V2-PARTIAL"]
        assert rollback["written_partitions"] == sorted(PARTITIONS[:2])
        # 刚发布版本留下的半成品快照状态与缺失分区仍可在明细中区分
        assert "SNAP-V2-PARTIAL" in rollback["current_version_snapshot_ids"]
        assert rollback["requested_version"] == "v2.0"
        assert rollback["leftover_partitions"] == [PARTITIONS[2]]

    def test_version_switch_missing_one_partition_rollback_and_reenter_detail(
        self, store: Store, svc: PartitionBaselineService,
    ) -> None:
        """同表切换版本、新快照缺一个分区时回滚，重进详情核对版本与持久化分区。"""
        self._prepare_v1(svc)

        # v2 的新快照尚缺 PARTITIONS[2]
        svc.record_model_release("REL-V2", TABLE, "v2.0", PARTITIONS, source_model=MODEL)
        _land_v1(svc, version="v2.0")
        svc.build_baselines_for_release("REL-V2", MODEL)
        svc.register_complete_snapshot(
            "SNAP-V2-PARTIAL", TABLE, "v2.0", PARTITIONS[:2],
        )

        created = svc.execute_rollback(TABLE, "v2.0")["rollback"]

        # 重新进入回滚详情：返回的模型版本与持久化日期分区必须一致
        detail = svc.get_rollback_detail(created["rollback_id"])
        assert detail["restored_model_version"] == "v2.0"
        assert detail["written_partitions"] == sorted(PARTITIONS[:2])
        assert detail["snapshot_ids"] == ["SNAP-V2-PARTIAL"]
        # 缺失分区没有被回写，作为刚发布版本的遗留快照状态保留
        assert detail["leftover_partitions"] == [PARTITIONS[2]]

    def test_v2_complete_snapshot_rolls_back_to_v2(self, store: Store, svc: PartitionBaselineService) -> None:
        """v2 快照补齐后，回滚优先选择新版本自身的完整基线。"""
        self._prepare_v1(svc)
        svc.record_model_release("REL-V2", TABLE, "v2.0", PARTITIONS, source_model=MODEL)
        _land_v1(svc, version="v2.0")
        svc.build_baselines_for_release("REL-V2", MODEL)
        svc.register_complete_snapshot("SNAP-V2-FULL", TABLE, "v2.0", PARTITIONS)

        candidate = svc.select_rollback_candidate(TABLE, "v2.0")
        assert candidate["fallback"] is False
        assert candidate["selected_version"] == "v2.0"
        rollback = svc.execute_rollback(TABLE, "v2.0")["rollback"]
        assert rollback["snapshot_ids"] == ["SNAP-V2-FULL"]
        assert rollback["restored_model_version"] == "v2.0"

    def test_summary_counts_eligibility_and_snapshots(self, store: Store, svc: PartitionBaselineService) -> None:
        self._prepare_v1(svc)
        summary = svc.summary()
        assert summary["baselines"] == 3
        assert summary["eligible_baselines"] == 3
        assert summary["snapshot_ready_baselines"] == 3
        assert summary["snapshots"] == 1
        assert summary["rollback_records"] == 0

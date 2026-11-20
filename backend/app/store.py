"""内存数据仓库：跨模块数据通道、失败恢复、快照引擎和产物注册。

各业务模块产生的数据产物通过 DataChannel 发布，下游模块消费；
FailureRecovery 管理断点恢复；SnapshotEngine 提供版本快照能力。
"""
from __future__ import annotations

import hashlib
import uuid
from datetime import datetime, timezone
from typing import Any

from app.seed import SEED_ROWS


class DataChannel:
    """跨模块数据通道：上游模块发布数据产物，下游模块通过 topic 消费。"""

    def __init__(self) -> None:
        self._channels: dict[str, list[dict[str, Any]]] = {}
        self._consumers: dict[str, list[str]] = {}

    def publish(self, topic: str, payload: dict[str, Any]) -> str:
        """将数据产物发布到指定 topic，返回消息ID。"""
        msg_id = str(uuid.uuid4())[:12]
        entry = {
            "msg_id": msg_id,
            "topic": topic,
            "payload": payload,
            "published_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
            "expired": False,
        }
        self._channels.setdefault(topic, []).append(entry)
        return msg_id

    def consume(self, topic: str, consumer: str) -> list[dict[str, Any]]:
        """消费指定 topic 上未被当前消费者消费过的消息，标记已消费。"""
        channel = self._channels.get(topic, [])
        results: list[dict[str, Any]] = []
        for entry in channel:
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                results.append(entry)
        return results

    def consume_latest(self, topic: str, consumer: str) -> dict[str, Any] | None:
        """消费指定 topic 上最新一条未被消费的消息。"""
        channel = self._channels.get(topic, [])
        for entry in reversed(channel):
            if consumer not in entry["consumed_by"] and not entry["expired"]:
                entry["consumed_by"].append(consumer)
                return entry
        return None

    def topic_snapshot(self, topic: str) -> list[dict[str, Any]]:
        """获取 topic 上所有消息的快照。"""
        return list(self._channels.get(topic, []))

    def expire_topic(self, topic: str) -> int:
        """过期指定 topic 上所有未消费消息，返回过期数量。"""
        count = 0
        for entry in self._channels.get(topic, []):
            if not entry["expired"]:
                entry["expired"] = True
                count += 1
        return count

    def list_topics(self) -> list[str]:
        return sorted(self._channels)

    def topic_stats(self) -> dict[str, dict[str, int]]:
        stats: dict[str, dict[str, int]] = {}
        for topic, entries in self._channels.items():
            consumed = sum(1 for e in entries if len(e["consumed_by"]) > 0)
            pending = len(entries) - consumed
            expired = sum(1 for e in entries if e["expired"])
            stats[topic] = {"total": len(entries), "consumed": consumed, "pending": pending, "expired": expired}
        return stats


class FailureRecovery:
    """失败恢复管理器：检查点管理、重试计数和失败链路追踪。"""

    def __init__(self) -> None:
        self._checkpoints: dict[str, dict[str, Any]] = {}
        self._retry_logs: dict[str, list[dict[str, Any]]] = {}
        self._failure_chains: dict[str, list[dict[str, Any]]] = {}

    def create_checkpoint(self, module: str, task_id: str, state: dict[str, Any]) -> str:
        """创建处理检查点，返回检查点ID。"""
        cp_id = f"{module}-{task_id}-{uuid.uuid4().hex[:8]}"
        self._checkpoints[cp_id] = {
            "cp_id": cp_id,
            "module": module,
            "task_id": task_id,
            "state": dict(state),
            "created_at": datetime.now(timezone.utc).isoformat(),
            "restored_at": None,
            "restore_count": 0,
            "completed": False,
        }
        return cp_id

    def get_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        return self._checkpoints.get(cp_id)

    def update_checkpoint(self, cp_id: str, state: dict[str, Any]) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["state"].update(state)
        return True

    def mark_completed(self, cp_id: str) -> bool:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return False
        cp["completed"] = True
        return True

    def restore_checkpoint(self, cp_id: str) -> dict[str, Any] | None:
        cp = self._checkpoints.get(cp_id)
        if cp is None:
            return None
        cp["restored_at"] = datetime.now(timezone.utc).isoformat()
        cp["restore_count"] += 1
        return dict(cp)

    def log_retry(self, module: str, task_id: str, attempt: int, reason: str, success: bool) -> None:
        self._retry_logs.setdefault(module, [])
        self._retry_logs[module].append({
            "task_id": task_id,
            "attempt": attempt,
            "reason": reason,
            "success": success,
            "logged_at": datetime.now(timezone.utc).isoformat(),
        })

    def retry_history(self, module: str) -> list[dict[str, Any]]:
        return self._retry_logs.get(module, [])

    def log_failure_chain(self, chain_id: str, step: str, module: str, error: str, upstream: str | None = None) -> None:
        self._failure_chains.setdefault(chain_id, [])
        self._failure_chains[chain_id].append({
            "step": step,
            "module": module,
            "error": error,
            "upstream": upstream,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        })

    def chain_summary(self, chain_id: str) -> list[dict[str, Any]]:
        return self._failure_chains.get(chain_id, [])

    def recovery_summary(self) -> dict[str, Any]:
        completed = sum(1 for cp in self._checkpoints.values() if cp["completed"])
        pending = len(self._checkpoints) - completed
        total_retries = sum(len(logs) for logs in self._retry_logs.values())
        total_chains = len(self._failure_chains)
        return {
            "checkpoints": {"total": len(self._checkpoints), "completed": completed, "pending": pending},
            "retries": total_retries,
            "failure_chains": total_chains,
        }


class SnapshotEngine:
    """版本快照引擎：创建、比较和管理数据快照。"""

    def __init__(self) -> None:
        self._snapshots: dict[str, dict[str, Any]] = {}
        self._diff_logs: dict[str, dict[str, Any]] = {}

    def create_snapshot(self, module: str, entry_id: int, data: dict[str, Any], version_label: str = "") -> str:
        """创建数据快照，返回快照ID。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-v{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": None,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def create_version_snapshot(self, module: str, entry_id: int, data: dict[str, Any],
                                version_label: str, parent_snap_id: str | None = None) -> str:
        """创建带父子关系的版本快照，用于模型发布版本管理。"""
        data_str = str(sorted(data.items()))
        content_hash = hashlib.sha256(data_str.encode()).hexdigest()[:16]
        snap_id = f"{module}-{entry_id}-{version_label}-{uuid.uuid4().hex[:8]}"
        self._snapshots[snap_id] = {
            "snap_id": snap_id,
            "module": module,
            "entry_id": entry_id,
            "version_label": version_label,
            "data": dict(data),
            "content_hash": content_hash,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "parent_snap_id": parent_snap_id,
            "retention_policy": "keep_forever",
        }
        return snap_id

    def get_snapshot(self, snap_id: str) -> dict[str, Any] | None:
        return self._snapshots.get(snap_id)

    def list_snapshots(self, module: str, entry_id: int) -> list[dict[str, Any]]:
        return [
            s for s in self._snapshots.values()
            if s["module"] == module and s["entry_id"] == entry_id
        ]

    def diff_snapshots(self, snap_id_a: str, snap_id_b: str) -> dict[str, Any] | None:
        snap_a = self._snapshots.get(snap_id_a)
        snap_b = self._snapshots.get(snap_id_b)
        if not snap_a or not snap_b:
            return None
        keys_a = set(snap_a["data"])
        keys_b = set(snap_b["data"])
        added = {k: snap_b["data"][k] for k in keys_b - keys_a}
        removed = {k: snap_a["data"][k] for k in keys_a - keys_b}
        modified = {}
        for k in keys_a & keys_b:
            if snap_a["data"][k] != snap_b["data"][k]:
                modified[k] = {"old": snap_a["data"][k], "new": snap_b["data"][k]}
        diff_id = f"diff-{uuid.uuid4().hex[:8]}"
        result = {
            "diff_id": diff_id,
            "snap_a": snap_id_a,
            "snap_b": snap_id_b,
            "added_fields": added,
            "removed_fields": removed,
            "modified_fields": modified,
            "total_changes": len(added) + len(removed) + len(modified),
            "created_at": datetime.now(timezone.utc).isoformat(),
        }
        self._diff_logs[diff_id] = result
        return result

    def rollback_to_snapshot(self, module: str, entry_id: int, snap_id: str, rows: list[dict[str, Any]]) -> dict[str, Any] | None:
        """将数据回滚到指定快照版本。"""
        snap = self._snapshots.get(snap_id)
        if not snap:
            return None
        for row in rows:
            if row.get("id") == entry_id:
                for k, v in snap["data"].items():
                    row[k] = v
                row["_rollback_to"] = snap_id
                row["_rollback_at"] = datetime.now(timezone.utc).isoformat()
                return row
        return None

    def snapshot_summary(self) -> dict[str, Any]:
        by_module: dict[str, int] = {}
        for s in self._snapshots.values():
            by_module[s["module"]] = by_module.get(s["module"], 0) + 1
        return {"total_snapshots": len(self._snapshots), "by_module": by_module, "total_diffs": len(self._diff_logs)}


class PartitionBaselineRegistry:
    """分区恢复基线注册表：按「目标表 + 模型版本 + 日期分区」登记可回滚基线。

    只有通过模型发布记录与目标表首落分区校验的基线才具备回滚资格；
    其他模型的同名分区、首轮只写了一半的数据均不予登记。
    """

    def __init__(self) -> None:
        # 模型发布记录：记录每次发布落在哪些目标表、写入了哪些日期分区
        self._release_records: dict[str, dict[str, Any]] = {}
        # 目标表首落分区数据（实际落地行数），用于首轮完整性校验
        self._first_landings: dict[str, dict[str, Any]] = {}
        # 已登记的恢复基线，key = (目标表, 模型版本, 日期分区)
        self._baselines: dict[tuple[str, str, str], dict[str, Any]] = {}
        # 已登记的完整快照：snap_id -> 覆盖的 (目标表, 日期分区) 集合
        self._snapshots: dict[str, dict[str, Any]] = {}
        # 回写执行记录
        self._rollback_records: dict[str, dict[str, Any]] = {}

    # ------------------------------------------------------------------
    # 模型发布记录与首落分区数据
    # ------------------------------------------------------------------

    def record_release(
        self,
        release_id: str,
        target_table: str,
        model_version: str,
        partitions: list[str],
    ) -> dict[str, Any]:
        """登记一条模型发布记录，标明本次发布覆盖的目标表与日期分区。"""
        record = {
            "release_id": release_id,
            "target_table": target_table,
            "model_version": model_version,
            "partitions": list(partitions),
            "released_at": datetime.now(timezone.utc).isoformat(),
        }
        self._release_records[release_id] = record
        return record

    def get_release(self, release_id: str) -> dict[str, Any] | None:
        return self._release_records.get(release_id)

    def releases_for(self, target_table: str) -> list[dict[str, Any]]:
        return [
            r for r in self._release_records.values()
            if _table_matches(r["target_table"], target_table)
        ]

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
        """登记目标表某个日期分区首次落下的数据及行数校验依据。"""
        key = f"{target_table}|{model_version}|{date_partition}"
        landing = {
            "target_table": target_table,
            "model_version": model_version,
            "date_partition": date_partition,
            "actual_rows": actual_rows,
            "expected_rows": expected_rows,
            "source_model": source_model,
            "complete": actual_rows >= expected_rows and expected_rows > 0,
            "landed_at": datetime.now(timezone.utc).isoformat(),
        }
        self._first_landings[key] = landing
        return landing

    def get_first_landing(
        self, target_table: str, model_version: str, date_partition: str,
    ) -> dict[str, Any] | None:
        return self._first_landings.get(f"{target_table}|{model_version}|{date_partition}")

    # ------------------------------------------------------------------
    # 恢复基线登记与查询
    # ------------------------------------------------------------------

    def register_baseline(
        self,
        target_table: str,
        model_version: str,
        date_partition: str,
        metadata: dict[str, Any],
    ) -> dict[str, Any]:
        """登记一条分区恢复基线；同一识别键重复登记返回既有基线。"""
        key = (target_table, model_version, date_partition)
        existing = self._baselines.get(key)
        if existing is not None:
            return existing
        baseline = {
            "baseline_id": f"BL-{uuid.uuid4().hex[:10].upper()}",
            "target_table": target_table,
            "model_version": model_version,
            "date_partition": date_partition,
            "eligible": bool(metadata.get("eligible", False)),
            "source_release": metadata.get("source_release", ""),
            "actual_rows": metadata.get("actual_rows", 0),
            "expected_rows": metadata.get("expected_rows", 0),
            "snapshot_id": None,
            "snapshot_complete": False,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "reason": metadata.get("reason", ""),
        }
        self._baselines[key] = baseline
        return baseline

    def get_baseline(
        self, target_table: str, model_version: str, date_partition: str,
    ) -> dict[str, Any] | None:
        return self._baselines.get((target_table, model_version, date_partition))

    def list_baselines(
        self, target_table: str | None = None, model_version: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = list(self._baselines.values())
        if target_table:
            rows = [r for r in rows if _table_matches(target_table, r["target_table"])]
        if model_version:
            rows = [r for r in rows if r["model_version"] == model_version]
        return rows

    def eligible_baselines(
        self, target_table: str, model_version: str | None = None,
    ) -> list[dict[str, Any]]:
        rows = [
            r for r in self._baselines.values()
            if r["eligible"] and r["snapshot_complete"]
            and _table_matches(target_table, r["target_table"])
        ]
        if model_version:
            rows = [r for r in rows if r["model_version"] == model_version]
        return rows

    # ------------------------------------------------------------------
    # 完整快照登记：部署流水线只能回写快照标明的表和分区
    # ------------------------------------------------------------------

    def register_snapshot(
        self,
        snapshot_id: str,
        target_table: str,
        model_version: str,
        partitions: list[str],
    ) -> dict[str, Any]:
        """登记一份完整快照，并回写其覆盖基线的快照状态。"""
        covered = {(target_table, p) for p in partitions}
        snapshot = {
            "snapshot_id": snapshot_id,
            "target_table": target_table,
            "model_version": model_version,
            "partitions": list(partitions),
            "covered": covered,
            "registered_at": datetime.now(timezone.utc).isoformat(),
        }
        self._snapshots[snapshot_id] = snapshot

        missing: list[str] = []
        for baseline in self._baselines.values():
            if not baseline["eligible"]:
                continue
            if baseline["model_version"] != model_version:
                continue
            if not _table_matches(target_table, baseline["target_table"]):
                continue
            # 目标表（含通配策略）与日期分区同时被快照覆盖才挂接
            if baseline["date_partition"] in partitions:
                baseline["snapshot_id"] = snapshot_id
                baseline["snapshot_complete"] = True
            else:
                missing.append(baseline["date_partition"])
        snapshot["missing_partitions"] = sorted(set(missing))
        return snapshot

    def get_snapshot(self, snapshot_id: str) -> dict[str, Any] | None:
        return self._snapshots.get(snapshot_id)

    def list_snapshots(self, target_table: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._snapshots.values())
        if target_table:
            rows = [r for r in rows if _table_matches(target_table, r["target_table"])]
        return rows

    # ------------------------------------------------------------------
    # 回写执行记录
    # ------------------------------------------------------------------

    def add_rollback_record(self, record: dict[str, Any]) -> dict[str, Any]:
        rollback_id = f"RB-{uuid.uuid4().hex[:10].upper()}"
        entry = {"rollback_id": rollback_id, **record,
                 "rolled_back_at": datetime.now(timezone.utc).isoformat()}
        self._rollback_records[rollback_id] = entry
        return entry

    def get_rollback_record(self, rollback_id: str) -> dict[str, Any] | None:
        return self._rollback_records.get(rollback_id)

    def list_rollback_records(self, target_table: str | None = None) -> list[dict[str, Any]]:
        rows = list(self._rollback_records.values())
        if target_table:
            rows = [r for r in rows if _table_matches(target_table, r["target_table"])]
        return rows

    def baseline_summary(self) -> dict[str, Any]:
        eligible = sum(1 for b in self._baselines.values() if b["eligible"])
        snap_ready = sum(
            1 for b in self._baselines.values()
            if b["eligible"] and b["snapshot_complete"]
        )
        return {
            "release_records": len(self._release_records),
            "first_landings": len(self._first_landings),
            "baselines": len(self._baselines),
            "eligible_baselines": eligible,
            "snapshot_ready_baselines": snap_ready,
            "snapshots": len(self._snapshots),
            "rollback_records": len(self._rollback_records),
        }


def _table_matches(pattern: str, actual: str) -> bool:
    """目标表匹配：精确相等，或 pattern 以 * 结尾时按前缀匹配。"""
    if pattern == actual:
        return True
    if pattern.endswith("*"):
        return actual.startswith(pattern[:-1])
    if actual.endswith("*"):
        return pattern.startswith(actual[:-1])
    return False


class ArtifactRegistry:
    """数据产物注册表：追踪各模块产出的数据工件。"""

    def __init__(self) -> None:
        self._artifacts: dict[str, dict[str, Any]] = {}

    def register(self, module: str, artifact_type: str, artifact_id: str,
                 metadata: dict[str, Any], source_modules: list[str] | None = None) -> str:
        """注册数据产物，返回注册ID。"""
        reg_id = f"{module}-{artifact_type}-{artifact_id}"
        self._artifacts[reg_id] = {
            "reg_id": reg_id,
            "module": module,
            "artifact_type": artifact_type,
            "artifact_id": artifact_id,
            "metadata": dict(metadata),
            "source_modules": source_modules or [module],
            "registered_at": datetime.now(timezone.utc).isoformat(),
            "consumed_by": [],
        }
        return reg_id

    def get_artifact(self, reg_id: str) -> dict[str, Any] | None:
        return self._artifacts.get(reg_id)

    def list_by_module(self, module: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["module"] == module]

    def mark_consumed(self, reg_id: str, consumer: str) -> bool:
        a = self._artifacts.get(reg_id)
        if a is None:
            return False
        a["consumed_by"].append(consumer)
        return True

    def list_all(self) -> list[dict[str, Any]]:
        return list(self._artifacts.values())

    def by_type(self, artifact_type: str) -> list[dict[str, Any]]:
        return [a for a in self._artifacts.values() if a["artifact_type"] == artifact_type]

    def artifact_summary(self) -> dict[str, Any]:
        by_type: dict[str, int] = {}
        by_module: dict[str, int] = {}
        for a in self._artifacts.values():
            by_type[a["artifact_type"]] = by_type.get(a["artifact_type"], 0) + 1
            by_module[a["module"]] = by_module.get(a["module"], 0) + 1
        return {"total": len(self._artifacts), "by_type": by_type, "by_module": by_module}


class Store:
    def __init__(self) -> None:
        self._tables: dict[str, list[dict[str, Any]]] = {
            name: [dict(row) for row in rows] for name, rows in SEED_ROWS.items()
        }
        self.channel = DataChannel()
        self.recovery = FailureRecovery()
        self.snapshot = SnapshotEngine()
        self.artifacts = ArtifactRegistry()
        self.partition_baselines = PartitionBaselineRegistry()

    def module_names(self) -> list[str]:
        return sorted(self._tables)

    def rows(self, module: str) -> list[dict[str, Any]]:
        return self._tables.setdefault(module, [])

    def find(self, module: str, entry_id: int) -> dict[str, Any] | None:
        for row in self.rows(module):
            if int(row.get("id", 0)) == entry_id:
                return row
        return None

    def insert(self, module: str, entry: dict[str, Any]) -> None:
        self.rows(module).append(entry)

    def overview(self) -> dict[str, object]:
        modules: list[dict[str, object]] = []
        for name in self.module_names():
            rows = self.rows(name)
            modules.append({
                "name": name,
                "created": len(rows),
                "pending": sum(1 for row in rows if row.get("pending")),
                "abnormal": sum(1 for row in rows if row.get("abnormal")),
            })
        cards = [
            {"label": "业务模块", "value": len(modules)},
            {"label": "今日新增", "value": sum(int(item["created"]) for item in modules)},
            {"label": "待处理", "value": sum(int(item["pending"]) for item in modules)},
            {"label": "异常量", "value": sum(int(item["abnormal"]) for item in modules)},
        ]
        channel_stats = self.channel.topic_stats()
        recovery = self.recovery.recovery_summary()
        snapshot_info = self.snapshot.snapshot_summary()
        artifact_info = self.artifacts.artifact_summary()
        baseline_info = self.partition_baselines.baseline_summary()
        return {
            "cards": cards,
            "modules": modules,
            "data_channels": {"topics": len(channel_stats), "stats": channel_stats},
            "recovery": recovery,
            "snapshots": snapshot_info,
            "artifacts": artifact_info,
            "partition_baselines": baseline_info,
        }


store = Store()
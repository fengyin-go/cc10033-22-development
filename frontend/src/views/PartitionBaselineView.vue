<template>
  <section class="page" data-module="partition_baseline">
    <header class="page-head">
      <div>
        <h2>分区恢复基线</h2>
        <p class="page-desc">
          按目标表、模型版本与日期分区建立可回滚数据基线；快照计划登记完整快照后，部署流水线才可按标明的表和分区回写。
        </p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="reload" type="button" @click="reloadAll">刷新</button>
      </div>
    </header>

    <div class="stat-row">
      <article class="stat-card">
        <span class="stat-label">基线总数</span>
        <strong class="stat-value">{{ summary.baselines ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">具备回滚资格</span>
        <strong class="stat-value">{{ summary.eligible_baselines ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">快照可回写</span>
        <strong class="stat-value">{{ summary.snapshot_ready_baselines ?? 0 }}</strong>
      </article>
      <article class="stat-card">
        <span class="stat-label">完整快照 / 回写记录</span>
        <strong class="stat-value">{{ summary.snapshots ?? 0 }} / {{ summary.rollback_records ?? 0 }}</strong>
      </article>
    </div>

    <form class="filter-bar" @submit.prevent="reloadAll">
      <label class="filter-item">
        <span>目标表过滤</span>
        <input v-model="tableFilter" data-testid="filter-table" placeholder="如 dwd_sales_detail" />
      </label>
      <button class="btn" data-testid="query" type="submit">查询</button>
      <button class="btn ghost" type="button" @click="tableFilter = ''">清空</button>
    </form>

    <div class="analysis-form">
      <label>
        发布记录ID
        <input v-model="releaseForm.release_id" data-testid="release-id" placeholder="REL-V1" />
      </label>
      <label>
        目标表
        <input v-model="releaseForm.target_table" data-testid="release-table" placeholder="dwd_sales_detail" />
      </label>
      <label>
        模型版本
        <input v-model="releaseForm.model_version" data-testid="release-version" placeholder="v1.0" />
      </label>
      <label>
        日期分区（逗号分隔）
        <input v-model="releasePartitionsText" data-testid="release-partitions" placeholder="20261117,20261118" />
      </label>
      <label>
        源模型
        <input v-model="releaseForm.source_model" data-testid="release-source" placeholder="sales_model" />
      </label>
      <button class="btn primary" type="button" data-testid="submit-release" @click="submitRelease">
        登记发布并建基线
      </button>
    </div>

    <h3>恢复基线（目标表 + 模型版本 + 日期分区）</h3>
    <table class="data-table" data-testid="baseline-table">
      <thead>
        <tr>
          <th>基线编号</th>
          <th>目标表</th>
          <th>模型版本</th>
          <th>日期分区</th>
          <th>实际/预期行数</th>
          <th>回滚资格</th>
          <th>快照编号</th>
          <th>不通过原因</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="row in baselines" :key="row.baseline_id" :data-eligible="String(row.eligible)">
          <td>{{ row.baseline_id }}</td>
          <td>{{ row.target_table }}</td>
          <td>{{ row.model_version }}</td>
          <td>{{ row.date_partition }}</td>
          <td>{{ row.actual_rows }} / {{ row.expected_rows }}</td>
          <td>{{ row.eligible ? (row.snapshot_complete ? '可回滚' : '待快照') : '不具备' }}</td>
          <td>{{ row.snapshot_id ?? '—' }}</td>
          <td>{{ row.reason || '—' }}</td>
        </tr>
        <tr v-if="!baselines.length">
          <td colspan="8" class="empty-state">暂无分区恢复基线</td>
        </tr>
      </tbody>
    </table>

    <div class="analysis-form">
      <label>
        快照编号
        <input v-model="snapshotForm.snapshot_id" data-testid="snap-id" placeholder="SNAP-V1-FULL" />
      </label>
      <label>
        目标表
        <input v-model="snapshotForm.target_table" data-testid="snap-table" placeholder="dwd_sales_detail" />
      </label>
      <label>
        模型版本
        <input v-model="snapshotForm.model_version" data-testid="snap-version" placeholder="v1.0" />
      </label>
      <label>
        覆盖分区（逗号分隔）
        <input v-model="snapshotPartitionsText" data-testid="snap-partitions" placeholder="20261117,20261118" />
      </label>
      <button class="btn primary" type="button" data-testid="submit-snapshot" @click="submitSnapshot">
        登记完整快照
      </button>
    </div>

    <h3>快照登记与缺失分区</h3>
    <table class="data-table" data-testid="snapshot-table">
      <thead>
        <tr>
          <th>快照编号</th>
          <th>目标表</th>
          <th>模型版本</th>
          <th>覆盖日期分区</th>
          <th>缺失分区</th>
          <th>登记时间</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="snap in snapshots" :key="snap.snapshot_id">
          <td>{{ snap.snapshot_id }}</td>
          <td>{{ snap.target_table }}</td>
          <td>{{ snap.model_version }}</td>
          <td>{{ snap.partitions.join(', ') || '—' }}</td>
          <td>{{ snap.missing_partitions.join(', ') || '无' }}</td>
          <td>{{ snap.registered_at }}</td>
        </tr>
        <tr v-if="!snapshots.length">
          <td colspan="6" class="empty-state">暂无完整快照登记</td>
        </tr>
      </tbody>
    </table>

    <div class="analysis-form">
      <label>
        回滚目标表
        <input v-model="rollbackForm.target_table" data-testid="rb-table" placeholder="dwd_sales_detail" />
      </label>
      <label>
        当前发布版本
        <input v-model="rollbackForm.current_version" data-testid="rb-version" placeholder="v2.0" />
      </label>
      <label>
        指定回写分区（留空回写全部可回滚分区）
        <input v-model="rollbackPartitionsText" data-testid="rb-partitions" placeholder="20261117" />
      </label>
      <button class="btn primary" type="button" data-testid="submit-rollback" @click="submitRollback">
        触发分区回滚
      </button>
    </div>

    <h3>回滚明细（实际快照编号与写回日期分区）</h3>
    <table class="data-table" data-testid="rollback-table">
      <thead>
        <tr>
          <th>回滚编号</th>
          <th>目标表</th>
          <th>请求版本</th>
          <th>恢复版本</th>
          <th>沿用旧版</th>
          <th>实际快照编号</th>
          <th>写回日期分区</th>
          <th>新版本遗留分区</th>
          <th>操作</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="rb in rollbacks" :key="rb.rollback_id">
          <td>{{ rb.rollback_id }}</td>
          <td>{{ rb.target_table }}</td>
          <td>{{ rb.requested_version }}</td>
          <td>{{ rb.restored_model_version }}</td>
          <td>{{ rb.fallback ? '是' : '否' }}</td>
          <td>{{ rb.snapshot_ids.join(', ') || '—' }}</td>
          <td>{{ rb.written_partitions.join(', ') || '—' }}</td>
          <td>{{ rb.leftover_partitions.join(', ') || '—' }}</td>
          <td>
            <button class="link" type="button" :data-testid="`detail-${rb.rollback_id}`" @click="openDetail(rb.rollback_id)">
              回滚详情
            </button>
          </td>
        </tr>
        <tr v-if="!rollbacks.length">
          <td colspan="9" class="empty-state">暂无回滚记录</td>
        </tr>
      </tbody>
    </table>

    <div v-if="detail" class="modal-backdrop" data-testid="rollback-detail-modal">
      <div class="modal">
        <h3>回滚详情 {{ detail.rollback_id }}</h3>
        <dl class="dag-detail">
          <dt>目标表</dt><dd>{{ detail.target_table }}</dd>
          <dt>请求版本</dt><dd>{{ detail.requested_version }}</dd>
          <dt>恢复模型版本</dt><dd>{{ detail.restored_model_version }}</dd>
          <dt>是否沿用上一版基线</dt><dd>{{ detail.fallback ? '是（首快照时序冲突）' : '否' }}</dd>
          <dt>实际快照编号</dt><dd>{{ detail.snapshot_ids.join(', ') || '—' }}</dd>
          <dt>写回日期分区</dt><dd>{{ detail.written_partitions.join(', ') || '—' }}</dd>
          <dt>跳过分区</dt><dd>{{ detail.skipped_partitions.join(', ') || '—' }}</dd>
          <dt>新版本遗留分区</dt><dd>{{ detail.leftover_partitions.join(', ') || '—' }}</dd>
          <dt>新版本快照状态</dt><dd>{{ detail.current_version_snapshot_ids.join(', ') || '—' }}</dd>
          <dt>回滚时间</dt><dd>{{ detail.rolled_back_at }}</dd>
        </dl>
        <div class="page-actions">
          <button class="btn primary" type="button" @click="detail = null">关闭</button>
        </div>
      </div>
    </div>

    <footer class="page-foot">
      <span>共 {{ baselines.length }} 条基线 / {{ snapshots.length }} 份快照 / {{ rollbacks.length }} 条回滚</span>
      <span v-if="errorMessage" class="error-text">{{ errorMessage }}</span>
    </footer>
  </section>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue'

import {
  buildBaselinesForRelease,
  executeRollback,
  fetchBaselineSummary,
  fetchBaselines,
  fetchRollbackDetail,
  fetchRollbacks,
  fetchSnapshots,
  recordFirstLanding,
  recordRelease,
  registerSnapshot,
  type BaselineRow,
  type RollbackRow,
  type SnapshotRow,
} from '@/api/partition-baseline'

const tableFilter = ref('')
const errorMessage = ref('')
const baselines = ref<BaselineRow[]>([])
const snapshots = ref<SnapshotRow[]>([])
const rollbacks = ref<RollbackRow[]>([])
const detail = ref<RollbackRow | null>(null)
const summary = ref<Record<string, number>>({})

const releaseForm = ref({
  release_id: '',
  target_table: '',
  model_version: '',
  source_model: '',
})
const releasePartitionsText = ref('')
const snapshotForm = ref({ snapshot_id: '', target_table: '', model_version: '' })
const snapshotPartitionsText = ref('')
const rollbackForm = ref({ target_table: '', current_version: '' })
const rollbackPartitionsText = ref('')

function splitPartitions(text: string): string[] {
  return text.split(/[,，]/).map((item) => item.trim()).filter(Boolean)
}

async function reloadAll() {
  errorMessage.value = ''
  try {
    const [baselineRows, snapshotRows, rollbackRows, summaryRows] = await Promise.all([
      fetchBaselines(tableFilter.value),
      fetchSnapshots(tableFilter.value),
      fetchRollbacks(tableFilter.value),
      fetchBaselineSummary(),
    ])
    baselines.value = baselineRows
    snapshots.value = snapshotRows
    rollbacks.value = rollbackRows
    summary.value = summaryRows
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '数据加载失败'
  }
}

async function submitRelease() {
  errorMessage.value = ''
  try {
    const partitions = splitPartitions(releasePartitionsText.value)
    await recordRelease({ ...releaseForm.value, partitions })
    // 登记每个分区的首落数据（演示按满量落地，后续可在页面扩展实际/预期行数）
    for (const partition of partitions) {
      await recordFirstLanding({
        target_table: releaseForm.value.target_table,
        model_version: releaseForm.value.model_version,
        date_partition: partition,
        actual_rows: 100,
        expected_rows: 100,
        source_model: releaseForm.value.source_model,
      })
    }
    await buildBaselinesForRelease(releaseForm.value.release_id, releaseForm.value.source_model)
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '发布登记失败'
  }
}

async function submitSnapshot() {
  errorMessage.value = ''
  try {
    await registerSnapshot({
      ...snapshotForm.value,
      partitions: splitPartitions(snapshotPartitionsText.value),
    })
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '快照登记失败'
  }
}

async function submitRollback() {
  errorMessage.value = ''
  try {
    const requested = splitPartitions(rollbackPartitionsText.value)
    await executeRollback({
      target_table: rollbackForm.value.target_table,
      current_version: rollbackForm.value.current_version,
      requested_partitions: requested.length ? requested : undefined,
    })
    await reloadAll()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回滚执行失败'
  }
}

async function openDetail(rollbackId: string) {
  errorMessage.value = ''
  try {
    detail.value = await fetchRollbackDetail(rollbackId)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '回滚详情读取失败'
  }
}

onMounted(reloadAll)
</script>

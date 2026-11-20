import { request } from './client'

/** 分区恢复基线领域接口封装：基线、完整快照、回滚候选与回写明细。 */

export type BaselineRow = {
  baseline_id: string
  target_table: string
  model_version: string
  date_partition: string
  eligible: boolean
  source_release: string
  actual_rows: number
  expected_rows: number
  snapshot_id: string | null
  snapshot_complete: boolean
  created_at: string
  reason: string
}

export type SnapshotRow = {
  snapshot_id: string
  target_table: string
  model_version: string
  partitions: string[]
  missing_partitions: string[]
  registered_at: string
}

export type RollbackRow = {
  rollback_id: string
  target_table: string
  requested_version: string
  restored_model_version: string
  fallback: boolean
  snapshot_ids: string[]
  written_partitions: string[]
  skipped_partitions: string[]
  leftover_partitions: string[]
  current_version_snapshot_ids: string[]
  reason: string
  rolled_back_at: string
}

async function postJson<T>(path: string, body: unknown): Promise<T> {
  const response = await request(path, {
    method: 'POST',
    body: JSON.stringify(body),
  })
  if (!response.ok) {
    const detail = await response.json().catch(() => ({ detail: '请求失败' }))
    throw new Error(detail.detail ?? '请求失败')
  }
  return await response.json()
}

export async function recordRelease(payload: {
  release_id: string
  target_table: string
  model_version: string
  partitions: string[]
  source_model: string
}) {
  return postJson('/api/partition-baselines/releases', payload)
}

export async function recordFirstLanding(payload: {
  target_table: string
  model_version: string
  date_partition: string
  actual_rows: number
  expected_rows: number
  source_model: string
}) {
  return postJson('/api/partition-baselines/first-landings', payload)
}

export async function buildBaselinesForRelease(release_id: string, source_model: string) {
  return postJson('/api/partition-baselines/build-for-release', { release_id, source_model })
}

export async function registerSnapshot(payload: {
  snapshot_id: string
  target_table: string
  model_version: string
  partitions: string[]
}) {
  return postJson('/api/partition-baselines/snapshots', payload)
}

export async function fetchBaselines(targetTable = ''): Promise<BaselineRow[]> {
  const query = targetTable ? `?target_table=${encodeURIComponent(targetTable)}` : ''
  const response = await request(`/api/partition-baselines${query}`)
  if (!response.ok) throw new Error('分区基线读取失败')
  const payload = await response.json()
  return payload.items
}

export async function fetchSnapshots(targetTable = ''): Promise<SnapshotRow[]> {
  const query = targetTable ? `?target_table=${encodeURIComponent(targetTable)}` : ''
  const response = await request(`/api/partition-baselines/snapshots${query}`)
  if (!response.ok) throw new Error('快照登记读取失败')
  const payload = await response.json()
  return payload.items
}

export async function fetchRollbackCandidate(targetTable: string, currentVersion: string) {
  const query = new URLSearchParams({
    target_table: targetTable,
    current_version: currentVersion,
  }).toString()
  const response = await request(`/api/partition-baselines/rollback-candidate?${query}`)
  if (!response.ok) throw new Error('回滚候选读取失败')
  return await response.json()
}

export async function executeRollback(payload: {
  target_table: string
  current_version: string
  requested_partitions?: string[]
  reason?: string
}) {
  return postJson('/api/partition-baselines/rollbacks', payload)
}

export async function fetchRollbacks(targetTable = ''): Promise<RollbackRow[]> {
  const query = targetTable ? `?target_table=${encodeURIComponent(targetTable)}` : ''
  const response = await request(`/api/partition-baselines/rollbacks${query}`)
  if (!response.ok) throw new Error('回滚明细读取失败')
  const payload = await response.json()
  return payload.items
}

export async function fetchRollbackDetail(rollbackId: string): Promise<RollbackRow> {
  const response = await request(`/api/partition-baselines/rollbacks/${rollbackId}`)
  if (!response.ok) throw new Error('回滚详情读取失败')
  return await response.json()
}

export async function fetchBaselineSummary() {
  const response = await request('/api/partition-baselines/summary')
  if (!response.ok) throw new Error('基线汇总读取失败')
  return await response.json()
}

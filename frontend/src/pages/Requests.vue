<script setup>
import { computed, onMounted, onUnmounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const rows = ref([])
const stats = ref(null)
const idempotencyKey = ref('')
const note = ref('')
const busy = ref(false)
const err = ref('')
const msg = ref('')
let timer = null

const STATUS_CLASS = {
  pending: 'warn',
  running: 'warn',
  succeeded: 'ok',
  failed: 'danger',
}

const sampleKey = computed(() => `run-${new Date().toISOString().replace(/[-:.TZ]/g, '').slice(0, 14)}`)

async function refresh() {
  try {
    const [list, s] = await Promise.all([
      getJSON('/api/requests?limit=100'),
      getJSON('/api/requests/stats'),
    ])
    rows.value = list
    stats.value = s
  } catch (e) {
    err.value = String(e.message || e)
  }
}

async function submit() {
  err.value = ''
  msg.value = ''
  const key = idempotencyKey.value.trim()
  if (!key) {
    err.value = '幂等键必填'
    return
  }
  busy.value = true
  try {
    const body = { idempotency_key: key }
    if (note.value.trim()) body.note = note.value.trim()
    const res = await postJSON('/api/requests', body)
    msg.value = res.created
      ? `已入队请求 #${res.request.id}（状态 pending）`
      : `幂等键已存在，回到已有请求 #${res.request.id}（第 ${res.request.dedupe_count} 次去重），未开新编排`
    idempotencyKey.value = ''
    note.value = ''
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function fillSample() {
  idempotencyKey.value = sampleKey.value
}

onMounted(async () => {
  await refresh()
  // Auto refresh so pending -> running -> succeeded transitions are visible.
  timer = setInterval(refresh, 2000)
})
onUnmounted(() => clearInterval(timer))
</script>

<template>
  <div class="page">
    <h1>请求队列</h1>
    <p class="muted">
      外部只通过幂等键投递编排请求；串行消费者按入队顺序取 pending 执行。
      同一幂等键重复提交不会再开新编排，只回到已有请求并累计去重次数。
    </p>

    <p v-if="err" class="tag danger">{{ err }}</p>
    <p v-if="msg" class="tag ok">{{ msg }}</p>

    <div class="panel">
      <h3>入队</h3>
      <div class="form-row">
        <label>
          幂等键 <span class="danger">*</span>
          <input
            v-model="idempotencyKey"
            type="text"
            maxlength="128"
            placeholder="例如 daily-20261112"
            style="width: 18rem"
            @keyup.enter="submit"
          />
        </label>
        <label>
          备注（可选）
          <input
            v-model="note"
            type="text"
            maxlength="500"
            placeholder="这次投递的说明"
            style="width: 20rem"
            @keyup.enter="submit"
          />
        </label>
        <button :disabled="busy" @click="submit">{{ busy ? '提交中…' : '提交入队' }}</button>
        <button class="secondary" type="button" @click="fillSample">填一个示例键</button>
      </div>
    </div>

    <div class="panel" v-if="stats">
      <h3>队列账</h3>
      <div class="stat-grid">
        <div class="stat"><div class="stat-num">{{ stats.total }}</div><div class="stat-label">入队请求数</div></div>
        <div class="stat"><div class="stat-num">{{ stats.deduped_submissions }}</div><div class="stat-label">去重提交数</div></div>
        <div class="stat"><div class="stat-num">{{ stats.pending }}</div><div class="stat-label">pending</div></div>
        <div class="stat"><div class="stat-num">{{ stats.running }}</div><div class="stat-label">running</div></div>
        <div class="stat stat-ok"><div class="stat-num">{{ stats.succeeded }}</div><div class="stat-label">成功</div></div>
        <div class="stat stat-danger"><div class="stat-num">{{ stats.failed }}</div><div class="stat-label">失败</div></div>
      </div>
      <p class="muted small">
        口径：入队数 = 请求列表行数；HTTP 提交总数 = 入队数 + 去重数；
        成功 + 失败 = 已消费请求数 = 关联编排记录数。
      </p>
    </div>

    <div class="panel">
      <h3>队列（{{ rows.length }}）</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th><th>幂等键</th><th>备注</th><th>状态</th>
            <th>去重次数</th><th>编排</th><th>消费者</th>
            <th>入队</th><th>开始</th><th>完成</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in rows" :key="r.id">
            <td>{{ r.id }}</td>
            <td class="mono">{{ r.idempotency_key }}</td>
            <td>{{ r.note || '—' }}</td>
            <td><span class="tag" :class="STATUS_CLASS[r.status] || ''">{{ r.status }}</span></td>
            <td>{{ r.dedupe_count }}</td>
            <td>
              <router-link
                v-if="r.orchestration_id"
                :to="{ path: '/orchestrate', query: { orchestration_id: r.orchestration_id } }"
              >
                #{{ r.orchestration_id }} 报告 →
              </router-link>
              <span v-else class="muted">—</span>
            </td>
            <td class="mono small">{{ r.worker_name || '—' }}</td>
            <td class="mono small">{{ r.enqueued_at }}</td>
            <td class="mono small">{{ r.started_at || '—' }}</td>
            <td class="mono small">{{ r.finished_at || '—' }}</td>
          </tr>
        </tbody>
      </table>
      <p v-if="!rows.length" class="muted">暂无请求，先在上方入队。</p>
    </div>
  </div>
</template>

<style scoped>
.form-row {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
  align-items: flex-end;
}
.form-row label {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
  font-size: 0.9rem;
}
.stat-grid {
  display: flex;
  flex-wrap: wrap;
  gap: 0.75rem;
}
.stat {
  background: #f4f1e8;
  border: 1px solid var(--line);
  border-radius: 10px;
  padding: 0.6rem 1rem;
  min-width: 7rem;
  text-align: center;
}
.stat-num { font-size: 1.5rem; font-weight: 700; }
.stat-label { color: var(--muted); font-size: 0.82rem; }
.stat-ok { background: #d8eadf; }
.stat-danger { background: #f3d7d2; }
.small { font-size: 0.82rem; }
.danger { color: var(--danger); }
</style>

<script setup>
import { computed, onMounted, ref } from 'vue'
import { useRoute } from 'vue-router'
import { getJSON } from '../api'

const route = useRoute()
const history = ref([])
const selectedId = ref(null)
const err = ref('')

const selected = computed(() => history.value.find((o) => o.id === selectedId.value) || null)

async function refresh() {
  history.value = await getJSON('/api/orchestrations')
  if (selectedId.value == null && history.value.length) {
    selectedId.value = history.value[0].id
  }
}

onMounted(async () => {
  try {
    const q = Number(route.query.orchestration_id)
    if (q) selectedId.value = q
    await refresh()
  } catch (e) {
    err.value = String(e.message || e)
  }
})
</script>

<template>
  <div class="page">
    <h1>编排报告</h1>
    <p class="muted">
      编排只能由「请求队列」里的请求驱动，这里不再支持点一下同步跑全图。
      每次消费都会生成一条编排记录，可从队列页面的「报告」链接直达。
    </p>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="selected">
      <h3>编排 #{{ selected.id }} 报告</h3>
      <p>
        状态 <span class="tag" :class="selected.status === 'ok' ? 'ok' : (selected.status === 'failed' ? 'danger' : 'warn')">{{ selected.status }}</span>
        · 开始 <span class="mono">{{ selected.started_at }}</span>
        · 完成 <span class="mono">{{ selected.finished_at || '—' }}</span>
      </p>
      <p>
        succeeded {{ selected.report?.succeeded }} /
        failed {{ selected.report?.failed }} /
        skipped {{ selected.report?.skipped }} /
        attempts {{ selected.report?.attempt_count }}
      </p>
      <p class="mono muted">
        job_total={{ selected.report?.job_total }} ·
        watermarked_jobs={{ selected.report?.watermarked_jobs }} ·
        layers={{ selected.report?.layer_count }}
      </p>
      <p v-if="selected.report?.error" class="tag danger">{{ selected.report.error }}</p>
      <p>
        <router-link :to="{ path: '/attempts', query: { orchestration_id: selected.id } }">查看本次尝试 →</router-link>
        ·
        <router-link to="/watermarks">产出水位 →</router-link>
      </p>
    </div>

    <div class="panel">
      <h3>历史编排</h3>
      <table>
        <thead><tr><th>ID</th><th>状态</th><th>成功</th><th>失败</th><th>跳过</th><th>开始</th><th></th></tr></thead>
        <tbody>
          <tr v-for="o in history" :key="o.id" :class="{ active: o.id === selectedId }">
            <td>{{ o.id }}</td>
            <td><span class="tag" :class="o.status === 'ok' ? 'ok' : (o.status === 'failed' ? 'danger' : 'warn')">{{ o.status }}</span></td>
            <td>{{ o.report?.succeeded }}</td>
            <td>{{ o.report?.failed }}</td>
            <td>{{ o.report?.skipped }}</td>
            <td class="mono">{{ o.started_at }}</td>
            <td><button class="secondary" @click="selectedId = o.id">打开报告</button></td>
          </tr>
        </tbody>
      </table>
      <p v-if="!history.length" class="muted">还没有编排记录，去请求队列投递一个幂等请求。</p>
    </div>
  </div>
</template>

<style scoped>
tr.active { background: #e5eee7; }
</style>

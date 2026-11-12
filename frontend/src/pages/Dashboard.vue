<script setup>
import { onMounted, ref } from 'vue'
import { getJSON } from '../api'
const stats = ref(null)
const err = ref('')
onMounted(async () => {
  try { stats.value = await getJSON('/api/dashboard') } catch (e) { err.value = String(e.message || e) }
})
</script>
<template>
  <div class="page">
    <h1>总览</h1>
    <p class="muted">离线作业依赖编排：看作业数、请求队列账、编排次数与已写出水位的作业数。</p>
    <p v-if="err" class="tag danger">{{ err }}</p>
    <div class="panel" v-if="stats">
      <p>作业 {{ stats.job_count }}（启用 {{ stats.enabled_jobs }}）</p>
      <p>编排 {{ stats.orchestration_count }} · 尝试 {{ stats.attempt_count }}</p>
      <p>已有水位的作业 {{ stats.watermarked_jobs }}</p>
    </div>
    <div class="panel" v-if="stats?.request_queue">
      <h3>请求队列</h3>
      <p>
        入队 {{ stats.request_queue.total }} ·
        去重提交 {{ stats.request_queue.deduped_submissions }}
      </p>
      <p>
        <span class="tag warn">pending {{ stats.request_queue.pending }}</span>
        <span class="tag warn">running {{ stats.request_queue.running }}</span>
        <span class="tag ok">succeeded {{ stats.request_queue.succeeded }}</span>
        <span class="tag danger">failed {{ stats.request_queue.failed }}</span>
      </p>
      <p><router-link to="/requests">打开请求队列 →</router-link></p>
    </div>
  </div>
</template>

# 02-batchgate（闸序）

Batchgate — 离线作业依赖编排：作业定义 → 依赖就绪检查 → 执行/失败重试 → 产出水位与对账。

与 Feedstitch（多源 Feed 归并）主链路不同，禁止做成 Feed 解析/冲突/回看换皮。

## 启动

```bash
docker compose up --build
```

| 入口 | 地址 |
| --- | --- |
| 前端 | http://localhost:5200 |
| API | http://localhost:10200 |

## 主链

外部带幂等键投递请求入队 → 串行消费者按 FIFO 取 pending → 按依赖顺序执行夹具作业 → 写入尝试记录与产出水位 → 请求与编排报告 1:1 对账

## 请求队列与幂等

外部不再有「点一下同步跑全图」入口（`POST /api/orchestrations` 已返回 410），所有编排只能经请求队列触发：

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/requests` | 入队，body 必带 `idempotency_key`、可选 `note`；首次返回 201，同键重复提交返回 200 且只累计 `dedupe_count`，不会再开新编排 |
| GET | `/api/requests` | 请求列表（含状态、去重次数、消费者、关联编排 ID 与时间戳） |
| GET | `/api/requests/stats` | 队列账：入队数 / 去重数 / pending / running / succeeded / failed |
| GET | `/api/requests/{id}` | 单个请求 |
| GET | `/api/requests/{id}/report` | 该请求关联的编排报告（页面深链数据源） |

- 请求状态：`pending` → `running` → `succeeded` / `failed`。
- 并发策略：单进程内**唯一一个**守护线程串行消费；领取用 `BEGIN IMMEDIATE` 原子把最老的 pending 置为 running，执行顺序可由 `worker_name` + `started_at`/`finished_at` 完全审计。
- 崩溃恢复：启动时把残留 `running` 重新置回 `pending`（编排已完成则直接对账，不重跑），半成品编排记为 `aborted`。
- 对账口径：入队数 = 请求列表行数；HTTP 提交总数 = 入队数 + 去重数；`succeeded + failed` = 已消费请求数 = 关联编排记录数，无孤儿编排。
- 页面：「请求队列」可入队、看队列与队列账，并通过「报告」链接打开关联编排报告（2 秒自动刷新状态流转）。

## 种子说明

- **ingest_orders**：无依赖，写出订单水位。
- **dim_customer**：无依赖，写出客户维表水位。
- **fact_order_daily**：依赖上两者成功且水位推进后才可执行（底座仅做“上游成功”检查，严格水位闸与重试预算留给 0-1 题）。

## 技术栈

Python 3.12 + FastAPI + SQLite；Vue 3 + Vite + Nginx。

## 0-1 切口（未实现）

`dep_gate` / `retry_budget` / `watermark_reconcile` / `sla_breach`

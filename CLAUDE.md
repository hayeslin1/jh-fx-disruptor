# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 技术栈

Spring Boot 2.7.18 · JDK 17 · MyBatis-Plus 3.5.2 · LMAX Disruptor 4.0.0 · MySQL 8 · Lombok · FastJSON2

## 构建与运行

```bash
# 构建（dev 环境）
mvn -Denv=dev clean package

# 运行（jar）
java -jar target/hayes-fx-disruptor-dev.jar

# IDE 直接启动：application.yml 的 @profile.active.env@ 不会被 Maven 过滤，
# 必须加 VM 参数：-Dspring.profiles.active=dev
# 或 Run Config → Active Profiles: dev

# 冒烟测试
curl -X POST http://localhost:8080/fx/push \
  -H 'Content-Type: application/json' \
  -d '{"ccyPair":"USDCNY","channelCd":"BOC","buyPrice":7.1234,"sellPrice":7.1256,"blPrice":7.1245,"deliTyp":"00","dtChannelPublish":"20260427","tmChannelPublish":"120000","utcTimes":"2026-04-27T12:00:00.000Z"}'

# 压测（需先启动 Application）
mvn -Denv=dev test -Dtest=FxRatePushTest#pushTest \
  -Dpush.qps=1000 -Dpush.duration=60 -Dpush.workers=32
```

### 本地配置

```bash
cp src/main/resources/application-example.yml src/main/resources/application-dev.yml
# 填入 MySQL 连接信息
```

真实 `application-{dev,prod,rc,test}.yml` 已被 `.gitignore`，只提交 `application-example.yml`。

JDBC URL 必须带 `rewriteBatchedStatements=true` 才能让批量 VALUES 生效。

## 架构

**全链路**：HTTP POST `/fx/push` → Controller → Service (traceId) → Producer → RingBuffer(4096, MULTI) → N 个 `FxRatePersistenceEventHandler`（μs 级，modulo 分片）→ 双内存缓冲 → 定时批量 flush → MySQL。

### 核心模块

| 包 / 类 | 职责 |
|---------|------|
| `config/DisruptorConfig` | 装配 Disruptor Bean + N 个 Handler；实现 `SmartLifecycle(phase=MAX_VALUE)` 优雅停机 |
| `buffer/FxRateLatestBuffer` | `ConcurrentHashMap`，按 `(channelCd, ccyPair, deliTyp)` coalesce 最新值，**按 `UTC_TIMES` 保序** |
| `buffer/FxRateHistoryBuffer` | `LinkedBlockingQueue(100k)`，保留全量历史，队列满时触发 `flushNow()` |
| `flusher/FxRateLatestFlusher` | 每 50 ms drainAll → `upsertBatch`；`SmartLifecycle(phase=MAX_VALUE-100)` |
| `flusher/FxRateHistoryFlusher` | 每 2s drainTo(1000) → `insertBatch`；`SmartLifecycle(phase=MAX_VALUE-100)` |
| `service/FxRatePersistenceService` | 批量落库入口 + 单条重试 + DLQ；`doPersist/doPersistLatestBatch/doPersistHistoryBatch` 各自独立事务 |
| `mapper/FxXrInfMapper.xml` | UPSERT + 时间守卫（`IF(VALUES(UTC_TIMES) >= UTC_TIMES, ...)` 防并发乱序覆盖） |

### 关键设计约束

- **Disruptor 4.0.0 已移除 `WorkerPool`**——用 N 个 Handler + `sequence % workerCount == myIndex` modulo 分片实现等价语义。
- **停机顺序由 phase 保证**：`DisruptorConfig(MAX_VALUE)` 先停让 in-flight 归位；`Flushers(MAX_VALUE-100)` 后停最终 drain。顺序不能颠倒。
- **`@Transactional` 内部调用陷阱**：`FxRatePersistenceService.persist()` 通过 `appCtx.getBean(FxRatePersistenceService.class)` 取 self，绕过 `this.` 直调导致事务失效的问题。
- **历史队列满的背压**：Handler 调 `historyFlusher.flushNow()` 同步 flush 腾空间，再重试 offer；仍失败只记 ERROR 日志，不直接调同步 persist（否则退化回 118ms/条，阻塞 Disruptor）。
- **时间守卫统一用 `UTC_TIMES`**：内存层 `FxRateLatestBuffer.merge()` 和 DB 层 UPSERT 的保序字段均为 `UTC_TIMES`（ISO-8601 字典序与时间序一致）；`DT/TM_CHANNEL_PUBLISH` 仅作业务字段保留。
- **JVM 崩溃会丢数据**：内存 buffer 未 flush 部分无法恢复，上游需具备重推能力。

### 数据表

| 表 | 写入语义 | 唯一/分区键 |
|----|---------|------------|
| `fx_xr_inf` | UPSERT（最新态） | UK `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` |
| `fx_xr_inf_his` | INSERT 全量流水 | IDX `(DT_CTE)` + `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` |
| `fx_xr_inf_dlq` | INSERT 死信（独立事务） | IDX `(TRACE_ID)`, `(DT_CTE)` |

DLQ 触发条件：批量失败 → 回退单条 `persist` 重试 `retry-times`（默认 3）次仍失败 → 写 DLQ，写 DLQ 本身失败只记 ERROR，不再级联。

## 关键配置项

```yaml
app.disruptor.ring-buffer-size: 4096             # 必须 2 的幂
app.disruptor.worker-count: 8                    # modulo 分片数
app.disruptor.wait-strategy: blocking            # blocking|yielding|busy-spin|sleeping
app.disruptor.shutdown-timeout-seconds: 5        # 优雅停机超时
app.fx.order-guard-enabled: true                 # UPSERT 时间守卫防乱序
app.fx.retry-times: 3                            # 单条失败重试次数（不含首次）
app.fx.retry-backoff-ms: 10,50,200               # 指数退避序列（逗号分隔，长度须≥retry-times）
app.fx.dlq-enabled: true                         # 全部重试失败后写 DLQ 表
app.fx.flush.latest-interval-ms: 50             # 报价通道 flush 间隔
app.fx.flush.latest-batch-max-size: 500          # 报价通道单次批量上限
app.fx.flush.history-interval-ms: 2000           # 历史通道 flush 间隔
app.fx.flush.history-batch-max-size: 1000        # 历史通道单次批量上限
app.fx.flush.history-queue-capacity: 100000      # 历史队列容量
app.fx.flush.history-queue-warn-threshold: 50000 # 历史队列高水位告警阈值
```

## 文档

- `docs/fx-async-batch-persist_20260430.md`：当前 v2 双通道异步批量架构完整说明
- `docs/fx-external-api_20260507.md`：对外 API 协议说明
- `docs/fx-rate-disruptor-plan.md`：初始设计规划（含方案对比）
- `docs/fx-push-flow-analysis_20260427.md`：v1 同步路径逐行解析（参考）
- `docs/sql/ddl-fx_xr_inf_dlq.sql`：DLQ 建表脚本

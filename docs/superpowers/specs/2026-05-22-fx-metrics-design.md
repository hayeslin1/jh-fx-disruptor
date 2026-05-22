# FX Disruptor 监控指标设计

**日期**：2026-05-22
**最近更新**：2026-05-22（review v2：补齐 @EnableScheduling、DLQ 失败计数、单位/HDR/字段化等 10 项）
**状态**：已批准，待实现
**需求**：为 Disruptor 双通道异步落库链路增加可观测性，覆盖 RingBuffer 容量、批量落库耗时、历史通道拒绝数及扩展指标。

---

## 一、技术选型

| 层 | 选型 |
|----|------|
| 指标暴露 | Spring Boot Actuator + Micrometer（加 `spring-boot-starter-actuator` 依赖） |
| 周期性日志 | `FxMetricsReporter`（`@Scheduled`）每 60s 打一条结构化 JSON 日志 |
| 指标门面 | 集中式 `FxMetrics` Spring Component；**所有 Meter 以 final 字段持有**，Reporter 直接读字段，不通过 `MeterRegistry.find(name)` 反向查，避免硬编码字符串与 NaN 风险 |
| 调度启用 | **必须在 `Application.java` 加 `@EnableScheduling`**，否则 `FxMetricsReporter` 永远不触发 |

---

## 二、指标全集（16 个）

### Gauge（实时快照，3 个）

| 指标名 | 数据源 | 含义 / 备注 |
|--------|--------|------|
| `fx.ring_buffer.remaining_capacity` | `RingBuffer.remainingCapacity()` | RingBuffer 可写槽位；**注册时附 tag `buffer_size=<ringBufferSize>`**，便于运维直接判断危险度（128/4096 vs 128/65536） |
| `fx.history_buffer.size` | `FxRateHistoryBuffer.size()` | 历史队列当前深度；超 warnThreshold 时已有日志告警 |
| `fx.latest_buffer.size` | `FxRateLatestBuffer.size()` | 报价 ConcurrentHashMap 条目数；反映 coalesce 积压量 |

### Counter（累计，只增不减，8 个）

| 指标名 | increment 时机 | 含义 / 告警优先级 |
|--------|---------------|------|
| `fx.history_buffer.reject` | `FxRatePersistenceEventHandler`：二次 `offer` 仍失败 | 历史通道丢弃事件数；非零即严重 |
| `fx.handler.unexpected_error` | `FxRatePersistenceEventHandler` 整体 try-catch 兜底块（`FxRatePersistenceEventHandler.java:77-81`） | Handler 未预期异常；非零需翻日志定位 |
| `fx.flush.latest.fallback` | `FxRateLatestFlusher`：批量 UPSERT 失败降级逐条 | 报价批次降级次数 |
| `fx.flush.history.fallback` | `FxRateHistoryFlusher`：批量 INSERT 失败降级逐条；**两处都要计：`flushOnce()` 第 130 行 + `drainUntilEmpty()` 第 156 行** | 历史批次降级次数（含 shutdown 阶段） |
| `fx.persist.retry` | `FxRatePersistenceService.persist()` 第 54-68 行：每次进入第 i≥1 次重试时 +1 | 单条重试次数；早于 P99 抖动出现，是稳定性早期信号 |
| `fx.persist.giveup` | `FxRatePersistenceService.persist()`：跑完 `attempts` 仍失败（无论 DLQ 是否开） | 重试放弃次数；与 `fx.dlq.write` 不重叠（DLQ 可能 disabled） |
| `fx.dlq.write` | `FxRatePersistenceService.writeDlq()` 成功写入后 | DLQ 写入次数；非零即触发告警 |
| **`fx.dlq.write.failure`** | `FxRatePersistenceService.persist()` 第 77-79 行：DLQ 也写失败的 catch 块 | **P0 告警**——DLQ 不可用意味着数据真丢，必须独立计数 |

### Timer（延迟分布，含 count / sum / max / P50 / P95 / P99，3 个）

| 指标名 | 包裹逻辑 | 含义 |
|--------|---------|------|
| `fx.producer.publish.duration` | `ringBuffer.publishEvent(...)` | RingBuffer 投递耗时；BlockingWaitStrategy 下满载时 P99 飙升 |
| `fx.flush.latest.batch.duration` | `doPersistLatestBatch(chunk)` | 报价批量 UPSERT 耗时 |
| `fx.flush.history.batch.duration` | `doPersistHistoryBatch(batch)` | 历史批量 INSERT 耗时 |

**Timer 构造统一规范**（必须显式开启 HDR + 滚动窗口，否则 P99 长期偏差或为 NaN）：

```java
Timer.builder("fx.xxx.duration")
     .publishPercentiles(0.5, 0.95, 0.99)
     .publishPercentileHistogram()                       // 开 HDR 直方图
     .distributionStatisticExpiry(Duration.ofMinutes(1)) // 1min 滚动
     .distributionStatisticBufferLength(5)
     .register(registry);
```

### DistributionSummary（批量大小分布，2 个）

| 指标名 | 含义 |
|--------|------|
| `fx.flush.latest.batch.size` | 每次 flush 的报价条数分布 |
| `fx.flush.history.batch.size` | 每次 flush 的历史条数分布 |

构造同样需 `publishPercentileHistogram()` + 滚动窗口配置。

---

## 三、新增文件

### `monitor/FxMetrics.java`

```
职责：
- 构造器注入 MeterRegistry、RingBuffer、FxRateLatestBuffer、FxRateHistoryBuffer
- 注册 3 个 Gauge：
    * fx.ring_buffer.remaining_capacity 带 tag buffer_size=<ringBuffer.getBufferSize()>
    * 三个 Gauge 均通过 Gauge.builder(name, beanRef, ToDoubleFunction) 注册，
      bean 由 Spring 强引用，无 weak ref 失效风险
- 持有 8 Counter + 3 Timer + 2 DistributionSummary 字段（全部 final，编译期防误写指标名）
- Timer / DistributionSummary 显式开 publishPercentileHistogram() + 1min 滚动窗口
- 对外暴露语义化方法：
    incHistoryReject()
    incHandlerUnexpectedError()
    incLatestFallback()
    incHistoryFallback()
    incPersistRetry()
    incPersistGiveup()
    incDlqWrite()
    incDlqWriteFailure()
    recordPublish(long nanos)
    recordLatestFlush(int batchSize, long nanos)
    recordHistoryFlush(int batchSize, long nanos)
- 暴露 getter（package-private）供 FxMetricsReporter 取 Snapshot
- 不包含任何业务逻辑，不依赖 FxProperties
```

### `monitor/FxMetricsReporter.java`

```
职责：
- @Scheduled(fixedDelayString = "${app.fx.flush.metrics-log-interval-ms:60000}")
- 方法体首行：if (intervalMs <= 0) return;  ← 0 = 禁用
  （@ConditionalOnProperty 不支持"非零判断"，直接在方法体判即可）
- 通过注入的 FxMetrics 直接读 final 字段，调用 timer.takeSnapshot().percentileValues()
  拿 P50/P95/P99；DistributionSummary 同理
- log.info("[fx-metrics] {}", jsonSnapshot)  ← 一条完整 JSON
- 不通过 MeterRegistry.find(name) 反向查（脆 + 慢 + 易拼写错）
```

---

## 四、改动现有文件

| 文件 | 改动内容 | 幅度 |
|------|----------|------|
| `pom.xml` | 加 `spring-boot-starter-actuator` | 3 行 |
| `application.yml` | 暴露 `metrics,health`；加 `metrics-log-interval-ms: 60000`（注意：与 `latestIntervalMs/historyIntervalMs` 单位一致用 ms） | 8 行 |
| **`Application.java`** | **新增 `@EnableScheduling`，否则 `@Scheduled` 不生效** | 2 行 |
| `FxProperties.java` | `Flush` 内加 `metricsLogIntervalMs` 字段（默认 60000） | 3 行 |
| `FxRateEventProducer.java` | 注入 `FxMetrics`；用 `recordPublish()` 包裹 publish；**删除当前第 33 行的 `log.info("...耗时...")`**——INFO 级每条都打，1k QPS 灌爆日志 | 6 行 |
| `FxRateLatestFlusher.java` | 注入 `FxMetrics`；`flushOnce()` 记录 batchSize、duration、fallback 计数 | 10 行 |
| `FxRateHistoryFlusher.java` | 注入 `FxMetrics`；`flushOnce()` + **`drainUntilEmpty()` 两处都要 `incHistoryFallback()`**；记录 batchSize、duration | 14 行 |
| `FxRatePersistenceEventHandler.java` | 构造器加 `FxMetrics` 参数；二次 `offer` 失败调 `incHistoryReject()`；外层 catch 块（第 77-81 行）调 `incHandlerUnexpectedError()` | 6 行 |
| `FxRatePersistenceService.java` | 注入 `FxMetrics`；重试循环内每次 i≥1 调 `incPersistRetry()`；attempts 全败调 `incPersistGiveup()`；`writeDlq` 成功后调 `incDlqWrite()`；**第 77-79 行 DLQ 失败 catch 调 `incDlqWriteFailure()`** | 8 行 |
| `DisruptorConfig.java` | 构造器注入 `FxMetrics`，`ringBuffer()` 方法创建 Handler 时透传 | 4 行 |

**不改动**：`FxRateLatestBuffer`、`FxRateHistoryBuffer`（Gauge 通过 `size()` 方法引用接入，零侵入）

---

## 五、配置项

```yaml
# application.yml 新增
management:
  endpoints:
    web:
      exposure:
        include: metrics,health
  endpoint:
    metrics:
      enabled: true
# 生产部署提示：默认 actuator 暴露在业务端口 8080/actuator/*。
# 若部署到非纯内网环境，建议追加 management.server.port: 9090 单独分管理端口，
# 或加 spring-security basic auth 鉴权（本 spec 不强制，按运维策略落地）

app:
  fx:
    flush:
      metrics-log-interval-ms: 60000   # 结构化日志快照间隔（毫秒），0=禁用；与 latestIntervalMs/historyIntervalMs 命名/单位一致
```

---

## 六、结构化日志格式

每 60s 输出一条，字段说明：
- `_total`：进程启动后累计值
- `_p99_ms` / `_max_ms`：Timer 的 HDR 滚动 1min 窗口分位（依赖 `publishPercentileHistogram()`）
- `_p99`：DistributionSummary 的分位值

```json
{
  "ts": "2026-05-22T10:00:00.000Z",
  "ring_buffer_remaining": 3841,
  "ring_buffer_size": 4096,
  "history_queue_size": 1024,
  "latest_buffer_size": 12,
  "history_reject_total": 0,
  "handler_unexpected_error_total": 0,
  "latest_fallback_total": 0,
  "history_fallback_total": 0,
  "persist_retry_total": 0,
  "persist_giveup_total": 0,
  "dlq_write_total": 0,
  "dlq_write_failure_total": 0,
  "publish_p99_ms": 0.12,
  "publish_max_ms": 1.43,
  "latest_flush_p99_ms": 8.5,
  "latest_flush_max_ms": 23.1,
  "latest_batch_size_p99": 48,
  "history_flush_p99_ms": 31.2,
  "history_flush_max_ms": 89.4,
  "history_batch_size_p99": 982
}
```

字段中文对照：

| 字段名 | 含义 | 类型来源 |
|--------|------|---------|
| `ts` | 快照采集时间（ISO-8601 UTC） | Reporter 自填 |
| `ring_buffer_remaining` | RingBuffer 剩余可写槽位 | Gauge |
| `ring_buffer_size` | RingBuffer 总容量（固定值，用于换算占用率） | Gauge tag → 平铺到字段 |
| `history_queue_size` | 历史队列当前堆积条数 | Gauge |
| `latest_buffer_size` | 报价缓存（coalesce map）当前条目数 | Gauge |
| `history_reject_total` | 历史队列二次 offer 仍失败的累计丢弃数（**非零即严重**） | Counter |
| `handler_unexpected_error_total` | Handler 外层兜底 catch 累计触发次数 | Counter |
| `latest_fallback_total` | 报价批量 UPSERT 失败 → 逐条降级累计次数 | Counter |
| `history_fallback_total` | 历史批量 INSERT 失败 → 逐条降级累计次数（含 shutdown 阶段） | Counter |
| `persist_retry_total` | 单条 persist 进入第 2 次及以后重试的累计次数 | Counter |
| `persist_giveup_total` | 重试全部用尽仍失败的累计次数 | Counter |
| `dlq_write_total` | DLQ 成功写入累计次数（**非零告警**） | Counter |
| `dlq_write_failure_total` | DLQ 写入也失败的累计次数（**P0 告警，数据真丢**） | Counter |
| `publish_p99_ms` | RingBuffer 投递耗时 P99（毫秒，1min 滚动窗口） | Timer |
| `publish_max_ms` | RingBuffer 投递耗时窗口内最大值 | Timer |
| `latest_flush_p99_ms` | 报价批量 UPSERT 耗时 P99 | Timer |
| `latest_flush_max_ms` | 报价批量 UPSERT 耗时窗口内最大值 | Timer |
| `latest_batch_size_p99` | 单次报价 flush 条数 P99 | DistributionSummary |
| `history_flush_p99_ms` | 历史批量 INSERT 耗时 P99 | Timer |
| `history_flush_max_ms` | 历史批量 INSERT 耗时窗口内最大值 | Timer |
| `history_batch_size_p99` | 单次历史 flush 条数 P99 | DistributionSummary |

---

## 七、Actuator 使用示例

```bash
# 列出所有 fx.* 指标
curl http://localhost:8080/actuator/metrics | jq '.names[] | select(startswith("fx"))'

# 查看 RingBuffer 容量
curl http://localhost:8080/actuator/metrics/fx.ring_buffer.remaining_capacity

# 查看投递延迟（含 P99）
curl http://localhost:8080/actuator/metrics/fx.producer.publish.duration
```

---

## 八、告警分级建议（与新增指标配套）

| 优先级 | 触发条件 | 含义 |
|--------|---------|------|
| **P0** | `fx.dlq.write.failure > 0` | DLQ 不可用，数据真丢 |
| P1 | `fx.history_buffer.reject > 0` | 历史通道丢事件 |
| P1 | `fx.dlq.write > 0` | 进入死信，需人工处理 |
| P2 | `fx.handler.unexpected_error > 0` | Handler 异常，需排查 |
| P2 | `fx.persist.giveup` rate 抬头 | 重试全败率上升 |
| P3 | `fx.persist.retry` rate 抬头 | DB 抖动早期信号 |
| P3 | `fx.flush.*.fallback` rate 抬头 | 批量降级，DB 负载偏高 |

---

## 九、验收标准

1. 启动后 `/actuator/metrics` 能枚举出全部 16 个 `fx.*` 指标名
2. 压测 1000 QPS 60s 后，日志中出现至少 1 条 `[fx-metrics]` JSON 快照（验证 `@EnableScheduling` 已生效）
3. 模拟 historyBuffer 满：日志中 `history_reject_total > 0`，Actuator 对应 Counter 值一致
4. 批量 UPSERT 耗时 Timer 的 `fx.flush.latest.batch.duration` P99 ≤ 50ms（正常负载）
5. **`fx.producer.publish.duration` 的 P99 在 Actuator 端点返回非 NaN**（验证 HDR 已正确开启）
6. **构造异常注入：让 `writeDlq` 抛错一次，确认 `fx.dlq.write.failure` 自增 1**（验证 P0 计数路径）
7. 配置 `metrics-log-interval-ms: 0` 重启，确认 `[fx-metrics]` 日志不再输出（验证禁用开关）

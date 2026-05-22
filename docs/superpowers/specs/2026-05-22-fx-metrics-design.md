# FX Disruptor 监控指标设计

**日期**：2026-05-22  
**状态**：已批准，待实现  
**需求**：为 Disruptor 双通道异步落库链路增加可观测性，覆盖 RingBuffer 容量、批量落库耗时、历史通道拒绝数及 8 个扩展指标。

---

## 一、技术选型

| 层 | 选型 |
|----|------|
| 指标暴露 | Spring Boot Actuator + Micrometer（加 `spring-boot-starter-actuator` 依赖） |
| 周期性日志 | `FxMetricsReporter`（`@Scheduled`）每 60s 打一条结构化 JSON 日志 |
| 指标门面 | 集中式 `FxMetrics` Spring Component，其他类只调用语义化方法，不直接接触 MeterRegistry |

---

## 二、指标全集（12 个）

### Gauge（实时快照）

| 指标名 | 数据源 | 含义 |
|--------|--------|------|
| `fx.ring_buffer.remaining_capacity` | `RingBuffer.remainingCapacity()` | RingBuffer 可写槽位；趋近 0 表示生产者即将阻塞 |
| `fx.history_buffer.size` | `FxRateHistoryBuffer.size()` | 历史队列当前深度；超 warnThreshold 时已有日志告警 |
| `fx.latest_buffer.size` | `FxRateLatestBuffer.size()` | 报价 ConcurrentHashMap 条目数；反映 coalesce 积压量 |

### Counter（累计，只增不减）

| 指标名 | increment 时机 | 含义 |
|--------|---------------|------|
| `fx.history_buffer.reject` | `FxRatePersistenceEventHandler`：二次 `offer` 仍失败 | 历史通道丢弃事件数；非零即严重 |
| `fx.flush.latest.fallback` | `FxRateLatestFlusher`：批量 UPSERT 失败降级逐条时 | 报价批次降级次数 |
| `fx.flush.history.fallback` | `FxRateHistoryFlusher`：批量 INSERT 失败降级逐条时 | 历史批次降级次数 |
| `fx.dlq.write` | `FxRatePersistenceService.writeDlq()` 成功写入后 | DLQ 写入次数；非零即触发告警 |

### Timer（延迟分布，含 count / sum / max / P50 / P95 / P99）

| 指标名 | 包裹逻辑 | 含义 |
|--------|---------|------|
| `fx.producer.publish.duration` | `ringBuffer.publishEvent(...)` | RingBuffer 投递耗时；BlockingWaitStrategy 下满载时 P99 飙升 |
| `fx.flush.latest.batch.duration` | `doPersistLatestBatch(chunk)` | 报价批量 UPSERT 耗时 |
| `fx.flush.history.batch.duration` | `doPersistHistoryBatch(batch)` | 历史批量 INSERT 耗时 |

### DistributionSummary（批量大小分布）

| 指标名 | 含义 |
|--------|------|
| `fx.flush.latest.batch.size` | 每次 flush 的报价条数分布 |
| `fx.flush.history.batch.size` | 每次 flush 的历史条数分布 |

---

## 三、新增文件

### `monitor/FxMetrics.java`

```
职责：
- 构造器注入 MeterRegistry、RingBuffer、FxRateLatestBuffer、FxRateHistoryBuffer
- 注册 3 个 Gauge（方法引用，零侵入 buffer 内部）
- 持有 4 Counter + 2 Timer（publishPercentiles(0.5,0.95,0.99)）+ 2 DistributionSummary 字段
- 对外暴露语义化方法：
    incHistoryReject()
    incLatestFallback()
    incHistoryFallback()
    incDlqWrite()
    recordPublish(long nanos)
    recordLatestFlush(int batchSize, long nanos)
    recordHistoryFlush(int batchSize, long nanos)
- 不包含任何业务逻辑，不依赖 FxProperties
```

### `monitor/FxMetricsReporter.java`

```
职责：
- @Scheduled fixedDelayString = "${app.fx.flush.metrics-log-interval-seconds:60}000"
- 通过 MeterRegistry 查询所有 fx.* 指标的当前值
- log.info("[fx-metrics] {}", jsonSnapshot)  ← 一条完整 JSON
- metrics-log-interval-seconds = 0 时禁用（@ConditionalOnProperty 或 if 判断）
```

---

## 四、改动现有文件

| 文件 | 改动内容 | 幅度 |
|------|----------|------|
| `pom.xml` | 加 `spring-boot-starter-actuator` | 3 行 |
| `application.yml` | 暴露 `metrics,health`；加 `metrics-log-interval-seconds: 60` | 6 行 |
| `FxProperties.java` | `Flush` 内加 `metricsLogIntervalSeconds` 字段（默认 60） | 3 行 |
| `FxRateEventProducer.java` | 注入 `FxMetrics`；`publish()` 的 `System.nanoTime()` 计时改为 `recordPublish()` | 5 行 |
| `FxRateLatestFlusher.java` | 注入 `FxMetrics`；`flushOnce()` 记录 batchSize、duration、fallback | 8 行 |
| `FxRateHistoryFlusher.java` | 同上，历史通道 | 8 行 |
| `FxRatePersistenceEventHandler.java` | 构造器加 `FxMetrics` 参数；二次 `offer` 失败处调用 `incHistoryReject()` | 4 行 |
| `FxRatePersistenceService.java` | 注入 `FxMetrics`；`writeDlq()` 成功后调用 `incDlqWrite()` | 3 行 |
| `DisruptorConfig.java` | 构造器注入 `FxMetrics`，`ringBuffer()` 方法创建 Handler 时透传 | 4 行 |

**不改动**：`FxRateLatestBuffer`、`FxRateHistoryBuffer`

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

app:
  fx:
    flush:
      metrics-log-interval-seconds: 60   # 结构化日志快照间隔（秒），0=禁用
```

---

## 六、结构化日志格式

每 60s 输出一条，字段说明：
- `_total`：进程启动后累计值
- `_p99_ms` / `_max_ms`：Micrometer 默认 2min 滚动窗口内分布统计
- `_p99`：DistributionSummary 的分位值

```json
{
  "ts": "2026-05-22T10:00:00.000Z",
  "ring_buffer_remaining": 3841,
  "history_queue_size": 1024,
  "latest_buffer_size": 12,
  "history_reject_total": 0,
  "latest_fallback_total": 0,
  "history_fallback_total": 0,
  "dlq_write_total": 0,
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

## 八、验收标准

1. 启动后 `/actuator/metrics` 能枚举出全部 12 个 `fx.*` 指标名
2. 压测 1000 QPS 60s 后，日志中出现至少 1 条 `[fx-metrics]` JSON 快照
3. 模拟 historyBuffer 满：日志中 `history_reject_total > 0`，Actuator 对应 Counter 值一致
4. 批量 UPSERT 耗时 Timer 的 `fx.flush.latest.batch.duration` P99 ≤ 50ms（正常负载）

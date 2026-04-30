# 双通道异步批量落库（v2）

> 分支：`feat/async-batch-persist` · 基线：`master`（v1 同步单条落库）
> 问题：v1 `onEvent` ~118 ms，生产者 ~17 μs，差 ~6900×，高频推送下 Disruptor 堆积、生产者被 gating sequence 阻塞
> 目标：消费侧 μs 级入队，报价延迟 ≤ 50 ms，历史延迟 ≤ 2 s，JVM 正常停机不丢数据

---

## 一、核心洞察

两张表 SLA 数量级差异，不该共用同一事务：

| 表 | 角色 | SLA |
|---|---|---|
| `fx_xr_inf` | 对客报价源 | 实时查询，同 `(channelCd, ccyPair, deliTyp)` 只关心最新值，中间值可丢 |
| `fx_xr_his` | 审计流水 | 不报价、极少查询，不丢即可，晚 2 分钟 OK |

v1 强制 `UPSERT + INSERT` 同事务 = 性能漏斗。v2 拆成独立双通道。

---

## 二、架构

```
┌─────────────────────────────────────────────────────────┐
│ Disruptor Handler（μs 级，纯内存）                      │
│   latestBuffer.merge(key, event.copy())    ← 保序覆盖   │
│   historyBuffer.offer(event.copy())        ← FIFO 入队  │
└──────────┬────────────────────────┬─────────────────────┘
           │                        │
   ┌───────▼─────────┐    ┌─────────▼──────────┐
   │ LatestFlusher    │    │ HistoryFlusher      │
   │ 每 50 ms drainAll│    │ 每 2 s drainTo(1000)│
   │ 批量 UPSERT      │    │ 批量 INSERT         │
   │ fx_xr_inf        │    │ fx_xr_his           │
   └───────┬─────────┘    └─────────┬──────────┘
           ▼                         ▼
         MySQL                     MySQL
```

- `latestBuffer`：`ConcurrentHashMap<LatestKey, FxRateEvent>`，`merge` 按 `DT+TM_CHANNEL_PUBLISH` 字典序保留较新值（coalescing）。
- `historyBuffer`：`LinkedBlockingQueue<FxRateEvent>(100_000)`，不合并，保留全量。

---

## 三、关键代码

| 路径 | 职责 |
|---|---|
| `buffer/LatestKey.java` | `(channelCd, ccyPair, deliTyp)` 复合 key，缓存 hashCode |
| `buffer/FxRateLatestBuffer.java` | ConcurrentHashMap 包装；`merge` 时间戳保序；`drainAll` 原子快照 |
| `buffer/FxRateHistoryBuffer.java` | LinkedBlockingQueue 包装；`offer/drainTo/drainAll`；高水位节流告警 |
| `flusher/FxRateLatestFlusher.java` | SmartLifecycle，50 ms 调度，`drainAll → doPersistLatestBatch` |
| `flusher/FxRateHistoryFlusher.java` | SmartLifecycle，2 s 调度，`drainTo → doPersistHistoryBatch`；`flushNow()` 供 Handler 背压 |
| `disruptor/handler/FxRatePersistenceEventHandler.java` | 双通道入队，异常兜底不冒出；buffer 满 → `flushNow()` 再重试一次 |
| `service/FxRatePersistenceService.java` | 新增 `doPersistLatestBatch / doPersistHistoryBatch`，整批失败逐条回退老 `persist` 走 DLQ |
| `mapper/FxXrInfMapper.xml` | 新增 `upsertBatch`（`<foreach>` 多 VALUES，保留 orderGuard 行级生效） |
| `mapper/FxXrInfHisMapper.xml` | 新增 `insertBatch` 多 VALUES |
| `config/FxProperties.java` | 新增 `Flush` 配置段（见下） |
| `config/DisruptorConfig.java` | 改为 SmartLifecycle，phase = `Integer.MAX_VALUE` |

---

## 四、Handler μs 级入队

`FxRatePersistenceEventHandler.onEvent` 仅做两步内存操作：

```java
FxRateEvent forLatest = event.copy();
LatestKey key = new LatestKey(forLatest.getChannelCd(), forLatest.getCcyPair(), forLatest.getDeliTyp());
latestBuffer.merge(key, forLatest);

FxRateEvent forHis = event.copy();
if (!historyBuffer.offer(forHis)) {
    historyFlusher.flushNow();          // 背压：同步 drain 一次
    if (!historyBuffer.offer(forHis)) { // 仍失败：ERROR 日志，不阻塞 Disruptor
        log.error("[fx-rate] history buffer full after flushNow ...");
    }
}
```

- **Disruptor 事件实例复用**，入 map/queue 前**必须深拷贝** `FxRateEvent.copy()`。
- modulo 分片保留（`sequence % workerCount == myIndex`），现在瓶颈是内存 op，单 handler 也够，但保留分片便于未来扩展 CPU 密集计算。
- 异常全部 catch，防止 Disruptor 停机。

---

## 五、刷盘触发条件

### Latest 通道（`fx_xr_inf`）

| 触发点 | 频率 | 备注 |
|---|---|---|
| `scheduleAtFixedRate` 50 ms tick | 定时 | 稳态主路径 |
| 停机 `stop()` 最后一次 `flushOnceSilently` | 一次性 | 保证不丢最新态 |

按 `latestBatchMaxSize`（默认 500）分片提交，避免单事务过大。

### History 通道（`fx_xr_his`）

| 触发点 | 频率 | 备注 |
|---|---|---|
| `scheduleAtFixedRate` 2 s tick | 定时 | 稳态主路径 |
| Handler `offer()` 队列满 → `flushNow()` | 事件驱动 | 背压兜底 |
| 停机 `stop()` → `drainUntilEmpty()` safetyLimit=100 | 一次性 | 保证不丢历史 |

> **注**：`historyBatchMaxSize=1000` 是**单次 drain 的上限**，不是触发条件。上游停推、队列只有 N（N < 1000）时，下一个 2 s tick 仍会把 N 条全部写入。

---

## 六、失败降级

两个批量方法整批失败时，走**原 v1 路径**：

```java
for (FxRateEvent e : batch) {
    try { persistenceService.persist(e); } catch (Throwable ignored) { /* 已 DLQ */ }
}
```

- `persist()` 内含 3 次 `[10, 50, 200] ms` 指数退避重试 + DLQ 独立事务写入。
- Latest 通道失败回退会重复 UPSERT 最新态，但幂等安全（仍按 `orderGuard` 保序）。

---

## 七、关停顺序（关键防丢）

SmartLifecycle `stop()` 按 `phase` **降序**触发。

| 组件 | phase | 何时 stop |
|---|---|---|
| `DisruptorConfig` | `Integer.MAX_VALUE` | **先停**：等 in-flight handler 返回，最后一批事件进入 buffer |
| `FxRateLatestFlusher` / `FxRateHistoryFlusher` | `Integer.MAX_VALUE - 100` | 后停：调度器 shutdown + 最后 drain |

```
容器关闭：
  1. DisruptorConfig.stop()        ← disruptor.shutdown(timeout) 等消费完
  2. FxRateLatestFlusher.stop()    ← 调度器停 + 最后一次 flushOnce
  3. FxRateHistoryFlusher.stop()   ← 调度器停 + drainUntilEmpty 直到为空
  4. DataSource 关闭（Spring 默认）
```

> 若 JVM 进程崩溃（OOM / SIGKILL），内存 buffer 未 flush 的部分会丢——**需上游具备重推能力**。真同步强一致路径（Outbox）留待接入 HTTP 同步推送业务方时再做。

---

## 八、新增配置

```yaml
app:
  fx:
    flush:
      latest-interval-ms: 50          # 报价 flush 间隔
      latest-batch-max-size: 500      # 单批 UPSERT 上限
      history-interval-ms: 2000       # 历史 flush 间隔
      history-batch-max-size: 1000    # 单批 INSERT 上限
      history-queue-capacity: 100000  # history queue 容量
      history-queue-warn-threshold: 50000  # 高水位告警阈值
```

JDBC URL 建议启用 `rewriteBatchedStatements=true`，让 `<foreach>` 批量生效。

---

## 九、预期收益（1000 TPS / 20 ccyPair 估算）

| 指标 | v1（同步） | v2（异步批量） |
|---|---|---|
| `onEvent` 耗时 | ~118 ms | **~5 μs** |
| `fx_xr_inf` UPSERT QPS | 1000 | **~400**（coalesce 后）|
| `fx_xr_his` 写事务数 | 1000/s | **~0.5/s**（2 s 批）|
| 报价查询滞后 | 立即 | **≤ 50 ms** |
| 历史查询滞后 | 立即 | **≤ 2 s** |

---

## 十、待办

- [ ] `FxRateLatestBufferTest` / `FxRateHistoryBufferTest` 并发单测
- [ ] `DisruptorMain` 改造做 baseline vs v2 压测对比
- [ ] SIGTERM 触发 shutdown 不丢数据校验
- [ ] Prometheus 指标：`latestBuffer.size`、`historyBuffer.size`、每轮 batch size / 耗时、降级次数

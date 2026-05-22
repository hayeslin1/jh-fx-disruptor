package com.hayes.base.fx.monitor;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.lmax.disruptor.RingBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 指标门面：集中持有所有 fx.* Meter 实例。
 * <p>
 * 设计要点：
 * 1. 3 Gauge 通过方法引用零侵入接入 RingBuffer / 两个 Buffer，bean 由 Spring 强引用持有，无 weak ref 失效风险；
 *    `fx.ring_buffer.remaining_capacity` 附 `buffer_size` tag 便于运维直接判断危险度；
 * 2. 8 Counter / 3 Timer / 2 DistributionSummary 全部 final 字段持有，对外只暴露语义化方法 incXxx/recordXxx；
 *    Reporter 通过 package-private getter 直接取引用，避免按 name 反向查询的脆弱与拼写错误；
 * 3. Timer / DS 显式开 publishPercentileHistogram + 1min 滚动窗口（5 桶），
 *    否则在线无衰减算法下 P99 长期偏差或为 NaN。
 */
@Component
public class FxMetrics {

    // ===== Counter =====
    private final Counter historyReject;
    private final Counter handlerUnexpectedError;
    private final Counter latestFallback;
    private final Counter historyFallback;
    private final Counter persistRetry;
    private final Counter persistGiveup;
    private final Counter dlqWrite;
    private final Counter dlqWriteFailure;

    // ===== Timer =====
    private final Timer publishTimer;
    private final Timer latestFlushTimer;
    private final Timer historyFlushTimer;

    // ===== DistributionSummary =====
    private final DistributionSummary latestBatchSize;
    private final DistributionSummary historyBatchSize;

    public FxMetrics(MeterRegistry registry,
                     RingBuffer<FxRateEvent> ringBuffer,
                     FxRateLatestBuffer latestBuffer,
                     FxRateHistoryBuffer historyBuffer) {
        // ===== Gauge：方法引用接入，不持字段（注册到 registry 即可由 Reporter 按 name 取）=====
        Gauge.builder("fx.ring_buffer.remaining_capacity", ringBuffer,
                        rb -> (double) rb.remainingCapacity())
                .description("RingBuffer 剩余可写槽位")
                .tag("buffer_size", String.valueOf(ringBuffer.getBufferSize()))
                .register(registry);

        Gauge.builder("fx.history_buffer.size", historyBuffer,
                        hb -> (double) hb.size())
                .description("历史队列当前堆积条数")
                .register(registry);

        Gauge.builder("fx.latest_buffer.size", latestBuffer,
                        lb -> (double) lb.size())
                .description("报价缓存 coalesce map 条目数")
                .register(registry);

        // ===== Counter =====
        this.historyReject = Counter.builder("fx.history_buffer.reject")
                .description("历史队列二次 offer 仍失败累计")
                .register(registry);
        this.handlerUnexpectedError = Counter.builder("fx.handler.unexpected_error")
                .description("Handler 外层兜底 catch 累计触发")
                .register(registry);
        this.latestFallback = Counter.builder("fx.flush.latest.fallback")
                .description("报价批量 UPSERT 失败降级逐条累计")
                .register(registry);
        this.historyFallback = Counter.builder("fx.flush.history.fallback")
                .description("历史批量 INSERT 失败降级逐条累计（含 shutdown）")
                .register(registry);
        this.persistRetry = Counter.builder("fx.persist.retry")
                .description("单条 persist 进入第 2 次及以后重试累计")
                .register(registry);
        this.persistGiveup = Counter.builder("fx.persist.giveup")
                .description("重试全部用尽仍失败累计")
                .register(registry);
        this.dlqWrite = Counter.builder("fx.dlq.write")
                .description("DLQ 成功写入累计")
                .register(registry);
        this.dlqWriteFailure = Counter.builder("fx.dlq.write.failure")
                .description("DLQ 写入也失败累计（P0：数据真丢）")
                .register(registry);

        // ===== Timer =====
        this.publishTimer = buildTimer(registry, "fx.producer.publish.duration",
                "RingBuffer 投递耗时");
        this.latestFlushTimer = buildTimer(registry, "fx.flush.latest.batch.duration",
                "报价批量 UPSERT 耗时");
        this.historyFlushTimer = buildTimer(registry, "fx.flush.history.batch.duration",
                "历史批量 INSERT 耗时");

        // ===== DistributionSummary =====
        this.latestBatchSize = buildSummary(registry, "fx.flush.latest.batch.size",
                "单次报价 flush 条数");
        this.historyBatchSize = buildSummary(registry, "fx.flush.history.batch.size",
                "单次历史 flush 条数");
    }

    /** 统一 Timer 构造：HDR 直方图 + 1min 滚动窗口（5 桶） */
    private static Timer buildTimer(MeterRegistry r, String name, String desc) {
        return Timer.builder(name)
                .description(desc)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .distributionStatisticBufferLength(5)
                .register(r);
    }

    /** 统一 DS 构造：同 Timer */
    private static DistributionSummary buildSummary(MeterRegistry r, String name, String desc) {
        return DistributionSummary.builder(name)
                .description(desc)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .distributionStatisticBufferLength(5)
                .register(r);
    }

    // ===== Counter 语义化方法 =====
    public void incHistoryReject()         { historyReject.increment(); }
    public void incHandlerUnexpectedError() { handlerUnexpectedError.increment(); }
    public void incLatestFallback()         { latestFallback.increment(); }
    public void incHistoryFallback()        { historyFallback.increment(); }
    public void incPersistRetry()           { persistRetry.increment(); }
    public void incPersistGiveup()          { persistGiveup.increment(); }
    public void incDlqWrite()               { dlqWrite.increment(); }
    public void incDlqWriteFailure()        { dlqWriteFailure.increment(); }

    // ===== Timer / DS 语义化方法 =====
    /** 记录一次 publish 耗时（入参单位：纳秒） */
    public void recordPublish(long nanos) {
        publishTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    /** 记录一次报价 flush：batchSize 进 DS，nanos 进 Timer */
    public void recordLatestFlush(int batchSize, long nanos) {
        latestFlushTimer.record(nanos, TimeUnit.NANOSECONDS);
        latestBatchSize.record(batchSize);
    }

    /** 记录一次历史 flush：batchSize 进 DS，nanos 进 Timer */
    public void recordHistoryFlush(int batchSize, long nanos) {
        historyFlushTimer.record(nanos, TimeUnit.NANOSECONDS);
        historyBatchSize.record(batchSize);
    }

    // ===== package-private getter（仅供 FxMetricsReporter 取 Snapshot）=====
    Counter historyRejectCounter()         { return historyReject; }
    Counter handlerUnexpectedErrorCounter() { return handlerUnexpectedError; }
    Counter latestFallbackCounter()         { return latestFallback; }
    Counter historyFallbackCounter()        { return historyFallback; }
    Counter persistRetryCounter()           { return persistRetry; }
    Counter persistGiveupCounter()          { return persistGiveup; }
    Counter dlqWriteCounter()               { return dlqWrite; }
    Counter dlqWriteFailureCounter()        { return dlqWriteFailure; }

    Timer publishTimer()         { return publishTimer; }
    Timer latestFlushTimer()     { return latestFlushTimer; }
    Timer historyFlushTimer()    { return historyFlushTimer; }
    DistributionSummary latestBatchSizeSummary()  { return latestBatchSize; }
    DistributionSummary historyBatchSizeSummary() { return historyBatchSize; }
}

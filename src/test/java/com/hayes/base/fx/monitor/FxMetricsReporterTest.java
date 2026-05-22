package com.hayes.base.fx.monitor;

import com.alibaba.fastjson2.JSONObject;
import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reporter 单测：验证 JSON 字段完整 + 字段值与 Meter 状态一致。
 * <p>
 * 不测 @Scheduled 触发（那是 Spring 行为，不需要二次验证），只测 report() 方法体。
 */
class FxMetricsReporterTest {

    private SimpleMeterRegistry registry;
    private Disruptor<FxRateEvent> disruptor;
    private FxMetrics metrics;
    private FxMetricsReporter reporter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        disruptor = new Disruptor<>(
                new FxRateEventFactory(), 1024, Executors.defaultThreadFactory(),
                ProducerType.MULTI, new BlockingWaitStrategy());
        RingBuffer<FxRateEvent> ringBuffer = disruptor.getRingBuffer();
        FxRateLatestBuffer latestBuffer = new FxRateLatestBuffer();
        FxProperties props = new FxProperties();
        FxRateHistoryBuffer historyBuffer = new FxRateHistoryBuffer(props);
        metrics = new FxMetrics(registry, latestBuffer, historyBuffer);
        metrics.registerRingBufferGauge(ringBuffer);
        reporter = new FxMetricsReporter(metrics, registry, props);
    }

    @AfterEach
    void tearDown() {
        if (disruptor != null) disruptor.halt();
    }

    @Test
    void jsonContainsAllExpectedFields() {
        JSONObject json = reporter.buildSnapshot();
        Set<String> expected = Set.of(
                "ts",
                "ring_buffer_remaining", "ring_buffer_size",
                "history_queue_size", "latest_buffer_size",
                "history_reject_total", "handler_unexpected_error_total",
                "latest_fallback_total", "history_fallback_total",
                "persist_retry_total", "persist_giveup_total",
                "dlq_write_total", "dlq_write_failure_total",
                "publish_p99_ms", "publish_max_ms",
                "latest_flush_p99_ms", "latest_flush_max_ms", "latest_batch_size_p99",
                "history_flush_p99_ms", "history_flush_max_ms", "history_batch_size_p99"
        );
        assertTrue(json.keySet().containsAll(expected),
                "missing keys: " + expected.stream().filter(k -> !json.containsKey(k)).toList());
    }

    @Test
    void counterValuesReflectMetricsState() {
        metrics.incDlqWrite();
        metrics.incDlqWrite();
        metrics.incDlqWriteFailure();
        JSONObject json = reporter.buildSnapshot();
        assertEquals(2L, json.getLongValue("dlq_write_total"));
        assertEquals(1L, json.getLongValue("dlq_write_failure_total"));
    }

    @Test
    void ringBufferSizeTagExposedAsField() {
        JSONObject json = reporter.buildSnapshot();
        assertEquals(1024L, json.getLongValue("ring_buffer_size"));
    }

    @Test
    void timerP99NotNaNAfterHistogramEnabled() {
        // 注入足够样本让 HDR 出 p99
        for (int i = 0; i < 100; i++) {
            metrics.recordPublish(1_000_000L * (i + 1)); // 1ms..100ms
        }
        JSONObject json = reporter.buildSnapshot();
        double p99 = json.getDoubleValue("publish_p99_ms");
        assertTrue(p99 > 0 && !Double.isNaN(p99),
                "p99 should be a positive finite number, got " + p99);
    }
}

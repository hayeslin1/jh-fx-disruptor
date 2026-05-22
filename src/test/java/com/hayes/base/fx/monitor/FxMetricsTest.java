package com.hayes.base.fx.monitor;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FxMetrics 单测：用 SimpleMeterRegistry 验证 16 个指标全部注册成功 + 语义化方法工作。
 */
class FxMetricsTest {

    private SimpleMeterRegistry registry;
    private Disruptor<FxRateEvent> disruptor;
    private RingBuffer<FxRateEvent> ringBuffer;
    private FxRateLatestBuffer latestBuffer;
    private FxRateHistoryBuffer historyBuffer;
    private FxMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        // 构造一个最小 Disruptor 以拿到真实 RingBuffer；不 start，仅用于 metrics 注册
        disruptor = new Disruptor<>(
                new FxRateEventFactory(), 1024, Executors.defaultThreadFactory(), ProducerType.MULTI,
                new com.lmax.disruptor.BlockingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();

        latestBuffer = new FxRateLatestBuffer();
        FxProperties props = new FxProperties();
        historyBuffer = new FxRateHistoryBuffer(props);

        metrics = new FxMetrics(registry, latestBuffer, historyBuffer);
        metrics.registerRingBufferGauge(ringBuffer);
    }

    @AfterEach
    void tearDown() {
        if (disruptor != null) {
            disruptor.halt();
        }
    }

    @Test
    void allSixteenMetricsRegistered() {
        Set<String> expected = Set.of(
                "fx.ring_buffer.remaining_capacity",
                "fx.history_buffer.size",
                "fx.latest_buffer.size",
                "fx.history_buffer.reject",
                "fx.handler.unexpected_error",
                "fx.flush.latest.fallback",
                "fx.flush.history.fallback",
                "fx.persist.retry",
                "fx.persist.giveup",
                "fx.dlq.write",
                "fx.dlq.write.failure",
                "fx.producer.publish.duration",
                "fx.flush.latest.batch.duration",
                "fx.flush.history.batch.duration",
                "fx.flush.latest.batch.size",
                "fx.flush.history.batch.size"
        );
        Set<String> actual = registry.getMeters().stream()
                .map(m -> m.getId().getName())
                .collect(Collectors.toSet());
        assertTrue(actual.containsAll(expected),
                "missing meters: " + expected.stream().filter(n -> !actual.contains(n)).toList());
    }

    @Test
    void ringBufferGaugeCarriesBufferSizeTag() {
        Meter g = registry.find("fx.ring_buffer.remaining_capacity").meter();
        assertNotNull(g);
        assertEquals("1024", g.getId().getTag("buffer_size"));
    }

    @Test
    void counterIncrementsWork() {
        metrics.incHistoryReject();
        metrics.incHistoryReject();
        metrics.incDlqWriteFailure();
        assertEquals(2.0, registry.find("fx.history_buffer.reject").counter().count());
        assertEquals(1.0, registry.find("fx.dlq.write.failure").counter().count());
    }

    @Test
    void timerRecordWorks() {
        metrics.recordPublish(1_500_000L);   // 1.5ms
        metrics.recordPublish(2_500_000L);   // 2.5ms
        assertEquals(2L, registry.find("fx.producer.publish.duration").timer().count());
    }

    @Test
    void recordLatestFlushUpdatesBothTimerAndSummary() {
        metrics.recordLatestFlush(120, 5_000_000L);
        assertEquals(1L, registry.find("fx.flush.latest.batch.duration").timer().count());
        assertEquals(1L, registry.find("fx.flush.latest.batch.size").summary().count());
        assertEquals(120.0, registry.find("fx.flush.latest.batch.size").summary().totalAmount());
    }
}

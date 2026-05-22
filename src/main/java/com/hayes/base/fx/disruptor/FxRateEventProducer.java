package com.hayes.base.fx.disruptor;

import com.hayes.base.fx.dto.FxRatePushDTO;
import com.hayes.base.fx.monitor.FxMetrics;
import com.lmax.disruptor.RingBuffer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 发布器：把入参 DTO 投递到 Disruptor RingBuffer
 * <p>
 * RingBuffer 由 {@code DisruptorConfig} 注入；
 * 使用 {@code publishEvent(translator, dto, traceId)} 的方式避免显式 next/publish 配对错误。
 * Ring 满时由当前配置的 WaitStrategy 决定阻塞或告警——当前默认 BlockingWaitStrategy 会阻塞生产线程，
 * 这是对后端 MySQL 堆积压力的天然背压。
 * <p>
 * 监控：每条 publish 的耗时由 {@link FxMetrics#recordPublish} 进入 Timer
 * {@code fx.producer.publish.duration}（HDR 直方图 + 1min 滚动 P99），
 * 不再每条打 INFO 日志（高 QPS 下会灌爆日志）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FxRateEventProducer {

    private final RingBuffer<FxRateEvent> ringBuffer;
    private final FxMetrics metrics;

    /**
     * 单条投递
     *
     * @param dto     入参
     * @param traceId 链路追踪 ID，调用方生成
     */
    public void publish(FxRatePushDTO dto, String traceId) {
        long t0 = System.nanoTime();
        try {
            ringBuffer.publishEvent(FxRateEventTranslator.INSTANCE, dto, traceId);
        } finally {
            // 无论成功失败都记录耗时，便于发现 publish 阻塞（BlockingWaitStrategy 满载时 P99 飙升）
            metrics.recordPublish(System.nanoTime() - t0);
        }
    }
}

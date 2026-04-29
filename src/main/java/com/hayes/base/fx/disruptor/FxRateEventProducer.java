package com.hayes.base.fx.disruptor;

import com.hayes.base.fx.dto.FxRatePushDTO;
import com.lmax.disruptor.RingBuffer;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Component;

/**
 * 发布器：把入参 DTO 投递到 Disruptor RingBuffer
 * <p>
 * RingBuffer 由 {@code DisruptorConfig} 注入；
 * 使用 {@code publishEvent(translator, dto, traceId)} 的方式避免显式 next/publish 配对错误。
 * Ring 满时由当前配置的 WaitStrategy 决定阻塞或告警——当前默认 BlockingWaitStrategy 会阻塞生产线程，
 * 这是对后端 MySQL 堆积压力的天然背压。
 */
@Component
@RequiredArgsConstructor
public class FxRateEventProducer {

    private final RingBuffer<FxRateEvent> ringBuffer;

    /**
     * 单条投递
     *
     * @param dto     入参
     * @param traceId 链路追踪 ID，调用方生成
     */
    public void publish(FxRatePushDTO dto, String traceId) {
        ringBuffer.publishEvent(FxRateEventTranslator.INSTANCE, dto, traceId);
    }
}

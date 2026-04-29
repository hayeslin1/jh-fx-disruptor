package com.hayes.base.fx.service;

import com.hayes.base.fx.disruptor.FxRateEventProducer;
import com.hayes.base.fx.dto.FxRatePushDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * 汇率接入 Service：负责生成 traceId、投递到 Disruptor
 * <p>
 * 不做落库动作（落库由 EventHandler → PersistenceService 完成），
 * 这里仅承担 HTTP/未来 MQ 接入的统一出入口，便于后续替换接入通道。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FxRateService {

    private final FxRateEventProducer producer;

    /**
     * 接收一条汇率推送
     *
     * @return traceId，便于调用方排查
     */
    public String receive(FxRatePushDTO dto) {
        // traceId：简短 UUID，用于贯穿 RingBuffer → Handler → DB 日志
        String traceId = UUID.randomUUID().toString().replace("-", "").substring(0, 16);
        producer.publish(dto, traceId);
        if (log.isDebugEnabled()) {
            log.debug("[fx-rate] accepted traceId={} ccyPair={} channelCd={}",
                    traceId, dto.getCcyPair(), dto.getChannelCd());
        }
        return traceId;
    }
}

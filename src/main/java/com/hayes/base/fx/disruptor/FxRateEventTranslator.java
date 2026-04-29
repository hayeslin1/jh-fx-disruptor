package com.hayes.base.fx.disruptor;

import com.hayes.base.fx.common.FxConst;
import com.hayes.base.fx.dto.FxRatePushDTO;
import com.lmax.disruptor.EventTranslatorTwoArg;
import org.apache.commons.lang3.StringUtils;

/**
 * Event 翻译器：将入参 DTO + traceId 填充到 RingBuffer 预分配的 Event 槽
 * <p>
 * 使用 {@link EventTranslatorTwoArg} 避免在 publish 时创建临时对象（对 1k QPS 场景足够）。
 */
public class FxRateEventTranslator implements EventTranslatorTwoArg<FxRateEvent, FxRatePushDTO, String> {

    /** 复用单例，无状态 */
    public static final FxRateEventTranslator INSTANCE = new FxRateEventTranslator();

    @Override
    public void translateTo(FxRateEvent event, long sequence, FxRatePushDTO dto, String traceId) {
        // 先清空复用槽，防止上一轮残留字段
        event.clear();
        event.setCcyPair(dto.getCcyPair());
        event.setChannelCd(dto.getChannelCd());
        event.setBuyPrice(dto.getBuyPrice());
        event.setSellPrice(dto.getSellPrice());
        event.setBlPrice(dto.getBlPrice());
        // 交割类型缺失时兜底 DEFAULT_DELI_TYP（HARD-GATE Q-D）
        event.setDeliTyp(StringUtils.defaultIfBlank(dto.getDeliTyp(), FxConst.DEFAULT_DELI_TYP));
        event.setDtChannelPublish(dto.getDtChannelPublish());
        event.setTmChannelPublish(dto.getTmChannelPublish());
        event.setUtcTimes(dto.getUtcTimes());
        event.setReceiveNanos(System.nanoTime());
        event.setTraceId(traceId);
    }
}

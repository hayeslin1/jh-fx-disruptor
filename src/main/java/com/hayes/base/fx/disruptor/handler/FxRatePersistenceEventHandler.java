package com.hayes.base.fx.disruptor.handler;

import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.service.FxRatePersistenceService;
import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 持久化 EventHandler（modulo 分片）
 * <p>
 * Disruptor 4.0.0 移除了 WorkerPool，通过 N 个 EventHandler 并行订阅 + modulo(sequence) 分片
 * 来实现"每条事件恰好被一个 handler 处理"的语义。
 * <p>
 * 构造参数：
 * <ul>
 *   <li>myIndex     本 handler 在分片组内的下标（0 ~ workerCount-1）</li>
 *   <li>workerCount handler 总数</li>
 *   <li>service     Spring 托管的持久化 Service（@Transactional 在它上面）</li>
 * </ul>
 */
@Slf4j
public class FxRatePersistenceEventHandler implements EventHandler<FxRateEvent> {

    private final int myIndex;
    private final int workerCount;
    private final FxRatePersistenceService persistenceService;

    public FxRatePersistenceEventHandler(int myIndex, int workerCount, FxRatePersistenceService persistenceService) {
        this.myIndex = myIndex;
        this.workerCount = workerCount;
        this.persistenceService = persistenceService;
    }

    @Override
    public void onEvent(FxRateEvent event, long sequence, boolean endOfBatch) {
        // modulo 分片：仅处理属于自己的 sequence，其它 handler 会处理其余部分
        if ((sequence % workerCount) != myIndex) {
            return;
        }
        try {
            persistenceService.persist(event);
        } catch (Throwable ex) {
            // Service 内部已带重试与 DLQ；此处兜底不让异常冒出，否则 Disruptor 会停止
            log.error("[fx-rate] handler#{} 未预期异常 traceId={} ccyPair={} channelCd={}",
                    myIndex, event.getTraceId(), event.getCcyPair(), event.getChannelCd(), ex);
        }
    }
}

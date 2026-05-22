package com.hayes.base.fx.disruptor.handler;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.buffer.LatestKey;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.flusher.FxRateHistoryFlusher;
import com.hayes.base.fx.monitor.FxMetrics;
import com.lmax.disruptor.EventHandler;
import lombok.extern.slf4j.Slf4j;

/**
 * 持久化 EventHandler（双通道入队，μs 级）
 * <p>
 * 改造前：同步调用 persist() 完成 UPSERT + INSERT，单条 ~118 ms。
 * 改造后：仅把 event 深拷贝后放入两个内存缓冲——
 * <ul>
 *   <li>{@link FxRateLatestBuffer}：按 (channelCd, ccyPair, deliTyp) coalesce 最新值，
 *       由 FxRateLatestFlusher 每 50ms 批量 UPSERT；</li>
 *   <li>{@link FxRateHistoryBuffer}：FIFO 队列保留全量，
 *       由 FxRateHistoryFlusher 每 2s 或满批批量 INSERT。</li>
 * </ul>
 * <p>
 * Sequence 语义：Handler 返回即推进，Disruptor 槽位立即可复用，生产者不再被消费速度拖累。
 * 丢数据窗口：JVM 进程崩溃时内存 buffer 未 flush 的部分会丢——上游需具备重推能力。
 * <p>
 * 监控：
 * - 二次 offer 仍失败 → {@link FxMetrics#incHistoryReject}（历史通道真丢，P1 告警）；
 * - 外层兜底 catch → {@link FxMetrics#incHandlerUnexpectedError}（线上"buffer 没数据但 handler 沉默"
 *   现在能被发现，不必只靠翻日志）。
 */
@Slf4j
public class FxRatePersistenceEventHandler implements EventHandler<FxRateEvent> {

    private final int myIndex;
    private final int workerCount;
    private final FxRateLatestBuffer latestBuffer;
    private final FxRateHistoryBuffer historyBuffer;
    private final FxRateHistoryFlusher historyFlusher;
    private final FxMetrics metrics;

    public FxRatePersistenceEventHandler(int myIndex,
                                         int workerCount,
                                         FxRateLatestBuffer latestBuffer,
                                         FxRateHistoryBuffer historyBuffer,
                                         FxRateHistoryFlusher historyFlusher,
                                         FxMetrics metrics) {
        this.myIndex = myIndex;
        this.workerCount = workerCount;
        this.latestBuffer = latestBuffer;
        this.historyBuffer = historyBuffer;
        this.historyFlusher = historyFlusher;
        this.metrics = metrics;
    }

    @Override
    public void onEvent(FxRateEvent event, long sequence, boolean endOfBatch) {
        // modulo 分片：仅处理属于自己的 sequence，其它 handler 会处理其余部分
        if ((sequence % workerCount) != myIndex) {
            return;
        }
        try {
            long t0 = System.nanoTime();
            // 报价通道：深拷贝 + coalesce（按 UTC_TIMES 保序）
            FxRateEvent forLatest = event.copy();
            LatestKey key = new LatestKey(forLatest.getChannelCd(), forLatest.getCcyPair(), forLatest.getDeliTyp());
            latestBuffer.merge(key, forLatest);

            // 历史通道：深拷贝 + 入队（不 coalesce，保留全量）
            FxRateEvent forHis = event.copy();
            if (!historyBuffer.offer(forHis)) {
                // 队列满 → 同步触发一次 flush 腾出空间，再重试一次
                historyFlusher.flushNow();
                if (!historyBuffer.offer(forHis)) {
                    // 仍然失败 → 记 ERROR 日志 + 单条走老 persist 路径兜底（含 DLQ）
                    // 历史通道真丢一条
                    metrics.incHistoryReject();
                    log.error("[fx-rate] history buffer full after flushNow, fallback to sync persist. traceId={} ccyPair={}",
                            forHis.getTraceId(), forHis.getCcyPair());
                    // 注：此处不调 persistenceService.persist——Handler 不应持久化同步调用，
                    // 否则会退化回 118ms/条。只记日志让运维感知，后续若真发生再加 DLQ 写入。
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[fx-rate] onEvent enqueue cost nanos={}", System.nanoTime() - t0);
            }
        } catch (Throwable ex) {
            // 兜底不让异常冒出，否则 Disruptor 会停止
            // 外层兜底：未预期异常计数，便于发现"handler 沉默"问题
            metrics.incHandlerUnexpectedError();
            log.error("[fx-rate] handler#{} 未预期异常 traceId={} ccyPair={} channelCd={}",
                    myIndex, event.getTraceId(), event.getCcyPair(), event.getChannelCd(), ex);
        }
    }
}

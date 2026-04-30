package com.hayes.base.fx.buffer;

import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * 历史流水缓冲（双通道架构中的"历史通道"入口）
 * <p>
 * 特性：
 * 1. 有界队列，容量来自 {@code FxProperties.Flush.historyQueueCapacity}；
 * 2. {@link #offer} 非阻塞：满时返回 false，由 Handler 决定兜底策略（调用方本地的 flushNow 或 DLQ）；
 * 3. {@link #drainTo} 最多取 maxElements 条，供 Flusher 凑批；
 * 4. 高水位告警：超过 warnThreshold 时周期性打 WARN 日志。
 */
@Slf4j
@Component
public class FxRateHistoryBuffer {

    private final LinkedBlockingQueue<FxRateEvent> queue;
    private final int warnThreshold;

    /** 上次告警时间戳（ms），用于节流告警频率 */
    private volatile long lastWarnAtMs = 0L;
    /** 告警最小间隔：避免日志刷屏 */
    private static final long WARN_INTERVAL_MS = 5_000L;

    @Autowired
    public FxRateHistoryBuffer(FxProperties fxProps) {
        FxProperties.Flush flush = fxProps.getFlush();
        this.queue = new LinkedBlockingQueue<>(flush.getHistoryQueueCapacity());
        this.warnThreshold = flush.getHistoryQueueWarnThreshold();
    }

    /**
     * 非阻塞入队。
     *
     * @param event 已深拷贝的事件
     * @return true 入队成功；false 队列已满
     */
    public boolean offer(FxRateEvent event) {
        boolean ok = queue.offer(event);
        if (ok) {
            maybeWarnHighWatermark();
        }
        return ok;
    }

    /**
     * 批量取出最多 maxElements 条到目标 list。
     *
     * @param target      目标容器（调用方持有）
     * @param maxElements 单次 drain 上限
     * @return 实际取出条数
     */
    public int drainTo(List<FxRateEvent> target, int maxElements) {
        return queue.drainTo(target, maxElements);
    }

    /** 把所有剩余条目 drain 到目标 list，shutdown 场景用 */
    public int drainAll(List<FxRateEvent> target) {
        return queue.drainTo(target);
    }

    public int size() {
        return queue.size();
    }

    /** 高水位告警（节流） */
    private void maybeWarnHighWatermark() {
        int size = queue.size();
        if (size < warnThreshold) {
            return;
        }
        long now = System.currentTimeMillis();
        if (now - lastWarnAtMs < WARN_INTERVAL_MS) {
            return;
        }
        lastWarnAtMs = now;
        log.warn("[fx-rate] history buffer high watermark: size={}, threshold={}", size, warnThreshold);
    }
}

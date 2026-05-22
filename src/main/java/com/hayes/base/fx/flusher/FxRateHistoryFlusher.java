package com.hayes.base.fx.flusher;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.monitor.FxMetrics;
import com.hayes.base.fx.service.FxRatePersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 历史流水批量 flusher
 * <p>
 * 定时（默认 2s）从 {@link FxRateHistoryBuffer} drain 出一批事件，批量 INSERT 到 fx_xr_his。
 * 不做 coalesce——保留全量。整批失败时逐条走老 {@code persist} 路径（不会 UPSERT 最新态，
 * 因为那一路径会重复 UPSERT；TODO：后续拆成独立历史路径）。
 * <p>
 * 生命周期同 {@link FxRateLatestFlusher}：SmartLifecycle，stop 时强制排干队列。
 * <p>
 * 监控：成功调 {@link FxMetrics#recordHistoryFlush}；正常调度与 shutdown 两处 fallback
 * 都调 {@link FxMetrics#incHistoryFallback}（shutdown 阶段问题不能被吞）。
 */
@Slf4j
@Component
public class FxRateHistoryFlusher implements SmartLifecycle {

    private final FxRateHistoryBuffer buffer;
    private final FxRatePersistenceService persistenceService;
    private final FxProperties.Flush flushProps;
    private final FxMetrics metrics;

    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public FxRateHistoryFlusher(FxRateHistoryBuffer buffer,
                                FxRatePersistenceService persistenceService,
                                FxProperties fxProps,
                                FxMetrics metrics) {
        this.buffer = buffer;
        this.persistenceService = persistenceService;
        this.flushProps = fxProps.getFlush();
        this.metrics = metrics;
    }

    @Override
    public void start() {
        if (!running.compareAndSet(false, true)) {
            return;
        }
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "fx-history-flusher");
            t.setDaemon(true);
            return t;
        });
        long interval = flushProps.getHistoryIntervalMs();
        scheduler.scheduleAtFixedRate(this::flushOnceSilently, interval, interval, TimeUnit.MILLISECONDS);
        log.info("[fx-flusher-history] started, intervalMs={}", interval);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                // 历史通道给更长超时，确保 drain 到底（批量 INSERT 可能耗时）
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
        // 最后强制把队列排干
        drainUntilEmpty();
        log.info("[fx-flusher-history] stopped.");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 供 Handler 在 buffer 满时同步触发一次 flush（兜底背压）。
     * 非线程安全意义上的同步——只是"立即执行一次 flushOnce"；若调度线程正在 flush，两者可并发 drain，
     * LinkedBlockingQueue.drainTo 本身线程安全。
     */
    public void flushNow() {
        flushOnceSilently();
    }

    /** 执行一次 flush；所有异常本方法内消化 */
    private void flushOnceSilently() {
        try {
            flushOnce();
        } catch (Throwable ex) {
            log.error("[fx-flusher-history] unexpected flush error", ex);
        }
    }

    private void flushOnce() {
        int maxBatch = flushProps.getHistoryBatchMaxSize();
        List<FxRateEvent> batch = new ArrayList<>(maxBatch);
        int drained = buffer.drainTo(batch, maxBatch);
        if (drained == 0) {
            return;
        }
        long t0 = System.nanoTime();
        try {
            persistenceService.doPersistHistoryBatch(batch);
            // 成功路径：记录批量耗时 + batchSize 分布
            metrics.recordHistoryFlush(drained, System.nanoTime() - t0);
            if (log.isDebugEnabled()) {
                log.debug("[fx-flusher-history] flushed size={} costNanos={}",
                        drained, System.nanoTime() - t0);
            }
        } catch (Throwable ex) {
            // 整批失败 → 降级逐条；计数 fallback
            metrics.incHistoryFallback();
            // 整批失败 → 逐条走老 persist（注意：会重复 UPSERT 最新态，但幂等安全）
            log.warn("[fx-flusher-history] batch failed, fallback to per-event persist. size={} reason={}",
                    drained, ex.getMessage());
            for (FxRateEvent e : batch) {
                try {
                    persistenceService.persist(e);
                } catch (Throwable ignored) {
                    // persist 内部已 DLQ 兜底
                }
            }
        }
    }

    /** Shutdown 时反复 drain 直到为空，确保不丢历史 */
    private void drainUntilEmpty() {
        int safetyLimit = 100; // 最多循环 100 轮，防止病态情况死循环
        while (safetyLimit-- > 0) {
            List<FxRateEvent> batch = new ArrayList<>();
            int drained = buffer.drainAll(batch);
            if (drained == 0) {
                return;
            }
            try {
                long t0 = System.nanoTime();
                persistenceService.doPersistHistoryBatch(batch);
                // shutdown 阶段同样计入正常 flush 指标，保留可观测性
                metrics.recordHistoryFlush(drained, System.nanoTime() - t0);
                log.info("[fx-flusher-history] shutdown drain size={}", drained);
            } catch (Throwable ex) {
                // shutdown 阶段降级也必须计入，否则停机期间问题被吞
                metrics.incHistoryFallback();
                log.error("[fx-flusher-history] shutdown drain failed, fallback per-event. size={}",
                        drained, ex);
                for (FxRateEvent e : batch) {
                    try {
                        persistenceService.persist(e);
                    } catch (Throwable ignored) {
                        // 已 DLQ 兜底
                    }
                }
            }
        }
        log.error("[fx-flusher-history] shutdown drain hit safetyLimit, remaining={}", buffer.size());
    }
}

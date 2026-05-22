package com.hayes.base.fx.flusher;

import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.buffer.LatestKey;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.monitor.FxMetrics;
import com.hayes.base.fx.service.FxRatePersistenceService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.SmartLifecycle;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * 报价最新态批量 flusher
 * <p>
 * 定时（默认 50ms）从 {@link FxRateLatestBuffer#drainAll} 取出 coalesced 快照，
 * 批量 UPSERT 到 fx_xr_inf。整批失败时逐条走老 {@code persist} 路径保证不丢（走老重试 + DLQ）。
 * <p>
 * 生命周期（SmartLifecycle）：
 * - {@link #start} 开启调度；
 * - {@link #stop} 停调度并执行最后一次 flush，保证 shutdown 时 buffer 被排空；
 * - phase 设为较大值，保证在 Disruptor（DisposableBean）之后停，接住 in-flight。
 * <p>
 * 监控：每次 flush 调 {@link FxMetrics#recordLatestFlush}（Timer + DS）；
 * 整批失败降级时调 {@link FxMetrics#incLatestFallback}。
 */
@Slf4j
@Component
public class FxRateLatestFlusher implements SmartLifecycle {

    private final FxRateLatestBuffer buffer;
    private final FxRatePersistenceService persistenceService;
    private final FxProperties.Flush flushProps;
    private final FxMetrics metrics;

    private ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public FxRateLatestFlusher(FxRateLatestBuffer buffer,
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
            Thread t = new Thread(r, "fx-latest-flusher");
            t.setDaemon(true);
            return t;
        });
        long interval = flushProps.getLatestIntervalMs();
        scheduler.scheduleAtFixedRate(this::flushOnceSilently, interval, interval, TimeUnit.MILLISECONDS);
        log.info("[fx-flusher-latest] started, intervalMs={}", interval);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        // 先停调度器，再同步执行最后一次 flush 把缓存排空
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(2, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
        // 最后一次强制 flush，确保不丢最新态
        flushOnceSilently();
        log.info("[fx-flusher-latest] stopped.");
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    /**
     * SmartLifecycle 的 phase：数值越大越晚启动、越早停。
     * 用 MAX_VALUE - 100 保证比 DisruptorConfig.destroy()（Spring 默认）晚启动、早停——
     * 但我们实际希望的是"Disruptor 先停、Flusher 后停"。Spring 关闭顺序 = 启动顺序倒序，
     * DisruptorConfig 是 @Bean 先于本 Component 创建（因为 @Configuration 先扫），
     * 所以 Flusher 会后启动、先停。这正是我们要的：先停 Disruptor 让 in-flight 归位到 buffer，
     * 然后停 Flusher 时把 buffer 排空。
     * <p>
     * 注：SmartLifecycle.phase 与 DisposableBean 顺序无直接关系；本 phase 值只用于
     * 与其他 SmartLifecycle 组件协调。若将来 DisruptorConfig 也改为 SmartLifecycle，
     * 需保证其 phase < 本 phase。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE - 100;
    }

    /** 容器启动后自动 start */
    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /** 执行一次 flush；所有异常本方法内消化，保证调度器不被打断 */
    private void flushOnceSilently() {
        try {
            flushOnce();
        } catch (Throwable ex) {
            log.error("[fx-flusher-latest] unexpected flush error", ex);
        }
    }

    private void flushOnce() {
        Map<LatestKey, FxRateEvent> snapshot = buffer.drainAll();
        if (snapshot.isEmpty()) {
            return;
        }
        int maxBatch = flushProps.getLatestBatchMaxSize();
        List<FxRateEvent> all = new ArrayList<>(snapshot.values());

        // 按 maxBatch 分片提交，避免单个事务过大
        int from = 0;
        while (from < all.size()) {
            int to = Math.min(from + maxBatch, all.size());
            List<FxRateEvent> chunk = all.subList(from, to);
            long t0 = System.nanoTime();
            try {
                persistenceService.doPersistLatestBatch(chunk);
                // 成功路径：记录批量耗时 + batchSize 分布
                metrics.recordLatestFlush(chunk.size(), System.nanoTime() - t0);
                if (log.isDebugEnabled()) {
                    log.debug("[fx-flusher-latest] flushed chunk size={} costNanos={}",
                            chunk.size(), System.nanoTime() - t0);
                }
            } catch (Throwable ex) {
                // 整批失败 → 降级逐条；同时计数 fallback
                metrics.incLatestFallback();
                log.warn("[fx-flusher-latest] batch failed, fallback to per-event persist. size={} reason={}",
                        chunk.size(), ex.getMessage());
                for (FxRateEvent e : chunk) {
                    try {
                        persistenceService.persist(e);
                    } catch (Throwable ignored) {
                        // persist 内部已 DLQ 兜底
                    }
                }
            }
            from = to;
        }
    }
}

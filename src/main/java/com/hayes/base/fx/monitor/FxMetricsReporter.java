package com.hayes.base.fx.monitor;

import com.alibaba.fastjson2.JSONObject;
import com.hayes.base.fx.config.FxProperties;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.ValueAtPercentile;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.TimeUnit;

/**
 * 周期性指标快照 Reporter。
 * <p>
 * - 每 metrics-log-interval-ms 输出一行 [fx-metrics] {json}；
 * - 通过 @ConditionalOnExpression 控制：metrics-log-interval-ms<=0 时本 Bean 不注册，
 *   避免 @Scheduled fixedDelay=0 的未定义行为；
 * - Counter / Timer / DS 直接读 FxMetrics 字段（强引用 + 编译期防误写）；
 * - Gauge（3 个）注册期已挂在 registry，按 name 取即可。
 */
@Slf4j
@Component
@ConditionalOnExpression("${app.fx.flush.metrics-log-interval-ms:60000} > 0")
public class FxMetricsReporter {

    private final FxMetrics metrics;
    private final MeterRegistry registry;

    public FxMetricsReporter(FxMetrics metrics, MeterRegistry registry, FxProperties fxProps) {
        this.metrics = metrics;
        this.registry = registry;
        // fxProps 当前仅用于声明依赖，确保配置变更触发 bean 重新装配；间隔由 @Scheduled placeholder 直接消费
    }

    /** 定时入口：从 application.yml 读 placeholder，间隔默认 60s */
    @Scheduled(fixedDelayString = "${app.fx.flush.metrics-log-interval-ms:60000}")
    public void report() {
        JSONObject snap = buildSnapshot();
        log.info("[fx-metrics] {}", snap.toJSONString());
    }

    /**
     * 构造一份完整 JSON 快照。
     * <p>
     * 抽取为 package-private 以便单测直接断言字段，避免 @Scheduled 行为耦合。
     */
    JSONObject buildSnapshot() {
        JSONObject json = new JSONObject();
        json.put("ts", DateTimeFormatter.ISO_INSTANT.format(Instant.now()));

        // ===== Gauge：按 name 取（这 3 个不持字段）=====
        json.put("ring_buffer_remaining", gaugeLong("fx.ring_buffer.remaining_capacity"));
        json.put("ring_buffer_size",
                gaugeTagLong("fx.ring_buffer.remaining_capacity", "buffer_size"));
        json.put("history_queue_size", gaugeLong("fx.history_buffer.size"));
        json.put("latest_buffer_size", gaugeLong("fx.latest_buffer.size"));

        // ===== Counter =====
        json.put("history_reject_total",            (long) metrics.historyRejectCounter().count());
        json.put("handler_unexpected_error_total",  (long) metrics.handlerUnexpectedErrorCounter().count());
        json.put("latest_fallback_total",           (long) metrics.latestFallbackCounter().count());
        json.put("history_fallback_total",          (long) metrics.historyFallbackCounter().count());
        json.put("persist_retry_total",             (long) metrics.persistRetryCounter().count());
        json.put("persist_giveup_total",            (long) metrics.persistGiveupCounter().count());
        json.put("dlq_write_total",                 (long) metrics.dlqWriteCounter().count());
        json.put("dlq_write_failure_total",         (long) metrics.dlqWriteFailureCounter().count());

        // ===== Timer（毫秒）=====
        putTimer(json, "publish",       metrics.publishTimer());
        putTimer(json, "latest_flush",  metrics.latestFlushTimer());
        putTimer(json, "history_flush", metrics.historyFlushTimer());

        // ===== DistributionSummary（无量纲，取 p99）=====
        json.put("latest_batch_size_p99",
                round2(summaryPercentile(metrics.latestBatchSizeSummary(), 0.99)));
        json.put("history_batch_size_p99",
                round2(summaryPercentile(metrics.historyBatchSizeSummary(), 0.99)));

        return json;
    }

    /** Timer 的 p99 / max（毫秒） */
    private void putTimer(JSONObject json, String prefix, Timer timer) {
        HistogramSnapshot snap = timer.takeSnapshot();
        json.put(prefix + "_p99_ms", round2(timerPercentileMs(snap, 0.99)));
        json.put(prefix + "_max_ms", round2(timer.max(TimeUnit.MILLISECONDS)));
    }

    /** Timer 的 ValueAtPercentile.value(TimeUnit) 已自动从 base ns 转 ms */
    private static double timerPercentileMs(HistogramSnapshot snap, double p) {
        for (ValueAtPercentile v : snap.percentileValues()) {
            if (Math.abs(v.percentile() - p) < 1e-6) {
                return v.value(TimeUnit.MILLISECONDS);
            }
        }
        return Double.NaN;
    }

    /** DistributionSummary 无量纲，直接取 value() */
    private static double summaryPercentile(DistributionSummary summary, double p) {
        for (ValueAtPercentile v : summary.takeSnapshot().percentileValues()) {
            if (Math.abs(v.percentile() - p) < 1e-6) {
                return v.value();
            }
        }
        return Double.NaN;
    }

    private Long gaugeLong(String name) {
        Gauge g = registry.find(name).gauge();
        return g == null ? null : (long) g.value();
    }

    private Long gaugeTagLong(String name, String tag) {
        Gauge g = registry.find(name).gauge();
        if (g == null) return null;
        String v = g.getId().getTag(tag);
        return v == null ? null : Long.parseLong(v);
    }

    /** 保留两位小数，避免 JSON 出现 8.499999999999999 这种噪声 */
    private static double round2(double v) {
        if (Double.isNaN(v) || Double.isInfinite(v)) return 0d;
        return Math.round(v * 100.0) / 100.0;
    }
}

package com.hayes.base.fx;

import com.hayes.base.fx.dto.FxRatePushDTO;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.web.client.RestTemplate;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 推送压测：模拟银行端 /fx/push 真实推文，默认 100 QPS 持续 60 秒。
 * <p>
 * 使用方式：
 * <pre>
 *   1) 先本地启动 Application（默认 8080 端口）
 *   2) IDE 直接跑 pushTest() 方法；或
 *      mvn -Denv=test test -Dtest=FxRatePushTest#pushTest
 * </pre>
 * 可调参数（-D 系统属性）：
 * <ul>
 *   <li>push.url       目标地址，默认 http://localhost:8080/fx/push</li>
 *   <li>push.qps       每秒条数，默认 100</li>
 *   <li>push.duration  持续秒数，默认 60</li>
 *   <li>push.workers   HTTP 工作线程数，默认 16（吸收接口抖动，保障节拍不漂）</li>
 * </ul>
 */
public class FxRatePushTest {

    // 货币对池：覆盖常见跨境结算品种（顺序与 BASE_PRICES 对齐）
    private static final List<String> CCY_PAIRS = List.of("USD/CNY", "EUR/CNY", "JPY/CNY", "HKD/CNY", "GBP/CNY");

    // 各货币对的基准中间价，用于随机游走产生合理的买卖报价
    private static final double[] BASE_PRICES = {7.1234, 7.7321, 0.0478, 0.9137, 9.0128};

    // 通道池：模拟多家银行并发推送
    private static final List<String> CHANNELS = List.of("BOC", "ICBC", "ABC", "CCB");

    // 交割类型池：TOD=00 / TOM=01 / SPT=02
    private static final List<String> DELI_TYPS = List.of("00", "01", "02");

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HHmmss");
    /** ISO-8601 UTC 时间戳格式，固定 3 位毫秒；与 DTO 端 @Pattern 校验对齐 */
    private static final DateTimeFormatter UTC_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'").withZone(ZoneOffset.UTC);

    /**
     * 主测试入口：按指定 QPS 持续向 /fx/push 推送模拟数据，并打印实时进度与最终汇总。
     */
    @Test
    public void pushTest() throws InterruptedException {
        // —— 读取可调参数 ——
        String url = System.getProperty("push.url", "http://localhost:8080/fx/push");
        int qps = Integer.parseInt(System.getProperty("push.qps", "100"));
        int durationSec = Integer.parseInt(System.getProperty("push.duration", "60"));
        int workers = Integer.parseInt(System.getProperty("push.workers", "16"));
        int total = qps * durationSec;
        // 节拍间隔（纳秒）：100 QPS → 10,000,000ns = 10ms
        long intervalNanos = TimeUnit.SECONDS.toNanos(1) / qps;

        System.out.printf("[push-test] url=%s qps=%d duration=%ds workers=%d total=%d%n",
                url, qps, durationSec, workers, total);

        // RestTemplate 复用底层 HttpURLConnection 连接；Jackson 自动序列化 DTO
        RestTemplate rest = new RestTemplate();
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);

        // 统计指标：已投产、成功、失败、累计 RT（纳秒）
        AtomicInteger produced = new AtomicInteger();
        AtomicInteger ok = new AtomicInteger();
        AtomicInteger fail = new AtomicInteger();
        AtomicLong rtNanosTotal = new AtomicLong();
        AtomicInteger firstFailPrint = new AtomicInteger();
        Random rnd = new Random(42);

        // 全部请求完成的同步器
        CountDownLatch done = new CountDownLatch(total);

        // —— 三个执行器：节拍器 / 进度打印 / HTTP 工作池 ——
        // 节拍器负责按固定频率提交任务到 HTTP 工作池，避免慢请求拖慢节拍
        ScheduledExecutorService ticker = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "push-ticker"));
        ScheduledExecutorService reporter = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "push-reporter"));
        ExecutorService httpPool = Executors.newFixedThreadPool(workers,
                r -> new Thread(r, "push-http"));

        // 每秒打印一次进度（已发/成功/失败/平均 RT 微秒）
        ScheduledFuture<?> reportFuture = reporter.scheduleAtFixedRate(() -> {
            int s = produced.get();
            long avgRtUs = s == 0 ? 0 : rtNanosTotal.get() / s / 1_000;
            System.out.printf("[push-test] produced=%d ok=%d fail=%d avgRt=%dus%n",
                    s, ok.get(), fail.get(), avgRtUs);
        }, 1, 1, TimeUnit.SECONDS);

        long start = System.nanoTime();

        // 节拍任务：每 intervalNanos 触发一次，抵达 total 后自我取消
        ScheduledFuture<?>[] tickRef = new ScheduledFuture[1];
        tickRef[0] = ticker.scheduleAtFixedRate(() -> {
            int idx = produced.getAndIncrement();
            if (idx >= total) {
                // 已达目标总数，取消后续节拍
                if (tickRef[0] != null) {
                    tickRef[0].cancel(false);
                }
                return;
            }
            // 构造一条模拟推送数据
            FxRatePushDTO dto = buildDto(rnd);
            // 提交到 HTTP 工作池异步执行，保证节拍线程本身零阻塞
            httpPool.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    rest.postForObject(url, new HttpEntity<>(dto, headers), String.class);
                    ok.incrementAndGet();
                } catch (Exception e) {
                    fail.incrementAndGet();
                    // 只打前 3 次异常，避免刷屏
                    if (firstFailPrint.incrementAndGet() <= 3) {
                        System.out.println("[push-test] failed: " + e.getMessage());
                    }
                } finally {
                    rtNanosTotal.addAndGet(System.nanoTime() - t0);
                    done.countDown();
                }
            });
        }, 0, intervalNanos, TimeUnit.NANOSECONDS);

        // 等所有请求全部回来；给 duration + 30s 的上限防卡死
        boolean completed = done.await(durationSec + 30L, TimeUnit.SECONDS);
        long costMs = (System.nanoTime() - start) / 1_000_000;

        // —— 收尾：停调度与工作池 ——
        reportFuture.cancel(false);
        ticker.shutdownNow();
        reporter.shutdownNow();
        httpPool.shutdown();
        httpPool.awaitTermination(5, TimeUnit.SECONDS);

        double realQps = total * 1000.0 / Math.max(costMs, 1);
        System.out.printf("[push-test] DONE completed=%s total=%d ok=%d fail=%d cost=%dms realQps=%.1f%n",
                completed, total, ok.get(), fail.get(), costMs, realQps);
    }

    /**
     * 构造一条模拟银行推送数据。
     * <p>
     * 规则：买卖价围绕基准中间价做 ±0.5% 随机游走，点差固定 2bp（卖价 > 买价符合市场规则）；
     * 发布日期/时间取当前本地时间，UTC 字段用毫秒时间戳串。
     *
     * @param rnd 随机源（由调用方持有以便结果可复现）
     * @return 一条有效 DTO，必填字段全部填充
     */
    private FxRatePushDTO buildDto(Random rnd) {
        int idx = rnd.nextInt(CCY_PAIRS.size());
        String ccyPair = CCY_PAIRS.get(idx);
        String channel = CHANNELS.get(rnd.nextInt(CHANNELS.size()));
        String deliTyp = DELI_TYPS.get(rnd.nextInt(DELI_TYPS.size()));
        double base = BASE_PRICES[idx];
        // 中间价在基准价上下 ±0.5% 随机游走
        double mid = base * (1 + (rnd.nextDouble() - 0.5) * 0.01);
        // 固定 2bp 点差
        double spread = base * 0.0002;
        BigDecimal buy = BigDecimal.valueOf(mid - spread / 2).setScale(4, RoundingMode.HALF_UP);
        BigDecimal sell = BigDecimal.valueOf(mid + spread / 2).setScale(4, RoundingMode.HALF_UP);
        BigDecimal bl = BigDecimal.valueOf(mid).setScale(4, RoundingMode.HALF_UP);

        LocalDateTime now = LocalDateTime.now();
        FxRatePushDTO dto = new FxRatePushDTO();
        dto.setCcyPair(ccyPair);
        dto.setChannelCd(channel);
        dto.setBuyPrice(buy);
        dto.setSellPrice(sell);
        dto.setBlPrice(bl);
        dto.setDeliTyp(deliTyp);
        dto.setDtChannelPublish(now.format(DATE_FMT));
        dto.setTmChannelPublish(now.format(TIME_FMT));
        // ISO-8601 UTC 24 字符串，毫秒精度（DTO 端 @Pattern 强制 .SSS 三位）
        dto.setUtcTimes(UTC_FMT.format(Instant.now()));
        return dto;
    }
}

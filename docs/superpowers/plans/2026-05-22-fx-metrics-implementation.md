# FX Disruptor 监控指标实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 按 `docs/superpowers/specs/2026-05-22-fx-metrics-design.md` 落地 16 个 fx.* 指标，包含 Meter facade、周期 JSON Reporter、9 处接入点改造。

**Architecture:** 集中式 `FxMetrics` Component 持有所有 Meter（final 字段），3 Gauge 通过方法引用零侵入接入 Buffer / RingBuffer；8 Counter + 3 Timer（HDR + 1min 滚动）+ 2 DistributionSummary 字段化；`FxMetricsReporter` `@Scheduled` 周期读字段输出结构化 JSON；其余 9 个现有类只增 `recordXxx` / `incXxx` 调用，不引业务逻辑。

**Tech Stack:** Spring Boot 2.7.18 + Micrometer (随 spring-boot-starter-actuator) + LMAX Disruptor 4.0.0 + fastjson2 + JUnit 5（spring-boot-starter-test 已在 pom）。

**Spec 参考：** `docs/superpowers/specs/2026-05-22-fx-metrics-design.md`

---

## 文件结构总览

### 新增文件

| 路径 | 职责 |
|------|------|
| `src/main/java/com/hayes/base/fx/monitor/FxMetrics.java` | Meter facade。注册 3 Gauge + 8 Counter + 3 Timer + 2 DS，对外只暴露语义化方法 `incXxx()` / `recordXxx()` + package-private getter 供 Reporter 用。 |
| `src/main/java/com/hayes/base/fx/monitor/FxMetricsReporter.java` | `@Scheduled` 周期 reporter。直接读 FxMetrics 字段，输出一行 `[fx-metrics] {json}`。`@ConditionalOnExpression` 控制 `metrics-log-interval-ms <= 0` 时不注册。 |
| `src/test/java/com/hayes/base/fx/monitor/FxMetricsTest.java` | 单测：用 `SimpleMeterRegistry` 验证全部 16 指标注册成功 + 所有语义化方法工作。 |
| `src/test/java/com/hayes/base/fx/monitor/FxMetricsReporterTest.java` | 单测：JSON 字段完整性 + 字段值与 Meter 状态一致。 |

### 修改文件

| 路径 | 改动概要 |
|------|---------|
| `pom.xml` | 加 `spring-boot-starter-actuator` 依赖 |
| `src/main/resources/application.yml` | 暴露 actuator `metrics,health`；新增 `app.fx.flush.metrics-log-interval-ms: 60000` |
| `src/main/java/com/hayes/base/fx/Application.java` | 加 `@EnableScheduling` |
| `src/main/java/com/hayes/base/fx/config/FxProperties.java` | `Flush` 内部类加 `metricsLogIntervalMs` 字段 |
| `src/main/java/com/hayes/base/fx/disruptor/FxRateEventProducer.java` | 注入 FxMetrics，`publish` 用 `recordPublish` 包裹，删除现有 INFO 级 `每条耗时` log |
| `src/main/java/com/hayes/base/fx/flusher/FxRateLatestFlusher.java` | 注入 FxMetrics，`flushOnce()` 内：成功路径调 `recordLatestFlush(size, nanos)`；catch 块调 `incLatestFallback()` |
| `src/main/java/com/hayes/base/fx/flusher/FxRateHistoryFlusher.java` | 同上，且 `drainUntilEmpty()` 的 catch 块也要调 `incHistoryFallback()` |
| `src/main/java/com/hayes/base/fx/disruptor/handler/FxRatePersistenceEventHandler.java` | 构造器加 `FxMetrics` 第 6 个参数；二次 offer 失败调 `incHistoryReject()`；外层 catch 调 `incHandlerUnexpectedError()` |
| `src/main/java/com/hayes/base/fx/config/DisruptorConfig.java` | 构造器注入 `FxMetrics`，`ringBuffer()` 创建 Handler 时透传 |
| `src/main/java/com/hayes/base/fx/service/FxRatePersistenceService.java` | 注入 FxMetrics；重试循环 i≥1 调 `incPersistRetry()`；attempts 全败调 `incPersistGiveup()`；`writeDlq` 成功后调 `incDlqWrite()`；DLQ 失败 catch 块调 `incDlqWriteFailure()` |

**不改动：** `FxRateLatestBuffer`、`FxRateHistoryBuffer`（Gauge 通过方法引用接入）

### 关键设计决策

1. **`metrics-log-interval-ms <= 0` 时整个 Reporter Bean 不注册** —— 用 `@ConditionalOnExpression("${app.fx.flush.metrics-log-interval-ms:60000} > 0")`。这比"方法体内 if return"更稳：避免 `@Scheduled(fixedDelay = 0)` 在 Spring 中的"立即重复触发"未定义行为。
2. **3 个 Gauge 不持有字段引用**（特例） —— Gauge 注册期通过方法引用挂在 registry 上，无字段化必要；Reporter 用 `registry.find(name).gauge()` 读，**但 Counter / Timer / DS 必须走 FxMetrics 字段**（spec §一明确）。
3. **Timer / DS 必须显式 `publishPercentileHistogram()` + 1min 滚动窗口** —— 否则 P99 长期偏差或 NaN（spec §二）。
4. **`fx.ring_buffer.remaining_capacity` 注册时附 `buffer_size` tag** —— Reporter 把 tag 平铺到 JSON 字段 `ring_buffer_size`（spec §二）。
5. **DTO 全部用 `FxRateEvent`**，不要重复造领域对象。

---

## Task 1: 加 actuator 依赖

**Files:**
- Modify: `pom.xml`（在 `</dependencies>` 之前插入）

- [x] **Step 1: 在 pom.xml 加 actuator 依赖**

在 `pom.xml:134` 后（`spring-boot-starter-validation` 之后、`</dependencies>` 之前）插入：

```xml
        <!-- Actuator + Micrometer：fx.* 指标暴露与 /actuator/metrics 端点 -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-actuator</artifactId>
        </dependency>
```

- [x] **Step 2: 验证依赖能拉到**

Run: `mvn -Denv=dev dependency:tree | grep -E "actuator|micrometer-core"`
Expected: 列出 `spring-boot-starter-actuator` 和 `micrometer-core`（通常 1.9.x）

- [x] **Step 3: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 4: Commit**

```bash
git add pom.xml
git commit -m "build: add spring-boot-starter-actuator for fx.* metrics"
```

---

## Task 2: FxProperties 加 metricsLogIntervalMs 字段

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/config/FxProperties.java`（`Flush` 静态内部类末尾）

- [x] **Step 1: 在 Flush 内部类末尾追加字段**

在 `FxProperties.java:71`（`private int historyQueueWarnThreshold = 50_000;` 之后、`}` 之前）插入：

```java

        /**
         * 结构化指标日志（FxMetricsReporter）输出间隔（ms）。
         * <p>
         * 60000 = 每 60s 一行 [fx-metrics] JSON；<=0 时 Reporter Bean 整体不注册（由
         * @ConditionalOnExpression 控制），避免 Spring @Scheduled fixedDelay=0 的未定义行为。
         */
        private int metricsLogIntervalMs = 60_000;
```

- [x] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/config/FxProperties.java
git commit -m "feat(props): add metricsLogIntervalMs (default 60000)"
```

---

## Task 3: Application 加 @EnableScheduling

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/Application.java`

- [x] **Step 1: 加 import 和注解**

修改 `Application.java`：

```java
package com.hayes.base.fx;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * 应用启动类
 * <p>
 * 作用：Spring Boot 工程入口，加载自动配置并启动内嵌容器。
 * 包扫描：默认扫描 {@code com.hayes.base.fx} 及其子包；
 * {@link MapperScan} 明确扫描 MyBatis Mapper 接口包；
 * {@link EnableScheduling} 启用 @Scheduled（FxMetricsReporter 依赖此注解，
 * 不加则 reporter 永远不会触发）。
 */
@SpringBootApplication
@MapperScan("com.hayes.base.fx.mapper")
@EnableScheduling
public class Application {

    /**
     * 主入口
     *
     * @param args 启动参数，可通过 --spring.profiles.active=xxx 指定激活环境
     */
    public static void main(String[] args) {
        // 启动 Spring 容器，返回上下文用于打印启动信息
        ConfigurableApplicationContext context = SpringApplication.run(Application.class, args);
        Environment env = context.getEnvironment();
        // 读取当前服务端口与激活 profile，便于启动后直接定位服务地址
        String port = env.getProperty("server.port", "8080");
        String contextPath = env.getProperty("server.servlet.context-path", "");
        String[] profiles = env.getActiveProfiles();
        System.out.println("\n----------------------------------------------------------");
        System.out.println("  Application '" + env.getProperty("spring.application.name") + "' is running!");
        System.out.println("  Local:   http://localhost:" + port + contextPath);
        System.out.println("  Profile: " + (profiles.length == 0 ? "default" : String.join(",", profiles)));
        System.out.println("----------------------------------------------------------\n");
    }
}
```

- [x] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/Application.java
git commit -m "feat: enable @Scheduled support via @EnableScheduling"
```

---

## Task 4: 实现 FxMetrics（含单测）

**Files:**
- Create: `src/main/java/com/hayes/base/fx/monitor/FxMetrics.java`
- Create: `src/test/java/com/hayes/base/fx/monitor/FxMetricsTest.java`

- [x] **Step 1: 先写失败的单测**

创建 `src/test/java/com/hayes/base/fx/monitor/FxMetricsTest.java`：

```java
package com.hayes.base.fx.monitor;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * FxMetrics 单测：用 SimpleMeterRegistry 验证 16 个指标全部注册成功 + 语义化方法工作。
 */
class FxMetricsTest {

    private SimpleMeterRegistry registry;
    private Disruptor<FxRateEvent> disruptor;
    private RingBuffer<FxRateEvent> ringBuffer;
    private FxRateLatestBuffer latestBuffer;
    private FxRateHistoryBuffer historyBuffer;
    private FxMetrics metrics;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        // 构造一个最小 Disruptor 以拿到真实 RingBuffer；不 start，仅用于 metrics 注册
        disruptor = new Disruptor<>(
                new FxRateEventFactory(), 1024, Executors.defaultThreadFactory(), ProducerType.MULTI,
                new com.lmax.disruptor.BlockingWaitStrategy());
        ringBuffer = disruptor.getRingBuffer();

        latestBuffer = new FxRateLatestBuffer();
        FxProperties props = new FxProperties();
        historyBuffer = new FxRateHistoryBuffer(props);

        metrics = new FxMetrics(registry, ringBuffer, latestBuffer, historyBuffer);
    }

    @AfterEach
    void tearDown() {
        if (disruptor != null) {
            disruptor.halt();
        }
    }

    @Test
    void allSixteenMetricsRegistered() {
        Set<String> expected = Set.of(
                "fx.ring_buffer.remaining_capacity",
                "fx.history_buffer.size",
                "fx.latest_buffer.size",
                "fx.history_buffer.reject",
                "fx.handler.unexpected_error",
                "fx.flush.latest.fallback",
                "fx.flush.history.fallback",
                "fx.persist.retry",
                "fx.persist.giveup",
                "fx.dlq.write",
                "fx.dlq.write.failure",
                "fx.producer.publish.duration",
                "fx.flush.latest.batch.duration",
                "fx.flush.history.batch.duration",
                "fx.flush.latest.batch.size",
                "fx.flush.history.batch.size"
        );
        Set<String> actual = registry.getMeters().stream()
                .map(m -> m.getId().getName())
                .collect(Collectors.toSet());
        assertTrue(actual.containsAll(expected),
                "missing meters: " + expected.stream().filter(n -> !actual.contains(n)).toList());
    }

    @Test
    void ringBufferGaugeCarriesBufferSizeTag() {
        Meter g = registry.find("fx.ring_buffer.remaining_capacity").meter();
        assertNotNull(g);
        assertEquals("1024", g.getId().getTag("buffer_size"));
    }

    @Test
    void counterIncrementsWork() {
        metrics.incHistoryReject();
        metrics.incHistoryReject();
        metrics.incDlqWriteFailure();
        assertEquals(2.0, registry.find("fx.history_buffer.reject").counter().count());
        assertEquals(1.0, registry.find("fx.dlq.write.failure").counter().count());
    }

    @Test
    void timerRecordWorks() {
        metrics.recordPublish(1_500_000L);   // 1.5ms
        metrics.recordPublish(2_500_000L);   // 2.5ms
        assertEquals(2L, registry.find("fx.producer.publish.duration").timer().count());
    }

    @Test
    void recordLatestFlushUpdatesBothTimerAndSummary() {
        metrics.recordLatestFlush(120, 5_000_000L);
        assertEquals(1L, registry.find("fx.flush.latest.batch.duration").timer().count());
        assertEquals(1L, registry.find("fx.flush.latest.batch.size").summary().count());
        assertEquals(120.0, registry.find("fx.flush.latest.batch.size").summary().totalAmount());
    }
}
```

- [x] **Step 2: 跑测试确认全部失败（FxMetrics 还没建）**

Run: `mvn -Denv=dev test -Dtest=FxMetricsTest`
Expected: COMPILE FAIL — `FxMetrics` 类不存在

- [x] **Step 3: 实现 FxMetrics**

创建 `src/main/java/com/hayes/base/fx/monitor/FxMetrics.java`：

```java
package com.hayes.base.fx.monitor;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.lmax.disruptor.RingBuffer;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * 指标门面：集中持有所有 fx.* Meter 实例。
 * <p>
 * 设计要点：
 * 1. 3 Gauge 通过方法引用零侵入接入 RingBuffer / 两个 Buffer，bean 由 Spring 强引用持有，无 weak ref 失效风险；
 *    `fx.ring_buffer.remaining_capacity` 附 `buffer_size` tag 便于运维直接判断危险度；
 * 2. 8 Counter / 3 Timer / 2 DistributionSummary 全部 final 字段持有，对外只暴露语义化方法 incXxx/recordXxx；
 *    Reporter 通过 package-private getter 直接取引用，避免按 name 反向查询的脆弱与拼写错误；
 * 3. Timer / DS 显式开 publishPercentileHistogram + 1min 滚动窗口（5 桶），
 *    否则在线无衰减算法下 P99 长期偏差或为 NaN。
 */
@Component
public class FxMetrics {

    // ===== Counter =====
    private final Counter historyReject;
    private final Counter handlerUnexpectedError;
    private final Counter latestFallback;
    private final Counter historyFallback;
    private final Counter persistRetry;
    private final Counter persistGiveup;
    private final Counter dlqWrite;
    private final Counter dlqWriteFailure;

    // ===== Timer =====
    private final Timer publishTimer;
    private final Timer latestFlushTimer;
    private final Timer historyFlushTimer;

    // ===== DistributionSummary =====
    private final DistributionSummary latestBatchSize;
    private final DistributionSummary historyBatchSize;

    public FxMetrics(MeterRegistry registry,
                     RingBuffer<FxRateEvent> ringBuffer,
                     FxRateLatestBuffer latestBuffer,
                     FxRateHistoryBuffer historyBuffer) {
        // ===== Gauge：方法引用接入，不持字段（注册到 registry 即可由 Reporter 按 name 取）=====
        Gauge.builder("fx.ring_buffer.remaining_capacity", ringBuffer,
                        rb -> (double) rb.remainingCapacity())
                .description("RingBuffer 剩余可写槽位")
                .tag("buffer_size", String.valueOf(ringBuffer.getBufferSize()))
                .register(registry);

        Gauge.builder("fx.history_buffer.size", historyBuffer,
                        hb -> (double) hb.size())
                .description("历史队列当前堆积条数")
                .register(registry);

        Gauge.builder("fx.latest_buffer.size", latestBuffer,
                        lb -> (double) lb.size())
                .description("报价缓存 coalesce map 条目数")
                .register(registry);

        // ===== Counter =====
        this.historyReject = Counter.builder("fx.history_buffer.reject")
                .description("历史队列二次 offer 仍失败累计")
                .register(registry);
        this.handlerUnexpectedError = Counter.builder("fx.handler.unexpected_error")
                .description("Handler 外层兜底 catch 累计触发")
                .register(registry);
        this.latestFallback = Counter.builder("fx.flush.latest.fallback")
                .description("报价批量 UPSERT 失败降级逐条累计")
                .register(registry);
        this.historyFallback = Counter.builder("fx.flush.history.fallback")
                .description("历史批量 INSERT 失败降级逐条累计（含 shutdown）")
                .register(registry);
        this.persistRetry = Counter.builder("fx.persist.retry")
                .description("单条 persist 进入第 2 次及以后重试累计")
                .register(registry);
        this.persistGiveup = Counter.builder("fx.persist.giveup")
                .description("重试全部用尽仍失败累计")
                .register(registry);
        this.dlqWrite = Counter.builder("fx.dlq.write")
                .description("DLQ 成功写入累计")
                .register(registry);
        this.dlqWriteFailure = Counter.builder("fx.dlq.write.failure")
                .description("DLQ 写入也失败累计（P0：数据真丢）")
                .register(registry);

        // ===== Timer =====
        this.publishTimer = buildTimer(registry, "fx.producer.publish.duration",
                "RingBuffer 投递耗时");
        this.latestFlushTimer = buildTimer(registry, "fx.flush.latest.batch.duration",
                "报价批量 UPSERT 耗时");
        this.historyFlushTimer = buildTimer(registry, "fx.flush.history.batch.duration",
                "历史批量 INSERT 耗时");

        // ===== DistributionSummary =====
        this.latestBatchSize = buildSummary(registry, "fx.flush.latest.batch.size",
                "单次报价 flush 条数");
        this.historyBatchSize = buildSummary(registry, "fx.flush.history.batch.size",
                "单次历史 flush 条数");
    }

    /** 统一 Timer 构造：HDR 直方图 + 1min 滚动窗口（5 桶） */
    private static Timer buildTimer(MeterRegistry r, String name, String desc) {
        return Timer.builder(name)
                .description(desc)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .distributionStatisticBufferLength(5)
                .register(r);
    }

    /** 统一 DS 构造：同 Timer */
    private static DistributionSummary buildSummary(MeterRegistry r, String name, String desc) {
        return DistributionSummary.builder(name)
                .description(desc)
                .publishPercentiles(0.5, 0.95, 0.99)
                .publishPercentileHistogram()
                .distributionStatisticExpiry(Duration.ofMinutes(1))
                .distributionStatisticBufferLength(5)
                .register(r);
    }

    // ===== Counter 语义化方法 =====
    public void incHistoryReject()         { historyReject.increment(); }
    public void incHandlerUnexpectedError() { handlerUnexpectedError.increment(); }
    public void incLatestFallback()         { latestFallback.increment(); }
    public void incHistoryFallback()        { historyFallback.increment(); }
    public void incPersistRetry()           { persistRetry.increment(); }
    public void incPersistGiveup()          { persistGiveup.increment(); }
    public void incDlqWrite()               { dlqWrite.increment(); }
    public void incDlqWriteFailure()        { dlqWriteFailure.increment(); }

    // ===== Timer / DS 语义化方法 =====
    /** 记录一次 publish 耗时（入参单位：纳秒） */
    public void recordPublish(long nanos) {
        publishTimer.record(nanos, TimeUnit.NANOSECONDS);
    }

    /** 记录一次报价 flush：batchSize 进 DS，nanos 进 Timer */
    public void recordLatestFlush(int batchSize, long nanos) {
        latestFlushTimer.record(nanos, TimeUnit.NANOSECONDS);
        latestBatchSize.record(batchSize);
    }

    /** 记录一次历史 flush：batchSize 进 DS，nanos 进 Timer */
    public void recordHistoryFlush(int batchSize, long nanos) {
        historyFlushTimer.record(nanos, TimeUnit.NANOSECONDS);
        historyBatchSize.record(batchSize);
    }

    // ===== package-private getter（仅供 FxMetricsReporter 取 Snapshot）=====
    Counter historyRejectCounter()         { return historyReject; }
    Counter handlerUnexpectedErrorCounter() { return handlerUnexpectedError; }
    Counter latestFallbackCounter()         { return latestFallback; }
    Counter historyFallbackCounter()        { return historyFallback; }
    Counter persistRetryCounter()           { return persistRetry; }
    Counter persistGiveupCounter()          { return persistGiveup; }
    Counter dlqWriteCounter()               { return dlqWrite; }
    Counter dlqWriteFailureCounter()        { return dlqWriteFailure; }

    Timer publishTimer()         { return publishTimer; }
    Timer latestFlushTimer()     { return latestFlushTimer; }
    Timer historyFlushTimer()    { return historyFlushTimer; }
    DistributionSummary latestBatchSizeSummary()  { return latestBatchSize; }
    DistributionSummary historyBatchSizeSummary() { return historyBatchSize; }
}
```

- [x] **Step 4: 跑单测确认全部通过**

Run: `mvn -Denv=dev test -Dtest=FxMetricsTest`
Expected: `Tests run: 5, Failures: 0, Errors: 0`

- [x] **Step 5: Commit**

```bash
git add src/main/java/com/hayes/base/fx/monitor/FxMetrics.java \
        src/test/java/com/hayes/base/fx/monitor/FxMetricsTest.java
git commit -m "feat(monitor): introduce FxMetrics facade (3 Gauge + 8 Counter + 3 Timer + 2 DS)"
```

---

## Task 5: 实现 FxMetricsReporter（含单测）

**Files:**
- Create: `src/main/java/com/hayes/base/fx/monitor/FxMetricsReporter.java`
- Create: `src/test/java/com/hayes/base/fx/monitor/FxMetricsReporterTest.java`

- [x] **Step 1: 先写失败的单测**

创建 `src/test/java/com/hayes/base/fx/monitor/FxMetricsReporterTest.java`：

```java
package com.hayes.base.fx.monitor;

import com.alibaba.fastjson2.JSONObject;
import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Reporter 单测：验证 JSON 字段完整 + 字段值与 Meter 状态一致。
 * <p>
 * 不测 @Scheduled 触发（那是 Spring 行为，不需要二次验证），只测 report() 方法体。
 */
class FxMetricsReporterTest {

    private SimpleMeterRegistry registry;
    private Disruptor<FxRateEvent> disruptor;
    private FxMetrics metrics;
    private FxMetricsReporter reporter;

    @BeforeEach
    void setUp() {
        registry = new SimpleMeterRegistry();
        disruptor = new Disruptor<>(
                new FxRateEventFactory(), 1024, Executors.defaultThreadFactory(),
                ProducerType.MULTI, new BlockingWaitStrategy());
        RingBuffer<FxRateEvent> ringBuffer = disruptor.getRingBuffer();
        FxRateLatestBuffer latestBuffer = new FxRateLatestBuffer();
        FxProperties props = new FxProperties();
        FxRateHistoryBuffer historyBuffer = new FxRateHistoryBuffer(props);
        metrics = new FxMetrics(registry, ringBuffer, latestBuffer, historyBuffer);
        reporter = new FxMetricsReporter(metrics, registry, props);
    }

    @AfterEach
    void tearDown() {
        if (disruptor != null) disruptor.halt();
    }

    @Test
    void jsonContainsAllExpectedFields() {
        JSONObject json = reporter.buildSnapshot();
        Set<String> expected = Set.of(
                "ts",
                "ring_buffer_remaining", "ring_buffer_size",
                "history_queue_size", "latest_buffer_size",
                "history_reject_total", "handler_unexpected_error_total",
                "latest_fallback_total", "history_fallback_total",
                "persist_retry_total", "persist_giveup_total",
                "dlq_write_total", "dlq_write_failure_total",
                "publish_p99_ms", "publish_max_ms",
                "latest_flush_p99_ms", "latest_flush_max_ms", "latest_batch_size_p99",
                "history_flush_p99_ms", "history_flush_max_ms", "history_batch_size_p99"
        );
        assertTrue(json.keySet().containsAll(expected),
                "missing keys: " + expected.stream().filter(k -> !json.containsKey(k)).toList());
    }

    @Test
    void counterValuesReflectMetricsState() {
        metrics.incDlqWrite();
        metrics.incDlqWrite();
        metrics.incDlqWriteFailure();
        JSONObject json = reporter.buildSnapshot();
        assertEquals(2L, json.getLongValue("dlq_write_total"));
        assertEquals(1L, json.getLongValue("dlq_write_failure_total"));
    }

    @Test
    void ringBufferSizeTagExposedAsField() {
        JSONObject json = reporter.buildSnapshot();
        assertEquals(1024L, json.getLongValue("ring_buffer_size"));
    }

    @Test
    void timerP99NotNaNAfterHistogramEnabled() {
        // 注入足够样本让 HDR 出 p99
        for (int i = 0; i < 100; i++) {
            metrics.recordPublish(1_000_000L * (i + 1)); // 1ms..100ms
        }
        JSONObject json = reporter.buildSnapshot();
        double p99 = json.getDoubleValue("publish_p99_ms");
        assertTrue(p99 > 0 && !Double.isNaN(p99),
                "p99 should be a positive finite number, got " + p99);
    }
}
```

- [x] **Step 2: 跑测试确认全部失败**

Run: `mvn -Denv=dev test -Dtest=FxMetricsReporterTest`
Expected: COMPILE FAIL — `FxMetricsReporter` 不存在

- [x] **Step 3: 实现 FxMetricsReporter**

创建 `src/main/java/com/hayes/base/fx/monitor/FxMetricsReporter.java`：

```java
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
```

- [x] **Step 4: 跑单测确认通过**

Run: `mvn -Denv=dev test -Dtest=FxMetricsReporterTest`
Expected: `Tests run: 4, Failures: 0, Errors: 0`

- [x] **Step 5: Commit**

```bash
git add src/main/java/com/hayes/base/fx/monitor/FxMetricsReporter.java \
        src/test/java/com/hayes/base/fx/monitor/FxMetricsReporterTest.java
git commit -m "feat(monitor): add FxMetricsReporter (@Scheduled JSON snapshot, ConditionalOnExpression-disable)"
```

---

## Task 6: 接入 FxRateEventProducer

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/disruptor/FxRateEventProducer.java`

- [x] **Step 1: 改 publish 用 FxMetrics 计时 + 删 INFO 日志**

完整替换 `FxRateEventProducer.java`：

```java
package com.hayes.base.fx.disruptor;

import com.hayes.base.fx.dto.FxRatePushDTO;
import com.hayes.base.fx.monitor.FxMetrics;
import com.lmax.disruptor.RingBuffer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

/**
 * 发布器：把入参 DTO 投递到 Disruptor RingBuffer
 * <p>
 * RingBuffer 由 {@code DisruptorConfig} 注入；
 * 使用 {@code publishEvent(translator, dto, traceId)} 的方式避免显式 next/publish 配对错误。
 * Ring 满时由当前配置的 WaitStrategy 决定阻塞或告警——当前默认 BlockingWaitStrategy 会阻塞生产线程，
 * 这是对后端 MySQL 堆积压力的天然背压。
 * <p>
 * 监控：每条 publish 的耗时由 {@link FxMetrics#recordPublish} 进入 Timer
 * {@code fx.producer.publish.duration}（HDR 直方图 + 1min 滚动 P99），
 * 不再每条打 INFO 日志（高 QPS 下会灌爆日志）。
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class FxRateEventProducer {

    private final RingBuffer<FxRateEvent> ringBuffer;
    private final FxMetrics metrics;

    /**
     * 单条投递
     *
     * @param dto     入参
     * @param traceId 链路追踪 ID，调用方生成
     */
    public void publish(FxRatePushDTO dto, String traceId) {
        long t0 = System.nanoTime();
        try {
            ringBuffer.publishEvent(FxRateEventTranslator.INSTANCE, dto, traceId);
        } finally {
            // 无论成功失败都记录耗时，便于发现 publish 阻塞（BlockingWaitStrategy 满载时 P99 飙升）
            metrics.recordPublish(System.nanoTime() - t0);
        }
    }
}
```

- [x] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/disruptor/FxRateEventProducer.java
git commit -m "feat(metrics): wire FxMetrics.recordPublish in Producer; drop per-event INFO log"
```

---

## Task 7: 接入 FxRateLatestFlusher

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/flusher/FxRateLatestFlusher.java`

- [x] **Step 1: 改构造器加 FxMetrics + 改 flushOnce 计时计数**

完整替换 `FxRateLatestFlusher.java`：

```java
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
        flushOnceSilently();
        log.info("[fx-flusher-latest] stopped.");
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
```

- [x] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/flusher/FxRateLatestFlusher.java
git commit -m "feat(metrics): wire FxMetrics in FxRateLatestFlusher (recordLatestFlush + latest.fallback)"
```

---

## Task 8: 接入 FxRateHistoryFlusher（含 drainUntilEmpty 兜底路径）

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/flusher/FxRateHistoryFlusher.java`

- [x] **Step 1: 改构造器 + flushOnce + drainUntilEmpty 两处 fallback 计数**

完整替换 `FxRateHistoryFlusher.java`：

```java
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
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
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

    public void flushNow() {
        flushOnceSilently();
    }

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
        int safetyLimit = 100;
        while (safetyLimit-- > 0) {
            List<FxRateEvent> batch = new ArrayList<>();
            int drained = buffer.drainAll(batch);
            if (drained == 0) {
                return;
            }
            long t0 = System.nanoTime();
            try {
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
```

- [x] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [x] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/flusher/FxRateHistoryFlusher.java
git commit -m "feat(metrics): wire FxMetrics in HistoryFlusher (record + fallback on both paths)"
```

---

## Task 9: 接入 FxRatePersistenceEventHandler + DisruptorConfig 透传

> 同步改：Handler 构造参数从 5 → 6，DisruptorConfig 必须同时同步，否则编译失败。

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/disruptor/handler/FxRatePersistenceEventHandler.java`
- Modify: `src/main/java/com/hayes/base/fx/config/DisruptorConfig.java`

- [ ] **Step 1: 改 Handler 加 FxMetrics 参数 + 两处计数**

完整替换 `FxRatePersistenceEventHandler.java`：

```java
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
        if ((sequence % workerCount) != myIndex) {
            return;
        }
        try {
            long t0 = System.nanoTime();
            // 报价通道：深拷贝 + coalesce（按 UTC_TIMES 保序）
            FxRateEvent forLatest = event.copy();
            LatestKey key = new LatestKey(forLatest.getChannelCd(), forLatest.getCcyPair(), forLatest.getDeliTyp());
            latestBuffer.merge(key, forLatest);

            // 历史通道：深拷贝 + 入队（不 coalesce）
            FxRateEvent forHis = event.copy();
            if (!historyBuffer.offer(forHis)) {
                historyFlusher.flushNow();
                if (!historyBuffer.offer(forHis)) {
                    // 历史通道真丢一条
                    metrics.incHistoryReject();
                    log.error("[fx-rate] history buffer full after flushNow, fallback to sync persist. traceId={} ccyPair={}",
                            forHis.getTraceId(), forHis.getCcyPair());
                }
            }

            if (log.isDebugEnabled()) {
                log.debug("[fx-rate] onEvent enqueue cost nanos={}", System.nanoTime() - t0);
            }
        } catch (Throwable ex) {
            // 外层兜底：不让异常冒出（否则 Disruptor 停），并计入未预期错误
            metrics.incHandlerUnexpectedError();
            log.error("[fx-rate] handler#{} 未预期异常 traceId={} ccyPair={} channelCd={}",
                    myIndex, event.getTraceId(), event.getCcyPair(), event.getChannelCd(), ex);
        }
    }
}
```

- [ ] **Step 2: 改 DisruptorConfig 注入 FxMetrics 并透传**

完整替换 `DisruptorConfig.java`：

```java
package com.hayes.base.fx.config;

import com.hayes.base.fx.buffer.FxRateHistoryBuffer;
import com.hayes.base.fx.buffer.FxRateLatestBuffer;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.hayes.base.fx.disruptor.handler.FxRatePersistenceEventHandler;
import com.hayes.base.fx.flusher.FxRateHistoryFlusher;
import com.hayes.base.fx.monitor.FxMetrics;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disruptor 装配
 * <p>
 * 注意循环依赖：FxMetrics 也需要 RingBuffer 才能注册 Gauge，而 RingBuffer 是本类的 @Bean。
 * 解决方案：FxMetrics 用 ObjectProvider<FxMetrics> 延迟解析，handler 创建时再取 → 此时 RingBuffer
 * 已是 ringBuffer() 方法局部变量，构造 FxMetrics Bean 时通过构造器注入拿到。
 * 调用时序：Spring 先调 ringBuffer() → 触发 FxMetrics Bean 实例化（拿到 ringBuffer 引用）
 *           → ObjectProvider.getObject() 返回该 FxMetrics → 透传给 handlers。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({DisruptorProperties.class, FxProperties.class})
public class DisruptorConfig implements SmartLifecycle {

    private final DisruptorProperties props;
    private final FxRateLatestBuffer latestBuffer;
    private final FxRateHistoryBuffer historyBuffer;
    private final FxRateHistoryFlusher historyFlusher;
    private final ObjectProvider<FxMetrics> metricsProvider;
    private Disruptor<FxRateEvent> disruptor;
    private final AtomicBoolean running = new AtomicBoolean(false);

    public DisruptorConfig(DisruptorProperties props,
                           FxRateLatestBuffer latestBuffer,
                           FxRateHistoryBuffer historyBuffer,
                           FxRateHistoryFlusher historyFlusher,
                           ObjectProvider<FxMetrics> metricsProvider) {
        this.props = props;
        this.latestBuffer = latestBuffer;
        this.historyBuffer = historyBuffer;
        this.historyFlusher = historyFlusher;
        this.metricsProvider = metricsProvider;
    }

    @Bean
    public RingBuffer<FxRateEvent> ringBuffer() {
        int workerCount = props.getWorkerCount();
        if (workerCount <= 0) {
            throw new IllegalArgumentException("app.disruptor.worker-count 必须 > 0");
        }
        if (Integer.bitCount(props.getRingBufferSize()) != 1) {
            throw new IllegalArgumentException("app.disruptor.ring-buffer-size 必须是 2 的幂");
        }

        ThreadFactory threadFactory = new NamedDaemonThreadFactory("fx-disruptor");

        disruptor = new Disruptor<>(
                new FxRateEventFactory(),
                props.getRingBufferSize(),
                threadFactory,
                ProducerType.MULTI,
                resolveWaitStrategy(props.getWaitStrategy())
        );

        // 通过 ObjectProvider 延迟解析，打破 RingBuffer ↔ FxMetrics 的构造依赖环
        FxMetrics metrics = metricsProvider.getObject();

        FxRatePersistenceEventHandler[] handlers = new FxRatePersistenceEventHandler[workerCount];
        for (int i = 0; i < workerCount; i++) {
            handlers[i] = new FxRatePersistenceEventHandler(
                    i, workerCount, latestBuffer, historyBuffer, historyFlusher, metrics);
        }
        disruptor.handleEventsWith(handlers);

        disruptor.start();
        log.info("[fx-disruptor] started. ringBufferSize={}, workerCount={}, waitStrategy={}",
                props.getRingBufferSize(), workerCount, props.getWaitStrategy());
        return disruptor.getRingBuffer();
    }

    @Override
    public void start() {
        running.set(true);
    }

    @Override
    public void stop() {
        if (!running.compareAndSet(true, false)) {
            return;
        }
        if (disruptor == null) {
            return;
        }
        try {
            disruptor.shutdown(props.getShutdownTimeoutSeconds(), TimeUnit.SECONDS);
            log.info("[fx-disruptor] shutdown gracefully.");
        } catch (Exception ex) {
            log.warn("[fx-disruptor] shutdown timeout after {}s, force halt.",
                    props.getShutdownTimeoutSeconds(), ex);
            disruptor.halt();
        }
    }

    @Override
    public boolean isRunning() {
        return running.get();
    }

    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    private static WaitStrategy resolveWaitStrategy(String name) {
        return switch (name == null ? "" : name.toLowerCase()) {
            case "yielding" -> new YieldingWaitStrategy();
            case "busy-spin", "busyspin" -> new BusySpinWaitStrategy();
            case "sleeping" -> new SleepingWaitStrategy();
            case "blocking" -> new BlockingWaitStrategy();
            default -> new BlockingWaitStrategy();
        };
    }

    private static class NamedDaemonThreadFactory implements ThreadFactory {
        private final String prefix;
        private final AtomicInteger seq = new AtomicInteger();

        NamedDaemonThreadFactory(String prefix) {
            this.prefix = prefix;
        }

        @Override
        public Thread newThread(Runnable r) {
            Thread t = new Thread(r, prefix + "-" + seq.incrementAndGet());
            t.setDaemon(true);
            return t;
        }
    }
}
```

- [ ] **Step 3: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 4: 跑现有单测确认没回归**

Run: `mvn -Denv=dev test -Dtest=FxMetricsTest,FxMetricsReporterTest`
Expected: 9 tests passed

- [ ] **Step 5: Commit**

```bash
git add src/main/java/com/hayes/base/fx/disruptor/handler/FxRatePersistenceEventHandler.java \
        src/main/java/com/hayes/base/fx/config/DisruptorConfig.java
git commit -m "feat(metrics): wire FxMetrics in Handler + DisruptorConfig (ObjectProvider breaks cycle)"
```

---

## Task 10: 接入 FxRatePersistenceService（4 个计数点）

**Files:**
- Modify: `src/main/java/com/hayes/base/fx/service/FxRatePersistenceService.java`

- [ ] **Step 1: 注入 FxMetrics + 4 处计数点**

完整替换 `FxRatePersistenceService.java`：

```java
package com.hayes.base.fx.service;

import com.hayes.base.fx.common.FxConst;
import com.hayes.base.fx.common.FxTimeUtils;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.entity.FxXrInfDlq;
import com.hayes.base.fx.mapper.FxXrInfDlqMapper;
import com.hayes.base.fx.mapper.FxXrInfHisMapper;
import com.hayes.base.fx.mapper.FxXrInfMapper;
import com.hayes.base.fx.monitor.FxMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 汇率落库 Service
 * <p>
 * 监控接入：
 * - 每次进入第 i≥1 次重试 → {@link FxMetrics#incPersistRetry}（早期 DB 抖动信号）；
 * - attempts 全败 → {@link FxMetrics#incPersistGiveup}（与 dlq.write 不重叠，DLQ 可 disabled）；
 * - DLQ 成功写入 → {@link FxMetrics#incDlqWrite}（P1 告警）；
 * - DLQ 自身失败 → {@link FxMetrics#incDlqWriteFailure}（**P0：数据真丢**）。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FxRatePersistenceService {

    private final FxXrInfMapper fxXrInfMapper;
    private final FxXrInfHisMapper fxXrInfHisMapper;
    private final FxXrInfDlqMapper fxXrInfDlqMapper;
    private final FxProperties fxProps;
    private final FxMetrics metrics;
    /** 自注入以保证 @Transactional 生效（内部方法调用要走代理） */
    private final org.springframework.context.ApplicationContext appCtx;

    /**
     * 对外入口：持久化单个事件，失败则指数退避重试，仍失败则写 DLQ / 日志
     */
    public void persist(FxRateEvent event) {
        int attempts = fxProps.getRetryTimes() + 1;
        List<Long> backoff = fxProps.getRetryBackoffMsList();
        Throwable last = null;

        FxRatePersistenceService self = appCtx.getBean(FxRatePersistenceService.class);

        for (int i = 0; i < attempts; i++) {
            if (i > 0) {
                // 进入第 2 次及以后的尝试 = 真实"重试"
                metrics.incPersistRetry();
            }
            try {
                self.doPersist(event);
                if (i > 0) {
                    log.warn("[fx-rate] persist recovered after {} retries. traceId={}", i, event.getTraceId());
                }
                return;
            } catch (Throwable ex) {
                last = ex;
                log.warn("[fx-rate] persist attempt {}/{} failed. traceId={} reason={}",
                        i + 1, attempts, event.getTraceId(), ex.getMessage());
                if (i < attempts - 1) {
                    sleepQuietly(backoff.get(Math.min(i, backoff.size() - 1)));
                }
            }
        }

        // 全部重试失败
        metrics.incPersistGiveup();
        log.error("[fx-rate] persist FINALLY failed after {} attempts. traceId={} ccyPair={} channelCd={}",
                attempts, event.getTraceId(), event.getCcyPair(), event.getChannelCd(), last);
        if (fxProps.isDlqEnabled()) {
            try {
                self.writeDlq(event, last);
                // DLQ 成功
                metrics.incDlqWrite();
            } catch (Throwable dlqEx) {
                // P0：DLQ 也写失败 = 数据真丢，必须独立计数
                metrics.incDlqWriteFailure();
                log.error("[fx-rate] DLQ write failed. traceId={}", event.getTraceId(), dlqEx);
            }
        }
    }

    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public void doPersist(FxRateEvent event) {
        String opCode = fxProps.getOpCode();
        String today = FxTimeUtils.today();
        String now = FxTimeUtils.now();
        int todayInt = FxTimeUtils.todayInt();

        if (StringUtils.isBlank(event.getDeliTyp())) {
            event.setDeliTyp(fxProps.getDefaultDeliTyp());
        }

        fxXrInfMapper.upsert(event, opCode, today, now, fxProps.isOrderGuardEnabled());
        fxXrInfHisMapper.insertOne(event, opCode, todayInt, now, FxConst.SUCC_FLG_SUCCESS);
    }

    @Transactional(rollbackFor = Throwable.class)
    public void doPersistLatestBatch(List<FxRateEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        String opCode = fxProps.getOpCode();
        String today = FxTimeUtils.today();
        String now = FxTimeUtils.now();

        for (FxRateEvent e : events) {
            if (StringUtils.isBlank(e.getDeliTyp())) {
                e.setDeliTyp(fxProps.getDefaultDeliTyp());
            }
        }
        fxXrInfMapper.upsertBatch(events, opCode, today, now, fxProps.isOrderGuardEnabled());
    }

    @Transactional(rollbackFor = Throwable.class)
    public void doPersistHistoryBatch(List<FxRateEvent> events) {
        if (events == null || events.isEmpty()) {
            return;
        }
        String opCode = fxProps.getOpCode();
        String now = FxTimeUtils.now();
        int todayInt = FxTimeUtils.todayInt();

        for (FxRateEvent e : events) {
            if (StringUtils.isBlank(e.getDeliTyp())) {
                e.setDeliTyp(fxProps.getDefaultDeliTyp());
            }
        }
        fxXrInfHisMapper.insertBatch(events, opCode, todayInt, now, FxConst.SUCC_FLG_SUCCESS);
    }

    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public void writeDlq(FxRateEvent event, Throwable cause) {
        FxXrInfDlq dlq = new FxXrInfDlq();
        dlq.setTraceId(event.getTraceId());
        dlq.setCcyPair(event.getCcyPair());
        dlq.setChannelCd(event.getChannelCd());
        dlq.setDeliTyp(event.getDeliTyp());
        dlq.setBuyPrice(event.getBuyPrice());
        dlq.setSellPrice(event.getSellPrice());
        dlq.setBlPrice(event.getBlPrice());
        dlq.setDtChannelPublish(event.getDtChannelPublish());
        dlq.setTmChannelPublish(event.getTmChannelPublish());
        dlq.setUtcTimes(event.getUtcTimes());
        dlq.setFailReason(truncate(cause == null ? "unknown" : cause.toString(), 1000));
        dlq.setDtCte(FxTimeUtils.today());
        dlq.setTmCte(FxTimeUtils.now());
        fxXrInfDlqMapper.insert(dlq);
    }

    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
```

- [ ] **Step 2: 编译通过**

Run: `mvn -Denv=dev clean compile`
Expected: `BUILD SUCCESS`

- [ ] **Step 3: Commit**

```bash
git add src/main/java/com/hayes/base/fx/service/FxRatePersistenceService.java
git commit -m "feat(metrics): wire FxMetrics in PersistenceService (retry/giveup/dlq.write/dlq.write.failure)"
```

---

## Task 11: application.yml 加 actuator 暴露与配置项

**Files:**
- Modify: `src/main/resources/application.yml`

- [ ] **Step 1: 在 application.yml 末尾追加 management 与 metrics-log-interval-ms**

在 `application.yml:65`（`dlq-enabled: true` 之后）追加：

```yaml
    # 双通道异步批量落库配置
    flush:
      # 结构化指标日志（FxMetricsReporter）间隔（ms）
      # 60000 = 每 60s 输出一行 [fx-metrics] JSON；<=0 时 Reporter Bean 整体不注册
      metrics-log-interval-ms: 60000

# ==========================================================
# Actuator 监控端点
# 注意：默认暴露在业务端口 8080/actuator/*；生产环境若非纯内网，
# 应单独配置 management.server.port 或加 spring-security 鉴权
# ==========================================================
management:
  endpoints:
    web:
      exposure:
        # 仅暴露 metrics + health，避免泄漏 env / configprops
        include: metrics,health
  endpoint:
    metrics:
      enabled: true
```

- [ ] **Step 2: 启动验证 actuator 端点可用**

Run（一个终端启动应用）：
```bash
mvn -Denv=dev spring-boot:run
```

Run（另一终端验证）：
```bash
curl -s http://localhost:8080/actuator/metrics | jq '.names[] | select(startswith("fx"))' | sort
```

Expected: 列出 16 个 fx.* 指标名（顺序无关）：
```
"fx.dlq.write"
"fx.dlq.write.failure"
"fx.flush.history.batch.duration"
"fx.flush.history.batch.size"
"fx.flush.history.fallback"
"fx.flush.latest.batch.duration"
"fx.flush.latest.batch.size"
"fx.flush.latest.fallback"
"fx.handler.unexpected_error"
"fx.history_buffer.reject"
"fx.history_buffer.size"
"fx.latest_buffer.size"
"fx.persist.giveup"
"fx.persist.retry"
"fx.producer.publish.duration"
"fx.ring_buffer.remaining_capacity"
```

- [ ] **Step 3: Commit**

```bash
git add src/main/resources/application.yml
git commit -m "feat(config): expose actuator metrics endpoint + metrics-log-interval-ms"
```

---

## Task 12: 端到端验收（对齐 spec §九 7 条）

> 不写代码，只跑验证。每条验证产出可贴的命令输出作为 R12 证据。

**Files:** 无文件改动；按 spec `2026-05-22-fx-metrics-design.md` 验收清单逐条跑。

- [ ] **Step 1: 启动应用并保留终端**

Run（保持运行）：
```bash
mvn -Denv=dev clean package -DskipTests
java -jar target/hayes-fx-disruptor-dev.jar
```

Expected: 日志出现 `[fx-disruptor] started.` 与 `[fx-flusher-latest] started.` 与 `[fx-flusher-history] started.`

- [ ] **Step 2: 验收 #1 —— 16 个指标全部注册**

Run:
```bash
curl -s http://localhost:8080/actuator/metrics | jq '[.names[] | select(startswith("fx"))] | length'
```

Expected: `16`

- [ ] **Step 3: 验收 #2 —— 60s 后出现 [fx-metrics] JSON 快照**

发一条 push 制造样本：
```bash
curl -X POST http://localhost:8080/fx/push \
  -H 'Content-Type: application/json' \
  -d '{"ccyPair":"USDCNY","channelCd":"BOC","buyPrice":7.1234,"sellPrice":7.1256,"blPrice":7.1245,"deliTyp":"00","dtChannelPublish":"20260522","tmChannelPublish":"120000","utcTimes":"2026-05-22T12:00:00.000Z"}'
```

等待 ≥60s 后在应用日志中检索：
```bash
grep "\[fx-metrics\]" <应用日志路径>
```

Expected: 至少一条 JSON 行，包含 spec §六 所列的 21 个字段。

- [ ] **Step 4: 验收 #3 —— 历史队列满 reject 计数生效**

临时把 `application-dev.yml` 里 `app.fx.flush.history-queue-capacity` 改为 `10`（极小），重启；
用 `FxRatePushTest` 灌 1000 QPS 5 秒：
```bash
mvn -Denv=dev test -Dtest=FxRatePushTest#pushTest -Dpush.qps=1000 -Dpush.duration=5 -Dpush.workers=8
```

Run:
```bash
curl -s http://localhost:8080/actuator/metrics/fx.history_buffer.reject | jq '.measurements[0].value'
```

Expected: `> 0`，且应用日志中 `[fx-metrics]` 的 `history_reject_total` 与之一致。

- [ ] **Step 5: 验收 #4 —— 报价批量 P99 ≤ 50ms**

恢复 `history-queue-capacity` 为默认 `100000`，重启。跑 1000 QPS 60s：
```bash
mvn -Denv=dev test -Dtest=FxRatePushTest#pushTest -Dpush.qps=1000 -Dpush.duration=60 -Dpush.workers=32
```

Run:
```bash
curl -s http://localhost:8080/actuator/metrics/fx.flush.latest.batch.duration | jq '.measurements[] | select(.statistic=="VALUE") | .value'
```

Expected: P99 字段（也可读 `[fx-metrics]` 日志的 `latest_flush_p99_ms`）≤ 50.0

- [ ] **Step 6: 验收 #5 —— Timer P99 非 NaN（HDR 已开）**

Run:
```bash
curl -s http://localhost:8080/actuator/metrics/fx.producer.publish.duration?tag=phi:0.99 2>/dev/null || \
  curl -s http://localhost:8080/actuator/metrics/fx.producer.publish.duration | jq '.availableTags[] | select(.tag=="phi") | .values'
```

Expected: 输出包含 `"0.99"` 标签（确认 HDR 直方图启用）；
或在 `[fx-metrics]` JSON 中 `publish_p99_ms` 字段为正有限数（**不是 0 也不是 NaN**）。

- [ ] **Step 7: 验收 #6 —— DLQ 失败计数路径**

在 MySQL 端临时执行：
```sql
RENAME TABLE fx_xr_inf_dlq TO fx_xr_inf_dlq_bak;
```

然后用 mapper 中已知会失败的输入（例如违反 NOT NULL 约束的 push）触发重试 → DLQ 写入失败。
等待重试全失败 + DLQ 写入也失败后：
```bash
curl -s http://localhost:8080/actuator/metrics/fx.dlq.write.failure | jq '.measurements[0].value'
```

Expected: `≥ 1`

收尾恢复表：
```sql
RENAME TABLE fx_xr_inf_dlq_bak TO fx_xr_inf_dlq;
```

- [ ] **Step 8: 验收 #7 —— 禁用开关：interval=0 时 Reporter 不注册**

把 `application-dev.yml`（或临时启动参数）设置：
```yaml
app:
  fx:
    flush:
      metrics-log-interval-ms: 0
```

重启应用。Run:
```bash
grep "FxMetricsReporter" <启动日志> ; \
sleep 70 ; \
grep "\[fx-metrics\]" <应用日志> || echo "ok: no fx-metrics line, reporter disabled"
```

Expected: 启动日志中**没有** FxMetricsReporter 被装配的迹象；70s 内无 `[fx-metrics]` 输出。

恢复 `metrics-log-interval-ms: 60000`。

- [ ] **Step 9: 把全部 7 条验收的实际输出粘到 PR 描述或 commit message**

无 commit，只记录证据（对齐 R12）。

---

## Self-Review

### Spec 覆盖

| Spec 章节 | 对应 Task |
|----------|----------|
| §一 技术选型 actuator | Task 1 |
| §一 @EnableScheduling | Task 3 |
| §一 字段化 facade | Task 4 |
| §二 Gauge ×3（含 buffer_size tag） | Task 4 |
| §二 Counter ×8 | Task 4 + Task 6/7/8/9/10（接入） |
| §二 Timer ×3（HDR 配置） | Task 4 |
| §二 DistributionSummary ×2 | Task 4 |
| §三 FxMetrics / FxMetricsReporter | Task 4 / Task 5 |
| §四 改动现有文件 9 处 | Task 2/3/6/7/8/9/9/10/11（全覆盖） |
| §五 配置项 | Task 2 + Task 11 |
| §六 结构化日志 21 字段 | Task 5（含单测） |
| §七 actuator 示例 | Task 11 + Task 12 |
| §八 告警分级 | 仅文档，无对应代码 |
| §九 验收 7 条 | Task 12 |

无遗漏。

### Placeholder 扫描

无 TBD / TODO / "implement later" / "similar to Task N" / "appropriate error handling"。所有代码步骤都给出完整代码块。

### 类型一致性

- `FxMetrics` 构造器参数顺序在所有 task 中保持 `(MeterRegistry, RingBuffer<FxRateEvent>, FxRateLatestBuffer, FxRateHistoryBuffer)`
- Handler 构造参数 5→6：Task 9 同时改 Handler 和 DisruptorConfig，编译期强制一致
- `incXxx` / `recordXxx` 方法名在 spec 与 Task 4 定义、Task 6/7/8/9/10 调用处全部一致
- `metricsLogIntervalMs` 字段名一致（Task 2 定义、Task 5/11 引用）
- DistributionSummary 单测断言用 `totalAmount()`（micrometer 公开 API，非 `total()`）

无类型冲突。

---

## Execution Handoff

Plan complete and saved to `docs/superpowers/plans/2026-05-22-fx-metrics-implementation.md`. Two execution options:

**1. Subagent-Driven (recommended)** — Fresh subagent per task, review between tasks, fast iteration

**2. Inline Execution** — Execute tasks in this session using executing-plans, batch execution with checkpoints

Which approach?

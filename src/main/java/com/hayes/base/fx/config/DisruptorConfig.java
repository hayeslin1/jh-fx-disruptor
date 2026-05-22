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
 * 关键点：
 * 1. Disruptor 4.0.0 需要 ThreadFactory（Executor 构造函数已移除）；
 * 2. 通过 handleEventsWith(handler0..handlerN-1) 并行订阅，每个 handler 内部 modulo 分片；
 * 3. 实现 SmartLifecycle，phase = Integer.MAX_VALUE，确保关闭顺序为
 *    "先停 Disruptor → 再停 Flushers"：Spring 的 SmartLifecycle stop 按 phase 降序触发，
 *    Flushers 使用 MAX_VALUE-100，自然晚于本组件停止，从而能接住 in-flight 事件进入 buffer 后
 *    的最后一次 drain。
 * <p>
 * 4. 循环依赖处理：FxMetrics 也需要 RingBuffer 才能注册 Gauge，而 RingBuffer 是本类的 @Bean。
 *    解决方案：注入 {@link ObjectProvider}&lt;FxMetrics&gt; 延迟解析，在 {@link #ringBuffer()}
 *    方法体内、Disruptor 已构造完成后再调 {@code getObject()}——此时 Spring 才会真正实例化
 *    FxMetrics（其构造器消费 RingBuffer Bean）→ 返回给我们透传给 handlers。
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

    /**
     * 构造并启动 Disruptor，暴露 RingBuffer 给 Producer 注入
     */
    @Bean
    public RingBuffer<FxRateEvent> ringBuffer() {
        int workerCount = props.getWorkerCount();
        if (workerCount <= 0) {
            throw new IllegalArgumentException("app.disruptor.worker-count 必须 > 0");
        }
        if (Integer.bitCount(props.getRingBufferSize()) != 1) {
            throw new IllegalArgumentException("app.disruptor.ring-buffer-size 必须是 2 的幂");
        }

        // 命名线程工厂便于线上堆栈排查
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

        // 构建 N 个分片 handler；传给 handleEventsWith 的是"并行"订阅，每个都会看到全部 sequence，
        // 但 handler 内部按 sequence % N == myIndex 过滤——语义等价于 WorkerPool
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

    /**
     * SmartLifecycle.start：Disruptor 实际启动发生在 {@link #ringBuffer()} @Bean 里，
     * 这里仅标记状态，避免重复 start。
     */
    @Override
    public void start() {
        running.set(true);
    }

    /**
     * 容器关闭时优雅停机：等待 in-flight 事件消费完，使 buffer 接到所有剩余事件。
     * <p>
     * phase = Integer.MAX_VALUE，stop 顺序中本组件最先被触发——先停 Disruptor，
     * 让 in-flight 事件最后一次走完 handler 进入 latestBuffer / historyBuffer；
     * 然后 phase 较小的 Flushers（MAX-100）再 stop，把 buffer drain 干净。
     */
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

    /**
     * phase 最大：SmartLifecycle stop 按 phase 降序触发——本组件先停，Flushers 后停。
     */
    @Override
    public int getPhase() {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean isAutoStartup() {
        return true;
    }

    /**
     * 字符串 → WaitStrategy 映射
     */
    private static WaitStrategy resolveWaitStrategy(String name) {
        return switch (name == null ? "" : name.toLowerCase()) {
            case "yielding" -> new YieldingWaitStrategy();
            case "busy-spin", "busyspin" -> new BusySpinWaitStrategy();
            case "sleeping" -> new SleepingWaitStrategy();
            case "blocking" -> new BlockingWaitStrategy();
            default -> new BlockingWaitStrategy();
        };
    }

    /**
     * 带名字的守护线程工厂，便于 jstack / arthas 识别
     */
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

package com.hayes.base.fx.config;

import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.disruptor.FxRateEventFactory;
import com.hayes.base.fx.disruptor.handler.FxRatePersistenceEventHandler;
import com.hayes.base.fx.service.FxRatePersistenceService;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.BusySpinWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SleepingWaitStrategy;
import com.lmax.disruptor.WaitStrategy;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Disruptor 装配
 * <p>
 * 关键点：
 * 1. Disruptor 4.0.0 需要 ThreadFactory（Executor 构造函数已移除）；
 * 2. 通过 handleEventsWith(handler0..handlerN-1) 并行订阅，每个 handler 内部 modulo 分片；
 * 3. 实现 DisposableBean#destroy 以在 Spring 容器关闭时先停 Disruptor，避免丢事件。
 */
@Slf4j
@Configuration
@EnableConfigurationProperties({DisruptorProperties.class, FxProperties.class})
public class DisruptorConfig implements DisposableBean {

    private final DisruptorProperties props;
    private final FxRatePersistenceService persistenceService;
    private Disruptor<FxRateEvent> disruptor;

    public DisruptorConfig(DisruptorProperties props, FxRatePersistenceService persistenceService) {
        this.props = props;
        this.persistenceService = persistenceService;
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

        // 构建 N 个分片 handler；传给 handleEventsWith 的是"并行"订阅，每个都会看到全部 sequence，
        // 但 handler 内部按 sequence % N == myIndex 过滤——语义等价于 WorkerPool
        FxRatePersistenceEventHandler[] handlers = new FxRatePersistenceEventHandler[workerCount];
        for (int i = 0; i < workerCount; i++) {
            handlers[i] = new FxRatePersistenceEventHandler(i, workerCount, persistenceService);
        }
        disruptor.handleEventsWith(handlers);

        disruptor.start();
        log.info("[fx-disruptor] started. ringBufferSize={}, workerCount={}, waitStrategy={}",
                props.getRingBufferSize(), workerCount, props.getWaitStrategy());
        return disruptor.getRingBuffer();
    }

    /**
     * 容器关闭时优雅停机：等待 in-flight 事件消费完
     */
    @Override
    public void destroy() {
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

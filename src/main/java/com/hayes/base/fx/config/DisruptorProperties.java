package com.hayes.base.fx.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * app.disruptor.* 配置绑定
 */
@Data
@ConfigurationProperties(prefix = "app.disruptor")
public class DisruptorProperties {

    /** RingBuffer 容量（必须为 2 的幂） */
    private int ringBufferSize = 4096;

    /** 并行 EventHandler 数 */
    private int workerCount = 8;

    /** 等待策略：blocking | yielding | busy-spin | sleeping */
    private String waitStrategy = "blocking";

    /** 优雅停机超时（秒） */
    private int shutdownTimeoutSeconds = 5;
}

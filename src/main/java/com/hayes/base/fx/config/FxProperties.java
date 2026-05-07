package com.hayes.base.fx.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.Arrays;
import java.util.List;

/**
 * app.fx.* 配置绑定
 */
@Data
@ConfigurationProperties(prefix = "app.fx")
public class FxProperties {

    /** OP_CTE / OP_UTE 固定账号（Q-A） */
    private String opCode = "SYS_FX_RATE";

    /** 银行缺 DELI_TYP 的兜底值（Q-D） */
    private String defaultDeliTyp = "00";

    /** 启用 UPSERT 时间守卫防乱序覆盖（Q-B） */
    private boolean orderGuardEnabled = true;

    /** 失败重试次数（不含首次） */
    private int retryTimes = 3;

    /** 指数退避毫秒序列（逗号分隔），长度须 ≥ retryTimes */
    private String retryBackoffMs = "10,50,200";

    /** 全败后是否写 DLQ 表（Q-C） */
    private boolean dlqEnabled = true;

    /** 双通道异步批量落库配置 */
    private Flush flush = new Flush();

    /** 便捷解析退避序列，返回不可变 long 列表 */
    public List<Long> getRetryBackoffMsList() {
        return Arrays.stream(retryBackoffMs.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .toList();
    }

    /**
     * Flush 相关配置：双通道批量 flush 节奏与容量
     * <p>
     * - 报价通道（fx_xr_inf）：高频 flush（毫秒级），同 ccyPair 覆盖取最新
     * - 历史通道（fx_xr_his）：低频 flush（秒级），保留全量
     */
    @Data
    public static class Flush {

        /** 报价通道 flush 间隔（ms），值越小报价越实时但 DB 压力越大 */
        private int latestIntervalMs = 100;

        /** 报价通道单次批量上限，防止某次 drain 过大导致长事务 */
        private int latestBatchMaxSize = 500;

        /** 历史通道 flush 间隔（ms） */
        private int historyIntervalMs = 2000;

        /** 历史通道单次批量上限 */
        private int historyBatchMaxSize = 1000;

        /** 历史队列容量上限，满后 Handler 走兜底 */
        private int historyQueueCapacity = 100_000;

        /** 历史队列高水位阈值，超过后打告警 */
        private int historyQueueWarnThreshold = 50_000;
    }
}

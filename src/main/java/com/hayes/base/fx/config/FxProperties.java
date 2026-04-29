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

    /** 便捷解析退避序列，返回不可变 long 列表 */
    public List<Long> getRetryBackoffMsList() {
        return Arrays.stream(retryBackoffMs.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .map(Long::parseLong)
                .toList();
    }
}

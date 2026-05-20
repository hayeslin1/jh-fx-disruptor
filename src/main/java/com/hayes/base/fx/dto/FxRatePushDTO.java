package com.hayes.base.fx.dto;

import javax.validation.constraints.DecimalMin;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Pattern;
import javax.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 银行推送入参 DTO
 * <p>
 * 当前通道：HTTP POST /fx/push（单条）；
 * 字段与 Event/DB 对齐；JSR303 校验在 Controller 层 {@code @Valid} 触发，
 * 校验失败由全局异常处理器统一返回 400。
 */
@Data
public class FxRatePushDTO {

    /** 货币对，如 "USDCNY" */
    @NotBlank(message = "ccyPair 不能为空")
    @Size(max = 7)
    private String ccyPair;

    /** 通道编号 */
    @NotBlank(message = "channelCd 不能为空")
    @Size(max = 8)
    private String channelCd;

    /** 买入价（必须 > 0） */
    @NotNull(message = "buyPrice 不能为空")
    @DecimalMin(value = "0", inclusive = false, message = "buyPrice 必须大于 0")
    private BigDecimal buyPrice;

    /** 卖出价（必须 > 0） */
    @NotNull(message = "sellPrice 不能为空")
    @DecimalMin(value = "0", inclusive = false, message = "sellPrice 必须大于 0")
    private BigDecimal sellPrice;

    /** 彭博价（可空；有值时必须 > 0） */
    @DecimalMin(value = "0", inclusive = false, message = "blPrice 必须大于 0")
    private BigDecimal blPrice;

    /** 交割类型，可空；空则兜底为 FxConst.DEFAULT_DELI_TYP；有值必须为 2 位数字 */
    @NotBlank(message = "deliTyp 不能为空")
    @Pattern(regexp = "\\d{2}", message = "deliTyp 必须是 2 位数字")
    private String deliTyp;

    /** 渠道发布日期 YYYYMMDD */
    @NotBlank(message = "dtChannelPublish 不能为空")
    @Pattern(regexp = "\\d{8}", message = "dtChannelPublish 必须是 8 位数字 YYYYMMDD")
    private String dtChannelPublish;

    /** 渠道发布时间 HHmmss */
    @NotBlank(message = "tmChannelPublish 不能为空")
    @Pattern(regexp = "\\d{6}", message = "tmChannelPublish 必须是 6 位数字 HHmmss")
    private String tmChannelPublish;

    /**
     * UTC 时间戳，必填。固定 ISO-8601 UTC 格式 "yyyy-MM-ddTHH:mm:ss.SSSZ"（24 字符），
     * 例 "2026-05-20T16:37:33.188Z"。
     * <p>
     * 该字段是保序基准（DB orderGuard 与内存 coalesce 都依赖它做字符串字典序比较）。
     * 必须锁死毫秒精度——变长小数位会让字典序与时间序背离（".188Z" 字典序 > ".1880Z"，
     * 但二者语义相同），破坏保序判定。
     */
    @NotBlank(message = "utcTimes 不能为空")
    @Pattern(regexp = "\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}Z",
             message = "utcTimes 必须为 ISO-8601 UTC 格式 yyyy-MM-ddTHH:mm:ss.SSSZ")
    private String utcTimes;
}

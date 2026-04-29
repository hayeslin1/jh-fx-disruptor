package com.hayes.base.fx.dto;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Size;
import lombok.Data;

import java.math.BigDecimal;

/**
 * 银行推送入参 DTO
 * <p>
 * 当前通道：HTTP POST /fx/push（单条）；
 * 字段与 Event/DB 对齐；JSR303 校验在 Controller 层触发。
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

    /** 买入价 */
    @NotNull(message = "buyPrice 不能为空")
    private BigDecimal buyPrice;

    /** 卖出价 */
    @NotNull(message = "sellPrice 不能为空")
    private BigDecimal sellPrice;

    /** 彭博价（可空） */
    private BigDecimal blPrice;

    /** 交割类型，可空；空则兜底为 FxConst.DEFAULT_DELI_TYP */
    @Size(max = 2)
    private String deliTyp;

    /** 渠道发布日期 YYYYMMDD */
    @NotBlank(message = "dtChannelPublish 不能为空")
    @Size(min = 8, max = 8)
    private String dtChannelPublish;

    /** 渠道发布时间 HHmmss */
    @NotBlank(message = "tmChannelPublish 不能为空")
    @Size(min = 6, max = 6)
    private String tmChannelPublish;

    /** UTC 时间戳串（可空） */
    @Size(max = 32)
    private String utcTimes;
}

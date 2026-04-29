package com.hayes.base.fx.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * FX 汇率落库死信表 fx_xr_inf_dlq
 * <p>
 * 场景：一次 UPSERT+INSERT HIS 事务重试 N 次仍失败后，把原始事件落这张表人工兜底。
 * 建表 DDL 见 {@code docs/fx-rate-disruptor-plan.md} §4.5 或本次交付附带的 DDL。
 */
@Data
@TableName("fx_xr_inf_dlq")
public class FxXrInfDlq implements Serializable {

    /** 主键 */
    @TableId(type = IdType.AUTO)
    private Long id;

    /** 链路追踪 ID */
    private String traceId;

    /** 货币对 */
    private String ccyPair;

    /** 通道编号 */
    private String channelCd;

    /** 交割类型 */
    private String deliTyp;

    /** 买入价 */
    private BigDecimal buyPrice;

    /** 卖出价 */
    private BigDecimal sellPrice;

    /** 彭博价 */
    private BigDecimal blPrice;

    /** 渠道发布日期 */
    private String dtChannelPublish;

    /** 渠道发布时间 */
    private String tmChannelPublish;

    /** UTC 时间 */
    private String utcTimes;

    /** 失败原因（异常消息截断 1000 字符） */
    private String failReason;

    /** 记录创建日期 YYYYMMDD */
    private String dtCte;

    /** 记录创建时间 HHmmss */
    private String tmCte;
}

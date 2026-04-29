package com.hayes.base.fx.disruptor;

import lombok.Data;

import java.math.BigDecimal;

/**
 * Disruptor 事件对象
 * <p>
 * 设计要点：
 * 1. 可变 POJO，Disruptor 会在 RingBuffer 中复用同一实例；
 * 2. 字段与 {@code fx_xr_inf} / {@code fx_xr_inf_his} 对齐；
 * 3. 运行时字段（receiveNanos / traceId）仅用于监控与追踪，不入库。
 */
@Data
public class FxRateEvent {

    // ========== 业务字段（映射 DB） ==========

    /** 货币对，CCY_PAIR，如 "USDCNY" */
    private String ccyPair;

    /** 通道编号 CHANNEL_CD */
    private String channelCd;

    /** 买入价 BUY_PRICE */
    private BigDecimal buyPrice;

    /** 卖出价 SELL_PRICE */
    private BigDecimal sellPrice;

    /** 彭博价 BL_PRICE（可空） */
    private BigDecimal blPrice;

    /** 交割类型 DELI_TYP，默认 "00"(TOD) */
    private String deliTyp;

    /** 渠道发布日期 DT_CHANNEL_PUBLISH，格式 YYYYMMDD */
    private String dtChannelPublish;

    /** 渠道发布时间 TM_CHANNEL_PUBLISH，格式 HHmmss */
    private String tmChannelPublish;

    /** UTC 时间 UTC_TIMES */
    private String utcTimes;

    // ========== 运行时字段 ==========

    /** 进入 RingBuffer 的纳秒时间戳，用于端到端延迟统计 */
    private long receiveNanos;

    /** 链路追踪 ID */
    private String traceId;

    /**
     * 复用前清空，避免 RingBuffer 循环到旧数据污染下一次发布
     */
    public void clear() {
        this.ccyPair = null;
        this.channelCd = null;
        this.buyPrice = null;
        this.sellPrice = null;
        this.blPrice = null;
        this.deliTyp = null;
        this.dtChannelPublish = null;
        this.tmChannelPublish = null;
        this.utcTimes = null;
        this.receiveNanos = 0L;
        this.traceId = null;
    }
}

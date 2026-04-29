package com.hayes.base.fx.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * FX 汇率最新态表实体 fx_xr_inf
 * <p>
 * 唯一键：{@code uk_fx_xrinf_channelCcyPair (CHANNEL_CD, CCY_PAIR, DELI_TYP)}
 */
@Data
@TableName("fx_xr_inf")
public class FxXrInf implements Serializable {

    /** 主键 */
    @TableId(type = IdType.AUTO)
    private Integer id;

    /** 货币对 */
    private String ccyPair;

    /** 通道编号 */
    private String channelCd;

    /** 买入价 */
    private BigDecimal buyPrice;

    /** 卖出价 */
    private BigDecimal sellPrice;

    /** 彭博价，可空 */
    private BigDecimal blPrice;

    /** 创建人编号 */
    private String opCte;

    /** 创建日期 YYYYMMDD */
    private String dtCte;

    /** 创建时间 HHmmss */
    private String tmCte;

    /** 最后修改人编号 */
    private String opUte;

    /** 最后修改日期 YYYYMMDD */
    private String dtUte;

    /** 最后修改时间 HHmmss */
    private String tmUte;

    /** 渠道发布日期 YYYYMMDD */
    private String dtChannelPublish;

    /** 渠道发布时间 HHmmss */
    private String tmChannelPublish;

    /** 交割类型，默认 "00"(TOD) */
    private String deliTyp;

    /** UTC 时间 */
    private String utcTimes;
}

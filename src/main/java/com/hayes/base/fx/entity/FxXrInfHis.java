package com.hayes.base.fx.entity;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.io.Serializable;
import java.math.BigDecimal;

/**
 * FX 汇率历史流水表实体 fx_xr_inf_his
 * <p>
 * 主键 (ID, DT_CTE)，按 DT_CTE 做 RANGE 分区；
 * 注意 DT_CTE 为 int（YYYYMMDD 数值），与主表 varchar(8) 不一致。
 */
@Data
@TableName("fx_xr_inf_his")
public class FxXrInfHis implements Serializable {

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

    /** 彭博价 */
    private BigDecimal blPrice;

    /** 创建人编号 */
    private String opCte;

    /** 创建日期 YYYYMMDD（int） */
    private Integer dtCte;

    /** 创建时间 HHmmss */
    private String tmCte;

    /** 渠道发布日期 */
    private String dtChannelPublish;

    /** 渠道发布时间 */
    private String tmChannelPublish;

    /** 成功标识，01 成功 / 00 失败 */
    private String succFlg;

    /** 交割类型 */
    private String deliTyp;

    /** UTC 时间 */
    private String utcTimes;
}

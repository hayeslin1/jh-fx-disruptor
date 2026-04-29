package com.hayes.base.fx.common;

/**
 * FX 业务常量
 */
public final class FxConst {

    private FxConst() {
    }

    /** 默认交割类型：TOD */
    public static final String DEFAULT_DELI_TYP = "00";

    /** 历史表成功标识 */
    public static final String SUCC_FLG_SUCCESS = "01";

    /** 历史表失败标识（当前实时流默认不写 his 失败，仅 DLQ 使用） */
    public static final String SUCC_FLG_FAIL = "00";
}

package com.hayes.base.fx.common;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * 时间格式化工具
 * <p>
 * 主表 DT_* 为 varchar(8) YYYYMMDD，TM_* 为 varchar(6) HHmmss；
 * 历史表 DT_CTE 为 int（YYYYMMDD 数值）。
 */
public final class FxTimeUtils {

    private static final DateTimeFormatter DATE_FMT = DateTimeFormatter.ofPattern("yyyyMMdd");
    private static final DateTimeFormatter TIME_FMT = DateTimeFormatter.ofPattern("HHmmss");

    private FxTimeUtils() {
    }

    /** 当前日期字符串 YYYYMMDD */
    public static String today() {
        return LocalDateTime.now().format(DATE_FMT);
    }

    /** 当前日期数值 YYYYMMDD（用于 his 表 DT_CTE int 字段） */
    public static int todayInt() {
        return Integer.parseInt(today());
    }

    /** 当前时间字符串 HHmmss */
    public static String now() {
        return LocalDateTime.now().format(TIME_FMT);
    }
}

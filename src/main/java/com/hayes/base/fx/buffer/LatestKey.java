package com.hayes.base.fx.buffer;

import lombok.EqualsAndHashCode;
import lombok.Value;

/**
 * 报价最新态缓存 key：对应 fx_xr_inf 的唯一键 uk(CHANNEL_CD, CCY_PAIR, DELI_TYP)
 * <p>
 * 用作 {@code ConcurrentHashMap<LatestKey, FxRateEvent>} 的键，同一业务唯一键的高频推送会被
 * {@code merge} 覆盖成最新值，天然去掉中间值，极大降低落库 QPS。
 * <p>
 * 设计要点：
 * 1. {@code @Value} 等价于 {@code @Getter + @ToString + @EqualsAndHashCode + @AllArgsConstructor}，
 *    并自动把字段变成 {@code private final}、类变成 {@code final}——即不可变对象、天然线程安全；
 * 2. {@code cacheStrategy = LAZY} 让 Lombok 生成带缓存的 hashCode：首次计算后存入
 *    transient 字段，后续调用直接读——作为 ConcurrentHashMap 高频 key，避免每次重算；
 * 3. 字段不允许为空——DELI_TYP 缺失由上游 Translator 兜底（见 FxRateEventTranslator）。
 */
@Value
@EqualsAndHashCode(cacheStrategy = EqualsAndHashCode.CacheStrategy.LAZY)
public class LatestKey {

    /** 通道编号 */
    String channelCd;
    /** 货币对 */
    String ccyPair;
    /** 交割类型 */
    String deliTyp;
}

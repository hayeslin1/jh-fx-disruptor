package com.hayes.base.fx.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.entity.FxXrInf;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

/**
 * 主表 Mapper：INSERT...ON DUPLICATE KEY UPDATE
 */
@Mapper
public interface FxXrInfMapper extends BaseMapper<FxXrInf> {

    /**
     * UPSERT 最新态。
     * <p>
     * {@code orderGuard=true} 时 SQL 会用 {@code IF} 包裹每个被更新字段，
     * 仅当新推送的 {@code DT+TM_CHANNEL_PUBLISH} 不早于现存行时才覆盖（防并发乱序）。
     *
     * @param event      事件
     * @param opCode     OP_CTE/OP_UTE 账号常量
     * @param today      YYYYMMDD
     * @param now        HHmmss
     * @param orderGuard 是否启用时间守卫
     * @return 受影响行数（INSERT=1，UPDATE=2，无变更=0）
     */
    int upsert(@Param("e") FxRateEvent event,
               @Param("opCode") String opCode,
               @Param("today") String today,
               @Param("now") String now,
               @Param("orderGuard") boolean orderGuard);
}

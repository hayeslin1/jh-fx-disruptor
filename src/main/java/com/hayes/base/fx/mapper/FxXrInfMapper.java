package com.hayes.base.fx.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.entity.FxXrInf;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 主表 Mapper：INSERT...ON DUPLICATE KEY UPDATE
 */
@Mapper
public interface FxXrInfMapper extends BaseMapper<FxXrInf> {

    /**
     * UPSERT 最新态。
     * <p>
     * {@code orderGuard=true} 时 SQL 会用 {@code IF} 包裹每个被更新字段，
     * 仅当新推送的 {@code UTC_TIMES} 不早于现存行时才覆盖（防并发乱序）。
     * 与内存层 {@code FxRateLatestBuffer} 的保序字段保持一致。
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

    /**
     * 批量 UPSERT 最新态（多 VALUES 一条 SQL）。
     * <p>
     * 语义与 {@link #upsert} 一致，但通过一条 INSERT ... VALUES(...),(...),... 多行提交，
     * 节省网络 RTT 与事务 fsync 次数。orderGuard 分支同样生效——每行独立比较时间戳。
     *
     * @param list       事件列表（非空，调用方保证已做时间戳 coalesce）
     * @param opCode     创建/修改人
     * @param today      YYYYMMDD
     * @param now        HHmmss
     * @param orderGuard 是否启用时间守卫
     * @return 受影响行数
     */
    int upsertBatch(@Param("list") List<FxRateEvent> list,
                    @Param("opCode") String opCode,
                    @Param("today") String today,
                    @Param("now") String now,
                    @Param("orderGuard") boolean orderGuard);
}

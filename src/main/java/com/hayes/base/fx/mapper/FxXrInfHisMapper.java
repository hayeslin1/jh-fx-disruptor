package com.hayes.base.fx.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.entity.FxXrInfHis;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

/**
 * 历史表 Mapper：每条事件追加一行
 */
@Mapper
public interface FxXrInfHisMapper extends BaseMapper<FxXrInfHis> {

    /**
     * 单行插入
     *
     * @param event    事件
     * @param opCode   OP_CTE 账号
     * @param todayInt YYYYMMDD 的 int 值（与表字段类型匹配）
     * @param now      HHmmss
     * @param succFlg  成功标识 '01'/'00'
     * @return 受影响行数
     */
    int insertOne(@Param("e") FxRateEvent event,
                  @Param("opCode") String opCode,
                  @Param("todayInt") int todayInt,
                  @Param("now") String now,
                  @Param("succFlg") String succFlg);

    /**
     * 批量 INSERT 历史流水（多 VALUES 一条 SQL）。
     * <p>
     * 所有行共用同一 opCode/todayInt/now/succFlg（调用方在 flusher 一批内只取一次）。
     * 行级字段（ccyPair/channelCd/...）从 list 元素中逐行取。
     *
     * @param list     事件列表（非空）
     * @param opCode   创建人
     * @param todayInt YYYYMMDD 的 int 值
     * @param now      HHmmss
     * @param succFlg  成功标识
     * @return 受影响行数
     */
    int insertBatch(@Param("list") List<FxRateEvent> list,
                    @Param("opCode") String opCode,
                    @Param("todayInt") int todayInt,
                    @Param("now") String now,
                    @Param("succFlg") String succFlg);
}

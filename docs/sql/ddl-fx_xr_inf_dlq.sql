-- ==========================================================
-- FX 汇率落库死信表（DLQ）
-- 场景：Disruptor 消费时一次事务（UPSERT + INSERT HIS）重试 N 次仍失败，原始事件落此表人工兜底。
-- 对应配置：app.fx.dlq-enabled=true
-- ==========================================================
CREATE TABLE `fx_xr_inf_dlq` (
  `ID` bigint NOT NULL AUTO_INCREMENT COMMENT '主键',
  `TRACE_ID` varchar(64) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '链路追踪ID',
  `CCY_PAIR` varchar(7) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '货币对',
  `CHANNEL_CD` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '通道编号',
  `DELI_TYP` varchar(2) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '交割类型',
  `BUY_PRICE` decimal(20,10) DEFAULT NULL COMMENT '买入价',
  `SELL_PRICE` decimal(20,10) DEFAULT NULL COMMENT '卖出价',
  `BL_PRICE` decimal(20,10) DEFAULT NULL COMMENT '彭博价',
  `DT_CHANNEL_PUBLISH` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '渠道发布日期YYYYMMDD',
  `TM_CHANNEL_PUBLISH` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '渠道发布时间HHmmss',
  `UTC_TIMES` varchar(32) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT 'UTC时间',
  `FAIL_REASON` varchar(1000) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '失败原因（异常消息截断）',
  `DT_CTE` varchar(8) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '记录创建日期YYYYMMDD',
  `TM_CTE` varchar(6) CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci DEFAULT NULL COMMENT '记录创建时间HHmmss',
  PRIMARY KEY (`ID`),
  KEY `idx_dlq_trace` (`TRACE_ID`),
  KEY `idx_dlq_dtCte` (`DT_CTE`),
  KEY `idx_dlq_channel_ccy` (`CHANNEL_CD`,`CCY_PAIR`,`DELI_TYP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci ROW_FORMAT=DYNAMIC COMMENT='FX汇率落库死信表';

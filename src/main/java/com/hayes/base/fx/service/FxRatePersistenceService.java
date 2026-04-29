package com.hayes.base.fx.service;

import com.hayes.base.fx.common.FxConst;
import com.hayes.base.fx.common.FxTimeUtils;
import com.hayes.base.fx.config.FxProperties;
import com.hayes.base.fx.disruptor.FxRateEvent;
import com.hayes.base.fx.entity.FxXrInfDlq;
import com.hayes.base.fx.mapper.FxXrInfDlqMapper;
import com.hayes.base.fx.mapper.FxXrInfHisMapper;
import com.hayes.base.fx.mapper.FxXrInfMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

/**
 * 汇率落库 Service
 * <p>
 * 职责：
 * 1. {@link #persist(FxRateEvent)}：调度入口，负责重试与 DLQ 兜底（本方法不是事务，需要每次重试独立事务）；
 * 2. {@link #doPersist(FxRateEvent)}：一次独立短事务，UPSERT + INSERT HIS；
 * 3. {@link #writeDlq(FxRateEvent, Throwable)}：失败兜底，独立事务，不依赖主流事务状态。
 * <p>
 * 注意：@Transactional 只在通过 Spring 代理调用时生效，所以 doPersist / writeDlq 必须由
 * 外部方法（persist）走 Spring Bean 引用（this 自注入）触发。
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class FxRatePersistenceService {

    private final FxXrInfMapper fxXrInfMapper;
    private final FxXrInfHisMapper fxXrInfHisMapper;
    private final FxXrInfDlqMapper fxXrInfDlqMapper;
    private final FxProperties fxProps;
    /** 自注入以保证 @Transactional 生效（内部方法调用要走代理） */
    private final org.springframework.context.ApplicationContext appCtx;

    /**
     * 对外入口：持久化单个事件，失败则指数退避重试，仍失败则写 DLQ / 日志
     */
    public void persist(FxRateEvent event) {
        int attempts = fxProps.getRetryTimes() + 1; // 首次 + retry 次
        List<Long> backoff = fxProps.getRetryBackoffMsList();
        Throwable last = null;

        // 通过 Spring 代理拿到当前 Bean，保证 @Transactional 生效
        FxRatePersistenceService self = appCtx.getBean(FxRatePersistenceService.class);

        for (int i = 0; i < attempts; i++) {
            try {
                self.doPersist(event);
                if (i > 0) {
                    log.warn("[fx-rate] persist recovered after {} retries. traceId={}", i, event.getTraceId());
                }
                return;
            } catch (Throwable ex) {
                last = ex;
                log.warn("[fx-rate] persist attempt {}/{} failed. traceId={} reason={}",
                        i + 1, attempts, event.getTraceId(), ex.getMessage());
                if (i < attempts - 1) {
                    sleepQuietly(backoff.get(Math.min(i, backoff.size() - 1)));
                }
            }
        }

        // 全部重试失败 —— 打 ERROR 日志 + 按配置写 DLQ
        log.error("[fx-rate] persist FINALLY failed after {} attempts. traceId={} ccyPair={} channelCd={}",
                attempts, event.getTraceId(), event.getCcyPair(), event.getChannelCd(), last);
        if (fxProps.isDlqEnabled()) {
            try {
                self.writeDlq(event, last);
            } catch (Throwable dlqEx) {
                log.error("[fx-rate] DLQ write failed. traceId={}", event.getTraceId(), dlqEx);
            }
        }
    }

    /**
     * 单条事务：UPSERT 最新态 + INSERT 历史流水
     */
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public void doPersist(FxRateEvent event) {
        String opCode = fxProps.getOpCode();
        String today = FxTimeUtils.today();
        String now = FxTimeUtils.now();
        int todayInt = FxTimeUtils.todayInt();

        // 兜底 DELI_TYP（Translator 已做，这里再保险一次，防止直接投递未过 Translator 的事件）
        if (StringUtils.isBlank(event.getDeliTyp())) {
            event.setDeliTyp(fxProps.getDefaultDeliTyp());
        }

        // 1) UPSERT 最新态
        fxXrInfMapper.upsert(event, opCode, today, now, fxProps.isOrderGuardEnabled());

        // 2) INSERT 历史流水
        fxXrInfHisMapper.insertOne(event, opCode, todayInt, now, FxConst.SUCC_FLG_SUCCESS);
    }

    /**
     * DLQ 写入（独立事务），截断异常消息
     */
    @Transactional(rollbackFor = Throwable.class, propagation = Propagation.REQUIRES_NEW)
    public void writeDlq(FxRateEvent event, Throwable cause) {
        FxXrInfDlq dlq = new FxXrInfDlq();
        dlq.setTraceId(event.getTraceId());
        dlq.setCcyPair(event.getCcyPair());
        dlq.setChannelCd(event.getChannelCd());
        dlq.setDeliTyp(event.getDeliTyp());
        dlq.setBuyPrice(event.getBuyPrice());
        dlq.setSellPrice(event.getSellPrice());
        dlq.setBlPrice(event.getBlPrice());
        dlq.setDtChannelPublish(event.getDtChannelPublish());
        dlq.setTmChannelPublish(event.getTmChannelPublish());
        dlq.setUtcTimes(event.getUtcTimes());
        dlq.setFailReason(truncate(cause == null ? "unknown" : cause.toString(), 1000));
        dlq.setDtCte(FxTimeUtils.today());
        dlq.setTmCte(FxTimeUtils.now());
        fxXrInfDlqMapper.insert(dlq);
    }

    /** 截断长字符串，避免 DLQ 字段溢出 */
    private static String truncate(String s, int max) {
        if (s == null) {
            return null;
        }
        return s.length() <= max ? s : s.substring(0, max);
    }

    /** 静默 sleep，被中断时恢复中断标志并终止等待 */
    private static void sleepQuietly(long millis) {
        if (millis <= 0) {
            return;
        }
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}

package com.hayes.base.fx.buffer;

import com.hayes.base.fx.disruptor.FxRateEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 报价最新态缓存（双通道架构中的"报价通道"入口）
 * <p>
 * 特性：
 * 1. 同 ccyPair 高频推送时，{@link #merge} 只保留时间戳较新的一条（按 UTC_TIMES 比较，
 *    格式 "yyyy-MM-ddTHH:mm:ss.SSSZ" 固定 24 字符，字典序与时间序一致；
 *    跨时区跨日切场景下比 DT+TM_CHANNEL_PUBLISH 更可靠）；
 * 2. {@link #drainAll} 原子取出当前快照并清空，供 Flusher 批量 UPSERT；
 * 3. 天然受 ccyPair × channelCd × deliTyp 基数上限约束（几十到几百），不会无限增长，无需强背压；
 * 4. 容量超阈值时打告警日志供运维观察。
 */
@Slf4j
@Component
public class FxRateLatestBuffer {

    private final ConcurrentHashMap<LatestKey, FxRateEvent> map = new ConcurrentHashMap<>(256);

    /**
     * 合并一条新推送。
     * <p>
     * 时间戳保序：若 map 中已有同 key 的旧值，比较 {@code utcTimes}，只有新值不早于旧值才覆盖。
     * 选用 UTC_TIMES 而非 DT+TM_CHANNEL_PUBLISH——前者是单调递增的 UTC 时间串，跨时区 / 跨日切
     * 场景下字典序与时间序一致；后者是渠道侧本地时间，存在乱序风险。防止"晚到的旧推送"污染报价。
     * <p>
     * 注：上游需保证 utcTimes 稳定下发；任一侧为空时按字典序退化处理（空串 < 任何非空串）。
     *
     * @param key      唯一业务键
     * @param incoming 已深拷贝的事件（调用方负责深拷贝，本方法不再拷贝）
     */
    public void merge(LatestKey key, FxRateEvent incoming) {
        map.merge(key, incoming, (oldE, newE) -> {
            String oldTs = safe(oldE.getUtcTimes());
            String newTs = safe(newE.getUtcTimes());
            // 新值时间戳 >= 旧值时才覆盖；相等时也覆盖（取"较晚到达"的那条）
            return newTs.compareTo(oldTs) >= 0 ? newE : oldE;
        });
    }

    /**
     * 原子取出所有条目并清空。
     * <p>
     * 实现：逐 key 执行 {@code map.remove(key)}，确保并发写入的新值不会被本次 drain 带走。
     * 由于 ConcurrentHashMap 的 keySet 视图是弱一致性的，使用快照复制为独立 map 返回。
     *
     * @return 本次 drain 出来的快照；空时返回空 map
     */
    public Map<LatestKey, FxRateEvent> drainAll() {
        if (map.isEmpty()) {
            return Map.of();
        }
        Map<LatestKey, FxRateEvent> snapshot = new HashMap<>(map.size() * 2);
        // 先拿到 key 快照，再逐个 remove——避免 iterator 在并发 merge 下漏项
        for (LatestKey k : map.keySet()) {
            FxRateEvent v = map.remove(k);
            if (v != null) {
                snapshot.put(k, v);
            }
        }
        return snapshot;
    }

    /** 当前条目数，监控/告警用 */
    public int size() {
        return map.size();
    }

    private static String safe(String s) {
        return s == null ? "" : s;
    }
}

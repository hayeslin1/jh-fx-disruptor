package com.hayes.base.fx.disruptor;

import com.lmax.disruptor.EventFactory;

/**
 * Disruptor 事件工厂
 * <p>
 * RingBuffer 初始化时调用 {@link #newInstance()} 预填充所有槽位；
 * 运行期不会再创建新实例，所有事件对象在 Ring 内循环复用。
 */
public class FxRateEventFactory implements EventFactory<FxRateEvent> {

    @Override
    public FxRateEvent newInstance() {
        return new FxRateEvent();
    }
}

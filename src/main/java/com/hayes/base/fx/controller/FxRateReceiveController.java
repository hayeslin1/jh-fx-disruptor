package com.hayes.base.fx.controller;

import com.hayes.base.fx.dto.FxRatePushDTO;
import com.hayes.base.fx.service.FxRateService;
import javax.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * 汇率推送接收 Controller（HTTP）
 * <p>
 * 当前暴露：POST /fx/push —— 单条接收；
 * 后续若上游切换为 Kafka/MQ，只需新增一个 Consumer 类调用 {@link FxRateService#receive}，
 * 本 Controller 无需改动。
 */
@Slf4j
@RestController
@RequestMapping("/fx")
@RequiredArgsConstructor
public class FxRateReceiveController {

    private final FxRateService fxRateService;

    /**
     * 单条推送
     */
    @PostMapping("/push")
    public Map<String, Object> push(@RequestBody @Valid FxRatePushDTO dto) {
        String traceId = fxRateService.receive(dto);
        // 返回极简 ACK，避免阻塞银行线程
        Map<String, Object> resp = new HashMap<>(3);
        resp.put("code", "0");
        resp.put("msg", "ok");
        resp.put("traceId", traceId);
        return resp;
    }
}

# hayes-fx-disruptor · 外部对接接口文档

> 版本：2026-05-07 · v1
> 适用服务：`hayes-fx-disruptor`（应用名 `spring.application.name=hayes-fx-disruptor`）
> 定位：银行侧实时汇率推送接入端，承接峰值 ≈ 1000 QPS，单条接收后异步批量落库。
> 代码锚点：`FxRateReceiveController.java:34` / `FxRatePushDTO.java:17` / `FxRateService.java:29`

---

## 1. 接入信息


| 项目         | 值                                                                                               |
| ------------ | ------------------------------------------------------------------------------------------------ |
| 协议         | HTTP/1.1                                                                                         |
| 传输编码     | `application/json; charset=UTF-8`                                                                |
| 服务端口     | `8080`（`application.yml:17`）                                                                   |
| Context-Path | `/`（`application.yml:20`）                                                                      |
| 鉴权         | 无（v1 内网直连；如需鉴权由网关层承担）                                                          |
| 字符集       | UTF-8                                                                                            |
| 时区         | 银行侧以`dtChannelPublish` + `tmChannelPublish` 为准，系统落库记录带 UTC 时间戳（见 `utcTimes`） |

Base URL 示例：`http://{host}:8080`

---

## 2. 接口清单


| 编号       | 名称         | 方法 | 路径       | 说明                     |
| ---------- | ------------ | ---- | ---------- | ------------------------ |
| FX-PUSH-01 | 单条汇率推送 | POST | `/fx/push` | 银行侧推送一条货币对汇率 |

> 当前仅暴露单条推送接口；如后续上游切换为 Kafka/MQ，本接口路径与语义保持不变。

---

## 3. FX-PUSH-01 · 单条汇率推送

### 3.1 接口说明

- **路径**：`POST /fx/push`
- **语义**：银行侧每产生一次新汇率（或更新），即调用一次本接口推送。
- **处理模型**：
  1. Controller 入参 JSR303 校验（失败 → HTTP 400）；
  2. Service 生成 16 位 `traceId`；
  3. 投递至 LMAX Disruptor RingBuffer（非阻塞）；
  4. 立即返回 ACK（避免阻塞银行线程）；
  5. 消费端异步双通道批量落库（主表 UPSERT + 历史表 INSERT）。
- **返回延迟目标**：P99 < 10ms（仅入 RingBuffer 的耗时）。

### 3.2 请求头


| Header         | 必填 | 值                                | 说明         |
| -------------- | ---- | --------------------------------- | ------------ |
| `Content-Type` | 是   | `application/json; charset=UTF-8` | 必须为 JSON  |
| `Accept`       | 否   | `application/json`                | 建议显式声明 |

### 3.3 请求体字段


| 字段               | 类型   | 必填 | 长度/格式             | 说明                                               |
| ------------------ | ------ | ---- | --------------------- |--------------------------------------------------|
| `ccyPair`          | string | 是   | 最大 7                | 货币对，如`USD/CNY`                                   |
| `channelCd`        | string | 是   | 最大 8                | 通道编号                                             |
| `buyPrice`         | number | 是   | BigDecimal            | 买入价（保留原始精度）                                      |
| `sellPrice`        | number | 是   | BigDecimal            | 卖出价（保留原始精度）                                      |
| `blPrice`          | number | 否   | BigDecimal            | 彭博价，可为空                                          |
| `deliTyp`          | string | 否   | 最大 2                | 交割类型(00,01,02)，为空时服务端兜底为`00`                     |
| `dtChannelPublish` | string | 是   | 固定 8 位，`yyyyMMdd` | 渠道发布日期 :20260507                                 |
| `tmChannelPublish` | string | 是   | 固定 6 位，`HHmmss`   | 渠道发布时间 :121212                                   |
| `utcTimes`         | string | 否   | 最大 32               | UTC 时间戳串（银行原样透传):为空默认：System.currentTimeMillis() |


> 校验由 JSR303 注解强制，详见 `FxRatePushDTO.java:17-57`。

### 3.4 请求示例

```http
POST /fx/push HTTP/1.1
Host: fx-api.internal:8080
Content-Type: application/json; charset=UTF-8

{
  "ccyPair": "USDCNY",
  "channelCd": "BOC01",
  "buyPrice": 7.1234,
  "sellPrice": 7.1256,
  "blPrice": 7.1245,
  "deliTyp": "00",
  "dtChannelPublish": "20260507",
  "tmChannelPublish": "093015",
  "utcTimes": "2026-05-07T01:30:15.123Z"
}
```
### 3.5 响应体字段


| 字段      | 类型   | 说明                                                                    |
| --------- | ------ | ----------------------------------------------------------------------- |
| `code`    | string | 业务码，`"0"` 表示已受理（ACK）                                         |
| `msg`     | string | 业务消息，成功固定`"ok"`                                                |
| `traceId` | string | 16 位去连字符 UUID，贯穿 RingBuffer → Handler → DB 日志，便于对账排查 |

### 3.6 响应示例

**成功（HTTP 200）：**

```json
{
  "code": "0",
  "msg": "ok",
  "traceId": "a1b2c3d4e5f60718"
}
```
**参数校验失败（HTTP 400，Spring 默认异常体）：**

```json
{
  "timestamp": "2026-05-07T01:30:15.123+00:00",
  "status": 400,
  "error": "Bad Request",
  "message": "ccyPair 不能为空",
  "path": "/fx/push"
}
```
**服务端异常（HTTP 500）：**

```json
{
  "timestamp": "2026-05-07T01:30:15.123+00:00",
  "status": 500,
  "error": "Internal Server Error",
  "path": "/fx/push"
}
```
### 3.7 错误码与处置建议


| HTTP | 触发条件                             | 银行侧建议                                |
| ---- | ------------------------------------ | ----------------------------------------- |
| 200  | 业务`code=0`                         | 推送成功，已入 RingBuffer                 |
| 400  | JSON 解析失败 / JSR303 校验失败      | 检查字段名与约束，不重试（重试依旧失败）  |
| 415  | `Content-Type` 不是 JSON             | 改为`application/json`                    |
| 500  | 服务端异常（含 RingBuffer 投递失败） | 指数退避重试（建议初始 100ms，最多 3 次） |
| 503  | 服务未就绪 / 优雅停机中              | 指数退避重试并切到备用节点                |

> 接收层 ACK 即"已入环"，后续落库若失败会被重试，终极失败进 DLQ 表，**银行侧无需关心**。

### 3.8 重试与幂等

- **ACK 之后银行侧不需重发**：服务端负责落库重试 + DLQ 兜底。
- **ACK 之前超时/失败可重发**：服务端按 `ccyPair + channelCd + dtChannelPublish + tmChannelPublish` 组成业务键，主表 UPSERT 带时间守卫（`order-guard-enabled: true`，`application.yml:59`），不会出现旧值覆盖新值。
- **重试策略建议**：指数退避 `100ms / 400ms / 1600ms`，最多 3 次。

### 3.9 限流与容量


| 项目             | 值                                            |
| ---------------- | --------------------------------------------- |
| 设计峰值 QPS     | 1000                                          |
| RingBuffer 容量  | 4096（`application.yml:46`，≈ 4 秒峰值缓冲） |
| 并行消费 Handler | 8（`application.yml:48`）                     |
| 优雅停机等待     | 5s（`application.yml:52`）                    |

若银行侧预期 QPS 持续 > 1000，提前联系服务方扩容。

---

## 4. 调用示例

### 4.1 curl

```bash
curl -X POST "http://fx-api.internal:8080/fx/push" \
  -H "Content-Type: application/json; charset=UTF-8" \
  -d '{
    "ccyPair": "USDCNY",
    "channelCd": "BOC01",
    "buyPrice": 7.1234,
    "sellPrice": 7.1256,
    "dtChannelPublish": "20260507",
    "tmChannelPublish": "093015"
  }'
```
### 4.2 Java（OkHttp 4.12.0）

```java
// 组装请求体
String body = "{"
        + "\"ccyPair\":\"USDCNY\","
        + "\"channelCd\":\"BOC01\","
        + "\"buyPrice\":7.1234,"
        + "\"sellPrice\":7.1256,"
        + "\"dtChannelPublish\":\"20260507\","
        + "\"tmChannelPublish\":\"093015\""
        + "}";

Request req = new Request.Builder()
        .url("http://fx-api.internal:8080/fx/push")
        .post(RequestBody.create(body, MediaType.parse("application/json; charset=UTF-8")))
        .build();

try (Response resp = client.newCall(req).execute()) {
    if (!resp.isSuccessful()) {
        // 4xx 不重试，5xx 指数退避重试
        throw new IllegalStateException("fx push failed: " + resp.code());
    }
    String json = resp.body().string();
    // 解析 code/msg/traceId
}
```
### 4.3 Postman

- Method：`POST`
- URL：`http://fx-api.internal:8080/fx/push`
- Body：`raw` → `JSON`，粘贴 §3.4 示例

---

## 5. 对账与排错


| 场景                           | 排错入口                                                                                                 |
| ------------------------------ | -------------------------------------------------------------------------------------------------------- |
| 银行拿到`traceId` 但怀疑未落库 | 服务端日志`grep "{traceId}"`；主表 `fx_xr_inf` 查 `ccyPair + channelCd`；历史表 `fx_xr_inf_his` 查时间段 |
| 银行收到 4xx                   | 对照 §3.3 字段约束排查请求体                                                                            |
| 银行收到 5xx                   | 联系服务方，提供`traceId`（若有响应体）或发生时间段                                                      |
| 落库丢失                       | 查 DLQ 表`fx_xr_inf_dlq`，按业务键定位                                                                   |

---

## 6. 变更历史


| 版本 | 日期       | 变更                      |
| ---- | ---------- | ------------------------- |
| v1   | 2026-05-07 | 初版：单条`POST /fx/push` |

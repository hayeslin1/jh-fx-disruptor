# hayes-fx-disruptor

基于 **LMAX Disruptor 4.0.0** 的实时外汇汇率接收 / 落库微服务。承接上游（银行 / 报价渠道）高频推送，单条事件**实时**进库，目标 ≈ **1000 QPS**（100 货币对 × 10 渠道）。

> HTTP 接入 → Disruptor 内存 RingBuffer → 8 线程并行消费 → 主表 UPSERT + 历史表 INSERT（单事务） → 失败重试 + DLQ 兜底。

---

## 目录

- [特性](#特性)
- [技术栈](#技术栈)
- [架构](#架构)
- [快速开始](#快速开始)
- [HTTP API](#http-api)
- [数据模型](#数据模型)
- [关键配置](#关键配置)
- [压测](#压测)
- [目录结构](#目录结构)
- [FAQ](#faq)
- [License](#license)

---

## 特性

- **实时单条落库**：生产 → RingBuffer → 消费 → SQL，端到端延迟毫秒级，主表随时可查最新态。
- **UPSERT + 时间守卫**：`INSERT ... ON DUPLICATE KEY UPDATE` 基于 `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` 唯一键；可选"渠道发布时间"比较，防止并发乱序覆盖新值。
- **指数退避重试 + DLQ**：`@Transactional(REQUIRES_NEW)` + 3 次 `[10, 50, 200]ms` 退避；全败写死信表 + ERROR 日志。
- **优雅停机**：容器关闭前 `disruptor.shutdown(timeout)` 等待 in-flight 事件消费完毕。
- **生产侧背压**：`BlockingWaitStrategy` 让 RingBuffer 满时生产者线程阻塞，自动把流控压回 HTTP / MQ。
- **分片并行消费**：8 个 `EventHandler` 共享 RingBuffer，基于 `sequence % workerCount` modulo 过滤，序列号相同的事件走同一个 worker（保证同一货币对的时序在单线程内有序）。

## 技术栈


| 组件           | 版本                                   |
| -------------- | -------------------------------------- |
| JDK            | 17                                     |
| Spring Boot    | 2.7.18（`spring-boot-starter-parent`） |
| LMAX Disruptor | 4.0.0                                  |
| MyBatis-Plus   | 3.5.2                                  |
| MySQL 驱动     | 8.0.33                                 |
| Lombok         | 1.18.26                                |
| Fastjson2      | 2.0.61                                 |
| EasyExcel      | 3.1.3                                  |

## 架构

```
[银行/渠道]
    │  HTTP POST /fx/push
    ▼
Controller → Service → Producer ──publishEvent──▶ ┌──────────────────┐
                                                   │  RingBuffer 4096  │ ← 多生产者 MULTI
                                                   └────────┬─────────┘
                                                            │ (BlockingWaitStrategy)
                                          ┌─────────┬───────┼───────┬─────────┐
                                    fx-disruptor-1      ...       fx-disruptor-8
                                          │                                   │
                                          ▼                                   ▼
                           FxRatePersistenceEventHandler (modulo 分片)
                                          │
                                          ▼
                           FxRatePersistenceService
                              │
                              ├─ doPersist() @Transactional(REQUIRES_NEW)
                              │     ├─ UPSERT fx_xr_inf
                              │     └─ INSERT fx_xr_inf_his
                              │
                              └─ [重试 3 次仍失败]
                                 writeDlq() @Transactional(REQUIRES_NEW)
                                     └─ INSERT fx_xr_inf_dlq
```

详细逐行解析见 [`docs/fx-push-flow-analysis_20260427.md`](docs/fx-push-flow-analysis_20260427.md)。
设计 Spec 见 [`docs/fx-rate-disruptor-plan.md`](docs/fx-rate-disruptor-plan.md)。

## 快速开始

### 1. 前置

- JDK 17+
- Maven 3.8+
- MySQL 8.x（`utf8mb4`）

### 2. 建表

```sql
-- 主表（最新态，UPSERT 目标）
CREATE TABLE `fx_xr_inf` (
  `ID` bigint NOT NULL AUTO_INCREMENT,
  `CCY_PAIR` varchar(7) NOT NULL COMMENT '货币对',
  `CHANNEL_CD` varchar(8) NOT NULL COMMENT '通道编号',
  `DELI_TYP` varchar(2) NOT NULL DEFAULT '00' COMMENT '交割类型 TOD=00/TOM=01/SPT=02',
  `BUY_PRICE` decimal(20,10) DEFAULT NULL,
  `SELL_PRICE` decimal(20,10) DEFAULT NULL,
  `BL_PRICE` decimal(20,10) DEFAULT NULL,
  `OP_CTE` varchar(32) NOT NULL,
  `DT_CTE` varchar(8) NOT NULL COMMENT 'YYYYMMDD',
  `TM_CTE` varchar(6) NOT NULL COMMENT 'HHmmss',
  `OP_UTE` varchar(32) NOT NULL,
  `DT_UTE` varchar(8) NOT NULL,
  `TM_UTE` varchar(6) NOT NULL,
  `DT_CHANNEL_PUBLISH` varchar(8) DEFAULT NULL,
  `TM_CHANNEL_PUBLISH` varchar(6) DEFAULT NULL,
  `UTC_TIMES` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  UNIQUE KEY `uk_fx_xrinf_channelCcyPair` (`CHANNEL_CD`,`CCY_PAIR`,`DELI_TYP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 历史流水表（INSERT 单行，`DT_CTE` 为 int，便于做日期分区）
CREATE TABLE `fx_xr_inf_his` (
  `ID` bigint NOT NULL AUTO_INCREMENT,
  `CCY_PAIR` varchar(7) NOT NULL,
  `CHANNEL_CD` varchar(8) NOT NULL,
  `DELI_TYP` varchar(2) DEFAULT NULL,
  `BUY_PRICE` decimal(20,10) DEFAULT NULL,
  `SELL_PRICE` decimal(20,10) DEFAULT NULL,
  `BL_PRICE` decimal(20,10) DEFAULT NULL,
  `OP_CTE` varchar(32) NOT NULL,
  `DT_CTE` int NOT NULL COMMENT 'YYYYMMDD 整数，分区键',
  `TM_CTE` varchar(6) NOT NULL,
  `DT_CHANNEL_PUBLISH` varchar(8) DEFAULT NULL,
  `TM_CHANNEL_PUBLISH` varchar(6) DEFAULT NULL,
  `SUCC_FLG` varchar(2) DEFAULT NULL,
  `UTC_TIMES` varchar(32) DEFAULT NULL,
  PRIMARY KEY (`ID`),
  KEY `idx_his_dtCte` (`DT_CTE`),
  KEY `idx_his_channel_ccy` (`CHANNEL_CD`,`CCY_PAIR`,`DELI_TYP`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- 死信表（见 docs/sql/ddl-fx_xr_inf_dlq.sql）
source docs/sql/ddl-fx_xr_inf_dlq.sql;
```

> 主表与历史表的 DDL 未随仓库一并提供（属于上游业务约束），以上为**最小可跑版本**，可按需扩展字段。

### 3. 配置

```bash
cp src/main/resources/application-example.yml src/main/resources/application-dev.yml
# 编辑 application-dev.yml 填入真实 MySQL 连接信息
```

真实 `application-{dev,prod,rc,test}.yml` 已被 `.gitignore`，不会进仓库。

### 4. 运行

```bash
mvn -Denv=dev clean package
java -jar target/hayes-fx-disruptor-dev.jar
# 或 IDE 直接跑 com.hayes.base.fx.Application
```

启动后默认监听 `http://localhost:8080`。

### 5. 冒烟

```bash
curl -X POST http://localhost:8080/fx/push \
  -H 'Content-Type: application/json' \
  -d '{
    "ccyPair": "USDCNY",
    "channelCd": "BOC",
    "buyPrice": 7.1234,
    "sellPrice": 7.1256,
    "blPrice": 7.1245,
    "deliTyp": "00",
    "dtChannelPublish": "20260427",
    "tmChannelPublish": "120000"
  }'
```

期望响应：`{"code":"0","msg":"ok","traceId":"..."}`；DB 主表 / 历史表各落 1 行。

## HTTP API

### `POST /fx/push`


| 字段               | 类型    | 必填 | 约束  | 说明                     |
| ------------------ | ------- | ---- | ----- | ------------------------ |
| `ccyPair`          | string  | Y    | ≤ 7  | 货币对，如`USD/CNY`      |
| `channelCd`        | string  | Y    | ≤ 8  | 通道编号                 |
| `buyPrice`         | decimal | Y    | —    | 买入价                   |
| `sellPrice`        | decimal | Y    | —    | 卖出价                   |
| `blPrice`          | decimal | N    | —    | 彭博参考价               |
| `deliTyp`          | string  | N    | ≤ 2  | 交割类型，空则兜底`"00"` |
| `dtChannelPublish` | string  | Y    | 8 位  | `YYYYMMDD`               |
| `tmChannelPublish` | string  | Y    | 6 位  | `HHmmss`                 |
| `utcTimes`         | string  | N    | ≤ 32 | UTC 时间戳串             |

响应：

```json
{ "code": "0", "msg": "ok", "traceId": "<uuid>" }
```

注意：**HTTP 202 语义**—— Controller 收到即应答，实际落库由消费线程异步完成。若 RingBuffer 满，生产者线程会阻塞（背压），HTTP 响应也会随之变慢。

## 数据模型


| 表              | 作用         | 写入语义                                          | 唯一键 / 索引                                                   |
| --------------- | ------------ | ------------------------------------------------- | --------------------------------------------------------------- |
| `fx_xr_inf`     | 最新态       | UPSERT（`ON DUPLICATE KEY UPDATE`，可选时间守卫） | UK`(CHANNEL_CD, CCY_PAIR, DELI_TYP)`                            |
| `fx_xr_inf_his` | 全量历史流水 | INSERT（单行）                                    | IDX`(DT_CTE)`、`(CHANNEL_CD, CCY_PAIR, DELI_TYP)`               |
| `fx_xr_inf_dlq` | 死信         | INSERT（独立事务）                                | IDX`(TRACE_ID)`、`(DT_CTE)`、`(CHANNEL_CD, CCY_PAIR, DELI_TYP)` |

**时间守卫**（`orderGuard=true`）：UPSERT 的 UPDATE 分支在每个字段上包一层

```
IF( CONCAT(VALUES(DT_PUB), VALUES(TM_PUB)) >= CONCAT(DT_PUB, TM_PUB),
    VALUES(X), X )
```

字段定长（`YYYYMMDD` + `HHmmss`）保证字典序 == 时间序，防止并发 worker 导致旧覆盖新。

## 关键配置

`application.yml`：

```yaml
app:
  disruptor:
    ring-buffer-size: 4096        # 必须 2 的幂
    worker-count: 8               # 并行 EventHandler 数
    wait-strategy: blocking       # blocking | yielding | busy-spin | sleeping
    shutdown-timeout-seconds: 5   # 优雅停机等待
  fx:
    op-code: SYS_FX_RATE          # 主表 OP_CTE / OP_UTE 固定账号
    default-deli-typ: "00"        # DELI_TYP 缺省兜底
    order-guard-enabled: true     # 启用时间守卫
    retry-times: 3                # 失败重试次数（不含首次）
    retry-backoff-ms: 10,50,200   # 指数退避序列
    dlq-enabled: true             # 全败写 DLQ 表
```

## 压测

内置压测入口 `com.hayes.base.fx.FxRatePushTest#pushTest`：

```bash
# 默认 100 QPS × 60 秒
mvn -Denv=dev test -Dtest=FxRatePushTest#pushTest

# 自定义（先启动 Application）
mvn -Denv=dev test -Dtest=FxRatePushTest#pushTest \
  -Dpush.url=http://localhost:8080/fx/push \
  -Dpush.qps=1000 \
  -Dpush.duration=60 \
  -Dpush.workers=32
```

参数：


| 参数            | 默认                            | 说明            |
| --------------- | ------------------------------- | --------------- |
| `push.url`      | `http://localhost:8080/fx/push` | 目标地址        |
| `push.qps`      | 100                             | 每秒条数        |
| `push.duration` | 60                              | 持续秒数        |
| `push.workers`  | 16                              | HTTP 工作线程数 |

## 目录结构

```
src/main/java/com/hayes/base/fx/
├── Application.java                          入口
├── common/
│   ├── FxConst.java                          常量（DELI_TYP、SUCC_FLG）
│   └── FxTimeUtils.java                      today / now / todayInt
├── config/
│   ├── DisruptorProperties.java              app.disruptor.*
│   ├── FxProperties.java                     app.fx.*
│   └── DisruptorConfig.java                  Disruptor Bean + 8 handler + 优雅关闭
├── disruptor/
│   ├── FxRateEvent.java                      事件 POJO
│   ├── FxRateEventFactory.java               RingBuffer 预填充
│   ├── FxRateEventTranslator.java            DTO → Event
│   ├── FxRateEventProducer.java              publishEvent
│   └── handler/FxRatePersistenceEventHandler.java   modulo 分片消费
├── controller/
│   └── FxRateReceiveController.java          POST /fx/push
├── service/
│   ├── FxRateService.java                    traceId + 投递
│   └── FxRatePersistenceService.java         persist + 重试 + DLQ
├── entity/{FxXrInf, FxXrInfHis, FxXrInfDlq}.java
├── mapper/{FxXrInfMapper, FxXrInfHisMapper, FxXrInfDlqMapper}.java
└── dto/FxRatePushDTO.java

src/main/resources/
├── application.yml                           公共配置
├── application-example.yml                   env 配置模板
└── mapper/
    ├── FxXrInfMapper.xml                     UPSERT + 时间守卫
    └── FxXrInfHisMapper.xml                  INSERT 单行

docs/
├── fx-rate-disruptor-plan.md                 设计 Spec v2.1
├── fx-push-flow-analysis_20260427.md         全链路逐行解析
└── sql/ddl-fx_xr_inf_dlq.sql                 DLQ 建表脚本
```

## FAQ

**Q1：为什么不用 `WorkerPool`？**
LMAX Disruptor 4.0.0 移除了 `WorkerPool`。本项目用 **N 个 EventHandler + modulo 分片** 实现等价的工作队列语义：每个 handler 只处理 `sequence % N == myIndex` 的事件，互不重复。

**Q2：为什么 worker-count 默认是 8？**
单条路径两条 SQL（UPSERT + INSERT）≈ 4ms，1000 QPS 需要 4 并发。8 提供 2× 余量，生产前请用压测校准。

**Q3：RingBuffer 满会怎样？**
`BlockingWaitStrategy` 让生产者线程阻塞在 `ringBuffer.next()`，这个阻塞会向上传导到 Tomcat 工作线程（即 HTTP 响应变慢），形成天然背压。

**Q4：DLQ 什么时候触发？**
持久化事务重试 `retry-times` 次后仍抛异常，使用**独立事务**写 `fx_xr_inf_dlq`；DLQ 写入本身失败只记 ERROR 日志，不再级联重试。

**Q5：如何在生产环境观测？**
建议后续接入 Prometheus，采集三项指标：`RingBuffer.remainingCapacity()`、`persist` 耗时分布、DLQ 计数。目前已通过 SLF4J 打印关键日志。

## License

MIT（根据需要调整）。

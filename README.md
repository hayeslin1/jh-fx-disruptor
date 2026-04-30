# hayes-fx-disruptor

基于 **LMAX Disruptor 4.0.0** 的实时外汇汇率接收 / 落库微服务。承接上游（银行 / 报价渠道）高频推送，目标 ≈ **1000 QPS**（100 货币对 × 10 渠道）。

> HTTP 接入 → Disruptor 内存 RingBuffer → N 线程 **μs 级双通道入队** →
> `fx_xr_inf` 每 50 ms 批量 UPSERT ＋ `fx_xr_his` 每 2 s 批量 INSERT →
> 整批失败回退单条 `persist` 重试 + DLQ 兜底。

> **v2 改造（2026-04-30）**：消费侧由"同步单条落库 118 ms"改为"纯内存入队 μs 级 + 定时批量 flush"，详见
> [`docs/fx-async-batch-persist_20260430.md`](docs/fx-async-batch-persist_20260430.md)。

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

- **双通道异步批量落库**：
  - `fx_xr_inf` 报价通道：`ConcurrentHashMap` 按 `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` 合并最新值，每 **50 ms** 批量 UPSERT，报价延迟上限 ≤ 50 ms。
  - `fx_xr_his` 历史通道：`LinkedBlockingQueue(100k)` 保留全量，每 **2 s** 或队列满时批量 INSERT，延迟上限 ≤ 2 s。
- **消费侧 μs 级入队**：`onEvent` 仅做内存 `merge + offer`，Disruptor sequence 立即推进，生产者不再被消费速度拖累（v1 118 ms → v2 ≈ 5 μs）。
- **UPSERT + 时间守卫**：`INSERT ... ON DUPLICATE KEY UPDATE` 基于 `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` 唯一键；`orderGuard` 按 `DT+TM_CHANNEL_PUBLISH` 比较，防止并发乱序覆盖新值（批量模式下 VALUES(col) 行级生效）。
- **指数退避重试 + DLQ**：批量失败回退原 `persist` 路径，3 次 `[10, 50, 200] ms` 退避；全败写 DLQ 独立事务 + ERROR 日志。
- **优雅停机**（SmartLifecycle，按 `phase` 降序停）：`DisruptorConfig`（`MAX_VALUE`）先停等 in-flight 归位 → Flushers（`MAX_VALUE - 100`）后停，最后一次 drain 到底，JVM 正常退出不丢。
- **生产侧背压**：`BlockingWaitStrategy` 让 RingBuffer 满时生产者线程阻塞；`historyBuffer.offer` 满时 Handler 同步 `flushNow()` 再重试，保住 Disruptor 不阻塞。
- **分片并行消费**：N 个 `EventHandler` 共享 RingBuffer，基于 `sequence % workerCount` modulo 过滤，互不重复。

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
                                                   │  RingBuffer 4096 │ ← 多生产者 MULTI
                                                   └────────┬─────────┘
                                                            │ (BlockingWaitStrategy)
                                          ┌─────────┬───────┼───────┬─────────┐
                                    fx-disruptor-1      ...       fx-disruptor-N
                                          │                                   │
                                          ▼                                   ▼
                       FxRatePersistenceEventHandler（μs 级，modulo 分片）
                          │                                      │
                   event.copy() → latestBuffer.merge    event.copy() → historyBuffer.offer
                          │                                      │
                ┌─────────▼───────────┐              ┌───────────▼────────────┐
                │ FxRateLatestFlusher  │              │ FxRateHistoryFlusher    │
                │ 每 50 ms drainAll    │              │ 每 2 s drainTo(1000)    │
                │ doPersistLatestBatch │              │ doPersistHistoryBatch   │
                │ UPSERT fx_xr_inf     │              │ INSERT fx_xr_inf_his    │
                └─────────┬───────────┘              └───────────┬────────────┘
                          │ 整批失败                              │ 整批失败
                          └──────────┐         ┌─────────────────┘
                                     ▼         ▼
                           FxRatePersistenceService.persist()
                              ├─ 3 次 [10,50,200]ms 退避重试
                              └─ 全败 → writeDlq() @REQUIRES_NEW
                                       └─ INSERT fx_xr_inf_dlq
```

- **v2 改造文档**：[`docs/fx-async-batch-persist_20260430.md`](docs/fx-async-batch-persist_20260430.md)（本次改动的完整说明）
- **v1 全链路解析**：[`docs/fx-push-flow-analysis_20260427.md`](docs/fx-push-flow-analysis_20260427.md)（改造前同步路径逐行讲解，保留作参考）
- **v1 设计 Spec**：[`docs/fx-rate-disruptor-plan.md`](docs/fx-rate-disruptor-plan.md)

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
    shutdown-timeout-seconds: 5   # Disruptor 优雅停机等待
  fx:
    op-code: SYS_FX_RATE          # 主表 OP_CTE / OP_UTE 固定账号
    default-deli-typ: "00"        # DELI_TYP 缺省兜底
    order-guard-enabled: true     # 启用时间守卫
    retry-times: 3                # 批量失败回退单条重试次数（不含首次）
    retry-backoff-ms: 10,50,200   # 指数退避序列
    dlq-enabled: true             # 全败写 DLQ 表
    flush:
      latest-interval-ms: 50              # 报价通道 flush 间隔
      latest-batch-max-size: 500          # 单批 UPSERT 上限
      history-interval-ms: 2000           # 历史通道 flush 间隔
      history-batch-max-size: 1000        # 单批 INSERT 上限
      history-queue-capacity: 100000      # 历史队列容量
      history-queue-warn-threshold: 50000 # 高水位告警阈值
```

> JDBC URL 建议启用 `rewriteBatchedStatements=true`，让多 VALUES 批量生效。

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
│   ├── FxProperties.java                     app.fx.*（含 Flush 内嵌段）
│   └── DisruptorConfig.java                  Disruptor Bean + N handler + SmartLifecycle(phase=MAX)
├── buffer/                                     v2 新增，双通道内存缓冲
│   ├── LatestKey.java                        (channelCd, ccyPair, deliTyp) 复合 key
│   ├── FxRateLatestBuffer.java               ConcurrentHashMap + 时间戳保序 merge
│   └── FxRateHistoryBuffer.java              LinkedBlockingQueue(100k) + 高水位告警
├── flusher/                                    v2 新增，批量 flusher
│   ├── FxRateLatestFlusher.java              50 ms drainAll → UPSERT 批量
│   └── FxRateHistoryFlusher.java             2 s drainTo → INSERT 批量，flushNow 供背压
├── disruptor/
│   ├── FxRateEvent.java                      事件 POJO（含 copy() 深拷贝）
│   ├── FxRateEventFactory.java               RingBuffer 预填充
│   ├── FxRateEventTranslator.java            DTO → Event
│   ├── FxRateEventProducer.java              publishEvent
│   └── handler/FxRatePersistenceEventHandler.java   μs 级双通道入队
├── controller/
│   └── FxRateReceiveController.java          POST /fx/push
├── service/
│   ├── FxRateService.java                    traceId + 投递
│   └── FxRatePersistenceService.java         persist + doPersistLatestBatch + doPersistHistoryBatch + DLQ
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
├── fx-async-batch-persist_20260430.md        v2 双通道异步批量落库改造（当前架构）
├── fx-rate-disruptor-plan.md                 v1 设计 Spec
├── fx-push-flow-analysis_20260427.md         v1 全链路逐行解析
└── sql/ddl-fx_xr_inf_dlq.sql                 DLQ 建表脚本
```

## FAQ

**Q1：为什么不用 `WorkerPool`？**
LMAX Disruptor 4.0.0 移除了 `WorkerPool`。本项目用 **N 个 EventHandler + modulo 分片** 实现等价的工作队列语义：每个 handler 只处理 `sequence % N == myIndex` 的事件，互不重复。

**Q2：为什么 worker-count 默认是 8？**
v2 下消费侧是内存 op（μs 级），单 handler 理论上够用；保留分片是为将来加 CPU 密集逻辑时不用再改架构。生产前用压测校准。

**Q3：RingBuffer 满会怎样？**
`BlockingWaitStrategy` 让生产者线程阻塞在 `ringBuffer.next()`，向上传导到 Tomcat 工作线程（HTTP 响应变慢），形成天然背压。v2 下消费侧 μs 级，RingBuffer 几乎不会堆积。

**Q4：DLQ 什么时候触发？**
批量通道整批失败 → 回退单条 `persist` 重试 `retry-times` 次仍失败 → 独立事务写 `fx_xr_inf_dlq`；DLQ 写入本身失败只记 ERROR 日志，不再级联重试。

**Q5：如何在生产环境观测？**
建议接入 Prometheus 采集：`RingBuffer.remainingCapacity()`、`latestBuffer.size`、`historyBuffer.size`、每轮 flush batch size / 耗时、降级次数、DLQ 计数。目前已通过 SLF4J 打印关键日志。

**Q6：v2 会丢数据吗？**
JVM **正常停机**（Spring `SmartLifecycle` 正常触发）不丢：Disruptor 先停让 in-flight 归位，Flushers 后停把 buffer 排干。
JVM **崩溃**（OOM、SIGKILL）时，内存 buffer 未 flush 的部分会丢——这是"纯内存队列"方案的已知窗口，**依赖上游具备重推能力**。
接入 HTTP 同步推送业务方时需升级到 Outbox 表方案。

**Q7：为什么上游停推时，history 不足 1000 条也没到 2 s 就停了？**
`historyBatchMaxSize=1000` 是**单次 drain 的上限**，不是触发条件。`scheduleAtFixedRate` 每 2 s 无条件 tick 一次——上游停推后，最迟在下一个 tick 把队列里剩余的 N 条全部 drain 批量写入。触发条件只有三种：定时 tick / 队列满 `flushNow()` / 停机 `drainUntilEmpty`。

## License

MIT（根据需要调整）。

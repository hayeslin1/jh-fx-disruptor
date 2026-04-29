# FX 实时汇率 · Disruptor 接入与落库方案

> 目标：银行实时推送汇率（100 货币对 × 10 通道 = **1000 条/秒**），使用 Disruptor 作为内存队列，**实时逐条**落库至 `fx_xr_inf`（UPSERT 最新态，供实时查询）和 `fx_xr_inf_his`（INSERT 历史流水）。
>
> 文档状态：**Spec v2.1 · HARD-GATE 通过 · 执行中** · Disruptor 4.0.0 移除 WorkerPool，本版改为 N EventHandler + modulo(sequence) 分片（语义等价）。

---

## 0. 现状与约束

| 项目 | 现状 | 出处 |
|---|---|---|
| 启动类 | 已就绪，包路径 `com.hayes.base.fx` | `src/main/java/com/hayes/base/fx/Application.java` |
| 依赖 | spring-boot-starter-web / mybatis-plus:3.5.2 / mysql / lombok / fastjson2 / easyexcel / commons-lang3 | `pom.xml:63-100` |
| 构建 | `spring-boot-maven-plugin` 打 ZIP，`mainClass=com.hayes.base.fx.Application` | `pom.xml:118-134` |
| Profile | test / rc / prod（无 dev） | `pom.xml:24-61` |
| Disruptor 依赖 | **未引入**，需新增 `com.lmax:disruptor:4.0.0`（JDK17+） | — |
| 业务代码 | **无任何业务类** | — |
| 表 DDL | ✅ **已确认**（见下 §0.1 / §0.2） | 用户提供 |
| 上游协议 | **暂定 HTTP Push**，接口层预留扩展点（后续可替换为 Kafka/MQ 消费者） | 用户决定 |
| JDK | **17+**（对应 Disruptor 4.0.0） | 用户决定 |

### 0.1 `fx_xr_inf`（最新态 · UPSERT 目标）

- 唯一键 `uk_fx_xrinf_channelCcyPair (CHANNEL_CD, CCY_PAIR, DELI_TYP)` —— 即 UPSERT 的匹配键。
- NOT NULL 且无默认值字段：`CCY_PAIR / CHANNEL_CD / BUY_PRICE / SELL_PRICE / DT_CTE / TM_CTE / DT_UTE / TM_UTE / DT_CHANNEL_PUBLISH / TM_CHANNEL_PUBLISH`。
- 日期 `DT_*` 格式 `YYYYMMDD`；时间 `TM_*` 格式 `HHmmss`；均为字符串。
- `DELI_TYP` 默认 `'0'`（按 DDL 默认值）；银行若未推则默认 `'00'`（TOD）—— 待 §8 Q-A 确认。
- `BL_PRICE`（彭博价）允许 NULL。

### 0.2 `fx_xr_inf_his`（历史流水 · INSERT）

- 主键 `(ID, DT_CTE)`，按 `DT_CTE` RANGE 分区（当前仅 `pmax`）。
- `DT_CTE` 为 `int`（YYYYMMDD 数值）；与主表 `DT_CTE` 字段类型**不一致**（主表是 varchar(8)），需在 Entity / Mapper 转换。
- `SUCC_FLG` NOT NULL —— 实时推送场景固定填 `'01'`（成功）；失败场景由 DLQ/日志承担，不落此表。
- 其余字段基本可空，按银行来报填。

---

## 1. 六步实施规划（概览）

| 步骤 | 产物 | 关键决策 |
|---|---|---|
| **Step 1** 需求封板 | 本文档 v2 + §8 HARD-GATE 回执 | 禁止攒批；UPSERT 匹配键；DLQ 策略；OP 账号 |
| **Step 2** Disruptor 拓扑与事件建模 | `FxRateEvent / Factory / Producer / Config` | RingBuffer 4096、ProducerType.MULTI、BlockingWaitStrategy、8 个 EventHandler + modulo 分片 |
| **Step 3** 持久化层 · **实时单条落库** | `FxXrInfMapper.upsert` + `FxXrInfHisMapper.insert`；单条事务 | `INSERT ... ON DUPLICATE KEY UPDATE` + 可选时间守卫防乱序 |
| **Step 4** 代码落盘清单 | 全部类的路径/职责/方法签名 | 字段对齐 DDL |
| **Step 5** 依赖 & 配置 & 装配 | `pom.xml` + `application.yml` + Spring 生命周期 | Disruptor 4.0.0；@PreDestroy 优雅关闭 |
| **Step 6** 验证与压测 | 单测 / 集成测试 / 1k QPS 压测 | 实时性：端到端 P99 < 100ms |

---

## 2. Step 1 · 需求对齐（确认状态）

### 2.1 业务流

```
[银行] ──HTTP POST /fx/push──▶ [Controller/Service]
                                     │ 参数校验 + traceId
                                     ▼
                              [Disruptor RingBuffer 4096]
                                     │ 并行订阅（8 个 EventHandler，同一 sequence 只被 sequence%8==idx 的 handler 认领）
                          ┌──────────┼──────────┐
                          ▼          ▼          ▼   ... ×8
                      [Handler0] [Handler1] ... [Handler7]
                          │
              单条事务（每条独立）：
                  1) UPSERT fx_xr_inf （可选时间守卫）
                  2) INSERT fx_xr_inf_his
              失败 → 指数退避重试 3 次 → DLQ 处理
```

> **实时性承诺**：事件进 Ring 到提交事务，端到端延迟目标 P99 < 100ms。查询侧永远能读到最近一次推送的最新值。

### 2.2 确认状态一览

| # | 项 | 决议 |
|---|---|---|
| Q1 上游协议 | ✅ HTTP Push（`POST /fx/push`），接口/DTO 预留扩展（后续可包 MQ Consumer） |
| Q2 payload 形态 | ✅ **单条**（body = 单条 JSON）；如需批量后续加 `/fx/push/batch` |
| Q3 主表 DDL | ✅ 已确认 |
| Q4 历史表 DDL | ✅ 已确认 |
| Q5 UPSERT 匹配键 | ✅ `(CHANNEL_CD, CCY_PAIR, DELI_TYP)` |
| Q6 顺序语义 | ⚠ 默认"最新覆盖"；禁止攒批后多 Worker 并行可能旧覆盖新 → **推荐启用 `TM_CHANNEL_PUBLISH` 时间守卫**，见 §8 Q-B |
| Q7 失败策略 | ⚠ 推荐 "重试 3 次（10/50/200ms 指数退避）→ 写 DLQ 表 `fx_xr_inf_dlq`"；若不建 DLQ 则改为"日志 + 告警"，见 §8 Q-C |
| Q8 Disruptor 版本 | ✅ 4.0.0 |
| Q9 JDK | ✅ 17+ |
| Q10 事务粒度 | ✅ 单条一事务（UPSERT + INSERT HIS 在同一 `@Transactional`） |

**剩余待你决定的只有 §8 的 Q-A / Q-B / Q-C / Q-D 四项。**

---

## 3. Step 2 · Disruptor 拓扑与事件建模

### 3.1 拓扑（禁止攒批 · 每条独立消费）

```
Producer(MULTI) ──▶ RingBuffer(4096) ──▶ 并行 handleEventsWith(h0, h1, ..., h7)
                                              │
                                              └─ 单条事务：UPSERT + INSERT HIS
```

- Disruptor 4.0.0 已移除 `WorkerPool`，改用 **N 个 `EventHandler` + modulo 分片**：
  - `disruptor.handleEventsWith(h0, h1, ..., h7)`（并行订阅，每个 handler 看到全部 sequence）
  - 每个 handler 的 `onEvent(event, sequence, endOfBatch)` 开头判断 `if (sequence % workerCount != myIndex) return;`
  - 语义等价 WorkerPool：每条事件恰好被 1 个 handler 处理，充分并行。
- 不做本地攒批：每条事件触发一次事务，立即落库。
- 并行带来的"旧覆盖新"问题由 §4.3 的 SQL 级时间守卫解决。

### 3.2 参数推荐（实时单条场景）

| 参数 | 值 | 理由 |
|---|---|---|
| RingBuffer 大小 | **4096** (2^12) | 1k QPS × 4s 缓冲，应对 GC/慢 SQL 抖动；2 的幂 |
| ProducerType | **MULTI** | Tomcat 多线程写入 |
| WaitStrategy | **BlockingWaitStrategy** | 生产环境稳态 CPU 友好；低延迟场景可换 `YieldingWaitStrategy`（配置可切） |
| EventHandler 数 (worker-count) | **8** | 单条两 SQL ≈ 4ms，1000/s ÷ (1000/4) = 4 够用，取 8 留 2× 余量 |
| 事务边界 | **单条** | UPSERT + INSERT HIS 在同一事务，保证一致性 |
| 连接池 | Hikari `maximum-pool-size=16` | 8 worker + 少量查询连接 + 余量 |

### 3.3 事件类 `FxRateEvent`（字段对齐 DDL）

```java
// 可变对象，Disruptor 复用实例；每次 publish 前由 Translator 填充
public class FxRateEvent {
    // 业务字段（映射表字段）
    private String ccyPair;            // CCY_PAIR  货币对，如 "USDCNY"
    private String channelCd;          // CHANNEL_CD 通道编号
    private BigDecimal buyPrice;       // BUY_PRICE  买入价
    private BigDecimal sellPrice;      // SELL_PRICE 卖出价
    private BigDecimal blPrice;        // BL_PRICE   彭博价，可空
    private String deliTyp;            // DELI_TYP   交割类型，默认 "00"
    private String dtChannelPublish;   // DT_CHANNEL_PUBLISH YYYYMMDD
    private String tmChannelPublish;   // TM_CHANNEL_PUBLISH HHmmss
    private String utcTimes;           // UTC_TIMES
    // 运行时字段
    private long receiveNanos;         // 入队纳秒，用于端到端延迟统计
    private String traceId;            // 链路追踪
    public void clear() { /* 复用前清空 */ }
}
```

---

## 4. Step 3 · 持久化层设计（实时单条）

### 4.1 单条事务边界

```java
@Transactional(rollbackFor = Exception.class)
public void persist(FxRateEvent e) {
    fxXrInfMapper.upsert(e);          // UPSERT 最新态
    fxXrInfHisMapper.insert(e);       // 追加历史流水
}
```

### 4.2 UPSERT SQL（`fx_xr_inf`）

用 `INSERT ... ON DUPLICATE KEY UPDATE`，覆盖"首次到达的 pair 自动 INSERT、存在则 UPDATE"两种语义：

```xml
<insert id="upsert" parameterType="com.hayes.base.fx.disruptor.FxRateEvent">
  INSERT INTO fx_xr_inf (
    CCY_PAIR, CHANNEL_CD, BUY_PRICE, SELL_PRICE, BL_PRICE,
    OP_CTE, DT_CTE, TM_CTE, OP_UTE, DT_UTE, TM_UTE,
    DT_CHANNEL_PUBLISH, TM_CHANNEL_PUBLISH, DELI_TYP, UTC_TIMES
  ) VALUES (
    #{ccyPair}, #{channelCd}, #{buyPrice}, #{sellPrice}, #{blPrice},
    #{opCode}, #{today}, #{now}, #{opCode}, #{today}, #{now},
    #{dtChannelPublish}, #{tmChannelPublish}, #{deliTyp}, #{utcTimes}
  )
  ON DUPLICATE KEY UPDATE
    BUY_PRICE           = VALUES(BUY_PRICE),
    SELL_PRICE          = VALUES(SELL_PRICE),
    BL_PRICE            = VALUES(BL_PRICE),
    OP_UTE              = VALUES(OP_UTE),
    DT_UTE              = VALUES(DT_UTE),
    TM_UTE              = VALUES(TM_UTE),
    DT_CHANNEL_PUBLISH  = VALUES(DT_CHANNEL_PUBLISH),
    TM_CHANNEL_PUBLISH  = VALUES(TM_CHANNEL_PUBLISH),
    UTC_TIMES           = VALUES(UTC_TIMES)
</insert>
```

`opCode / today / now` 由 Handler 预先计算并塞进 event（OP 账号见 §8 Q-A）。

### 4.3 乱序保护（推荐启用 · §8 Q-B）

多 Worker 并行会出现"旧覆盖新"，用 SQL 级守卫兜底：

```sql
-- 在 ON DUPLICATE KEY UPDATE 子句里把每个被更新字段包一层 IF
BUY_PRICE = IF(CONCAT(VALUES(DT_CHANNEL_PUBLISH), VALUES(TM_CHANNEL_PUBLISH))
              >= CONCAT(DT_CHANNEL_PUBLISH, TM_CHANNEL_PUBLISH),
              VALUES(BUY_PRICE), BUY_PRICE)
-- SELL_PRICE / BL_PRICE / OP_UTE / DT_UTE / TM_UTE / DT_CHANNEL_PUBLISH / TM_CHANNEL_PUBLISH / UTC_TIMES 同样包一层
```

效果：只有新来的 `DT_CHANNEL_PUBLISH+TM_CHANNEL_PUBLISH` 不早于当前行才覆盖，否则保持原值。**单条 SQL，无锁争用**。

### 4.4 历史表 INSERT（`fx_xr_inf_his`）

纯 INSERT，`SUCC_FLG='01'`，`DT_CTE` 为 `int` 类型（需要把 YYYYMMDD 字符串转 int）：

```xml
<insert id="insert" parameterType="com.hayes.base.fx.disruptor.FxRateEvent"
        useGeneratedKeys="true" keyProperty="id">
  INSERT INTO fx_xr_inf_his (
    CCY_PAIR, CHANNEL_CD, BUY_PRICE, SELL_PRICE, BL_PRICE,
    OP_CTE, DT_CTE, TM_CTE,
    DT_CHANNEL_PUBLISH, TM_CHANNEL_PUBLISH, SUCC_FLG, DELI_TYP, UTC_TIMES
  ) VALUES (
    #{ccyPair}, #{channelCd}, #{buyPrice}, #{sellPrice}, #{blPrice},
    #{opCode}, #{todayInt}, #{now},
    #{dtChannelPublish}, #{tmChannelPublish}, '01', #{deliTyp}, #{utcTimes}
  )
</insert>
```

### 4.5 失败处理

- Handler 内 `try/catch DataAccessException` → 指数退避重试 3 次（10ms / 50ms / 200ms）。
- 全部失败 → §8 Q-C 决定：
  - 选项 1（默认推荐）：写 `fx_xr_inf_dlq` 表（需额外建 DDL，附简易表结构）
  - 选项 2：仅打 ERROR 日志 + 指标上报（不建表）
- 异常独立，不阻塞后续事件。

---

## 5. Step 4 · 代码落盘清单

```
src/main/java/com/hayes/base/fx/
├── Application.java                                      【已存在】
├── config/
│   └── DisruptorConfig.java                              @Configuration；@Bean Disruptor/RingBuffer + handleEventsWith(N 个 handler)；@PreDestroy 优雅关闭
├── disruptor/
│   ├── FxRateEvent.java                                  事件 DTO（可变，字段对齐 DDL）
│   ├── FxRateEventFactory.java                           EventFactory<FxRateEvent>
│   ├── FxRateEventTranslator.java                        EventTranslatorOneArg<FxRateEvent, FxRatePushDTO>
│   ├── FxRateEventProducer.java                          publish(FxRatePushDTO)；Ring 满时由 WaitStrategy 阻塞
│   └── handler/
│       └── FxRatePersistenceEventHandler.java            EventHandler<FxRateEvent>；构造参数 (myIndex, workerCount)，内部 modulo 过滤后调 FxRatePersistenceService.persist(event)
├── controller/
│   └── FxRateReceiveController.java                      @RestController POST /fx/push（单条）；预留 /fx/push/batch
├── service/
│   ├── FxRateService.java                                参数校验 + traceId + 投递 Producer
│   └── FxRatePersistenceService.java                     @Transactional persist(event)：upsert + insert his；失败重试 + DLQ
├── entity/
│   ├── FxXrInf.java                                      @TableName("fx_xr_inf") · 对齐 DDL
│   └── FxXrInfHis.java                                   @TableName("fx_xr_inf_his") · DT_CTE 为 Integer
├── mapper/
│   ├── FxXrInfMapper.java                                BaseMapper<FxXrInf> + int upsert(FxRateEvent)
│   ├── FxXrInfMapper.xml                                 带时间守卫的 INSERT...ON DUPLICATE KEY UPDATE
│   ├── FxXrInfHisMapper.java                             BaseMapper<FxXrInfHis> + int insert(FxRateEvent)
│   └── FxXrInfHisMapper.xml                              单行 INSERT
├── dto/
│   └── FxRatePushDTO.java                                接收入参（含 JSR303 校验注解）
└── common/
    ├── FxConst.java                                      OP 账号常量、DELI_TYP 默认值、SUCC_FLG
    └── FxTimeUtils.java                                  Date → YYYYMMDD / HHmmss 工具
```

---

## 6. Step 5 · 依赖 & 配置

### 6.1 `pom.xml` 新增

```xml
<dependency>
  <groupId>com.lmax</groupId>
  <artifactId>disruptor</artifactId>
  <version>4.0.0</version>
</dependency>
```

（若 parent pom 已有 Hikari / validation-starter 则沿用；否则在 Step 2 落盘时按需补）

### 6.2 `application.yml` 新增段

```yaml
app:
  disruptor:
    ring-buffer-size: 4096
    worker-count: 8
    wait-strategy: blocking            # blocking | yielding | busy-spin | sleeping
    shutdown-timeout-seconds: 5
  fx:
    op-code: SYS_FX_RATE                # 见 §8 Q-A，默认 SYS_FX_RATE
    default-deli-typ: "00"
    order-guard-enabled: true           # 见 §8 Q-B，默认启用时间守卫
    retry-times: 3
    retry-backoff-ms: 10,50,200
    dlq-enabled: true                   # 见 §8 Q-C，默认启用 DLQ 表
```

### 6.3 装配要点

- `DisruptorConfig`：`@Bean` 构建 `Disruptor<FxRateEvent>`，`handleEventsWith(handler0..handler7)` 后 `start()`；`@PreDestroy disruptor.shutdown(shutdownTimeoutSeconds, SECONDS)`。
- 每个 `FxRatePersistenceEventHandler` 实例持有 `(myIndex, workerCount)`，`onEvent(event, seq, endOfBatch)` 首句 `if (seq % workerCount != myIndex) return;`，再调 Spring 管理的 `FxRatePersistenceService.persist(event)`（事务在 Service 上）。
- Disruptor 4.0.0 API 注意：`Disruptor` 构造函数接收 `ThreadFactory`（不再支持 `Executor`）；`EventHandler` 合并了 `LifecycleAware / BatchStartAware`。

---

## 7. Step 6 · 验证与压测

### 7.1 单元测试

| 用例 | 断言 |
|---|---|
| `Producer.publish(dto)` | WorkHandler 收到的 event 字段与 dto 一致 |
| `PersistenceService.persist` 成功 | Mapper `upsert`/`insert` 各被调用 1 次 |
| `PersistenceService.persist` 主表失败 | 事务回滚，his 表无新增 |
| 时间守卫启用 · 旧时间戳覆盖 | 主表字段不变，his 仍落一条 |
| 重试 · 前 2 次失败第 3 次成功 | 最终一次成功入库 |
| 重试 3 次全失败 | 写入 DLQ 表 / 打 ERROR 日志（按 Q-C） |

### 7.2 集成测试（Testcontainers MySQL 8）

- 推 1000 条不同 pair × channel 组合 → 主表最终行数 = 唯一组合数；his 表 = 1000。
- 同一 (pair, channel, deli) 乱序推 10 条时间戳混乱的事件 → 主表最终为"时间戳最大那条"。

### 7.3 压测

- 工具：`wrk` / `jmeter` 打 `/fx/push`，或自写多线程 producer 直推 Ring。
- 场景：1k QPS 稳态 60s；2k QPS 尖峰 10s。
- 指标：
  - RingBuffer `remainingCapacity()` 曲线（不应长期接近 0）
  - 端到端延迟 P50/P99（接收→事务提交）
  - MySQL QPS / 连接池利用率 / 慢查询
- 验收（R12 证据）：
  - ✅ 1k QPS 稳态 60s 无丢失（his 计数 = 推送计数）
  - ✅ P99 端到端延迟 < 100ms
  - ✅ RingBuffer 峰值利用率 < 30%
  - ✅ 无慢查询告警

---

## 8. HARD-GATE · 剩余待确认（4 项）

> 其余 Q1-Q5/Q8-Q10 已在 §2.2 闭环；以下 4 项需你拍板。

| # | 问题 | 候选 | 我的默认推荐 |
|---|---|---|---|
| **Q-A** | `OP_CTE / OP_UTE` 填什么？（NOT NULL 字段兜底） | ① 固定常量 `SYS_FX_RATE`；② 银行推文里带的操作员字段；③ 空串 `''` | **① `SYS_FX_RATE`**（写入 `common/FxConst.java`） |
| **Q-B** | 是否启用"时间戳守卫"防乱序覆盖？ | ① 启用（SQL 内嵌 IF 比较 `DT+TM_CHANNEL_PUBLISH`）；② 不启用（最新到达即覆盖，接受偶发乱序） | **① 启用**（零性能代价，防并发乱序） |
| **Q-C** | 重试全败后如何兜底？ | ① 建 `fx_xr_inf_dlq` 表（需我出 DDL 一并交付）；② 仅 ERROR 日志 + Prometheus 计数器；③ 两者都做 | **③ 两者都做**（日志必做，DLQ 强烈推荐） |
| **Q-D** | 当银行推文缺 `DELI_TYP` 字段，兜底默认值？ | ① `"00"`（TOD）；② DDL 默认 `"0"`；③ 拒绝入库 | **① `"00"`**（语义最明确） |

---

## 9. 执行顺序（确认后）

1. `pom.xml` 加 `disruptor:4.0.0`；`application.yml` 新增 `app.disruptor` / `app.fx` 两段。
2. 落盘 `FxRateEvent / EventFactory / EventTranslator / DisruptorConfig`。
3. 落盘 `Entity / Mapper / Mapper.xml`（含 UPSERT + 时间守卫 + HIS INSERT + DLQ 表 DDL）。
4. 落盘 `FxRatePersistenceService`（@Transactional 事务 + 重试 + DLQ/日志）。
5. 落盘 `EventHandler / Producer / Controller / Service / DTO / Const / TimeUtils`。
6. 单测 + 集成测试 + 本地压测；在 `tasks/todo.md` 补 Review 小节（证据）。

> 执行中若发现偏差，按 R8 "Reverse Sync" 先改本文档再改代码。

---

## 🚦 HARD-GATE 请回复：

请回复 **Q-A / Q-B / Q-C / Q-D** 四项的选择（或直接说"默认推荐全采用"），我即开始 §9 落盘。

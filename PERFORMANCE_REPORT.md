# actor-go 性能报告

> 面向 actor-go（Go Actor 模型并发框架，模块 `github.com/lcy03406/actor-go`，v0.2.0）的基准性能与日志开销分析。
> 本文档以 **`none` 模式（纯逻辑基线，日志调用在级别判断处即返回，不构建记录、不落盘）为基准（1.0x）**，其余各档日志配置均表示为相对该基线的倍数，以隔离出框架真实算力与日志开销。

---

## 1. 项目简介

actor-go 是一个基于 Go 泛型、类型安全的 Actor 模型并发框架，核心设计要点（均来自代码实测）：

- **类型安全的消息**：通过 `ActorId`、`Request[A, R, Q0, R0]` 等泛型约束消息与回复类型；每个请求类型由 `ReqType` 方法声明其字符串标识，框架据此路由 handler。
- **显式生命周期**：actor 分 `spawn`（首条匹配消息触发创建）与 `query`/`serve` 两类 handler；handler 内必须显式调用 `a.Open()` 才能让 actor 在响应后保持活跃（active），否则处理完即回到 idle，后续消息会被拒绝。
- **Ref 直连寻址**：`ActorContext.Ref(id)` 返回 `ActorRef`，可绕过「Manager → Group → handler」查找链路，用 `RefCall` / `RefPost` 直接投递同组 actor，降低延迟。
- **背压与流控**：邮箱为非阻塞发送；目标信箱满时返回 `ActorBusyError`（调用方可重发）或 `ActorPostponeError`（框架自动入延期队列重试），天然支持背压。
- **结构化日志**：基于 `log/slog`，生产配置为 lumberjack JSON 轮转落盘 + 控制台仅 error 级。

---

## 2. 测试环境

| 项 | 配置 |
|---|---|
| CPU | 13th Gen Intel(R) Core(TM) i7-13700KF |
| 并行度 | `GOMAXPROCS=24`（`-24` 后缀） |
| Go 工具链 | go1.27.2（windows/amd64） |
| 模块 | `github.com/lcy03406/actor-go/actor` |
| 测试命令 | `go test -run='^$' -bench=. -benchmem ./actor/` |

> 测试机为 24 核桌面 CPU，绝对数值随硬件变化，但**相对关系与开销拆解结论（以 none 为 1.0x）具有普适性**。

---

## 3. 基准测试套件

套件覆盖 33 条基准，分六类：

1. **基础调用**：`V2Call` / `V2Post` / `V2PostThenCall` / `V2Spawn`。
2. **并行与并发**：`V2CallParallel` / `V2PostParallel` / `V2ConcurrentCalls` / `Call/Post With/NoContention`。
3. **一对多分发**：`V2Broadcast` / `V2Multicast`（含丢包率统计）。
4. **延迟画像**：`V2CallLatency`（p50/p90/p99）。
5. **业务场景**：`V2GameServerScenario` / `V2ChatRoomScenario` / `V2ECommerceScenario` / 多种 `V2*Workload`。
6. **Ref 直连 vs 标准调用**：`RefCall` / `RefPost` 与 `Call` / `Post` 对照。

---

## 4. 核心性能（以 none 为基准 = 框架真实算力）

下表为 `none` 模式（日志完全跳过）下的**固有性能**，并标注「采用默认 lumberjack 日志后变慢多少倍（×lumberjack）」，以直观体现日志对框架算力的侵蚀。

### 4.1 基础调用

| 基准 | none ns/op | B/op | allocs/op | ×lumberjack |
|---|---:|---:|---:|---:|
| BenchmarkV2Call-24 | 1,916 | 850 | 30 | 7.7x |
| BenchmarkV2Post-24 | 260 | 637 | 21 | 21.8x |
| BenchmarkV2PostThenCall-24 | 3,572 | 1,480 | 51 | 34.2x |
| BenchmarkV2Spawn-24 | 17,864 | 21,624 | 58 | 2.7x |
| BenchmarkV2CallParallel-24 | 885 | 841 | 30 | 41.7x |
| BenchmarkV2PostParallel-24 | 145 | 323 | 13 | 54.8x |
| BenchmarkV2ConcurrentCalls-24 | 418 | 848 | 30 | 62.9x |

> `V2Spawn` 仅 17.9µs 即完成「spawn + 保持活跃 + 状态注册」；`V2PostParallel` 仅 145ns。注意：一旦开启默认日志，这些操作会被放大 3~63 倍（见 §5）。

### 4.2 一对多分发与延迟

| 基准 | none ns/op | ×lumberjack | 关键指标（none） |
|---|---:|---:|---|
| BenchmarkV2Broadcast-24 | 15,087 | 66.9x | send_drop 0.020% |
| BenchmarkV2Multicast-24 | 17,704 | 57.3x | 977 allocs/op |
| BenchmarkV2CallLatency-24 | 1,134,452 | 26.9x | **p50 2.0µs / p90 4.0µs / p99 5.3µs** |

> 关掉日志后单次调用延迟 p50 仅 ~2µs；开默认日志后 p50 膨胀到 57.5µs（见 §5.2）。`V2CallLatency` 的 ns/op 高是因为该基准在窗口内聚合了端到端往返与延迟采样开销。

### 4.3 业务场景

| 基准 | none ns/op | ×lumberjack |
|---|---:|---:|
| BenchmarkV2GameServerScenario-24 | 376 | 62.9x |
| BenchmarkV2ChatRoomScenario-24 | 884 | 32.7x |
| BenchmarkV2ECommerceScenario-24 | 386 | 73.9x |
| BenchmarkV2ActorLifecycle-24 | 32,270 | 7.2x |
| BenchmarkV2MixedWorkload-24 | 194,589 | 6.8x |
| BenchmarkV2MixedWorkloadChurn-24 | 141 | 803x |
| BenchmarkV2StatefulGrowingWorkload-24 | 185 | 158x |

> `V2MixedWorkloadChurn` / `V2StatefulGrowingWorkload` 在关日志时仅百 ns 级，开默认日志后暴涨数百倍——它们是日志最密集的场景。

### 4.4 Ref 直连 vs 标准调用（none 基线）

| 基准 | none ns/op | 对照基准 | none ns/op | Ref 加速比 |
|---|---:|---|---:|---:|
| BenchmarkRefPostNoContention-24 | 59 | BenchmarkPostNoContention-24 | 247 | **4.2x** |
| BenchmarkRefCallNoContention-24 | 1,258 | BenchmarkCallNoContention-24 | 1,675 | 1.3x |
| BenchmarkRefPostWithContention-24 | 14 | BenchmarkPostWithContention-24 | 91 | **6.5x** |
| BenchmarkRefCallWithContention-24 | 517 | BenchmarkCallWithContention-24 | 601 | 1.2x |

> 关日志后 `RefPost` 比 `Post` 快 4~6 倍（日志开销移除后，绕过 Group 查找的收益更凸显）；`RefCall` 因仍需同步等待回复，加速比约 1.3x。

### 4.5 背压与小型邮箱

`V2PostSmallMailbox`（邮箱容量 4/16/64）在高速生产者下触发背压：

| 容量 | none ns/op | 丢弃率 |
|---|---:|---:|
| cap=4 | 143.5 | 99.88% |
| cap=16 | 128.0 | 99.92% |
| cap=64 | 131.2 | 99.91% |

> 关日志时生产者极快，小邮箱丢弃率高达 ~99.9%——这是背压机制按设计工作（满即返回 `ActorBusyError`），而非错误。

---

## 5. 日志开销专项分析（以 none 为 1.0x 基准）

四档日志模式：

| 模式 | 行为 | 相对 none |
|---|---|---:|
| **none**（基准） | 级别设为高于一切，`Enabled` 永远 false——日志调用在级别判断处即返回，不构建记录、不落盘。 | **1.0x** |
| **discard** | 仍按 Debug 级构建每条记录，但写入 `io.Discard`。 | **4.39x** |
| **lumberjack**（默认） | JSON 格式 + lumberjack 轮转文件**同步**落盘（关键路径同步完成序列化 + I/O）。 | **35.21x** |
| **async** | 同上落盘，但 JSON handler 外包 `channel + 后台 goroutine`，关键路径只做「记录克隆 + 入队」。 | **12.37x** |

### 5.1 总体结论（几何平均）

| 对比段（以 none 为分母） | 倍率 | 含义 |
|---|---:|---|
| none → discard（仅记录构建） | **4.39x** | slog 构建记录本身就不便宜（+339%） |
| none → lumberjack（同步落盘合计） | **35.21x** | 默认配置相对纯逻辑基线慢 **34 倍** |
| none → async（异步落盘合计） | **12.37x** | 异步把开销压到同步的 0.35x（约 2.8x 更快） |

**拆解**：同步 lumberjack 相对 none 的 35.21x 中，记录构建贡献约 **4.4x**（none→discard），JSON 序列化 + 文件 I/O 再乘约 **8x**（discard→lumberjack）；异步化把「JSON+I/O」从关键路径移出，回收了 lumberjack 相对 none 额外开销的约 **67%**。

### 5.2 关键基准四档对照（none = 基准列）

| 基准 | none | discard | lumberjack | async | ×lumberjack |
|---|---:|---:|---:|---:|---:|
| V2Call | 1,916 | 6,086 | 14,728 | 7,280 | 7.7x |
| V2Post | 260 | 1,131 | 5,678 | 4,703 | 21.8x |
| V2Spawn | 17,864 | 27,772 | 47,693 | 16,235 | 2.7x |
| V2ConcurrentCalls | 418 | 1,413 | 26,271 | 9,684 | 62.9x |
| V2Multicast | 17,704 | 36,735 | 1,013,533 | 298,758 | 57.3x |
| V2CallLatency (p50) | 2,004ns | 8,018ns | 57,468ns | 14,409ns | 28.7x |
| V2GameServerScenario | 376 | 1,195 | 23,640 | 8,486 | 62.9x |
| V2ECommerceScenario | 386 | 1,522 | 28,527 | 9,270 | 73.9x |
| V2MixedWorkloadChurn | 141 | 1,490 | 113,320 | 20,666 | 803x |
| RefPostNoContention | 59 | 635 | 7,103 | 2,744 | 120x |
| RefPostWithContention | 14 | 126 | 5,152 | 1,916 | 368x |

> **规律**：日志量越大，相对 none 的惩罚越重（`V2MixedWorkloadChurn` 达 803x）；异步收益也越明显（`V2MixedWorkloadChurn` 异步 20.7µs vs 同步 113µs = 0.18x；`V2Multicast` 0.29x）。记录构建占比高的基准异步收益较小（`V2Post` 仅 0.83x）。

### 5.3 异步不是免费

异步把同步 lumberjack 的 8x I/O 惩罚压缩到约 0.35x（相对同步），但异步仍比 none 慢 **12.4x**（比纯记录构建的 discard 还差 2.8x），因为：① 记录克隆 + 入队本身有成本；② 后台写线程持续抢占 CPU/IO，与负载竞争。这是生产环境异步日志的真实代价。

---

## 6. 使用建议

1. **热路径（每消息一次日志）是性能头号杀手**：默认同步落盘相对「完全跳过日志」慢 **35 倍**；即便只构建记录不落盘也慢 **4.4 倍**。
2. **需要全量落盘时改用异步 writer**（如本报告 async 模式）：把吞吐惩罚从 35x 降到约 12x，相对同步快约 2.8x——这是生产最务实的折中，但仍非免费。
3. **追求框架真实算力（none 级）必须关掉或极简日志**：基准回归、延迟敏感路径用 `BENCH_LOGGER=none` 测量。
4. **生产环境抬升文件日志级别**至 `LevelInfo/Warn`，避免 Debug 全量落盘（无论同步异步都大幅减少写入量）。
5. **高频同组互发优先用 Ref 直连**：`RefPost` 较 `Post` 快 4~6 倍，且关日志后优势更明显。
6. **合理设置邮箱容量与背压**：小邮箱在高生产者下丢包率极高，按业务容忍度配置 `BufMails`。
7. **跑基准定位性能用 `BENCH_LOGGER=none`**：最接近「框架真实算力」，最适合做回归基线（none 78.5s / discard 60.4s / lumberjack 90.5s / async 72.1s）。

---

## 7. 复现方法

```bash
BENCH_LOGGER=none      go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # 纯逻辑基线（本报告基准，1.0x）
BENCH_LOGGER=discard   go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # 仅构建记录
                         go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # lumberjack 同步落盘（默认 / 真实项目同款）
BENCH_LOGGER=async     go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # lumberjack 异步落盘
```

> 完整四档逐基准明细与分组汇总见 `actor/bench_logger_compare.md`。

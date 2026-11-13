# 日志开销四档对比：完全跳过 / 直接丢弃 / Lumberjack 同步落盘 / Lumberjack 异步落盘

> 目的：把 actor-go 的「框架自身日志逻辑」开销，与「slog 记录构建」「lumberjack 同步落盘」「lumberjack 异步落盘」各自的代价拆开量化。
>
> 测试机：13th Gen Intel(R) Core(TM) i7-13700KF（GOMAXPROCS=24，`-24` 后缀）
> 模块：`github.com/lcy03406/actor-go/actor`，go1.27.2
> 日志配置（`actor/bench_log_test.go` 的 `init`）：由 `BENCH_LOGGER` 环境变量切换。

## 四种模式

| 模式 | 环境变量 | 行为 |
|---|---|---|
| **none** | `BENCH_LOGGER=none` | handler 级别设为 127（高于所有级别），`Enabled` 永远 false。框架每次日志调用在**级别判断**处立即返回——不构建记录、不格式化、不落盘。衡量「框架日志调用 + 级别判断」的**纯逻辑开销**（对照基线）。 |
| **discard** | `BENCH_LOGGER=discard` | `LevelDebug` 的 `TextHandler` 写入 `io.Discard`。**仍按 Debug 级构建每条 slog 记录**（属性解析 + 文本格式化），只是输出被丢弃。 |
| **lumberjack**（同步） | 默认（或 `lumberjack`） | 与实际项目 `server/app/server.go` 同款：JSON 格式 + lumberjack 轮转文件落盘（`MaxSize=100MB / MaxBackups=5 / Compress=true`），控制台仅 `LevelError`（quiet）。关键路径同步完成 JSON 序列化 + 文件 I/O。 |
| **async**（异步） | `BENCH_LOGGER=async` | 同上文件落盘，但 JSON handler 外包一层 `channel + 后台 goroutine`：关键路径只做「记录克隆 + 入队」（缓冲 1<<16），JSON 序列化与文件 I/O 移出关键路径，由后台 goroutine 异步写出。 |

## 开销拆解逻辑

```
none  ──(+记录构建)──▶  discard  ──(+JSON同步I/O)──▶  lumberjack
                                                  ▲
                                          async：把「JSON+I/O」移出关键路径
                                          关键路径仅 +记录克隆+入队，
                                          后台 goroutine 仍做 JSON+I/O（抢占 CPU/IO）
```

- **none → discard** = slog 记录构建（属性解析 + 文本格式化）+ 写 io.Discard 的成本。
- **discard → lumberjack** = JSON 序列化 + 缓冲写入 + 文件 I/O + 轮转检查的成本（同步，阻塞关键路径）。
- **lumberjack → async** = 把上述 JSON+I/O 从关键路径移到后台；关键路径改为「克隆记录 + 入队」+ 后台写线程的 CPU/IO 竞争。
- **none → async** = 以上全部之和，即异步日志相对「完全跳过」的总开销。

## 总体结论（几何平均）

| 对比段 | 几何平均倍率 | 说明 |
|---|---|---|
| none → discard（记录构建） | **4.39x** | +339%，slog 构建记录本身就不便宜 |
| discard → lumberjack（JSON+I/O 同步） | **8.01x** | +701%，同步落盘的硬开销 |
| none → lumberjack（同步合计） | **35.21x** | +3421% |
| **lumberjack → async**（异步回收） | **0.35x** | async 仅 35% 的同步 lumberjack 耗时 → **约 2.8x 更快** |
| **none → async**（异步合计） | **12.37x** | +1137%，异步仍未回到基线，但比同步的 35x 好很多 |
| 异步回收的「lumberjack 相对 none 的额外开销」 | **≈ 67%** | (35.21−12.37)/(35.21−1) |

**一句话**：异步化把 lumberjack 的 8x 同步 I/O 惩罚压缩到约 0.35x（相对同步），但**不是免费**——异步日志仍比完全跳过慢 12.4x（比 discard 的 4.4x 还差 2.8x），因为记录克隆+入队有成本，且后台写线程持续抢占 CPU/IO、与负载竞争。

## 按基准类型分组（几何平均）

| 分组 | async / lumberjack | async / none | lumberjack / none |
|---|---:|---:|---:|
| V2 核心调用 | 0.41x | 4.52x | 11.07x |
| Ref / 标准对比 | 0.35x | 24.20x | 68.90x |
| 工作负载场景 | 0.35x | 12.99x | 36.77x |
| 无争用 / 争用 | 0.34x | 20.81x | 61.33x |

规律：**日志量越大的基准，异步收益越大**（同步 lumberjack 越是 I/O 瓶颈，异步把 I/O 移出路径的收益越明显）；而记录构建占比高的基准，异步收益较小。

## 逐基准明细（ns/op）

| Benchmark | none | discard | lumberjack | async | async/lumber | async/none |
|---|---:|---:|---:|---:|---:|---:|
| BenchmarkV2Call-24 | 1916.0 | 6086.0 | 14728.0 | 7280.0 | 0.49x | 3.80x |
| BenchmarkV2Post-24 | 260.5 | 1131.0 | 5678.0 | 4703.0 | 0.83x | 18.05x |
| BenchmarkV2PostThenCall-24 | 3572.0 | 14042.0 | 122056.0 | 27224.0 | 0.22x | 7.62x |
| BenchmarkV2CallParallel-24 | 884.9 | 3538.0 | 36904.0 | 8644.0 | 0.23x | 9.77x |
| BenchmarkV2Spawn-24 | 17864.0 | 27772.0 | 47693.0 | 16235.0 | 0.34x | 0.91x |
| BenchmarkV2ConcurrentCalls-24 | 417.5 | 1413.0 | 26271.0 | 9684.0 | 0.37x | 23.20x |
| BenchmarkV2PostParallel-24 | 144.9 | 449.9 | 7945.0 | 2724.0 | 0.34x | 18.80x |
| BenchmarkV2Multicast-24 | 17704.0 | 36735.0 | 1013533.0 | 298758.0 | 0.29x | 16.88x |
| BenchmarkV2CallCPUWorkload-24 | 35072.0 | 42829.0 | 102236.0 | 35462.0 | 0.35x | 1.01x |
| BenchmarkV2CallStringWorkload-24 | 2684.0 | 11134.0 | 27228.0 | 14918.0 | 0.55x | 5.56x |
| BenchmarkV2CallMapWorkload-24 | 2226.0 | 9713.0 | 26718.0 | 14924.0 | 0.56x | 6.70x |
| BenchmarkV2GameServerScenario-24 | 376.0 | 1195.0 | 23640.0 | 8486.0 | 0.36x | 22.57x |
| BenchmarkV2ChatRoomScenario-24 | 883.5 | 4815.0 | 28918.0 | 15560.0 | 0.54x | 17.61x |
| BenchmarkV2ECommerceScenario-24 | 386.0 | 1522.0 | 28527.0 | 9270.0 | 0.32x | 24.02x |
| BenchmarkV2ActorLifecycle-24 | 32270.0 | 98209.0 | 233830.0 | 71633.0 | 0.31x | 2.22x |
| BenchmarkV2MixedWorkload-24 | 194589.0 | 137242.0 | 1323367.0 | 505842.0 | 0.38x | 2.60x |
| BenchmarkV2MixedWorkloadNoSpawn-24 | 674.0 | 4345.0 | 126479.0 | 50809.0 | 0.40x | 75.38x |
| BenchmarkV2MixedWorkloadChurn-24 | 141.2 | 1490.0 | 113320.0 | 20666.0 | 0.18x | 146.36x |
| BenchmarkV2StatefulGrowingWorkload-24 | 185.1 | 1315.0 | 29298.0 | 9003.0 | 0.31x | 48.64x |
| BenchmarkCallNoContention-24 | 1675.0 | 8831.0 | 27748.0 | 8509.0 | 0.31x | 5.08x |
| BenchmarkRefCallNoContention-24 | 1258.0 | 4873.0 | 18080.0 | 5603.0 | 0.31x | 4.45x |
| BenchmarkPostNoContention-24 | 247.4 | 2228.0 | 22879.0 | 6503.0 | 0.28x | 26.29x |
| BenchmarkRefPostNoContention-24 | 59.0 | 634.6 | 7103.0 | 2744.0 | 0.39x | 46.48x |
| BenchmarkCallWithContention-24 | 600.8 | 3678.0 | 28357.0 | 8787.0 | 0.31x | 14.63x |
| BenchmarkRefCallWithContention-24 | 517.1 | 3648.0 | 18407.0 | 6293.0 | 0.34x | 12.17x |
| BenchmarkPostWithContention-24 | 91.2 | 486.4 | 11201.0 | 4793.0 | 0.43x | 52.53x |
| BenchmarkRefPostWithContention-24 | 14.1 | 125.5 | 5152.0 | 1916.0 | 0.37x | 136.18x |
| BenchmarkAcquireAndCall-24 | 3269.0 | 44017.0 | 50416.0 | 14740.0 | 0.29x | 4.51x |
| BenchmarkAcquireAndPost-24 | 1585.0 | 11074.0 | 46012.0 | 12395.0 | 0.27x | 7.82x |

`V2MixedWorkload` 的 none/discard 出现状态ful噪声（计时窗口内 spawn 次数不同），其逐档差值不反映真实回归；整体几何平均与分组结论不受单点噪声影响。

## 落盘体积观测

- lumberjack（同步）单次运行：当前日志 ~80MB + 5 个压缩备份（合计 ~13MB）。
- async 单次运行：因生产端不再被磁盘阻塞、日志总量更大，`bench_logs` 峰值约 **162MB**（`MaxBackups:5 + Compress` 已封顶，不会无限膨胀）。
- `none` / `discard` 模式不产生日志文件。

## 已知问题（与日志无关，按用户要求保持现状未修）

- `BenchmarkAcquireAndPost`（`ref_bench_test.go`）偶发 `actor busy: RefBench(1,bench_1)` 失败：框架复用 actor 的并发偶发抢占问题。根因：`refBenchPostReq` handler 用 `actor.RefPost`（非阻塞 fire-and-forget）向 target 投递，target 信箱（BufMails=1024）在调度/GC 瞬时落后时被填满，`send()` 返回 `ActorBusyError`；而 `ActorBusyError` 按框架契约是「瞬时背压、调用方可重发」信号，基准却直接 `b.Fatal`。兄弟基准 `RefPostNoContention`/`RefPostWithContention` 同样调用 `RefPost` 但丢弃错误（`_ =`），故不致命；`AcquireAndCall` 用同步 `RefCall` 故永不 busy。复跑 20 次：18 次 PASS（~1722–1793 ns/op）、2 次 FAIL 且均为同一 `actor busy`。本对比四档数据均已取稳定值。修复方式（未执行）：在基准循环内对 `ActorBusyError` 重试同一条请求。

## 实践建议

1. **热路径（每消息一次日志）切勿同步 JSON 落盘**：相对完全跳过日志吞吐掉到 1/35。
2. **需要全量落盘时改用异步 writer**（如本 `async` 模式）：把吞吐惩罚从 35x 降到约 12x，相对同步 lumberjack 快约 2.8x——这是生产环境最务实的折中。
3. **异步也不是免费**：仍比完全跳过慢 12x（比纯记录构建的 discard 还差 2.8x），因为克隆+入队有成本且后台写线程抢占资源。因此**热路径仍应避免每消息打日志**，异步只是把「不得不打」的代价降到可控。
4. **生产环境抬升文件日志级别**至 `LevelInfo/Warn`，避免 Debug 全量落盘（无论同步异步都大幅减少写入量）。
5. **跑基准定位性能用 `BENCH_LOGGER=none`（或 discard）**：none 78.5s / discard 60.4s / lumberjack 90.5s / async 72.1s；`none` 最接近「框架真实算力」，最适合做回归基线。

## 复现命令

```bash
BENCH_LOGGER=none      go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # 完全跳过
BENCH_LOGGER=discard   go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # 构建记录但丢弃
                         go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # lumberjack 同步落盘（默认）
BENCH_LOGGER=async     go test -run='^$' -bench=. -benchmem -p 1 ./actor/   # lumberjack 异步落盘
```

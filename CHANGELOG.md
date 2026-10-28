# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- `cmd/readmegen`：把各包 `example_test.go` 中用 `// region:` 圈出的片段注入
  `README.md` / `README.zh.md` 的 `<!-- example: name -->` 标记区。示例因此始终是编译
  单元（`go test` 会编译 `example_test.go`），API 变更会让示例过时当场暴露；
  `readmegen -check` 校验 README 是否最新（CI 已接入）。README 中游离的 ```go 代码块
  会被拒绝，防止再长出无人校验的片段。
- `cmd/shared_example`：共享数据容器的可运行示例（owner 发布 → 容器推送 → 读者
  缓存命中 → `Fetch` 强一致读取）。

### Changed

- 示例全面翻新：新增 `actor` / `shared` / `grain` / `rpc` 四个包的 `example_test.go`，
  README 中的示例改为由上面的标记注入；新增 README 小节覆盖 `SetOnSpawn` 钩子与
  `CtxValue`。
- 修正两处过期示例：`actor.Serve(mgr, 100, ...)` 旧签名（现为 `actor.Options{BufMails}`）、
  `ctx.Ref(roomId)` 跨 Group 引用（`ActorRef` 只能同 Group）。
- `grain` 的快照器实例改为由调用方显式传入：`SetupGrain(b, pm, snap, onActivate)` /
  `State.Activate(ctx, pm, snap)`。框架不再用 `var t T` 自造零值，因此删除了
  `snapshotterOf` 与反射：T 只负责类型层面（锚定 S 并校验实例形态），实例类型由 `snap T`
  与 T 在编译期对齐（写错形态直接编译失败）。也不再要求快照器无状态——值类型实例每个
  Grain 一份副本，指针类型实例在 Group 内共享；只有当快照器方法用指针接收者且不访问
  接收者时，传 nil 才是安全的（`ShotSelf` 是值接收者，写 `*ShotSelf[D]` 时必须传 `&{}`）。
- 修正 `cmd/actor_example` / `cmd/rpc_example` / `cmd/cluster_example` 缺少 `ctx.Open()`
  导致 Actor 处理完首条消息即被回收（后续 `Call` 报 `actor not found`）；
  `cmd/cluster_example` 不再对同一 Group 二次 `Serve`（会 panic），迁移 handler 与业务
  handler 在同一次 `Serve` 中注册。
- 延后队列（postpone）不再只由发送方下一次发消息被动驱动：队列非空时武装 100ms
  的兜底 flush 定时器（`armPostponeFlush`），生命周期结束时（`clear()`，Quit 与回到
  空闲池共用）再 flush 一次并逐条 `Error` 告警仍未投出的消息。队列仍挂在
  `ActorControl` 上（下次 spawn 是新生命周期，**不**自动重发），但"消息静默消失"
  不再是静默的。
- `Manager.WithValue(key, value any)` 泛型化为强类型 context 值：新增 `CtxValue[T]` /
  `DefineValue[T]`，挂载用包级 `WithValue[T](mgr, v, value)`，读取用 `CtxValue.Get(ctx)`
  返回 `(T, bool)`，免除私有 key struct 与类型断言样板；且在 Serve 之后调用会 panic
  （原先对已注册的 Group 静默不可见）。
- `shared` 包重写为共享数据容器：移除 `Cache` / `Versioned` / `ServeWriter` /
  `RegisterRefresh`（同一 Group 只能持有一份数据，多份会静默覆盖），改为按 key 的
  发布订阅模型：`Define` / `Publish` / `ServeReader` / `Watch` / `Fetch`。
  容器是框架内置的分片 Group，handler 由框架提供、只 Reply 并用 Post 推送，
  不 Call 任何 actor，因此不可能形成调用环（无死锁）；支持一个 Group 订阅多个 key、
  冷却合并推送、TTL 对账自愈、`WithClone` 发布时克隆。

## [0.1.0] - 2026-08-01

### Added

- Actor 核心模型：泛型 Actor[A, S]、Group、Manager，支持类型安全的单线程 Actor 并发模型
- 消息处理：Post（fire-and-forget）、Call（请求-回复）、Broadcast、Multicast
- 生命周期管理：Spawn、Close、Kill、Quit，支持 drain 排空和 in-flight handler 保护
- 可取消 Timer：Actor 内部定时器，支持 Stop 和退出时自动取消
- RPC 远程通信：WebSocket Server/Client + JSON Codec，支持远程 Post/Call/Broadcast/Multicast
- 可扩展 Codec/Transport 接口：支持替换序列化协议（JSON、Protobuf 等）
- Grain 生命周期管理：带租约的持久化 Actor，支持 JSON/YAML/Redis/MongoDB 驱动
- 分布式租约：Local/Redis/MongoDB/SQL 实现，提供 fencing token 机制
- 集群支持：节点发现、路由、Placement 策略
- 编译期类型安全：Request[A, R] 接口确保跨 Group 类型错误在编译期暴露
- Content 超时：Call 支持 context 超时和取消
- 完善的测试覆盖：单元测试、并发测试、Benchmark 测试
- 示例代码：cmd/actor_example（本地 Actor）、cmd/rpc_example（RPC）、cmd/grain_example（持久化 Grain）
- OnSpawn 钩子：`RegistryBuilder.SetOnSpawn` 支持在 Actor 首次创建（spawn 消息）时、
  用户 spawn/serve handler 之前自动调用一次，用于初始化状态或资源

### Fixed

- 修复 OnSpawn 初始化失败时 `nctx` 为 `nil` 导致 `prevIdle = nctx.idle` 的空指针 panic，
  并让 spawn 消息 caller 通过 `m.Fail(err)` 收到该错误而非永久阻塞
- 修复 `prevIdle` 读取时机错误导致在 OnSpawn 内调用 `Open()` 无法使 Actor 保持活跃的问题
  （idle 计数未被正确扣减，对外表现为 Actor 未创建，Count 为 0）
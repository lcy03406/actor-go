# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
- 示例代码：cmd/example（本地 Actor）、cmd/rpc_example（RPC）、cmd/grain_example（持久化 Grain）
- OnSpawn 钩子：`RegistryBuilder.SetOnSpawn` 支持在 Actor 首次创建（spawn 消息）时、
  用户 spawn/serve handler 之前自动调用一次，用于初始化状态或资源

### Fixed

- 修复 OnSpawn 初始化失败时 `nctx` 为 `nil` 导致 `prevIdle = nctx.idle` 的空指针 panic，
  并让 spawn 消息 caller 通过 `m.Fail(err)` 收到该错误而非永久阻塞
- 修复 `prevIdle` 读取时机错误导致在 OnSpawn 内调用 `Open()` 无法使 Actor 保持活跃的问题
  （idle 计数未被正确扣减，对外表现为 Actor 未创建，Count 为 0）
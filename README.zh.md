# actor-go

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/lcy03406/actor-go)](https://goreportcard.com/report/github.com/lcy03406/actor-go)

**actor-go** 是一个面向 Go 的类型安全 Actor 模型框架，内置 RPC 远程调用、分布式集群，以及持久化 Grain 生命周期管理。

> A type-safe Actor Model framework for Go, featuring built-in RPC, distributed clustering, and persistent grain lifecycle management.

## 快速开始

```bash
# 安装
go get github.com/lcy03406/actor-go

# 运行示例
go run ./cmd/actor_example/    # 本地 Actor：spawn/Call/Post、OnSpawn、Timer、CtxValue、ActorRef
go run ./cmd/rpc_example/      # 基于 WebSocket 的 RPC
go run ./cmd/grain_example/    # 持久化 Grain（激活 / 存盘 / 重新加载）
go run ./cmd/shared_example/   # 共享数据容器：发布 → 推送 → 读者缓存

# 运行测试
go test ./...

# 校验 README 里的示例与代码一致（改过示例后跑）
go run ./cmd/readmegen -check
```

## 项目结构

```
actor-go/
├── cmd/
│   ├── actor_example/        # 本地 Actor 示例
│   ├── rpc_example/          # 基于 WebSocket 的 RPC 示例
│   ├── grain_example/        # 持久化 Grain 示例
│   ├── shared_example/       # 共享数据容器示例
│   ├── cluster_example/      # 集群迁移示例
│   ├── engineering_example/  # 推荐的包结构与端到端示例
│   └── readmegen/            # 把 example_test.go 的 region 注入 README
├── actor/                    # Actor 核心
│   ├── types.go              # ActorId、Request 接口
│   ├── actor.go              # actorRuntime —— 单线程事件循环
│   ├── actor_context.go      # ActorContext —— 处理器上下文
│   ├── group.go              # Group[A,S] —— 类型化 Actor 池
│   ├── manager.go            # Manager —— 多 Group 容器
│   ├── handler.go            # 处理器分发
│   ├── invoke.go             # Post/Call/Broadcast/Multicast
│   ├── ref.go                # ActorRef —— 直接 Actor 引用（绕过 Group 查找）
│   ├── registry_builder.go   # RegisterSpawn / RegisterQuery / RegisterServe
│   ├── timer.go              # 可取消的定时器
│   ├── close.go              # 优雅关闭（排空 + 在途请求）
│   └── errors.go             # 错误类型
├── rpc/                      # 基于 WebSocket 的 RPC
│   ├── types.go              # Message、Codec、Transport 接口
│   ├── server.go             # WebSocket 服务端
│   ├── client.go             # WebSocket 客户端
│   ├── entry.go              # Post/Call/Broadcast/Multicast 适配器
│   ├── json.go               # JSON 编解码 + 传输
│   └── registry.go           # RPC 请求注册表
├── grain/                    # 持久化 Grain Actor
│   ├── lifecycle.go          # Activate（租约 + 加载，返回 loaded/created）
│   ├── manager.go            # PersistenceManager
│   ├── snapshot.go           # Snapshotter 接口 + ShotSelf
│   ├── driver_json.go        # JSON 文件驱动
│   ├── driver_yaml.go        # YAML 文件驱动
│   ├── driver_redis.go       # Redis 驱动
│   └── driver_mongo.go       # MongoDB 驱动
├── cluster/                  # 分布式集群
│   ├── cluster.go            # Router + 租约重试路由
│   ├── node.go               # 节点类型（Node、NodeSet、MemberEvent）
│   ├── membership.go         # Membership 接口 + 事件
│   ├── placement.go          # PlacementStrategy（一致性哈希、组感知）
│   └── migration.go          # 所有权迁移（ShouldOwn）
├── shared/                   # 共享数据容器（owner 按 key 发布 + 读者按 key 订阅 + 冷却合并推送）
│   ├── key.go                # Define / Def[V] / Key[V] / KeyOption —— key 定义与编译期类型绑定
│   ├── store.go              # Serve / storeId / 分片容器 Group 与发布订阅 handler
│   ├── owner.go              # Publish / PublishWait / Unpublish —— owner 侧发布
│   └── reader.go             # ServeReader / Reader / Watch / Fetch —— 读者侧订阅与缓存
├── LICENSE
├── CONTRIBUTING.md
├── CHANGELOG.md
├── CODE_OF_CONDUCT.md
└── SECURITY.md
```

## 架构

```
                     ┌───────────────────────┐
                     │       Manager         │
                     │  (非泛型)             │
                     └──────────┬────────────┘
                                │
               ┌────────────────┼────────────────┐
               │                │                │
     ┌─────────▼──────┐  ┌──────▼───────┐  ┌─────▼──────────┐
     │ Group[A1, S1]  │  │ Group[A2,S2] │  │ Group[A3, S3]  │
     │ (ActorId,State)│  │              │  │                │
     └────────────────┘  └──────────────┘  └────────────────┘
```

- **Manager** 持有多个 **Group**，每个 Group 对应一种独立的 `(ActorId, State)` 类型组合。
- 每个 **Actor** 运行在独立的 goroutine 中，使用串行化的邮箱 —— 无需加锁。
- 泛型操作以包级函数形式提供（Go 的方法无法拥有独立的类型参数）。
- `A` 由 `Request[A, R]` 推导；`S` 由 `Serve` 注册推导。

### 串行设计（单线程事件循环）

每个 Actor 都是一个**单线程状态机**。发送给同一个 Actor 的所有消息都严格按照
到达顺序、一次只处理一个 —— 其状态永远不会被并发访问，因此处理器无需加锁或使用互斥量。

```
         多个发送方（goroutine）
                │  Post / Call / RefPost / RefCall
                ▼
        ┌───────────────────────┐
        │   带缓冲的 channel      │   ← 邮箱（FIFO 先进先出）
        │   (mailbox chan)       │
        └───────────┬───────────┘
                    │  ← 一次取一条，由 run() 顺序出队
                    ▼
        ┌───────────────────────┐
        │   actorRuntime.run()   │   ← 唯一能触碰该 Actor
        │   （事件循环）          │      State 与 Context 的 goroutine
        │  for msg in mailbox:   │
        │    invoke handler       │
        └───────────┬───────────┘
                    │
                    ▼
            handler 修改 ctx.State()  （无需加锁）
```

**工作原理**

1. **每 Actor 一个 goroutine**。`resolveActor` 仅通过 `go actor.run()` 创建一次
   Actor。此后，该唯一 goroutine 在整个生命周期内独占该 Actor 的 `State` 与
   `ActorContext` —— 其他 goroutine 永不触碰。
2. **串行邮箱**。发送方不会直接调用处理器，而是将一个 `invokable` 推入 Actor 的
   带缓冲 `mailbox` channel（`send`）。`run` 循环按 FIFO 顺序出队并依次调用处理器。
3. **批量、保序执行**。`run` 将当前可取到的全部消息批量取出（`pumpMailbox`），
   再按原顺序逐个调用（`invokeBatch`），保证顺序。每条消息的处理都完整结束后
   才开始下一条。
4. **无共享状态竞争**。由于状态变更只发生在 `run` 这一个 goroutine 内，
   `ctx.State()` 的读写天然无数据竞争 —— 处理器内部无需 `sync.Mutex`。
5. **并发发生在 Actor *之间*，而非单个 Actor *之内***。不同的 Actor 运行在
   不同的 goroutine 中，可以真正并行执行；框架保证的是：*单个* Actor 绝不会
   同时处理两条消息。

**对处理器编写者的启示**

- 你可以放心地修改 `ctx.State()`、调用 `ctx.SetState(...)`，无需加锁。
- 一个缓慢或阻塞的处理器只会拖慢该 Actor *自身*的邮箱 —— 不影响其他 Actor。
  长耗时工作（I/O、计算）应通过 `ctx.Timer`、`ctx.Ref`，或尽早回复来卸出，
  使事件循环保持响应。
- Actor 之间**不保证**跨 Actor 的到达顺序：若 `A` 与 `B` 先后都给 `C` 发消息，
  它们到达 `C` 邮箱的顺序取决于调度。需要因果顺序时请用显式建模（例如 `C` 先
  回复 `A`，再由 `A` 去通知 `B`）。

> 这正是 Actor 模型的核心：通过「串行化」实现隔离。框架在结构上强制了这一点，
> 因此正确性不依赖于处理器作者是否记得加锁。

#### ⚠️ 避免死锁（关键）

由于消息处理器运行在该 Actor 的 `run` 循环**内部**，而该循环又是**唯一**能
处理此 Actor 邮箱的东西，因此处理器**绝不可阻塞等待它自己的邮箱**。最常见的
死锁，是在处理器中对自己调用 `Call` / `RefCall`（不论直接，还是经由一条
绕回调用方的 Actor 链间接发生）：

```
Actor X 的处理器：
    Call(X, &SomeReq{})   // 把回复消息推入 X 的邮箱，然后阻塞等待……
                          // 但 X 的邮箱此刻被正在运行的处理器挡住
                          // → 回复永远无法被处理 → 死锁
```

**避免死锁的规则**

1. **绝不要对自己调用 `Call`/`RefCall`**。不要在正在运行调用方的同一个 Actor 上
   请求自己的处理器。改用 `Post`（发后不管）配合一条显式回复消息，或者把逻辑
   重构成一个直接修改 `ctx.State()` 的单一处理器。
2. **警惕循环 `Call` 链**。若 `A` 调 `B`，`B` 的处理器调 `C`，而 `C` 又调回 `A`
   （或任何环路），这些 Actor 会互相阻塞在彼此的邮箱上而死锁。对于 Actor 图
   内部的通知，优先用 `Post`，使参与者都不阻塞。
3. **`Call`/`RefCall` 必须带上下文超时**。务必传入带超时的 `context` ——
   `Call(ctx, …)` / `RefCall(ctx, …)`。没有超时的情况下，下游 Actor 崩溃或变慢
   会把一个逻辑错误变成无限期挂起。`ctx` 超时能让调用方及时返回（它并不「解决」
   死锁，但能避免调用方 goroutine 永远卡死）。
4. **不要长时间阻塞事件循环**。一个会 sleep、做重同步 I/O、或等待外部 channel
   的处理器，会占住该 Actor 唯一的 goroutine，从而卡住该 Actor 的**所有**待处理
   消息。这类工作应通过 `ctx.Timer`、由独立 goroutine 完成后用 `Post` 回报，或
   尽早回复来卸出。
5. **`Post` 永远安全**。`Post`/`RefPost` 永不阻塞调用方，也从不等待任何邮箱，
   因此不可能让发送方死锁。优先用 `Post`；只有当目标是**另一个**独立存活的
   Actor、且已设置超时时，才使用 `Call`。

> 小结：只要处理器阻塞在**同一个** Actor 的邮箱上（或阻塞在一串邮箱的环路上），
> 死锁就会发生。保持处理器非阻塞、优先用 `Post`、给每次 `Call` 设置超时、绝不
> 调用自己。

### 类型安全

- **`Request[A, R]`**：`ReqType(A, *R) string` 确保编译期 `A`/`Q`/`R` 匹配。
- **Post 约束**：只有 `Request[A, OkReply]` 才能用于 `Post`；带自定义回复的请求必须使用 `Call`。
- **跨 Group 隔离**：某 Group 的请求无法发送给另一个 Group —— 由编译器直接拒绝。

```text
// 编译错误：Attack 返回 *AttackReply，不能 Post
actor.Post(mgr, id, &Attack{Damage: 10})
// → *Attack does not implement Request[PlayerId, OkReply]

// 正确：用 Call 取回复
reply, err := actor.Call(ctx, mgr, id, &Attack{Damage: 10})
```

## 核心 API

### 1. 定义类型

<!-- example: actor/types -->
```go
// Actor ID：ActorType() 决定所属 Group，String() 用于日志与错误信息。
type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
	return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

// 状态：处理器直接读写，无需加锁。
type PlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}

// 回复：Call 的返回类型。
type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

// 请求：实现 Request[A, R]，ReqType 把 Id 与 Reply 绑定，跨 Group 发送时编译期报错。
type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
	Damage int `json:"damage"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type Heal struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ PlayerId, _ *HealReply) string { return "Heal" }

type Close struct{}

func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }
```
<!-- /example: actor/types -->

### 2. 注册处理器

<!-- example: actor/serve -->
```go
mgr := actor.NewManager(slog.Default())

actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	// RegisterSpawn：首条消息创建 Actor（发后不理）
	actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
		ctx.Open() // spawn 时框架不会自动激活，需显式 Open 才能常驻
		ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
		return actor.OK, nil
	})

	// RegisterQuery：只处理已存在的 Actor（返回回复）
	actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
		ctx.State().HP -= req.Damage
		alive := ctx.State().HP > 0
		return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
	})

	// RegisterServe：首条消息创建 Actor，并且返回回复
	actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, spawning bool) (*HealReply, error) {
		if spawning {
			ctx.Open()
		}
		ctx.State().HP += req.Amount
		return &HealReply{NewHP: ctx.State().HP}, nil
	})
})
```
<!-- /example: actor/serve -->

| 注册 | Spawn（创建） | Query（已有） | 回复 |
|------|:---:|:---:|:---:|
| `RegisterSpawn` | 是 | 否 | `OkReply` |
| `RegisterQuery` | 否 | 是 | 自定义 |
| `RegisterServe` | 是 | 是 | 自定义 |

#### RequestHandler —— 自包含的请求类型

除了单独传入处理函数，请求类型也可以实现 `RequestHandler`，将处理逻辑直接绑定在请求结构体上。
这样可以把相关逻辑聚合在一起，减少样板代码 —— 无需为每个注册编写单独闭包。

<!-- example: actor/request_handler -->
```go
type Logout struct{}

func (*Logout) ReqType(_ PlayerId, _ actor.OkReply) string { return "Logout" }

// Handle 包含处理逻辑 —— 无需在 Serve 中另写闭包。
func (req *Logout) Handle(ctx *actor.ActorContext[PlayerId, PlayerState], _ bool) (actor.OkReply, error) {
	ctx.Quit()
	return actor.OK, nil
}
```
<!-- /example: actor/request_handler -->

<!-- example: actor/register_handler -->
```go
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	// 请求类型自带 Handle：注册时只需给出类型，不必再传闭包
	actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
})
```
<!-- /example: actor/register_handler -->

**处理器注册变体：**

| 注册 | 处理器类型 | 签名 |
|------|-------------|-----------|
| `RegisterSpawn` / `RegisterQuery` / `RegisterServe` | `HandlerFunc` | `func(ctx, req, spawning) (R, error)` —— 作为参数传入 |
| `RegisterSpawnHandler` / `RegisterQueryHandler` / `RegisterServeHandler` | `RequestHandler` | `func(req) Handle(ctx, spawning) (R, error)` —— 定义在请求类型上的方法 |

**对比：**

```text
// 传统方式：处理逻辑写在闭包里
actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
    ctx.SetState(PlayerState{HP: req.InitHP})
    return actor.OK, nil
})

// RequestHandler：处理逻辑在请求类型上 —— 少一个函数参数
actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
```

两种模式完全互通 —— 你可以在同一个 `RegistryBuilder` 中混用 `RegisterSpawn` 和
`RegisterSpawnHandler`。选择依据是处理逻辑放在请求结构体上更自然，还是和其他处理器放在注册块中更自然。


### 2.1 OnSpawn 钩子 —— 首次创建时初始化

`SetOnSpawn` 注册的钩子由框架在 Actor **首次创建**（即第一条触发 spawn 的消息）时
**自动调用且只调用一次**：它运行在你的 spawn/serve 处理器**之前**，拿到同一个
`ActorContext`，适合初始化状态或申请资源。

<!-- example: actor/onspawn -->
```go
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	b.SetOnSpawn(func(ctx *actor.ActorContext[PlayerId, PlayerState]) error {
		// 首次创建时、spawn 处理器之前运行一次
		ctx.SetState(PlayerState{HP: 100})
		return nil // 返回 error 会中止创建，spawn 消息的调用方收到该错误
	})
	actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
})
```
<!-- /example: actor/onspawn -->

| 方面 | 说明 |
|------|------|
| **时机** | 仅在 `spawning == true`（Actor 原本不存在）时调用；后续消息不会再触发。 |
| **顺序** | 在用户的 spawn/serve 处理器之前运行，共享同一个 `ActorContext`。 |
| **调用时状态** | Actor 尚未活跃（idle）。可以 `SetState`；在 `OnSpawn` 内调用 `ctx.Open()` 会让 Actor 保持存活，即使 spawn 处理器本身没有 Open。 |
| **返回值** | 返回 `nil` 则继续创建并执行 spawn 处理器；返回非 nil error 则中止创建：Actor **不会**被创建，spawn 消息的调用方收到该错误，下次 spawn 时重试。 |
| **不要手动调用** | `OnSpawn` 由框架管理，永远不要自己调用。 |

> **错误传播**：`OnSpawn` 返回错误时，spawn 消息的调用方会收到它（经 `Call` / `Post` 的回复通道）。
> `Post` 在入队成功后立即返回 `nil`，因此要观察失败请用 `Call`（或检查 `actor.Count`）。

### 3. 发送消息

<!-- example: actor/send -->
```go
playerId := PlayerId{ServerId: 1, OpenId: "alice"}
ctx := context.Background()

// Post：发后不理（必要时按 spawn 语义创建 Actor）
if err := actor.Post(mgr, playerId, &Login{InitHP: 100, InitLevel: 1}); err != nil {
	slog.Error("post login failed", "err", err)
}

// Call：同步等待回复
reply, err := actor.Call(ctx, mgr, playerId, &Attack{Damage: 30})
if err != nil {
	slog.Error("call attack failed", "err", err)
}
fmt.Println(reply.RemainingHP) // 70

// 带超时的 Call
timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
defer cancel()
reply, err = actor.Call(timeoutCtx, mgr, playerId, &Attack{Damage: 10})
if err != nil {
	slog.Error("call attack with timeout failed", "err", err)
}
fmt.Println(reply.RemainingHP) // 60

// Broadcast：发给 Group 中的所有 Actor；Multicast：发给指定的一组 Actor
_, _ = actor.Broadcast(mgr, &Close{})
_, _ = actor.Multicast(mgr, []PlayerId{playerId}, &Close{})

// Count / Finalize：统计活跃数量 / 关闭 Group 中所有 Actor 并等待
n, _ := actor.Count[PlayerId](mgr)
fmt.Println(n)
actor.Finalize(mgr, &Close{})
```
<!-- /example: actor/send -->

### 4. ActorContext 方法

<!-- example: actor/context -->
```go
teammate := PlayerId{ServerId: 1, OpenId: "bob"}
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
		state := ctx.State()  // *PlayerState —— 无需类型断言
		id := ctx.Id()        // 当前 ActorId
		log := ctx.Logger()   // *slog.Logger
		base := ctx.Context() // 随 Actor 退出而取消的 context

		log.Info("attacked", "player", id, "hp", state.HP)

		// 调度延迟回调（返回定时器 ID），以及取消它
		timer := ctx.Timer("regen", time.Second, func() { state.HP++ })
		ctx.StopTimer(timer)

		// 下游调用用 ctx.Context()：Actor 退出时下游请求一并取消
		if _, err := actor.Call(base, ctx.Manager(), teammate, &Heal{Amount: 1}); err != nil {
			log.Warn("heal teammate failed", "err", err)
		}

		state.HP -= req.Damage
		return &AttackReply{RemainingHP: state.HP, Alive: state.HP > 0}, nil
	})
})
```
<!-- /example: actor/context -->

### 5. ActorRef —— 直接 Actor 引用

当两个 Actor 频繁互相发消息时（例如同组内的两个 Player 互相关注），
`ActorRef` 提供一种**绕过 Group 查找**的直接引用，将消息直接投递到目标 Actor 的邮箱。
注意：`ctx.Ref(id)` 只能引用**同一个 Group**（同一个 ActorId 类型）内的 Actor。

> **关键行为**：`ActorRef` 会对目标 Actor 持有引用计数，阻止其在空闲时退出。
> 使用完毕后请调用 `Release()`，允许目标退出。

<!-- example: actor/ref_types -->
```go
type Follow struct {
	Target PlayerId
}

func (*Follow) ReqType(_ PlayerId, _ actor.OkReply) string { return "Follow" }

// Ping 发给被引用的 Player（发后不理）。
type Ping struct {
	From PlayerId
}

func (*Ping) ReqType(_ PlayerId, _ actor.OkReply) string { return "Ping" }

// QueryHP 向被引用的 Player 查询血量（请求-回复）。
type QueryHP struct{}

type HPReply struct {
	HP int `json:"hp"`
}

func (*QueryHP) ReqType(_ PlayerId, _ *HPReply) string { return "QueryHP" }
```
<!-- /example: actor/ref_types -->

<!-- example: actor/ref -->
```go
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Follow, _ bool) (actor.OkReply, error) {
		// ctx.Ref 只在同一 Group 中查找已存在的 Actor —— 不会创建，找不到返回 nil
		ref := ctx.Ref(req.Target)
		if ref == nil {
			return nil, ErrPlayerNotFound
		}
		defer ref.Release() // 释放持有，允许目标之后空闲退出

		// RefPost：发后不理，绕过 Group 查找
		if err := actor.RefPost(ref, &Ping{From: ctx.Id()}); err != nil {
			return nil, err
		}

		// RefCall：请求-回复，绕过 Group 查找
		hp, err := actor.RefCall(ctx.Context(), ref, &QueryHP{})
		if err != nil {
			return nil, err
		}
		ctx.Logger().Info("target hp", "target", ref.Id(), "hp", hp.HP)
		return actor.OK, nil
	})
})
```
<!-- /example: actor/ref -->

**API 参考：**

| 函数 | 说明 |
|------|-------------|
| `ctx.Ref(id)` | 获取已有 Actor 的直接引用（同 ActorType）。未找到返回 `nil`。 |
| `actor.RefPost(ref, req)` | 通过 `ActorRef` 发后不理，绕过 Group 查找。 |
| `actor.RefCall(ctx, ref, req)` | 通过 `ActorRef` 请求-回复，绕过 Group 查找。 |
| `actor.RefSafeCall(ctx, ref, req)` | 通过 `ActorRef` 请求-回复，超时/取消时自动清理资源。回复必须实现 `SafeReply`。 |
| `ref.Release()` | 释放对目标 Actor 的持有（幂等）。 |
| `ref.Valid()` | 检查引用是否仍有效（未释放、目标未关闭）。 |
| `ref.Id()` | 返回目标 Actor 的 ID。 |

**性能**：`RefCall` 比标准 `Call` 约快 10%（708ns vs 787ns），因为它省去了 Group 查找
（`findHandler` → `findGroup` → `resolveActor`）。`RefPost` 的差距更明显（94ns vs ~100ns+），
因为它完全跳过了 `resolveActor`。当同一引用在多次调用中被复用时，收益更大 —— 持有成本只支付一次，
后续所有发送都直达目标邮箱。

**与标准 `Post`/`Call` 对比：**

```
标准：   Post/Call → findHandler → findGroup → resolveActor → mailbox
ActorRef: RefPost/RefCall → mailbox  （无 Group 查找，无 resolveActor）
```

### 6. SafeCall 与 SafeReply —— 资源安全的回复

当处理器返回的回复持有外部资源（数据库连接、文件句柄、网络套接字等）时，
无论调用方是否收到回复，这些资源都必须被释放。`SafeCall` 通过 `SafeReply` 接口保证这一点。

**工作原理：**

- `SafeReply[R0]` 要求 `~*R0`（指针类型）+ `Close()` 方法
- 成功时：调用方收到回复，负责调用 `Close()`
- 超时/取消时：框架自动对「孤儿」回复调用 `Close()`
- `SafeCall` / `RefSafeCall` 与 `Call` / `RefCall` 对应，但增加了 `SafeReply` 约束

<!-- example: actor/safe_reply -->
```go
// ResourceReply 持有外部资源，实现 SafeReply[ResourceReply]（~*R0 + Close()）。
type ResourceReply struct {
	Data   []byte
	handle *os.File
}

func (r *ResourceReply) Close() {
	if r.handle != nil {
		_ = r.handle.Close()
	}
}

type LoadData struct{}

func (*LoadData) ReqType(_ PlayerId, _ *ResourceReply) string { return "LoadData" }
```
<!-- /example: actor/safe_reply -->

<!-- example: actor/safe_call -->
```go
// SafeCall：成功时调用方负责 Close，超时/取消时由框架兜底 Close。
func loadData(ctx context.Context, mgr *actor.Manager, id PlayerId) error {
	resource, err := actor.SafeCall(ctx, mgr, id, &LoadData{})
	if err != nil {
		// 超时或取消时框架已经调用过 resource 的 Close()
		return err
	}
	defer resource.Close() // 成功时调用方负责清理

	fmt.Println(len(resource.Data))
	return nil
}
```
<!-- /example: actor/safe_call -->

| API | 约束 | 成功时清理 | 超时/取消时清理 |
|-----|-----------|-------------------|--------------------------|
| `Call` / `RefCall` | `PtrReply` | 调用方 N/A | 回复被丢弃（无 Close） |
| `SafeCall` / `RefSafeCall` | `SafeReply` | 调用方调用 `Close()` | 框架自动调用 `Close()` |

> **类型安全**：`SafeCall` 仅接受回复实现了 `SafeReply` 的请求。
> 对普通 `PtrReply` 类型使用 `SafeCall` 会导致编译错误：
> `*AttackReply does not implement SafeReply[*AttackReply] (missing Close method)`

### 7. Manager 生命周期

<!-- example: actor/manager -->
```go
// 优雅关闭：停止接收新消息，等待所有 Actor 退出
mgr.CloseManager()
mgr.JoinManager()

// 检查 Manager 是否已关闭
if mgr.IsClosed() {
	slog.Info("manager closed")
}

// 单 Actor 生命周期
actor.CloseActor[PlayerId](mgr, playerId) // 温和关闭：排空邮箱，完成在途请求
actor.KillActor[PlayerId](mgr, playerId)  // 强制关闭：取消 ctx，丢弃待处理
actor.JoinActor[PlayerId](mgr, playerId)  // 等待 Actor 的 goroutine 退出
```
<!-- /example: actor/manager -->

### 8. CtxValue —— 强类型 Manager context 值

`WithValue` 把强类型值挂到 Manager 根 context 上，之后本 Manager 创建的所有 Actor 都能
从 `ctx.Context()` 取到。用于把 ID 分配器、时钟等基础设施句柄送给各处理器，替代进程级单例
——同进程多 Manager 并存时各持一份，互不可见。

<!-- example: actor/ctx_value -->
```go
// 定义 per-Manager 的强类型 context 值：key 由句柄自带，写入与读取都在编译期检查类型，
// 不需要手写私有 key struct 与类型断言。
var SeqAllocValue = actor.DefineValue[*SeqAlloc]()

// SeqAlloc 是挂在 Manager 上的基础设施句柄（如 ID 分配器）。
type SeqAlloc struct{ n int }

// Next 只在所属 Manager 的 actor 内调用，因此无需加锁。
func (a *SeqAlloc) Next() int {
	a.n++
	return a.n
}
```
<!-- /example: actor/ctx_value -->

<!-- example: actor/ctx_value_use -->
```go
// 必须在任何 Serve 之前挂载：Group 创建时捕获当时的 Manager context，
// 之后挂载的值对已注册的 Group 不可见（框架直接 panic，避免静默失效）。
actor.WithValue(mgr, SeqAllocValue, &SeqAlloc{})

actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
	actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
		alloc, ok := SeqAllocValue.Get(ctx.Context()) // 编译期已知类型，无需断言
		if !ok {
			return actor.OK, errors.New("seq alloc not mounted")
		}
		ctx.Open()
		ctx.SetState(PlayerState{HP: req.InitHP, Level: alloc.Next()})
		return actor.OK, nil
	})
})
```
<!-- /example: actor/ctx_value_use -->

| API | 说明 |
|-----|------|
| `actor.DefineValue[T]()` | 定义 per-Manager 的强类型值句柄。key 由句柄自带，多次定义互不冲突。 |
| `actor.WithValue(mgr, v, value)` | 挂载值。**必须在任何 `Serve` 之前**调用，否则 panic（避免对已注册 Group 静默失效）。 |
| `v.Get(ctx)` | 以 `(T, bool)` 读取；未挂载时返回零值与 `false`。 |

## RPC

基于 WebSocket、使用 JSON 编解码的远程 Actor 通信。

### 服务端

<!-- example: rpc/server -->
```go
server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
	":8080", mgr,
	func(b *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]) {
		rpc.RegisterRequest(b, &DemoLogin{})
		rpc.RegisterRequest(b, &DemoAttack{})
		rpc.RegisterRequest(b, &DemoClose{})
	},
)
if err := server.Start(); err != nil { // 非阻塞；需要阻塞时改用 server.Run()
	slog.Error("server start failed", "err", err)
}

// 优雅关闭：等待在途请求处理完
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
if err := server.Shutdown(ctx); err != nil {
	slog.Error("server shutdown failed", "err", err)
}
```
<!-- /example: rpc/server -->

### 客户端

<!-- example: rpc/client -->
```go
// 远程 Post（发后不理）
if err := rpc.Post(client, playerId, &DemoLogin{InitHP: 100}); err != nil {
	slog.Error("post login failed", "err", err)
}

// 远程 Call
reply, err := rpc.Call(ctx, client, playerId, &DemoAttack{Damage: 30})
if err != nil {
	slog.Error("call attack failed", "err", err)
}
slog.Info("attacked", "remainingHP", reply.RemainingHP)

// 带超时的远程 Call
reply, err = rpc.CallTimeout(ctx, client, playerId, &DemoAttack{Damage: 10}, 5*time.Second)
if err != nil {
	slog.Error("call attack with timeout failed", "err", err)
}
slog.Info("attacked", "remainingHP", reply.RemainingHP)

// 远程 Broadcast
if err := rpc.Broadcast(client, &DemoClose{}); err != nil {
	slog.Error("broadcast failed", "err", err)
}
```
<!-- /example: rpc/client -->

### 线上格式

```json
// 请求
{"seq": 1, "method": "call", "actorType": "Player",
 "reqType": "Attack", "actorId": {"serverId": 1, "openId": "alice"},
 "req": {"damage": 30}}

// 响应
{"seq": 1, "reply": {"remainingHP": 70, "alive": true}}
```

## Grain —— 持久化 Actor

Grain 为 Actor 增加了**基于租约管理的持久化**能力。在首条（spawn）消息中，你显式调用
`State.Activate(ctx, pm, snap)`，并把快照器实例传给它：它获取分布式租约、加载已持久化的状态（若不存在则全新开始），并打开 Actor ——
返回数据是**loaded（已加载）**还是 **created（已创建）**。在停用时，状态被保存，租约被释放。

Actor **不会**在处理 spawn 消息前被自动激活。你需通过 `ctx.Open()`（普通 Actor）或
`ctx.State().Activate(ctx, pm, snap)`（Grain）在 spawn/serve 回调中决定何时激活。若不激活，
该 Actor 在消息处理完成后保持空闲，并在空闲时被销毁（或被下一条 spawn 消息重新激活）。

### 概念

| 概念 | 说明 |
|------|-------------|
| **PersistenceManager** | 管理 driver + 租约管理器 + 续租设置 |
| **Driver** | 加载/保存快照（JSON、YAML、Redis、MongoDB） |
| **Lease** | 确保跨节点单一所有权的分布式锁 |
| **Snapshotter** | 将业务数据与可持久化快照相互转换 |
| **Activate** | 在 spawn 回调中显式激活：返回 `ActivateCreated` / `ActivateLoaded` |

### 快速示例

<!-- example: grain/setup -->
```go
// PersistenceManager 聚合 driver + 租约；租约内建于 driver，无需单独的租约管理器。
pm := grain.NewPersistenceManager(
	grain.WithDriver(grain.NewJsonDriver("./grain_data")),
	grain.WithNodeId("node-1"),
	grain.WithAutoPersistInterval(5*time.Second),
)

actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
	// 生命周期托管给框架：OnSpawn 自动 Activate + 定时存盘，退出时自动 Persist + 释放租约。
	grain.SetupGrain(b, pm, grain.ShotSelf[PlayerData]{}, func(ctx *GrainCtx, created bool) error {
		if created { // 仅首次创建时初始化业务数据
			ctx.State().Data.HP = 100
			ctx.State().Data.Level = 1
		}
		return nil
	})

	// 业务 handler 只关心业务：不再手动 Activate / Persist / Deactivate。
	actor.RegisterSpawn(b, func(ctx *GrainCtx, req *Login, _ bool) (actor.OkReply, error) {
		ctx.State().Data.HP = req.InitHP
		return actor.OK, nil
	})

	actor.RegisterQuery(b, func(ctx *GrainCtx, req *Attack, _ bool) (*AttackReply, error) {
		ctx.State().Data.HP -= req.Damage
		return &AttackReply{RemainingHP: ctx.State().Data.HP}, nil
	})
})
```
<!-- /example: grain/setup -->

### Grain State 方法

<!-- example: grain/state -->
```go
state := ctx.State()
ctx.Logger().Info("hp", "hp", state.Data.HP) // Data：业务数据（D）

if err := state.Persist(ctx); err != nil { // 立即存盘并续租，Actor 继续运行
	return actor.OK, err
}
state.Deactivate(ctx) // 存盘 + 释放租约 + 退出
```
<!-- /example: grain/state -->

### 生命周期

```
  首条消息到达
        │
        ▼
  获取租约 ──失败──▶ 错误（另一节点持有）
        │
        ▼
  从 driver 加载快照
  （未找到则为零值）
        │
        ▼
  ┌─── 处理器运行 ──────────────────┐
  │  • Persist() —— 保存但不退出    │
  │  • Deactivate() —— 保存 + 退出  │
  │  • 自动续租（若启用）           │
  └───────────────────────────────────┘
        │
        ▼  （Deactivate）
  保存快照 → 释放租约 → 退出
```

## 集群

`cluster` 包提供跨多节点的分布式 Actor 放置与路由：

- **Membership（成员关系）**：集群成员管理（`Membership` 接口，含
  `Self` / `Members` / `Events` / `Join` / `Leave` / `Close`）。
- **Placement（放置）**：决定每个 Actor 由哪个节点持有（`PlacementStrategy`
  接口；`ConsistentHashPlacement` 和 `GroupAwarePlacement` 实现）。
  `GroupMapping` 可限制哪些节点类型承载哪些 Actor 类型，用于异构集群。
- **Routing（路由）**：`Router` 封装 Membership + Placement + `rpc.Client`
  池。它为每个 Actor 选择首选节点，并自动在本地（通过 `actor.Manager`）或
  远端（通过 `rpc.Client`）进行路由。
- **Call API**：`cluster.Post` / `cluster.Call` / `cluster.Broadcast` /
  `cluster.Multicast` 与 `actor` 包 API 对应，但透明地转发到持有节点。
- **租约重试**：`Router` 可通过 `WithLeaseRetry` / `WithForceReleaser`
  与 Grain 租约集成；遇到 `ErrLeaseTaken` 时，转发到当前持有者或在重试前强制释放租约。
- **Migration（迁移）**：`ShouldOwn(placement, members, selfID, actorType, actorId)`
  告知节点是否应当持有某个 Actor，用于驱动优雅的所有权交接（参见 `cluster_example`）。

## 共享数据（按 key 发布 / 订阅）

`shared` 包为「读远多于写、可容忍最终一致」的共享数据（世界种子、公共配置、家族科技）提供通用框架：
**owner 按 key 发布 + 读者按 key 订阅 + 冷却合并推送**，流程完全收口在框架内。

三种角色：

- **容器（Store）**：框架内置的分片 Group，项目侧不能注册 handler。它只回复请求、只用 Post 推送，
  永不主动 Call 任何 actor，因此不可能参与调用环——**结构上没有死锁**。数据纯内存，重启后由 owner 重新发布；
- **owner**：权威数据所在的业务 actor（`world` / `family` 等），数据变更时 `Publish`；
- **读者**：State 内嵌 `shared.Reader`，`Watch` 首次向容器订阅并取快照，此后由推送更新本地缓存。

<!-- example: shared/keys -->
```go
// 定义 key：值类型在编译期与 key 绑定，pattern 全局唯一，可带占位符。
var (
	WorldSeed  = shared.Define[int64]("world/%s/seed", shared.Immutable())
	FamilyTech = shared.Define[TechState]("family/%s/tech",
		shared.WithCooldown(50*time.Millisecond), shared.WithTTL(30*time.Second))
)
```
<!-- /example: shared/keys -->

<!-- example: shared/usage -->
```go
// 启动：注册容器 Group（分片数在进程内必须一致，重复 Serve 会 panic）
shared.Serve(mgr, shared.Options{Shards: 4, Cooldown: 50 * time.Millisecond})

// owner：权威数据仍在自己手里，变更时 Publish，容器只负责分发
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[WorldId, WorldState]) {
	actor.RegisterSpawn(b, func(ctx *actor.ActorContext[WorldId, WorldState], req *InitWorld, _ bool) (actor.OkReply, error) {
		ctx.Open()
		ctx.State().Seed = req.Seed
		return actor.OK, shared.Publish(ctx, WorldSeed.Of(ctx.Id()), ctx.State().Seed)
	})
})

// 读者：Group 只需注册一次 ServeReader，之后可 Watch 任意 key
actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[RegionId, RegionState]) {
	shared.ServeReader(b, func(s *RegionState) *shared.Reader { return &s.Vars })

	actor.RegisterSpawn(b, func(ctx *actor.ActorContext[RegionId, RegionState], req *EnterRegion, _ bool) (actor.OkReply, error) {
		ctx.Open()

		// 首次 Watch 订阅并取回快照，之后命中本地缓存（由容器推送更新）
		seed, err := shared.Watch(ctx, &ctx.State().Vars, WorldSeed.Of(req.World))
		if err != nil {
			return actor.OK, err
		}
		ctx.State().Seed = seed

		// 需要"必须最新"的读取用 Fetch：每次触网、不订阅、不写缓存
		latest, err := shared.Fetch(ctx, WorldSeed.Of(req.World))
		if err != nil {
			return actor.OK, err
		}
		ctx.Logger().Info("joined", "seed", latest)
		return actor.OK, nil
	})
})

// 业务侧：先由 owner 发布，再让读者订阅
_ = actor.Post(mgr, worldID, &InitWorld{Seed: 42})
_ = actor.Post(mgr, regionID, &EnterRegion{World: worldID})
```
<!-- /example: shared/usage -->

**一致性语义（最终一致）**

- **按需订阅**：首次 `Watch` 向容器注册，容器只向已注册读者推送（非广播）；
- **冷却合并**：冷却期内多次发布合并为一次推送，读者可能跳过中间版本，但最终拿到最新值；
- **自愈**：读者消失后容器在推送失败时清理其注册，重建后重新订阅；`WithTTL` 的 key 到期后 `Watch` 会重新对账；
- **强一致**：需要每次都看到最新值（如发号、校验）时用 `Fetch`，不要用 `Watch`；
- `Reader` 非线程安全，只能在所属 actor 的 run goroutine 内访问；
- 值类型 V 应是不可变的；若 owner 发布后仍会修改其内容，用 `WithClone` 在发布时克隆。

## 设计亮点

| 特性 | 说明 |
|------|-------------|
| 单线程 Actor | 每个 Actor 一个 goroutine，串行 channel 处理，无锁 |
| 多 Group | 一个 Manager 持有多个 `(ActorId, State)` 类型组合 |
| 编译期安全 | `Request[A, R]` 绑定 Id/Reply；跨 Group 错误由编译器捕获 |
| Post 约束 | 仅限 `Request[A, OkReply]`；自定义回复必须用 `Call` |
| 自动创建 | 首条消息触发 Actor 创建（RegisterSpawn / RegisterServe） |
| 显式 Open | spawn 处理器不调 `ctx.Open()`（Grain 为 `State.Activate`）时，Actor 处理完本条消息即被回收 |
| 排空 | 关闭前排空邮箱；消息不丢失 |
| 上下文超时 | `Call(ctx, ...)` 支持超时与取消 |
| 可取消定时器 | `ctx.Timer()` 返回定时器 ID，`ctx.StopTimer(id)` 取消 |
| ActorRef | Actor 间直接引用，绕过 Group 查找；Call 约快 10% |
| SafeCall / SafeReply | 资源清理有保障：超时/取消自动 Close，成功时手动 Close |
| RequestHandler | 通过 `Handle()` 方法将处理逻辑绑定在请求类型上；减少样板代码 |
| 显式 Manager | `NewManager()` 创建独立实例，无全局状态 |
| 包名别名 | 使用 `import act "github.com/lcy03406/actor-go/actor"` 避免冲突 |
| Codec 接口 | 易于替换序列化；支持 JSON、protobuf 等 |
| 优雅关闭 | `Server.Shutdown(ctx)` 等待在途请求完成 |
| 连接丢失 | `Client.Close()` 通过 `done` 通道通知所有等待中的调用 |
| 共享数据容器 | `shared` 包：按 key 发布/订阅 + 冷却合并推送，容器为无死锁的叶子 Group |

## 许可证

MIT —— 见 [LICENSE](LICENSE)。

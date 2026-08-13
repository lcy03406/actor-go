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
go run ./cmd/actor_example/   # 本地 Actor
go run ./cmd/rpc_example/    # 基于 WebSocket 的 RPC
go run ./cmd/grain_example/  # 持久化 Grain

# 运行测试
go test ./...
```

## 项目结构

```
actor-go/
├── cmd/
│   ├── actor_example/        # 本地 Actor 示例
│   ├── rpc_example/          # 基于 WebSocket 的 RPC 示例
│   ├── grain_example/        # 持久化 Grain 示例
│   ├── cluster_example/      # 集群迁移示例
│   └── engineering_example/  # 吞吐量 / 压测示例
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

```go
// 编译错误：Attack 返回 *AttackReply，不能 Post
actor.Post(mgr, id, &Attack{Damage: 10})
// → *Attack does not implement Request[PlayerId, OkReply]

// 正确：使用 Call 获取回复
reply, err := actor.Call(ctx, mgr, id, &Attack{Damage: 10})
```

## 核心 API

### 1. 定义类型

```go
import "github.com/lcy03406/actor-go/actor"

// Actor ID
type PlayerId struct {
    ServerId int    `json:"serverId"`
    OpenId   string `json:"openId"`
}
func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
    return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

// 状态
type PlayerState struct {
    HP    int `json:"hp"`
    Level int `json:"level"`
}

// 回复
type AttackReply struct {
    RemainingHP int  `json:"remainingHP"`
    Alive       bool `json:"alive"`
}

// SafeReply —— 需要资源清理的回复
// 实现 actor.SafeReply[*SafeAttackReply]（~*R0 + Close()）
type SafeAttackReply struct {
    RemainingHP int  `json:"remainingHP"`
    Alive       bool `json:"alive"`
    // 内部资源，如连接句柄、文件描述符等
}
func (r *SafeAttackReply) Close() {
    // 释放资源（例如归还到连接池、关闭文件等）
}

// 请求 —— 实现 Request[A, R]
type Login struct {
    InitHP    int `json:"initHP"`
    InitLevel int `json:"initLevel"`
}
func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
    Damage int `json:"damage"`
}
func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type Close struct{}
func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }
```

### 2. 注册处理器

```go
mgr := actor.NewManager()

actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
    // RegisterSpawn：首条消息创建 Actor（发后不理）
    actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
        ctx.Open() // 框架不再在 spawn 时自动激活，需显式 Open 保持活跃
        ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
        return actor.OK, nil
    })

    // RegisterQuery：查询已有 Actor（返回回复）
    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
        ctx.State().HP -= req.Damage
        alive := ctx.State().HP > 0
        return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
    })

    // RegisterServe：首条消息创建 Actor 且返回回复
    actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, spawning bool) (*AttackReply, error) {
        if spawning {
            ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
        }
        return &AttackReply{RemainingHP: ctx.State().HP, Alive: ctx.State().HP > 0}, nil
    })
})
```

| 注册 | Spawn（创建） | Query（已有） | 回复 |
|------|:---:|:---:|:---:|
| `RegisterSpawn` | 是 | 否 | `OkReply` |
| `RegisterQuery` | 否 | 是 | 自定义 |
| `RegisterServe` | 是 | 是 | 自定义 |

#### RequestHandler —— 自包含的请求类型

除了单独传入处理函数，请求类型也可以实现 `RequestHandler`，将处理逻辑直接绑定在请求结构体上。
这样可以把相关逻辑聚合在一起，减少样板代码 —— 无需为每个注册编写单独闭包。

```go
// RequestHandler 将 Request + Handler 组合为一个类型
type Login struct {
    InitHP    int `json:"initHP"`
    InitLevel int `json:"initLevel"`
}

// ReqType 标识请求（与 Request 接口一致）
func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

// Handle 包含处理逻辑 —— 无需单独闭包
func (req *Login) Handle(ctx *actor.ActorContext[PlayerId, PlayerState], spawning bool) (actor.OkReply, error) {
    ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
    return actor.OK, nil
}

type Attack struct {
    Damage int `json:"damage"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }
func (req *Attack) Handle(ctx *actor.ActorContext[PlayerId, PlayerState], spawning bool) (*AttackReply, error) {
    ctx.State().HP -= req.Damage
    alive := ctx.State().HP > 0
    return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
}

// 注册：仅传入类型，无需函数参数
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
    actor.RegisterSpawnHandler[PlayerId, PlayerState, *Login](b)
    actor.RegisterQueryHandler[PlayerId, PlayerState, *Attack](b)
})
```

**处理器注册变体：**

| 注册 | 处理器类型 | 签名 |
|------|-------------|-----------|
| `RegisterSpawn` / `RegisterQuery` / `RegisterServe` | `HandlerFunc` | `func(ctx, req, spawning) (R, error)` —— 作为参数传入 |
| `RegisterSpawnHandler` / `RegisterQueryHandler` / `RegisterServeHandler` | `RequestHandler` | `func(req) Handle(ctx, spawning) (R, error)` —— 定义在请求类型上的方法 |

**对比：**

```go
// 传统方式：处理逻辑写在闭包中
actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
    ctx.SetState(PlayerState{HP: req.InitHP})
    return actor.OK, nil
})

// RequestHandler：处理逻辑在请求类型上 —— 少一个函数参数
actor.RegisterSpawnHandler[PlayerId, PlayerState, *Login](b)
```

两种模式完全互通 —— 你可以在同一个 `RegistryBuilder` 中混用 `RegisterSpawn` 和
`RegisterSpawnHandler`。选择依据是处理逻辑放在请求结构体上更自然，还是和其他处理器放在注册块中更自然。

### 3. 发送消息

```go
ctx := context.Background()

// Post：发后不理（必要时自动创建）
actor.Post(mgr, playerId, &Login{InitHP: 100, InitLevel: 1})

// Call：直接返回回复
reply, err := actor.Call(ctx, mgr, playerId, &Attack{Damage: 30})
if err != nil {
    // 处理错误
}
fmt.Println(reply.RemainingHP) // 70

// 带超时的 Call
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
reply, err = actor.Call(ctx, mgr, playerId, &Attack{Damage: 10})

// SafeCall：用于需要显式资源清理的回复（例如连接句柄）
// 要求回复实现 SafeReply[R0]（~*R0 + Close()）
safeReply, err := actor.SafeCall(ctx, mgr, playerId, &Attack{Damage: 30})
if err != nil {
    // 处理错误
}
defer safeReply.Close() // 用完后清理资源
// 若 SafeCall 超时或 ctx 被取消，Close() 会被自动调用

// Broadcast：发送给 Group 中所有 Actor
count, _ := actor.Broadcast(mgr, &Close{})

// Multicast：发送给指定 Actor
hit, _ := actor.Multicast(mgr, []PlayerId{id1, id2}, &Close{})

// Count：Group 中活跃 Actor 数量
n, _ := actor.Count[PlayerId](mgr)

// Finalize：关闭 Group 中所有 Actor 并等待
actor.Finalize(mgr, &Close{})
```

### 4. ActorContext 方法

```go
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
    ctx.State()           // *PlayerState —— 无需类型断言
    ctx.SetState(...)     // 替换状态
    ctx.Id()              // 当前 ActorId
    ctx.Logger()          // *slog.Logger
    ctx.Context()         // context.Context（Actor 退出时取消）
    ctx.Quit()            // 请求退出（先排空邮箱）
    ctx.Open()            // 激活：离开空闲态（与 Quit 相反）；spawn 处理器必须调用它或 Activate 以保持 Actor 存活
    ctx.Ref(id)           // 获取另一个 Actor 的直接引用（绕过 Group 查找）
    ctx.Timer(d, fn)      // 调度延迟回调，返回定时器 ID
    ctx.StopTimer(id)     // 取消已调度的定时器
    return &AttackReply{}, nil
})
```

### 5. ActorRef —— 直接 Actor 引用

当两种 Actor 类型存在明确对应关系时（例如 Player → Room、Order → User），
`ActorRef` 提供一种**绕过 Group 查找**的直接引用，将消息直接投递到目标 Actor 的邮箱。

> **关键行为**：`ActorRef` 会对目标 Actor 持有引用计数，阻止其在空闲时退出。
> 使用完毕后请调用 `Release()`，允许目标退出。

```go
type RoomId struct {
    RoomId string `json:"roomId"`
}
func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string { return "Room(" + id.RoomId + ")" }

// 在 Player 处理器中，获取 Player 所在 Room 的直接引用：
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *JoinRoom, _ bool) (actor.OkReply, error) {
    // ctx.Ref() 在同 Group 中查找已有 Actor —— 不创建。
    roomRef := ctx.Ref(req.RoomId)
    if roomRef == nil {
        return nil, ErrRoomNotFound // 未找到：返回业务错误，ErrRoomNotFound 仅为示例
    }
    defer roomRef.Release() // 释放持有，允许 Room 之后空闲退出

    // RefPost：发后不理，绕过 Group 查找
    if err := actor.RefPost(roomRef, &AddPlayer{PlayerId: ctx.Id()}); err != nil {
        return nil, err
    }

    // RefCall：请求-回复，绕过 Group 查找
    info, err := actor.RefCall(context.Background(), roomRef, &GetRoomInfo{})
    if err != nil {
        return nil, err
    }
    return actor.OK, nil
})
```

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

```go
// 定义 SafeReply 类型
type ResourceReply struct {
    Data   []byte
    handle *os.File  // 需要清理
}

func (r *ResourceReply) Close() {
    if r.handle != nil {
        r.handle.Close()
    }
}

// 注册返回 SafeReply 的处理器
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *LoadData, _ bool) (*ResourceReply, error) {
    f, _ := os.Open("data.bin")
    data, _ := io.ReadAll(f)
    return &ResourceReply{Data: data, handle: f}, nil
})

// 使用 SafeCall —— Close() 有保障
reply, err := actor.SafeCall(ctx, mgr, id, &LoadData{})
if err != nil {
    // 若超时/取消：reply.Close() 已被自动调用
    return err
}
defer reply.Close() // 成功时调用方必须关闭
// 使用 reply.Data...
```

| API | 约束 | 成功时清理 | 超时/取消时清理 |
|-----|-----------|-------------------|--------------------------|
| `Call` / `RefCall` | `PtrReply` | 调用方 N/A | 回复被丢弃（无 Close） |
| `SafeCall` / `RefSafeCall` | `SafeReply` | 调用方调用 `Close()` | 框架自动调用 `Close()` |

> **类型安全**：`SafeCall` 仅接受回复实现了 `SafeReply` 的请求。
> 对普通 `PtrReply` 类型使用 `SafeCall` 会导致编译错误：
> `*AttackReply does not implement SafeReply[*AttackReply] (missing Close method)`

### 7. Manager 生命周期

```go
mgr := actor.NewManager()

// 优雅关闭：停止接收新消息，等待所有 Actor 退出
mgr.CloseManager()
mgr.JoinManager()

// 检查 Manager 是否已关闭
if mgr.IsClosed() {
    // ...
}

// 单 Actor 生命周期
actor.CloseActor[PlayerId](mgr, id)   // 温和关闭：排空邮箱，完成在途请求
actor.KillActor[PlayerId](mgr, id)    // 强制关闭：取消 ctx，丢弃待处理
actor.JoinActor[PlayerId](mgr, id)    // 等待 Actor 的 goroutine 退出
```

## RPC

基于 WebSocket、使用 JSON 编解码的远程 Actor 通信。

### 服务端

```go
mgr := actor.NewManager()
// ... 注册处理器 ...

server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
    ":8080", mgr,
    func(b *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]) {
        rpc.RegisterRequest(b, &Login{})
        rpc.RegisterRequest(b, &Attack{})
        rpc.RegisterRequest(b, &Close{})
    },
)
server.Start() // 非阻塞
// server.Run() // 阻塞

// 优雅关闭
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
server.Shutdown(ctx)
```

### 客户端

```go
client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]("localhost:8080")
client.Connect()
defer client.Close()

// 远程 Post（发后不理）
rpc.Post(client, playerId, &Login{InitHP: 100, InitLevel: 1})

// 远程 Call
reply, err := rpc.Call(ctx, client, playerId, &Attack{Damage: 30})

// 带超时的远程 Call
reply, err = rpc.CallTimeout(ctx, client, playerId, &Attack{Damage: 10}, 5*time.Second)

// 远程 Broadcast
rpc.Broadcast(client, &Close{})
```

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
`State.Activate(ctx, pm)`：它获取分布式租约、加载已持久化的状态（若不存在则全新开始），并打开 Actor ——
返回数据是**loaded（已加载）**还是 **created（已创建）**。在停用时，状态被保存，租约被释放。

Actor **不会**在处理 spawn 消息前被自动激活。你需通过 `ctx.Open()`（普通 Actor）或
`ctx.State().Activate(ctx, pm)`（Grain）在 spawn/serve 回调中决定何时激活。若不激活，
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

```go
import (
    "github.com/lcy03406/actor-go/actor"
    "github.com/lcy03406/actor-go/grain"
)

// 当业务数据可直接序列化时使用 ShotSelf
type PlayerData struct {
    HP    int `json:"hp"`
    Level int `json:"level"`
}

// 为可读性定义 State 类型别名
type GrainState = grain.State[PlayerId, PlayerData, PlayerData, *grain.ShotSelf[PlayerData]]

// 创建 PersistenceManager：租约内建于 driver，无需单独的租约管理器
pm := grain.NewPersistenceManager(
    grain.WithDriver(grain.NewJsonDriver("./data")),
    grain.WithNodeId("node-1"),
)

// 注册：spawn 处理器显式激活 Grain
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
    actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *Login, _ bool) (actor.OkReply, error) {
        res, err := ctx.State().Activate(ctx, pm)
        if err != nil {
            return actor.OK, err
        }
        if res == grain.ActivateCreated { // 仅首次创建时初始化
            ctx.State().Data.HP = req.InitHP
            ctx.State().Data.Level = req.InitLevel
        }
        ctx.State().Persist(ctx)  // 立即保存
        return actor.OK, nil
    })

    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *Attack, _ bool) (*AttackReply, error) {
        ctx.State().Data.HP -= req.Damage
        alive := ctx.State().Data.HP > 0
        return &AttackReply{RemainingHP: ctx.State().Data.HP, Alive: alive}, nil
    })

    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *SaveAndQuit, _ bool) (actor.OkReply, error) {
        ctx.State().Deactivate(ctx)  // 保存 + 释放租约 + 退出
        return actor.OK, nil
    })
})
```

### Grain State 方法

```go
state := ctx.State()
state.Data           // 你的业务数据（D）
state.Persist(ctx)   // 立即保存，保持运行（同时续租租约 TTL）
state.Deactivate(ctx)  // 保存 + 释放租约 + 退出
```

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

## 设计亮点

| 特性 | 说明 |
|------|-------------|
| 单线程 Actor | 每个 Actor 一个 goroutine，串行 channel 处理，无锁 |
| 多 Group | 一个 Manager 持有多个 `(ActorId, State)` 类型组合 |
| 编译期安全 | `Request[A, R]` 绑定 Id/Reply；跨 Group 错误由编译器捕获 |
| Post 约束 | 仅限 `Request[A, OkReply]`；自定义回复必须用 `Call` |
| 自动创建 | 首条消息触发 Actor 创建（RegisterSpawn / RegisterServe） |
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

## 许可证

MIT —— 见 [LICENSE](LICENSE)。

# actor-go

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/lcy03406/actor-go)](https://goreportcard.com/report/github.com/lcy03406/actor-go)

**actor-go** is a type-safe Actor Model framework for Go, with built-in RPC, distributed clustering, and persistent grain lifecycle management.

> 基于 Go 泛型的 Actor 模型框架，提供 RPC 远程调用、集群支持和持久化 Grain 生命周期管理。

## 项目结构

```
actor-go/
├── go.mod
├── actor/                  # Actor 核心包
│   ├── types.go            # ActorId + Request 接口
│   ├── actor.go            # Actor[A,S] + RegistryBuilder[A,S]
│   ├── actor_context.go    # ActorContext — handler 上下文
│   ├── group.go            # Group[A,S] 管理同类型 Actor
│   ├── manager.go          # Manager 多 Group 集合 + 泛型操作函数
│   ├── handler.go          # 消息处理器注册与分发
│   ├── invoke.go           # 泛型 Post/Call/Broadcast/Multicast
│   ├── registry_builder.go # RegistryBuilder 注册阶段
│   ├── timer.go            # Timer 可取消定时器
│   ├── close.go            # 优雅关闭（drain + in-flight 保护）
│   └── errors.go           # 错误类型
├── rpc/                    # RPC 远程通信包
│   ├── types.go            # Message/Codec/Transport 接口
│   ├── server.go           # WebSocket RPC 服务端
│   ├── client.go           # WebSocket RPC 客户端
│   ├── entry.go            # 泛型 Post/Call/Broadcast/Multicast 入口
│   ├── json.go             # JSON Codec 实现
│   └── registry.go         # RPC 请求注册表
├── grain/                  # Grain 持久化 Actor
│   ├── grain.go            # Grain 接口定义
│   ├── lifecycle.go        # 生命周期 + 租约管理
│   ├── manager.go          # GrainManager
│   ├── snapshot.go         # 快照持久化
│   ├── driver_json.go      # JSON 文件驱动
│   ├── driver_yaml.go      # YAML 文件驱动
│   ├── driver_redis.go     # Redis 驱动
│   └── driver_mongo.go     # MongoDB 驱动
├── cluster/                # 集群支持
│   ├── cluster.go          # Cluster 入口
│   ├── node.go             # 节点管理
│   ├── membership.go       # 成员发现
│   ├── placement.go        # Actor 放置策略
│   ├── route.go            # 路由
│   └── transport.go        # 节点间通信
├── lease/                  # 分布式租约
│   ├── lease.go            # Lease 接口
│   ├── local_lease.go      # 本地租约（单机）
│   ├── redis_lease.go      # Redis 租约
│   ├── mongo_lease.go      # MongoDB 租约
│   ├── sql_lease.go        # SQL 租约
│   └── retry.go            # 重试策略
├── example/
│   └── main.go             # 本地 Actor 示例
├── rpc_example/
│   └── main.go             # RPC 远程调用示例
├── LICENSE
├── CONTRIBUTING.md
├── CHANGELOG.md
├── CODE_OF_CONDUCT.md
└── SECURITY.md
```

## 快速开始

```bash
# 安装
go get github.com/lcy03406/actor-go

# 运行本地示例
cd actor-go
go run ./example/

# 运行 RPC 示例
go run ./rpc_example/

# 运行测试
go test ./...
```

## Kotlin → Go 核心概念对照

| Kotlin | Go | 说明 |
|--------|-----|------|
| `ActorIdBase` | `ActorId` 接口 | `ActorType() string` + `String() string` |
| `ActorStateBase<Id>` | 泛型 `S` | `Serve` 注册时绑定，handler 拿具体类型 |
| `ActorRequest<Id, Reply>` | `Request[A, R]` 接口 | `ReqType(A, *R) string` 实现编译期类型检查 |
| `ActorReply` | 无 | Go 不需要，类型由注册表确定 |
| `Actor<Id, State>` | `Actor[A, S]` | `State()` 返回 `*S`，无需类型断言 |
| `ActorGroup` | `Group[A, S]` | 薄封装 `rawGroup` |
| `ActorManager` (object) | `Manager` | 显式实例化，容纳多个 Group |
| `ActorRegistryBuilder` | `RegistryBuilder[A, S]` | 注册阶段与运行阶段分离 |
| `CompletableDeferred` | `chan callResult` | 单通道结构体 |
| `ActorTimer` | `Actor.Timer()` | `time.AfterFunc`，返回可取消的 `*time.Timer` |
| `RpcServer` | `Server` | gorilla/websocket + `Start()`/`Run()`/`Shutdown()` |
| `RpcClient` | `Client[A, S]` | `context` 超时 + `done` 通道通知断线 |

## 架构设计

### Manager 是多个 Group 的集合

```
                    ┌───────────────────────┐
                    │       Manager         │
                    │  (无类型参数)           │
                    └──────────┬────────────┘
                               │
              ┌────────────────┼────────────────┐
              │                │                │
    ┌─────────▼──────┐  ┌──────▼───────┐  ┌─────▼──────────┐
    │ Group[A1, S1]  │  │ Group[A2,S2] │  │ Group[A3, S3]   │
    │ (ActorId,State)│  │              │  │                 │
    └────────────────┘  └──────────────┘  └─────────────────┘
```

- 一个 `Manager` 可容纳多个 `Group`，每个 Group 对应独立的 `(ActorId, State)` 类型对
- 泛型操作通过包级函数实现（Go 方法不支持独立类型参数）
- `A` 类型由 `Request[A, R]` 约束自动推导，`S` 类型由 `Serve` 注册时推导

### 类型安全

- **Request[A, R]**：`ReqType(A, *R) string` 方法签名确保 Q 与 A、R 匹配
- **Post 约束为 `Request[A, OkReply]`**：handler 返回非 OkReply 的请求不能 Post，必须 Call
- **跨 Group 类型隔离**：不同 Group 的请求类型无法互相发送，编译器会拒绝

### 编译期能检查的错误

```go
// 错误：TestAdd 返回 TestAddReply，不能 Post（编译失败）
actor.Post(mgr, testId, &TestAdd{Add: 10})
// → *TestAdd does not implement Request[TestActorId, OkReply]

// 正确：必须用 Call 获取返回值
var reply TestAddReply
actor.Call(ctx, mgr, testId, &TestAdd{Add: 10}, &reply)
```

## 核心 API

### 包名冲突处理

Go 惯例：当 `actor` 包名与变量名冲突时，使用 import 别名：

```go
import act "github.com/lcy03406/actor-go/actor"

mgr := act.NewManager()
act.Serve(mgr, 100, func(b *act.RegistryBuilder[MyId, MyState]) { ... })
```

### 1. 定义类型

```go
import "github.com/lcy03406/actor-go/actor"

// Actor ID
type MyActorId struct {
    ServerId int    `json:"serverId"`
    OpenId   string `json:"openId"`
}
func (id MyActorId) ActorType() string { return "MyActor" }
func (id MyActorId) String() string    { return fmt.Sprintf("%d:%s", id.ServerId, id.OpenId) }

// State
type MyState struct {
    Data int
}

// 回复
type MyReply struct {
    Result int `json:"result"`
}

// 请求：实现 actor.Request[MyActorId, R] 接口
// ReqType 的参数类型确保编译期 Q/A/R 三方匹配
type MyLogin struct {
    InitData int `json:"initData"`
}
func (*MyLogin) ReqType(_ MyActorId, _ *actor.OkReply) string { return "MyLogin" }

type MyReq struct {
    Value int `json:"value"`
}
func (*MyReq) ReqType(_ MyActorId, _ *MyReply) string { return "MyReq" }

type MyClose struct{}
func (*MyClose) ReqType(_ MyActorId, _ *actor.OkReply) string { return "MyClose" }
```

### 2. 注册处理器

```go
mgr := actor.NewManager()

// 注册 Group1：MyActorId + MyState
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[MyActorId, MyState]) {
    // spawn: 首次消息创建 Actor，不等待回复
    actor.RegisterSpawn(b,
        func(a *actor.Actor[MyActorId, MyState], req *MyLogin, spawning bool) (actor.OkReply, error) {
            a.SetState(&MyState{Data: req.InitData})
            return actor.OkReply{}, nil
        })

    // request: 需要回复，handler 拿到具体类型 State
    actor.RegisterRequest(b,
        func(a *actor.Actor[MyActorId, MyState], req *MyReq) (MyReply, error) {
            a.State().Data += req.Value  // 无需类型断言
            return MyReply{Result: a.State().Data}, nil
        })

    // requestSpawn: 需要回复 + 首次消息创建 Actor
    actor.RegisterRequestSpawn(b,
        func(a *actor.Actor[MyActorId, MyState], req *MyLogin, spawning bool) (MyReply, error) {
            a.SetState(&MyState{Data: req.InitData})
            return MyReply{Result: a.State().Data}, nil
        })

    // post: fire-and-forget（RegisterPost 需要手动指定 reqType 字符串）
    actor.RegisterPost(b, "MyNotify",
        func(a *actor.Actor[MyActorId, MyState], req *MyNotify) (actor.OkReply, error) {
            a.Logger().Info("notified", "msg", req.Msg)
            return actor.OkReply{}, nil
        })
})

// 注册 Group2：AnotherId + AnotherState（同一 Manager 中）
actor.Serve(mgr, 50, func(b *actor.RegistryBuilder[AnotherId, AnotherState]) {
    // ...
})
```

### 3. 发送消息

```go
ctx := context.Background()

// Post: fire-and-forget，A 由 Request 推导
actor.Post(mgr, actorId, &MyLogin{InitData: 42})

// Call: 结果写入 reply 指针，Go 从 &reply 推导 R 类型
var reply MyReply
if err := actor.Call(ctx, mgr, actorId, &MyReq{Value: 10}, &reply); err != nil {
    // handle error
}
fmt.Println(reply.Result) // 直接使用，无需类型断言

// 带超时
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
var reply2 MyReply
actor.Call(ctx, mgr, actorId, &MyReq{Value: 10}, &reply2)

// Broadcast: 广播 fire-and-forget 消息
actor.Broadcast(mgr, &MyClose{})

// Multicast: 多播到指定 Actor
actor.Multicast(mgr, []MyActorId{id1, id2}, &MyClose{})

// Count: 查询指定 Group 的 Actor 数量（A 需显式指定）
actor.Count[MyActorId](mgr)

// Finalize: 优雅关闭指定 Group
actor.Finalize(mgr, &MyClose{})
```

### 4. Actor 内部方法

```go
actor.RegisterRequest(b,
    func(a *actor.Actor[MyActorId, MyState], req *MyReq) (MyReply, error) {
        a.State()       // *MyState，无需类型断言
        a.SetState(...) // 设置新状态
        a.Id()          // 当前 ActorId
        a.Logger()      // *slog.Logger
        a.Post(&MyNotify{Msg: "done"})  // 向自身发送消息
        a.Close()       // 关闭自身（排空 mailbox 后退出）
        a.AtClose(func() { ... })        // 注册关闭回调
        timer := a.Timer(5*time.Second, func() { ... }) // 延迟回调
        timer.Stop()    // 取消未执行的定时器
        return MyReply{}, nil
    })
```

## RPC 远程调用

### 服务端

```go
codec := rpc.NewJSONCodec()
codec.RegisterActorId("MyActor", func() actor.ActorId { return &MyActorId{} })
codec.RegisterRequest("MyReq", func() any { return &MyReq{} })
codec.RegisterReply("MyReply", func() any { return &MyReply{} })

server := rpc.NewServer(":8080", mgr.Raw(), codec)
server.Start() // 非阻塞

// 或阻塞运行
// server.Run()

// 优雅关闭
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
server.Shutdown(ctx)
```

### 客户端

```go
codec := rpc.NewJSONCodec()
codec.RegisterActorId("MyActor", func() actor.ActorId { return &MyActorId{} })
codec.RegisterRequest("MyReq", func() any { return &MyReq{} })
codec.RegisterReply("MyReply", func() any { return &MyReply{} })

client := rpc.NewClient[MyActorId, MyState]("localhost:8080", codec)
client.Connect()

// 远程 Post（fire-and-forget）
client.Post(actorId, &MyNotify{Msg: "hello"})

// 远程 Call：结果写入 reply 指针
var reply MyReply
if err := rpc.Call(ctx, client, actorId, &MyReq{Value: 10}, &reply); err != nil {
    // handle error
}

// 远程 Call 快捷超时
var reply2 MyReply
rpc.CallTimeout(client, actorId, &MyReq{Value: 10}, &reply2, 5*time.Second)

// 关闭（通知所有 pending call）
client.Close()
```

### RPC 消息格式

```json
// 请求
{"id": "1", "method": "call", "actorType": "MyActor",
 "actorId": {"serverId": 1, "openId": "player_1"},
 "reqType": "MyReq", "req": {"value": 10}}

// 响应（replyType 用于客户端解码）
{"id": "1", "replyType": "MyReply", "reply": {"result": 20}}
```

## 设计要点

| 特性 | 说明 |
|------|------|
| 单线程 Actor | 每个 Actor 独立 goroutine，channel 串行处理，无需加锁 |
| 多 Group | 一个 `Manager` 容纳多个 `(ActorId, State)` 类型对，独立计数、独立操作 |
| 类型安全 | `Request[A, R]` 绑定 Id/Reply，编译期检查跨 Group 类型错误 |
| Post 约束 | 只接受 `Request[A, OkReply]`，有返回值的请求必须用 Call |
| spawn 模式 | 首次消息触发 Actor 创建，适合 Login |
| request 模式 | `Call` 阻塞等待回复，`Post` 是 fire-and-forget |
| drain 排空 | 关闭 mailbox 后 `for range` 自动排空缓冲消息，不丢消息 |
| context 超时 | `Call(ctx, ...)` 支持超时和取消 |
| 可取消 Timer | `Actor.Timer()` 返回 `*time.Timer`，可 `Stop()` |
| Manager 显式实例 | `NewManager()` 创建，无隐式全局状态 |
| 包名冲突 | 用 import 别名，如 `import act "github.com/lcy03406/actor-go/actor"` |
| Codec 接口 | 便于测试 mock 和替换序列化实现 |
| 优雅关闭 | `Server.Shutdown(ctx)` 等待现有请求完成 |
| 断线通知 | `Client.Close()` 通过 `done` 通道通知所有 pending call |
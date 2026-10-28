# actor-go

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/lcy03406/actor-go)](https://goreportcard.com/report/github.com/lcy03406/actor-go)

**actor-go** is a type-safe Actor Model framework for Go, featuring built-in RPC, distributed clustering, and persistent grain lifecycle management.

> 基于 Go 泛型的类型安全 Actor 模型框架，提供 RPC 远程调用、分布式集群和持久化 Grain 生命周期管理。

中文文档：[README.zh.md](README.zh.md)

## Quick Start

```bash
# Install
go get github.com/lcy03406/actor-go

# Run examples
go run ./cmd/actor_example/    # local Actor: spawn/Call/Post, OnSpawn, Timer, CtxValue, ActorRef
go run ./cmd/rpc_example/      # RPC over WebSocket
go run ./cmd/grain_example/    # persistent Grain (activate / persist / reload)
go run ./cmd/shared_example/   # shared data container: publish → push → reader cache

# Run tests
go test ./...

# Check that the README examples match the code (run after changing an example)
go run ./cmd/readmegen -check
```

## Project Structure

```
actor-go/
├── cmd/
│   ├── actor_example/        # local Actor example
│   ├── rpc_example/          # RPC over WebSocket example
│   ├── grain_example/        # persistent Grain example
│   ├── shared_example/       # shared data container example
│   ├── cluster_example/      # cluster migration example
│   ├── engineering_example/  # throughput / stress examples
│   └── readmegen/            # injects example_test.go regions into the READMEs
├── actor/                    # Actor core
│   ├── types.go              # ActorId, Request interfaces
│   ├── actor.go              # actorRuntime — single-threaded event loop
│   ├── actor_context.go      # ActorContext — handler context
│   ├── group.go              # Group[A,S] — typed Actor pool
│   ├── manager.go            # Manager — multi-Group container
│   ├── handler.go            # handler dispatch
│   ├── invoke.go             # Post/Call/Broadcast/Multicast
│   ├── ref.go                # ActorRef — direct Actor references (bypass Group lookup)
│   ├── registry_builder.go   # RegisterSpawn / RegisterQuery / RegisterServe
│   ├── timer.go              # cancellable Timer
│   ├── close.go              # graceful close (drain + in-flight)
│   └── errors.go             # error types
├── rpc/                      # RPC over WebSocket
│   ├── types.go              # Message, Codec, Transport interfaces
│   ├── server.go             # WebSocket server
│   ├── client.go             # WebSocket client
│   ├── entry.go              # Post/Call/Broadcast/Multicast adapters
│   ├── json.go               # JSON codec + transport
│   └── registry.go           # RPC request registry
├── grain/                    # persistent Grain Actor
│   ├── lifecycle.go          # Activate (lease + load, returns loaded/created)
│   ├── manager.go            # PersistenceManager
│   ├── snapshot.go           # Snapshotter interface + ShotSelf
│   ├── driver_json.go        # JSON file driver
│   ├── driver_yaml.go        # YAML file driver
│   ├── driver_redis.go       # Redis driver
│   └── driver_mongo.go       # MongoDB driver
├── cluster/                  # distributed clustering
│   ├── cluster.go            # Router + lease retry routing
│   ├── node.go               # node types (Node, NodeSet, MemberEvent)
│   ├── membership.go         # Membership interface + events
│   ├── placement.go          # PlacementStrategy (consistent hash, group aware)
│   └── migration.go          # ownership migration (ShouldOwn)
├── shared/                   # shared data container (owner publishes by key + readers subscribe by key)
│   ├── key.go                # Define / Def[V] / Key[V] / KeyOption — key definitions bound to a value type
│   ├── store.go              # Serve / storeId / sharded container Group and pub-sub handlers
│   ├── owner.go              # Publish / PublishWait / Unpublish — owner side
│   └── reader.go             # ServeReader / Reader / Watch / Fetch — reader side
├── LICENSE
├── CONTRIBUTING.md
├── CHANGELOG.md
├── CODE_OF_CONDUCT.md
└── SECURITY.md
```

## Architecture

```
                     ┌───────────────────────┐
                     │       Manager         │
                     │  (non-generic)        │
                     └──────────┬────────────┘
                                │
               ┌────────────────┼────────────────┐
               │                │                │
     ┌─────────▼──────┐  ┌──────▼───────┐  ┌─────▼──────────┐
     │ Group[A1, S1]  │  │ Group[A2,S2] │  │ Group[A3, S3]  │
     │ (ActorId,State)│  │              │  │                │
     └────────────────┘  └──────────────┘  └────────────────┘
```

- A **Manager** holds multiple **Groups**, each for a distinct `(ActorId, State)` type pair.
- Each **Actor** runs in its own goroutine with a serialized mailbox — no locks needed.
- Generic operations are package-level functions (Go methods cannot have independent type parameters).
- `A` is inferred from `Request[A, R]`; `S` is inferred from `Serve` registration.

### Serial Design (Single-Threaded Event Loop)

Each Actor is a **single-threaded state machine**. All messages addressed to the same
Actor are processed strictly one-at-a-time in the order they arrive — there is never
concurrent access to an Actor's state, so handlers need no locks or mutexes.

```
         multiple senders (goroutines)
                │  Post / Call / RefPost / RefCall
                ▼
        ┌───────────────────────┐
        │   buffered channel     │   ← mailbox (FIFO)
        │   (mailbox chan)       │
        └───────────┬───────────┘
                    │  ← one message at a time, dequeued by run()
                    ▼
        ┌───────────────────────┐
        │   actorRuntime.run()   │   ← the ONLY goroutine touching this
        │   (event loop)         │      Actor's state & context
        │  for msg in mailbox:   │
        │    invoke handler      │
        └───────────┬───────────┐
                    │
                    ▼
            handler mutates ctx.State()  (no locking required)
```

**How it works**

1. **One goroutine per Actor.** `resolveActor` spawns the Actor with `go actor.run()`
   exactly once. From then on, that single goroutine owns the Actor's `State` and
   `ActorContext` for its entire lifetime — no other goroutine ever touches them.
2. **Serial mailbox.** Senders do not call the handler directly. They push an
   `invokable` onto the Actor's buffered `mailbox` channel (`send`). The `run` loop
   pops messages in FIFO order and invokes handlers in the same sequence.
3. **Batched, in-order execution.** `run` pumps all currently-available messages
   into a batch and invokes them sequentially (`invokeBatch`), preserving order.
   Each handler completes fully before the next begins.
4. **No shared-state races.** Because state mutation happens only inside the
   `run` goroutine, `ctx.State()` reads/writes are race-free by construction —
   you do not need `sync.Mutex` inside handlers.
5. **Concurrency happens *between* Actors, not *within* one.** Different Actors
   run in different goroutines and may execute handlers in parallel; the guarantee
   is that a *single* Actor never processes two messages at once.

**Implications for handler authors**

- You may freely mutate `ctx.State()` and call `ctx.SetState(...)` without locks.
- A slow or blocking handler delays only that Actor's own mailbox — not other Actors.
  Offload long work (I/O, compute) via `ctx.Timer`, `ctx.Ref`, or by replying
  early, so the event loop stays responsive.
- Cross-Actor ordering is not guaranteed: if `A` then `B` both message `C`, the
  order they reach `C`'s mailbox depends on scheduling. Model causal ordering
  explicitly (e.g. `C` replies to `A`, then `A` messages `B`) when required.

> This is the core of the Actor Model: isolation through serialization. The
> framework enforces it structurally, so correctness does not depend on the
> handler author remembering to lock.

#### ⚠️ Avoiding Deadlocks (critical)

Because a message handler runs *inside* the Actor's `run` loop and that same loop
is the **only** thing that can process this Actor's mailbox, a handler must never
*block waiting on its own mailbox*. The most common deadlock is calling
`Call` / `RefCall` **on the Actor itself** (directly, or indirectly through a chain
of Actors that loops back to the caller):

```
handler for Actor X:
    Call(X, &SomeReq{})   // pushes reply msg to X's mailbox, then blocks…
                          // but X's mailbox is stuck behind the running handler
                          // → the reply can never be processed → DEADLOCK
```

**Rules to avoid deadlocks**

1. **Never `Call`/`RefCall` yourself.** Do not invoke a request handler on the
   same Actor that is currently running the caller. Use `Post` (fire-and-forget)
   plus an explicit reply message, or restructure the logic into a single handler
   that mutates `ctx.State()` directly.
2. **Beware cyclic `Call` chains.** If `A` calls `B`, and `B`'s handler calls
   `C`, and `C` calls back `A` (or any cycle), all those Actors block on each
   other's mailboxes and deadlock. Prefer `Post` for intra-Actor-graph
   notifications so no participant blocks.
3. **`Call`/`RefCall` must have a context deadline.** Always pass a `context` with
   a timeout — `Call(ctx, …)` / `RefCall(ctx, …)`. Without a deadline, a crash or
   slow downstream Actor turns a logic error into an indefinite hang rather than a
   bounded error. A deadline returns promptly; it does not "fix" the deadlock but
   prevents the caller goroutine from being stuck forever.
4. **Don't block the event loop for long.** A handler that sleeps, does heavy
   synchronous I/O, or waits on an external channel holds the Actor's single
   goroutine, stalling *all* of that Actor's pending messages. Offload such work
   via `ctx.Timer`, a separate goroutine that reports back with `Post`, or by
   replying early.
5. **`Post` is always safe.** `Post`/`RefPost` never block the caller and never
   wait on a mailbox, so they cannot deadlock the sender. Reach for `Post` first;
   use `Call` only for genuine request/reply where the target is a *different*,
   independently-live Actor and a deadline is set.

> Summary: deadlocks arise the moment a handler blocks on the *same* Actor's
> mailbox (or on a cycle of mailboxes). Keep handlers non-blocking, prefer `Post`,
> set deadlines on every `Call`, and never call yourself.

### Type Safety

- **`Request[A, R]`**: `ReqType(A, *R) string` ensures compile-time `A`/`Q`/`R` match.
- **Post constraint**: only `Request[A, OkReply]` can be used with `Post`; requests with custom replies must use `Call`.
- **Cross-Group isolation**: requests for one Group cannot be sent to another — the compiler rejects it.

```text
// Compile error: Attack returns *AttackReply, cannot Post
actor.Post(mgr, id, &Attack{Damage: 10})
// -> *Attack does not implement Request[PlayerId, OkReply]

// Correct: use Call to get the reply
reply, err := actor.Call(ctx, mgr, id, &Attack{Damage: 10})
```

## Core API

### 1. Define Types

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

### 2. Register Handlers

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

| Register | Spawn (create) | Query (existing) | Reply |
|----------|:---:|:---:|:---:|
| `RegisterSpawn` | yes | no | `OkReply` |
| `RegisterQuery` | no | yes | custom |
| `RegisterServe` | yes | yes | custom |

#### RequestHandler — Self-Contained Request Types

As an alternative to passing handler functions separately, request types can implement
`RequestHandler` to bundle the handler logic directly on the request struct.
This keeps related logic together and reduces boilerplate — no need to write a
separate closure for each registration.

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

**Handler registration variants:**

| Register | Handler type | Signature |
|----------|-------------|-----------|
| `RegisterSpawn` / `RegisterQuery` / `RegisterServe` | `HandlerFunc` | `func(ctx, req, spawning) (R, error)` — passed as argument |
| `RegisterSpawnHandler` / `RegisterQueryHandler` / `RegisterServeHandler` | `RequestHandler` | `func(req) Handle(ctx, spawning) (R, error)` — method on request type |

**Comparison:**

```text
// Traditional: handler logic in the closure
actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
    ctx.SetState(PlayerState{HP: req.InitHP})
    return actor.OK, nil
})

// RequestHandler: handler logic lives on the request type - one less function parameter
actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
```

Both patterns are fully interoperable — you can mix `RegisterSpawn` and
`RegisterSpawnHandler` within the same `RegistryBuilder`. Choose based on
whether the handler logic feels more natural on the request struct itself
or alongside other handlers in the registration block.

### 2.1 OnSpawn Hook — Initialization on First Creation

`SetOnSpawn` registers a hook that the framework calls **automatically, exactly once**,
the first time an Actor is created (i.e. on the first — spawn — message that finds no
existing Actor). It runs **before** your spawn/serve handler, on the same
`ActorContext`, and is the ideal place to initialize state or acquire resources.

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

**Behavior & contract:**

| Aspect | Detail |
|--------|--------|
| **When** | Only when `spawning == true` (Actor did not exist). Subsequent messages to the same Actor never call it again. |
| **Order** | Runs *before* the user's spawn/serve handler, sharing the same `ActorContext`. |
| **State at call** | The Actor is not yet active (idle). You may `SetState`, and calling `ctx.Open()` / `ctx.State().Activate(...)` inside `OnSpawn` keeps the Actor alive even if the spawn handler itself does not open it. |
| **Return value** | `nil` → creation proceeds and the spawn handler runs. Non-`nil` error → the creation is aborted: the Actor is **not** created, the spawn message's caller receives that error, and `OnSpawn` will be retried on the next spawn attempt. |
| **Not manual** | `OnSpawn` is framework-managed — never call it yourself. |

> **Note on error propagation:** when `OnSpawn` returns an error, the caller of the
> spawn message receives it (via `Call`/`Post`'s reply channel). `Post` returns `nil`
> immediately upon enqueueing, so use `Call` (or check `actor.Count`) to observe the
> failure.

### 3. Send Messages

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

### 4. ActorContext Methods

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

### 5. ActorRef — Direct Actor References

When two Actors of the same group talk to each other frequently (e.g. two Players),
`ActorRef` provides a direct reference that **bypasses Group lookup**, delivering
messages straight to the target Actor's mailbox.
Note: `ctx.Ref(id)` can only reference Actors of the **same Group** (same ActorId type).

> **Key behavior**: `ActorRef` holds a reference count on the target Actor, preventing
> it from exiting while idle. Call `Release()` when done to allow the target to exit.

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

**API Reference:**

| Function | Description |
|----------|-------------|
| `ctx.Ref(id)` | Get a direct reference to an existing Actor (same ActorType). Returns `nil` if not found. |
| `actor.RefPost(ref, req)` | Fire-and-forget message via `ActorRef`, bypassing Group lookup. |
| `actor.RefCall(ctx, ref, req)` | Request-reply via `ActorRef`, bypassing Group lookup. |
| `actor.RefSafeCall(ctx, ref, req)` | Request-reply via `ActorRef` with automatic resource cleanup on timeout/cancel. Reply must implement `SafeReply`. |
| `ref.Release()` | Release the hold on the target Actor (idempotent). |
| `ref.Valid()` | Check if the reference is still valid (not released, target not closed). |
| `ref.Id()` | Return the target Actor's ID. |

**Performance:** `RefCall` is ~10% faster than standard `Call` (708ns vs 787ns) by
avoiding Group lookup (`findHandler` → `findGroup` → `resolveActor`). `RefPost` is
even more pronounced (94ns vs ~100ns+) since it skips `resolveActor` entirely.
The benefit grows when the same reference is reused across many calls — the hold
is paid once, and all subsequent sends go directly to the target's mailbox.

**Comparison with standard `Post`/`Call`:**

```
Standard:   Post/Call → findHandler → findGroup → resolveActor → mailbox
ActorRef:   RefPost/RefCall → mailbox  (no Group lookup, no resolveActor)
```

### 6. SafeCall & SafeReply — Resource-Safe Replies

When a handler returns a reply that holds external resources (database connections,
file handles, network sockets, etc.), those resources must be released regardless of
whether the caller receives the reply. `SafeCall` guarantees this through the
`SafeReply` interface.

**How it works:**

- `SafeReply[R0]` requires `~*R0` (pointer type) + `Close()` method
- On success: caller receives the reply and is responsible for calling `Close()`
- On timeout/cancel: the framework automatically calls `Close()` on the orphaned reply
- `SafeCall` / `RefSafeCall` mirrors `Call` / `RefCall` but with `SafeReply` constraint

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

| API | Constraint | Cleanup on success | Cleanup on timeout/cancel |
|-----|-----------|-------------------|--------------------------|
| `Call` / `RefCall` | `PtrReply` | Caller N/A | Reply discarded (no Close) |
| `SafeCall` / `RefSafeCall` | `SafeReply` | Caller calls `Close()` | Framework calls `Close()` automatically |

> **Type safety**: `SafeCall` only accepts requests whose reply implements `SafeReply`.
> Attempting `SafeCall` with a plain `PtrReply` type results in a compile error:
> `*AttackReply does not implement SafeReply[*AttackReply] (missing Close method)`

### 7. Manager Lifecycle

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

### 8. CtxValue - Typed Manager Context Values

`WithValue` mounts a typed value on the Manager's root context; every Actor created by
that Manager can read it from `ctx.Context()`. Use it to deliver per-Manager
infrastructure handles (ID allocators, clocks) to handlers instead of process-wide
singletons - two Managers in one process each get their own instance.

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

| API | Description |
|-----|-------------|
| `actor.DefineValue[T]()` | Define a per-Manager typed value handle. The key lives in the handle, so multiple definitions never collide. |
| `actor.WithValue(mgr, v, value)` | Mount the value. **Must be called before any `Serve`**, otherwise it panics (so a value can never be silently invisible to registered Groups). |
| `v.Get(ctx)` | Read the value as `(T, bool)`; returns the zero value and `false` when not mounted. |

## RPC

Remote Actor communication over WebSocket with JSON codec.

### Server

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

### Client

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

### Wire Format

```json
// Request
{"seq": 1, "method": "call", "actorType": "Player",
 "reqType": "Attack", "actorId": {"serverId": 1, "openId": "alice"},
 "req": {"damage": 30}}

// Response
{"seq": 1, "reply": {"remainingHP": 70, "alive": true}}
```

## Grain — Persistent Actor

Grain adds **lease-managed persistence** to Actors. On the first (spawn) message you explicitly call `State.Activate(ctx, pm)`: it acquires a distributed lease, loads persisted state (or starts fresh if none exists), and opens the Actor — returning whether the data was **loaded** or **created**. On deactivation, state is saved and the lease is released.

Actors are **not** activated automatically before handling the spawn message. You decide when to activate by calling `ctx.Open()` (plain Actors) or `ctx.State().Activate(ctx, pm)` (Grain), inside the spawn/serve callback. If you don't activate, the Actor stays idle after the message and is destroyed on idle (or re-activated by the next spawn message).

### Concepts

| Concept | Description |
|---------|-------------|
| **PersistenceManager** | Manages driver + lease manager + renewal settings |
| **Driver** | Loads/saves snapshots (JSON, YAML, Redis, MongoDB) |
| **Lease** | Distributed lock ensuring single-ownership across nodes |
| **Snapshotter** | Converts business data to/from persistable snapshots |
| **Activate** | Explicit activation in spawn callback: returns `ActivateCreated` / `ActivateLoaded` |

### Quick Example

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
	grain.SetupGrain(b, pm, func(ctx *GrainCtx, created bool) error {
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

### Grain State Methods

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

### Lifecycle

```
  first message arrives
        │
        ▼
  acquire lease ──fail──▶ error (another node owns it)
        │
        ▼
  load snapshot from driver
  (zero-value if not found)
        │
        ▼
  ┌─── handler runs ──────────────────┐
  │  • Persist() — save without quit  │
  │  • Deactivate() — save + quit     │
  │  • auto-renew lease (if enabled)  │
  └───────────────────────────────────┘
        │
        ▼  (Deactivate)
  save snapshot → release lease → quit
```

## Cluster

The `cluster` package provides distributed Actor placement and routing
across multiple nodes:

- **Membership**: cluster member management (`Membership` interface with
  `Self` / `Members` / `Events` / `Join` / `Leave` / `Close`).
- **Placement**: decides which node owns each Actor (`PlacementStrategy`
  interface; `ConsistentHashPlacement` and `GroupAwarePlacement`
  implementations). A `GroupMapping` restricts which node types host which
  Actor types for heterogeneous clusters.
- **Routing**: `Router` wraps Membership + Placement + an `rpc.Client`
  pool. It picks the preferred node per Actor and routes locally (via
  `actor.Manager`) or remotely (via `rpc.Client`) automatically.
- **Call API**: `cluster.Post` / `cluster.Call` / `cluster.Broadcast` /
  `cluster.Multicast` mirror the `actor` package APIs but transparently
  forward to the owner node.
- **Lease retry**: `Router` optionally integrates with Grain leases via
  `WithLeaseRetry` / `WithForceReleaser`; on `ErrLeaseTaken` it forwards to
  the current owner or force-releases the lease before retrying.
- **Migration**: `ShouldOwn(placement, members, selfID, actorType, actorId)`
  tells a node whether it should own a given Actor, used to drive graceful
  ownership hand-off (see `cluster_example`).

## Shared Data (Publish / Subscribe by Key)

The `shared` package is a general framework for read-heavy, eventually-consistent
shared data (world seed, public config, family tech). It uses
**owner publishes by key + readers subscribe by key + cooldown coalescing**, with the
whole flow managed inside the framework.

Three roles:

- **Store**: a built-in sharded Group; projects cannot register handlers on it. It only
  replies to requests and pushes via Post — it never Calls any actor, so it can never
  take part in a call cycle (**deadlock-free by construction**). Data is in-memory;
  owners re-publish after a restart;
- **Owner**: the business actor that owns the authoritative data (`world` / `family`),
  calling `Publish` when data changes;
- **Reader**: embeds `shared.Reader` in its State; `Watch` subscribes on first use and
  then serves from the local cache updated by pushes.

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

**Consistency semantics (eventual)**

- **On-demand subscription**: the first `Watch` registers with the container, which
  pushes only to registered readers (no broadcast);
- **Cooldown coalescing**: multiple publishes within the cooldown window collapse into
  one push; a reader may skip intermediate versions but always converges to the latest;
- **Self-healing**: the container prunes readers whose push failed; a reborn reader
  re-subscribes on its next `Watch`; keys defined with `WithTTL` are re-fetched after
  the TTL expires (reconciliation);
- **Strongly consistent reads**: use `Fetch` when every read must see the latest value
  (e.g. ID allocation, validation) — `Watch` is the read-heavy path;
- `Reader` is not thread-safe; access it only from the owning actor's run goroutine;
- V should be immutable; if the owner keeps mutating a published value, use
  `WithClone` to clone at publish time;
- Cross-node distribution (`rpc` / `cluster`) is a natural extension point.

## Design Highlights

| Feature | Description |
|---------|-------------|
| Single-threaded Actor | One goroutine per Actor, serialized channel processing, no locks |
| Multi-Group | One Manager holds multiple `(ActorId, State)` type pairs |
| Compile-time safety | `Request[A, R]` binds Id/Reply; cross-Group errors caught by compiler |
| Post constraint | `Request[A, OkReply]` only; custom replies must use `Call` |
| Auto-spawn | First message triggers Actor creation (RegisterSpawn / RegisterServe) |
| Explicit Open | A spawned Actor idles out unless the handler calls `ctx.Open()` (or `State.Activate` for Grain) |
| Drain | Mailbox is drained before close; no messages lost |
| Context timeout | `Call(ctx, ...)` supports timeout and cancellation |
| Cancellable Timer | `ctx.Timer()` returns timer ID, `ctx.StopTimer(id)` cancels |
| ActorRef | Direct Actor-to-Actor references bypass Group lookup; ~10% faster Call |
| SafeCall / SafeReply | Guaranteed resource cleanup: auto-Close on timeout/cancel, manual Close on success |
| RequestHandler | Handler logic bundled on request type via `Handle()` method; reduces boilerplate |
| Explicit Manager | `NewManager()` creates independent instances, no global state |
| Package name alias | Use `import act "github.com/lcy03406/actor-go/actor"` to avoid conflicts |
| Codec interface | Easy to swap serialization; supports JSON, protobuf, etc. |
| Graceful shutdown | `Server.Shutdown(ctx)` waits for in-flight requests |
| Connection loss | `Client.Close()` notifies all pending calls via `done` channel |
| Shared data container | `shared` package: publish/subscribe by key + cooldown push, deadlock-free leaf Group |

## License

MIT — see [LICENSE](LICENSE).
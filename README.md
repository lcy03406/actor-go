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
go run ./cmd/actor_example/   # local Actor
go run ./cmd/rpc_example/    # RPC over WebSocket
go run ./cmd/grain_example/  # persistent Grain

# Run tests
go test ./...
```

## Project Structure

```
actor-go/
├── cmd/
│   ├── actor_example/        # local Actor example
│   ├── rpc_example/          # RPC over WebSocket example
│   ├── grain_example/        # persistent Grain example
│   ├── cluster_example/      # cluster migration example
│   └── engineering_example/  # throughput / stress examples
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

```go
// Compile error: Attack returns *AttackReply, cannot Post
actor.Post(mgr, id, &Attack{Damage: 10})
// → *Attack does not implement Request[PlayerId, OkReply]

// Correct: use Call to get the reply
reply, err := actor.Call(ctx, mgr, id, &Attack{Damage: 10})
```

## Core API

### 1. Define Types

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

// State
type PlayerState struct {
    HP    int `json:"hp"`
    Level int `json:"level"`
}

// Reply
type AttackReply struct {
    RemainingHP int  `json:"remainingHP"`
    Alive       bool `json:"alive"`
}

// SafeReply — reply that requires resource cleanup
// Implements actor.SafeReply[*SafeAttackReply] (~*R0 + Close())
type SafeAttackReply struct {
    RemainingHP int  `json:"remainingHP"`
    Alive       bool `json:"alive"`
    // internal resources like connection handles, file descriptors, etc.
}
func (r *SafeAttackReply) Close() {
    // release resources (e.g. return to connection pool, close file, etc.)
}

// Requests — implement Request[A, R]
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

### 2. Register Handlers

```go
mgr := actor.NewManager()

actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
    // RegisterSpawn: first message creates the Actor (fire-and-forget)
    actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
        ctx.Open() // 框架不再在 spawn 时自动激活，需显式 Open 保持活跃
        ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
        return actor.OK, nil
    })

    // RegisterQuery: query an existing Actor (returns reply)
    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
        ctx.State().HP -= req.Damage
        alive := ctx.State().HP > 0
        return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
    })

    // RegisterServe: first message creates the Actor AND returns reply
    actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, spawning bool) (*AttackReply, error) {
        if spawning {
            ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
        }
        return &AttackReply{RemainingHP: ctx.State().HP, Alive: ctx.State().HP > 0}, nil
    })
})
```

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

```go
// RequestHandler combines Request + Handler into one type
type Login struct {
    InitHP    int `json:"initHP"`
    InitLevel int `json:"initLevel"`
}

// ReqType identifies the request (same as Request interface)
func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

// Handle contains the handler logic — no separate closure needed
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

// Registration: pass only the type, no function argument
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
    actor.RegisterSpawnHandler[PlayerId, PlayerState, *Login](b)
    actor.RegisterQueryHandler[PlayerId, PlayerState, *Attack](b)
})
```

**Handler registration variants:**

| Register | Handler type | Signature |
|----------|-------------|-----------|
| `RegisterSpawn` / `RegisterQuery` / `RegisterServe` | `handlerFunc` | `func(ctx, req, spawning) (R, error)` — passed as argument |
| `RegisterSpawnHandler` / `RegisterQueryHandler` / `RegisterServeHandler` | `RequestHandler` | `func(req) Handle(ctx, spawning) (R, error)` — method on request type |

**Comparison:**

```go
// Traditional: handler logic in closure
actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
    ctx.SetState(PlayerState{HP: req.InitHP})
    return actor.OK, nil
})

// RequestHandler: handler logic on request type — one less function parameter
actor.RegisterSpawnHandler[PlayerId, PlayerState, *Login](b)
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

```go
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
    b.SetOnSpawn(func(ctx *actor.ActorContext[PlayerId, PlayerState]) error {
        // runs once, before the spawn handler
        ctx.SetState(PlayerState{Health: 100})
        return nil // return an error to abort creation
    })
    actor.RegisterSpawnHandler[PlayerId, PlayerState, *Login](b)
})
```

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

```go
ctx := context.Background()

// Post: fire-and-forget (spawns if needed)
actor.Post(mgr, playerId, &Login{InitHP: 100, InitLevel: 1})

// Call: returns reply directly
reply, err := actor.Call(ctx, mgr, playerId, &Attack{Damage: 30})
if err != nil {
    // handle error
}
fmt.Println(reply.RemainingHP) // 70

// Call with timeout
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
reply, err = actor.Call(ctx, mgr, playerId, &Attack{Damage: 10})

// SafeCall: for replies that need explicit resource cleanup (e.g. connection handles)
// Requires reply to implement SafeReply[R0] (~*R0 + Close())
safeReply, err := actor.SafeCall(ctx, mgr, playerId, &Attack{Damage: 30})
if err != nil {
    // handle error
}
defer safeReply.Close() // cleanup resources when done
// If SafeCall times out or ctx is cancelled, Close() is called automatically

// Broadcast: send to all Actors in the Group
count, _ := actor.Broadcast(mgr, &Close{})

// Multicast: send to specific Actors
hit, _ := actor.Multicast(mgr, []PlayerId{id1, id2}, &Close{})

// Count: number of active Actors in a Group
n, _ := actor.Count[PlayerId](mgr)

// Finalize: close all Actors in a Group and wait
actor.Finalize(mgr, &Close{})
```

### 4. ActorContext Methods

```go
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
    ctx.State()           // *PlayerState — no type assertion needed
    ctx.SetState(...)     // replace state
    ctx.Id()              // current ActorId
    ctx.Logger()          // *slog.Logger
    ctx.Context()         // context.Context (cancelled on Actor exit)
    ctx.Quit()            // request exit (drain mailbox first)
    ctx.Open()            // activate: leave idle state (opposite of Quit); spawn handlers must call this or Activate to keep the Actor alive
    ctx.Ref(id)           // get a direct reference to another Actor (bypasses Group lookup)
    ctx.Timer(d, fn)      // schedule delayed callback, returns timer ID
    ctx.StopTimer(id)     // cancel a scheduled timer
    return &AttackReply{}, nil
})
```

### 5. ActorRef — Direct Actor References

When two Actor types have a clear correspondence (e.g. Player → Room, Order → User),
`ActorRef` provides a direct reference that **bypasses Group lookup**, delivering
messages straight to the target Actor's mailbox.

> **Key behavior**: `ActorRef` holds a reference count on the target Actor, preventing
> it from exiting while idle. Call `Release()` when done to allow the target to exit.

```go
type RoomId struct {
    RoomId string `json:"roomId"`
}
func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string { return "Room(" + id.RoomId + ")" }

// In a Player handler, get a direct reference to the Player's Room:
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *JoinRoom, _ bool) (actor.OkReply, error) {
    // ctx.Ref() looks up an existing Actor in the same Group — no spawn.
    roomRef := ctx.Ref(req.RoomId)
    if roomRef == nil {
        return nil, ErrRoomNotFound // 未找到：返回业务错误，ErrRoomNotFound 仅为示例
    }
    defer roomRef.Release() // release hold, allow Room to idle-exit later

    // RefPost: fire-and-forget, bypasses Group lookup
    if err := actor.RefPost(roomRef, &AddPlayer{PlayerId: ctx.Id()}); err != nil {
        return nil, err
    }

    // RefCall: request-reply, bypasses Group lookup
    info, err := actor.RefCall(context.Background(), roomRef, &GetRoomInfo{})
    if err != nil {
        return nil, err
    }
    return actor.OK, nil
})
```

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

### 5. SafeCall & SafeReply — Resource-Safe Replies

When a handler returns a reply that holds external resources (database connections,
file handles, network sockets, etc.), those resources must be released regardless of
whether the caller receives the reply. `SafeCall` guarantees this through the
`SafeReply` interface.

**How it works:**

- `SafeReply[R0]` requires `~*R0` (pointer type) + `Close()` method
- On success: caller receives the reply and is responsible for calling `Close()`
- On timeout/cancel: the framework automatically calls `Close()` on the orphaned reply
- `SafeCall` / `RefSafeCall` mirrors `Call` / `RefCall` but with `SafeReply` constraint

```go
// Define a SafeReply type
type ResourceReply struct {
    Data   []byte
    handle *os.File  // needs cleanup
}

func (r *ResourceReply) Close() {
    if r.handle != nil {
        r.handle.Close()
    }
}

// Register handler returning SafeReply
actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *LoadData, _ bool) (*ResourceReply, error) {
    f, _ := os.Open("data.bin")
    data, _ := io.ReadAll(f)
    return &ResourceReply{Data: data, handle: f}, nil
})

// Use SafeCall — Close() is guaranteed
reply, err := actor.SafeCall(ctx, mgr, id, &LoadData{})
if err != nil {
    // If timeout/cancel: reply.Close() already called automatically
    return err
}
defer reply.Close() // caller must close on success
// use reply.Data...
```

| API | Constraint | Cleanup on success | Cleanup on timeout/cancel |
|-----|-----------|-------------------|--------------------------|
| `Call` / `RefCall` | `PtrReply` | Caller N/A | Reply discarded (no Close) |
| `SafeCall` / `RefSafeCall` | `SafeReply` | Caller calls `Close()` | Framework calls `Close()` automatically |

> **Type safety**: `SafeCall` only accepts requests whose reply implements `SafeReply`.
> Attempting `SafeCall` with a plain `PtrReply` type results in a compile error:
> `*AttackReply does not implement SafeReply[*AttackReply] (missing Close method)`

### 7. Manager Lifecycle

```go
mgr := actor.NewManager()

// Graceful shutdown: stop accepting new messages, wait for all Actors to exit
mgr.CloseManager()
mgr.JoinManager()

// Check if Manager is already closed
if mgr.IsClosed() {
    // ...
}

// Per-Actor lifecycle
actor.CloseActor[PlayerId](mgr, id)   // gentle close: drain mailbox, finish in-flight
actor.KillActor[PlayerId](mgr, id)    // force close: cancel ctx, drop pending
actor.JoinActor[PlayerId](mgr, id)    // wait for Actor's goroutine to exit
```

## RPC

Remote Actor communication over WebSocket with JSON codec.

### Server

```go
mgr := actor.NewManager()
// ... register handlers ...

server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
    ":8080", mgr,
    func(b *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]) {
        rpc.RegisterRequest(b, &Login{})
        rpc.RegisterRequest(b, &Attack{})
        rpc.RegisterRequest(b, &Close{})
    },
)
server.Start() // non-blocking
// server.Run() // blocking

// graceful shutdown
ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
defer cancel()
server.Shutdown(ctx)
```

### Client

```go
client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]("localhost:8080")
client.Connect()
defer client.Close()

// Remote Post (fire-and-forget)
rpc.Post(client, playerId, &Login{InitHP: 100, InitLevel: 1})

// Remote Call
reply, err := rpc.Call(ctx, client, playerId, &Attack{Damage: 30})

// Remote Call with timeout
reply, err = rpc.CallTimeout(ctx, client, playerId, &Attack{Damage: 10}, 5*time.Second)

// Remote Broadcast
rpc.Broadcast(client, &Close{})
```

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

```go
import (
    "github.com/lcy03406/actor-go/actor"
    "github.com/lcy03406/actor-go/grain"
)

// Use ShotSelf when business data is directly serializable
type PlayerData struct {
    HP    int `json:"hp"`
    Level int `json:"level"`
}

// State type alias for readability
type GrainState = grain.State[PlayerId, PlayerData, PlayerData, *grain.ShotSelf[PlayerData]]

// Create PersistenceManager: lease is built into the driver, no separate lease manager needed
pm := grain.NewPersistenceManager(
    grain.WithDriver(grain.NewJsonDriver("./data")),
    grain.WithNodeId("node-1"),
)

// Register: spawn handler explicitly activates the Grain
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
    actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *Login, _ bool) (actor.OkReply, error) {
        res, err := ctx.State().Activate(ctx, pm)
        if err != nil {
            return actor.OK, err
        }
        if res == grain.ActivateCreated { // 首次创建才初始化
            ctx.State().Data.HP = req.InitHP
            ctx.State().Data.Level = req.InitLevel
        }
        ctx.State().Persist(ctx)  // save immediately
        return actor.OK, nil
    })

    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *Attack, _ bool) (*AttackReply, error) {
        ctx.State().Data.HP -= req.Damage
        alive := ctx.State().Data.HP > 0
        return &AttackReply{RemainingHP: ctx.State().Data.HP, Alive: alive}, nil
    })

    actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *SaveAndQuit, _ bool) (actor.OkReply, error) {
        ctx.State().Deactivate(ctx)  // save + release lease + quit
        return actor.OK, nil
    })
})
```

### Grain State Methods

```go
state := ctx.State()
state.Data           // your business data (D)
state.Persist(ctx)   // save now, keep running (also renews the lease TTL)
state.Deactivate(ctx)  // save + release lease + quit
```

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

## Design Highlights

| Feature | Description |
|---------|-------------|
| Single-threaded Actor | One goroutine per Actor, serialized channel processing, no locks |
| Multi-Group | One Manager holds multiple `(ActorId, State)` type pairs |
| Compile-time safety | `Request[A, R]` binds Id/Reply; cross-Group errors caught by compiler |
| Post constraint | `Request[A, OkReply]` only; custom replies must use `Call` |
| Auto-spawn | First message triggers Actor creation (RegisterSpawn / RegisterServe) |
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

## License

MIT — see [LICENSE](LICENSE).
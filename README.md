# actor-go

[![Go Version](https://img.shields.io/badge/Go-1.26+-00ADD8?style=flat&logo=go)](https://go.dev/)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Go Report Card](https://goreportcard.com/badge/github.com/lcy03406/actor-go)](https://goreportcard.com/report/github.com/lcy03406/actor-go)

**actor-go** is a type-safe Actor Model framework for Go, featuring built-in RPC, distributed clustering, and persistent grain lifecycle management.

> 基于 Go 泛型的类型安全 Actor 模型框架，提供 RPC 远程调用、分布式集群和持久化 Grain 生命周期管理。

## Quick Start

```bash
# Install
go get github.com/lcy03406/actor-go

# Run examples
go run ./cmd/example/        # local Actor
go run ./cmd/rpc_example/    # RPC over WebSocket
go run ./cmd/grain_example/  # persistent Grain

# Run tests
go test ./...
```

## Project Structure

```
actor-go/
├── cmd/
│   ├── example/              # local Actor example
│   ├── rpc_example/          # RPC example
│   └── grain_example/        # persistent Grain example
├── actor/                    # Actor core
│   ├── types.go              # ActorId, Request interfaces
│   ├── actor.go              # actorRuntime — single-threaded event loop
│   ├── actor_context.go      # ActorContext — handler context
│   ├── group.go              # Group[A,S] — typed Actor pool
│   ├── manager.go            # Manager — multi-Group container
│   ├── handler.go            # handler dispatch
│   ├── invoke.go             # Post/Call/Broadcast/Multicast
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
│   ├── lifecycle.go          # activate (lease + load), WrapSpawn
│   ├── manager.go            # PersistenceManager
│   ├── snapshot.go           # Snapshotter interface + ShotSelf
│   ├── driver_json.go        # JSON file driver
│   ├── driver_yaml.go        # YAML file driver
│   ├── driver_redis.go       # Redis driver
│   └── driver_mongo.go       # MongoDB driver
├── cluster/                  # distributed clustering
│   ├── cluster.go            # Cluster entry
│   ├── node.go               # node management
│   ├── membership.go         # member discovery
│   ├── placement.go          # Actor placement
│   ├── route.go              # routing
│   └── transport.go          # node-to-node transport
├── lease/                    # distributed lease
│   ├── lease.go              # Lease interface
│   ├── local_lease.go        # local (single-node)
│   ├── redis_lease.go        # Redis lease
│   ├── mongo_lease.go        # MongoDB lease
│   ├── sql_lease.go          # SQL lease
│   └── retry.go              # retry strategies
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
    ctx.Timer(d, fn)      // schedule delayed callback, returns timer ID
    ctx.StopTimer(id)     // cancel a scheduled timer
    return &AttackReply{}, nil
})
```

### 5. Manager Lifecycle

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

Grain adds **lease-managed persistence** to Actors. Each Grain Actor is automatically activated on first message: acquire a distributed lease, load persisted state from storage, and start periodic lease renewal. On deactivation, state is saved and the lease is released.

### Concepts

| Concept | Description |
|---------|-------------|
| **PersistenceManager** | Manages driver + lease manager + renewal settings |
| **Driver** | Loads/saves snapshots (JSON, YAML, Redis, MongoDB) |
| **Lease** | Distributed lock ensuring single-ownership across nodes |
| **Snapshotter** | Converts business data to/from persistable snapshots |
| **WrapSpawn** | Wraps spawn handler to auto-activate on first message |

### Quick Example

```go
import (
    "github.com/lcy03406/actor-go/actor"
    "github.com/lcy03406/actor-go/grain"
    "github.com/lcy03406/actor-go/lease"
)

// Use ShotSelf when business data is directly serializable
type PlayerData struct {
    HP    int `json:"hp"`
    Level int `json:"level"`
}

// State type alias for readability
type GrainState = grain.State[PlayerId, PlayerData, PlayerData, *grain.ShotSelf[PlayerData]]

// Create PersistenceManager
pm := grain.NewPersistenceManager(
    grain.WithDriver(grain.NewJsonDriver("./data")),
    grain.WithLeaseManager(lease.NewLocalManager(30*time.Second)),
    grain.WithNodeId("node-1"),
    grain.WithRenewInterval(30*time.Second),
)

// Register with WrapSpawn
actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
    actor.RegisterSpawn(b, grain.WrapSpawn(pm,
        func(ctx *actor.ActorContext[PlayerId, GrainState], req *Login, _ bool) (actor.OkReply, error) {
            ctx.State().Data.HP = req.InitHP
            ctx.State().Data.Level = req.InitLevel
            ctx.State().Persist(ctx)  // save immediately
            return actor.OK, nil
        }))

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
state.Persist(ctx)   // save now, keep running
state.Deactivate(ctx)  // save + release lease + quit
state.RenewLease(ctx)  // manual lease renewal (auto if RenewInterval > 0)
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

The `cluster` package provides distributed Actor placement across multiple nodes:

- **Membership**: node discovery and health checks
- **Placement**: decides which node owns each Actor
- **Routing**: forwards messages to the owning node
- **Transport**: node-to-node communication

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
| Explicit Manager | `NewManager()` creates independent instances, no global state |
| Package name alias | Use `import act "github.com/lcy03406/actor-go/actor"` to avoid conflicts |
| Codec interface | Easy to swap serialization; supports JSON, protobuf, etc. |
| Graceful shutdown | `Server.Shutdown(ctx)` waits for in-flight requests |
| Connection loss | `Client.Close()` notifies all pending calls via `done` channel |

## License

MIT — see [LICENSE](LICENSE).
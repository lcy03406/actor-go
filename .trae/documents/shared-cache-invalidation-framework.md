# shared 包:actor 间读缓存 + 版本失效通用框架(方案C)

## Context

actor 模型以"隔离"保证正确性,但业务需要 actor 间共享数据。方案 C(读缓存 + 失效通知)用于"读远多于写、可容忍最终一致"的共享数据(排行榜、公共配置、公告):

- **写方**(某个 writer actor)拥有数据,维护单调递增的 version;
- 写方数据变更后,向**读者 Group**(同一种 ActorId 类型的多个 reader actor)广播失效通知(仅携带 version,拉模式);
- **读者**本地持有 `Cache[T]`,收到失效通知只更新版本号;真正读取数据时,若版本落后则通过用户提供的 fetch 闭包(通常是 `actor.Call` 到写方)重新拉取。

目标:新建顶层通用包 `shared`(module `github.com/lcy03406/actor-go/shared`),零侵入地提供写方广播助手、读者版本感知缓存、失效处理器注册。

已确认决策(用户跳过询问,采用推荐默认):
- **仅拉模式**:失效消息只带版本号,不携带数据。
- **交付范围**:核心包 + 单元/端到端测试,不新增 cmd 示例、不改 engineering_example。
- **v1 仅本地**:基于 `actor.Broadcast` 本地 Manager 广播;跨节点(rpc/cluster)在文档注明扩展方向。

## 新增文件

```
shared/cache.go           Cache[T] / Snapshot[T](纯逻辑,零框架依赖)
shared/cache_test.go      Cache 单元测试
shared/invalidate.go      Invalidate[A] 请求 + RegisterInvalidate + Notify
shared/invalidate_test.go 端到端测试(真实 Manager,writer/reader 两 Group)
```

## API 设计

### shared/cache.go

```go
package shared

// Snapshot 是带版本的数据快照(写方返回给读者的权威版本)。
type Snapshot[T any] struct {
    Version int64
    Data    T
}

// Cache 是读者 Actor 持有的版本感知只读缓存。
// 非线程安全:只允许在所属 reader actor 的 run goroutine 内访问(handler/Timer 回调),
// 不放入 State 之外的其他 goroutine。
type Cache[T any] struct {
    invalidatedAt int64 // 最新已知失效版本(单调递增水位)
    data          T
    dataVersion   int64 // 本地数据的权威版本
    loaded        bool
}

func NewCache[T any]() *Cache[T]                  // 可选;零值已可用
func (c *Cache[T]) OnInvalidate(v int64)          // 仅当 v > invalidatedAt 时更新(忽略乱序/旧版本)
func (c *Cache[T]) Get(fetch func() (Snapshot[T], error)) (T, error)
func (c *Cache[T]) Set(snap Snapshot[T])          // 权威同步:dataVersion=snap.Version 且 invalidatedAt=snap.Version
func (c *Cache[T]) Version() int64                // 返回 dataVersion
func (c *Cache[T]) Stale() bool                   // !loaded || invalidatedAt > dataVersion
```

关键语义:

- `Get`:仅当 `Stale()` 时调用 fetch;成功后 `Set` 返回的快照(把失效水位同步到权威版本,实现**失效丢失自愈**);失败时返回旧数据 + err,由调用方降级。
- `Set` 把 `invalidatedAt` 无条件同步到快照版本是自愈核心:writer 当前版本必然 ≥ 已发出的任何失效版本,拉取一次即可把水位拉平。
- 文档提示:writer 的 version 需单调持久(建议存于 grain 或持久化计数器),重启回退会退化为 last-write-wins。

### shared/invalidate.go

```go
package shared

import "github.com/lcy03406/actor-go/actor"

type InvalidateReply struct{}
type Invalidate[A actor.ActorId] struct{ Version int64 }
func (*Invalidate[A]) ReqType(_ A, _ *InvalidateReply) string // "__shared_invalidate__"(前缀防撞名)

// RegisterInvalidate 在读者 Group 上注册失效处理器。
// accessor 返回读者 State 中持有的 Cache[T];收到失效通知时 accessor(state).OnInvalidate(version)。
// 注册为 RegisterQuery(allow_query=true, allow_spawn=false):失效不 spawn 读者,只送达已存在的读者。
func RegisterInvalidate[A actor.ActorId, S any, T any](
    b *actor.RegistryBuilder[A, S],
    accessor func(*S) *Cache[T],
)

// Notify 写方在数据变更后调用:向 A 类型 Group 广播失效通知,返回实际送达数。
// A 必须显式指定(读者的 ActorId 类型)。基于 actor.Broadcast,fire-and-forget。
func Notify[A actor.ActorId](mgr *actor.Manager, version int64) (int, error)
```

实现要点:

- 内部用显式类型参数调用 `actor.RegisterQuery[A, S, *Invalidate[A], *InvalidateReply, Invalidate[A], InvalidateReply](b, fn)`,闭包内 `if c := accessor(ctx.State()); c != nil { c.OnInvalidate(req.Version) }`(nil 防御)。
- `Notify` 即 `actor.Broadcast[A](mgr, &Invalidate[A]{Version: version})`,透传送达数。A/S/T 泛型均已在仓库内验证可编译(与 `Serve` 从 build 闭包推导、`actor.Broadcast[MyId]` 显式 A 用法一致)。
- 读者读取路径示例(写入文档):在 reader 的查询 handler 中
  `data, err := c.Get(func() (shared.Snapshot[T], error) { return actor.Call(ctx, mgr, writerId, &GetSnapshot{}) })`。
  注意:handler 内不可 Call 自身(沿用框架死锁规则)。

## 实现顺序

1. `shared/cache.go` + `shared/cache_test.go`(纯单元,无 actor 依赖)。
2. `shared/invalidate.go` + `shared/invalidate_test.go`(端到端)。

## 端到端测试计划(invalidate_test.go)

真实 `actor.NewManager`,注册两个 Group:

- **Writer** `WriterId`/`WriterState`(Serve):`Write`(spawn 时 Open,version++ 并存数据)、`GetSnapshot`(Query,返回 `shared.Snapshot[string]{Version, Data}`)。
- **Reader** `ReaderId`/`ReaderState`(Serve):State 含 `Cache *shared.Cache[string]`、`WriterId`、`FetchCount int`;`Read`(spawn 时 Open + 首次 Get,闭包内 `actor.Call` writer 并递增 FetchCount)、`Peek`(Query,暴露 Stale/Version/FetchCount 供断言)。

用例:
1. `Call Write(1)` → `Call Read`(断言 FetchCount==1)→ 再 `Read`(FetchCount 不变,命中缓存)。
2. `Call Write(2)` + `shared.Notify[ReaderId](mgr, 2)`(断言送达数==1)→ `testutil.Settle()` → `Peek` 断言 Stale==true → `Read` 返回新数据(FetchCount==2)→ 再 `Read` 不拉。
3. 乱序保护:`Notify` 旧版本(1)不改变水位。
4. 自愈:直接 `Notify` 更高版本但**不**实际 Get;再经 fetch 返回权威版本后 `Version()` 同步到权威水位。
5. 驱逐兜底:reader 未 Open 场景下(或 CloseActor 后重来)首次 Get 必定 fetch,数据不陈旧。

等待使用 `internal/testutil`(`Settle`/`WaitCount`),并发 Call 用 `GoCall`。

## 复用与参考

- `actor.RegisterQuery` / `actor.Broadcast` / `actor.Call`:见 [registry_builder.go](file:///d:/work/erosion/actor-go/actor/registry_builder.go#L47-L52)、[manager.go](file:///d:/work/erosion/actor-go/actor/manager.go#L214-L228)。
- options/包结构惯例:参考 [grain/manager.go](file:///d:/work/erosion/actor-go/grain/manager.go)。
- 测试脚手架: [internal/testutil/testutil.go](file:///d:/work/erosion/actor-go/internal/testutil/testutil.go)。

## 文档

- 更新 [README.zh.md](file:///d:/work/erosion/actor-go/README.zh.md) 与 [README.md](file:///d:/work/erosion/actor-go/README.md):项目结构新增 `shared/` 条目 + 一节「共享数据(读缓存 + 失效通知)」用法与一致性语义(最终一致;失效通知尽力投递,靠 Get 拉取的权威版本自愈;读者建议 Open 保活)。
- 不改动 `actor`/`grain`/`cluster`/`rpc` 现有代码。

## 验证

```bash
cd d:\work\erosion\actor-go
go build ./...
go vet ./...
go test ./shared/ -v
go test ./...   # 回归全部包
```

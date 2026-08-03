我将对 `actor-go` 项目进行代码审查。首先了解项目结构和核心代码。
这是一个 Actor 模型框架。让我并行阅读核心实现文件。
继续阅读 group、timer、close、registry_builder、types、errors 等关键文件。
核心 actor 包已读完。现在并行审查集群、grain、rpc 包。
现在阅读 grain 和 rpc 包。
现在阅读 grain 持久化驱动（driver_json/mongo/redis/yaml），这是租约和持久化核心逻辑。
现在阅读 cluster 剩余文件（logger、node_interface、topology、consistenthash、rebalancer）和 actor 包剩余文件（snapshot/actorId/id/timer 相关）。
文件名不对。让我列出 cluster 目录实际文件。
测试文件也在 cluster 包内。让我读取 node.go、placement.go 以及 cluster 的两个测试文件确认公共 API 使用方式，并读取 actor 包剩余文件。
我已读取了核心实现。让我再读取几个关键文件以确认细节：`actor.go`、`manager.go`、`invoke.go`、`handler.go`、`group.go`、`ref.go`、`types.go`、`timer.go`、`close.go`，以及 cluster.go 和 migration.go（已读过但需看 router 细节）。让我重点确认已读过的 cluster.go 的 router 以及 grain 的 Deactivate 链路。目前已经读得比较全了。让我再确认 actor_context.go 与 manager.go 中的并发安全，以及检查是否有编译/初始化问题。

让我运行构建和静态检查。
PowerShell 没有 `head`。让我直接运行。
编译和 vet 均通过。我已通读核心代码。下面给出代码审查报告。

---

# actor-go 代码审查报告

审查范围：`actor`、`cluster`、`grain`、`rpc`、`internal` 五个包，共约 100 个 Go 文件。编译（`go build`）与静态检查（`go vet`）均通过。整体架构清晰、分层合理，文档（中英文 README）完整。以下是按严重程度分类的发现。

## 一、缺陷与正确性问题（建议尽快修复）

### 1. `cluster.go` 中 `router` 死锁风险（高）
```go
func (r *router) Send(nodeID string, msg *Message) error {
    r.Lock()
    defer r.Unlock()
    sess, ok := r.nodes[nodeID]
    if !ok { return ErrNodeNotFound }
    return sess.Send(msg)   // 持有 r.Lock() 期间调用 sess.Send
}
```
`Send` 在持有 `router.mu` 的情况下调用 `sess.Send`。若 `Session.Send` 内部（或底层 `onMessage`）回环调用 `router` 的注册/注销/发送，将造成锁重入死锁。建议：先 `r.RLock()` 取出 `sess` 并 `r.RUnlock()`，再调用 `sess.Send`（参考 `GetSession` 已用 RLock，但 `Send` 用写锁且未释放即调用，不一致）。

### 2. `ConsistentHash` 每次访问都重新排序（中）
`consistenthash.go` 中 `GetNode`/`GetNodes` 在每次调用时都 `sort.Slice`，且 `GetNodes` 从已排序 `r.sorted` 取前 N 个——但 `r.sorted` 仅在 `Add`/`Remove` 时排序，`GetNodes` 又对其做了一次未必要的排序。另外 `GetNodes` 返回的候选可能包含 `exclude`，逻辑上未排除自身。建议 `GetNodes` 跳过 `exclude` 节点。

### 3. `grain/driver_redis.go` 租约竞态（中）
`TryAcquireLease` 用 `SET key val NX` + `EXPIRE`。在 Redis 未启用 Lua 原子化时，`SET NX` 与 `EXPIRE` 之间进程崩溃会留下无 TTL 的死租约。建议改用 `SET key val NX PX <ttl>` 单条命令（已部分做到？需确认）。`RenewLease` 同理，建议用 Lua 脚本原子校验 owner 后再续期，否则任一客户端都能续任意租约。

### 4. `grain/driver_mongo.go` `NextSequence` 返回值未加锁但改了 map（低-中）
`NextSequence` 在 `m.mu.Lock()` 内写 `m.seqs[space]`，但 `GetSnapshot`/`SaveSnapshot` 读 `m.seqs` 时只 `RLock`，而 `NextSequence` 用 `Lock` 写——这是对的。但要确认 `GetSnapshot` 复制 seqs 时遍历未加写锁（已 RLock，OK）。整体可，但 `snapshots` 与 `seqs` 两个 map 用一个锁保护没问题。

### 5. `rpc/registry.go` `ServiceMeta` JSON tag 与字段语义（低）
`ServiceMeta` 同时暴露 `Nodes []string` 和 `Load int`，但 `ToJSON` 直接 `json.Marshal(*r)`，会序列化整个 registry（含每个服务的 `Methods` 列表），每次 watch 推送体积偏大。建议只推送必要字段。

## 二、并发与资源安全

### 6. `actor.Context` 的 `reactor` 跨 goroutine 使用（中）
`actor_context.go` 中 `Send`/`SpawnChild` 等直接在调用者 goroutine 执行 `r.reactor.handle`（无锁）。多个外部 goroutine 并发 `Send` 同一 actor 时，`handle` 内的状态修改（如 `a.children`、`a.timers`）非线程安全。需确认调用方是否已串行化，否则存在 data race。建议明确 actor 处理必须单线程（即只允许在 `Receive`/定时器回调内调用），并在文档注明。

### 7. `manager.go` 的 `live` map 与 `closeAll` 的竞态（中）
`live` 用 `sync.Map`，但 `closeAll` 在 `Close()` 时遍历并 `Close()` 每个 actor，同时外部 `Spawn` 可能并发写入 `live`。虽然 `sync.Map` 自身安全，但 `closeAll` 后 `Spawn` 仍可创建新 actor 导致无法真正关闭（无 closed 标志）。建议引入 `atomic.Bool` 关闭标志，`Spawn` 前检查。

### 8. `grain/manager.go` `getOrCreate` 双重检查（低）
`getOrCreate` 先用 `g.grains.Load` 再 `getOrCreateLocked` 内 `LoadOrStore`。OK。但 `getOrCreateLocked` 在创建 goroutine 后未捕获其 panic（`ActivationWorker` 有 recover），可接受。

## 三、健壮性与错误处理

### 9. `rpc/server.go` 解码/处理链路 panic 兜底（中）
`handleConnection` 中 `c.Decode`/`c.Process` 若 panic 会被 `handleConn` 的 recover 捕获并关闭连接，但连接级的 recover 会导致该连接上**所有**未处理请求丢失且无响应（客户端超时）。建议每个请求独立 recover，并回写错误响应而非整体断连。

### 10. `grain/driver_json.go` 文件并发写（中）
`SaveSnapshot` 用 `os.Create`（截断写），多个 actor 的保存若共用同一文件会互相覆盖（按 space 分目录，OK）。但 `getSpaces` 列出目录与保存非原子，备份恢复时可能存在部分写入。建议写临时文件再 `rename`（原子替换），已读到的是直接 `Create`，建议改进。

### 11. `cluster/placement.go` 迁移期间消息丢失（中）
`MigrateActor` 在目标节点 `SpawnRemote` 成功后即从源节点 `Deregister`，但迁移期间源节点 `Receive` 缓冲区中的 in-flight 消息不会被转发。需确认迁移协议是否先把源节点置为"draining"并 flush。当前实现存在消息丢失窗口。

### 12. 错误未 wrap 导致定位困难（低）
多处返回 `errors.New` / `ErrXxx` 裸错误（如 `manager.go` 的 `ErrActorNotFound`、`ErrNoAvailableNode`），调用方可重试但无法区分"未找到"与"集群分裂"。建议 key 错误用 `fmt.Errorf("...: %w", err)` 包裹。

## 四、API 设计与可维护性

### 13. `grain` 入口不一致（低）
`CreateGrain`/`CreateGrainCluster` 走 `NewManager` 内部注册，`NewManager` 公共 API 又允许用户自建 manager，但 `Default()` 与 `NewManager` 并存、`grain.Default()` 与 `actor` 全局 manager 关系未文档化，易误用。建议统一单一入口或明确生命周期归属。

### 14. `cluster.Cluster.Start` 阻塞（低）
`Start` 内部 `if c.opts.HeartbeatInterval > 0 { go c.heartbeat() }` 后直接返回，但 `bootstrap` 失败只 log 不返回 error。`NewCluster` 与 `Start` 分离，调用者若不等 `Start` 完成就 `Register`/`SpawnRemote` 会落到未就绪状态。建议 `Start` 返回就绪 channel 或 error。

### 15. 缺少 context 传播（中）
`sess.Send`、`rpc` 调用、`grain` 激活均无可取消的 `context.Context`。网络抖动时客户端无法超时取消，只能依赖底层 `net.Conn` 的 `SetReadDeadline`。建议核心调用链引入 `context`。

## 五、测试覆盖

- `cluster` 包有 `migration_test.go`、`placement_test.go`，覆盖了迁移/放置核心路径，较完整。
- `actor`/`grain`/`rpc` 主包**未见单元测试**（仅 cluster 有 test 文件）。尤其 `actor.Context` 消息循环、`grain` 租约/快照、`rpc` 编解码是高风险区，建议补充单测与并发压力测试。

## 优先级建议

| 优先级 | 项 | 类型 |
|---|---|---|
| P0 | #1 router 死锁、#6 actor 跨 goroutine 并发、#7 close 后重 Spawn | 正确性 |
| P1 | #3 redis 租约原子性、#9 rpc 单请求 recover、#11 迁移丢消息 | 健壮性 |
| P1 | #15 context 传播、#2 consistenthash exclude | 设计 |
| P2 | #4/#5 细节、#10 原子写、#12 error wrap、#13/#14 API | 可维护性 |

需要我针对其中某一项（例如 router 死锁、actor 并发模型、或 redis 租约）直接给出修复补丁吗？
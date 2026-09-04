package actor

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"
)

// Manager 是 Actor 系统的顶层管理器，是多个 Group 的集合。
// 每个 Group 对应一组 (ActorId, State) 类型对，通过 Serve 函数注册。
type Manager struct {
	ctx        context.Context
	cancel     context.CancelFunc
	stopping   atomic.Bool
	joined     atomic.Bool
	groups     map[ActorType]groupErased
	rootLogger *slog.Logger
	logger     *slog.Logger
	name       string
	seq        atomic.Int32
}

// NewManager 创建一个新的 Manager。
func NewManager(logger *slog.Logger) *Manager {
	name := "mgr"
	ctx, cancel := context.WithCancel(context.Background())
	return &Manager{
		ctx:        ctx,
		cancel:     cancel,
		groups:     make(map[ActorType]groupErased),
		rootLogger: logger,
		logger:     logger.With("component", name),
		name:       name,
	}
}

// Clear 清除所有注册的 Actor 类型（用于测试）。
func (m *Manager) Clear() {
	m.groups = make(map[ActorType]groupErased)
}

func (m *Manager) RootLogger() *slog.Logger {
	return m.rootLogger
}

// Logger 返回 Manager 的日志记录器。
func (m *Manager) Logger() *slog.Logger {
	return m.logger
}

// newSeq 返回一个新序号。
func (m *Manager) newFrom() From {
	seq := fmt.Sprintf("%s.%d", m.name, m.seq.Add(1))
	return OriginFrom(seq)
}

type TraceOption int

const (
	TraceNone TraceOption = iota
	TraceHead
	TraceBrief
	TraceVerbose
)

type Options struct {
	BufMails  int
	TraceSend TraceOption
}

// ServeWith 注册一个 Group 的 Actor 类型及其处理器。
// A 是 ActorId 类型，S 是 State 类型，由 RegistryBuilder 推导。
// 调用后不应再修改builder。
// 如果同类型 Group 已存在则 panic。
func ServeWith[A ActorId, S any](mgr *Manager, options Options, builder *RegistryBuilder[A, S]) {
	actorType := actorTypeOf[A]()

	if _, ok := mgr.groups[actorType]; ok {
		panic("actor type already registered: " + actorType)
	}

	mgr.groups[actorType] = newGroup(mgr, builder.handlers, builder.on_spawn, options)
	mgr.logger.Info("serving actor type", "type", actorType, "options", options)
}

// Serve 注册一个 Group 的 Actor 类型及其处理器。
// A 是 ActorId 类型，S 是 State 类型，由 RegistryBuilder 中的 handler 函数签名推导。
// 如果同类型 Group 已存在则 panic。
func Serve[A ActorId, S any](mgr *Manager, options Options, build func(*RegistryBuilder[A, S])) {
	builder := NewRegistryBuilder[A, S]()
	build(builder)
	ServeWith(mgr, options, builder)
}

func findGroup[A ActorId](mgr *Manager, id A) groupBase[A] {
	if mgr.stopping.Load() {
		return nil
	}
	g := mgr.groups[id.ActorType()]
	if g == nil {
		return nil
	}
	return g.(groupBase[A])
}

type groupAndHandler[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] struct {
	g groupBase[A]
	h handlerBase[A, Q, R, Q0, R0]
}

func findHandler[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, id A, req Q) (gh groupAndHandler[A, Q, R, Q0, R0], err error) {
	g := findGroup(mgr, id)
	if g == nil {
		err = &GroupNotFoundError{id}
		return
	}
	reqType := req.ReqType(id, nil)
	h, ok := g.findHandler(reqType)
	if !ok {
		err = &HandlerNotFoundError{id, reqType}
		return
	}
	gh.g = g
	gh.h = h.(handlerBase[A, Q, R, Q0, R0])
	return
}

// Post 向指定 Group 中的 Actor 发送 fire-and-forget 消息。
func Post[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, id A, req Q) error {
	return FPost(mgr, mgr.newFrom(), id, req)
}

// FPost 向指定 Group 中的 Actor 发送 fire-and-forget 消息。
func FPost[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, from From, id A, req Q) error {
	gh, err := findHandler(mgr, id, req)
	if err != nil {
		return &GroupNotFoundError{id}
	}
	return gh.h.handlerPost(from, gh.g, id, req)
}

// Call 向指定 Group 中的 Actor 发送请求，结果作为返回值返回（R, error）。
// R 由显式类型参数或 req 的 ReqType(A, *R) 方法签名推导，且 Q 的 ReqType
// 签名确保 Q、A、R 三方匹配，不匹配会在编译期报错。
//
//	reply, err := actor.Call[TestActorId](ctx, mgr, id, &TestAdd{Add: 10})
//	if err != nil { ... }
func Call[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](ctx context.Context, mgr *Manager, id A, req Q) (R, error) {
	return FCall(ctx, mgr, mgr.newFrom(), id, req)
}

func FCall[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](ctx context.Context, mgr *Manager, from From, id A, req Q) (R, error) {
	gh, err := findHandler(mgr, id, req)
	if err != nil {
		return nil, &GroupNotFoundError{id}
	}
	ch, err := gh.h.handlerCall(from, gh.g, id, req, nil)
	if err != nil {
		return nil, err
	}
	return waitResult(ctx, ch)
}

func waitResult[R PtrReply[R0], R0 any](ctx context.Context, ch <-chan result[R, R0]) (R, error) {
	select {
	case result := <-ch:
		return result.Rep, result.Err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// SafeCall 向指定 Group 中的 Actor 发送请求并等待回复，回复类型 R 必须为 SafeReply
// （即含 Close 方法的指针回复）。成功送达时 Close 由调用方负责（用完即 Close，如 defer
// r.Close()）；当调用方超时或 ctx 被取消导致 reply 成为孤儿、或 handler panic 时，框架
// 会自动 Close 以避免资源泄漏；若 handler 返回 error 则不产出 reply，临时资源须由 handler
// 在返回错误前自行释放。其类型推导与错误处理语义与 Call 一致，差别仅在于回复需支持可释放。
// 详见 SafeReply 的注释。
func SafeCall[A ActorId, Q Request[A, R, Q0, R0], R SafeReply[R0], Q0 any, R0 any](ctx context.Context, mgr *Manager, id A, req Q) (R, error) {
	return SafeFCall(ctx, mgr, mgr.newFrom(), id, req)
}

func SafeFCall[A ActorId, Q Request[A, R, Q0, R0], R SafeReply[R0], Q0 any, R0 any](ctx context.Context, mgr *Manager, from From, id A, req Q) (R, error) {
	gh, err := findHandler(mgr, id, req)
	if err != nil {
		return nil, &GroupNotFoundError{id}
	}
	ch, err := gh.h.handlerCall(from, gh.g, id, req, R.Close)
	if err != nil {
		return nil, err
	}
	return safeResult(ctx, ch)
}

func safeResult[R SafeReply[R0], R0 any](ctx context.Context, ch chan result[R, R0]) (R, error) {
	select {
	case result := <-ch:
		return result.Rep, result.Err
	case <-ctx.Done():
		close(ch)
		for result := range ch {
			if rep := result.Rep; rep != nil {
				rep.Close()
			}
		}
		return nil, ctx.Err()
	}
}

// Broadcast 向指定 Group 的所有 Actor 广播 fire-and-forget 消息。
func Broadcast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, req Q) (int, error) {
	return FBroadcast(mgr, mgr.newFrom(), req)
}

// Broadcast 向指定 Group 的所有 Actor 广播 fire-and-forget 消息。
func FBroadcast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, from From, req Q) (int, error) {
	var id0 A
	gh, err := findHandler(mgr, id0, req)
	if err != nil {
		return 0, &GroupNotFoundError{id0}
	}
	return gh.h.handlerBroadcast(from, gh.g, req)
}

// Multicast 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。
func Multicast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, ids []A, req Q) ([]IdErr[A], error) {
	return FMulticast(mgr, mgr.newFrom(), ids, req)
}

// Multicast 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。
func FMulticast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](mgr *Manager, from From, ids []A, req Q) ([]IdErr[A], error) {
	if len(ids) == 0 {
		return nil, nil
	}
	var id0 A
	gh, err := findHandler(mgr, id0, req)
	if err != nil {
		return nil, &GroupNotFoundError{id0}
	}
	return gh.h.handlerMulticast(from, gh.g, ids, req)
}

// Count 返回指定 Group 当前活跃的 Actor 数量（不含 idle 退出的 Actor）。
// A 需要显式指定，如 actor.Count[TestActorId](mgr)。
func Count[A ActorId](mgr *Manager) (int, error) {
	var id0 A
	g := findGroup(mgr, id0)
	if g == nil {
		return 0, &GroupNotFoundError{id0}
	}
	return g.count(), nil
}

// Finalize 关闭指定 Group 的所有 Actor 并等待处理完毕。
// A 由 Request[A, OkReply] 约束推导。
func Finalize[A ActorId, Q Request[A, OkReply, Q0, Ok], Q0 any](mgr *Manager, req Q) {
	var id0 A
	gh, err := findHandler(mgr, id0, req)
	if err != nil {
		return
	}
	if _, err := gh.h.handlerBroadcast(mgr.newFrom(), gh.g, req); err != nil {
		mgr.logger.Error("finalize broadcast failed", "error", err)
	}
	gh.g.joinGroup()
	mgr.logger.Info("finalized actor type", "type", actorTypeOf[A]())
}

// CloseActor 温和关闭单个 Actor：仅关闭 mailbox，不打断 in-flight handler。
// 当前正在执行的 handler 会正常完成，已排队的后续消息会以 ActorClosedError 失败。
func CloseActor[A ActorId](mgr *Manager, id A) bool {
	g := findGroup(mgr, id)
	if g == nil {
		return false
	}
	return g.closeActor(id)
}

// KillActor 强制关闭单个 Actor：取消 ctx 并关闭 mailbox。
// in-flight handler 中监听 ctx.Done 的操作会立即返回。
func KillActor[A ActorId](mgr *Manager, id A) bool {
	g := findGroup(mgr, id)
	if g == nil {
		return false
	}
	return g.killActor(id)
}

// JoinActor 等待指定 Actor 的 run 循环完全退出。
// 若 Actor 不存在（从未创建或已清理）则返回 false。
// 通常先调用 CloseActor 或 KillActor，再调用 JoinActor。
func JoinActor[A ActorId](mgr *Manager, id A) bool {
	g := findGroup(mgr, id)
	if g == nil {
		return false
	}
	return g.joinActor(id)
}

// CloseManager 关闭 Manager 中所有 Group：不再接受新消息。
// 已存在的 in-flight handler 会正常执行完毕。可在 handler 中调用。
func (m *Manager) CloseManager() {
	if !m.stopping.CompareAndSwap(false, true) {
		return
	}
	m.cancel()
	for _, g := range m.groups {
		g.closeGroup()
	}
}

// JoinManager 等待所有 Actor 的 run 循环退出。
// 会隐式先调用 Close。不可在 handler 中调用（会死锁）。
func (m *Manager) JoinManager() {
	m.CloseManager()
	if !m.joined.CompareAndSwap(false, true) {
		return
	}
	for _, g := range m.groups {
		g.joinGroup()
	}
}

// IsClosed 返回 Manager 是否已关闭。
func (m *Manager) IsClosed() bool {
	return m.stopping.Load()
}

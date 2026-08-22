package actor

import (
	"context"
	"log/slog"
	"sync"
	"sync/atomic"
)

type groupErased interface {
	closeGroup()
	joinGroup()
	count() int
}

type groupBase[A ActorId] interface {
	groupErased
	findHandler(reqType string) (handler[A], bool)
	isStopping() bool
	closeActor(id A) bool
	killActor(id A) bool
	joinActor(id A) bool
}

type group[A ActorId, S anyState] struct {
	ctx      context.Context
	cancel   context.CancelFunc
	options  Options
	mu       sync.RWMutex
	stopping atomic.Bool
	logger   *slog.Logger
	mgr      *Manager
	registry map[string]handler[A]
	on_spawn OnSpawnFn[A, S]
	actors   map[A]*actorRuntime[A, S]
	idle     atomic.Int32
}

func newGroup[A ActorId, S anyState](m *Manager, registry map[string]handler[A], on_spawn OnSpawnFn[A, S], options Options) *group[A, S] {
	ctx, cancel := context.WithCancel(m.ctx)
	actorType := actorTypeOf[A]()
	logger := m.rootLogger.With("actorType", actorType)
	return &group[A, S]{
		ctx:      ctx,
		cancel:   cancel,
		options:  options,
		logger:   logger,
		mgr:      m,
		registry: registry,
		on_spawn: on_spawn,
		actors:   make(map[A]*actorRuntime[A, S]),
	}
}

func (g *group[A, S]) findHandler(reqType string) (handler[A], bool) {
	h, ok := g.registry[reqType]
	return h, ok
}

func (g *group[A, S]) isStopping() bool {
	return g.stopping.Load()
}

func (g *group[A, S]) holdActor(id A) *actorRuntime[A, S] {
	g.mu.RLock()
	defer g.mu.RUnlock()
	a := g.actors[id]
	if a == nil || a.closed.Load() {
		return nil
	}
	a.hold()
	return a
}

// holdActorForRef 在读锁内 hold 目标 Actor，返回用于构造 ActorRef 的运行时句柄。
// 调用方负责在 ActorRef.Release() 中 unhold。
func (g *group[A, S]) holdActorForRef(id A) *actorRuntime[A, S] {
	g.mu.RLock()
	defer g.mu.RUnlock()
	a := g.actors[id]
	if a == nil || a.closed.Load() {
		return nil
	}
	a.hold()
	return a
}

func (g *group[A, S]) spawnActor(id A) (*actorRuntime[A, S], bool) {
	// newActor 在锁外预分配，锁内仅做 map 插入。
	actor := newActor(id, g, g.options)
	// 新 actor 对其他 goroutine 不可见，Store 放在锁外安全。
	// holder=2：1 管理器引用（阻止退出），1 调用方引用（用完 unhold）。
	actor.holder.Store(2)
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.stopping.Load() {
		return nil, false
	}
	old := g.actors[id]
	if old != nil {
		if old.closed.Load() {
			//已经在关闭了，等它死透
			<-old.doneCh
			//map不用管，removeActor只在匹配它自己时移除
		} else {
			//旧的能用。设置closed的地方都加锁了，判断可靠。
			old.hold()
			return old, false
		}
	}
	g.actors[id] = actor
	g.actorIdle(id) // 与 map insert 原子：防止外部在 actorWake 前读到未标 idle 的 actor
	//初始为Idle状态，在实际处理到spawn消息时Wake
	//注意这里不要hold()，初始已经有2了
	return actor, true
}

// resolveActor 唤醒已有 actor 或创建新 actor。
// 找到已关闭（正在退出）的 actor 时返回 nil——不覆盖创建新 actor，
// 需等旧 actor 完全退出（从 map 删除）后才能 spawn。
func (g *group[A, S]) resolveActor(id A, allow_spawn bool) *actorRuntime[A, S] {
	if g.stopping.Load() {
		return nil
	}
	actor := g.holdActor(id)
	if actor != nil {
		return actor
	}
	if !allow_spawn {
		return nil
	}
	actor, spawn := g.spawnActor(id)
	if spawn {
		go actor.run()
	}
	return actor
}

func (g *group[A, S]) holdActors(ids []A) []*actorRuntime[A, S] {
	values := make([]*actorRuntime[A, S], 0, len(ids))
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, id := range ids {
		a := g.actors[id]
		if a != nil && !a.closed.Load() {
			a.hold()
			values = append(values, a)
		}
	}
	return values
}

func (g *group[A, S]) holdAllActors() []*actorRuntime[A, S] {
	values := make([]*actorRuntime[A, S], 0, len(g.actors))
	g.mu.RLock()
	defer g.mu.RUnlock()
	for _, a := range g.actors {
		if !a.closed.Load() {
			a.hold()
			values = append(values, a)
		}
	}
	return values
}

func unhold[A ActorId, S anyState](actors []*actorRuntime[A, S]) {
	for _, a := range actors {
		a.unhold()
	}
}

func (g *group[A, S]) broadcast(m invokable[A, S]) (int, error) {
	if g.stopping.Load() {
		return 0, nil
	}
	count := 0
	actors := g.holdAllActors()
	defer unhold(actors)
	for _, a := range actors {
		if a.send(m) == nil {
			count++
		}
	}
	return count, nil
}

func (g *group[A, S]) multicast(ids []A, m invokable[A, S]) (int, error) {
	if g.stopping.Load() {
		return 0, nil
	}
	count := 0
	actors := g.holdActors(ids)
	defer unhold(actors)
	for _, a := range actors {
		if a.send(m) == nil {
			count++
		}
	}
	return count, nil
}

func (g *group[A, S]) count() int {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return len(g.actors) - int(g.idle.Load())
}

// closeGroup 关闭整个 group：
//   - stopping=true 阻止新 actor spawn
//   - cancel(ctx) 级联取消所有 actor 的 ctx（清理 timer、中断 in-flight handler 中监听 ctx.Done 的操作）
//   - requestClose 每个 actor（标记 closed 拒绝新 send + closeGroup quit 通知 run 退出）
//
// cancel(ctx) 与 requestClose 不重复：前者清理 ctx 关联资源，后者拒绝 send 并让 run 循环退出。
func (g *group[A, S]) closeGroup() {
	if !g.stopping.CompareAndSwap(false, true) {
		return
	}
	g.cancel()
	g.mu.RLock()
	for _, a := range g.actors {
		a.requestClose()
	}
	g.mu.RUnlock()
}

// joinGroup 等待所有 actor 的 run 循环退出。
// 快照当前 actors 的 done chan 后逐个等待。close() 后 stopping=true 不会 spawn 新 actor，
// 快照完整；新 actor 只可能在快照前加入（Finalize 场景下 broadcast 后并发 Post 创建的新 actor
// 不在快照中，与 C++ joinGroup 语义一致——只等当前已存在的 actor）。
func (g *group[A, S]) joinGroup() {
	g.mu.RLock()
	dones := make([]chan struct{}, 0, len(g.actors))
	for _, a := range g.actors {
		dones = append(dones, a.doneCh)
	}
	g.mu.RUnlock()
	for _, d := range dones {
		<-d
	}
}

// closeActor 温和关闭单个 actor：requestClose（标记 closed + close quit）。
// 不 cancel ctx，in-flight handler 正常完成。
func (g *group[A, S]) closeActor(id A) bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	a, ok := g.actors[id]
	if !ok {
		return false
	}
	a.requestClose()
	return true
}

// killActor 强制关闭单个 actor：cancel ctx（中断 handler）+ requestClose。
func (g *group[A, S]) killActor(id A) bool {
	g.mu.RLock()
	a, ok := g.actors[id]
	g.mu.RUnlock()
	if !ok {
		return false
	}
	a.kill()
	return true
}

// joinActor 等待指定 actor 的 run 循环完全退出。
// 若 actor 不存在（从未创建或已从 map 移除）则返回 false。
// 通常先调用 CloseActor 或 KillActor，再调用 JoinActor。
func (g *group[A, S]) joinActor(id A) bool {
	g.mu.RLock()
	a, ok := g.actors[id]
	g.mu.RUnlock()
	if !ok {
		return false
	}
	<-a.doneCh
	return true
}

func (g *group[A, S]) actorIdle(_ A) {
	g.idle.Add(1)
}

func (g *group[A, S]) actorWake(_ A) {
	g.idle.Add(-1)
}

func (g *group[A, S]) actorQuit(a *actorRuntime[A, S], buf []invokable[A, S]) ([]invokable[A, S], bool) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if a.closed.Load() {
		//已经准备关了，不需要这边处理
		return buf, false
	}
	if a.holder.Load() > 1 {
		//有别人正在send，先不关
		return buf, false
	}
	//加了锁不会有人拿到Actor, holder == 1说明已经拿到Actor的也用完了，所以不会有人send，尝试排空mailbox
	buf = a.pumpMailbox(buf)
	if len(buf) > 0 {
		//还有事情没处理，不能退出
		return buf, false
	}
	//这就准备退了
	a.requestClose()
	return buf, true
}

func (g *group[A, S]) removeActor(a *actorRuntime[A, S]) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.actors[a.id] != a {
		return
	}
	delete(g.actors, a.id)
}

package actor

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"
)

// ActorContext 是类型安全的 Actor 封装。
// A 是 ActorId 类型，S 是 State 类型。
type ActorContext[A ActorId, S anyState] struct {
	ctx     context.Context
	cancel  func()
	actor   *actorRuntime[A, S]
	state   S
	idle    bool // Actor 运行状态：true=空闲/未激活，false=活跃/运行中。
	              // spawn 时默认为 true（不占用运行资源），由用户调用 Open 翻转为 false。
	timers  map[int]*time.Timer
	timerId int
}

func newActorContext[A ActorId, S anyState](actor *actorRuntime[A, S]) *ActorContext[A, S] {
	ctx, cancel := context.WithCancel(actor.ctx)
	return &ActorContext[A, S]{
		ctx:    ctx,
		cancel: cancel,
		actor:  actor,
		idle:   true, // spawn 默认空闲/未激活
	}
}

func (a *ActorContext[A, S]) clear() {
	for len(a.timers) > 0 {
		for id, timer := range a.timers {
			timer.Stop()
			delete(a.timers, id)
		}
	}
	a.cancel()
}

func (a *ActorContext[A, S]) Context() context.Context {
	return a.ctx
}

// Id 返回 Actor 的 ID。
func (a *ActorContext[A, S]) Id() A {
	return a.actor.id
}

// State 返回 Actor 的状态指针。
func (a *ActorContext[A, S]) State() *S {
	return &a.state
}

// SetState 设置 Actor 的状态。
func (a *ActorContext[A, S]) SetState(s S) {
	a.state = s
}

// Logger 返回 Actor 的日志记录器。
func (a *ActorContext[A, S]) Logger() *slog.Logger {
	return a.actor.logger
}

// Manager 返回 Actor 系统顶层 Manager，用于跨 Group 的 Actor 通信。
// 通过 Manager 可以向其他 Group 的 Actor 发送 Post/Call/Broadcast 消息。
//
// 示例：
//
//	// 在 Player handler 中向 Room Actor 发送消息
//	roomId := room.RoomId{RoomId: 123}
//	actor.Post(ctx.Manager(), roomId, &room.JoinRoom{PlayerId: ctx.Id().String()})
func (a *ActorContext[A, S]) Manager() *Manager {
	return a.actor.g.mgr
}

// Quit 请求 Actor 退出：置 idle=true（回到空闲态）。
// 与 Open 对称——两者都只翻转 idle 标记（Open: true→false，Quit: false→true），
// 真正的 actorWake/actorIdle 由 run goroutine 在 handler 返回后比较状态跳变统一结算。
// 由于 idle 跳变（false→true），结算时必然触发一次 actorIdle，使 actor 回到 idle 池。
//
// 当前正在执行的 handler 会正常执行完毕，已在 mailbox 中的后续消息会以 ActorClosedError 失败。
func (a *ActorContext[A, S]) Quit() {
	a.idle = true
}

// Open 将 Actor 置为活跃状态，与 Quit 相对：置 idle=false（离开空闲态）。
//
// Open/Quit 都只设置 idle 标记，不直接操作 idle 计数。
// 真正的 actorWake/actorIdle 由 run goroutine 在 handler 返回后
// 通过比较处理前后的 idle 状态统一结算（settle），这样：
//   - Open 与 Quit 完全对称（各自只在状态跳变时触发一次计数变化）；
//   - 不存在 spawn 场景下的补偿分支与重复计数。
//
// Actor 在处理 spawn 消息前默认空闲（idle=true，不占用运行资源）。
// 用户需要在 spawn/serve 回调中显式调用 Open（或 grain.State.Activate）
// 来激活 Actor，使其进入持续运行态。
//
// 若不调用 Open（且不调用 Quit），Actor 处理完当前消息后保持空闲状态，
// 按已有逻辑处理下一条可 spawn 消息或空闲销毁。
func (a *ActorContext[A, S]) Open() {
	a.idle = false
}

// Ref 获取对同类型另一个 Actor 的直连引用，绕过 Group 查找。
// 只能查找同 Group（同 ActorType）中已存在的 Actor，不会 spawn。
// 返回的 ActorRef 持有目标 Actor 的引用计数，使用完毕后需调用 Release() 释放。
// 若目标不存在或已关闭，返回 nil。
func (a *ActorContext[A, S]) Ref(id A) *ActorRef[A, S] {
	target := a.actor.g.holdActorForRef(id)
	if target == nil {
		return nil
	}
	return &ActorRef[A, S]{
		target: target,
		mgr:    a.actor.g.mgr,
	}
}

type ContextTimer struct {
	canceled atomic.Bool
	timer    *time.Timer
	clear    func() bool
}

func (ct *ContextTimer) Stop() {
	c := ct.clear
	if c != nil {
		c()
		ct.clear = nil
	}
	t := ct.timer
	if t != nil {
		t.Stop()
		ct.timer = nil
	}
}

// Timer 在指定延迟后向 Actor 自身发送回调，返回可取消的 Timer Id。
func (a *ActorContext[A, S]) Timer(d time.Duration, fn func()) int {
	a.timerId++
	id := a.timerId
	i := &timerInvoke[A, S]{fn, id, nil}
	actor := a.actor
	timer := time.AfterFunc(d, func() {
		if err := actor.send(i); err != nil {
			actor.logger.Error("timer send failed", "error", err)
		}
	})
	i.t = timer
	if a.timers == nil {
		a.timers = make(map[int]*time.Timer)
	}
	a.timers[id] = timer
	return id
}

// StopTimer 取消定时器，返回true表示成功取消，false表示已经触发了或ID不存在
func (a *ActorContext[A, S]) StopTimer(timerId int) bool {
	if a.timers == nil {
		return false
	}
	timer, ok := a.timers[timerId]
	if !ok {
		return false
	}
	timer.Stop()              //不用管Stop结果，即使取消失败也没事
	delete(a.timers, timerId) //只要从map里拿掉，就不会执行回调函数
	return true
}

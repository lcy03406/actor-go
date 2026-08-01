package actor

import (
	"context"
	"log/slog"
	"strconv"
	"sync/atomic"
	"time"
)

// ActorContext 是类型安全的 Actor 封装。
// A 是 ActorId 类型，S 是 State 类型。
type ActorContext[A ActorId, S anyState] struct {
	ctrl  ActorControl
	actor *actorRuntime[A, S]
	state S
}

func newActorContext[A ActorId, S anyState](actor *actorRuntime[A, S]) *ActorContext[A, S] {
	ctx, cancel := context.WithCancel(actor.ctx)
	g := actor.g
	id := actor.id
	seqCounter := new(int)
	return &ActorContext[A, S]{
		ctrl: ActorControl{
			ctx:     ctx,
			alogger: actor.logger,
			ilogger: actor.logger,
			fromSeq: func(f From) From {
				*seqCounter++
				seq := *seqCounter
				origin := f.Origin
				reqSeq := actorNameOf(id) + "." + strconv.Itoa(seq)
				if len(origin) == 0 {
					origin = reqSeq
				}
				return From{Origin: origin, ReqSeq: reqSeq}
			},
			traceSend: actor.g.options.TraceSend,
			mgr:       actor.g.mgr,
			cancel:    cancel,
			timerFn: func(i *timerStub) func() {
				return func() {
					a := g.holdActor(id)
					if a == nil {
						actor.logger.Warn("timer fired after closed")
						return
					}
					defer a.unhold()
					if a != actor {
						actor.logger.Warn("timer fired after respawn")
						return
					}
					if err := actor.send(timerInvoke[A, S]{i}); err != nil {
						actor.logger.Error("timer send failed", "error", err)
					}
				}
			},
		},
		actor: actor,
	}
}

func (a *ActorContext[A, S]) clear() {
	a.ctrl.clear()
	if a.ctrl.OnQuit != nil {
		defer func() {
			if r := recover(); r != nil {
				a.ctrl.ilogger.Error("OnQuit panic", "error", r)
			}
			a.ctrl.resetLogger()
		}()
		a.ctrl.invokeLogger(MakeFrom(a.Id(), "OnQuit"))
		a.ctrl.OnQuit()
	}
}

// Control 获取控制句柄，在Actor活跃期有效，应仅在本Actor内部使用，调用Quit后不应再使用。
// 用于获取与类型A、S无关的控制句柄。
// 参考Ref()是用于获取对自身的直通句柄，可以发给其他Actor。
func (a *ActorContext[A, S]) Control() *ActorControl {
	return &a.ctrl
}

func (a *ActorContext[A, S]) Context() context.Context {
	return a.ctrl.Context()
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
	return a.ctrl.Logger()
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
	return a.ctrl.mgr
}

// Quit 请求 Actor 退出：置 active=false（回到空闲态）。
// 与 Open 对称——两者都只翻转 active 标记（Open: false→true，Quit: true→false），
// 真正的 actorWake/actorIdle 由 run goroutine 在 handler 返回后比较状态跳变统一结算。
// 由于 active 跳变（true→false），结算时必然触发一次 actorIdle，使 actor 回到 idle 池。
//
// 当前正在执行的 handler 会正常执行完毕，已在 mailbox 中的后续消息会以 ActorClosedError 失败。
func (a *ActorContext[A, S]) Quit() {
	a.ctrl.Quit()
}

// Open 将 Actor 置为活跃状态，与 Quit 相对：置 active=true（离开空闲态）。
//
// Open/Quit 都只设置 active 标记，不直接操作 idle 计数。
// 真正的 actorWake/actorIdle 由 run goroutine 在 handler 返回后
// 通过比较处理前后的 active 状态统一结算（settle），这样：
//   - Open 与 Quit 完全对称（各自只在状态跳变时触发一次计数变化）；
//   - 不存在 spawn 场景下的补偿分支与重复计数。
//
// Actor 在处理 spawn 消息前默认空闲（active=false，不占用运行资源）。
// 用户需要在 spawn/serve 回调中显式调用 Open（或 grain.State.Activate）
// 来激活 Actor，使其进入持续运行态。
//
// 若不调用 Open（且不调用 Quit），Actor 处理完当前消息后保持空闲状态，
// 按已有逻辑处理下一条可 spawn 消息或空闲销毁。
func (a *ActorContext[A, S]) Open() {
	a.ctrl.Open()
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
func (a *ActorContext[A, S]) Timer(name string, d time.Duration, fn func()) TimerId {
	return a.ctrl.Timer(name, d, fn)
}

// StopTimer 取消定时器，返回true表示成功取消，false表示已经触发了或ID不存在
func (a *ActorContext[A, S]) StopTimer(timerId TimerId) bool {
	return a.ctrl.StopTimer(timerId)
}

// ControlState 获取Control和State的便捷函数
func (a *ActorContext[A, S]) ControlState() (*ActorControl, A, *S) {
	return a.Control(), a.Id(), a.State()
}

package actor

import (
	"context"
	"log/slog"
	"math"
	"time"
)

type ActorControl struct {
	ctx     context.Context
	logger  *slog.Logger
	mgr     *Manager
	cancel  func()
	timerFn func(i *timerStub) func()
	active  bool   // Actor 运行状态：true=激活，false=空闲。 spawn 时默认为 false，由用户调用 Open 翻转为 true。
	OnQuit  func() //用户注册，退出时框架调用
	timers  map[TimerId]*time.Timer
	timerId TimerId
}

func (a *ActorControl) clear() {
	for len(a.timers) > 0 {
		for id, timer := range a.timers {
			timer.Stop()
			delete(a.timers, id)
		}
	}
	// 先执行 OnQuit，再取消 ctx：退出钩子（如 grain 退出落盘）需要 context 尚未取消
	// 才能完成清理 I/O。cancel 用 defer 兜底，即使 OnQuit panic 也能保证 ctx 被取消。
	defer func() {
		if a.cancel != nil {
			a.cancel()
		}
	}()
	if a.OnQuit != nil {
		defer func() {
			if r := recover(); r != nil {
				a.logger.Error("OnQuit panic", "error", r)
			}
		}()
		a.OnQuit()
	}
}

func (a *ActorControl) Context() context.Context {
	return a.ctx
}

// Logger 返回 Actor 的日志记录器。
func (a *ActorControl) Logger() *slog.Logger {
	return a.logger
}

// Manager 返回 Actor 系统顶层 Manager，用于跨 Group 的 Actor 通信。
// 通过 Manager 可以向其他 Group 的 Actor 发送 Post/Call/Broadcast 消息。
//
// 示例：
//
//	// 在 Player handler 中向 Room Actor 发送消息
//	roomId := room.RoomId{RoomId: 123}
//	actor.Post(ctx.Manager(), roomId, &room.JoinRoom{PlayerId: ctx.Id().String()})
func (a *ActorControl) Manager() *Manager {
	return a.mgr
}

func (a *ActorControl) PushOnQuit(fn func()) {
	if fn == nil {
		return
	}
	old := a.OnQuit
	if old == nil {
		a.OnQuit = fn
		return
	}
	a.OnQuit = func() {
		fn()
		old()
	}
}

// Quit 请求 Actor 退出：置 active=false（回到空闲态）。
// 与 Open 对称——两者都只翻转 active 标记（Open: false→true，Quit: true→false），
// 真正的 actorWake/actorIdle 由 run goroutine 在 handler 返回后比较状态跳变统一结算。
// 由于 active 跳变（true→false），结算时必然触发一次 actorIdle，使 actor 回到 idle 池。
//
// 当前正在执行的 handler 会正常执行完毕，已在 mailbox 中的后续消息会以 ActorClosedError 失败。
func (a *ActorControl) Quit() {
	a.active = false
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
func (a *ActorControl) Open() {
	a.active = true
}

// Timer 在指定延迟后向 Actor 自身发送回调，返回可取消的 Timer Id。
func (a *ActorControl) Timer(d time.Duration, fn func()) TimerId {
	if a.timers == nil {
		a.timers = make(map[TimerId]*time.Timer)
	}
	// 从 timerId+1 开始线性探查下一个空位；到达 math.MaxInt 后回绕到 1，
	// 避免 id++ 溢出到 math.MinInt。若回绕一圈仍无空位（理论上不可能，
	// 因为 timer 数量恒等于已注册槽位数），则退回 timerId+1 兜底，杜绝无限循环。
	id := a.timerId + 1
	if id == math.MaxInt {
		id = 1
	}
	start := id
	for {
		if _, ok := a.timers[id]; !ok {
			break
		}
		if id == math.MaxInt {
			id = 1
		} else {
			id++
		}
		if id == start {
			// 整轮探查无空位，强制复用起始位（理论上不可达，仅作防御）。
			break
		}
	}
	a.timerId = id
	i := &timerStub{fn, id, nil}
	timer := time.AfterFunc(d, a.timerFn(i))
	i.t = timer
	a.timers[id] = timer
	return id
}

// StopTimer 取消定时器，返回true表示成功取消，false表示已经触发了或ID不存在
func (a *ActorControl) StopTimer(timerId TimerId) bool {
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

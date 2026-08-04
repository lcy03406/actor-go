package actor

import (
	"context"
	"time"
)

type ActorControl struct {
	ctx     context.Context
	cancel  func()
	timerFn func(i *timerStub) func()
	active  bool // Actor 运行状态：true=激活，false=空闲。
	// spawn 时默认为 true（不占用运行资源），由用户调用 Open 翻转为 false。
	timers  map[int]*time.Timer
	timerId int
}

func (a *ActorControl) clear() {
	for len(a.timers) > 0 {
		for id, timer := range a.timers {
			timer.Stop()
			delete(a.timers, id)
		}
	}
	if a.cancel != nil {
		a.cancel()
	}
}

func (a *ActorControl) Context() context.Context {
	return a.ctx
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
func (a *ActorControl) Timer(d time.Duration, fn func()) int {
	a.timerId++
	id := a.timerId
	i := &timerStub{fn, id, nil}
	timer := time.AfterFunc(d, a.timerFn(i))
	i.t = timer
	if a.timers == nil {
		a.timers = make(map[int]*time.Timer)
	}
	a.timers[id] = timer
	return id
}

// StopTimer 取消定时器，返回true表示成功取消，false表示已经触发了或ID不存在
func (a *ActorControl) StopTimer(timerId int) bool {
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

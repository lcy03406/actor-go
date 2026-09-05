package actor

import (
	"context"
	"log/slog"
	"math"
	"time"
)

type ActorControl struct {
	ctx         context.Context
	alogger     *slog.Logger
	ilogger     *slog.Logger
	from        From
	fromSeq     func(From) From //生成一个新的唯一请求ID
	traceSend   TraceOption
	mgr         *Manager
	cancel      func()
	timerFn     func(i *timerStub) func()
	active      bool   // Actor 运行状态：true=激活，false=空闲。 spawn 时默认为 false，由用户调用 Open 翻转为 true。
	OnQuit      func() //用户注册，退出时框架调用
	timers      map[TimerId]*time.Timer
	timerId     TimerId
	postpone    []postponeItem
	postponeSet map[any]struct{}
}

type postponeItem struct {
	id any // 目标 Actor ID（必须可比较，如 string, int, uint64）
	fn func() error
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
}

func (a *ActorControl) Context() context.Context {
	return a.ctx
}

// Logger 返回 Actor 的日志记录器。
func (a *ActorControl) Logger() *slog.Logger {
	return a.ilogger
}

// InvokeLogger 设置一次调用的日志记录器。
func (a *ActorControl) invokeLogger(from From) {
	a.from = from
	a.ilogger = a.alogger.With("from", from)
}

// InvokeLogger 设置Actor日志记录器，取消一次调用的日志记录器。
func (a *ActorControl) resetLogger() {
	a.ilogger = a.alogger
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
	a.ilogger.Debug("timer start", "timer", id)
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
	a.ilogger.Debug("timer stop", "timer", timerId)
	return true
}

func (a *ActorControl) poseponePost() {
	if len(a.postpone) == 0 {
		return
	}

	// 1. 取出所有待处理项，并清空队列（切断底层数组共享，防止嵌套 append 覆盖）
	list := a.postpone
	a.postpone = nil

	// 2. 遍历执行，并收集仍需重试的项
	newPostpone := make([]postponeItem, 0, len(list))
	newSet := make(map[any]struct{})

	for _, item := range list {
		err := item.fn()
		if _, ok := err.(*ActorBusyError); ok {
			// 仍繁忙：保留
			newPostpone = append(newPostpone, item)
			newSet[item.id] = struct{}{}
		}
		// 成功或其他不可恢复错误：不保留，即从集合中移除该 ID
	}

	// 3. 原子替换
	a.postpone = newPostpone
	a.postponeSet = newSet
}

func appendPostpone[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, id A, req Q) {
	fn := func() error {
		next := a.fromSeq(a.from)
		traceLogSend(a.traceSend, a.ilogger, "postpone post", next, id, reqTypeOf(req), req)
		err := FPost(a.Manager(), next, id, req)
		if err != nil {
			a.ilogger.Warn("postpone fail", "err", err)
		}
		return err
	}
	a.postpone = append(a.postpone, postponeItem{id: id, fn: fn})
	if a.postponeSet == nil {
		a.postponeSet = make(map[any]struct{})
	}
	a.postponeSet[id] = struct{}{}
}

func (a *ActorControl) hasPostpone(id any) bool {
	_, ok := a.postponeSet[id]
	return ok
}

// APostOnce 向指定 Group 中的 Actor 发送 fire-and-forget 消息。
// 返回 ActorBusyError 或 ActorPostponeError 表示消息因对方正忙而没有成功投递，调用方可以再次发送同一请求。
// 返回其它错误如消息路由错误等，通常调用方业务逻辑不应重试。
func APostOnce[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, id A, req Q) error {
	a.poseponePost()
	if a.hasPostpone(id) {
		err := &ActorPostponeError{Id: id}
		a.ilogger.Warn("postonce postpone error")
		return err
	}
	next := a.fromSeq(a.from)
	traceLogSend(a.traceSend, a.ilogger, "send postonce", next, id, reqTypeOf(req), req)
	err := FPost(a.Manager(), next, id, req)
	if err != nil {
		a.ilogger.Warn("postonce fail", "err", err)
	}
	return err
}

// APost 向指定 Group 中的 Actor 发送 fire-and-forget 消息。对方忙则延后重试。
// 返回 ActorBusyError 或 ActorPostponeError 表示消息已进入延迟队列，将自动重试，调用方不应再次发送同一请求，否则会重复。
// 返回其它错误如消息路由错误等，不进入延迟队列，不会自动重试，通常调用方业务逻辑也不应重试。
// 与普通Post类似，保证消息顺序但不保证逻辑顺序。延后期间本actor将转而执行后续逻辑，特别是能处理对方发来的请求，避免死锁。
func APost[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, id A, req Q) (err error) {
	a.poseponePost()
	if a.hasPostpone(id) {
		// 仍然很忙，排队等待
		err = &ActorPostponeError{Id: id}
		a.ilogger.Info("post postpone")
	} else {
		// 无排队，尝试立即发送
		next := a.fromSeq(a.from)
		traceLogSend(a.traceSend, a.ilogger, "send post", next, id, reqTypeOf(req), req)
		err = FPost(a.Manager(), next, id, req)
		if err == nil {
			return
		}
		if _, ok := err.(*ActorBusyError); !ok {
			// 不可恢复错误
			a.ilogger.Warn("post fail", "err", err)
			return
		}
		// 恰好忙起来了呢
		a.ilogger.Warn("post busy", "err", err)
	}
	appendPostpone(a, id, req)
	return
}

// ACall 向指定 Group 中的 Actor 发送请求，结果作为返回值返回（R, error）。
// 返回 ActorBusyError 或 ActorPostponeError 表示消息因对方正忙而没有成功投递，调用方可以再次发送同一请求。
// 返回其它错误如消息路由错误、对端处理错误等，通常调用方业务逻辑不应重试。
func ACall[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, id A, req Q) (rep R, err error) {
	ctx := a.Context()
	mgr := a.Manager()
	for i := range 3 {
		if i > 0 {
			t := time.Duration(i*i*20) * time.Millisecond
			time.Sleep(t)
		}
		a.poseponePost()
		if a.hasPostpone(id) {
			err = &ActorPostponeError{Id: id}
			a.ilogger.Info("call postpone", "retry", i, "err", err)
			continue
		}
		next := a.fromSeq(a.from)
		traceLogSend(a.traceSend, a.ilogger, "send call", next, id, reqTypeOf(req), req)
		rep, err = FCall(ctx, mgr, next, id, req)
		if err == nil {
			a.ilogger.Info("recv reply", "rep", rep)
			return
		} else {
			a.ilogger.Warn("call fail", "retry", i, "err", err)
			if _, ok := err.(*ActorBusyError); !ok {
				return
			}
		}
	}
	a.ilogger.Warn("call retry fail", "err", err)
	return
}

func splitPostpone[A ActorId](a *ActorControl, ids []A) (p []A, np []A) {
	for _, id := range ids {
		if a.hasPostpone(id) {
			p = append(p, id)
		} else {
			np = append(np, id)
		}
	}
	return
}

func splitIdErr[A ActorId](list []IdErr[A]) (p []A, np []IdErr[A]) {
	for _, idErr := range list {
		err := idErr.Err
		if err == nil {
			np = append(np, idErr)
		} else if _, ok := err.(*ActorBusyError); !ok {
			np = append(np, idErr)
		} else {
			p = append(p, idErr.Id)
		}
	}
	return
}

// AMulticastOnce 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。
// 返回列表中 ActorBusyError 或 ActorPostponeError 表示消息因对方正忙而没有成功投递，调用方可以再次发送同一请求。
// 返回其它错误如消息路由错误等，通常调用方业务逻辑不应重试。
func AMulticastOnce[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, ids []A, req Q) ([]IdErr[A], error) {
	a.poseponePost()
	p, np := splitPostpone(a, ids)
	mgr := a.Manager()
	next := a.fromSeq(a.from)
	traceLogSend(a.traceSend, a.ilogger, "multicast", next, ids, reqTypeOf(req), req)
	list, err := FMulticast(mgr, next, np, req)
	if err != nil {
		return nil, err
	}
	for _, id := range p {
		err := &ActorPostponeError{Id: id}
		list = append(list, IdErr[A]{Id: id, Err: err})
	}
	return list, nil
}

// AMulticastOnceKeys 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。
func AMulticastOnceKeys[X any, A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, ids map[A]X, req Q) ([]IdErr[A], error) {
	keys := make([]A, 0, len(ids))
	for k := range ids {
		keys = append(keys, k)
	}
	return AMulticastOnce(a, keys, req)
}

// AMulticast 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。对方忙则延后重试。
// 返回列表中 ActorBusyError 或 ActorPostponeError 表示消息已进入延迟队列，将自动重试，调用方不应再次发送同一请求，否则会重复。
// 返回其它错误如消息路由错误等，不进入延迟队列，不会自动重试，通常调用方业务逻辑也不应重试。
func AMulticast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, ids []A, req Q) ([]IdErr[A], error) {
	a.poseponePost()
	p, np := splitPostpone(a, ids)
	next := a.fromSeq(a.from)
	traceLogSend(a.traceSend, a.ilogger, "multicast", next, ids, reqTypeOf(req), req)
	list, err := FMulticast(a.Manager(), next, np, req)
	if err != nil {
		return nil, err
	}
	b, nb := splitIdErr(list)
	result := nb
	for _, id := range b {
		err := &ActorBusyError{Id: id}
		result = append(result, IdErr[A]{Id: id, Err: err})
		appendPostpone(a, id, req)
	}
	for _, id := range p {
		err := &ActorPostponeError{Id: id}
		result = append(result, IdErr[A]{Id: id, Err: err})
		appendPostpone(a, id, req)
	}
	return result, nil
}

// AMulticastKeys 向指定 Group 的一组 Actor 发送 fire-and-forget 消息。
func AMulticastKeys[X any, A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, ids map[A]X, req Q) ([]IdErr[A], error) {
	keys := make([]A, 0, len(ids))
	for k := range ids {
		keys = append(keys, k)
	}
	return AMulticast(a, keys, req)
}

// ABroadcast 向指定 Group 的所有 Actor 广播 fire-and-forget 消息。
func ABroadcast[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](a *ActorControl, req Q) (int, error) {
	next := a.fromSeq(a.from)
	traceLogSend(a.traceSend, a.ilogger, "send broadcast", next, nil, reqTypeOf(req), req)
	return FBroadcast(a.Manager(), next, req)
}

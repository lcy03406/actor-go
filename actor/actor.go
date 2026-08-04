package actor

import (
	"context"
	"log/slog"
	"runtime"
	"sync/atomic"
)

// Actor 泛型 Actor。
type actorRuntime[A ActorId, S anyState] struct {
	ctx     context.Context
	cancel  context.CancelFunc
	id      A
	g       *group[A, S]
	logger  *slog.Logger
	closed  atomic.Bool //外部请求关闭
	mailbox chan invokable[A, S]
	doneCh  chan struct{}
	holder  atomic.Int32 //引用计数，初始1，为0时关闭mailbox
}

func newActor[A ActorId, S anyState](id A, g *group[A, S], bufMails int) *actorRuntime[A, S] {
	ctx, cancel := context.WithCancel(g.ctx)
	return &actorRuntime[A, S]{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		g:       g,
		logger:  slog.With("actor", id.String()),
		mailbox: make(chan invokable[A, S], bufMails),
		doneCh:  make(chan struct{}),
	}
}

func (a *actorRuntime[A, S]) hold() {
	a.holder.Add(1)
}
func (a *actorRuntime[A, S]) unhold() {
	if a.holder.Add(-1) == 0 {
		close(a.mailbox)
	}
}

func (a *actorRuntime[A, S]) send(m invokable[A, S]) error {
	a.mailbox <- m
	return nil
	// select {
	// case a.mailbox <- m:
	// 	return nil
	// default:
	// 	a.logger.Warn("mailbox full, dropping message")
	// 	return &ActorBusyError{a.id}
	// }
}

// requestClose 请求 actor 退出：标记 closed 拒绝新消息，发送 closeMsg 通知 run 退出。
// 幂等。不关闭 mailbox——mailbox 由 GC 回收，避免 send 向已关闭 channel 写入 panic。
func (a *actorRuntime[A, S]) requestClose() {
	if a.closed.CompareAndSwap(false, true) {
		a.unhold() //减掉管理器对actor的引用，为0则会关闭mailbox
	}
}

// kill 强制关闭：cancel ctx（中断 in-flight handler 中监听 ctx.Done 的操作）+ requestClose。
func (a *actorRuntime[A, S]) kill() {
	a.cancel()
	a.requestClose()
}

func (a *actorRuntime[A, S]) run() {
	defer a.g.removeActor(a)
	defer close(a.doneCh)
	buf := make([]invokable[A, S], 0)
	var ctx *ActorContext[A, S]
MainLoop:
	for {
		if a.closed.Load() {
			break MainLoop
		}
		//pop all messages
		buf = a.pumpMailbox(buf)
		if len(buf) == 0 {
			//blocking pop one
			m, ok := <-a.mailbox
			if !ok {
				break MainLoop
			}
			buf = append(buf, m)
			buf = a.pumpMailbox(buf)
		}
		x := 0
		for x < len(buf) {
			if a.closed.Load() {
				buf = buf[x:]
				break MainLoop
			}
			x, ctx = a.invokeBatch(buf, x, ctx)

		}
		buf = buf[:0]
		var ok bool
		if ctx == nil {
			buf, ok = a.g.actorQuit(a, buf)
			if ok {
				break MainLoop
			}
		}
	}
	a.cancel()
	if ctx == nil {
		a.g.actorWake(a.id)
	} else {
		ctx.clear()
		ctx = nil
	}
	closeErr := &ActorClosedError{a.id}
	for _, m := range buf {
		m.Fail(closeErr)
	}
	for m := range a.mailbox {
		m.Fail(closeErr)
	}
	if ctx != nil {
		ctx.clear()
	}
	a.logger.Info("actor closed")
}

func (a *actorRuntime[A, S]) invokeBatch(buf []invokable[A, S], x int, ctx *ActorContext[A, S]) (nx int, nctx *ActorContext[A, S]) {
	var prevActive bool // 当前消息进入 invoke 前的 active 状态，供 panic 恢复结算使用
	defer func() {
		if r := recover(); r != nil {
			stackbuf := make([]byte, 4096)
			n := runtime.Stack(stackbuf, false)
			err, _ := r.(error)
			a.logger.Warn("handler invoke panic recovered; replacing message in-place with recovery",
				"id", a.id, "panic", r, "stack", string(stackbuf[:n]))

			// 结算 idle 计数（与正常路径对称：比较 prevActive 与 nctx.ctrl.active 的跳变）。
			if prevActive && !nctx.ctrl.active {
				a.g.actorIdle(a.id)
			} else if !prevActive && nctx.ctrl.active {
				a.g.actorWake(a.id)
			}
			// 让被 panic 的原消息的 caller 及时拿到错误，不再阻塞。
			buf[nx].Fail(&HandlerCallError{a.id, "", err})

			// 原地替换 buf[nx] 为 recovery 消息，保持队列顺序（后续消息仍在后面）：
			//   - 注册了 PanicReq handler → 替换为 PanicReq invokable，交用户处理（落盘/Open/Quit）；
			//   - 未注册 → 替换为安全占位 panicDropInvoke，actor 保持存活。
			if h, ok := a.g.findHandler("__panic__"); ok {
				if entry, ok2 := h.(*handlerEntry[A, S, *PanicReq[A], *Ok, PanicReq[A], Ok]); ok2 {
					// 防护：recovery handler 自身又 panic，放弃替换，避免无限循环。
					if _, isPanic := buf[nx].(*invoke[A, S, *PanicReq[A], *Ok, PanicReq[A], Ok]); isPanic {
						a.logger.Warn("panic recovery handler panicked; giving up", "id", a.id)
						buf[nx] = &panicDropInvoke[A, S]{err: err}
					} else {
						buf[nx] = &invoke[A, S, *PanicReq[A], *Ok, PanicReq[A], Ok]{
							h:   entry,
							req: &PanicReq[A]{Err: err},
						}
					}
				} else {
					a.logger.Warn("panic handler registered with wrong type; actor keeps alive", "id", a.id)
					buf[nx] = &panicDropInvoke[A, S]{err: err}
				}
			} else {
				a.logger.Warn("no panic recovery handler registered; actor keeps alive", "id", a.id)
				buf[nx] = &panicDropInvoke[A, S]{err: err}
			}
			// 不销毁 actor：保留 nctx（含 state/timers/active），原地替换的消息将在
			// 本轮 for 循环继续被处理。recover 后 body 续跑到下方 if !nctx.ctrl.active 分支，
			// 已激活（active=true）的 actor 不会 clear，保持活跃；未激活的 actor 回 idle 池，仍存活。
		}
	}()
	nctx = ctx
	for nx = x; nx < len(buf); nx++ {
		m := buf[nx]
		spawning := nctx == nil
		if !m.Allow(a.id, spawning) {
			continue
		}
		if spawning {
		// 创建 ctx 且默认 active=false（空闲/未激活），
		// 由用户在回调中调用 Open / grain.State.Activate 翻转为 true。
			nctx = newActorContext(a)
		}
		// 记录进入 invoke 前的 active 状态（spawning 时此处必为 false，早于 OnSpawn，
		// 以便 OnSpawn 内的 Open/Quit 能让 prevActive 与最终 active 的跳变被正确结算）。
		prevActive = nctx.ctrl.active
		if spawning {
			on_spawn := a.g.on_spawn
			if on_spawn != nil {
				err := on_spawn(nctx)
				if err != nil {
					// OnSpawn 初始化失败：丢弃本次创建，Actor 不进入 idle 池也不注册，
					// ctx 直接 clear。随后 nctx==nil，本轮不会再调用 spawn handler，
					// 当前 spawn 消息的 caller 会收到该错误。
					nctx.clear()
					nctx = nil
					// 让当前 spawn 消息的 caller 收到初始化错误（避免永久阻塞）；
					// 随后 continue 跳过 Invoke，避免对 nil ctx 解引用导致 panic。
					m.Fail(err)
					continue
				}
			}
		}
		m.Invoke(nctx, spawning) // 若 panic，被上面 defer 捕获：原地替换 buf[nx] 并保留 nctx
		// 结算 idle 计数：Open/Quit 只翻 active 标记，run loop 比较状态跳变。
		// active: false→true（Open） ⇒ actorWake；true→false（Quit） ⇒ actorIdle。
		if prevActive && !nctx.ctrl.active {
			a.g.actorIdle(a.id)
		} else if !prevActive && nctx.ctrl.active {
			a.g.actorWake(a.id)
		}
		// 退出条件统一为：本轮消息处理完处于空闲态（active=false）。
		// 涵盖 spawn 未 Open、Open 后 Quit 等所有回到空闲池的场景。
		if !nctx.ctrl.active {
			nctx.clear()
			nctx = nil
		}
	}
	return
}

func (a *actorRuntime[A, S]) pumpMailbox(buffer []invokable[A, S]) []invokable[A, S] {
	for {
		select {
		case m := <-a.mailbox:
			buffer = append(buffer, m)
		default:
			return buffer
		}
	}
}

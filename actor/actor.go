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

func newActor[A ActorId, S anyState](id A, g *group[A, S], capacity int) *actorRuntime[A, S] {
	ctx, cancel := context.WithCancel(g.ctx)
	return &actorRuntime[A, S]{
		ctx:     ctx,
		cancel:  cancel,
		id:      id,
		g:       g,
		logger:  slog.With("actor", id.String()),
		mailbox: make(chan invokable[A, S], capacity),
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
	defer func() {
		if r := recover(); r != nil {
			stackbuf := make([]byte, 4096)
			n := runtime.Stack(stackbuf, false)
			stackInfo := string(stackbuf[:n])
			a.logger.Warn("handler invoke panic", "id", a.id, "panic", r, "stack", stackInfo)
			err, _ := r.(error)
			a.logger.Warn("handler invoke panic", "id", a.id, "panic", r)
			buf[nx].Fail(&HandlerCallError{a.id, "", err})
			if nctx.idle {
				a.g.actorIdle(a.id)
				nctx.clear()
				nctx = nil
			}
			buf[nx] = &panicRecoverInvoke[A, S]{err: err}
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
			nctx = newActorContext(a)
			a.g.actorWake(a.id)
		}
		m.Invoke(nctx, spawning)
		if nctx.idle {
			a.g.actorIdle(a.id)
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

package actor

type invokable[A ActorId, S anyState] interface {
	Allow(id A, spawning bool) bool
	Invoke(actor *ActorContext[A, S], spawning bool)
	Fail(err error)
}

type result[R PtrReply[R0], R0 any] struct {
	Rep R
	Err error
}

type invoke[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] struct {
	from  string
	h     *handlerEntry[A, S, Q, R, Q0, R0]
	req   Q
	ch    chan result[R, R0]
	clean func(R)
}

func (i *invoke[A, S, Q, R, Q0, R0]) CanSpawn() bool {
	return i.h.allow_spawn
}

func (i *invoke[A, S, Q, R, Q0, R0]) Allow(id A, spawning bool) bool {
	if (spawning && !i.h.allow_spawn) || (!spawning && !i.h.allow_query) {
		if i.ch != nil {
			i.ch <- result[R, R0]{Err: &HandlerNotAllowedError{id, i.h.reqType}}
		}
		return false
	}
	return true
}

func (i *invoke[A, S, Q, R, Q0, R0]) Invoke(actor *ActorContext[A, S], spawning bool) {
	traceRecv := actor.actor.g.options.TraceRecv
	actor.ctrl.invokeLogger(i.from)
	defer actor.ctrl.invokeLogger("")
	traceLogRecv(traceRecv, actor.Logger(), "recv invoke", reqTypeOf(i.req), i.req)
	rep, err := i.h.fn(actor, i.req, spawning)
	if i.ch != nil {
		if rep != nil && i.clean != nil {
			defer func() {
				if r := recover(); r != nil {
					i.clean(rep)
				}
			}()
		}
		traceLogRecv(traceRecv, actor.Logger(), "send reply", reqTypeOf(i.req), rep)
		i.ch <- result[R, R0]{rep, err}
	}
	traceLogRecv(traceRecv, actor.Logger(), "recv return", reqTypeOf(i.req), nil)
}

func (i *invoke[A, S, Q, R, Q0, R0]) Fail(err error) {
	if i.ch != nil {
		i.ch <- result[R, R0]{Err: err}
	}
}

// panicDropInvoke 是 handler panic 且用户未注册 PanicReq recovery handler 时，
// 用于原地替换原消息的安全占位：Invoke 为空操作（不再次 panic），
// Fail 为空操作。actor 保持存活——ctx 原样保留，idle 状态按 panic 前 settle，
// 本轮结束后 actor 回到空闲池，等待后续消息。
type panicDropInvoke[A ActorId, S anyState] struct {
	err error
}

func (p *panicDropInvoke[A, S]) Allow(_ A, _ bool) bool               { return true }
func (p *panicDropInvoke[A, S]) Invoke(_ *ActorContext[A, S], _ bool) {}
func (p *panicDropInvoke[A, S]) Fail(_ error)                         {}

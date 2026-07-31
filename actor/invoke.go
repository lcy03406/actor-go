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

func errorResult[R PtrReply[R0], R0 any](err error) (res result[R, R0]) {
	res.Err = err
	return
}

type invoke[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] struct {
	h   *handlerEntry[A, S, Q, R, Q0, R0]
	req Q
	ch  chan result[R, R0]
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
	rep, err := i.h.fn(actor, i.req, spawning)
	if i.ch != nil {
		i.ch <- result[R, R0]{rep, err}
	}
}

func (i *invoke[A, S, Q, R, Q0, R0]) Fail(err error) {
	if i.ch != nil {
		i.ch <- result[R, R0]{Err: err}
	}
}

// panicRecoverInvoke 是 handler panic 恢复后插入的占位消息，
// 不调用用户 handler，只打日志。后续请求照常处理。
type panicRecoverInvoke[A ActorId, S anyState] struct {
	err error
}

func (p *panicRecoverInvoke[A, S]) Allow(_ A, _ bool) bool { return true }
func (p *panicRecoverInvoke[A, S]) Invoke(actor *ActorContext[A, S], _ bool) {
	actor.Logger().Warn("handler panic recovered, resuming", "err", p.err)
}
func (p *panicRecoverInvoke[A, S]) Fail(_ error) {}

package actor

type HandlerFunc[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] func(actor *ActorContext[A, S], req Q, spawning bool) (R, error)

// handler 是擦除具体请求/回复类型的 handler 接口。
// 允许 registry map 存储不同类型的 handler。
type handler[A ActorId] interface {
	ReqType() string
}

type handlerBase[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] interface {
	handlerCall(from From, gb groupBase[A], id A, req Q, recoverFn func(R)) (chan result[R, R0], error)
	handlerPost(from From, gb groupBase[A], id A, req Q) error
	handlerBroadcast(from From, gb groupBase[A], req Q) (int, error)
	handlerMulticast(from From, gb groupBase[A], ids []A, req Q) ([]IdErr[A], error)
}

type handlerEntry[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] struct {
	reqType                  string
	allow_spawn, allow_query bool
	fn                       HandlerFunc[A, S, Q, R, Q0, R0]
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) ReqType() string {
	return h.reqType
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerCall(from From, gb groupBase[A], id A, req Q, clean func(R)) (chan result[R, R0], error) {
	g := gb.(*group[A, S])
	a, err := g.resolveActor(id, h.allow_spawn)
	if err != nil {
		return nil, err
	}
	defer a.unhold()
	ch := make(chan result[R, R0], 1)
	i := &invoke[A, S, Q, R, Q0, R0]{
		from:  from,
		h:     h,
		req:   req,
		ch:    ch,
		clean: clean,
	}
	if err := a.send(i); err != nil {
		return nil, err
	}
	return ch, nil
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerPost(from From, gb groupBase[A], id A, req Q) error {
	g := gb.(*group[A, S])
	a, err := g.resolveActor(id, h.allow_spawn)
	if err != nil {
		return err
	}
	defer a.unhold()
	i := &invoke[A, S, Q, R, Q0, R0]{
		from: from,
		h:    h,
		req:  req,
	}
	if err := a.send(i); err != nil {
		return err
	}
	return nil
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerBroadcast(from From, gb groupBase[A], req Q) (int, error) {
	g := gb.(*group[A, S])
	i := &invoke[A, S, Q, R, Q0, R0]{
		from: from,
		h:    h,
		req:  req,
	}
	return g.broadcast(i)
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerMulticast(from From, gb groupBase[A], ids []A, req Q) ([]IdErr[A], error) {
	g := gb.(*group[A, S])
	i := &invoke[A, S, Q, R, Q0, R0]{
		from: from,
		h:    h,
		req:  req,
	}
	return g.multicast(ids, i)
}

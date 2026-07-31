package actor

type handlerFunc[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] func(actor *ActorContext[A, S], req Q, spawning bool) (R, error)

// handler 是擦除具体请求/回复类型的 handler 接口。
// 允许 registry map 存储不同类型的 handler。
type handler[A ActorId] interface {
	ReqType() string
}

type handlerBase[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] interface {
	handlerCall(gb groupBase[A], id A, req Q) (chan result[R, R0], error)
	handlerPost(gb groupBase[A], id A, req Q) error
	handlerBroadcast(gb groupBase[A], req Q) (int, error)
	handlerMulticast(gb groupBase[A], ids []A, req Q) (int, error)
}

type handlerEntry[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any] struct {
	reqType                  string
	allow_spawn, allow_query bool
	fn                       handlerFunc[A, S, Q, R, Q0, R0]
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) ReqType() string {
	return h.reqType
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerCall(gb groupBase[A], id A, req Q) (chan result[R, R0], error) {
	g := gb.(*group[A, S])
	a := g.resolveActor(id, h.allow_spawn)
	if a == nil {
		return nil, resolveError(g, id, h.allow_spawn)
	}
	defer a.unhold()
	ch := make(chan result[R, R0], 1)
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   h,
		req: req,
		ch:  ch,
	}
	if err := a.send(i); err != nil {
		return nil, err
	}
	return ch, nil
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerPost(gb groupBase[A], id A, req Q) error {
	g := gb.(*group[A, S])
	a := g.resolveActor(id, h.allow_spawn)
	if a == nil {
		return resolveError(g, id, h.allow_spawn)
	}
	defer a.unhold()
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   h,
		req: req,
	}
	if err := a.send(i); err != nil {
		return err
	}
	return nil
}

// resolveError 根据 spawn 失败原因返回精确错误。
func resolveError[A ActorId, S anyState](g *group[A, S], id A, allowSpawn bool) error {
	if g.isStopping() {
		return &SpawnRefusedError{Id: id, Reason: "group stopping"}
	}
	if !allowSpawn {
		return &ActorNotFoundError{Id: id}
	}
	return &SpawnRefusedError{Id: id, Reason: "spawn failed"}
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerBroadcast(gb groupBase[A], req Q) (int, error) {
	g := gb.(*group[A, S])
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   h,
		req: req,
	}
	return g.broadcast(i)
}

func (h *handlerEntry[A, S, Q, R, Q0, R0]) handlerMulticast(gb groupBase[A], ids []A, req Q) (int, error) {
	g := gb.(*group[A, S])
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   h,
		req: req,
	}
	return g.multicast(ids, i)
}

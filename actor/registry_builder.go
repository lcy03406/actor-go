package actor

// RegistryBuilder 将注册阶段与运行阶段分离。
type RegistryBuilder[A ActorId, S anyState] struct {
	handlers map[string]handler[A]
}

// newRegistryBuilder 创建一个新的 RegistryBuilder。
func newRegistryBuilder[A ActorId, S anyState]() *RegistryBuilder[A, S] {
	return &RegistryBuilder[A, S]{
		handlers: make(map[string]handler[A]),
	}
}

func register[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	allow_spawn, allow_query bool,
	fn handlerFunc[A, S, Q, R, Q0, R0],
) {
	reqType := reqTypeOf[A, Q, R]()
	b.handlers[reqType] = &handlerEntry[A, S, Q, R, Q0, R0]{
		reqType:     reqType,
		allow_spawn: allow_spawn,
		allow_query: allow_query,
		fn:          fn,
	}
}

// RegisterSpawn 注册 spawn 处理器（首次消息创建 Actor，不等待回复）。
func RegisterSpawn[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	fn handlerFunc[A, S, Q, R, Q0, R0],
) {
	register(b, true, false, fn)
}

func RegisterQuery[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	fn handlerFunc[A, S, Q, R, Q0, R0],
) {
	register(b, false, true, fn)
}

func RegisterServe[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	fn handlerFunc[A, S, Q, R, Q0, R0],
) {
	register(b, true, true, fn)
}

// RegisterSpawnHandler 注册 spawn 处理器（首次消息创建 Actor，不等待回复）。
func RegisterSpawnHandler[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, false, fn)
}

func RegisterQueryHandler[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, false, true, fn)
}

func RegisterServeHandler[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, true, fn)
}

func RegisterSpawnHandler2[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	_ Q,
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, false, fn)
}

func RegisterQueryHandler2[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	_ Q,
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, false, true, fn)
}

func RegisterServeHandler2[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	_ Q,
) {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, true, fn)
}

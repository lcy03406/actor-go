package actor

// RegistryBuilder 将注册阶段与运行阶段分离。
type RegistryBuilder[A ActorId, S anyState] struct {
	handlers map[string]handler[A]
	on_spawn OnSpawnFn[A, S]
}

// NewRegistryBuilder 创建一个新的 RegistryBuilder。
func NewRegistryBuilder[A ActorId, S anyState]() *RegistryBuilder[A, S] {
	return &RegistryBuilder[A, S]{
		handlers: make(map[string]handler[A]),
	}
}

// SetOnSpawn 设置该 Group 的 OnSpawn 钩子，在每个 Actor 首次创建（收到第一条 spawn 消息）时、
// 用户的 spawn/serve handler 之前被自动调用一次，用于初始化状态或资源。
// 钩子返回非 nil error 会中止本次创建，当前 spawn 消息的 caller 将收到该错误。
// 详见 OnSpawnFn 的文档。未设置（或为 nil）则跳过。
func (b *RegistryBuilder[A, S]) SetOnSpawn(on_spawn OnSpawnFn[A, S]) {
	b.on_spawn = on_spawn
}

func register[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	allow_spawn, allow_query bool,
	fn handlerFunc[A, S, Q, R, Q0, R0],
) {
	reqType := reqTypeOf[A, Q]()
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
	typedNil Q,
) Q {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, false, fn)
	return typedNil
}

func RegisterQueryHandler2[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	typedNil Q,
) Q {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, false, true, fn)
	return typedNil
}

func RegisterServeHandler2[A ActorId, S anyState, Q RequestHandler[A, S, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](
	b *RegistryBuilder[A, S],
	typedNil Q,
) Q {
	fn := func(actor *ActorContext[A, S], req Q, spawning bool) (R, error) {
		return req.Handle(actor, spawning)
	}
	register(b, true, true, fn)
	return typedNil
}

package actor

// ActorType 是 Actor 的类型标识（等同于 Group 名），用于路由与注册查找。
type ActorType string

// ActorIdBase 是 ActorId 的最小接口，描述了任意 ActorId 必须提供的能力。
// 仅用于在对 A 的具体类型无约束的上下文中（如错误对象、日志）传递 ID。
type ActorIdBase interface {
	ActorType() ActorType // 返回 Actor 的类型（等同于 Group 名）。
	String() string       // 返回 Actor 的 ID 字符串，用于日志/调试。
}

// ActorId 是所有 Actor ID 必须实现的接口。
type ActorId interface {
	comparable
	ActorIdBase
}

func actorTypeOf[A ActorId]() ActorType {
	var id A
	return id.ActorType()
}

type anyState interface{}

// PtrReply 要求 reply 为指针类型 (~*R0)，handler 返回堆分配的指针。
// 看似比值返回多一次堆分配，但跨 goroutine 传递时值返回的变量同样逃逸到堆
// （&reply 被 invoke 结构体持有），实际无差异。指针返回省掉了 v1 的 any 装箱
// 和类型断言，且避免了跨核写入 caller 栈带来的 cache line 污染。
type PtrReply[R0 any] interface {
	~*R0
}

// SafeReply 是"需显式释放"的回复类型约束，在 PtrReply 基础上额外要求实现 Close 方法。
//
// "安全"仅指一条自动释放通道：当 handler 成功返回 reply，且该 reply 正常送达调用方时，
// 框架会在调用方读取完回复后自动调用 Close 释放资源。
//
// 除此之外的情况仍需调用方或 handler 自行负责释放，否则会泄漏：
//   - 调用方拿到 reply 后若想继续持有（按业务需要暂存），则不应依赖框架自动 Close，
//     应在用完该 reply 后自行调用 Close；
//   - 若 handler 返回 error（而非 reply），框架不会自动 Close，临时创建的 reply
//     必须由 handler 在返回错误前自行释放；
//   - 若回复在传递途中被丢弃（如 context 取消），框架也会尝试 Close。
//
// SafeReply 仅用于 SafeCall / RefSafeCall 这两条安全回复路径；对应的普通路径
// Call / RefCall 使用 PtrReply，不要求 Close。
type SafeReply[R0 any] interface {
	~*R0
	Close()
}

// Request 是所有请求类型必须实现的接口。
// A 限定请求所属的 ActorId 类型，R 限定请求的回复类型。
// ReqType 方法的参数类型(A, *R)确保编译器能检查 Q 与 A、R 的匹配关系。
// 例如 func (*TestAdd) ReqType(_ TestActorId, _ *TestAddReply) string 只能匹配
// Request[TestActorId, TestAddReply]，无法匹配其他 A/R 组合。
type Request[A ActorId, R PtrReply[R0], Q0 any, R0 any] interface {
	~*Q0
	ReqType(A, R) string
}

// RequestHandler 是"类型安全 handler"版本的请求接口。
// 在 Request 基础上额外要求实现 Handle 方法，使请求类型自身携带处理逻辑，
// 从而可用 RegisterSpawnHandler / RegisterServeHandler 等以结构体方法注册 handler，
// 无需单独编写闭包函数。A 限定 ActorId 类型，S 限定 State 类型，R 限定回复类型。
type RequestHandler[A ActorId, S anyState, R PtrReply[R0], Q0 any, R0 any] interface {
	~*Q0
	ReqType(A, R) string
	Handle(actor *ActorContext[A, S], spawning bool) (R, error)
}

func reqTypeOf[A ActorId, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any]() string {
	var id A
	return Q.ReqType(nil, id, nil)
}

// Ok 是表示"成功"的通用回复，等同于 struct{}。
type Ok = struct{}
type OkReply = *Ok

var OK OkReply = &Ok{}

// PanicReq 是框架在 handler panic 时投递给用户的内置消息。
//
// 用户通过 RegisterServe(b, (*PanicReq[ActorId])(nil)) 注册 recovery handler，
// 在 Handle 中读取 Err 自行决定后续行为：
//   - Persist / Save：把（可能不完整的）数据落盘；
//   - Open()：继续运行（忽略本次 panic）；
//   - Quit()：主动退出。
//
// 若用户未注册 PanicReq 的 handler，框架仅记录日志，actor 保持存活
// （panic 不会强制销毁 actor）。
//
// 注意：panic 恢复后 actor 的 ctx 会被原样保留（idle 状态按 panic 前 settle），
// 下一轮消息处理会取出这条 PanicReq 交给用户 handler。
type PanicReq[A ActorId] struct {
	Err error
}

func (*PanicReq[A]) ReqType(_ A, _ *Ok) string { return "__panic__" }

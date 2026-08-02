package actor

type ActorType string

type ActorIdBase interface {
	ActorType() ActorType
	String() string
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

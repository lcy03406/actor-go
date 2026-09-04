package actor

import "fmt"

// GroupNotFoundError 表示目标 Actor 所属的 Group（ActorType）不存在。
// 通常因为该 Actor 类型尚未通过 Manager.AddGroup 注册。
type GroupNotFoundError struct {
	Id ActorIdBase
}

func (e *GroupNotFoundError) Error() string {
	return fmt.Sprintf("actor group not found: %s", e.Id)
}

// GroupClosedError 表示 Group 正在关闭。
type GroupClosedError struct {
	Id ActorIdBase
}

func (e *GroupClosedError) Error() string {
	return fmt.Sprintf("actor group closed: %s", e.Id)
}

// ActorNotFoundError 表示目标 Actor 不存在（未 spawn 且不允许 spawn）。
type ActorNotFoundError struct {
	Id ActorIdBase
}

func (e *ActorNotFoundError) Error() string {
	return fmt.Sprintf("actor not found: %s", e.Id)
}

// ActorClosedError 表示目标 Actor 已关闭（正在退出或已退出）。
// 对已关闭 Actor 发送消息、获取引用或调用时返回该错误。
type ActorClosedError struct {
	Id ActorIdBase
}

func (e *ActorClosedError) Error() string {
	return fmt.Sprintf("actor closed: %s", e.Id)
}

// ActorBusyError 表示目标 Actor 正忙，暂时无法接受新的请求。
// 发送消息时目标信箱已满。
type ActorBusyError struct {
	Id ActorIdBase
}

func (e *ActorBusyError) Error() string {
	return fmt.Sprintf("actor busy: %s", e.Id)
}

// ActorPostponeError 表示目标 Actor 正忙，暂时无法接受新的请求。
// 发送消息前目标信箱已满。
type ActorPostponeError struct {
	Id ActorIdBase
}

func (e *ActorPostponeError) Error() string {
	return fmt.Sprintf("actor postpone: %s", e.Id)
}

// HandlerNotFoundError 表示目标 Actor 上未注册对应请求类型的 handler。
// Id 为 Actor ID，Req 为未找到的 reqType。
type HandlerNotFoundError struct {
	Id  ActorIdBase
	Req string
}

func (e *HandlerNotFoundError) Error() string {
	return fmt.Sprintf("handler not found: %s:%s", e.Id, e.Req)
}

// HandlerNotAllowedError 表示请求类型已注册，但当前阶段不允许处理该请求。
// 例如：未注册为 spawn 的消息在 Actor 尚未创建（spawning=true）时到达，
// 或未注册为 query 的消息在 Actor 已存在（spawning=false）时到达。
type HandlerNotAllowedError struct {
	Id  ActorIdBase
	Req string
}

func (e *HandlerNotAllowedError) Error() string {
	return fmt.Sprintf("handler not allowed: %s:%s", e.Id, e.Req)
}

// HandlerCallError 表示 Actor 调用过程中发生异常。
type HandlerCallError struct {
	ActorId ActorIdBase
	Req     string
	Cause   error
}

func (e *HandlerCallError) Error() string {
	return fmt.Sprintf("handler call error [%s]: %v", e.ActorId, e.Cause)
}

func (e *HandlerCallError) Unwrap() error {
	return e.Cause
}

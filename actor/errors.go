package actor

import "fmt"

type GroupNotFoundError struct {
	Id ActorIdBase
}

func (e *GroupNotFoundError) Error() string {
	return fmt.Sprintf("actor group not found: %s", e.Id)
}

// ActorNotFoundError 表示目标 Actor 不存在（未 spawn 且不允许 spawn）。
type ActorNotFoundError struct {
	Id ActorIdBase
}

func (e *ActorNotFoundError) Error() string {
	return fmt.Sprintf("actor not found: %s", e.Id)
}

// SpawnRefusedError 表示 Actor spawn 被拒绝。
// 可能原因：Group 正在关闭、集群中非 owner 节点、容量已满等。
// 上层（cluster/grain）可据此决定重定向还是返回错误给调用方。
type SpawnRefusedError struct {
	Id     ActorIdBase
	Reason string
}

func (e *SpawnRefusedError) Error() string {
	return fmt.Sprintf("spawn refused for %s: %s", e.Id, e.Reason)
}

type ActorClosedError struct {
	Id ActorIdBase
}

func (e *ActorClosedError) Error() string {
	return fmt.Sprintf("actor closed: %s", e.Id)
}

type ActorBusyError struct {
	Id ActorIdBase
}

func (e *ActorBusyError) Error() string {
	return fmt.Sprintf("actor busy: %s", e.Id)
}

type HandlerNotFoundError struct {
	Id  ActorIdBase
	Req string
}

func (e *HandlerNotFoundError) Error() string {
	return fmt.Sprintf("handler not found: %s:%s", e.Id, e.Req)
}

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

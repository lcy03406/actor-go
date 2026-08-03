package actor

import (
	"context"
)

// ActorRef 是 Actor 之间的直连引用，绕过 Group 查找。
// 持有目标 Actor 的引用计数，阻止其 idle 退出。
//
// 适用于两类 Actor 有明确对应关系的场景（如 Player→Room, Order→User）。
// 消息直接投递到目标 Actor 的 mailbox，无需经过 Manager→Group→handler 的查找链路。
//
// 用法：
//
//	// 在 handler 中获取对另一个 Actor 的引用
//	ref := ctx.Ref(roomId)
//	if ref == nil {
//	    // 未找到：返回用户的业务错误（此处 ErrRoomNotFound 仅为示例）
//	    return nil, ErrRoomNotFound
//	}
//	defer ref.Release() // 用完后释放，允许目标 idle 退出
//
//	err := actor.RefPost(ref, &JoinRoom{PlayerId: ctx.Id()})
//	reply, err := actor.RefCall(ctx, ref, &GetRoomInfo{})
type ActorRef[A ActorId, S anyState] struct {
	target *actorRuntime[A, S]
	mgr    *Manager
}

// Release 释放对目标 Actor 的引用计数。
// 调用后 ActorRef 不再有效，目标 Actor 在 idle 时可以正常退出。
// 幂等：重复调用安全。
func (r *ActorRef[A, S]) Release() {
	if r.target != nil {
		r.target.unhold()
		r.target = nil
	}
}

// Valid 检查引用是否仍然有效（未释放且目标 Actor 未关闭）。
func (r *ActorRef[A, S]) Valid() bool {
	return r.target != nil && !r.target.closed.Load()
}

// Id 返回目标 Actor 的 ID。
func (r *ActorRef[A, S]) Id() A {
	return r.target.id
}

// RefPost 通过 ActorRef 向目标 Actor 发送 fire-and-forget 消息，绕过 Group 查找。
// 消息处理仍受 handler 注册类型的 spawn/query 约束，与标准 Post 语义一致。
func RefPost[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](ref *ActorRef[A, S], req Q) error {
	if ref.target.closed.Load() {
		return &ActorClosedError{ref.target.id}
	}
	gh, err := findHandler(ref.mgr, ref.target.id, req)
	if err != nil {
		return err
	}
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   gh.h.(*handlerEntry[A, S, Q, R, Q0, R0]),
		req: req,
	}
	return ref.target.send(i)
}

// RefCall 通过 ActorRef 向目标 Actor 发送请求并等待回复，绕过 Group 查找。
// 消息处理仍受 handler 注册类型的 spawn/query 约束，与标准 Call 语义一致。
func RefCall[A ActorId, S anyState, Q Request[A, R, Q0, R0], R PtrReply[R0], Q0 any, R0 any](ctx context.Context, ref *ActorRef[A, S], req Q) (R, error) {
	if ref.target.closed.Load() {
		var zero R
		return zero, &ActorClosedError{ref.target.id}
	}
	gh, err := findHandler(ref.mgr, ref.target.id, req)
	if err != nil {
		var zero R
		return zero, err
	}
	ch := make(chan result[R, R0], 1)
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:   gh.h.(*handlerEntry[A, S, Q, R, Q0, R0]),
		req: req,
		ch:  ch,
	}
	if err := ref.target.send(i); err != nil {
		var zero R
		return zero, err
	}
	return waitResult(ctx, ch)
}

// RefSafeCall 通过 ActorRef 向目标 Actor 发送请求并等待回复，绕过 Group 查找。
// 消息处理仍受 handler 注册类型的 spawn/query 约束，与标准 Call 语义一致。
func RefSafeCall[A ActorId, S anyState, Q Request[A, R, Q0, R0], R SafeReply[R0], Q0 any, R0 any](ctx context.Context, ref *ActorRef[A, S], req Q) (R, error) {
	if ref.target.closed.Load() {
		var zero R
		return zero, &ActorClosedError{ref.target.id}
	}
	gh, err := findHandler(ref.mgr, ref.target.id, req)
	if err != nil {
		var zero R
		return zero, err
	}
	ch := make(chan result[R, R0], 1)
	i := &invoke[A, S, Q, R, Q0, R0]{
		h:     gh.h.(*handlerEntry[A, S, Q, R, Q0, R0]),
		req:   req,
		ch:    ch,
		clean: R.Close,
	}
	if err := ref.target.send(i); err != nil {
		var zero R
		return zero, err
	}
	return safeResult(ctx, ch)
}

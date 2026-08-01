package rpc

import (
	"github.com/lcy03406/actor-go/actor"
)

type registryKey struct {
	actorType actor.ActorType
	reqType   string
}

// RegistryBuilder 是 rpc 请求注册表构建器，收集所有需跨节点路由的请求类型。
// 调用 RegisterRequest 登记请求后，由 NewServerWith 消费的 entryMap 即作为 Server 的路由表。
// 该类型不承载实际处理逻辑（handler 仍在 actor 层注册），仅用于让 rpc 层
// 识别 reqType 并完成消息编解码与转发。
type RegistryBuilder[M Message, C Codec[M]] struct {
	entryMap map[registryKey]entry[M, C]
}

// NewRegistryBuilder 创建一个新的 RegistryBuilder。
func NewRegistryBuilder[M Message, C Codec[M]]() *RegistryBuilder[M, C] {
	return &RegistryBuilder[M, C]{
		entryMap: make(map[registryKey]entry[M, C]),
	}
}

// RegisterRequest 在注册表中登记一个请求类型，使其可被 rpc 层识别与路由。
// 仅写入 (actorType, reqType) 对应的空 entry，不涉及实际处理逻辑——
// 真正的 handler 仍由 actor.RegistryBuilder 在本地注册。req 用于推导
// 请求/回复的类型参数与 reqType 键，通常传入对应请求类型的 nil 指针。
func RegisterRequest[M Message, C Codec[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](b *RegistryBuilder[M, C], req Q) {
	var id0 A
	key := registryKey{id0.ActorType(), req.ReqType(id0, nil)}
	e := reqEntry[M, C, A, Q, R, Q0, R0]{}
	b.entryMap[key] = e
}

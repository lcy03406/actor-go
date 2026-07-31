package rpc

import (
	"github.com/lcy03406/actor-go/actor"
)

type registryKey struct {
	actorType actor.ActorType
	reqType   string
}

type RegustryBuilder[M Message, C Codec[M]] struct {
	entryMap map[registryKey]entry[M, C]
}

func RegisterRequest[M Message, C Codec[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](b *RegustryBuilder[M, C], req Q) {
	var id0 A
	key := registryKey{id0.ActorType(), req.ReqType(id0, nil)}
	e := reqEntry[M, C, A, Q, R, Q0, R0]{}
	b.entryMap[key] = e
}

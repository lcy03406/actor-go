package rpc

import (
	"context"

	"github.com/lcy03406/actor-go/actor"
)

type entry[M Message, C Codec[M]] interface {
	post(mgr *actor.Manager, idM M, reqM M) error
	call(ctx context.Context, mgr *actor.Manager, idM M, reqM M) (M, error)
	broadcast(mgr *actor.Manager, reqM M) (int, error)
	multicast(mgr *actor.Manager, idsM []M, reqM M) (int, error)
}

type reqEntry[M Message, C Codec[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any] struct{}

func (e reqEntry[M, C, A, Q, R, Q0, R0]) post(mgr *actor.Manager, idM M, reqM M) (err error) {
	var c C
	var id A
	req := Q(new(Q0))
	err = c.Decode(idM, &id)
	if err != nil {
		return
	}
	err = c.Decode(reqM, req)
	if err != nil {
		return
	}
	return actor.Post(mgr, id, req)
}

func (e reqEntry[M, C, A, Q, R, Q0, R0]) call(ctx context.Context, mgr *actor.Manager, idM M, reqM M) (repM M, err error) {
	var c C
	var id A
	req := Q(new(Q0))
	err = c.Decode(idM, &id)
	if err != nil {
		return
	}
	err = c.Decode(reqM, req)
	if err != nil {
		return
	}
	rep, err := actor.Call(ctx, mgr, id, req)
	if err != nil {
		return
	}
	return c.Encode(rep)
}

func (e reqEntry[M, C, A, Q, R, Q0, R0]) broadcast(mgr *actor.Manager, reqM M) (n int, err error) {
	var c C
	req := Q(new(Q0))
	err = c.Decode(reqM, req)
	if err != nil {
		return
	}
	return actor.Broadcast(mgr, req)
}

func (e reqEntry[M, C, A, Q, R, Q0, R0]) multicast(mgr *actor.Manager, idsM []M, reqM M) (n int, err error) {
	var c C
	ids := make([]A, 0, len(idsM))
	for _, idM := range idsM {
		var id A
		err = c.Decode(idM, &id)
		if err != nil {
			return
		}
		ids = append(ids, id)
	}
	req := Q(new(Q0))
	err = c.Decode(reqM, req)
	if err != nil {
		return
	}
	var list []actor.IdErr[A]
	list, err = actor.Multicast(mgr, ids, req)
	return actor.CountNoErr(list), err
}

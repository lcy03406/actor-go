package player

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type CheckOwnership struct {
	placement cluster.PlacementStrategy
	selfID    string
}

func (*CheckOwnership) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "CheckOwnership" }

func (req *CheckOwnership) Handle(ctx *types.PlayerActorCtx, spawning bool) (actor.OkReply, error) {
	if req.placement == nil {
		return actor.OK, nil
	}
	_, leave := cluster.CheckOwnership(req.placement, nil, req.selfID, "Player", ctx.Id().String())
	if leave {
		log.Printf("[迁移] Player %s 应迁移 (HP=%d Level=%d Gold=%d)",
			ctx.Id(), ctx.State().Data.HP, ctx.State().Data.Level, ctx.State().Data.Attr.Gold)
	}
	return actor.OK, nil
}

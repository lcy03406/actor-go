package player

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type AddGold struct {
	Amount int `json:"amount"`
}

type AddGoldReply struct {
	NewGold int `json:"newGold"`
}

func (*AddGold) ReqType(_ types.PlayerId, _ *AddGoldReply) string { return "AddGold" }

func (req *AddGold) Handle(ctx *types.PlayerActorCtx, spawning bool) (*AddGoldReply, error) {
	data := &ctx.State().Data
	data.Attr.AddGold(req.Amount)
	log.Printf("[Player] %s 获得 %d 金币, 总计=%d", ctx.Id(), req.Amount, data.Attr.Gold)
	return &AddGoldReply{NewGold: data.Attr.Gold}, nil
}

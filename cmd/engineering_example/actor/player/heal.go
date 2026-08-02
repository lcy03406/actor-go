package player

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type Heal struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ types.PlayerId, _ *HealReply) string { return "Heal" }

func (req *Heal) Handle(ctx *types.PlayerActorCtx, spawning bool) (*HealReply, error) {
	data := &ctx.State().Data
	data.HP += req.Amount
	log.Printf("[Player] %s 治疗 %d, HP=%d", ctx.Id(), req.Amount, data.HP)
	return &HealReply{NewHP: data.HP}, nil
}

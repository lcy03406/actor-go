package player

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type Close struct{}

func (*Close) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "Close" }

func (req *Close) Handle(ctx *types.PlayerActorCtx, spawning bool) (actor.OkReply, error) {
	log.Printf("[Player] %s 退出", ctx.Id())
	ctx.State().Deactivate(ctx) // 持久化 + 释放租约 + Quit
	return actor.OK, nil
}

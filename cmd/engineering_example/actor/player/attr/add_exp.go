// add_exp.go 定义 AddExp 请求的薄入口（handler）。
//
// 业务规则已下沉到 PlayerState.AddExp（见 types/methods.go）：加经验、按需升级
// （提升 Atr.Atk/Def、MaxHP 并回满血）。本 handler 仅做：取参数 → 调 AddExp → 持久化 → 返回。
//
// 【依赖】types/，不依赖 player 包。
package attr

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type AddExp struct {
	Amount int `json:"amount"`
}

type AddExpReply struct {
	Exp     int  `json:"exp"`
	Level   int  `json:"level"`
	LevelUp bool `json:"levelUp"`
}

func (*AddExp) ReqType(_ types.PlayerId, _ *AddExpReply) string { return "AddExp" }

func (req *AddExp) Handle(ctx *types.PlayerActorCtx, spawning bool) (*AddExpReply, error) {
	data := &ctx.State().Data
	exp, level, levelUp := data.AddExp(req.Amount)
	if levelUp {
		ctx.State().Persist(ctx) // 升级时持久化
		log.Printf("[Player.Attr] %s 升级! Level=%d Atk=%d Def=%d 回满血=%d",
			ctx.Id(), data.Level, data.Attr.Atk, data.Attr.Def, data.HP)
	} else {
		log.Printf("[Player.Attr] %s 获得 %d 经验, Exp=%d", ctx.Id(), req.Amount, exp)
	}
	return &AddExpReply{Exp: exp, Level: level, LevelUp: levelUp}, nil
}

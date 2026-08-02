// attr 子包定义属性模块的所有请求。
//
// 【依赖】types/ + actor + logic/，不依赖 player 包。
package attr

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
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
	attr := &data.Attr
	attr.Exp += req.Amount

	levelUp := false
	// 使用 logic 包计算升级所需经验
	for attr.Exp >= logic.CalcExpToLevel(data.Level) {
		attr.Exp -= logic.CalcExpToLevel(data.Level)
		data.Level++
		attr.Atk += 5
		attr.Def += 3
		levelUp = true
	}

	if levelUp {
		ctx.State().Persist(ctx) // 升级时持久化
		log.Printf("[Player.Attr] %s 升级! Level=%d Atk=%d Def=%d",
			ctx.Id(), data.Level, attr.Atk, attr.Def)
	} else {
		log.Printf("[Player.Attr] %s 获得 %d 经验, Exp=%d", ctx.Id(), req.Amount, attr.Exp)
	}

	return &AddExpReply{Exp: attr.Exp, Level: data.Level, LevelUp: levelUp}, nil
}

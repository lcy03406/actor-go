// skill 子包定义技能模块的所有请求。
//
// 【依赖】types/ + actor + logic/，不依赖 player 包。
package skill

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// ControlLearn 是玩家主动学习技能的意图入口（Control* = 玩家控制）。
type ControlLearn struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Cost int    `json:"cost"`
}

type ControlLearnReply struct {
	Learned bool   `json:"learned"`
	Reason  string `json:"reason"`
}

func (*ControlLearn) ReqType(_ types.PlayerId, _ *ControlLearnReply) string { return "ControlLearn" }

func (req *ControlLearn) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlLearnReply, error) {
	data := &ctx.State().Data
	sk := &data.Skill

	for _, s := range sk.Learned {
		if s.ID == req.ID {
			return &ControlLearnReply{Learned: false, Reason: "已学习过该技能"}, nil
		}
	}
	if len(sk.Learned) >= sk.MaxSlots {
		return &ControlLearnReply{Learned: false, Reason: fmt.Sprintf("技能槽已满 (%d/%d)", len(sk.Learned), sk.MaxSlots)}, nil
	}
	if data.Attr.Gold < req.Cost {
		return &ControlLearnReply{Learned: false, Reason: fmt.Sprintf("金币不足 (需要 %d, 当前 %d)", req.Cost, data.Attr.Gold)}, nil
	}

	data.Attr.Gold -= req.Cost
	sk.Learned = append(sk.Learned, types.Skill{ID: req.ID, Name: req.Name, Level: 1})
	ctx.State().Persist(ctx) // 学习技能后持久化
	log.Printf("[Player.Skill] %s 学习了 %s (消耗 %d 金币)", ctx.Id(), req.Name, req.Cost)
	return &ControlLearnReply{Learned: true}, nil
}

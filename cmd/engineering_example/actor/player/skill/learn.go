// skill 子包定义技能模块的所有请求。
//
// 【依赖】types/ + actor + logic/，不依赖 player 包。
package skill

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type LearnSkill struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
	Cost int    `json:"cost"`
}

type LearnSkillReply struct {
	Learned bool   `json:"learned"`
	Reason  string `json:"reason"`
}

func (*LearnSkill) ReqType(_ types.PlayerId, _ *LearnSkillReply) string { return "LearnSkill" }

func (req *LearnSkill) Handle(ctx *types.PlayerActorCtx, spawning bool) (*LearnSkillReply, error) {
	data := &ctx.State().Data
	sk := &data.Skill

	for _, s := range sk.Learned {
		if s.ID == req.ID {
			return &LearnSkillReply{Learned: false, Reason: "已学习过该技能"}, nil
		}
	}
	if len(sk.Learned) >= sk.MaxSlots {
		return &LearnSkillReply{Learned: false, Reason: fmt.Sprintf("技能槽已满 (%d/%d)", len(sk.Learned), sk.MaxSlots)}, nil
	}
	if data.Gold < req.Cost {
		return &LearnSkillReply{Learned: false, Reason: fmt.Sprintf("金币不足 (需要 %d, 当前 %d)", req.Cost, data.Gold)}, nil
	}

	data.Gold -= req.Cost
	sk.Learned = append(sk.Learned, types.Skill{ID: req.ID, Name: req.Name, Level: 1})
	ctx.State().Persist(ctx) // 学习技能后持久化
	log.Printf("[Player.Skill] %s 学习了 %s (消耗 %d 金币)", ctx.Id(), req.Name, req.Cost)
	return &LearnSkillReply{Learned: true}, nil
}

package skill

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
)

type CastSkill struct {
	ID     int          `json:"id"`
	Target string       `json:"target"`
	TargetPos logic.Point `json:"targetPos"` // 目标位置（用于寻路/距离判定）
}

type CastSkillReply struct {
	Cast      bool   `json:"cast"`
	SkillName string `json:"skillName"`
	Damage    int    `json:"damage"`
	Reason    string `json:"reason"`
	Critical  bool   `json:"critical"`
}

func (*CastSkill) ReqType(_ types.PlayerId, _ *CastSkillReply) string { return "CastSkill" }

func (req *CastSkill) Handle(ctx *types.PlayerActorCtx, spawning bool) (*CastSkillReply, error) {
	data := &ctx.State().Data
	sk := &data.Skill

	for i := range sk.Learned {
		if sk.Learned[i].ID == req.ID {
			s := &sk.Learned[i]
			if s.CoolDown > 0 {
				return &CastSkillReply{Cast: false, SkillName: s.Name, Reason: fmt.Sprintf("冷却中 (%d 回合)", s.CoolDown)}, nil
			}

			// 距离判定：使用 logic 包
			selfPos := logic.Point{X: 0, Y: 0} // 简化：玩家在原点
			skillRange := 5
			if !logic.InRange(selfPos, req.TargetPos, skillRange) {
				dist := logic.ManhattanDist(selfPos, req.TargetPos)
				return &CastSkillReply{Cast: false, SkillName: s.Name,
					Reason: fmt.Sprintf("目标超出技能范围 (距离=%d, 最大=%d)", dist, skillRange)}, nil
			}

			// 使用 logic 包计算伤害
			result := logic.CalcDamage(
				data.Attr.Atk, 0, s.Level,
				20, 0.5, 10, 0.15,
			)

			s.CoolDown = 3

			log.Printf("[Player.Skill] %s 释放 %s Lv.%d → %s 造成 %d 伤害 (暴击=%v)",
				ctx.Id(), s.Name, s.Level, req.Target, result.Mitigated, result.Critical)
			return &CastSkillReply{
				Cast: true, SkillName: s.Name, Damage: result.Mitigated, Critical: result.Critical,
			}, nil
		}
	}

	return nil, fmt.Errorf("未学习技能 ID=%d", req.ID)
}

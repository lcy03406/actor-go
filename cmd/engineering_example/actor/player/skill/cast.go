// cast.go 定义 ControlCast：玩家主动释放技能的意图入口（Control* = 玩家控制）。
//
// 与 ControlAttack 同构：技能伤害通过 combat.PlayerDamage 打到同房间目标，
// 复用与 ControlAttack 完全相同的受击链路（PlayerState.TakeDamage：同房间校验 + 扣血），
// 被攻击方再回传 PlayerCombatResult 给攻击方结算奖励。
// 技能伤害 = 攻击力 × 技能系数，因此 attr.AddExp 升级提升的 Atk 会直接放大技能威力。
package skill

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/combat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
)

type ControlCast struct {
	ID        int         `json:"id"`
	Target    string      `json:"target"`    // 目标玩家 openId（必须与施法者同处一房间）
	TargetPos logic.Point `json:"targetPos"` // 目标位置（用于距离判定）
}

type ControlCastReply struct {
	Cast      bool   `json:"cast"`
	SkillName string `json:"skillName"`
	Damage    int    `json:"damage"`
	Reason    string `json:"reason"`
	Critical  bool   `json:"critical"`
}

func (*ControlCast) ReqType(_ types.PlayerId, _ *ControlCastReply) string { return "ControlCast" }

func (req *ControlCast) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlCastReply, error) {
	data := &ctx.State().Data
	sk := &data.Skill

	for i := range sk.Learned {
		if sk.Learned[i].ID == req.ID {
			s := &sk.Learned[i]
			if s.CoolDown > 0 {
				return &ControlCastReply{Cast: false, SkillName: s.Name, Reason: fmt.Sprintf("冷却中 (%d 回合)", s.CoolDown)}, nil
			}

			// 距离判定：使用 logic 包
			selfPos := logic.Point{X: 0, Y: 0} // 简化：玩家在原点
			skillRange := 5
			if !logic.InRange(selfPos, req.TargetPos, skillRange) {
				dist := logic.ManhattanDist(selfPos, req.TargetPos)
				return &ControlCastReply{Cast: false, SkillName: s.Name,
					Reason: fmt.Sprintf("目标超出技能范围 (距离=%d, 最大=%d)", dist, skillRange)}, nil
			}

			// 使用 logic 包计算技能伤害（受攻击力加成，技能随属性成长而变强）
			result := logic.CalcDamage(
				data.Attr.Atk, 0, s.Level,
				20, 0.5, 10, 0.15,
			)

			s.CoolDown = 3

			// ★ 把技能伤害打到同房间目标玩家（复用 combat.PlayerDamage 受击链路）
			if req.Target != "" && req.Target != ctx.Id().OpenId {
				targetId := types.PlayerId{ServerId: ctx.Id().ServerId, OpenId: req.Target}
				_ = actor.Post(ctx.Manager(), targetId, &combat.PlayerDamage{
					Attacker:     ctx.Id(),
					AttackerRoom: data.CurrentRoom,
					Damage:       result.Mitigated,
				})
				log.Printf("[Player.Skill] %s 释放 %s Lv.%d → %s 造成 %d 伤害 (暴击=%v)",
					ctx.Id(), s.Name, s.Level, req.Target, result.Mitigated, result.Critical)
			} else {
				log.Printf("[Player.Skill] %s 释放 %s Lv.%d → 自身演练，伤害=%d (暴击=%v)",
					ctx.Id(), s.Name, s.Level, result.Mitigated, result.Critical)
			}

			return &ControlCastReply{
				Cast: true, SkillName: s.Name, Damage: result.Mitigated, Critical: result.Critical,
			}, nil
		}
	}

	return nil, fmt.Errorf("未学习技能 ID=%d", req.ID)
}

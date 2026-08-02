// heal.go 定义 ControlHeal：玩家主动治疗的意图入口（Control* = 玩家控制）。
//
// 治疗是 component 内状态变更（不跨 actor），直接调用 PlayerState.ApplyHeal。
// 治疗消耗金币、受 MaxHP 上限约束——把「治疗」与「金币 / 生命上限」系统结合：
// 金币来自战斗回报（PlayerCombatResult 结算），形成「战斗赚金币 → 金币回血续航」闭环。
package player

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// healCostPerHP 每点治疗的金币花费。
const healCostPerHP = 1

// ControlHeal 是玩家消耗金币自我治疗的请求。
type ControlHeal struct {
	Amount int `json:"amount"` // 期望治疗量（实际受金币与 MaxHP 上限约束）
}

// ControlHealReply 是治疗的回复。
type ControlHealReply struct {
	NewHP     int    `json:"newHP"`
	Healed    int    `json:"healed"`    // 实际恢复量
	GoldSpent int    `json:"goldSpent"` // 实际消耗金币
	Reason    string `json:"reason,omitempty"`
}

func (*ControlHeal) ReqType(_ types.PlayerId, _ *ControlHealReply) string { return "ControlHeal" }

func (req *ControlHeal) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlHealReply, error) {
	data := &ctx.State().Data
	healed, spent, reason := data.ApplyHeal(req.Amount, healCostPerHP)
	if reason != "" {
		log.Printf("[Player] %s 治疗未执行: %s (HP=%d)", ctx.Id(), reason, data.HP)
		return &ControlHealReply{NewHP: data.HP, Reason: reason}, nil
	}
	ctx.State().Persist(ctx)
	log.Printf("[Player] %s 治疗 +%d (HP=%d, 消耗金币 %d)", ctx.Id(), healed, data.HP, spent)
	return &ControlHealReply{NewHP: data.HP, Healed: healed, GoldSpent: spent}, nil
}

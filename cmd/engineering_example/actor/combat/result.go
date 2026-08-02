package combat

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// PlayerCombatResult 是被攻击方在扣血完成后回传给攻击方的跨 Actor 事件。
//
// 来源 = 被攻击方 Player；目标 = 攻击方 Player。攻击方收到后据此结算
// 金币 / 经验奖励（见本文件 Handle）。这样奖励严格发生在
// 「收到对面状态通知之后」，且跨 actor 与 component 内状态变更的边界清晰。
//
// 类型与 Handle 都定义在 combat 包：Handle 是「处理该事件」的入口，
// 内部仅调用 PlayerState / AttrState 的领域方法（state 带逻辑），handler 保持薄。
type PlayerCombatResult struct {
	Target       types.PlayerId `json:"target"`       // 被攻击方
	NewHP        int            `json:"newHP"`        // 被攻击方剩余 HP
	Dead         bool           `json:"dead"`         // 是否被击杀
	Damage       int            `json:"damage"`       // 造成的实际伤害
	AttackerRoom int            `json:"attackerRoom"` // 被攻击方所在房间（回传核对）
}

// PlayerCombatResultReply 仅用于框架约束。
type PlayerCombatResultReply struct{}

func (*PlayerCombatResult) ReqType(_ types.PlayerId, _ *PlayerCombatResultReply) string {
	return "PlayerCombatResult"
}

// battleReward 根据一次伤害结果计算攻击方获得的成长奖励。
func battleReward(dmg int, killed bool) (gold, exp int) {
	gold = 10 + dmg // 每点实际伤害 10 金币
	exp = 15 + dmg  // 每点实际伤害 15 经验
	if killed {
		gold += 50 // 击杀额外奖励
		exp += 80
	}
	return gold, exp
}

// Handle 攻击方收到回传后的结算入口：严格在收到对面状态通知后加金币、加经验。
func (req *PlayerCombatResult) Handle(ctx *types.PlayerActorCtx, spawning bool) (*PlayerCombatResultReply, error) {
	data := &ctx.State().Data
	gold, exp := battleReward(req.Damage, req.Dead)

	// ★ 调用状态领域方法（state 带逻辑）
	data.Attr.AddGold(gold)
	_, level, levelUp := data.AddExp(exp)
	if levelUp {
		ctx.State().Persist(ctx) // 升级时持久化
		log.Printf("[Player] %s 升级! Level=%d Atk=%d Def=%d 回满血=%d",
			ctx.Id(), data.Level, data.Attr.Atk, data.Attr.Def, data.HP)
	}

	ctx.State().Persist(ctx)
	log.Printf("[Player] %s 战斗结算: 金币+%d 经验+%d%s (目标%s 剩余HP=%d)",
		ctx.Id(), gold, exp,
		ternaryStr3(levelUp, fmt.Sprintf(" 升级至 Lv%d", level), ""),
		req.Target, req.NewHP)
	return &PlayerCombatResultReply{}, nil
}

func ternaryStr3(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

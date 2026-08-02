// combat 包承载「跨 Actor 流动的战斗事件协议」。
//
// 命名首词即来源（解决"内外/节点"无法稳定区分的问题）：
//   - Player*：落到某个 Player actor 身上的事件（由另一个 actor 推过来）
//       · PlayerDamage      —— 攻击方 → 被攻击方：造成一次伤害
//       · PlayerCombatResult —— 被攻击方 → 攻击方：回传伤害结果（用于结算奖励）
//
// combat 包只放「真正跨 actor 的那一跳」协议 + 极薄 handler 壳，
// 不持有任何业务状态逻辑（逻辑在 types.PlayerState / attr.AttrState 上）。
// 依赖：actor + types + logic + room + notify，无循环依赖。
package combat

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
)

// PlayerDamage 是被攻击方接收伤害的跨 Actor 事件（fire-and-forget，由攻击方 Post 过来）。
// AttackerRoom 携带攻击方当前所在房间，用于在被攻击方本 actor 内原子校验「同处一室」。
//
// 普通攻击（ControlAttack）与技能攻击（ControlCast）都复用这一受击事件，
// 保证「同房间校验 + 计算伤害 + 扣血」逻辑只有一份实现（在 PlayerState.TakeDamage）。
type PlayerDamage struct {
	Attacker     types.PlayerId `json:"attacker"`
	AttackerRoom int            `json:"attackerRoom"`
	Damage       int            `json:"damage"`
}

// PlayerDamageReply 仅用于框架约束（post 模式不等待回复）。
type PlayerDamageReply struct{}

func (*PlayerDamage) ReqType(_ types.PlayerId, _ *PlayerDamageReply) string { return "PlayerDamage" }

func (req *PlayerDamage) Handle(ctx *types.PlayerActorCtx, spawning bool) (*PlayerDamageReply, error) {
	data := &ctx.State().Data

	// ★ 同处一室校验 + 扣血：全部在 PlayerState.TakeDamage 内原子完成（避免 TOCTOU）。
	mit, dead := data.TakeDamage(req.Damage, req.AttackerRoom, req.Attacker)

	if mit == 0 && !dead {
		log.Printf("[Combat] %s 忽略 %s 的伤害：不在同一房间 (自身=%d, 对方声称=%d)",
			ctx.Id(), req.Attacker, data.CurrentRoom, req.AttackerRoom)
		return &PlayerDamageReply{}, nil
	}

	log.Printf("[Combat] %s 被 %s 攻击受到 %d 伤害, HP=%d%s",
		ctx.Id(), req.Attacker, mit, data.HP, ternaryStr(dead, ", 已死亡", ""))
	if dead {
		ctx.State().Deactivate(ctx) // 死亡 → 停用 + 释放租约
	}

	// ★ 把自身状态变化通过 Room 广播（供房间记录战斗日志）。
	_ = actor.Post(ctx.Manager(), room.RoomId{RoomId: data.CurrentRoom}, &room.RecordBattle{
		Attacker: req.Attacker,
		Target:   ctx.Id(),
		Damage:   mit,
		TargetHP: data.HP,
	})

	// ★ 回传战斗结果给攻击方：攻击方据此结算金币/经验奖励。
	// 这一跳是跨 actor 的（被攻击方 actor → 攻击方 actor），奖励严格发生在
	// 「收到对面状态通知之后」，而非攻击发出时。
	_ = actor.Post(ctx.Manager(), req.Attacker, &PlayerCombatResult{
		Target:   ctx.Id(),
		NewHP:    data.HP,
		Dead:     dead,
		Damage:   mit,
		AttackerRoom: data.CurrentRoom,
	})

	return &PlayerDamageReply{}, nil
}

func ternaryStr(cond bool, a, b string) string {
	if cond {
		return a
	}
	return b
}

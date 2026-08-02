// attack.go 定义 ControlAttack：玩家主动发起攻击的意图入口（Control* = 玩家控制）。
//
// 跨 Actor 链路（避免死锁与 TOCTOU）：
//   1. Player(攻击方) ——actor.Post——▶ Player(被攻击方).PlayerDamage
//   2. 被攻击方在自身 actor 内原子校验并扣血；
//   3. 被攻击方 ——actor.Post——▶ 攻击方.PlayerCombatResult（回传结果）
//   4. 攻击方收到回传后，结算金币 / 经验（见 combat_result.go）
//
// ControlAttack 本身只负责「校验参数 + post 伤害事件」，不发奖、不读写对方状态。
// 全程 actor.Post（fire-and-forget），无 actor.Call 嵌套等待，避免死锁。
package player

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/combat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// ControlAttack 是玩家主动发起房间内攻击的请求。
type ControlAttack struct {
	TargetOpenId string `json:"targetOpenId"`
}

// ControlAttackReply 仅确认请求已发出，结果通过 PlayerCombatResult 回传结算。
type ControlAttackReply struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (*ControlAttack) ReqType(_ types.PlayerId, _ *ControlAttackReply) string { return "ControlAttack" }

func (req *ControlAttack) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlAttackReply, error) {
	roomId := ctx.State().Data.CurrentRoom
	if roomId == 0 {
		return &ControlAttackReply{Success: false, Reason: "尚未加入任何房间"}, nil
	}
	if req.TargetOpenId == "" {
		return &ControlAttackReply{Success: false, Reason: "未指定攻击目标"}, nil
	}
	if req.TargetOpenId == ctx.Id().OpenId {
		return &ControlAttackReply{Success: false, Reason: "不能攻击自己"}, nil
	}

	// 攻击者与被攻击者必须在同一 ServerId 下（示例仅单服）。
	targetId := types.PlayerId{ServerId: ctx.Id().ServerId, OpenId: req.TargetOpenId}

	// ★ 跨 Actor：攻击方 post 伤害事件给被攻击方（fire-and-forget，无死锁）。
	_ = actor.Post(ctx.Manager(), targetId, &combat.PlayerDamage{
		Attacker:     ctx.Id(),
		AttackerRoom: roomId,
		Damage:       ctx.State().Data.Attr.Atk,
	})

	log.Printf("[Player] %s → post ControlAttack %s (房间%d, 攻击力=%d)",
		ctx.Id(), targetId, roomId, ctx.State().Data.Attr.Atk)
	return &ControlAttackReply{Success: true, Reason: fmt.Sprintf("已向 %s 发出攻击请求", req.TargetOpenId)}, nil
}

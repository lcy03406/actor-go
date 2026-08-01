package player

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/game"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// 本文件是"别人 Post 过来的事件"的落地：不回复、不反向 Call，只改本地状态再往外推。

// handleDamage 受击结算：改自己的血 → 回执攻击方 → 推下行 → 上报房间缓存。
//
// 四件事全是"发出去了就不管"（Post）：受击方的处理不该被任何一方的忙碌卡住。
func handleDamage(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.Damage, _ bool) (actor.OkReply, error) {
	s := &ctx.State().Data
	taken, slain, ok := s.applyDamage(req.Amount)
	if !ok {
		return actor.OK, nil // 已倒地：重复打击不结算
	}
	// 回执攻击方。等级随回执带过去 —— 击杀奖励按等级算，攻击方不必再查人。
	if err := actor.APost(ctx.Control(), req.From, &playerapi.CombatResult{
		Target: ctx.Id(),
		Level:  s.Level,
		Amount: taken,
		Slain:  slain,
	}); err != nil {
		ctx.Logger().Warn("player: 回执攻击方失败", "from", req.From, "err", err)
	}
	pushState(ctx, "damage")
	reportBrief(ctx)
	return actor.OK, nil
}

// handleCombatResult 攻击方收到回执后发奖：奖励是攻击方的事实，记在自己身上。
func handleCombatResult(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.CombatResult, _ bool) (actor.OkReply, error) {
	s := &ctx.State().Data
	gold, exp := game.Reward(req.Slain, req.Level)
	// 经验倍率是世界配置（读者缓存），改配置对下一次结算立即生效。
	exp = exp * expRate(ctx) / 100
	if s.applyReward(gold, exp) {
		ctx.Logger().Info("player level up", "id", ctx.Id(), "level", s.Level)
	}
	pushState(ctx, "combat")
	reportBrief(ctx)
	if req.Slain {
		report(ctx, fmt.Sprintf("击倒了 %s", req.Target.OpenID))
	}
	return actor.OK, nil
}

// handleRoomEvent 房间广播：玩家只是中转，原样推给连接。
// 房间是广播的权威，玩家不解析、不聚合，避免每个玩家都长出一份广播逻辑。
func handleRoomEvent(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.RoomEvent, _ bool) (actor.OkReply, error) {
	push(ctx, roomEventMsg(req))
	return actor.OK, nil
}

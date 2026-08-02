// battle.go 定义房间内战斗记录与离开房间请求。
//
// Room 不主动调用 Player（避免循环依赖与嵌套 call 死锁），仅被动接收：
//   - RecordBattle：由被攻击方 Player 在扣血完成后 post 过来，Room 记录并广播；
//   - LeaveRoom：由离房 Player post 过来，Room 移除成员并广播。
package room

import (
	"log"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/notify"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// RecordBattle 由被攻击方 Player 在扣血完成后发来，用于记录并广播。
type RecordBattle struct {
	Attacker types.PlayerId `json:"attacker"`
	Target   types.PlayerId `json:"target"`
	Damage   int            `json:"damage"`
	TargetHP int            `json:"targetHP"`
}

func (*RecordBattle) ReqType(_ RoomId, _ actor.OkReply) string { return "RecordBattle" }

func (req *RecordBattle) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (actor.OkReply, error) {
	now := time.Now().Unix()
	ctx.State().BattleLog = append(ctx.State().BattleLog, BattleRecord{
		Attacker: req.Attacker.String(),
		Target:   req.Target.String(),
		Damage:   req.TargetHP,
		Time:     now,
	})
	log.Printf("[Room] %s 记录战斗 %s → %s 伤害=%d 目标剩余HP=%d",
		ctx.Id(), req.Attacker, req.Target, req.Damage, req.TargetHP)

	// 广播给同房间所有玩家
	for _, p := range ctx.State().Players {
		_ = actor.Post(ctx.Manager(), p, &notify.ReceiveBattle{
			Attacker: req.Attacker.String(),
			Target:   req.Target.String(),
			Damage:   req.Damage,
			TargetHP: req.TargetHP,
			Time:     now,
		})
	}
	return actor.OK, nil
}

// LeaveRoom 由离房 Player 发来，移除成员并广播「离开」事件。
type LeaveRoom struct {
	Player types.PlayerId `json:"player"`
}

func (*LeaveRoom) ReqType(_ RoomId, _ actor.OkReply) string { return "LeaveRoom" }

func (req *LeaveRoom) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (actor.OkReply, error) {
	before := len(ctx.State().Players)
	kept := ctx.State().Players[:0]
	for _, p := range ctx.State().Players {
		if p != req.Player {
			kept = append(kept, p)
		}
	}
	ctx.State().Players = kept
	log.Printf("[Room] %s 玩家 %s 离开, 人数 %d → %d",
		ctx.Id(), req.Player, before, len(ctx.State().Players))

	// 广播「玩家离开」事件给同房间剩余成员
	for _, p := range ctx.State().Players {
		_ = actor.Post(ctx.Manager(), p, &notify.ReceiveRoomEvent{
			Type:   "leave",
			Player: req.Player.String(),
			Time:   time.Now().Unix(),
		})
	}
	return actor.OK, nil
}

// notify 包定义 Room → Player 的跨 Actor 广播通知类型。
//
// 这些请求由 Room 通过 actor.Post 发给同房间各 Player，仅用于通知（玩家侧记录日志）。
// 放在独立包中，避免 room 与 player 之间的循环依赖。
package notify

import (
	"log"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// ReceiveChat 是玩家收到的房间聊天广播。
type ReceiveChat struct {
	From    string `json:"from"`
	Content string `json:"content"`
	Time    int64  `json:"time"`
}

func (*ReceiveChat) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "ReceiveChat" }

func (req *ReceiveChat) Handle(ctx *types.PlayerActorCtx, spawning bool) (actor.OkReply, error) {
	log.Printf("[Player] %s 收到房间聊天 [%s]: %s", ctx.Id(), req.From, req.Content)
	return actor.OK, nil
}

// ReceiveBattle 是玩家收到的房间战斗广播。
type ReceiveBattle struct {
	Attacker string `json:"attacker"`
	Target   string `json:"target"`
	Damage   int    `json:"damage"`
	TargetHP int    `json:"targetHP"`
	Time     int64  `json:"time"`
}

func (*ReceiveBattle) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "ReceiveBattle" }

func (req *ReceiveBattle) Handle(ctx *types.PlayerActorCtx, spawning bool) (actor.OkReply, error) {
	log.Printf("[Player] %s 收到房间战斗广播: %s → %s 伤害=%d 目标HP=%d",
		ctx.Id(), req.Attacker, req.Target, req.Damage, req.TargetHP)
	return actor.OK, nil
}

// ReceiveRoomEvent 是玩家收到的房间事件广播（加入 / 离开）。
type ReceiveRoomEvent struct {
	Type   string `json:"type"` // "leave" / "join"
	Player string `json:"player"`
	Time   int64  `json:"time"`
}

func (*ReceiveRoomEvent) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "ReceiveRoomEvent" }

func (req *ReceiveRoomEvent) Handle(ctx *types.PlayerActorCtx, spawning bool) (actor.OkReply, error) {
	switch req.Type {
	case "leave":
		log.Printf("[Player] %s 收到房间事件: %s 离开了房间", ctx.Id(), req.Player)
	case "join":
		log.Printf("[Player] %s 收到房间事件: %s 加入了房间", ctx.Id(), req.Player)
	default:
		log.Printf("[Player] %s 收到房间事件: %s %s", ctx.Id(), req.Type, req.Player)
	}
	return actor.OK, nil
}

// Now 返回当前时间戳（供调用方构造通知）。
func Now() int64 { return time.Now().Unix() }

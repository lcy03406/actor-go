// roominfo.go 定义查询房间信息请求：结构体 + Reply + ReqType + Handle 写在同一文件。
package room

import (
	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// RoomInfo 是查询房间信息的请求。
type RoomInfo struct{}

// RoomInfoReply 是查询房间信息的回复。
type RoomInfoReply struct {
	MaxPlayers int               `json:"maxPlayers"`
	Players    []types.PlayerId  `json:"players"`
	ChatLog    []ChatMessage     `json:"chatLog"`
	BattleLog  []BattleRecord    `json:"battleLog"`
}

func (*RoomInfo) ReqType(_ RoomId, _ *RoomInfoReply) string { return "RoomInfo" }

func (req *RoomInfo) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (*RoomInfoReply, error) {
	return &RoomInfoReply{
		MaxPlayers: ctx.State().MaxPlayers,
		Players:    ctx.State().Players,
		ChatLog:    ctx.State().ChatLog,
		BattleLog:  ctx.State().BattleLog,
	}, nil
}

// joinroom.go 定义加入房间请求：结构体 + Reply + ReqType + Handle 写在同一文件。
package room

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// JoinRoom 是加入房间的请求。
type JoinRoom struct {
	Player types.PlayerId `json:"player"`
}

// JoinRoomReply 是加入房间请求的回复。
type JoinRoomReply struct {
	CurrentCount int              `json:"currentCount"`
	Players      []types.PlayerId `json:"players"`
}

func (*JoinRoom) ReqType(_ RoomId, _ *JoinRoomReply) string { return "JoinRoom" }

func (req *JoinRoom) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (*JoinRoomReply, error) {
	// 去重：同一玩家重复加入不重复计数
	for _, p := range ctx.State().Players {
		if p == req.Player {
			return &JoinRoomReply{CurrentCount: len(ctx.State().Players), Players: ctx.State().Players}, nil
		}
	}
	ctx.State().Players = append(ctx.State().Players, req.Player)
	log.Printf("[Room] %s 玩家 %s 加入, 当前人数=%d",
		ctx.Id(), req.Player, len(ctx.State().Players))
	return &JoinRoomReply{CurrentCount: len(ctx.State().Players), Players: ctx.State().Players}, nil
}

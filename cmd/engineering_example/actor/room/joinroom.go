// joinroom.go 定义加入房间请求：结构体 + Reply + ReqType + Handle 写在同一文件。
package room

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
)

// JoinRoom 是加入房间的请求。
type JoinRoom struct {
	PlayerId string `json:"playerId"`
}

// JoinRoomReply 是加入房间请求的回复。
type JoinRoomReply struct {
	CurrentCount int `json:"currentCount"`
}

func (*JoinRoom) ReqType(_ RoomId, _ *JoinRoomReply) string { return "JoinRoom" }

func (req *JoinRoom) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (*JoinRoomReply, error) {
	ctx.State().PlayerIds = append(ctx.State().PlayerIds, req.PlayerId)
	log.Printf("[Room] %s 玩家 %s 加入, 当前人数=%d",
		ctx.Id(), req.PlayerId, len(ctx.State().PlayerIds))
	return &JoinRoomReply{CurrentCount: len(ctx.State().PlayerIds)}, nil
}

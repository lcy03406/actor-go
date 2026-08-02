// createroom.go 定义创建房间请求：结构体 + ReqType + Handle 写在同一文件。
package room

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
)

// CreateRoom 是创建房间的请求。
type CreateRoom struct {
	MaxPlayers int `json:"maxPlayers"`
}

func (*CreateRoom) ReqType(_ RoomId, _ actor.OkReply) string { return "CreateRoom" }

func (req *CreateRoom) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (actor.OkReply, error) {
	ctx.SetState(RoomState{MaxPlayers: req.MaxPlayers, PlayerIds: []string{}})
	log.Printf("[Room] %s 创建 最大人数=%d", ctx.Id(), req.MaxPlayers)
	return actor.OK, nil
}

// sendchat.go 定义玩家在房间内聊天的请求。
//
// Player → Room 的跨 Actor 通信：玩家把聊天内容发给所在房间，
// 由 Room 追加聊天记录并广播给同房间所有成员（含自己）。
// 广播通知类型 ReceiveChat 定义于 notify 包。
package player

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
)

// PlayerRoomChat 是玩家在房间内聊天的请求（Player 对外公开的类型）。
type PlayerRoomChat struct {
	Content string `json:"content"`
}

// PlayerRoomChatReply 是聊天请求的回复。
type PlayerRoomChatReply struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (*PlayerRoomChat) ReqType(_ types.PlayerId, _ *PlayerRoomChatReply) string { return "PlayerRoomChat" }

func (req *PlayerRoomChat) Handle(ctx *types.PlayerActorCtx, spawning bool) (*PlayerRoomChatReply, error) {
	roomId := ctx.State().Data.CurrentRoom
	if roomId == 0 {
		return &PlayerRoomChatReply{Success: false, Reason: "尚未加入任何房间"}, nil
	}

	// ★ 跨 Group Actor 通信：Player → Room（fire-and-forget 模式）
	// 把聊天内容转发给所在房间，由 Room 负责记录并广播。
	err := actor.Post(ctx.Manager(), room.RoomId{RoomId: roomId}, &room.RoomChat{
		From:    ctx.Id().String(),
		Content: req.Content,
	})
	if err != nil {
		log.Printf("[Player] %s 房间聊天失败: %v", ctx.Id(), err)
		return &PlayerRoomChatReply{Success: false, Reason: fmt.Sprintf("Room(%d) 不可用: %v", roomId, err)}, nil
	}

	log.Printf("[Player] %s → Room(%d): %s", ctx.Id(), roomId, req.Content)
	return &PlayerRoomChatReply{Success: true, Reason: fmt.Sprintf("已发送到 Room(%d)", roomId)}, nil
}

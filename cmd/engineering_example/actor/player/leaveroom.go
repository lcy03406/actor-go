// leaveroom.go 定义玩家离开房间的请求（Player 对外公开的类型）。
//
// Player → Room 的跨 Actor 通信：Player 把离开请求 post 给所在房间，
// 由 Room 移除成员并广播「玩家离开」事件，Player 自身清理 CurrentRoom。
package player

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
)

// ControlLeaveRoom 是 Player 离开当前房间的请求（玩家主动意图）。
type ControlLeaveRoom struct{}

// ControlLeaveRoomReply 是离开房间的回复。
type ControlLeaveRoomReply struct {
	Success bool   `json:"success"`
	Reason  string `json:"reason,omitempty"`
}

func (*ControlLeaveRoom) ReqType(_ types.PlayerId, _ *ControlLeaveRoomReply) string {
	return "ControlLeaveRoom"
}

func (req *ControlLeaveRoom) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlLeaveRoomReply, error) {
	roomId := ctx.State().Data.CurrentRoom
	if roomId == 0 {
		return &ControlLeaveRoomReply{Success: false, Reason: "尚未加入任何房间"}, nil
	}
	playerId := ctx.Id()

	// ★ 跨 Group Actor 通信：Player → Room（fire-and-forget）
	// 通知房间移除自己，无需等待回复。
	_ = actor.Post(ctx.Manager(), room.RoomId{RoomId: roomId}, &room.LeaveRoom{Player: playerId})

	// 清理自身当前房间状态
	ctx.State().Data.CurrentRoom = 0

	log.Printf("[Player] %s 离开 Room(%d) — 跨 Actor Post", playerId, roomId)
	return &ControlLeaveRoomReply{Success: true, Reason: fmt.Sprintf("Player %s 已离开 Room(%d)", playerId, roomId)}, nil
}

// joinroom.go 演示 Player → Room 的跨 Actor 通信。
//
// 【关键设计：向其他 Actor 公开请求的类型】
//
//	两种请求类型的角色：
//
//	1. room.JoinRoom（Room 包定义）— Room Actor 的内部请求类型
//	   Room 包定义它需要的参数（PlayerId string），Room handler 实现加入逻辑。
//	   任何 Actor（Player 或外部调用者）都可以向 Room 发送此请求。
//
//	2. ControlJoinRoom（本文件定义）— Player 对外公开的请求类型
//	   Player 暴露"从 Player 视角加入房间"的操作语义。
//	   外部调用者（console/cluster.Call）通过此类型触发 Player，
//	   Player handler 内部再转发给 Room Actor。
//
//	┌─────────┐  cluster.Call(router, pid, &player.ControlJoinRoom{RoomId: 1})
//	│ console │ ─────────────────────────────────────────────────────────→ ┌──────────┐
//	└─────────┘                                                            │  Player   │
//	                                                                       │  Handle() │
//	                                                                       │    ↓      │
//	                                                                       │  actor.Post(ctx.Manager(), roomId, &room.JoinRoom{...})
//	                                                                       │    │      │
//	                                                                       └────┼──────┘
//	                                                                            │
//	                                                                            ▼
//	                                                                       ┌──────────┐
//	                                                                       │   Room   │
//	                                                                       │  Handle()│
//	                                                                       └──────────┘
//
// 【跨 Group 通信方式】
//
//	使用 ctx.Manager() 获取 Manager 引用，然后通过 actor.Post / actor.Call 发送消息。
//	不需要在请求结构体中注入 Manager，因为 ActorContext 始终持有 Manager 引用。
package player

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
)

// ControlJoinRoom 是 Player 加入房间的请求（Player 对外公开的类型）。
// 请求字段全部是 JSON 可序列化的，通过 RPC 传输后反序列化不会丢失数据。
type ControlJoinRoom struct {
	RoomId int `json:"roomId"`
}

// ControlJoinRoomReply 是 Player 加入房间的回复。
type ControlJoinRoomReply struct {
	Success      bool   `json:"success"`
	CurrentCount int    `json:"currentCount"`
	Reason       string `json:"reason,omitempty"`
}

func (*ControlJoinRoom) ReqType(_ types.PlayerId, _ *ControlJoinRoomReply) string {
	return "ControlJoinRoom"
}

// Handle 处理 Player 加入 Room 的请求。
//
// 核心演示：Player handler 中通过 actor.Post 向 Room Actor 发送 room.JoinRoom 请求。
//   - ctx.Manager() 获取 Manager，支持跨 Group 通信
//   - actor.Post 是 fire-and-forget 模式，不等待回复
//   - Manager 自动根据 room.RoomId.ActorType() 路由到 Room Group
func (req *ControlJoinRoom) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ControlJoinRoomReply, error) {
	roomId := room.RoomId{RoomId: req.RoomId}
	playerId := ctx.Id()

	// ★ 跨 Group Actor 通信：Player → Room（request-response 模式）
	// ctx.Manager() 返回 Manager，通过 actor.Call 向 Room Actor 发送消息并等待回复。
	reply, err := actor.Call(ctx.Context(), ctx.Manager(), roomId, &room.JoinRoom{Player: playerId})
	if err != nil {
		log.Printf("[Player] %s 加入 Room(%d) 失败: %v", playerId, req.RoomId, err)
		return &ControlJoinRoomReply{
			Success: false,
			Reason:  fmt.Sprintf("Room(%d) 不可用: %v", req.RoomId, err),
		}, nil
	}

	// 记录当前所在房间，供后续的聊天 / 战斗请求使用。
	ctx.State().Data.CurrentRoom = req.RoomId

	log.Printf("[Player] %s 成功加入 Room(%d) — 跨 Actor Call, 当前房间人数=%d",
		playerId, req.RoomId, reply.CurrentCount)
	return &ControlJoinRoomReply{
		Success:      true,
		CurrentCount: reply.CurrentCount,
		Reason:       fmt.Sprintf("Player %s 已加入 Room(%d), 当前 %d 人", playerId, req.RoomId, reply.CurrentCount),
	}, nil
}

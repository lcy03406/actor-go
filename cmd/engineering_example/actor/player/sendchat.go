// sendchat.go 演示 Player → Chat 的跨 Actor 通信（使用 actor.Call 等待回复）。
//
// 【与 joinroom.go 的对比】
//
//   joinroom.go 使用 actor.Post（fire-and-forget），不等待 Room 的回复。
//   本文件使用 actor.Call（请求-回复），等待 Chat Actor 处理完成后返回结果。
//
//   两种模式的选择：
//   - actor.Post：不关心结果、不需要回复的"通知"型操作
//   - actor.Call：需要确认结果、需要返回数据的"查询"型操作
//
// 【跨 Actor 请求类型的公开方式】
//
//   1. chat.SendMessage（chat 包定义）— Chat Actor 的内部请求类型
//      Player 直接导入 chat 包，使用 chat.SendMessage 向 Chat Actor 发消息。
//
//   2. PlayerSendChat（本文件定义）— Player 对外公开的请求类型
//      定义在 player 包中，是 Player API 的一部分。
//
//   这种设计让 Player 的调用方（console、测试）不需要知道 Chat 的存在，
//   只需知道 Player 提供了"发送聊天"的能力。Player 内部自行处理与 Chat 的交互。
package player

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/chat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// PlayerSendChat 是 Player 向聊天频道发送消息的请求（Player 对外公开的类型）。
// 请求字段全部是 JSON 可序列化的，通过 RPC 传输后反序列化不会丢失数据。
type PlayerSendChat struct {
	Channel string `json:"channel"`
	Text    string `json:"text"`
}

// PlayerSendChatReply 是 Player 发送聊天消息的回复。
type PlayerSendChatReply struct {
	Success bool   `json:"success"`
	Echo    string `json:"echo"`
	Reason  string `json:"reason,omitempty"`
}

func (*PlayerSendChat) ReqType(_ types.PlayerId, _ *PlayerSendChatReply) string { return "PlayerSendChat" }

// Handle 处理 Player 发送聊天消息的请求。
//
// 核心演示：Player handler 中通过 actor.Call 向 Chat Actor 发送请求并等待回复。
//   - ctx.Manager() 获取 Manager，支持跨 Group 通信
//   - actor.Call 是请求-回复模式，会阻塞等待 Chat Actor 处理完成
//   - 使用 ctx.Context() 作为 context，支持超时和取消
func (req *PlayerSendChat) Handle(ctx *types.PlayerActorCtx, spawning bool) (*PlayerSendChatReply, error) {
	chatId := chat.ChatId{Channel: req.Channel}
	playerId := ctx.Id().String()

	// ★ 跨 Group Actor 通信：Player → Chat（请求-回复模式）
	// ctx.Manager() 返回 Manager，通过 actor.Call 向 Chat Actor 发送消息并等待回复
	reply, err := actor.Call(ctx.Context(), ctx.Manager(), chatId, &chat.SendMessage{
		Text: fmt.Sprintf("[%s]: %s", playerId, req.Text),
	})
	if err != nil {
		log.Printf("[Player] %s 向 Chat(%s) 发送消息失败: %v", playerId, req.Channel, err)
		return &PlayerSendChatReply{
			Success: false,
			Reason:  fmt.Sprintf("Chat(%s) 不可用: %v", req.Channel, err),
		}, nil
	}

	log.Printf("[Player] %s → Chat(%s): %s (echo=%s)", playerId, req.Channel, req.Text, reply.Echo)
	return &PlayerSendChatReply{
		Success: true,
		Echo:    reply.Echo,
	}, nil
}

// chat.go 定义房间内聊天请求：玩家把消息发到房间，房间追加聊天记录并广播给同房间成员。
package room

import (
	"log"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/notify"
)

// RoomChat 是房间内聊天的请求（由 Player 转发进来）。
type RoomChat struct {
	From    string `json:"from"`
	Content string `json:"content"`
}

func (*RoomChat) ReqType(_ RoomId, _ actor.OkReply) string { return "RoomChat" }

func (req *RoomChat) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (actor.OkReply, error) {
	now := time.Now().Unix()
	ctx.State().ChatLog = append(ctx.State().ChatLog, ChatMessage{
		From:    req.From,
		Content: req.Content,
		Time:    now,
	})
	log.Printf("[Room] %s 聊天 from=%s content=%s", ctx.Id(), req.From, req.Content)

	// 广播给同房间所有玩家（含发送者）
	for _, p := range ctx.State().Players {
		_ = actor.Post(ctx.Manager(), p, &notify.ReceiveChat{From: req.From, Content: req.Content, Time: now})
	}
	return actor.OK, nil
}

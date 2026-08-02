// sendmessage.go 定义发送聊天消息请求：结构体 + Reply + ReqType + Handle 写在同一文件。
package chat

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
)

// SendMessage 是发送聊天消息的请求。
type SendMessage struct {
	Text string `json:"text"`
}

// SendMessageReply 是发送聊天消息的回复。
type SendMessageReply struct {
	Echo string `json:"echo"`
}

func (*SendMessage) ReqType(_ ChatId, _ *SendMessageReply) string { return "SendMessage" }

func (req *SendMessage) Handle(ctx *actor.ActorContext[ChatId, ChatState], spawning bool) (*SendMessageReply, error) {
	ctx.State().Messages = append(ctx.State().Messages, req.Text)
	log.Printf("[Chat] %s 消息: %s", ctx.Id(), req.Text)
	return &SendMessageReply{Echo: req.Text}, nil
}

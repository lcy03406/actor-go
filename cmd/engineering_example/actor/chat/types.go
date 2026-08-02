// chat 包定义 Chat Actor 的核心类型：ActorId 和 State。
package chat

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"
)

// ChatId 是 Chat Actor 的唯一标识。
type ChatId struct {
	Channel string `json:"channel"`
}

func (id ChatId) ActorType() actor.ActorType { return "Chat" }
func (id ChatId) String() string             { return fmt.Sprintf("Chat(%s)", id.Channel) }

// ChatState 是 Chat Actor 的可变业务状态。
type ChatState struct {
	Messages []string `json:"messages"`
}

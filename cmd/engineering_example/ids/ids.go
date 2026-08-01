// Package ids 集中定义全部 ActorId。
//
// ActorId 只在这里定义、其它包按需 import，是为了切断"下游要发消息就得 import 上游实现包"
// 造成的循环依赖：契约包（actors/*/api）只依赖 ids，任何 actor 包都能 import 契约包。
package ids

import (
	"strconv"

	"github.com/lcy03406/actor-go/actor"
)

// ClientId 是一条客户端连接（实际项目里叫 session）。
type ClientId struct {
	Conn string `json:"conn"`
}

func (id ClientId) ActorType() actor.ActorType { return "Client" }
func (id ClientId) String() string             { return id.Conn }

// PlayerId 是一个玩家。
type PlayerId struct {
	OpenID string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string             { return id.OpenID }

// RoomId 是一个房间。
type RoomId struct {
	Seq int `json:"seq"`
}

func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string             { return strconv.Itoa(id.Seq) }

// WorldId 是世界（全服单例）。
type WorldId struct {
	ID string `json:"id"`
}

func (id WorldId) ActorType() actor.ActorType { return "World" }
func (id WorldId) String() string             { return id.ID }

// TheWorld 世界 Actor 是单例：固定 ID，任何包都能按需 Call / Post。
func TheWorld() WorldId { return WorldId{ID: "w0"} }

// room 包定义 Room Actor 的核心类型：ActorId 和 State。
package room

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"
)

// RoomId 是 Room Actor 的唯一标识。
type RoomId struct {
	RoomId int `json:"roomId"`
}

func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string             { return fmt.Sprintf("Room(%d)", id.RoomId) }

// RoomState 是 Room Actor 的可变业务状态。
type RoomState struct {
	MaxPlayers int      `json:"maxPlayers"`
	PlayerIds  []string `json:"playerIds"`
}

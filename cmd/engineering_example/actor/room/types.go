// room 包定义 Room Actor 的核心类型：ActorId 和 State。
package room

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// RoomId 是 Room Actor 的唯一标识。
type RoomId struct {
	RoomId int `json:"roomId"`
}

func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string             { return fmt.Sprintf("Room(%d)", id.RoomId) }

// ChatMessage 是房间内的聊天记录。
type ChatMessage struct {
	From    string `json:"from"`
	Content string `json:"content"`
	Time    int64  `json:"time"`
}

// BattleRecord 是房间内一次战斗的记录。
type BattleRecord struct {
	Attacker string `json:"attacker"`
	Target   string `json:"target"`
	Damage   int    `json:"damage"`
	Time     int64  `json:"time"`
}

// RoomState 是 Room Actor 的可变业务状态。
type RoomState struct {
	MaxPlayers int               `json:"maxPlayers"`
	Players    []types.PlayerId  `json:"players"`
	ChatLog    []ChatMessage     `json:"chatLog"`
	BattleLog  []BattleRecord    `json:"battleLog"`
}

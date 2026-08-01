// Package api 是 room actor 的对外契约。
//
// room 被 player 沿链 Call（Join / Chat），也接收 player 的 Post 上报（BriefChanged /
// Report）；它自己只 Post 给成员 player，从不反向 Call 任何玩家。
package api

import (
	"github.com/lcy03406/actor-go/actor"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// Join 玩家进房。摘要（Brief）由玩家随请求带来：房间不反向 Call 玩家取值，
// 之后的变化靠 BriefChanged 增量上报 —— 缓存替代反向调用。
type Join struct {
	Player ids.PlayerId    `json:"player"`
	Brief  playerapi.Brief `json:"brief"`
}

func (*Join) ReqType(_ ids.RoomId, _ actor.OkReply) string { return "Join" }

// Leave 玩家退房。
type Leave struct {
	Player ids.PlayerId `json:"player"`
}

func (*Leave) ReqType(_ ids.RoomId, _ actor.OkReply) string { return "Leave" }

// Chat 房间内发言。
type Chat struct {
	From ids.PlayerId `json:"from"`
	Text string       `json:"text"`
}

func (*Chat) ReqType(_ ids.RoomId, _ actor.OkReply) string { return "Chat" }

// BriefChanged 玩家摘要变化（升级 / 受伤）上报，房间据此刷新缓存。
type BriefChanged struct {
	Player ids.PlayerId    `json:"player"`
	Brief  playerapi.Brief `json:"brief"`
}

func (*BriefChanged) ReqType(_ ids.RoomId, _ actor.OkReply) string { return "BriefChanged" }

// Report 战斗记录上报（离散事件，房间转成广播）。
type Report struct {
	Actor ids.PlayerId `json:"actor"`
	Text  string       `json:"text"`
}

func (*Report) ReqType(_ ids.RoomId, _ actor.OkReply) string { return "Report" }

// Status 查询房间成员摘要（缓存内容，即房间看到的玩家视图）。
type Status struct{}

func (*Status) ReqType(_ ids.RoomId, _ *StatusReply) string { return "Status" }

// StatusReply 成员摘要按 OpenID 稳定排序，便于断言与展示。
type StatusReply struct {
	Room    int               `json:"room"`
	Members []playerapi.Brief `json:"members"`
}

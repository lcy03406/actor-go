// Package api 是 client actor 的对外契约。
//
// client 是"入口层 + 下行出口"：上行把客户端命令映射成 player 的请求并沿链 Call 下去，
// 下行只消费 Push 并渲染。业务规则一条都不写在这里。
package api

import (
	"github.com/lcy03406/actor-go/actor"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
	"github.com/lcy03406/actor-go/cmd/engineering_example/protocol"
)

// Login 建立连接并登录。
type Login struct {
	OpenID string `json:"openId"`
}

func (*Login) ReqType(_ ids.ClientId, _ actor.OkReply) string { return "Login" }

// Join 让本连接的玩家进入房间。
type Join struct {
	Room ids.RoomId `json:"room"`
}

func (*Join) ReqType(_ ids.ClientId, _ actor.OkReply) string { return "Join" }

// Say 在房间内发言。
type Say struct {
	Text string `json:"text"`
}

func (*Say) ReqType(_ ids.ClientId, _ actor.OkReply) string { return "Say" }

// Attack 攻击另一名玩家。
type Attack struct {
	Target ids.PlayerId `json:"target"`
}

func (*Attack) ReqType(_ ids.ClientId, _ actor.OkReply) string { return "Attack" }

// Status 查询玩家快照。
type Status struct{}

func (*Status) ReqType(_ ids.ClientId, _ *playerapi.PlayerView) string { return "Status" }

// Push 下行推送：服务端把报文 Post 给连接。
//
// 下行一律 Post（不需要回复）；rep 只用于客户端发起的请求的同步应答。
// 实际项目里 Push 会带 seq 关联到具体请求，本示例没有传输层，省略。
type Push struct {
	Data protocol.Msg `json:"data"`
}

func (*Push) ReqType(_ ids.ClientId, _ actor.OkReply) string { return "Push" }

// Drain 取走本连接收到的全部报文（示例 / 测试用；真实客户端在这里渲染 UI）。
type Drain struct{}

func (*Drain) ReqType(_ ids.ClientId, _ *DrainReply) string { return "Drain" }

type DrainReply struct {
	Msgs []protocol.Msg
}

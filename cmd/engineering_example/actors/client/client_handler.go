package client

import (
	"errors"
	"fmt"

	"github.com/lcy03406/actor-go/actor"

	clientapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/client/api"
	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// ─── 上行：连接 → 玩家 ───

// handleLogin 建立连接并登录。
// 连接层不建角色：玩家的创建、数据初始化、首帧推送全部在 player 那边。
func handleLogin(ctx *actor.ActorContext[ids.ClientId, State], req *clientapi.Login, spawning bool) (actor.OkReply, error) {
	if spawning {
		ctx.Open() // 连接常驻，直到对端断开
	}
	self := ids.PlayerId{OpenID: req.OpenID}
	if _, err := actor.ACall(ctx.Control(), self, &playerapi.Login{Client: ctx.Id()}); err != nil {
		return nil, fmt.Errorf("client: 登录 %s 失败: %w", req.OpenID, err)
	}
	ctx.State().Self = self
	return actor.OK, nil
}

// forward 把命令交给本连接操控的玩家。
//
// 这是入口层唯一该做的事：翻译形状、沿链向下、把错误原样带回。
// 泛型让五个命令共用一份实现，不会因为复制粘贴长出不同的行为。
func forward[Q actor.Request[ids.PlayerId, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx *actor.ActorContext[ids.ClientId, State], req Q,
) (R, error) {
	var zero R
	self := ctx.State().Self
	if self.OpenID == "" {
		return zero, errors.New("client: 尚未登录")
	}
	return actor.ACall(ctx.Control(), self, req)
}

func handleJoin(ctx *actor.ActorContext[ids.ClientId, State], req *clientapi.Join, _ bool) (actor.OkReply, error) {
	return forward(ctx, &playerapi.JoinRoom{Room: req.Room})
}

func handleSay(ctx *actor.ActorContext[ids.ClientId, State], req *clientapi.Say, _ bool) (actor.OkReply, error) {
	return forward(ctx, &playerapi.Say{Text: req.Text})
}

func handleAttack(ctx *actor.ActorContext[ids.ClientId, State], req *clientapi.Attack, _ bool) (actor.OkReply, error) {
	return forward(ctx, &playerapi.Attack{Target: req.Target})
}

// handleStatus 直接把玩家的快照当成自己的回复：请求与回复类型都定义在契约包里，
// 连接层不需要再包一层同构的壳。
func handleStatus(ctx *actor.ActorContext[ids.ClientId, State], _ *clientapi.Status, _ bool) (*playerapi.PlayerView, error) {
	return forward(ctx, &playerapi.Status{})
}

// ─── 下行：服务端 → 连接 ───

// handlePush 收下行报文。这里只入箱，不做任何解释 —— 解析与渲染是客户端的事。
func handlePush(ctx *actor.ActorContext[ids.ClientId, State], req *clientapi.Push, _ bool) (actor.OkReply, error) {
	ctx.State().Inbox = append(ctx.State().Inbox, req.Data)
	return actor.OK, nil
}

// handleDrain 取走并清空收件箱（示例 / 测试专用；真实项目这里是"发出 WebSocket 帧"）。
func handleDrain(ctx *actor.ActorContext[ids.ClientId, State], _ *clientapi.Drain, _ bool) (*clientapi.DrainReply, error) {
	s := ctx.State()
	msgs := s.Inbox
	s.Inbox = nil
	return &clientapi.DrainReply{Msgs: msgs}, nil
}

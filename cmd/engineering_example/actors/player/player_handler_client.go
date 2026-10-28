package player

import (
	"errors"
	"fmt"

	"github.com/lcy03406/actor-go/actor"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	roomapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/room/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// 本文件是"客户端意图"的落地：每条命令都只改本 actor 的数据，
// 需要别的 actor 配合时沿链向右 Call（下游），从不反向。

// handleLogin 登录：绑定下行出口，然后推首帧全量。
//
// 首帧必须是权威全量：客户端手里什么也没有，增量对它是无意义的。
// 重连也是同一个入口 —— 会话换了，基准就重来一遍。
func handleLogin(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.Login, _ bool) (actor.OkReply, error) {
	ctx.State().Data.Client = req.Client
	pushLoginOK(ctx)
	return actor.OK, nil
}

// handleJoinRoom 进房：先说动房间，再改自己的归属。
// 顺序很重要 —— 房间没收下就去改自己的状态，会留下"玩家以为在房里、房间不认得他"的半状态。
func handleJoinRoom(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.JoinRoom, _ bool) (actor.OkReply, error) {
	s := &ctx.State().Data
	if s.Room == req.Room {
		return actor.OK, nil
	}
	prev := s.Room
	if _, err := actor.ACall(ctx.Control(), req.Room, &roomapi.Join{Player: ctx.Id(), Brief: s.brief(ctx.Id())}); err != nil {
		return nil, fmt.Errorf("player: 进入房间 %d 失败: %w", req.Room.Seq, err)
	}
	s.Room = req.Room
	// 换房：旧房间要摘掉本成员。上报类消息用 Post，不等确认。
	if prev.Seq != 0 {
		if err := actor.APost(ctx.Control(), prev, &roomapi.Leave{Player: ctx.Id()}); err != nil {
			ctx.Logger().Warn("player: 退出旧房间失败", "room", prev, "err", err)
		}
	}
	pushState(ctx, "join")
	return actor.OK, nil
}

// handleSay 发言：房间是唯一的广播者，玩家自己不扇出。
func handleSay(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.Say, _ bool) (actor.OkReply, error) {
	s := &ctx.State().Data
	if s.Room.Seq == 0 {
		return nil, errors.New("player: 还没进入任何房间")
	}
	if _, err := actor.ACall(ctx.Control(), s.Room, &roomapi.Chat{From: ctx.Id(), Text: req.Text}); err != nil {
		return nil, fmt.Errorf("player: 发言失败: %w", err)
	}
	return actor.OK, nil
}

// handleAttack 发起攻击：这里只投递"我打出了多少攻击力"，结果由被击方回执。
//
// 攻击方和被击方是同一个 Group 的两个 actor，只能 Post：
// 若用 Call，A 等 B 回复、B 又要通知 A，就成了 A→B→A 的死锁环。
func handleAttack(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], req *playerapi.Attack, _ bool) (actor.OkReply, error) {
	s := &ctx.State().Data
	if req.Target == ctx.Id() {
		return nil, errors.New("player: 不能攻击自己")
	}
	if s.HP <= 0 {
		return nil, errors.New("player: 已经倒地，无法攻击")
	}
	if err := actor.APost(ctx.Control(), req.Target, &playerapi.Damage{
		From:   ctx.Id(),
		Room:   s.Room,
		Amount: s.atk(),
	}); err != nil {
		return nil, fmt.Errorf("player: 攻击 %s 失败: %w", req.Target.OpenID, err)
	}
	return actor.OK, nil
}

// handleStatus 查询快照：直接读自己的 State，不绕任何一圈。
func handleStatus(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], _ *playerapi.Status, _ bool) (*playerapi.PlayerView, error) {
	return ctx.State().Data.view(ctx.Id(), expRate(ctx)), nil
}

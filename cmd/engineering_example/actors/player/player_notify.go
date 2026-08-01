package player

import (
	"github.com/lcy03406/actor-go/actor"

	clientapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/client/api"
	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	roomapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/room/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
	"github.com/lcy03406/actor-go/cmd/engineering_example/protocol"
)

// 本文件是玩家全部"副作用出口"：下行推给连接、摘要上报给房间。
//
// 集中在一处的价值：handler 只表达"决定了什么"，不关心"怎么送出去"，
// 于是"下行一律 Post、首帧全量 + 之后增量"这些纪律只有一处可能写错。

// push 把一条报文推给本玩家的连接。
//
// 没有会话（还没登录 / 已断线）就丢弃：推送是尽力而为，
// 玩家没连上不该让业务处理失败 —— 他重连时会拿到新的首帧全量。
func push(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], msg protocol.Msg) {
	s := &ctx.State().Data
	if s.Client.Conn == "" {
		return
	}
	if err := actor.APost(ctx.Control(), s.Client, &clientapi.Push{Data: msg}); err != nil {
		ctx.Logger().Warn("player: 推送失败", "client", s.Client, "err", err)
	}
}

// pushLoginOK 首帧全量：新上场 / 重连都必须由权威补一次基准。
func pushLoginOK(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState]) {
	s := &ctx.State().Data
	push(ctx, protocol.LoginOK{
		Player:  ctx.Id().OpenID,
		Level:   s.Level,
		HP:      s.HP,
		MaxHP:   s.maxHP(),
		Atk:     s.atk(),
		Gold:    s.Gold,
		Exp:     s.Exp,
		Room:    s.Room.Seq,
		ExpRate: expRate(ctx),
	})
}

// pushState 增量：只带会变的字段，reason 让客户端知道该播哪段表现。
func pushState(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], reason string) {
	s := &ctx.State().Data
	push(ctx, protocol.StateUpdate{
		Level:  s.Level,
		HP:     s.HP,
		MaxHP:  s.maxHP(),
		Gold:   s.Gold,
		Exp:    s.Exp,
		Reason: reason,
	})
}

// reportBrief 把摘要上报给房间刷新缓存。房间不在（或已回收）就丢：
// 这是缓存补全，不是业务事实，丢了下一次变化会重新补上。
func reportBrief(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState]) {
	s := &ctx.State().Data
	if s.Room.Seq == 0 {
		return
	}
	if err := actor.APost(ctx.Control(), s.Room, &roomapi.BriefChanged{
		Player: ctx.Id(),
		Brief:  s.brief(ctx.Id()),
	}); err != nil {
		ctx.Logger().Warn("player: 上报房间缓存失败", "room", s.Room, "err", err)
	}
}

// report 把一条战报交给房间广播（句子由房间拼，玩家只报"谁做了什么"）。
func report(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], text string) {
	if ctx.State().Data.Room.Seq == 0 {
		return
	}
	if err := actor.APost(ctx.Control(), ctx.State().Data.Room, &roomapi.Report{
		Actor: ctx.Id(),
		Text:  text,
	}); err != nil {
		ctx.Logger().Warn("player: 上报战报失败", "err", err)
	}
}

// roomEventMsg 内部事件 → 客户端报文。两者的字段刻意不同：
// 内部用 ActorId，对外用数字 ID —— 客户端契约不该跟着服务端结构漂移。
func roomEventMsg(req *playerapi.RoomEvent) protocol.RoomEvent {
	return protocol.RoomEvent{Room: req.Room.Seq, Kind: req.Kind, Text: req.Text}
}

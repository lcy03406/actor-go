package room

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	roomapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/room/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// ─── 成员进出 ───

// handleJoin 玩家进房。摘要随请求带来，房间不接受"稍后我自己来取"。
func handleJoin(ctx *actor.ActorContext[ids.RoomId, State], req *roomapi.Join, spawning bool) (actor.OkReply, error) {
	s := ctx.State()
	s.ensure()
	_, existed := s.Members[req.Player]
	s.Members[req.Player] = req.Brief
	if spawning {
		// 房间常驻：一旦回收，成员缓存就一起丢了（房间没有持久化）。
		ctx.Open()
	}
	if !existed {
		broadcast(ctx, "join", fmt.Sprintf("%s 进入房间 %d", req.Player.OpenID, ctx.Id().Seq))
	}
	return actor.OK, nil
}

// handleLeave 玩家退房。重复退房是幂等的。
func handleLeave(ctx *actor.ActorContext[ids.RoomId, State], req *roomapi.Leave, _ bool) (actor.OkReply, error) {
	s := ctx.State()
	if _, ok := s.Members[req.Player]; !ok {
		return actor.OK, nil
	}
	delete(s.Members, req.Player)
	broadcast(ctx, "leave", fmt.Sprintf("%s 离开房间 %d", req.Player.OpenID, ctx.Id().Seq))
	return actor.OK, nil
}

// handleBriefChanged 成员摘要变化（升级 / 受伤）。只刷新缓存，不广播：
// 摘要变化很频繁，广播应该留给离散事件（进出房 / 发言 / 战报）。
func handleBriefChanged(ctx *actor.ActorContext[ids.RoomId, State], req *roomapi.BriefChanged, _ bool) (actor.OkReply, error) {
	s := ctx.State()
	if _, ok := s.Members[req.Player]; !ok {
		return actor.OK, nil
	}
	s.Members[req.Player] = req.Brief
	return actor.OK, nil
}

// ─── 广播 ───

// handleChat 发言。成员资格由房间裁决 —— 客户端说什么不算数。
func handleChat(ctx *actor.ActorContext[ids.RoomId, State], req *roomapi.Chat, _ bool) (actor.OkReply, error) {
	if _, ok := ctx.State().Members[req.From]; !ok {
		return nil, fmt.Errorf("room: %s 不在房间 %d 里", req.From.OpenID, ctx.Id().Seq)
	}
	broadcast(ctx, "chat", fmt.Sprintf("%s: %s", req.From.OpenID, req.Text))
	return actor.OK, nil
}

// handleReport 战报：玩家只上报"谁做了什么"，句子由房间拼。
func handleReport(ctx *actor.ActorContext[ids.RoomId, State], req *roomapi.Report, _ bool) (actor.OkReply, error) {
	broadcast(ctx, "report", fmt.Sprintf("%s %s", req.Actor.OpenID, req.Text))
	return actor.OK, nil
}

// handleStatus 房间视角的成员快照：这是缓存内容，不是反向查询玩家得到的结果。
func handleStatus(ctx *actor.ActorContext[ids.RoomId, State], _ *roomapi.Status, _ bool) (*roomapi.StatusReply, error) {
	return &roomapi.StatusReply{Room: ctx.Id().Seq, Members: ctx.State().briefs()}, nil
}

// ─── 扇出 ───

// broadcast 把房间事件扇出给全体成员。
//
// 只用 Post（AMulticast）：房间不知道成员此刻在不在线，也不该等任何人回复 ——
// 一条广播卡住一个不在线玩家，整个房间都得停下来等。投递失败即视为退场，摘掉缓存。
func broadcast(ctx *actor.ActorContext[ids.RoomId, State], kind, text string) {
	members := ctx.State().members()
	if len(members) == 0 {
		return
	}
	ev := &playerapi.RoomEvent{Room: ctx.Id(), Kind: kind, Text: text}
	errs, _ := actor.AMulticast(ctx.Control(), members, ev)
	for _, e := range errs {
		if e.Err == nil {
			continue
		}
		ctx.Logger().Warn("room: broadcast failed", "room", ctx.Id(), "player", e.Id, "err", e.Err)
		delete(ctx.State().Members, e.Id)
	}
}

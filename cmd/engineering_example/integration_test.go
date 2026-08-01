package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"

	clientapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/client/api"
	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	roomapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/room/api"
	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
	"github.com/lcy03406/actor-go/cmd/engineering_example/protocol"
	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

// 端到端用例：只经 client 契约发请求、只读下行与快照做断言，
// 与真实客户端看到的接口完全一致。

func newServer(t *testing.T) *setup.Server {
	t.Helper()
	srv, err := setup.New(setup.Config{DataDir: t.TempDir()})
	if err != nil {
		t.Fatalf("装配失败: %v", err)
	}
	t.Cleanup(srv.Close)
	return srv
}

type conn struct {
	id     ids.ClientId
	player ids.PlayerId
}

func login(t *testing.T, ctx context.Context, mgr *actor.Manager, openID string) conn {
	t.Helper()
	c := conn{
		id:     ids.ClientId{Conn: "conn-" + openID},
		player: ids.PlayerId{OpenID: openID},
	}
	if _, err := actor.Call(ctx, mgr, c.id, &clientapi.Login{OpenID: openID}); err != nil {
		t.Fatalf("登录 %s 失败: %v", openID, err)
	}
	return c
}

// waitMsgs 等到至少 n 条下行再一起取走。
//
// 下行是异步的（Post 不等送达，房间广播还要绕玩家一圈），所以按条数轮询，
// 而不是 sleep 一个凑出来的时长 —— 后者在慢机器上必然偶发失败。
func waitMsgs(t *testing.T, ctx context.Context, mgr *actor.Manager, c conn, n int) []protocol.Msg {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	var got []protocol.Msg
	for {
		rep, err := actor.Call(ctx, mgr, c.id, &clientapi.Drain{})
		if err != nil {
			t.Fatalf("取 %s 下行失败: %v", c.player.OpenID, err)
		}
		got = append(got, rep.Msgs...)
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			t.Fatalf("%s 等待 %d 条下行超时（收到 %d 条）", c.player.OpenID, n, len(got))
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func status(t *testing.T, ctx context.Context, mgr *actor.Manager, c conn) *playerapi.PlayerView {
	t.Helper()
	view, err := actor.Call(ctx, mgr, c.id, &clientapi.Status{})
	if err != nil {
		t.Fatalf("查询 %s 快照失败: %v", c.player.OpenID, err)
	}
	return view
}

func loginOK(t *testing.T, msgs []protocol.Msg) protocol.LoginOK {
	t.Helper()
	for _, m := range msgs {
		if ok, is := m.(protocol.LoginOK); is {
			return ok
		}
	}
	t.Fatalf("下行里没有首帧 LoginOK: %+v", msgs)
	return protocol.LoginOK{}
}

func hasStateUpdate(msgs []protocol.Msg, reason string) bool {
	for _, m := range msgs {
		if u, ok := m.(protocol.StateUpdate); ok && u.Reason == reason {
			return true
		}
	}
	return false
}

func roomEvents(msgs []protocol.Msg, kind string) []string {
	var out []string
	for _, m := range msgs {
		if e, ok := m.(protocol.RoomEvent); ok && e.Kind == kind {
			out = append(out, e.Text)
		}
	}
	return out
}

// TestCombatFlow 走一遍完整链路：登录 → 进房 → 聊天 → 三击必杀 → 房间缓存 → 热更配置。
func TestCombatFlow(t *testing.T) {
	srv := newServer(t)
	mgr := srv.Mgr
	ctx := context.Background()

	alice := login(t, ctx, mgr, "alice")
	bob := login(t, ctx, mgr, "bob")

	// 首帧必须是权威全量：派生字段（MaxHP / Atk / 倍率）也在里面。
	first := loginOK(t, waitMsgs(t, ctx, mgr, alice, 1))
	if first.Level != 1 || first.HP != 100 || first.MaxHP != 100 || first.Atk != 40 || first.ExpRate != 100 {
		t.Fatalf("首帧不符: %+v", first)
	}
	waitMsgs(t, ctx, mgr, bob, 1)

	// 进同一个房间：alice 收到自己进场 + bob 进场，bob 只有自己那条。
	for _, c := range []conn{alice, bob} {
		if _, err := actor.Call(ctx, mgr, c.id, &clientapi.Join{Room: ids.RoomId{Seq: 3}}); err != nil {
			t.Fatalf("%s 进房失败: %v", c.player.OpenID, err)
		}
	}
	if got := roomEvents(waitMsgs(t, ctx, mgr, alice, 3), "join"); len(got) != 2 {
		t.Fatalf("alice 应收到 2 条进场广播，得到 %v", got)
	}
	if got := roomEvents(waitMsgs(t, ctx, mgr, bob, 2), "join"); len(got) != 1 {
		t.Fatalf("bob 应收到 1 条进场广播，得到 %v", got)
	}

	// 聊天：房间广播回全体成员。
	if _, err := actor.Call(ctx, mgr, alice.id, &clientapi.Say{Text: "大家好啊"}); err != nil {
		t.Fatalf("发言失败: %v", err)
	}
	if got := roomEvents(waitMsgs(t, ctx, mgr, alice, 1), "chat"); len(got) != 1 || got[0] != "alice: 大家好啊" {
		t.Fatalf("alice 没收到自己的发言广播: %v", got)
	}
	if got := roomEvents(waitMsgs(t, ctx, mgr, bob, 1), "chat"); len(got) != 1 {
		t.Fatalf("bob 没收到发言广播: %v", got)
	}

	// 三击必杀：Lv1 攻击力 40，对 0 防御造成 40，100 → 60 → 20 → 倒地。
	for i, wantHP := range []int{60, 20, 0} {
		if _, err := actor.Call(ctx, mgr, alice.id, &clientapi.Attack{Target: bob.player}); err != nil {
			t.Fatalf("第 %d 击失败: %v", i+1, err)
		}
		// 前两下各一条增量；第三下额外各收一条战报。
		push := 1
		if i == 2 {
			push = 2
		}
		bobPushes := waitMsgs(t, ctx, mgr, bob, push)
		alicePushes := waitMsgs(t, ctx, mgr, alice, push)
		if !hasStateUpdate(bobPushes, "damage") {
			t.Fatalf("第 %d 击：bob 没收到受击增量: %+v", i+1, bobPushes)
		}
		if !hasStateUpdate(alicePushes, "combat") {
			t.Fatalf("第 %d 击：alice 没收到战斗结算增量: %+v", i+1, alicePushes)
		}
		if got := status(t, ctx, mgr, bob); got.HP != wantHP {
			t.Fatalf("第 %d 击后 bob HP = %d，期望 %d", i+1, got.HP, wantHP)
		}
		if i == 2 {
			if got := roomEvents(bobPushes, "report"); len(got) != 1 || got[0] != "alice 击倒了 bob" {
				t.Fatalf("倒地战报不符: %v", got)
			}
		}
	}

	// 奖励结算：命中 5 金币 / 10 经验，击杀换成一整份 20 金币 / 40 经验；
	// 10 + 10 + 40 = 60 经验，1 级需 30，升到 2 级余 30，升级回满 120 血。
	av := status(t, ctx, mgr, alice)
	if av.Level != 2 || av.Exp != 30 || av.Gold != 30 || av.HP != 120 || av.MaxHP != 120 {
		t.Fatalf("击杀奖励结算不符: %+v", av)
	}

	// 房间缓存：房间看到的是玩家上报的摘要，不是反向查询玩家得到的。
	roomView, err := actor.Call(ctx, mgr, ids.RoomId{Seq: 3}, &roomapi.Status{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	want := []playerapi.Brief{
		{OpenID: "alice", Level: 2, HP: 120},
		{OpenID: "bob", Level: 1, HP: 0},
	}
	if len(roomView.Members) != len(want) {
		t.Fatalf("房间成员数不符: %+v", roomView.Members)
	}
	for i, m := range want {
		if roomView.Members[i] != m {
			t.Fatalf("房间成员[%d] = %+v，期望 %+v", i, roomView.Members[i], m)
		}
	}

	// 世界配置热更：world 发布 → 容器推送 → 玩家缓存更新（最终一致，轮询等待）。
	if _, err := actor.Call(ctx, mgr, ids.TheWorld(), &worldapi.SetConfig{ExpRate: 200}); err != nil {
		t.Fatalf("热更配置失败: %v", err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		rate := status(t, ctx, mgr, alice).ExpRate
		if rate == 200 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("alice 读到的经验倍率始终是 %d，期望 200", rate)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestGrainPersistence 验证持久化边界：角色成长必须活过重启，会话与房间归属不落盘。
func TestGrainPersistence(t *testing.T) {
	dir := t.TempDir()
	ctx := context.Background()

	srv1, err := setup.New(setup.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("首次装配失败: %v", err)
	}
	t.Cleanup(srv1.Close)

	alice := login(t, ctx, srv1.Mgr, "alice")
	bob := login(t, ctx, srv1.Mgr, "bob")
	if _, err := actor.Call(ctx, srv1.Mgr, alice.id, &clientapi.Join{Room: ids.RoomId{Seq: 3}}); err != nil {
		t.Fatalf("alice 进房失败: %v", err)
	}
	for i := 0; i < 3; i++ {
		if _, err := actor.Call(ctx, srv1.Mgr, alice.id, &clientapi.Attack{Target: bob.player}); err != nil {
			t.Fatalf("第 %d 击失败: %v", i+1, err)
		}
	}
	if got := status(t, ctx, srv1.Mgr, alice); got.Level != 2 || got.Gold != 30 {
		t.Fatalf("升级前状态不符: %+v", got)
	}

	// 优雅停机：退出钩子在 CloseManager 里把 grain 落盘。
	srv1.Close()

	srv2, err := setup.New(setup.Config{DataDir: dir})
	if err != nil {
		t.Fatalf("二次装配失败: %v", err)
	}
	t.Cleanup(srv2.Close)

	// 重连 = 新连接 + 重新登录：绑定关系重建，数据从磁盘恢复。
	again := conn{
		id:     ids.ClientId{Conn: "conn-alice-reconnect"},
		player: alice.player,
	}
	if _, err := actor.Call(ctx, srv2.Mgr, again.id, &clientapi.Login{OpenID: "alice"}); err != nil {
		t.Fatalf("重连登录失败: %v", err)
	}
	first := loginOK(t, waitMsgs(t, ctx, srv2.Mgr, again, 1))
	if first.Level != 2 || first.Exp != 30 || first.Gold != 30 || first.HP != 120 {
		t.Fatalf("重启后成长数据没恢复: %+v", first)
	}
	if first.Room != 0 {
		t.Fatalf("房间归属是运行时字段，不该被持久化: %+v", first)
	}
}

// TestQueryDoesNotSpawn 验证注册语义：给不存在的玩家发请求必须报错，而不是造出一个空角色。
func TestQueryDoesNotSpawn(t *testing.T) {
	srv := newServer(t)
	ctx := context.Background()

	_, err := actor.Call(ctx, srv.Mgr, ids.PlayerId{OpenID: "ghost"}, &playerapi.Status{})
	if err == nil {
		t.Fatal("对不存在的玩家发请求应当失败")
	}
	var nf *actor.ActorNotFoundError
	if !errors.As(err, &nf) {
		t.Fatalf("期望 ActorNotFoundError，得到 %v", err)
	}
}

// Package demo 是脚本化的"客户端模拟器"：像真实客户端一样发请求、读下行。
//
// 它只依赖 ids + 各 actor 的契约包（actors/*/api）与 protocol，不 import 任何
// actor 实现包 —— 这正是"契约包"存在的意义：调用方不必知道谁在实现它。
package demo

import (
	"context"
	"fmt"
	"io"
	"time"

	"github.com/lcy03406/actor-go/actor"

	clientapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/client/api"
	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	roomapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/room/api"
	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
	"github.com/lcy03406/actor-go/cmd/engineering_example/protocol"
)

// Run 跑一段固定剧本：两条连接登录 → 进同一个房间 → 聊天 → 开打 → 热更世界配置。
func Run(ctx context.Context, mgr *actor.Manager, out io.Writer) error {
	s := &script{mgr: mgr, out: out}
	return s.run(ctx)
}

// conn 是本地的一条连接句柄：只记 ClientId，所有请求依旧经 client actor 走完整链路。
type conn struct {
	id     ids.ClientId
	player ids.PlayerId
}

type script struct {
	mgr *actor.Manager
	out io.Writer
}

func (s *script) printf(format string, args ...any) {
	fmt.Fprintf(s.out, format+"\n", args...)
}

func (s *script) run(ctx context.Context) error {
	alice, err := s.login(ctx, "alice")
	if err != nil {
		return err
	}
	bob, err := s.login(ctx, "bob")
	if err != nil {
		return err
	}

	s.printf("\n=== 1. 登录：连接 → player，玩家回推首帧全量 ===")
	s.show("alice", s.expect(ctx, alice, 1))
	s.show("bob", s.expect(ctx, bob, 1))

	s.printf("\n=== 2. 进 3 号房间：房间把成员进出广播给全体成员 ===")
	for _, c := range []*conn{alice, bob} {
		if _, err := actor.Call(ctx, s.mgr, c.id, &clientapi.Join{Room: ids.RoomId{Seq: 3}}); err != nil {
			return fmt.Errorf("demo: %s 进房失败: %w", c.player.OpenID, err)
		}
	}
	// alice 会收到两条：自己进场 + bob 进场；bob 只有自己那条。
	s.show("alice", s.expect(ctx, alice, 3))
	s.show("bob", s.expect(ctx, bob, 2))

	s.printf("\n=== 3. 聊天：玩家 → 房间 → 广播回全体成员 ===")
	if _, err := actor.Call(ctx, s.mgr, alice.id, &clientapi.Say{Text: "一会儿一起打一架？"}); err != nil {
		return fmt.Errorf("demo: 发言失败: %w", err)
	}
	s.show("alice", s.expect(ctx, alice, 1))
	s.show("bob", s.expect(ctx, bob, 1))

	s.printf("\n=== 4. 开打：同组 peer 只 Post，结果靠回执 ===")
	for round := 1; round <= 3; round++ {
		if _, err := actor.Call(ctx, s.mgr, alice.id, &clientapi.Attack{Target: bob.player}); err != nil {
			return fmt.Errorf("demo: 攻击失败: %w", err)
		}
		// 前两下只有状态增量；最后一下倒地，双方还会各收到一条战报。
		wait := 1
		if round == 3 {
			wait = 2
		}
		s.printf("  第 %d 击", round)
		s.show("bob", s.expect(ctx, bob, wait))
		s.show("alice", s.expect(ctx, alice, wait))
	}

	aliceView, err := s.status(ctx, alice)
	if err != nil {
		return err
	}
	bobView, err := s.status(ctx, bob)
	if err != nil {
		return err
	}
	s.printf("  结算：alice Lv%d 金币 %d 经验 %d（击杀奖励已入账）", aliceView.Level, aliceView.Gold, aliceView.Exp)
	s.printf("        bob   Lv%d HP %d/%d（已倒地，重复攻击不再结算）", bobView.Level, bobView.HP, bobView.MaxHP)

	s.printf("\n=== 5. 房间缓存：房间看到的是玩家上报的摘要，不是反向查询 ===")
	roomView, err := actor.Call(ctx, s.mgr, ids.RoomId{Seq: 3}, &roomapi.Status{})
	if err != nil {
		return fmt.Errorf("demo: 查询房间失败: %w", err)
	}
	for _, m := range roomView.Members {
		s.printf("  %-6s Lv%d HP %d", m.OpenID, m.Level, m.HP)
	}

	s.printf("\n=== 6. 热更世界配置：world 发布，玩家从容器读到新值 ===")
	if _, err := actor.Call(ctx, s.mgr, ids.TheWorld(), &worldapi.SetConfig{ExpRate: 200}); err != nil {
		return fmt.Errorf("demo: 热更配置失败: %w", err)
	}
	rate, err := s.waitExpRate(ctx, alice, 200)
	if err != nil {
		return err
	}
	s.printf("  alice 读到的经验倍率：%d%%（world 没有被打扰过，读的是容器推送）", rate)

	s.printf("\n=== 完成：关闭 Manager 时 grain 会把玩家数据落盘 ===")
	return nil
}

// ─── 请求助手 ───

func (s *script) login(ctx context.Context, openID string) (*conn, error) {
	c := &conn{
		id:     ids.ClientId{Conn: "conn-" + openID},
		player: ids.PlayerId{OpenID: openID},
	}
	if _, err := actor.Call(ctx, s.mgr, c.id, &clientapi.Login{OpenID: openID}); err != nil {
		return nil, fmt.Errorf("demo: 登录 %s 失败: %w", openID, err)
	}
	return c, nil
}

// status 查询玩家快照：权威数据，不受下行异步影响。
func (s *script) status(ctx context.Context, c *conn) (*playerapi.PlayerView, error) {
	view, err := actor.Call(ctx, s.mgr, c.id, &clientapi.Status{})
	if err != nil {
		return nil, fmt.Errorf("demo: 查询 %s 状态失败: %w", c.player.OpenID, err)
	}
	return view, nil
}

// waitExpRate 轮询玩家快照直到读到期望的倍率。
// 容器推送是异步的，读到自己想要的值为止比 sleep 一个"够久"的时间更可靠。
func (s *script) waitExpRate(ctx context.Context, c *conn, want int) (int, error) {
	deadline := time.Now().Add(2 * time.Second)
	for {
		view, err := s.status(ctx, c)
		if err != nil {
			return 0, err
		}
		if view.ExpRate == want || time.Now().After(deadline) {
			return view.ExpRate, nil
		}
		if err := sleep(ctx, 10*time.Millisecond); err != nil {
			return 0, err
		}
	}
}

// ─── 下行助手 ───

func (s *script) drain(ctx context.Context, c *conn) ([]protocol.Msg, error) {
	rep, err := actor.Call(ctx, s.mgr, c.id, &clientapi.Drain{})
	if err != nil {
		return nil, fmt.Errorf("demo: 取 %s 下行失败: %w", c.player.OpenID, err)
	}
	return rep.Msgs, nil
}

// expect 等到至少 n 条下行报文再一起取走。
//
// 下行是异步的：服务端 Post 之后并不等连接处理完，房间广播还要绕玩家一圈。
// 所以这里按"报文条数"轮询，而不是 sleep 一个拍脑袋的时长。
func (s *script) expect(ctx context.Context, c *conn, n int) []protocol.Msg {
	deadline := time.Now().Add(2 * time.Second)
	var got []protocol.Msg
	for {
		batch, err := s.drain(ctx, c)
		if err != nil {
			s.printf("  [%s] %v", c.player.OpenID, err)
			return got
		}
		got = append(got, batch...)
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		if err := sleep(ctx, 10*time.Millisecond); err != nil {
			return got
		}
	}
}

// show 以"谁 收到什么"的格式打印一批下行报文，名字左对齐便于对读。
func (s *script) show(who string, msgs []protocol.Msg) {
	if len(msgs) == 0 {
		s.printf("  %-5s （没有收到报文）", who)
		return
	}
	for _, m := range msgs {
		s.printf("  %-5s %s", who, describe(m))
	}
}

// describe 把报文渲染成一行文本。真实项目里这一步是客户端的 UI 渲染。
func describe(msg protocol.Msg) string {
	switch m := msg.(type) {
	case protocol.LoginOK:
		return fmt.Sprintf("首帧  %s Lv%d HP %d/%d Atk %d 金币 %d 经验 %d 房间 %d 倍率 %d%%",
			m.Player, m.Level, m.HP, m.MaxHP, m.Atk, m.Gold, m.Exp, m.Room, m.ExpRate)
	case protocol.StateUpdate:
		return fmt.Sprintf("状态[%s] Lv%d HP %d/%d 金币 %d 经验 %d",
			m.Reason, m.Level, m.HP, m.MaxHP, m.Gold, m.Exp)
	case protocol.RoomEvent:
		return fmt.Sprintf("房间[%s] %s", m.Kind, m.Text)
	}
	return "未知报文"
}

func sleep(ctx context.Context, d time.Duration) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(d):
		return nil
	}
}

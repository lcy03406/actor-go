// 集成测试：模拟多玩家完整流程，覆盖所有 actor + 子模块 + 持久化。
//
// 通过 cluster.Call/Post 调用，验证 handler + RPC + cluster 完整组装链路。
package main

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/cluster"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/chat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/attr"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/inventory"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/skill"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
	"github.com/lcy03406/actor-go/cmd/engineering_example/setup"
)

// ─── 辅助 ───

type testNode struct {
	router *setup.Router
	cancel context.CancelFunc
}

func startTestNode(t *testing.T) *testNode {
	t.Helper()

	dataDir := filepath.Join(t.TempDir(), "data")
	os.MkdirAll(dataDir, 0755)

	ctx, cancel := context.WithCancel(context.Background())

	router, _, err := setup.StartNode(ctx, setup.NodeConfig{
		NodeType: "all-in-one",
		NodeID:   "test-node",
		Addr:     "localhost:0",
	})
	if err != nil {
		cancel()
		t.Fatalf("启动节点失败: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	return &testNode{router: router, cancel: cancel}
}

func (n *testNode) Close() { n.router.Close(); n.cancel() }

func pid(name string) types.PlayerId {
	return types.PlayerId{ServerId: 1, OpenId: name}
}

// ─── 单玩家完整生命周期 ───

func TestPlayerFullLifecycle(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	p1 := pid("alice")

	// 创建玩家
	if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 200, InitLevel: 3}); err != nil {
		t.Fatalf("创建玩家失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 查询状态
	status, err := cluster.Call(ctx, n.router, p1, &player.PlayerStatusReq{})
	if err != nil {
		t.Fatalf("查询状态失败: %v", err)
	}
	if status.HP != 200 {
		t.Errorf("HP 期望=200, 实际=%d", status.HP)
	}
	if status.Level != 3 {
		t.Errorf("Level 期望=3, 实际=%d", status.Level)
	}

	// 攻击
	atk, err := cluster.Call(ctx, n.router, p1, &player.Attack{Damage: 30})
	if err != nil {
		t.Fatalf("攻击失败: %v", err)
	}
	if !atk.Alive {
		t.Error("玩家应该存活")
	}

	// 治疗
	heal, err := cluster.Call(ctx, n.router, p1, &player.Heal{Amount: 20})
	if err != nil {
		t.Fatalf("治疗失败: %v", err)
	}
	if heal.NewHP != atk.RemainingHP+20 {
		t.Errorf("治疗后 HP 期望=%d, 实际=%d", atk.RemainingHP+20, heal.NewHP)
	}

	// 加金币
	gold, err := cluster.Call(ctx, n.router, p1, &player.AddGold{Amount: 200})
	if err != nil {
		t.Fatalf("加金币失败: %v", err)
	}
	if gold.NewGold != 300 {
		t.Errorf("金币 期望=300, 实际=%d", gold.NewGold)
	}
}

// ─── 子模块：属性 ───

func TestAttrModule(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	p1 := pid("attr_tester")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100, InitLevel: 1})
	time.Sleep(30 * time.Millisecond)

	exp1, err := cluster.Call(ctx, n.router, p1, &attr.AddExp{Amount: 50})
	if err != nil {
		t.Fatalf("加经验失败: %v", err)
	}
	if exp1.LevelUp {
		t.Error("50 经验不应升级")
	}

	exp2, err := cluster.Call(ctx, n.router, p1, &attr.AddExp{Amount: 60})
	if err != nil {
		t.Fatalf("加经验失败: %v", err)
	}
	if !exp2.LevelUp {
		t.Error("110 经验应该升级")
	}
	if exp2.Level != 2 {
		t.Errorf("Level 期望=2, 实际=%d", exp2.Level)
	}

	attrs, err := cluster.Call(ctx, n.router, p1, &attr.QueryAttr{})
	if err != nil {
		t.Fatalf("查询属性失败: %v", err)
	}
	if attrs.Level != 2 {
		t.Errorf("Level 期望=2, 实际=%d", attrs.Level)
	}

	up, err := cluster.Call(ctx, n.router, p1, &attr.UpgradeAttr{Stat: "atk"})
	if err != nil {
		t.Fatalf("升级属性失败: %v", err)
	}
	if up == nil {
		t.Error("属性升级不应返回 nil")
	}
}

// ─── 子模块：道具 ───

func TestInventoryModule(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	p1 := pid("inv_tester")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100, InitLevel: 1})
	time.Sleep(30 * time.Millisecond)

	add, err := cluster.Call(ctx, n.router, p1, &inventory.AddItem{
		ID: 1, Name: "血瓶", Count: 3, Type: "potion",
	})
	if err != nil {
		t.Fatalf("添加道具失败: %v", err)
	}
	if !add.Added {
		t.Error("应该成功添加道具")
	}

	bag, err := cluster.Call(ctx, n.router, p1, &inventory.ListItems{})
	if err != nil {
		t.Fatalf("查看背包失败: %v", err)
	}
	if len(bag.Items) != 1 {
		t.Errorf("道具数 期望=1, 实际=%d", len(bag.Items))
	}

	use, err := cluster.Call(ctx, n.router, p1, &inventory.UseItem{ID: 1})
	if err != nil {
		t.Fatalf("使用道具失败: %v", err)
	}
	if !use.Used {
		t.Error("应该成功使用道具")
	}

	bag2, err := cluster.Call(ctx, n.router, p1, &inventory.ListItems{})
	if err != nil {
		t.Fatalf("查看背包失败: %v", err)
	}
	if bag2.Used != 2 {
		t.Errorf("使用后已用格数 期望=2, 实际=%d", bag2.Used)
	}

	rm, err := cluster.Call(ctx, n.router, p1, &inventory.RemoveItem{ID: 1, Count: 2})
	if err != nil {
		t.Fatalf("移除道具失败: %v", err)
	}
	if !rm.Removed {
		t.Error("应该成功移除道具")
	}
}

// ─── 子模块：技能 ───

func TestSkillModule(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	p1 := pid("skill_tester")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100, InitLevel: 1})
	time.Sleep(30 * time.Millisecond)
	cluster.Call(ctx, n.router, p1, &player.AddGold{Amount: 500})

	learn, err := cluster.Call(ctx, n.router, p1, &skill.LearnSkill{
		ID: 101, Name: "火球术", Cost: 50,
	})
	if err != nil {
		t.Fatalf("学习技能失败: %v", err)
	}
	if !learn.Learned {
		t.Fatalf("学习失败: %s", learn.Reason)
	}

	list, err := cluster.Call(ctx, n.router, p1, &skill.ListSkills{})
	if err != nil {
		t.Fatalf("查看技能失败: %v", err)
	}
	if len(list.Skills) != 1 {
		t.Errorf("技能数 期望=1, 实际=%d", len(list.Skills))
	}

	cast, err := cluster.Call(ctx, n.router, p1, &skill.CastSkill{
		ID: 101, Target: "goblin", TargetPos: logic.Point{X: 3, Y: 0},
	})
	if err != nil {
		t.Fatalf("释放技能失败: %v", err)
	}
	if !cast.Cast {
		t.Fatalf("释放失败: %s", cast.Reason)
	}
	if cast.Damage <= 0 {
		t.Error("伤害应该大于 0")
	}

	cast2, err := cluster.Call(ctx, n.router, p1, &skill.CastSkill{
		ID: 101, Target: "goblin", TargetPos: logic.Point{X: 1, Y: 0},
	})
	if err != nil {
		t.Fatalf("释放技能失败: %v", err)
	}
	if cast2.Cast {
		t.Error("冷却中应该释放失败")
	}

	cast3, err := cluster.Call(ctx, n.router, p1, &skill.CastSkill{
		ID: 101, Target: "dragon", TargetPos: logic.Point{X: 10, Y: 0},
	})
	if err != nil {
		t.Fatalf("释放技能失败: %v", err)
	}
	if cast3.Cast {
		t.Error("超出范围应该释放失败")
	}
}

// ─── Room / Chat ───

func TestRoomActor(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	r1 := room.RoomId{RoomId: 100}
	if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, r1, &room.CreateRoom{MaxPlayers: 10}); err != nil {
		t.Fatalf("创建房间失败: %v", err)
	}
	time.Sleep(30 * time.Millisecond)

	join, err := cluster.Call(ctx, n.router, r1, &room.JoinRoom{PlayerId: "alice"})
	if err != nil {
		t.Fatalf("加入房间失败: %v", err)
	}
	if join.CurrentCount != 1 {
		t.Errorf("人数 期望=1, 实际=%d", join.CurrentCount)
	}

	info, err := cluster.Call(ctx, n.router, r1, &room.RoomInfo{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	if info.MaxPlayers != 10 {
		t.Errorf("MaxPlayers 期望=10, 实际=%d", info.MaxPlayers)
	}
}

func TestChatActor(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	ch := chat.ChatId{Channel: "world"}
	reply, err := cluster.Call(ctx, n.router, ch, &chat.SendMessage{Text: "hello"})
	if err != nil {
		t.Fatalf("发送消息失败: %v", err)
	}
	if reply.Echo != "hello" {
		t.Errorf("Echo 期望=hello, 实际=%s", reply.Echo)
	}
}

// ─── 多玩家并发 ───

func TestMultiPlayerConcurrent(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	players := []string{"alice", "bob", "charlie", "diana", "eve"}
	for _, name := range players {
		cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, pid(name), &player.Login{InitHP: 100, InitLevel: 1})
	}
	time.Sleep(50 * time.Millisecond)

	errCh := make(chan error, len(players))
	for _, name := range players {
		go func(name string) {
			p := pid(name)
			if _, err := cluster.Call(ctx, n.router, p, &player.Attack{Damage: 10}); err != nil {
				errCh <- err
				return
			}
			if _, err := cluster.Call(ctx, n.router, p, &attr.AddExp{Amount: 150}); err != nil {
				errCh <- err
				return
			}
			errCh <- nil
		}(name)
	}
	for range players {
		if err := <-errCh; err != nil {
			t.Errorf("并发操作失败: %v", err)
		}
	}

	for _, name := range players {
		status, err := cluster.Call(ctx, n.router, pid(name), &player.PlayerStatusReq{})
		if err != nil {
			t.Errorf("%s 查询失败: %v", name, err)
			continue
		}
		if status.HP >= 100 {
			t.Errorf("%s 攻击后 HP 应该减少, 实际=%d", name, status.HP)
		}
		if status.Level < 2 {
			t.Errorf("%s 150 经验应该至少 Lv2, 实际=%d", name, status.Level)
		}
	}
}

// ─── Close ───

func TestPlayerClose(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	p1 := pid("temp")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100, InitLevel: 1})
	time.Sleep(30 * time.Millisecond)

	if _, err := cluster.Call(ctx, n.router, p1, &player.Close{}); err != nil {
		t.Fatalf("关闭失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	_, err := cluster.Call(ctx, n.router, p1, &player.PlayerStatusReq{})
	if err == nil {
		t.Error("关闭后应该查询失败")
	}
}

// ─── Benchmark ───

func BenchmarkPlayerAttack(b *testing.B) {
	n := startTestNode(&testing.T{})
	defer n.Close()
	ctx := context.Background()

	p1 := pid("bench")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100000, InitLevel: 1})
	time.Sleep(50 * time.Millisecond)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.Call(ctx, n.router, p1, &player.Attack{Damage: 1})
	}
}

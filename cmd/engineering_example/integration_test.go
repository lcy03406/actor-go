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
		DataDir:  dataDir,
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

	// 治疗受 MaxHP 上限约束：初始即满血(200)，治疗应保持 200 且不溢出。
	// （HP 上限机制把 Heal 与 MaxHP 模块结合，避免无限回血。）
	heal, err := cluster.Call(ctx, n.router, p1, &player.ControlHeal{Amount: 20})
	if err != nil {
		t.Fatalf("治疗失败: %v", err)
	}
	if heal.NewHP != 200 {
		t.Errorf("满血治疗后 HP 期望=200, 实际=%d", heal.NewHP)
	}
	if heal.Healed != 0 {
		t.Errorf("满血治疗时 Healed 期望=0, 实际=%d", heal.Healed)
	}

	// 升级提升 MaxHP 上限：加足量经验触发升级（attr.AddExp 在升级时拉高 MaxHP 并回满血）。
	// 之后在房间内受击掉血即可用 Heal 消耗金币续航（金币来自战斗/AddGold）。
	if _, err := cluster.Call(ctx, n.router, p1, &attr.AddExp{Amount: 300}); err != nil {
		t.Fatalf("加经验失败: %v", err)
	}
	time.Sleep(30 * time.Millisecond)
	status2, err := cluster.Call(ctx, n.router, p1, &player.PlayerStatusReq{})
	if err != nil {
		t.Fatalf("查询状态失败: %v", err)
	}
	if status2.MaxHP <= 200 {
		t.Errorf("升级后 MaxHP 应提升 (>200), 实际=%d", status2.MaxHP)
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

	learn, err := cluster.Call(ctx, n.router, p1, &skill.ControlLearn{
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

	cast, err := cluster.Call(ctx, n.router, p1, &skill.ControlCast{
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

	cast2, err := cluster.Call(ctx, n.router, p1, &skill.ControlCast{
		ID: 101, Target: "goblin", TargetPos: logic.Point{X: 1, Y: 0},
	})
	if err != nil {
		t.Fatalf("释放技能失败: %v", err)
	}
	if cast2.Cast {
		t.Error("冷却中应该释放失败")
	}

	cast3, err := cluster.Call(ctx, n.router, p1, &skill.ControlCast{
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

	join, err := cluster.Call(ctx, n.router, r1, &room.JoinRoom{Player: pid("alice")})
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
	t.Skip("Chat Actor 已移除，聊天功能整合进 Room")
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
		if status.Level < 2 {
			t.Errorf("%s 150 经验应该至少 Lv2, 实际=%d", name, status.Level)
		}
	}
}

// ─── 跨 Actor 通信：Player → Room / Chat ───

func TestPlayerCrossActorRoom(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	// 1. 创建 Player
	p1 := pid("cross_room_player")
	if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100, InitLevel: 1}); err != nil {
		t.Fatalf("创建玩家失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 2. 创建 Room
	r1 := room.RoomId{RoomId: 200}
	if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, r1, &room.CreateRoom{MaxPlayers: 10}); err != nil {
		t.Fatalf("创建房间失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 3. Player 通过 PlayerJoinRoom 请求加入 Room（Player handler 内 actor.Post → Room）
	joinReply, err := cluster.Call(ctx, n.router, p1, &player.ControlJoinRoom{RoomId: 200})
	if err != nil {
		t.Fatalf("Player 加入房间失败: %v", err)
	}
	if !joinReply.Success {
		t.Errorf("Player 加入房间应成功: %s", joinReply.Reason)
	}

	// 4. 验证 Room 状态已更新
	time.Sleep(30 * time.Millisecond)
	roomInfo, err := cluster.Call(ctx, n.router, r1, &room.RoomInfo{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	if roomInfo.MaxPlayers != 10 {
		t.Errorf("MaxPlayers 期望=10, 实际=%d", roomInfo.MaxPlayers)
	}
}

func TestPlayerCrossActorRoomChatAndBattle(t *testing.T) {
	n := startTestNode(t)
	defer n.Close()
	ctx := context.Background()

	// 1. 创建两名玩家并加入同一房间
	attacker := pid("cross_chat_attacker")
	target := pid("cross_chat_target")
	for _, p := range []types.PlayerId{attacker, target} {
		if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p, &player.Login{InitHP: 1000, InitLevel: 5}); err != nil {
			t.Fatalf("创建玩家失败: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	r1 := room.RoomId{RoomId: 300}
	if err := cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, r1, &room.CreateRoom{MaxPlayers: 10}); err != nil {
		t.Fatalf("创建房间失败: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	for _, p := range []types.PlayerId{attacker, target} {
		joinReply, err := cluster.Call(ctx, n.router, p, &player.ControlJoinRoom{RoomId: 300})
		if err != nil {
			t.Fatalf("%s 加入房间失败: %v", p, err)
		}
		if !joinReply.Success {
			t.Fatalf("%s 加入房间应成功: %s", p, joinReply.Reason)
		}
	}

	// 2. 房间内聊天：Player → Room → 同房间广播
	chatReply, err := cluster.Call(ctx, n.router, attacker, &player.PlayerRoomChat{Content: "Hello room!"})
	if err != nil {
		t.Fatalf("Player 房间聊天失败: %v", err)
	}
	if !chatReply.Success {
		t.Errorf("Player 房间聊天应成功: %s", chatReply.Reason)
	}
	time.Sleep(30 * time.Millisecond)
	roomInfo, err := cluster.Call(ctx, n.router, r1, &room.RoomInfo{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	if len(roomInfo.ChatLog) != 1 {
		t.Errorf("聊天记录数 期望=1, 实际=%d", len(roomInfo.ChatLog))
	} else if roomInfo.ChatLog[0].Content != "Hello room!" {
		t.Errorf("聊天内容 期望='Hello room!', 实际='%s'", roomInfo.ChatLog[0].Content)
	}

	// 3. 房间内战斗：Player → Player(post) → 被攻击方校验同处一室后扣血 → Room 广播记录
	battleReply, err := cluster.Call(ctx, n.router, attacker, &player.ControlAttack{TargetOpenId: "cross_chat_target"})
	if err != nil {
		t.Fatalf("Player 房间战斗失败: %v", err)
	}
	if !battleReply.Success {
		t.Fatalf("Player 房间战斗应成功: %s", battleReply.Reason)
	}
	time.Sleep(50 * time.Millisecond)
	roomInfo2, err := cluster.Call(ctx, n.router, r1, &room.RoomInfo{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	if len(roomInfo2.BattleLog) != 1 {
		t.Errorf("战斗记录数 期望=1, 实际=%d", len(roomInfo2.BattleLog))
	}

	// 4. 验证被攻击者血量下降
	targetStatus, err := cluster.Call(ctx, n.router, target, &player.PlayerStatusReq{})
	if err != nil {
		t.Fatalf("查询被攻击者状态失败: %v", err)
	}
	if targetStatus.HP >= 1000 {
		t.Errorf("被攻击者 HP 应下降, 实际=%d", targetStatus.HP)
	}

	// 6. 攻击方应在收到 PlayerCombatResult 回传后获得金币/经验（异步结算）
	time.Sleep(50 * time.Millisecond)
	attackerStatus, err := cluster.Call(ctx, n.router, attacker, &player.PlayerStatusReq{})
	if err != nil {
		t.Fatalf("查询攻击方状态失败: %v", err)
	}
	if attackerStatus.Gold <= 100 {
		t.Errorf("攻击方战斗后应获得金币 (>100), 实际=%d", attackerStatus.Gold)
	}
	if attackerStatus.Attr.Exp <= 0 {
		t.Errorf("攻击方战斗后应获得经验 (>0), 实际=%d", attackerStatus.Attr.Exp)
	}

	// 5. 离开房间：Player → Room，Room 移除成员并广播
	leaveReply, err := cluster.Call(ctx, n.router, target, &player.ControlLeaveRoom{})
	if err != nil {
		t.Fatalf("Player 离开房间失败: %v", err)
	}
	if !leaveReply.Success {
		t.Fatalf("Player 离开房间应成功: %s", leaveReply.Reason)
	}
	time.Sleep(50 * time.Millisecond)
	roomInfo3, err := cluster.Call(ctx, n.router, r1, &room.RoomInfo{})
	if err != nil {
		t.Fatalf("查询房间失败: %v", err)
	}
	if len(roomInfo3.Players) != 1 {
		t.Errorf("离开后房间人数 期望=1, 实际=%d", len(roomInfo3.Players))
	}
	targetLeftStatus, err := cluster.Call(ctx, n.router, target, &player.PlayerStatusReq{})
	if err != nil {
		t.Fatalf("查询被攻击者状态失败: %v", err)
	}
	if targetLeftStatus.CurrentRoom != 0 {
		t.Errorf("离开后 CurrentRoom 应为 0, 实际=%d", targetLeftStatus.CurrentRoom)
	}
}

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

	p1 := pid("bench_atk")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p1, &player.Login{InitHP: 100000, InitLevel: 5})
	p2 := pid("bench_def")
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, p2, &player.Login{InitHP: 100000, InitLevel: 5})
	r1 := room.RoomId{RoomId: 999}
	cluster.Post[json.RawMessage, setup.JsonC, setup.JsonT](n.router, r1, &room.CreateRoom{MaxPlayers: 10})
	time.Sleep(50 * time.Millisecond)
	for _, p := range []types.PlayerId{p1, p2} {
		if _, err := cluster.Call(ctx, n.router, p, &player.ControlJoinRoom{RoomId: 999}); err != nil {
			b.Fatalf("加入房间失败: %v", err)
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster.Call(ctx, n.router, p1, &player.ControlAttack{TargetOpenId: "bench_def"})
	}
}

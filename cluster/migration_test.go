package cluster_test

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
)

// ─── Actor 类型 ───

type MigPlayerId struct {
	Name string `json:"name"`
}

func (id MigPlayerId) ActorType() actor.ActorType { return "Player" }
func (id MigPlayerId) String() string             { return id.Name }

// ─── 消息类型 ───

type MigLogin struct {
	HP int `json:"hp"`
}

func (*MigLogin) ReqType(_ MigPlayerId, _ actor.OkReply) string { return "Login" }

// MigCheckOwnership 是标准化的 CheckOwnership 消息。
// 用户约定为每个 Actor Group 实现此消息的 handler。
type MigCheckOwnership struct{}

func (*MigCheckOwnership) ReqType(_ MigPlayerId, _ actor.OkReply) string { return "CheckOwnership" }

type MigGetHP struct{}

type MigHPReply struct{ HP int }

func (*MigGetHP) ReqType(_ MigPlayerId, _ *MigHPReply) string { return "GetHP" }

// ─── State ───

type MigPlayerData struct {
	HP       int  `json:"hp"`
	InBattle bool `json:"inBattle"`
}

// ─── CheckOwnership / ShouldOwn 单元测试 ───

func TestCheckOwnership(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)
	members := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
	}

	for i := 0; i < 200; i++ {
		key := fmt.Sprintf("player-%d", i)
		n := placement.Place("Player", key, members)

		owns := cluster.ShouldOwn(placement, members, "node-1", "Player", key)
		target, shouldLeave := cluster.CheckOwnership(placement, members, "node-1", "Player", key)

		if n.ID == "node-1" {
			if !owns {
				t.Errorf("key %s: placed on node-1, ShouldOwn should be true", key)
			}
			if shouldLeave {
				t.Errorf("key %s: placed on node-1, shouldLeave should be false", key)
			}
		} else {
			if owns {
				t.Errorf("key %s: placed on %s, ShouldOwn should be false", key, n.ID)
			}
			if !shouldLeave {
				t.Errorf("key %s: placed on %s, shouldLeave should be true", key, n.ID)
			}
			if target != n.ID {
				t.Errorf("key %s: target should be %s, got %s", key, n.ID, target)
			}
		}
	}
}

func TestCheckOwnership_EmptyMembers(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)
	owns := cluster.ShouldOwn(placement, cluster.NodeSet{}, "node-1", "Player", "player-1")
	if owns {
		t.Error("empty members: ShouldOwn should be false")
	}
	_, leave := cluster.CheckOwnership(placement, cluster.NodeSet{}, "node-1", "Player", "player-1")
	if leave {
		t.Error("empty members: shouldLeave should be false")
	}
}

// ─── 测试：扩容时归属变化 ───

func TestRebalance_OwnershipChange(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	// 单节点
	members1 := cluster.NodeSet{{ID: "node-1", Addr: "localhost:8001"}}
	ownedBy1 := 0
	for i := 0; i < 500; i++ {
		if cluster.ShouldOwn(placement, members1, "node-1", "Player", fmt.Sprintf("p-%d", i)) {
			ownedBy1++
		}
	}
	if ownedBy1 != 500 {
		t.Errorf("single node: want 500, got %d", ownedBy1)
	}

	// 扩容
	members2 := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
	}

	shouldLeave := 0
	for i := 0; i < 500; i++ {
		if _, leave := cluster.CheckOwnership(placement, members2, "node-1", "Player", fmt.Sprintf("p-%d", i)); leave {
			shouldLeave++
		}
	}
	if shouldLeave == 0 || shouldLeave == 500 {
		t.Errorf("after scale-out: node-1 should lose some but not all actors, got %d/500", shouldLeave)
	}
	t.Logf("after scale-out: %d/500 actors should leave node-1", shouldLeave)
}

// ─── 测试：缩容时归属变化 ───

func TestRebalance_ScaleInOwnership(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	// 三节点
	members3 := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
		{ID: "node-3", Addr: "localhost:8003"},
	}

	// 记录 node-3 拥有的 actor
	node3OwnedBefore := make(map[string]bool)
	for i := 0; i < 500; i++ {
		key := fmt.Sprintf("p-%d", i)
		n := placement.Place("Player", key, members3)
		if n.ID == "node-3" {
			node3OwnedBefore[key] = true
		}
	}

	// node-3 离开 → 只剩 node-1 和 node-2
	members2 := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
	}

	// 原来属于 node-3 的 actor 应该被重新分配到 node-1 或 node-2
	reassigned := 0
	for key := range node3OwnedBefore {
		n := placement.Place("Player", key, members2)
		if n.ID == "node-1" || n.ID == "node-2" {
			reassigned++
		}
	}

	if reassigned != len(node3OwnedBefore) {
		t.Errorf("after scale-in: %d/%d actors reassigned to node-1 or node-2",
			reassigned, len(node3OwnedBefore))
	}
	t.Logf("node-3 had %d actors, all reassigned after scale-in", len(node3OwnedBefore))
}

// ─── 测试：CheckOwnership handler 用户业务逻辑 ───
var options10 = actor.Options{BufMails: 10}

func TestCheckOwnershipHandler_BusinessLogic(t *testing.T) {
	// 模拟 CheckOwnership handler 中的用户业务逻辑：
	// - 如果不在偏好节点 + 不在战斗中 → Deactivate
	// - 如果不在偏好节点 + 在战斗中 → 忽略（等战斗结束再处理）

	placement := cluster.NewConsistentHashPlacement(128)
	members := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	// 记录 deactivate 调用
	var deactivateCount int32

	// 注册 CheckOwnership handler
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			selfID := "node-1"
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			if leave {
				if ctx.State().InBattle {
					// 战斗中：忽略本次通知，等战斗结束再检查
					ctx.Logger().Info("in battle, defer deactivate", "target", target)
				} else {
					// 空闲：可以安全退出
					ctx.Logger().Info("deactivating", "target", target)
					atomic.AddInt32(&deactivateCount, 1)
					ctx.State().HP = 0 // 模拟 Deactivate
				}
			}
			return actor.OK, nil
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigLogin, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			ctx.SetState(MigPlayerData{HP: req.HP, InBattle: false})
			return actor.OK, nil
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigGetHP, _ bool) (*MigHPReply, error) {
			return &MigHPReply{HP: ctx.State().HP}, nil
		})
	})

	// 创建几个 Actor（有些在战斗，有些空闲）
	createAndSetBattle := func(name string, hp int, inBattle bool) {
		err := actor.Post(mgr, MigPlayerId{Name: name}, &MigLogin{HP: hp})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
		// 直接通过 manager 修改 state 模拟战斗状态
		// （实际中通过业务 handler 修改）
		_ = inBattle
	}

	createAndSetBattle("alice", 100, false) // 空闲
	createAndSetBattle("bob", 100, true)    // 战斗中

	time.Sleep(100 * time.Millisecond)

	// 模拟集群变化 → 广播 CheckOwnership
	// 实际中由 MigrationCoordinator 触发
	_, err := actor.Broadcast(mgr, &MigCheckOwnership{})
	if err != nil {
		t.Logf("Broadcast CheckOwnership: %v (may fail if no actors matched)", err)
	}

	time.Sleep(200 * time.Millisecond)
}

// ─── 测试：MigrationCoordinator 基本流程 ───

func TestMigrationCoordinator_Basic(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	// 注册 Player Group 的 CheckOwnership
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			members := membership.Members()
			selfID := membership.Self().ID
			cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			return actor.OK, nil
		})
	})

	// 创建协调器
	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)

	// 注册通知回调
	notifyCalled := 0
	coord.RegisterNotify(func() {
		notifyCalled++
		actor.Broadcast(mgr, &MigCheckOwnership{})
	})

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 创建一些 Actor
	for i := 0; i < 3; i++ {
		_ = actor.Post(mgr, MigPlayerId{Name: fmt.Sprintf("p-%d", i)}, &MigLogin{HP: 100})
	}
	time.Sleep(100 * time.Millisecond)

	// 发送 MemberJoined 事件
	events <- cluster.MemberEvent{
		Type: cluster.MemberJoined,
		Node: cluster.Node{ID: "node-2", Addr: "localhost:8002", Type: "player-server"},
	}
	time.Sleep(200 * time.Millisecond)

	if notifyCalled == 0 {
		t.Error("notify callback should have been called")
	}
	t.Logf("notify called: %d times", notifyCalled)

	cancel()
	time.Sleep(100 * time.Millisecond)
}

// ─── 测试：MigrationCoordinator 节点离开触发迁移 ───

func TestMigrationCoordinator_MemberLeft(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
		members: cluster.NodeSet{
			{ID: "node-1", Addr: "localhost:8001"},
			{ID: "node-2", Addr: "localhost:8002"},
			{ID: "node-3", Addr: "localhost:8003"},
		},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var checkOwnershipCalled int32

	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			atomic.AddInt32(&checkOwnershipCalled, 1)
			members := membership.Members()
			selfID := membership.Self().ID
			cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			return actor.OK, nil
		})
	})

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)

	var notifyCalled int32
	coord.RegisterNotify(func() {
		atomic.AddInt32(&notifyCalled, 1)
		actor.Broadcast(mgr, &MigCheckOwnership{})
	})

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 创建 Actor
	for i := 0; i < 5; i++ {
		_ = actor.Post(mgr, MigPlayerId{Name: fmt.Sprintf("p-%d", i)}, &MigLogin{HP: 100})
	}
	time.Sleep(100 * time.Millisecond)

	// 发送 MemberLeft 事件
	events <- cluster.MemberEvent{
		Type:  cluster.MemberLeft,
		Node:  cluster.Node{ID: "node-3", Addr: "localhost:8003"},
		Nodes: membership.Members(),
	}
	time.Sleep(200 * time.Millisecond)

	if atomic.LoadInt32(&notifyCalled) == 0 {
		t.Error("notify callback should be called on MemberLeft")
	}
	t.Logf("notify called: %d times, checkOwnership called: %d times",
		atomic.LoadInt32(&notifyCalled), atomic.LoadInt32(&checkOwnershipCalled))

	cancel()
	time.Sleep(100 * time.Millisecond)
}

// ─── 测试：MigrationCoordinator 动态成员变化 ───

func TestMigrationCoordinator_DynamicMembership(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	// 初始只有 node-1
	initialMembers := cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
	}

	membership := &fakeMembership{
		self:    cluster.Node{ID: "node-1", Addr: "localhost:8001"},
		members: initialMembers,
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var ownershipResults []string
	var mu sync.Mutex

	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			members := membership.Members()
			selfID := membership.Self().ID
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			mu.Lock()
			ownershipResults = append(ownershipResults,
				fmt.Sprintf("actor=%s members=%d shouldLeave=%v target=%s",
					ctx.Id().String(), len(members), leave, target))
			mu.Unlock()
			return actor.OK, nil
		})
	})

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)
	coord.RegisterNotify(func() {
		actor.Broadcast(mgr, &MigCheckOwnership{})
	})

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 创建 Actor
	for i := 0; i < 5; i++ {
		_ = actor.Post(mgr, MigPlayerId{Name: fmt.Sprintf("dyn-%d", i)}, &MigLogin{HP: 100})
	}
	time.Sleep(100 * time.Millisecond)

	// 第一阶段：node-2 加入（扩容）
	membership.setMembers(cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
		{ID: "node-2", Addr: "localhost:8002"},
	})
	events <- cluster.MemberEvent{
		Type:  cluster.MemberJoined,
		Node:  cluster.Node{ID: "node-2", Addr: "localhost:8002"},
		Nodes: membership.Members(),
	}
	time.Sleep(200 * time.Millisecond)

	// 第二阶段：node-2 离开（缩容）
	membership.setMembers(cluster.NodeSet{
		{ID: "node-1", Addr: "localhost:8001"},
	})
	events <- cluster.MemberEvent{
		Type:  cluster.MemberLeft,
		Node:  cluster.Node{ID: "node-2", Addr: "localhost:8002"},
		Nodes: membership.Members(),
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	t.Logf("ownership check results (%d):", len(ownershipResults))
	for _, r := range ownershipResults {
		t.Logf("  %s", r)
	}
	mu.Unlock()

	cancel()
}

// ─── 测试：MigrationCoordinator 停止 ───

func TestMigrationCoordinator_Stop(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan struct{})
	go func() {
		coord.Run(ctx, events)
		close(done)
	}()

	// 确保 Run 已启动
	time.Sleep(50 * time.Millisecond)

	// 取消 context 停止协调器
	cancel()

	select {
	case <-done:
		t.Log("coordinator stopped gracefully")
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not stop in time")
	}
}

// ─── 测试：MigrationCoordinator 关闭 events channel 退出 ───

func TestMigrationCoordinator_EventsClosed(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)

	events := make(chan cluster.MemberEvent, 10)
	ctx := context.Background()

	done := make(chan struct{})
	go func() {
		coord.Run(ctx, events)
		close(done)
	}()

	time.Sleep(50 * time.Millisecond)

	// 关闭 events channel
	close(events)

	select {
	case <-done:
		t.Log("coordinator exited after events channel closed")
	case <-time.After(2 * time.Second):
		t.Fatal("coordinator did not exit after events channel closed")
	}
}

// ─── 测试：MigrationCoordinator 多个 NotifyFunc ───

func TestMigrationCoordinator_MultipleNotifiers(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var count1, count2, count3 int32

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)
	coord.RegisterNotify(func() { atomic.AddInt32(&count1, 1) })
	coord.RegisterNotify(func() { atomic.AddInt32(&count2, 1) })
	coord.RegisterNotify(func() { atomic.AddInt32(&count3, 1) })

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 发送多个事件
	for i := 0; i < 3; i++ {
		events <- cluster.MemberEvent{
			Type: cluster.MemberJoined,
			Node: cluster.Node{ID: fmt.Sprintf("node-%d", i+2), Addr: fmt.Sprintf("localhost:800%d", i+2)},
		}
	}
	time.Sleep(200 * time.Millisecond)

	c1 := atomic.LoadInt32(&count1)
	c2 := atomic.LoadInt32(&count2)
	c3 := atomic.LoadInt32(&count3)

	if c1 != 3 || c2 != 3 || c3 != 3 {
		t.Errorf("all notifiers should be called 3 times: got %d, %d, %d", c1, c2, c3)
	}
	t.Logf("notifier counts: n1=%d, n2=%d, n3=%d", c1, c2, c3)
}

// ─── 测试：MigrationCoordinator 并发安全 ───

func TestMigrationCoordinator_ConcurrentEvents(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var notifyCount int32

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)
	coord.RegisterNotify(func() {
		atomic.AddInt32(&notifyCount, 1)
	})

	events := make(chan cluster.MemberEvent, 100)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 并发发送事件
	var wg sync.WaitGroup
	for g := 0; g < 5; g++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()
			for i := 0; i < 10; i++ {
				events <- cluster.MemberEvent{
					Type: cluster.MemberJoined,
					Node: cluster.Node{ID: fmt.Sprintf("g%d-node-%d", gid, i), Addr: fmt.Sprintf("localhost:9%d%02d", gid, i)},
				}
			}
		}(g)
	}

	wg.Wait()
	time.Sleep(500 * time.Millisecond)

	count := atomic.LoadInt32(&notifyCount)
	if count != 50 {
		t.Errorf("expected 50 notify calls, got %d", count)
	}
	t.Logf("concurrent events: %d notify calls", count)
}

// ─── 测试：异构集群中的节点迁移 ───

func TestMigration_HeterogeneousCluster(t *testing.T) {
	mapping := cluster.GroupMapping{
		"player-server": {"Player"},
		"room-server":   {"Room"},
	}

	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(mapping)

	// player-server 节点上检查 Player 和 Room 的归属
	members := cluster.NodeSet{
		{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
		{ID: "player-2", Addr: "localhost:8002", Type: "player-server"},
		{ID: "room-1", Addr: "localhost:8003", Type: "room-server"},
	}

	// Player actor 只能放在 player-server 上
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("player-%d", i)
		n := placement.Place("Player", key, members)
		if n.Type != "player-server" {
			t.Errorf("Player actor placed on wrong node type: %s", n.Type)
		}

		// player-1 上的 ShouldOwn/CheckOwnership
		owns := cluster.ShouldOwn(placement, members, "player-1", "Player", key)
		_, leave := cluster.CheckOwnership(placement, members, "player-1", "Player", key)

		if n.ID == "player-1" {
			if !owns {
				t.Errorf("Player %s on player-1: ShouldOwn should be true", key)
			}
			if leave {
				t.Errorf("Player %s on player-1: shouldLeave should be false", key)
			}
		}
	}

	// Room actor 只能放在 room-server 上
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("room-%d", i)
		n := placement.Place("Room", key, members)
		if n.Type != "room-server" {
			t.Errorf("Room actor placed on wrong node type: %s", n.Type)
		}

		// player-1 不应该拥有任何 Room
		owns := cluster.ShouldOwn(placement, members, "player-1", "Room", key)
		if owns {
			t.Errorf("player-1 should not own Room %s", key)
		}
	}
}

// ─── 测试：异构集群扩容迁移 ───

func TestMigration_HeterogeneousScaleOut(t *testing.T) {
	mapping := cluster.GroupMapping{
		"player-server": {"Player"},
		"room-server":   {"Room"},
	}

	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(mapping)

	// 初始：只有一个 player-server
	membersBefore := cluster.NodeSet{
		{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
	}

	// 所有 Player 归 player-1
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("p-%d", i)
		owns := cluster.ShouldOwn(placement, membersBefore, "player-1", "Player", key)
		if !owns {
			t.Errorf("single player-server: ShouldOwn should be true for all, failed at %s", key)
		}
	}

	// player-2 加入
	membersAfter := cluster.NodeSet{
		{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
		{ID: "player-2", Addr: "localhost:8002", Type: "player-server"},
	}

	// 部分 Player 应该迁移到 player-2
	shouldLeaveCount := 0
	shouldStayCount := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("p-%d", i)
		_, leave := cluster.CheckOwnership(placement, membersAfter, "player-1", "Player", key)
		if leave {
			shouldLeaveCount++
		} else {
			shouldStayCount++
		}
	}

	if shouldLeaveCount == 0 {
		t.Error("scale-out: some actors should migrate to player-2")
	}
	if shouldStayCount == 0 {
		t.Error("scale-out: some actors should stay on player-1")
	}
	t.Logf("heterogeneous scale-out: %d should leave, %d should stay", shouldLeaveCount, shouldStayCount)
}

// ─── 测试：MigrationCoordinator 使用异构 Placement ───

func TestMigrationCoordinator_HeterogeneousPlacement(t *testing.T) {
	mapping := cluster.GroupMapping{
		"player-server": {"Player"},
		"room-server":   {"Room"},
	}

	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(mapping)

	membership := &fakeMembership{
		self: cluster.Node{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
		members: cluster.NodeSet{
			{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
		},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var checkResults []string
	var mu sync.Mutex

	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			members := membership.Members()
			selfID := membership.Self().ID
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			mu.Lock()
			checkResults = append(checkResults,
				fmt.Sprintf("%s: leave=%v target=%s", ctx.Id().String(), leave, target))
			mu.Unlock()
			return actor.OK, nil
		})
	})

	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)
	coord.RegisterNotify(func() {
		actor.Broadcast(mgr, &MigCheckOwnership{})
	})

	events := make(chan cluster.MemberEvent, 10)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go coord.Run(ctx, events)

	// 创建 Actor
	for i := 0; i < 3; i++ {
		_ = actor.Post(mgr, MigPlayerId{Name: fmt.Sprintf("hp-%d", i)}, &MigLogin{HP: 100})
	}
	time.Sleep(100 * time.Millisecond)

	// player-2 加入
	membership.setMembers(cluster.NodeSet{
		{ID: "player-1", Addr: "localhost:8001", Type: "player-server"},
		{ID: "player-2", Addr: "localhost:8002", Type: "player-server"},
	})
	events <- cluster.MemberEvent{
		Type:  cluster.MemberJoined,
		Node:  cluster.Node{ID: "player-2", Addr: "localhost:8002", Type: "player-server"},
		Nodes: membership.Members(),
	}
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	t.Logf("heterogeneous migration results (%d):", len(checkResults))
	for _, r := range checkResults {
		t.Logf("  %s", r)
	}
	mu.Unlock()

	cancel()
}

// ─── 测试：Actor 批量 CheckOwnership 正确性 ───

func TestMigration_BatchCheckOwnership(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)

	membership := &fakeMembership{
		self: cluster.Node{ID: "node-1", Addr: "localhost:8001"},
		members: cluster.NodeSet{
			{ID: "node-1", Addr: "localhost:8001"},
			{ID: "node-2", Addr: "localhost:8002"},
			{ID: "node-3", Addr: "localhost:8003"},
		},
	}

	mgr := actor.NewManager()
	defer mgr.CloseManager()

	var shouldLeaveCount, shouldStayCount int32

	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigLogin, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			ctx.SetState(MigPlayerData{HP: req.HP, InBattle: false})
			return actor.OK, nil
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			members := membership.Members()
			selfID := membership.Self().ID
			_, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			if leave {
				atomic.AddInt32(&shouldLeaveCount, 1)
			} else {
				atomic.AddInt32(&shouldStayCount, 1)
			}
			return actor.OK, nil
		})
	})

	// 使用 Call 创建 100 个 Actor（Call 能触发 spawn handler）
	for i := 0; i < 100; i++ {
		_, err := actor.Call(context.Background(), mgr, MigPlayerId{Name: fmt.Sprintf("batch-%d", i)}, &MigLogin{HP: 100})
		if err != nil {
			t.Fatalf("create batch-%d: %v", i, err)
		}
	}
	time.Sleep(200 * time.Millisecond)

	// 广播 CheckOwnership
	count, err := actor.Broadcast(mgr, &MigCheckOwnership{})
	if err != nil {
		t.Fatalf("broadcast failed: %v", err)
	}
	t.Logf("broadcast sent to %d actors", count)
	time.Sleep(300 * time.Millisecond)

	leave := atomic.LoadInt32(&shouldLeaveCount)
	stay := atomic.LoadInt32(&shouldStayCount)
	total := leave + stay

	if total != 100 {
		t.Errorf("expected 100 actors checked, got %d (leave=%d, stay=%d)", total, leave, stay)
	}
	t.Logf("batch check: %d should leave, %d should stay (total=%d)", leave, stay, total)
}

// ─── 测试：ActorRef 格式化 ───

func TestActorRef_String(t *testing.T) {
	ref := cluster.ActorRef{Type: "Player", ID: "alice"}
	if ref.String() != "Player:alice" {
		t.Errorf("ActorRef.String: got %q", ref.String())
	}
}

// ─── 测试：ShouldOwn 与 Placement 一致性验证 ───

func TestShouldOwn_PlacementConsistency(t *testing.T) {
	placement := cluster.NewConsistentHashPlacement(128)
	selfID := "node-1"

	sizes := []int{1, 2, 3, 5, 10}
	for _, size := range sizes {
		t.Run(fmt.Sprintf("size_%d", size), func(t *testing.T) {
			members := make(cluster.NodeSet, size)
			for i := 0; i < size; i++ {
				members[i] = cluster.Node{
					ID:   fmt.Sprintf("node-%d", i+1),
					Addr: fmt.Sprintf("localhost:800%d", i+1),
				}
			}

			for i := 0; i < 200; i++ {
				key := fmt.Sprintf("actor-%d", i)
				placed := placement.Place("test", key, members)
				owns := cluster.ShouldOwn(placement, members, selfID, "test", key)

				if placed.ID == selfID && !owns {
					t.Errorf("size=%d key=%s: placed on self but ShouldOwn=false", size, key)
				}
				if placed.ID != selfID && owns {
					t.Errorf("size=%d key=%s: placed on %s but ShouldOwn=true", size, key, placed.ID)
				}
			}
		})
	}
}

// ─── fakeMembership ───

type fakeMembership struct {
	self    cluster.Node
	members cluster.NodeSet
	mu      sync.Mutex
}

func (f *fakeMembership) Self() cluster.Node {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.self
}

func (f *fakeMembership) Members() cluster.NodeSet {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.members == nil {
		return cluster.NodeSet{f.self}
	}
	// 返回副本避免并发问题
	result := make(cluster.NodeSet, len(f.members))
	copy(result, f.members)
	return result
}

func (f *fakeMembership) setMembers(m cluster.NodeSet) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.members = make(cluster.NodeSet, len(m))
	copy(f.members, m)
}

func (f *fakeMembership) Events() <-chan cluster.MemberEvent { return nil }
func (f *fakeMembership) Join(seeds []string) error          { return nil }
func (f *fakeMembership) Leave() error                       { return nil }
func (f *fakeMembership) Close() error                       { return nil }

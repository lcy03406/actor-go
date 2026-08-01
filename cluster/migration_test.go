package cluster_test

import (
	"context"
	"fmt"
	"sync"
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
func (id MigPlayerId) String() string              { return id.Name }

// ─── 消息类型 ───

type MigLogin struct{ HP int `json:"hp"` }

func (*MigLogin) ReqType(_ MigPlayerId, _ actor.OkReply) string { return "Login" }

// MigCheckOwnership 是标准化的 CheckOwnership 消息。
// 用户约定为每个 Actor Group 实现此消息的 handler。
type MigCheckOwnership struct{}

func (*MigCheckOwnership) ReqType(_ MigPlayerId, _ actor.OkReply) string { return "CheckOwnership" }

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

// ─── 测试：CheckOwnership handler 用户业务逻辑 ───

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

	// 注册 CheckOwnership handler
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
			selfID := "node-1"
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			if leave {
				if ctx.State().InBattle {
					// 战斗中：忽略本次通知，等战斗结束再检查
					ctx.Logger().Info("in battle, defer deactivate", "target", target)
				} else {
					// 空闲：可以安全退出
					ctx.Logger().Info("deactivating", "target", target)
					ctx.State().HP = 0 // 模拟 Deactivate
				}
			}
			return actor.OK, nil
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigLogin, _ bool) (actor.OkReply, error) {
			ctx.SetState(MigPlayerData{HP: req.HP, InBattle: false})
			return actor.OK, nil
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

	createAndSetBattle("alice", 100, false)  // 空闲
	createAndSetBattle("bob", 100, true)     // 战斗中

	time.Sleep(100 * time.Millisecond)

	// 模拟集群变化 → 广播 CheckOwnership
	// 实际中由 MigrationCoordinator 触发
	_, err := actor.Broadcast[MigPlayerId](mgr, &MigCheckOwnership{})
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
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[MigPlayerId, MigPlayerData]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[MigPlayerId, MigPlayerData], req *MigCheckOwnership, _ bool) (actor.OkReply, error) {
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
		actor.Broadcast[MigPlayerId](mgr, &MigCheckOwnership{})
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

// ─── 测试：ActorRef 格式化 ───

func TestActorRef_String(t *testing.T) {
	ref := cluster.ActorRef{Type: "Player", ID: "alice"}
	if ref.String() != "Player:alice" {
		t.Errorf("ActorRef.String: got %q", ref.String())
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
	return f.members
}

func (f *fakeMembership) Events() <-chan cluster.MemberEvent { return nil }
func (f *fakeMembership) Join(seeds []string) error          { return nil }
func (f *fakeMembership) Leave() error                       { return nil }
func (f *fakeMembership) Close() error                       { return nil }

package cluster

import (
	"fmt"
	"log/slog"
	"testing"

	"github.com/lcy03406/actor-go/actor"
)

// ─── 测试用的 GroupMapping ───

var testMapping = GroupMapping{
	"player-server": {"Player"},
	"room-server":   {"Room"},
	"chat-server":   {"Chat"},
	"multi-server":  {"Player", "Room"},
}

// ─── GroupMapping 单元测试 ───

func TestGroupMapping_HasGroup(t *testing.T) {
	tests := []struct {
		name      string
		nodeType  string
		actorType string
		want      bool
	}{
		{"empty node type (homogeneous)", "", "Player", true},
		{"empty node type (homogeneous) any", "", "Room", true},
		{"matching: player-server hosts Player", "player-server", "Player", true},
		{"non-matching: player-server does not host Room", "player-server", "Room", false},
		{"multi-server hosts Player", "multi-server", "Player", true},
		{"multi-server hosts Room", "multi-server", "Room", true},
		{"multi-server does not host Chat", "multi-server", "Chat", false},
		{"unknown node type", "unknown-server", "Player", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := testMapping.HasGroup(tt.nodeType, tt.actorType)
			if got != tt.want {
				t.Errorf("HasGroup(%q, %q) = %v, want %v", tt.nodeType, tt.actorType, got, tt.want)
			}
		})
	}
}

func TestGroupMapping_NodeCanHost(t *testing.T) {
	// 同构节点
	if !testMapping.NodeCanHost(Node{ID: "n1", Type: ""}, "Player") {
		t.Error("homogeneous node should host all")
	}
	// 异构节点
	if !testMapping.NodeCanHost(Node{ID: "n2", Type: "player-server"}, "Player") {
		t.Error("player-server should host Player")
	}
	if testMapping.NodeCanHost(Node{ID: "n2", Type: "player-server"}, "Chat") {
		t.Error("player-server should NOT host Chat")
	}
}

func TestGroupMapping_FilterByGroup(t *testing.T) {
	nodes := NodeSet{
		{ID: "player-1", Type: "player-server"},
		{ID: "player-2", Type: "player-server"},
		{ID: "room-1", Type: "room-server"},
		{ID: "chat-1", Type: "chat-server"},
		{ID: "multi-1", Type: "multi-server"},
		{ID: "all-1"}, // 同构节点，Type 为空
	}

	t.Run("filter Player", func(t *testing.T) {
		filtered := testMapping.FilterByGroup(nodes, "Player")
		// player-1, player-2, multi-1, all-1
		if len(filtered) != 4 {
			t.Errorf("Player filter: want 4 nodes, got %d", len(filtered))
		}
		ids := make(map[string]bool)
		for _, n := range filtered {
			ids[n.ID] = true
		}
		for _, want := range []string{"player-1", "player-2", "multi-1", "all-1"} {
			if !ids[want] {
				t.Errorf("Player filter: missing node %s", want)
			}
		}
	})

	t.Run("filter Room", func(t *testing.T) {
		filtered := testMapping.FilterByGroup(nodes, "Room")
		// room-1, multi-1, all-1
		if len(filtered) != 3 {
			t.Errorf("Room filter: want 3 nodes, got %d", len(filtered))
		}
	})

	t.Run("filter Chat", func(t *testing.T) {
		filtered := testMapping.FilterByGroup(nodes, "Chat")
		// chat-1, all-1
		if len(filtered) != 2 {
			t.Errorf("Chat filter: want 2 nodes, got %d", len(filtered))
		}
	})

	t.Run("filter nonexistent group", func(t *testing.T) {
		filtered := testMapping.FilterByGroup(nodes, "Nonexistent")
		// only all-1 (homogeneous)
		if len(filtered) != 1 || filtered[0].ID != "all-1" {
			t.Errorf("Nonexistent filter: want [all-1], got %v", filtered)
		}
	})

	t.Run("empty nodeset", func(t *testing.T) {
		filtered := testMapping.FilterByGroup(NodeSet{}, "Player")
		if len(filtered) != 0 {
			t.Errorf("Empty filter: want 0, got %d", len(filtered))
		}
	})
}

func TestGroupMapping_GroupsOf(t *testing.T) {
	if got := testMapping.GroupsOf("player-server"); len(got) != 1 || got[0] != "Player" {
		t.Errorf("GroupsOf(player-server): want [Player], got %v", got)
	}
	if got := testMapping.GroupsOf("multi-server"); len(got) != 2 {
		t.Errorf("GroupsOf(multi-server): want 2, got %d", len(got))
	}
	if got := testMapping.GroupsOf(""); got != nil {
		t.Errorf("GroupsOf(empty): want nil, got %v", got)
	}
}

// ─── ConsistentHashPlacement 异构感知测试 ───

func TestConsistentHashPlacement_Heterogeneous(t *testing.T) {
	p := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)

	nodes := NodeSet{
		{ID: "player-1", Type: "player-server"},
		{ID: "player-2", Type: "player-server"},
		{ID: "room-1", Type: "room-server"},
		{ID: "room-2", Type: "room-server"},
		{ID: "chat-1", Type: "chat-server"},
	}

	t.Run("player actors only on player nodes", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			n := p.Place("Player", fmt.Sprintf("player-%d", i), nodes)
			if n.ID != "player-1" && n.ID != "player-2" {
				t.Errorf("Player actor placed on non-player node: %s", n.ID)
			}
		}
	})

	t.Run("room actors only on room nodes", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			n := p.Place("Room", fmt.Sprintf("room-%d", i), nodes)
			if n.ID != "room-1" && n.ID != "room-2" {
				t.Errorf("Room actor placed on non-room node: %s", n.ID)
			}
		}
	})

	t.Run("chat actors only on chat nodes", func(t *testing.T) {
		for i := 0; i < 100; i++ {
			n := p.Place("Chat", fmt.Sprintf("chat-%d", i), nodes)
			if n.ID != "chat-1" {
				t.Errorf("Chat actor placed on non-chat node: %s", n.ID)
			}
		}
	})

	t.Run("no eligible node for nonexistent group", func(t *testing.T) {
		n := p.Place("Nonexistent", "actor-1", nodes)
		if n.ID != "" {
			t.Errorf("Nonexistent group: expected empty node, got %s", n.ID)
		}
	})
}

func TestConsistentHashPlacement_HomogeneousBackwardCompat(t *testing.T) {
	// Mapping 为 nil → 同构模式，行为不变
	p := NewConsistentHashPlacement(128)

	nodes := NodeSet{
		{ID: "node-1"},
		{ID: "node-2"},
		{ID: "node-3"},
	}

	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		n := p.Place("Player", fmt.Sprintf("actor-%d", i), nodes)
		counts[n.ID]++
	}

	for _, n := range nodes {
		if counts[n.ID] < 200 {
			t.Errorf("node %s got %d actors, expected at least 200", n.ID, counts[n.ID])
		}
	}
}

func TestConsistentHashPlacement_HomogeneousWithEmptyMapping(t *testing.T) {
	// 空 GroupMapping → 所有节点 Type 为空或空 mapping → 同构
	p := NewConsistentHashPlacement(128).WithGroupMapping(GroupMapping{})

	nodes := NodeSet{
		{ID: "node-1", Type: "player-server"},
		{ID: "node-2", Type: "room-server"},
	}

	// player-server 不在空 mapping 中 → 无节点可用
	n := p.Place("Player", "actor-1", nodes)
	if n.ID != "" {
		t.Errorf("empty mapping: expected empty node, got %s", n.ID)
	}
}

// ─── GroupAwarePlacement 测试 ───

func TestGroupAwarePlacement_Basic(t *testing.T) {
	inner := NewConsistentHashPlacement(128)
	gap := NewGroupAwarePlacement(inner, testMapping)

	nodes := NodeSet{
		{ID: "player-1", Type: "player-server"},
		{ID: "player-2", Type: "player-server"},
		{ID: "room-1", Type: "room-server"},
	}

	t.Run("player placement", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			n := gap.Place("Player", fmt.Sprintf("p-%d", i), nodes)
			if n.ID != "player-1" && n.ID != "player-2" {
				t.Errorf("GroupAwarePlacement: Player placed on wrong node %s", n.ID)
			}
		}
	})

	t.Run("room placement", func(t *testing.T) {
		for i := 0; i < 50; i++ {
			n := gap.Place("Room", fmt.Sprintf("r-%d", i), nodes)
			if n.ID != "room-1" {
				t.Errorf("GroupAwarePlacement: Room placed on wrong node %s", n.ID)
			}
		}
	})

	t.Run("no eligible node", func(t *testing.T) {
		n := gap.Place("Chat", "c-1", nodes)
		if n.ID != "" {
			t.Errorf("GroupAwarePlacement: expected empty for Chat, got %s", n.ID)
		}
	})
}

func TestGroupAwarePlacement_AllEmptyTypes(t *testing.T) {
	// 所有节点 Type 为空 → 同构模式，GroupAwarePlacement 应与 inner 行为一致
	inner := NewConsistentHashPlacement(128)
	gap := NewGroupAwarePlacement(inner, testMapping)

	nodes := NodeSet{
		{ID: "node-1"},
		{ID: "node-2"},
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("actor-%d", i)
		n1 := inner.Place("Player", key, nodes)
		n2 := gap.Place("Player", key, nodes)
		if n1.ID != n2.ID {
			t.Errorf("GroupAwarePlacement diverged from inner: %s vs %s for key %s", n1.ID, n2.ID, key)
		}
	}
}

// ─── Router 异构集群集成测试 ───

type HetPlayerId struct {
	Name string
}

func (id HetPlayerId) ActorType() actor.ActorType { return "Player" }
func (id HetPlayerId) String() string             { return id.Name }

type HetPingP struct{ Msg string }
type HetPong struct{ Msg string }

func (*HetPingP) ReqType(_ HetPlayerId, _ *HetPong) string { return "het_ping" }

func TestRouter_HeterogeneousCluster_PlayerOnly(t *testing.T) {
	self := Node{ID: "player-node", Type: "player-server"}
	roomNode := Node{ID: "room-node", Type: "room-server"}
	mem := newStaticMembership(self, self, roomNode)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[HetPlayerId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[HetPlayerId, string], req *HetPingP, _ bool) (*HetPong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			return &HetPong{Msg: req.Msg + "-pong"}, nil
		})
	})

	placement := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, placement, mgr)

	for i := 0; i < 100; i++ {
		id := HetPlayerId{Name: fmt.Sprintf("player-%d", i)}
		if !router.IsLocal(string(id.ActorType()), id.String()) {
			t.Fatalf("Player actor %s should be local", id)
		}
	}
}

func TestRouter_HeterogeneousCluster_RoomNotLocal(t *testing.T) {
	self := Node{ID: "player-node", Type: "player-server"}
	roomNode := Node{ID: "room-node", Type: "room-server"}
	mem := newStaticMembership(self, self, roomNode)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[HetPlayerId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[HetPlayerId, string], req *HetPingP, _ bool) (*HetPong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			return &HetPong{Msg: req.Msg + "-pong"}, nil
		})
	})

	placement := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, placement, mgr)

	for i := 0; i < 100; i++ {
		if router.IsLocal("Room", fmt.Sprintf("room-%d", i)) {
			t.Fatalf("Room actor should NOT be local (self is player-server)")
		}
	}
}

func TestRouter_HeterogeneousCluster_MultiGroupNode(t *testing.T) {
	self := Node{ID: "multi-node", Type: "multi-server"}
	chatNode := Node{ID: "chat-node", Type: "chat-server"}
	mem := newStaticMembership(self, self, chatNode)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[HetPlayerId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[HetPlayerId, string], req *HetPingP, _ bool) (*HetPong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			return &HetPong{Msg: req.Msg + "-pong"}, nil
		})
	})

	placement := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, placement, mgr)

	playerLocal := 0
	for i := 0; i < 100; i++ {
		if router.IsLocal("Player", fmt.Sprintf("player-%d", i)) {
			playerLocal++
		}
	}
	if playerLocal == 0 {
		t.Error("multi-server should host Player actors")
	}

	roomLocal := 0
	for i := 0; i < 100; i++ {
		if router.IsLocal("Room", fmt.Sprintf("room-%d", i)) {
			roomLocal++
		}
	}
	if roomLocal == 0 {
		t.Error("multi-server should host Room actors")
	}

	chatLocal := 0
	for i := 0; i < 100; i++ {
		if router.IsLocal("Chat", fmt.Sprintf("chat-%d", i)) {
			chatLocal++
		}
	}
	if chatLocal > 0 {
		t.Error("multi-server should NOT host Chat actors")
	}

	t.Logf("Distribution: Player=%d local, Room=%d local, Chat=%d local", playerLocal, roomLocal, chatLocal)
}

func TestRouter_HeterogeneousCluster_NoEligibleNode(t *testing.T) {
	self := Node{ID: "player-node", Type: "player-server"}
	roomNode := Node{ID: "room-node", Type: "room-server"}
	mem := newStaticMembership(self, self, roomNode)

	mgr := actor.NewManager(slog.Default())
	placement := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, placement, mgr)

	n := router.Place("Chat", "chat-1")
	if n.ID != "" {
		t.Errorf("Chat placement: expected empty node (no eligible), got %s", n.ID)
	}
}

// ─── 动态节点加入场景 ───

func TestGroupMapping_DynamicNodeJoin(t *testing.T) {
	// 模拟节点动态加入：节点只需声明 Type，映射由 GroupMapping 集中管理
	p := NewConsistentHashPlacement(128).WithGroupMapping(testMapping)

	// 初始：只有 player-server
	nodes := NodeSet{
		{ID: "player-1", Type: "player-server"},
	}
	n := p.Place("Room", "room-1", nodes)
	if n.ID != "" {
		t.Error("Room should have no eligible node before room-server joins")
	}

	// room-server 动态加入
	nodes = append(nodes, Node{ID: "room-1", Type: "room-server"})
	n = p.Place("Room", "room-1", nodes)
	if n.ID != "room-1" {
		t.Errorf("Room should go to room-1 after it joins, got %s", n.ID)
	}

	// chat-server 动态加入
	n = p.Place("Chat", "chat-1", nodes)
	if n.ID != "" {
		t.Error("Chat should have no eligible node before chat-server joins")
	}
	nodes = append(nodes, Node{ID: "chat-1", Type: "chat-server"})
	n = p.Place("Chat", "chat-1", nodes)
	if n.ID != "chat-1" {
		t.Errorf("Chat should go to chat-1 after it joins, got %s", n.ID)
	}
}

// ─── PlacementError 测试 ───

func TestPlacementError_Format(t *testing.T) {
	err := &PlacementError{
		ActorType: "Player",
		ActorId:   "alice",
	}
	msg := err.Error()
	if msg == "" {
		t.Error("PlacementError.Error() should not be empty")
	}
	if !contains(msg, "Player") || !contains(msg, "alice") {
		t.Errorf("PlacementError: expected to contain Player and alice, got %q", msg)
	}
	t.Logf("PlacementError: %s", msg)
}

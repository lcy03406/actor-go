package cluster

import (
	"testing"
)

func TestNodeSet_Lookup(t *testing.T) {
	ns := NodeSet{
		{ID: "node-1", Addr: "192.168.1.1:9000"},
		{ID: "node-2", Addr: "192.168.1.2:9000"},
		{ID: "node-3", Addr: "192.168.1.3:9000"},
	}

	n := ns.Lookup("node-2")
	if n == nil {
		t.Fatal("expected to find node-2")
	}
	if n.Addr != "192.168.1.2:9000" {
		t.Errorf("unexpected addr: %s", n.Addr)
	}

	n = ns.Lookup("node-99")
	if n != nil {
		t.Fatal("expected nil for non-existent node")
	}
}

func TestNodeSet_Contains(t *testing.T) {
	ns := NodeSet{
		{ID: "node-1", Addr: "addr-1"},
	}

	if !ns.Contains("node-1") {
		t.Fatal("expected node-1 to be in set")
	}
	if ns.Contains("node-2") {
		t.Fatal("expected node-2 to NOT be in set")
	}
}

func TestConsistentHashPlacement(t *testing.T) {
	members := NodeSet{
		{ID: "a", Addr: "addr-a"},
		{ID: "b", Addr: "addr-b"},
		{ID: "c", Addr: "addr-c"},
	}

	p := NewConsistentHashPlacement(128)

	// 确定性：相同 key 总是返回相同节点
	result1 := p.Place("test", "actor-1", members)
	result2 := p.Place("test", "actor-1", members)
	if result1.ID != result2.ID {
		t.Errorf("deterministic placement failed: %s != %s", result1.ID, result2.ID)
	}

	// 非空结果
	if result1.ID == "" {
		t.Fatal("expected non-empty result")
	}

	// 单节点
	single := NodeSet{{ID: "only", Addr: "addr-only"}}
	r := p.Place("test", "x", single)
	if r.ID != "only" {
		t.Errorf("single node: expected 'only', got '%s'", r.ID)
	}

	// 空成员
	r = p.Place("test", "x", nil)
	if r.ID != "" {
		t.Errorf("empty members: expected empty node, got '%s'", r.ID)
	}

	// 分布测试：相同类型不同 ID 分布到不同节点
	distribution := make(map[string]int)
	for i := 0; i < 1000; i++ {
		id := "actor-" + itoa(i)
		r := p.Place("type-x", id, members)
		distribution[r.ID]++
	}
	// 每个节点至少分配一些
	for _, node := range members {
		if distribution[node.ID] == 0 {
			t.Errorf("node %s received 0 actors (distribution too uneven)", node.ID)
		}
	}
}

func TestRoute(t *testing.T) {
	self := Node{ID: "local", Addr: "addr-local"}
	other := Node{ID: "remote", Addr: "addr-remote"}

	// 本地节点 → RouteLocal
	result := Route(self, self, true, true)
	if result.Decision != RouteLocal {
		t.Errorf("same node: expected RouteLocal, got %v", result.Decision)
	}

	// Serve 模式（spawn+query）→ RouteForward
	result = Route(self, other, true, true)
	if result.Decision != RouteForward {
		t.Errorf("serve to remote: expected RouteForward, got %v", result.Decision)
	}
	if result.Target.ID != "remote" {
		t.Errorf("target mismatch: %s", result.Target.ID)
	}

	// Spawn-only → RouteFail
	result = Route(self, other, true, false)
	if result.Decision != RouteFail {
		t.Errorf("spawn-only to remote: expected RouteFail, got %v", result.Decision)
	}
	if result.Err == nil {
		t.Fatal("expected RouteError for spawn-only")
	}
	if result.Err.Owner != "remote" {
		t.Errorf("unexpected owner: %s", result.Err.Owner)
	}

	// Query-only → RouteFail
	result = Route(self, other, false, true)
	if result.Decision != RouteFail {
		t.Errorf("query-only to remote: expected RouteFail, got %v", result.Decision)
	}
	if result.Err == nil {
		t.Fatal("expected RouteError for query-only")
	}
}

func TestIsRouteError(t *testing.T) {
	result := Route(Node{ID: "local"}, Node{ID: "remote"}, true, false)
	if result.Decision != RouteFail {
		t.Skip("unexpected decision")
	}

	re, ok := IsRouteError(result.Err)
	if !ok {
		t.Fatal("expected IsRouteError to return true")
	}
	if re.Owner != "remote" {
		t.Errorf("unexpected owner: %s", re.Owner)
	}
	if !re.AllowSpawn || re.AllowQuery {
		t.Error("unexpected AllowSpawn/AllowQuery flags")
	}
}

func TestRouteError_Error(t *testing.T) {
	e := &RouteError{
		ActorType:  "Player",
		ActorId:    "123",
		Owner:      "node-3",
		AllowSpawn: true,
		AllowQuery: false,
	}
	msg := e.Error()
	if msg == "" {
		t.Fatal("expected non-empty error message")
	}
	if !contains(msg, "node-3") {
		t.Errorf("error message should mention owner node: %s", msg)
	}
}

func TestMembersToNodeSet(t *testing.T) {
	members := []Node{
		{ID: "c"},
		{ID: "a"},
		{ID: "b"},
	}
	ns := MembersToNodeSet(members)
	if len(ns) != 3 {
		t.Fatalf("expected 3 nodes, got %d", len(ns))
	}
	// 应排序
	if ns[0].ID != "a" || ns[1].ID != "b" || ns[2].ID != "c" {
		t.Errorf("unexpected order: %v", ns)
	}
}

func TestMemberDiff(t *testing.T) {
	old := NodeSet{{ID: "a"}, {ID: "b"}, {ID: "c"}}
	new := NodeSet{{ID: "b"}, {ID: "c"}, {ID: "d"}}

	joined, left := MemberDiff(old, new)
	if len(joined) != 1 || joined[0].ID != "d" {
		t.Errorf("expected 'd' to join, got %v", joined)
	}
	if len(left) != 1 || left[0].ID != "a" {
		t.Errorf("expected 'a' to leave, got %v", left)
	}
}

func TestStaticMembership(t *testing.T) {
	nodes := NodeSet{
		{ID: "node-1", Addr: "addr-1"},
		{ID: "node-2", Addr: "addr-2"},
	}
	m := NewStaticMembership("node-1", nodes)

	if m.Self().ID != "node-1" {
		t.Errorf("expected self node-1, got %s", m.Self().ID)
	}

	members := m.Members()
	if len(members) != 2 {
		t.Errorf("expected 2 members, got %d", len(members))
	}

	// Join/Leave 是 no-op
	if err := m.Join([]string{"seed"}); err != nil {
		t.Error("Join should not error")
	}
	if err := m.Leave(); err != nil {
		t.Error("Leave should not error")
	}

	// Events channel 应该存在
	ch := m.Events()
	if ch == nil {
		t.Fatal("expected non-nil events channel")
	}

	m.Close()
}

func TestCluster_Resolve(t *testing.T) {
	nodes := NodeSet{
		{ID: "node-1", Addr: "addr-1"},
		{ID: "node-2", Addr: "addr-2"},
	}
	membership := NewStaticMembership("node-1", nodes)
	placement := NewConsistentHashPlacement(128)
	c := New(membership, placement)

	if c.Self().ID != "node-1" {
		t.Errorf("unexpected self: %s", c.Self().ID)
	}

	// 本地或转发取决于哈希结果
	result := c.Resolve("player", "123", true, true)
	if result.Decision != RouteLocal && result.Decision != RouteForward {
		t.Errorf("expected RouteLocal or RouteForward for serve, got %v", result.Decision)
	}

	// Spawn-only 在非本地节点 → RouteFail
	result = c.Resolve("player", "123", true, false)
	if result.Decision == RouteFail {
		// 这是一个合法的结果（如果哈希落到远程节点）
		if result.Err == nil {
			t.Error("expected RouteError when RouteFail")
		}
	}

	c.Close()
}

func TestItoa(t *testing.T) {
	tests := []struct {
		n    int
		want string
	}{
		{0, "0"},
		{1, "1"},
		{10, "10"},
		{128, "128"},
		{9999, "9999"},
	}
	for _, tt := range tests {
		if got := itoa(tt.n); got != tt.want {
			t.Errorf("itoa(%d) = %s, want %s", tt.n, got, tt.want)
		}
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchSubstr(s, substr)
}

func searchSubstr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}

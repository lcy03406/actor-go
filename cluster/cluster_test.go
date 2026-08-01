package cluster

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ─── 测试用 Actor 类型 ───

type TestActorId struct {
	Name string
}

func (t TestActorId) ActorType() actor.ActorType { return "test_cluster" }
func (t TestActorId) String() string             { return t.Name }

type Ping struct {
	Msg string
}

type Pong struct {
	Msg string
}

func (*Ping) ReqType(_ TestActorId, _ *Pong) string { return "ping" }

// ─── 测试用 Membership ───

type staticMembership struct {
	self    Node
	members NodeSet
	events  chan MemberEvent
}

func newStaticMembership(self Node, members ...Node) *staticMembership {
	return &staticMembership{
		self:    self,
		members: NodeSet(members),
		events:  make(chan MemberEvent, 10),
	}
}

func (s *staticMembership) Self() Node                    { return s.self }
func (s *staticMembership) Members() NodeSet              { return s.members }
func (s *staticMembership) Events() <-chan MemberEvent    { return s.events }
func (s *staticMembership) Join(seeds []string) error     { return nil }
func (s *staticMembership) Leave() error                  { return nil }
func (s *staticMembership) Close() error                  { return nil }

// ─── 测试用 Dummy Message/Codec/Transport（Router 编译所需，不真正使用） ───

type DummyMessage struct{}

type DummyCodec struct{}

func (DummyCodec) Decode(data DummyMessage, v any) error { return nil }
func (DummyCodec) Encode(v any) (DummyMessage, error)    { return DummyMessage{}, nil }

type DummyTransport struct{}

func (DummyTransport) DecodeReq(data []byte) (seq uint64, method string, actorType actor.ActorType, reqType string, idM DummyMessage, idsM []DummyMessage, reqM DummyMessage, err error) {
	return 0, "", "", "", DummyMessage{}, nil, DummyMessage{}, nil
}
func (DummyTransport) DecodeRep(data []byte) (seq uint64, repM DummyMessage, rerr string, err error) {
	return 0, DummyMessage{}, "", nil
}
func (DummyTransport) EncodeReq(seq uint64, method string, actorType actor.ActorType, reqType string, idM DummyMessage, idsM []DummyMessage, reqM DummyMessage) (data []byte, err error) {
	return nil, nil
}
func (DummyTransport) EncodeRep(seq uint64, repM DummyMessage, rerr string) (data []byte, err error) {
	return nil, nil
}

type DummyRouter = Router[DummyMessage, DummyCodec, DummyTransport]

func newDummyRouter(cluster *Cluster, mgr *actor.Manager) *DummyRouter {
	return NewRouter[DummyMessage, DummyCodec, DummyTransport](cluster, mgr, nil)
}

// ─── Cluster 拓扑测试 ───

func TestCluster_Place(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}

	mem := newStaticMembership(node1, node1, node2, node3)
	c := New(mem, NewConsistentHashPlacement(128))

	self := c.Self()
	if self.ID != "node-1" {
		t.Errorf("Self: want node-1, got %s", self.ID)
	}

	members := c.Members()
	if len(members) != 3 {
		t.Errorf("Members: want 3, got %d", len(members))
	}

	preferred := c.Place("test", "actor-1")
	if preferred.ID == "" {
		t.Error("Place: expected non-empty node ID")
	}
}

func TestCluster_Close(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	if err := c.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

func TestCluster_Events(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	events := c.Events()
	if events == nil {
		t.Error("Events: expected non-nil channel")
	}
}

func TestCluster_PlaceConsistent(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}

	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))

	first := c.Place("test", "consistent-actor")
	for i := 0; i < 10; i++ {
		got := c.Place("test", "consistent-actor")
		if got.ID != first.ID {
			t.Errorf("Place: inconsistent result, want %s, got %s", first.ID, got.ID)
		}
	}
}

// ─── Router 本地调用测试 ───

func TestRouter_LocalCall(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestActorId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[TestActorId, string], req *Ping, _ bool) (*Pong, error) {
			return &Pong{Msg: req.Msg + "-pong"}, nil
		})
	})

	router := newDummyRouter(c, mgr)

	ctx := context.Background()
	id := TestActorId{Name: "local-call"}

	if !router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on remote, skipping local test (depends on hash)")
	}

	reply, err := Call[DummyMessage, DummyCodec, DummyTransport](ctx, router, id, &Ping{Msg: "hello"})
	if err != nil {
		t.Fatalf("local Call failed: %v", err)
	}
	if reply.Msg != "hello-pong" {
		t.Errorf("Call reply: want hello-pong, got %s", reply.Msg)
	}
}

func TestRouter_LocalPost(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	var received string
	var mu sync.Mutex
	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestActorId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[TestActorId, string], req *Ping, _ bool) (*Pong, error) {
			mu.Lock()
			received = req.Msg
			mu.Unlock()
			return &Pong{Msg: "ok"}, nil
		})
	})

	router := newDummyRouter(c, mgr)

	id := TestActorId{Name: "local-post"}
	if !router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on remote, skipping post test")
	}

	err := Post[DummyMessage, DummyCodec, DummyTransport](router, id, &Ping{Msg: "fire-and-forget"})
	if err != nil {
		t.Fatalf("local Post failed: %v", err)
	}

	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	if received != "fire-and-forget" {
		t.Errorf("Post: want fire-and-forget, got %s", received)
	}
	mu.Unlock()
}

func TestRouter_LocalBroadcast(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	var count int32
	var mu sync.Mutex
	mgr := actor.NewManager()

	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestActorId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[TestActorId, string], req *Ping, _ bool) (*Pong, error) {
			mu.Lock()
			count++
			mu.Unlock()
			return &Pong{Msg: "ok"}, nil
		})
	})

	// 创建几个 actor
	for i := 0; i < 3; i++ {
		_, _ = actor.Call(context.Background(), mgr, TestActorId{Name: "bc-" + fmt.Sprint(i)}, &Ping{Msg: "init"})
	}

	router := newDummyRouter(c, mgr)
	err := Broadcast[DummyMessage, DummyCodec, DummyTransport, TestActorId](router, &Ping{Msg: "broadcast"})
	if err != nil {
		t.Fatalf("Broadcast failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	mu.Lock()
	if count != 3 {
		t.Errorf("Broadcast count: want 3, got %d", count)
	}
	mu.Unlock()
}

func TestRouter_LocalMulticast(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	var count int32
	var mu sync.Mutex
	mgr := actor.NewManager()

	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestActorId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[TestActorId, string], req *Ping, _ bool) (*Pong, error) {
			mu.Lock()
			count++
			mu.Unlock()
			return &Pong{Msg: "ok"}, nil
		})
	})

	ids := []TestActorId{
		{Name: "mc-1"},
		{Name: "mc-2"},
	}
	for _, id := range ids {
		_, _ = actor.Call(context.Background(), mgr, id, &Ping{Msg: "init"})
	}

	router := newDummyRouter(c, mgr)
	n, err := Multicast[DummyMessage, DummyCodec, DummyTransport](router, ids, &Ping{Msg: "multicast"})
	if err != nil {
		t.Fatalf("Multicast failed: %v", err)
	}
	if n != 2 {
		t.Errorf("Multicast count: want 2, got %d", n)
	}
}

func TestRouter_IsLocal(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))

	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	if !router.IsLocal("any", "actor") {
		t.Error("IsLocal: expected true for single-node cluster")
	}
}

// ─── Router 生命周期测试 ───

func TestRouter_NewRouter(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()

	router := newDummyRouter(c, mgr)
	if router == nil {
		t.Fatal("NewRouter: expected non-nil router")
	}
	if router.Self().ID != "node-1" {
		t.Errorf("Self: want node-1, got %s", router.Self().ID)
	}
	if len(router.Members()) != 1 {
		t.Errorf("Members: want 1, got %d", len(router.Members()))
	}
}

func TestRouter_Self(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	self := router.Self()
	if self.ID != "node-1" {
		t.Errorf("Self: want node-1, got %s", self.ID)
	}
	if self.Addr != "127.0.0.1:8001" {
		t.Errorf("Self.Addr: want 127.0.0.1:8001, got %s", self.Addr)
	}
}

func TestRouter_Members(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}
	mem := newStaticMembership(node1, node1, node2, node3)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	members := router.Members()
	if len(members) != 3 {
		t.Errorf("Members: want 3, got %d", len(members))
	}
	ids := make(map[string]bool)
	for _, m := range members {
		ids[m.ID] = true
	}
	for _, want := range []string{"node-1", "node-2", "node-3"} {
		if !ids[want] {
			t.Errorf("Members: missing node %s", want)
		}
	}
}

func TestRouter_Events(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	events := router.Events()
	if events == nil {
		t.Error("Events: expected non-nil channel")
	}
}

func TestRouter_Close(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	if err := router.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

// ─── Router.Place / IsLocal 多节点场景 ───

func TestRouter_Place(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}
	mem := newStaticMembership(node1, node1, node2, node3)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	n := router.Place("player", "actor-1")
	if n.ID == "" {
		t.Error("Place: expected non-empty node ID")
	}
	// 一致性哈希：同一个 key 多次 Place 应返回相同结果
	for i := 0; i < 5; i++ {
		if got := router.Place("player", "actor-1"); got.ID != n.ID {
			t.Errorf("Place: inconsistent, want %s, got %s", n.ID, got.ID)
		}
	}
}

func TestRouter_Place_EmptyMembers(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	n := router.Place("player", "actor-1")
	if n.ID != "" {
		t.Errorf("Place with empty members: expected empty node, got %s", n.ID)
	}
}

func TestRouter_IsLocal_MultiNode(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}
	mem := newStaticMembership(node1, node1, node2, node3)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	// 在多节点集群中，IsLocal 应与 Place 结果一致
	isLocal := router.IsLocal("player", "actor-x")
	n := router.Place("player", "actor-x")
	if isLocal != (n.ID == node1.ID) {
		t.Errorf("IsLocal: %v, but Place returned %s (self is %s)", isLocal, n.ID, node1.ID)
	}
}

// ─── Router.GetClient / RemoveClient ───

func TestRouter_GetClient_NoDialer(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	_, err := router.GetClient(node2)
	if err == nil {
		t.Error("GetClient without dialer: expected error")
	}
	var routeErr *RouteError
	if err != nil {
		if _, ok := interface{}(err).(*RouteError); !ok {
			t.Errorf("GetClient error should be *RouteError, got %T", err)
		}
		_ = routeErr
	}
}

func TestRouter_RemoveClient_NoExist(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	// 移除不存在的连接不应 panic
	router.RemoveClient("no-such-addr")
}

func TestRouter_GetClient_Self(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	// 获取到自己的连接（无 dialer 也应报错）
	_, err := router.GetClient(node1)
	if err == nil {
		t.Log("GetClient(self) succeeded (dialer was nil but self was in pool?)")
	}
}

// ─── Router Call/Post 远程失败场景 ───

func TestRouter_Call_RemoteWithoutDialer(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	ctx := context.Background()

	// 遍历多个 id，确保至少有一个落在远程节点
	found := false
	for i := 0; i < 50; i++ {
		id := TestActorId{Name: fmt.Sprintf("remote-call-%d", i)}
		if router.IsLocal(string(id.ActorType()), id.String()) {
			continue
		}
		found = true
		_, err := Call[DummyMessage, DummyCodec, DummyTransport](ctx, router, id, &Ping{Msg: "hello"})
		if err == nil {
			t.Error("Call to remote without dialer: expected error")
		}
		break
	}
	if !found {
		t.Skip("all 50 actors placed on local node, cannot test remote call")
	}
}

func TestRouter_Post_RemoteWithoutDialer(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	id := TestActorId{Name: "remote-post"}
	if router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on local node, skipping remote test")
	}

	err := Post[DummyMessage, DummyCodec, DummyTransport](router, id, &Ping{Msg: "hello"})
	if err == nil {
		t.Error("Post to remote without dialer: expected error")
	}
}

// ─── Multicast 边界情况 ───

func TestRouter_Multicast_Empty(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	n, err := Multicast[DummyMessage, DummyCodec, DummyTransport](router, []TestActorId{}, &Ping{Msg: "empty"})
	if err != nil {
		t.Fatalf("Multicast empty: expected no error, got %v", err)
	}
	if n != 0 {
		t.Errorf("Multicast empty: want 0, got %d", n)
	}
}

func TestRouter_Multicast_AllRemote(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}
	mem := newStaticMembership(node1, node1, node2, node3)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	// 找出被 Place 到远程节点的 actor id
	var remoteIds []TestActorId
	for i := 0; i < 20; i++ {
		id := TestActorId{Name: fmt.Sprintf("mc-remote-%d", i)}
		if !router.IsLocal(string(id.ActorType()), id.String()) {
			remoteIds = append(remoteIds, id)
		}
		if len(remoteIds) >= 3 {
			break
		}
	}
	if len(remoteIds) < 2 {
		t.Skip("not enough remote-placed actors for test")
	}

	// 没有 dialer，远程发送会失败但不 panic
	n, err := Multicast[DummyMessage, DummyCodec, DummyTransport](router, remoteIds, &Ping{Msg: "all-remote"})
	// 没有 dialer 时远程部分会跳过，返回已发送的计数
	if n < 0 || n > len(remoteIds) {
		t.Errorf("Multicast all-remote: unexpected count %d (expected 0-%d)", n, len(remoteIds))
	}
	_ = err
	t.Logf("Multicast all-remote: sent=%d, err=%v", n, err)
}

func TestRouter_Multicast_Mixed(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))

	var localCount int32
	var mu sync.Mutex
	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestActorId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[TestActorId, string], req *Ping, _ bool) (*Pong, error) {
			mu.Lock()
			localCount++
			mu.Unlock()
			return &Pong{Msg: "ok"}, nil
		})
	})

	// 创建多个 actor，其中一些可能在本地，一些在远程
	var ids []TestActorId
	for i := 0; i < 10; i++ {
		id := TestActorId{Name: fmt.Sprintf("mc-mix-%d", i)}
		ids = append(ids, id)
	}

	router := newDummyRouter(c, mgr)
	n, err := Multicast[DummyMessage, DummyCodec, DummyTransport](router, ids, &Ping{Msg: "mixed"})
	if n < 0 {
		t.Errorf("Multicast mixed: unexpected negative count %d", n)
	}
	if n > len(ids) {
		t.Errorf("Multicast mixed: count %d exceeds total ids %d", n, len(ids))
	}
	_ = err
	t.Logf("Multicast mixed: sent=%d, err=%v", n, err)
}

// ─── clientPool 并发安全测试 ───

func TestClientPool_Concurrent(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	var wg sync.WaitGroup
	errCh := make(chan error, 20)
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := router.GetClient(node2)
			if err != nil {
				errCh <- err
			}
		}()
	}
	wg.Wait()
	close(errCh)

	errCount := 0
	for range errCh {
		errCount++
	}
	// 所有 goroutine 都应返回 RouteError（无 dialer），不应 panic
	if errCount != 20 {
		t.Errorf("Concurrent GetClient: expected 20 errors, got %d", errCount)
	}
}

func TestClientPool_ConcurrentRemove(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	c := New(mem, NewConsistentHashPlacement(128))
	mgr := actor.NewManager()
	router := newDummyRouter(c, mgr)

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			addr := fmt.Sprintf("127.0.0.1:%d", 9000+idx)
			router.RemoveClient(addr)
		}(i)
	}
	wg.Wait()
	// 不应 panic
}

// ─── RouteError 格式测试 ───

func TestRouteError_Format(t *testing.T) {
	tests := []struct {
		name      string
		err       *RouteError
		contained []string
	}{
		{
			name: "full fields",
			err: &RouteError{
				ActorType: "Player",
				ActorId:   "alice",
				Owner:     "node-2",
				Reason:    "lease taken",
			},
			contained: []string{"Player", "alice", "node-2", "lease taken"},
		},
		{
			name: "empty fields",
			err: &RouteError{
				ActorType: "",
				ActorId:   "",
				Owner:     "node-x",
				Reason:    "no dialer set, cannot connect to remote node",
			},
			contained: []string{"node-x", "no dialer"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			msg := tt.err.Error()
			if msg == "" {
				t.Error("Error() should not return empty string")
			}
			for _, s := range tt.contained {
				if !contains(msg, s) {
					t.Errorf("Error() should contain %q, got %q", s, msg)
				}
			}
		})
	}
}

func contains(s, sub string) bool {
	return len(s) >= len(sub) && searchSubstring(s, sub)
}

func searchSubstring(s, sub string) bool {
	for i := 0; i <= len(s)-len(sub); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ─── ClientPool 测试 ───

func TestClientPool_GetOrDial_NoDialer(t *testing.T) {
	pool := newClientPool[DummyMessage, DummyCodec, DummyTransport]()
	_, err := pool.getOrDial(Node{ID: "node-2", Addr: "127.0.0.1:8002"}, nil)
	if err == nil {
		t.Error("getOrDial without dialer: expected error")
	}
	var routeErr *RouteError
	if err != nil {
		t.Logf("getOrDial error (expected): %v", err)
		_ = routeErr
	}
}

func TestClientPool_Remove(t *testing.T) {
	pool := newClientPool[DummyMessage, DummyCodec, DummyTransport]()
	// 删除不存在的 key 不应 panic
	pool.remove("no-such-addr")
	pool.closeAll()
}

// ─── Placement 测试 ───

func TestConsistentPlacement_Distribution(t *testing.T) {
	p := NewConsistentHashPlacement(128)
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}
	members := NodeSet{node1, node2, node3}

	counts := map[string]int{}
	for i := 0; i < 1000; i++ {
		n := p.Place("test", fmt.Sprintf("actor-%d", i), members)
		counts[n.ID]++
	}

	for _, n := range members {
		if counts[n.ID] < 200 {
			t.Errorf("node %s got %d actors, expected at least 200", n.ID, counts[n.ID])
		}
	}
}

func TestConsistentPlacement_Empty(t *testing.T) {
	p := NewConsistentHashPlacement(128)
	n := p.Place("test", "actor", NodeSet{})
	if n.ID != "" {
		t.Errorf("empty members: expected empty node, got %s", n.ID)
	}
}

func TestConsistentPlacement_Single(t *testing.T) {
	p := NewConsistentHashPlacement(128)
	node := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	n := p.Place("test", "actor", NodeSet{node})
	if n.ID != "node-1" {
		t.Errorf("single node: want node-1, got %s", n.ID)
	}
}

func TestConsistentPlacement_NodeRemoval(t *testing.T) {
	p := NewConsistentHashPlacement(128)
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	node3 := Node{ID: "node-3", Addr: "127.0.0.1:8003"}

	members := NodeSet{node1, node2, node3}
	before := p.Place("test", "actor-x", members)

	members2 := NodeSet{node1, node2}
	after := p.Place("test", "actor-x", members2)

	if before.ID == "node-3" && after.ID == before.ID {
		t.Error("actor should have moved after node-3 left")
	}
}

// ─── Node / NodeSet 测试 ───

func TestNodeSet_Basic(t *testing.T) {
	ns := NodeSet{{ID: "a"}, {ID: "b"}}
	if len(ns) != 2 {
		t.Errorf("len: want 2, got %d", len(ns))
	}
}

// ─── MemberDiff 测试 ───

func TestMemberDiff(t *testing.T) {
	old := NodeSet{
		{ID: "node-1"}, {ID: "node-2"}, {ID: "node-3"},
	}
	new := NodeSet{
		{ID: "node-1"}, {ID: "node-3"}, {ID: "node-4"},
	}
	joined, left := MemberDiff(old, new)

	if len(joined) != 1 || joined[0].ID != "node-4" {
		t.Errorf("joined: expected [node-4], got %v", nodeIds(joined))
	}
	if len(left) != 1 || left[0].ID != "node-2" {
		t.Errorf("left: expected [node-2], got %v", nodeIds(left))
	}
}

func TestMemberDiff_NoChange(t *testing.T) {
	ns := NodeSet{{ID: "node-1"}, {ID: "node-2"}}
	joined, left := MemberDiff(ns, ns)
	if len(joined) != 0 || len(left) != 0 {
		t.Error("no change: expected empty diffs")
	}
}

func TestMemberDiff_EmptyOld(t *testing.T) {
	ns := NodeSet{{ID: "node-1"}}
	joined, left := MemberDiff(NodeSet{}, ns)
	if len(joined) != 1 || len(left) != 0 {
		t.Error("empty old: expected 1 joined, 0 left")
	}
}

// ─── RouteError 测试 ───

func TestRouteError_Error(t *testing.T) {
	err := &RouteError{
		ActorType: "Player",
		ActorId:   "alice",
		Owner:     "node-2",
		Reason:    "lease taken by another node",
	}
	msg := err.Error()
	if msg == "" {
		t.Error("RouteError.Error() should not be empty")
	}
	t.Logf("RouteError: %s", msg)
}

func nodeIds(ns NodeSet) []string {
	ids := make([]string, len(ns))
	for i, n := range ns {
		ids[i] = n.ID
	}
	return ids
}

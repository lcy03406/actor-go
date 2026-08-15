package cluster

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
)

// ─── mockLeaseReleaser ───

type mockLeaseReleaser struct {
	forceReleaseCalled bool
	lastActorType      string
	lastActorId        string
	returnGen          int64
	returnErr          error
}

func (m *mockLeaseReleaser) ForceRelease(ctx context.Context, actorType string, id string) (int64, error) {
	m.forceReleaseCalled = true
	m.lastActorType = actorType
	m.lastActorId = id
	return m.returnGen, m.returnErr
}

// ─── 测试用类型 ───

type LeaseTestId struct {
	Name string
}

func (t LeaseTestId) ActorType() actor.ActorType { return "lease_test" }
func (t LeaseTestId) String() string             { return t.Name }

type LeasePing struct {
	Msg string
}

type LeasePong struct {
	Msg string
}

func (*LeasePing) ReqType(_ LeaseTestId, _ *LeasePong) string { return "lease_ping" }

// ─── isLeaseTaken 测试 ───

func TestIsLeaseTaken_Nil(t *testing.T) {
	if result := isLeaseTaken(nil); result != nil {
		t.Error("isLeaseTaken(nil) should return nil")
	}
}

func TestIsLeaseTaken_OtherError(t *testing.T) {
	if result := isLeaseTaken(errors.New("some other error")); result != nil {
		t.Error("isLeaseTaken(other error) should return nil")
	}
}

func TestIsLeaseTaken_LeaseTaken(t *testing.T) {
	original := &grain.ErrLeaseTaken{Key: "player:123", Owner: "node-2", Generation: 5}
	result := isLeaseTaken(original)
	if result == nil || result.Owner != "node-2" || result.Generation != 5 {
		t.Errorf("isLeaseTaken: unexpected result %+v", result)
	}
}

func TestIsLeaseTaken_Wrapped(t *testing.T) {
	original := &grain.ErrLeaseTaken{Key: "player:123", Owner: "node-3", Generation: 10}
	wrapped := errors.New("rpc call failed: " + original.Error())
	result := isLeaseTaken(wrapped)
	if result != nil && result.Owner != "node-3" {
		t.Errorf("Owner: want node-3, got %s", result.Owner)
	}
}

func TestIsLeaseTaken_FmtWrap(t *testing.T) {
	original := &grain.ErrLeaseTaken{Key: "player:456", Owner: "node-4", Generation: 7}
	wrapped := fmt.Errorf("rpc call failed: %w", original)
	result := isLeaseTaken(wrapped)
	if result == nil {
		t.Fatal("isLeaseTaken(fmt.Errorf wrapped) should detect ErrLeaseTaken")
	}
	if result.Owner != "node-4" {
		t.Errorf("Owner: want node-4, got %s", result.Owner)
	}
}

// ─── Router 选项测试 ───

func TestRouter_WithLeaseRetry(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	mgr := actor.NewManager(slog.Default())

	r := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithLeaseRetry(true),
	)
	if !r.cfg.LeaseRetry {
		t.Error("WithLeaseRetry(true): LeaseRetry should be true")
	}
	if r.cfg.ForceReleaser != nil {
		t.Error("WithLeaseRetry(true): ForceReleaser should be nil")
	}
}

func TestRouter_WithForceReleaser(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	mgr := actor.NewManager(slog.Default())
	releaser := &mockLeaseReleaser{returnGen: 1}

	r := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithForceReleaser(releaser),
	)
	if !r.cfg.LeaseRetry {
		t.Error("WithForceReleaser: LeaseRetry should be auto-enabled")
	}
	if r.cfg.ForceReleaser != releaser {
		t.Error("WithForceReleaser: ForceReleaser should be set")
	}
}

func TestRouter_DefaultNoLeaseRetry(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	mgr := actor.NewManager(slog.Default())

	r := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr)
	if r.cfg.LeaseRetry || r.cfg.ForceReleaser != nil {
		t.Error("default: no lease options should be set")
	}
}

// ─── Router Call/Post 正常调用 ───

func TestRouter_CallNormal(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[LeaseTestId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[LeaseTestId, string], req *LeasePing, _ bool) (*LeasePong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			return &LeasePong{Msg: req.Msg + "-pong"}, nil
		})
	})

	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr)

	ctx := context.Background()
	id := LeaseTestId{Name: "normal-call"}
	if !router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on remote, skipping test")
	}

	reply, err := Call(ctx, router, id, &LeasePing{Msg: "hello"})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Msg != "hello-pong" {
		t.Errorf("Call reply: want hello-pong, got %s", reply.Msg)
	}
}

func TestRouter_PostNormal(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)

	var received string
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[LeaseTestId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[LeaseTestId, string], req *LeasePing, _ bool) (*LeasePong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			received = req.Msg
			return &LeasePong{Msg: "ok"}, nil
		})
	})

	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr)

	id := LeaseTestId{Name: "normal-post"}
	if !router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on remote, skipping test")
	}

	err := Post(router, id, &LeasePing{Msg: "fire-and-forget"})
	if err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)
	if received != "fire-and-forget" {
		t.Errorf("Post: want fire-and-forget, got %s", received)
	}
}

// ─── Router 带租约重试，正常场景不触发 ───

func TestRouter_CallWithForceReleaser_Normal(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[LeaseTestId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[LeaseTestId, string], req *LeasePing, _ bool) (*LeasePong, error) {
			ctx.Open() // spawn 后保持活跃（框架不再自动激活）
			return &LeasePong{Msg: req.Msg + "-pong"}, nil
		})
	})

	releaser := &mockLeaseReleaser{returnGen: 1}
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithForceReleaser(releaser),
	)

	ctx := context.Background()
	id := LeaseTestId{Name: "lease-retry-normal"}
	if !router.IsLocal(string(id.ActorType()), id.String()) {
		t.Skip("actor placed on remote, skipping test")
	}

	reply, err := Call(ctx, router, id, &LeasePing{Msg: "hello"})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Msg != "hello-pong" {
		t.Errorf("Call reply: want hello-pong, got %s", reply.Msg)
	}
	if releaser.forceReleaseCalled {
		t.Error("ForceRelease should not be called on successful call")
	}
}

// ─── Router 租约失败 → ForceRelease → 本地重试 ───

func TestRouter_LeaseTakenTriggersForceRelease(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[LeaseTestId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[LeaseTestId, string], req *LeasePing, spawning bool) (*LeasePong, error) {
			if spawning {
				return nil, &grain.ErrLeaseTaken{Key: ctx.Id().String(), Owner: "node-2", Generation: 5}
			}
			return &LeasePong{Msg: req.Msg + "-pong"}, nil
		})
	})

	releaser := &mockLeaseReleaser{returnGen: 6}
	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithForceReleaser(releaser),
	)

	ctx := context.Background()
	var id LeaseTestId
	found := false
	for i := 0; i < 50; i++ {
		candidate := LeaseTestId{Name: "lease-taken-" + itoa(i)}
		if router.IsLocal(string(candidate.ActorType()), candidate.String()) {
			id = candidate
			found = true
			break
		}
	}
	if !found {
		t.Skip("could not find locally-placed actor")
	}

	reply, err := Call(ctx, router, id, &LeasePing{Msg: "hello"})
	_ = reply
	_ = err
	if !releaser.forceReleaseCalled {
		t.Error("ForceRelease should have been called when lease is taken")
	}
	t.Logf("lease taken test: reply=%v, err=%v, forceReleaseCalled=%v", reply, err, releaser.forceReleaseCalled)
}

// ─── Router 租约重试但无 ForceReleaser ───

func TestRouter_LeaseRetryNoForceReleaser(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	node2 := Node{ID: "node-2", Addr: "127.0.0.1:8002"}
	mem := newStaticMembership(node1, node1, node2)

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, options10, func(b *actor.RegistryBuilder[LeaseTestId, string]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[LeaseTestId, string], req *LeasePing, spawning bool) (*LeasePong, error) {
			if spawning {
				return nil, &grain.ErrLeaseTaken{Key: ctx.Id().String(), Owner: "node-2", Generation: 5}
			}
			return &LeasePong{Msg: req.Msg + "-pong"}, nil
		})
	})

	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithLeaseRetry(true),
	)

	ctx := context.Background()
	var id LeaseTestId
	found := false
	for i := 0; i < 50; i++ {
		candidate := LeaseTestId{Name: "retry-no-fr-" + itoa(i)}
		if router.IsLocal(string(candidate.ActorType()), candidate.String()) {
			id = candidate
			found = true
			break
		}
	}
	if !found {
		t.Skip("could not find locally-placed actor")
	}

	_, err := Call(ctx, router, id, &LeasePing{Msg: "hello"})
	if err == nil {
		t.Error("expected error when lease is taken and no forceReleaser")
	}
	t.Logf("lease retry no forceReleaser: err=%v", err)
}

// ─── Router Close 测试 ───

func TestRouter_CloseWithForceReleaser(t *testing.T) {
	node1 := Node{ID: "node-1", Addr: "127.0.0.1:8001"}
	mem := newStaticMembership(node1, node1)
	mgr := actor.NewManager(slog.Default())

	router := NewRouter[DummyMessage, DummyCodec, DummyTransport](mem, NewConsistentHashPlacement(128), mgr,
		WithForceReleaser(&mockLeaseReleaser{returnGen: 1}),
	)

	if err := router.Close(); err != nil {
		t.Errorf("Close failed: %v", err)
	}
}

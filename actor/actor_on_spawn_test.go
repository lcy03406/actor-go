package actor_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// OnSpawn 钩子测试
// ============================================================
//
// 使用独立的 ActorId/State 类型，每个测试再 NewManager + Serve，
// 避免与 actor_test.go 中已注册 TestActorId 的 Group 冲突。

type onSpawnId struct {
	Key string
}

func (id onSpawnId) ActorType() actor.ActorType { return "OnSpawnId" }
func (id onSpawnId) String() string             { return fmt.Sprintf("OnSpawnId(%s)", id.Key) }

type onSpawnState struct {
	Value int
}

// onSpawnLogin 在 spawn handler 中保持 Actor 活跃。
type onSpawnLogin struct {
	Data onSpawnState
}

func (*onSpawnLogin) ReqType(_ onSpawnId, _ actor.OkReply) string { return "OnSpawnLogin" }
func (req *onSpawnLogin) Handle(a *actor.ActorContext[onSpawnId, onSpawnState], _ bool) (actor.OkReply, error) {
	a.Open()
	a.SetState(onSpawnState{Value: req.Data.Value})
	return actor.OK, nil
}

// onSpawnNoOpenLogin 在 spawn handler 中不调用 Open()，
// 用于验证“OnSpawn 内部 Open 可独立激活 Actor”。
type onSpawnNoOpenLogin struct {
	Data onSpawnState
}

func (*onSpawnNoOpenLogin) ReqType(_ onSpawnId, _ actor.OkReply) string { return "OnSpawnNoOpenLogin" }
func (req *onSpawnNoOpenLogin) Handle(a *actor.ActorContext[onSpawnId, onSpawnState], _ bool) (actor.OkReply, error) {
	a.SetState(onSpawnState{Value: req.Data.Value})
	return actor.OK, nil
}

// onSpawnAdd 对已存在的 Actor 累加状态（用于验证 OnSpawn 不被重复调用）。
type onSpawnAdd struct {
	Delta int
}

func (*onSpawnAdd) ReqType(_ onSpawnId, _ *onSpawnState) string { return "OnSpawnAdd" }
func (req *onSpawnAdd) Handle(a *actor.ActorContext[onSpawnId, onSpawnState], _ bool) (*onSpawnState, error) {
	a.State().Value += req.Delta
	return a.State(), nil
}

// onSpawnQuit 关闭 Actor，供测试结束时清理。
type onSpawnQuit struct{}

func (*onSpawnQuit) ReqType(_ onSpawnId, _ actor.OkReply) string { return "OnSpawnQuit" }
func (req *onSpawnQuit) Handle(a *actor.ActorContext[onSpawnId, onSpawnState], _ bool) (actor.OkReply, error) {
	a.Quit()
	return actor.OK, nil
}

// errOnSpawn 是 OnSpawn 故意返回的错误，用于测试初始化失败路径。
var errOnSpawn = errors.New("on-spawn-init-failed")

// TestOnSpawnCalledOnceOnCreate 验证 OnSpawn 在 Actor 首次创建时被调用一次，
// 且其在 ctx 上设置的 state 不会被随后的 spawn handler 覆盖前丢失（两者按顺序执行）。
func TestOnSpawnCalledOnceOnCreate(t *testing.T) {
	var calls atomic.Int32

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[onSpawnId, onSpawnState]) {
		b.SetOnSpawn(func(a *actor.ActorContext[onSpawnId, onSpawnState]) error {
			calls.Add(1)
			a.SetState(onSpawnState{Value: 7}) // OnSpawn 设置的初始值
			return nil
		})
		actor.RegisterSpawnHandler[onSpawnId, onSpawnState, *onSpawnLogin](b)
		actor.RegisterQueryHandler[onSpawnId, onSpawnState, *onSpawnAdd](b)
		actor.RegisterServeHandler[onSpawnId, onSpawnState, *onSpawnQuit](b)
	})
	defer testutil.WaitStop[onSpawnId](t, mgr, time.Second)

	id := onSpawnId{Key: "once"}
	// spawn handler 用 req.Data.Value=0 覆盖 state，故最终 Value 由 handler 决定
	if err := actor.Post(mgr, id, &onSpawnLogin{Data: onSpawnState{Value: 0}}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	testutil.Settle()

	if got := calls.Load(); got != 1 {
		t.Fatalf("OnSpawn call count after first spawn = %d, want 1", got)
	}

	// 后续发送消息不应再次触发 OnSpawn
	if _, err := actor.Call(context.Background(), mgr, id, &onSpawnAdd{Delta: 3}); err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("OnSpawn call count after second message = %d, want 1 (must not re-run)", got)
	}

	_ = actor.Post(mgr, id, &onSpawnQuit{})
}

// TestOnSpawnNotCalledOnExistingActor 验证对已存在 Actor 发送消息时 OnSpawn 不再被调用。
func TestOnSpawnNotCalledOnExistingActor(t *testing.T) {
	var calls atomic.Int32

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[onSpawnId, onSpawnState]) {
		b.SetOnSpawn(func(a *actor.ActorContext[onSpawnId, onSpawnState]) error {
			calls.Add(1)
			return nil
		})
		actor.RegisterSpawnHandler[onSpawnId, onSpawnState, *onSpawnLogin](b)
		actor.RegisterQueryHandler[onSpawnId, onSpawnState, *onSpawnAdd](b)
		actor.RegisterServeHandler[onSpawnId, onSpawnState, *onSpawnQuit](b)
	})
	defer testutil.WaitStop[onSpawnId](t, mgr, time.Second)

	id := onSpawnId{Key: "existing"}

	if err := actor.Post(mgr, id, &onSpawnLogin{Data: onSpawnState{Value: 0}}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	testutil.Settle()

	for i := 0; i < 3; i++ {
		if _, err := actor.Call(context.Background(), mgr, id, &onSpawnAdd{Delta: 1}); err != nil {
			t.Fatalf("Call Add #%d failed: %v", i, err)
		}
	}

	if got := calls.Load(); got != 1 {
		t.Fatalf("OnSpawn call count = %d, want 1 (called only on first creation)", got)
	}

	_ = actor.Post(mgr, id, &onSpawnQuit{})
}

// TestOnSpawnErrorAbortsCreation 验证 OnSpawn 返回非 nil error 时本次创建被中止：
// Actor 不会被创建（Count==0），当前 spawn 消息的 caller 会收到该错误，
// 且下次再次 spawn 时 OnSpawn 仍会被重试调用。
func TestOnSpawnErrorAbortsCreation(t *testing.T) {
	var calls atomic.Int32
	fail := atomic.Bool{}

	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[onSpawnId, onSpawnState]) {
		b.SetOnSpawn(func(a *actor.ActorContext[onSpawnId, onSpawnState]) error {
			calls.Add(1)
			if fail.Load() {
				return errOnSpawn
			}
			return nil
		})
		actor.RegisterSpawnHandler[onSpawnId, onSpawnState, *onSpawnLogin](b)
		actor.RegisterServeHandler[onSpawnId, onSpawnState, *onSpawnQuit](b)
	})
	defer testutil.WaitStop[onSpawnId](t, mgr, time.Second)

	id := onSpawnId{Key: "abort"}

	// 第一次：OnSpawn 成功 → Actor 创建
	if err := actor.Post(mgr, id, &onSpawnLogin{Data: onSpawnState{Value: 0}}); err != nil {
		t.Fatalf("Post (success path) failed: %v", err)
	}
	testutil.Settle()
	if got := calls.Load(); got != 1 {
		t.Fatalf("OnSpawn call count = %d, want 1", got)
	}
	if count, _ := actor.Count[onSpawnId](mgr); count != 1 {
		t.Fatalf("expected 1 actor after success, got %d", count)
	}
	_ = actor.Post(mgr, id, &onSpawnQuit{})
	testutil.WaitStop[onSpawnId](t, mgr, time.Second)

	// 第二次：OnSpawn 失败 → Actor 不创建，caller 收到该错误
	fail.Store(true)
	_, err := actor.Call(context.Background(), mgr, id, &onSpawnLogin{Data: onSpawnState{Value: 0}})
	if err == nil {
		t.Fatalf("expected error from failed OnSpawn, got nil")
	}
	if !errors.Is(err, errOnSpawn) {
		t.Fatalf("error = %v, want wrapped %v", err, errOnSpawn)
	}
	testutil.Settle()
	if got := calls.Load(); got != 2 {
		t.Fatalf("OnSpawn call count = %d, want 2 (re-attempted on next spawn)", got)
	}
	if count, _ := actor.Count[onSpawnId](mgr); count != 0 {
		t.Fatalf("expected 0 actors after failed OnSpawn, got %d", count)
	}
}

// TestOnSpawnCanOpenActor 验证 OnSpawn 内部调用 Open() 可使 Actor 保持活跃，
// 即使随后的 spawn handler 未调用 Open() 也如此。
func TestOnSpawnCanOpenActor(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[onSpawnId, onSpawnState]) {
		b.SetOnSpawn(func(a *actor.ActorContext[onSpawnId, onSpawnState]) error {
			a.Open() // 在 OnSpawn 中激活 Actor
			return nil
		})
		actor.RegisterSpawnHandler[onSpawnId, onSpawnState, *onSpawnNoOpenLogin](b)
		actor.RegisterQueryHandler[onSpawnId, onSpawnState, *onSpawnAdd](b)
		actor.RegisterServeHandler[onSpawnId, onSpawnState, *onSpawnQuit](b)
	})
	defer testutil.WaitStop[onSpawnId](t, mgr, time.Second)

	id := onSpawnId{Key: "open-in-onspawn"}
	if err := actor.Post(mgr, id, &onSpawnNoOpenLogin{Data: onSpawnState{Value: 5}}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	testutil.Settle()

	// Actor 应保持存活（OnSpawn 中 Open 了），可继续被查询
	if count, _ := actor.Count[onSpawnId](mgr); count != 1 {
		t.Fatalf("expected 1 active actor (kept alive by OnSpawn Open), got %d", count)
	}
	reply, err := actor.Call(context.Background(), mgr, id, &onSpawnAdd{Delta: 10})
	if err != nil {
		t.Fatalf("Call after OnSpawn-Open failed: %v", err)
	}
	if reply.Value != 15 {
		t.Fatalf("expected Value=15, got %d", reply.Value)
	}

	_ = actor.Post(mgr, id, &onSpawnQuit{})
}

package actor_test

import (
	"log/slog"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// OnQuit 测试类型定义
// ============================================================

// testOnQuit 在 spawn 时注册 OnQuit，并立即调用 Quit 触发退出。
type testOnQuit struct {
	QuitCalled *atomic.Bool
}

func (*testOnQuit) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestOnQuit" }
func (req *testOnQuit) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.Control().OnQuit = func() {
		if req.QuitCalled != nil {
			req.QuitCalled.Store(true)
		}
	}
	a.Quit()
	return actor.OK, nil
}

// testOnQuitPush 在 spawn 时通过 PushOnQuit 注册多个回调，验证执行顺序（LIFO）。
type testOnQuitPush struct {
	Order *[]int
	Mu    *sync.Mutex
}

func (*testOnQuitPush) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestOnQuitPush" }
func (req *testOnQuitPush) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.Control().PushOnQuit(func() {
		req.Mu.Lock()
		*req.Order = append(*req.Order, 1)
		req.Mu.Unlock()
	})
	a.Control().PushOnQuit(func() {
		req.Mu.Lock()
		*req.Order = append(*req.Order, 2)
		req.Mu.Unlock()
	})
	a.Quit()
	return actor.OK, nil
}

// testOnQuitPanic 注册一个会 panic 的 OnQuit，验证框架 recover 不阻断退出，
// 且后续注册的 OnQuit 仍会被执行（通过 PushOnQuit 串联）。
type testOnQuitPanic struct {
	AfterPanic *atomic.Bool
}

func (*testOnQuitPanic) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestOnQuitPanic" }
func (req *testOnQuitPanic) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.Control().OnQuit = func() {
		panic("boom in OnQuit")
	}
	a.Control().PushOnQuit(func() {
		if req.AfterPanic != nil {
			req.AfterPanic.Store(true)
		}
	})
	a.Quit()
	return actor.OK, nil
}

// testOnQuitByKill 在 spawn 时注册 OnQuit，由外部 KillActor 触发退出。
type testOnQuitByKill struct {
	QuitCalled *atomic.Bool
	Done       chan struct{}
}

func (*testOnQuitByKill) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestOnQuitByKill" }
func (req *testOnQuitByKill) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.Control().OnQuit = func() {
		if req.QuitCalled != nil {
			req.QuitCalled.Store(true)
		}
	}
	if req.Done != nil {
		close(req.Done)
	}
	return actor.OK, nil
}

// ============================================================
// OnQuit 测试
// ============================================================

// TestOnQuitCalledOnSelfQuit 测试自身调用 Quit 时 OnQuit 被触发。
func TestOnQuitCalledOnSelfQuit(t *testing.T) {
	quitCalled := &atomic.Bool{}
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testOnQuit](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "onquit_self"}
	if err := actor.Post(mgr, id, &testOnQuit{QuitCalled: quitCalled}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	actor.JoinActor(mgr, id)

	if !quitCalled.Load() {
		t.Error("expected OnQuit to be called when actor quits itself")
	}
}

// TestOnQuitPushOrder 测试 PushOnQuit 多次注册时按 LIFO 顺序执行。
func TestOnQuitPushOrder(t *testing.T) {
	order := []int{}
	mu := &sync.Mutex{}
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testOnQuitPush](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "onquit_push"}
	if err := actor.Post(mgr, id, &testOnQuitPush{Order: &order, Mu: mu}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	actor.JoinActor(mgr, id)

	mu.Lock()
	got := make([]int, len(order))
	copy(got, order)
	mu.Unlock()

	// 第一次 Push 先注册（1），第二次后注册（2）；LIFO 应 2 先于 1。
	if len(got) != 2 || got[0] != 2 || got[1] != 1 {
		t.Errorf("expected OnQuit order [2,1] (LIFO), got %v", got)
	}
}

// TestOnQuitPanicRecovered 测试 OnQuit panic 被框架 recover，不影响退出，
// 且 PushOnQuit 串联的后续回调仍执行。
func TestOnQuitPanicRecovered(t *testing.T) {
	afterPanic := &atomic.Bool{}
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testOnQuitPanic](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "onquit_panic"}
	if err := actor.Post(mgr, id, &testOnQuitPanic{AfterPanic: afterPanic}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	// JoinActor 应成功返回（未因 panic 崩溃），并等待 run 退出。
	if !actor.JoinActor(mgr, id) {
		t.Fatal("JoinActor should succeed even if OnQuit panics")
	}

	if !afterPanic.Load() {
		t.Error("expected OnQuit pushed after panicking one to still execute")
	}
}

// TestOnQuitCalledOnKill 测试外部 KillActor 强制关闭时也触发 OnQuit。
func TestOnQuitCalledOnKill(t *testing.T) {
	quitCalled := &atomic.Bool{}
	done := make(chan struct{})
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testOnQuitByKill](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "onquit_kill"}
	if err := actor.Post(mgr, id, &testOnQuitByKill{QuitCalled: quitCalled, Done: done}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	<-done // 等待 handler 注册 OnQuit

	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false")
	}
	actor.JoinActor(mgr, id)

	if !quitCalled.Load() {
		t.Error("expected OnQuit to be called when actor is killed")
	}
}

// testOnQuitSetter 在 spawn 时注册 OnQuit 并保持在活跃态（不退出）。
type testOnQuitSetter struct {
	QuitCalled *atomic.Bool
}

func (*testOnQuitSetter) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestOnQuitSetter" }
func (req *testOnQuitSetter) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.Control().OnQuit = func() {
		if req.QuitCalled != nil {
			req.QuitCalled.Store(true)
		}
	}
	return actor.OK, nil
}

// TestOnQuitNotCalledWhileActive 测试 Actor 仍活跃（未退出）时 OnQuit 不被调用，
// 退出后才被调用。
func TestOnQuitNotCalledWhileActive(t *testing.T) {
	quitCalled := &atomic.Bool{}
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testOnQuitSetter](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "onquit_active"}
	if err := actor.Post(mgr, id, &testOnQuitSetter{QuitCalled: quitCalled}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	testutil.Settle()

	// 活跃期间短暂停留，OnQuit 不应被调用。
	time.Sleep(50 * time.Millisecond)
	if quitCalled.Load() {
		t.Error("OnQuit should NOT be called while actor is still active")
	}

	// 退出后才应调用。
	actor.CloseActor(mgr, id)
	actor.JoinActor(mgr, id)
	if !quitCalled.Load() {
		t.Error("expected OnQuit to be called after actor exited")
	}
}

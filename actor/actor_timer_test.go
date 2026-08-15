package actor_test

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// 定时器测试类型定义
// ============================================================

// testTimerLogin spawn 时挂载定时器：50ms 后修改状态。
type testTimerLogin struct {
	Data TestActorData
}

func (*testTimerLogin) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestTimerLogin" }
func (req *testTimerLogin) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open() // spawn 后保持活跃（框架不再自动激活）
	a.SetState(TestActorData{Int: req.Data.Int})
	a.Timer(50*time.Millisecond, func() {
		a.State().Int += 100
	})
	return actor.OK, nil
}

// testTimerCancelLogin spawn 时设置定时器并立即取消。
type testTimerCancelLogin struct {
	Data TestActorData
}

func (*testTimerCancelLogin) ReqType(_ TestActorId, _ actor.OkReply) string {
	return "TestTimerCancelLogin"
}
func (req *testTimerCancelLogin) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open() // spawn 后保持活跃（框架不再自动激活）
	a.SetState(TestActorData{Int: req.Data.Int})
	timer := a.Timer(50*time.Millisecond, func() {
		a.State().Int += 100
	})
	a.StopTimer(timer)
	return actor.OK, nil
}

// testLogoutTimer Logout 时设置定时器后 Quit（定时器应被取消）。
type testLogoutTimer struct {
	Fired *atomic.Bool
}

func (*testLogoutTimer) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestLogoutTimer" }
func (req *testLogoutTimer) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Timer(100*time.Millisecond, func() {
		if req.Fired != nil {
			req.Fired.Store(true)
		}
	})
	a.Quit()
	return actor.OK, nil
}

// testLoginTimer spawn 时设置定时器（供测试关闭/强制关闭取消定时器）。
type testLoginTimer struct {
	Data  TestActorData
	Fired *atomic.Bool
	Done  chan struct{}
}

func (*testLoginTimer) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestLoginTimer" }
func (req *testLoginTimer) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	a.SetState(TestActorData{Int: req.Data.Int})
	a.Timer(100*time.Millisecond, func() {
		req.Fired.Store(true)
	})
	if req.Done != nil {
		close(req.Done)
	}
	return actor.OK, nil
}

// TestActorTimer 测试定时器功能。
func TestActorTimer(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testTimerLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
	})

	testId := TestActorId{ServerId: 1, OpenId: "timer_test"}
	actor.Post(mgr, testId, &testTimerLogin{Data: TestActorData{Int: 10}})
	time.Sleep(100 * time.Millisecond) // 等待定时器触发

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Result != 110 {
		t.Errorf("expected timer-triggered result 110, got %d", reply.Result)
	}
}

// TestActorTimerCancel 测试取消定时器。
func TestActorTimerCancel(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testTimerCancelLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
	})

	testId := TestActorId{ServerId: 1, OpenId: "timer_cancel"}
	actor.Post(mgr, testId, &testTimerCancelLogin{Data: TestActorData{Int: 10}})
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	reply2, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply2.Result != 10 {
		t.Errorf("expected unchanged result 10 (timer cancelled), got %d", reply2.Result)
	}
}

// TestTimerCancelledOnQuit 测试自身退出（Quit）时定时器被取消，回调不触发。
func TestTimerCancelledOnQuit(t *testing.T) {
	var timerFired atomic.Bool
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		// handler 设置定时器后调用 Quit，定时器应被取消
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testLogoutTimer](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_quit"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	// 调用 Quit handler
	_, err := actor.Call(context.Background(), mgr, id, &testLogoutTimer{Fired: &timerFired})
	if err != nil {
		t.Fatalf("Quit handler should succeed: %v", err)
	}

	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间后，验证定时器未触发
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor quits, but it fired")
	}
}

// TestTimerCancelledOnClose 测试外部温和关闭（CloseActor）时定时器被取消，回调不触发。
func TestTimerCancelledOnClose(t *testing.T) {
	var timerFired atomic.Bool
	spawnDone := make(chan struct{})
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testLoginTimer](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_close"}
	if err := actor.Post(mgr, id, &testLoginTimer{Data: TestActorData{Int: 0}, Fired: &timerFired, Done: spawnDone}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	<-spawnDone // 确认 handler 已执行并设置定时器

	// 外部温和关闭
	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false")
	}
	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor is closed, but it fired")
	}
}

// TestTimerCancelledOnKill 测试外部强制关闭（KillActor）时定时器被取消，回调不触发。
func TestTimerCancelledOnKill(t *testing.T) {
	var timerFired atomic.Bool
	spawnDone := make(chan struct{})
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *testLoginTimer](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_kill"}
	if err := actor.Post(mgr, id, &testLoginTimer{Data: TestActorData{Int: 0}, Fired: &timerFired, Done: spawnDone}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	<-spawnDone // 确认 handler 已执行并设置定时器

	// 强制关闭：cancel ctx 应立即取消定时器
	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false")
	}
	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor is killed, but it fired")
	}
}

package actor_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// 退出/关闭测试类型定义
// ============================================================

// testSlowAdd 模拟慢查询（sleep）。
type testSlowAdd struct {
	Add int
}

func (*testSlowAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestSlowAdd" }
func (req *testSlowAdd) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	time.Sleep(200 * time.Millisecond)
	a.State().Int += req.Add
	return &TestAddReply{Result: a.State().Int}, nil
}

// testGracefulAdd 模拟慢查询：close Start 后 sleep，再累加。
type testGracefulAdd struct {
	Start chan struct{}
	Add   int
}

func (*testGracefulAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestGracefulAdd" }
func (req *testGracefulAdd) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	if req.Start != nil {
		close(req.Start)
	}
	time.Sleep(150 * time.Millisecond)
	a.State().Int += req.Add
	return &TestAddReply{Result: a.State().Int}, nil
}

// testKillAdd 模拟可被 ctx 取消的慢查询：监听 a.Context().Done()。
type testKillAdd struct {
	Start chan struct{}
	Add   int
}

func (*testKillAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestKillAdd" }
func (req *testKillAdd) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	close(req.Start)
	select {
	case <-a.Context().Done():
		return nil, a.Context().Err()
	case <-time.After(2 * time.Second):
		a.State().Int += req.Add
		return &TestAddReply{Result: a.State().Int}, nil
	}
}

// testCloseWait 模拟 in-flight 慢关闭：阻塞在 Done 直到外部释放。
type testCloseWait struct {
	Done chan struct{}
}

func (*testCloseWait) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestCloseWait" }
func (req *testCloseWait) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	<-req.Done
	a.Quit()
	return actor.OK, nil
}

// testCloseWait2 模拟 in-flight 慢关闭（带 Start/Done 双信号）。
type testCloseWait2 struct {
	Start chan struct{}
	Done  chan struct{}
}

func (*testCloseWait2) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestCloseWait2" }
func (req *testCloseWait2) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	close(req.Start)
	<-req.Done
	a.Quit()
	return actor.OK, nil
}

// testPanicAdd 模拟 handler panic。
type testPanicAdd struct {
	Add int
}

func (*testPanicAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestPanicAdd" }
func (req *testPanicAdd) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	panic("test panic in handler")
}

// TestCloseActorBasic 测试 CloseActor/JoinActor 的返回值：存在返回 true，不存在返回 false。
func TestCloseActorBasic(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "close_basic"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	if !actor.CloseActor(mgr, id) {
		t.Error("expected CloseActor to return true for existing actor")
	}
	if !actor.JoinActor(mgr, id) {
		t.Error("expected JoinActor to return true after CloseActor")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after close+join, got %d", count)
	}

	// 不存在的 actor
	missing := TestActorId{ServerId: 99, OpenId: "missing"}
	if actor.CloseActor(mgr, missing) {
		t.Error("expected CloseActor to return false for non-existent actor")
	}
	if actor.JoinActor(mgr, missing) {
		t.Error("expected JoinActor to return false for non-existent actor")
	}
}

// TestKillActorBasic 测试 KillActor 的返回值：存在返回 true，不存在返回 false。
func TestKillActorBasic(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "kill_basic"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	if !actor.KillActor(mgr, id) {
		t.Error("expected KillActor to return true for existing actor")
	}
	if !actor.JoinActor(mgr, id) {
		t.Error("expected JoinActor to return true after KillActor")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after kill+join, got %d", count)
	}

	missing := TestActorId{ServerId: 99, OpenId: "missing"}
	if actor.KillActor(mgr, missing) {
		t.Error("expected KillActor to return false for non-existent actor")
	}
}

var options100 actor.Options = actor.Options{
	BufMails: 100,
}

// TestCloseActorGraceful 测试温和关闭：in-flight handler 不被打断，正常完成。
func TestCloseActorGraceful(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerStart := make(chan struct{})
	actor.Serve(mgr, options100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testGracefulAdd](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "graceful"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 10}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	type callRes struct {
		reply *TestAddReply
		err   error
	}
	resCh := make(chan callRes, 1)
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &testGracefulAdd{Start: handlerStart, Add: 5})
		resCh <- callRes{reply, err}
	}()

	<-handlerStart // 等 handler 开始
	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false for existing actor")
	}

	// 温和关闭不应打断 in-flight handler
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Errorf("in-flight handler should complete, got err: %v", r.err)
		} else if r.reply.Result != 15 {
			t.Errorf("expected result 15, got %d", r.reply.Result)
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not complete in time")
	}

	actor.JoinActor(mgr, id)
}

// TestKillActorInterrupts 测试强制关闭：cancel ctx 中断 in-flight handler 中监听 ctx.Done 的操作。
func TestKillActorInterrupts(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerStart := make(chan struct{})
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testKillAdd](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "kill_interrupt"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	type callRes struct {
		err error
	}
	resCh := make(chan callRes, 1)
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &testKillAdd{Start: handlerStart, Add: 1})
		resCh <- callRes{err}
	}()

	<-handlerStart
	start := time.Now()
	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false for existing actor")
	}

	select {
	case r := <-resCh:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("KillActor should interrupt handler quickly, took %v", elapsed)
		}
		if r.err == nil {
			t.Error("expected handler to return error after kill (ctx canceled)")
		}
	case <-time.After(time.Second):
		t.Error("Call did not return after KillActor")
	}

	actor.JoinActor(mgr, id)
}

// TestCloseActorDrainsMailbox 测试温和关闭后排队消息以 ActorClosedError 失败，
// 而 in-flight handler 仍正常完成。
func TestCloseActorDrainsMailbox(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerStart := make(chan struct{})
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testGracefulAdd](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "drain"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	type callRes struct {
		reply *TestAddReply
		err   error
	}
	r1 := make(chan callRes, 1)
	r2 := make(chan callRes, 1)
	r3 := make(chan callRes, 1)

	// #1 进入 in-flight
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &testGracefulAdd{Start: handlerStart, Add: 1})
		r1 <- callRes{reply, err}
	}()
	<-handlerStart

	// #2 #3 进入 mailbox 排队
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &testGracefulAdd{Add: 10})
		r2 <- callRes{reply, err}
	}()
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &testGracefulAdd{Add: 100})
		r3 <- callRes{reply, err}
	}()
	time.Sleep(20 * time.Millisecond)

	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false for existing actor")
	}

	// #1 应该正常完成
	select {
	case r := <-r1:
		if r.err != nil {
			t.Errorf("in-flight Call should succeed, got err: %v", r.err)
		} else if r.reply.Result != 1 {
			t.Errorf("expected result 1, got %d", r.reply.Result)
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not complete")
	}

	// #2 #3 应该收到 ActorClosedError（drainMailbox 排空残余消息）
	for i, ch := range []chan callRes{r2, r3} {
		select {
		case r := <-ch:
			if r.err == nil {
				t.Errorf("queued Call #%d should fail, got nil err", i+2)
				continue
			}
			var ace *actor.ActorClosedError
			if !errors.As(r.err, &ace) {
				t.Errorf("queued Call #%d expected ActorClosedError, got %T: %v", i+2, r.err, r.err)
			}
		case <-time.After(time.Second):
			t.Errorf("queued Call #%d did not return", i+2)
		}
	}

	actor.JoinActor(mgr, id)
}

// TestRespawnAfterClose 测试 CloseActor + JoinActor 后可重新 spawn 同一 ID 的 Actor。
func TestRespawnAfterClose(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "respawn"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 100}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false")
	}
	if !actor.JoinActor(mgr, id) {
		t.Fatal("JoinActor returned false")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Fatalf("expected 0 actors after close+join, got %d", count)
	}

	// 重新 spawn 同一 ID，状态应被重置
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 50}}); err != nil {
		t.Fatalf("Post Login for respawn failed: %v", err)
	}
	testutil.Settle()

	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("expected 1 actor after respawn, got %d", count)
	}

	reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call after respawn failed: %v", err)
	}
	if reply.Result != 50 {
		t.Errorf("expected state reset to 50, got %d", reply.Result)
	}
}

// TestCloseJoinManager 测试 Manager 级别的关闭与等待：
// IsClosed、CloseManager 幂等、JoinManager 等待 in-flight handler 完成后才返回、退出后新消息失败。
func TestCloseJoinManager(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerDone := make(chan struct{})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testCloseWait](b)
	})

	for i := 0; i < 3; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("mgr_%d", i)}
		if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: i}}); err != nil {
			t.Fatalf("Post Login failed: %v", err)
		}
	}
	testutil.Settle()

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}
	if mgr.IsClosed() {
		t.Fatal("manager should not be closed initially")
	}

	// 对第一个 actor 发送 Close 请求，handler 会阻塞在 handlerDone 上
	id0 := TestActorId{ServerId: 1, OpenId: "mgr_0"}
	go actor.Call(context.Background(), mgr, id0, &testCloseWait{Done: handlerDone})
	testutil.Settle() // 确保 handler 已进入阻塞

	// 启动 JoinManager goroutine，它应阻塞等待 in-flight handler 完成
	joinDone := make(chan struct{})
	go func() {
		mgr.JoinManager()
		close(joinDone)
	}()

	// JoinManager 不应在 handler 仍阻塞时返回
	time.Sleep(100 * time.Millisecond)
	select {
	case <-joinDone:
		t.Fatal("JoinManager should not return while handler is still in-flight")
	default:
		// 预期行为：仍阻塞
	}

	// 释放 handler，JoinManager 应随后完成
	close(handlerDone)
	select {
	case <-joinDone:
	case <-time.After(2 * time.Second):
		t.Fatal("JoinManager did not return after handler unblocked")
	}

	if !mgr.IsClosed() {
		t.Error("manager should be closed after JoinManager")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after JoinManager, got %d", count)
	}

	// JoinManager 后新消息失败（stopping=true 阻止新请求）
	id := TestActorId{ServerId: 1, OpenId: "after_join"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err == nil {
		t.Error("expected Post to fail after JoinManager")
	}
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, id, &TestAdd{Add: 1}); err == nil {
		t.Error("expected Call to fail after JoinManager")
	}

	// 二次 CloseManager/JoinManager 应幂等
	mgr.CloseManager()
	mgr.JoinManager()
}

// TestQuitExitsAtEndOfMessage 测试 Quit（自身发起）：当前消息正常完成，在当前消息结束时退出，
// 退出后后续消息以 ActorClosedError 失败。
func TestQuitExitsAtEndOfMessage(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "quit_self"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 42}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	// Call TestLogout：handler 内部调用 a.Quit()，当前消息应正常完成
	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, id, &TestLogout{})
	if err != nil {
		t.Fatalf("Quit handler should complete normally, got: %v", err)
	}

	// Quit 在当前消息结束后退出，后续消息应失败
	_, err = actor.Call(ctx, mgr, id, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error for message after Quit")
	}

	// Quit 是自身发起的退出，在第一次 Call 返回后 run loop 已开始清理流程。
	// 先检查 Count：无论 run loop 清理到哪一步，closed=true 的 actor 已不被计数。
	// 再 JoinActor：无论返回 true（还在 map 中，等待 doneCh）还是 false（已从 map 移除），
	// 只要不卡死就说明 run goroutine 已退出。
	testutil.WaitStop[TestActorId](t, mgr, time.Second)
	actor.JoinActor(mgr, id) // 不应卡死，返回值无所谓
}

// TestKillDrainsMailbox 测试 Kill（外部强制关闭）：中断 in-flight handler，
// 并排空 mailbox 中排队消息以 ActorClosedError 失败。
func TestKillDrainsMailbox(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerStart := make(chan struct{})
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testKillAdd](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "kill_drain"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	type callRes struct {
		err error
	}
	r1 := make(chan callRes, 1)
	r2 := make(chan callRes, 1)
	r3 := make(chan callRes, 1)

	// #1 进入 in-flight
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &testKillAdd{Start: handlerStart, Add: 1})
		r1 <- callRes{err}
	}()
	<-handlerStart

	// #2 #3 进入 mailbox 排队
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &testKillAdd{Add: 10})
		r2 <- callRes{err}
	}()
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &testKillAdd{Add: 100})
		r3 <- callRes{err}
	}()
	time.Sleep(20 * time.Millisecond)

	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false for existing actor")
	}

	// #1 应被 Kill 中断（ctx.Done 触发）
	select {
	case r := <-r1:
		if r.err == nil {
			t.Error("in-flight Call should be interrupted by Kill")
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not return after Kill")
	}

	// #2 #3 应收到 ActorClosedError（drainMailbox 排空）
	for i, ch := range []chan callRes{r2, r3} {
		select {
		case r := <-ch:
			if r.err == nil {
				t.Errorf("queued Call #%d should fail after Kill, got nil err", i+2)
				continue
			}
			var ace *actor.ActorClosedError
			if !errors.As(r.err, &ace) {
				t.Errorf("queued Call #%d expected ActorClosedError, got %T: %v", i+2, r.err, r.err)
			}
		case <-time.After(time.Second):
			t.Errorf("queued Call #%d did not return", i+2)
		}
	}

	actor.JoinActor(mgr, id)
}

// TestActorRespawnAfterKill 测试 KillActor + JoinActor 后可重新 spawn 同一 ID 的 Actor。
func TestActorRespawnAfterKill(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "respawn_kill"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 100}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false")
	}
	if !actor.JoinActor(mgr, id) {
		t.Fatal("JoinActor returned false")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Fatalf("expected 0 actors after kill+join, got %d", count)
	}

	// 重新 spawn 同一 ID
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 50}}); err != nil {
		t.Fatalf("Post Login for respawn failed: %v", err)
	}
	testutil.Settle()

	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("expected 1 actor after respawn, got %d", count)
	}

	reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call after respawn failed: %v", err)
	}
	if reply.Result != 50 {
		t.Errorf("expected state reset to 50, got %d", reply.Result)
	}
}

// TestActorFinalizeWithInFlight 测试 Finalize 在 Actor 有 in-flight handler 时正确等待。
func TestActorFinalizeWithInFlight(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	handlerStart := make(chan struct{})
	handlerDone := make(chan struct{})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testCloseWait2](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "fin_inflight"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	// 发送会阻塞的请求
	go actor.Call(context.Background(), mgr, id, &testCloseWait2{Start: handlerStart, Done: handlerDone})
	<-handlerStart // 确认 handler 进入阻塞

	// 在另一个 goroutine 中调用 Finalize，应阻塞等待 handler 完成
	finalizeDone := make(chan struct{})
	go func() {
		actor.Finalize(mgr, &testCloseWait2{Done: handlerDone})
		close(finalizeDone)
	}()

	// Finalize 不应在 handler 仍阻塞时返回
	time.Sleep(100 * time.Millisecond)
	select {
	case <-finalizeDone:
		t.Fatal("Finalize should not return while handler is still in-flight")
	default:
		// 预期行为
	}

	// 释放 handler，Finalize 应完成
	close(handlerDone)
	select {
	case <-finalizeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Finalize did not return after handler unblocked")
	}

	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after finalize, got %d", count)
	}
}

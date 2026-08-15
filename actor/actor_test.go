package actor_test

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// 测试类型定义
// ============================================================

type TestActorId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id TestActorId) ActorType() actor.ActorType { return "TestActorId" }
func (id TestActorId) String() string {
	return fmt.Sprintf("TestActorId(%d,%s)", id.ServerId, id.OpenId)
}

type TestActorData struct {
	Int int
}

type TestAddReply struct {
	Result int
}

// ── RequestHandler 请求类型：Handle 方法内聚在请求上 ──

// TestLogin 实现 RequestHandler，spawn 后保持活跃。
type TestLogin struct {
	Data TestActorData
}

func (*TestLogin) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestLogin" }
func (req *TestLogin) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open() // spawn 后保持活跃（框架不再自动激活）
	a.SetState(TestActorData{Int: req.Data.Int})
	return actor.OK, nil
}

// TestLogout 实现 RequestHandler，直接关闭 Actor。
type TestLogout struct{}

func (*TestLogout) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestLogout" }
func (req *TestLogout) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Quit() // Logout 直接关闭 Actor
	return actor.OK, nil
}

// TestClose 实现 RequestHandler，关闭 Actor。
type TestClose struct{}

func (*TestClose) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestClose" }
func (req *TestClose) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Quit()
	return actor.OK, nil
}

// TestAdd 实现 RequestHandler，累加状态并返回回复。
type TestAdd struct {
	Add int
}

func (*TestAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestAdd" }
func (req *TestAdd) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	a.State().Int += req.Add
	return &TestAddReply{Result: a.State().Int}, nil
}

// TestLoginWithReply 实现 RequestHandler（RegisterServe 用），返回 TestAddReply 而非 OkReply。
type TestLoginWithReply struct {
	Data TestActorData
}

func (*TestLoginWithReply) ReqType(_ TestActorId, _ *TestAddReply) string {
	return "TestLoginWithReply"
}
func (req *TestLoginWithReply) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (*TestAddReply, error) {
	a.Open() // spawn 后保持活跃（框架不再自动激活）
	a.SetState(TestActorData{Int: req.Data.Int})
	return &TestAddReply{Result: a.State().Int}, nil
}

// testSpawningLogin 用于测试 spawning 标志：spawning=true 时初始化，
// spawning=false 时追加，验证 handler 在 Actor 已存在时仍被调用。
type testSpawningLogin struct {
	Data TestActorData
}

func (*testSpawningLogin) ReqType(_ TestActorId, _ actor.OkReply) string { return "TestSpawningLogin" }
func (req *testSpawningLogin) Handle(a *actor.ActorContext[TestActorId, TestActorData], spawning bool) (actor.OkReply, error) {
	if spawning {
		a.Open()                          // spawn 后保持活跃（框架不再自动激活）
		a.SetState(TestActorData{Int: 1}) // spawning=true 时初始化
	} else {
		a.State().Int += 100 // spawning=false 时追加
	}
	return actor.OK, nil
}

func setupManager(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestLogout](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestClose](b)
	})
}

// TestActorBasic 测试 Actor 的基本功能：spawn、call、post、close。
func TestActorBasic(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "4242"}

	// Post: spawn
	err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 1}})
	if err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	// Call: Go 从返回值推导 R=TestAddReply，ReqType(TestActorId, *TestAddReply) 确保兼容
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 10})
	if err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if reply.Result != 11 {
		t.Errorf("expected result 11, got %d", reply.Result)
	}

	// Post: logout (fire-and-forget, handler 内部会 post Close)
	err = actor.Post(mgr, testId, &TestLogout{})
	if err != nil {
		t.Fatalf("Post Logout failed: %v", err)
	}

	// 轮询等待 Actor 关闭
	testutil.WaitStop[TestActorId](t, mgr, time.Second)
}

// TestActorFinalize 测试 Finalize 广播关闭。
func TestActorFinalize(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	// 创建多个 Actor
	for i := 0; i < 3; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("player_%d", i)}
		err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: i}})
		if err != nil {
			t.Fatalf("Post Login failed: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Errorf("expected 3 actors, got %d", count)
	}

	actor.Finalize(mgr, &TestClose{})

	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after finalize, got %d", count)
	}
}

// TestActorNotFound 测试 Actor 不存在时的错误处理。
func TestActorNotFound(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 99, OpenId: "nonexistent"}

	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error for nonexistent actor")
	}
}

// TestActorBroadcast 测试广播功能。
func TestActorBroadcast(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	for i := 0; i < 5; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("player_%d", i)}
		actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}})
	}
	time.Sleep(100 * time.Millisecond)

	count, _ := actor.Broadcast(mgr, &TestClose{})
	if count != 5 {
		t.Errorf("expected broadcast to 5 actors, got %d", count)
	}
	// 所有 Actor 收到 Close 后应关闭
	testutil.WaitStop[TestActorId](t, mgr, time.Second)
}

// TestActorCallWithinTimeout 测试在超时时间内完成调用的正常路径。
func TestActorCallWithinTimeout(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "timeout_test"}

	// 创建 Actor
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	testutil.Settle()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err != nil {
		t.Fatalf("Call with timeout failed: %v", err)
	}
	if reply.Result != 1 {
		t.Errorf("expected result 1, got %d", reply.Result)
	}
}

// TestActorMulticast 测试多播功能。
func TestActorMulticast(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestClose](b)
	})

	ids := make([]TestActorId, 3)
	for i := 0; i < 3; i++ {
		ids[i] = TestActorId{ServerId: 1, OpenId: fmt.Sprintf("multi_%d", i)}
		actor.Post(mgr, ids[i], &TestLogin{Data: TestActorData{Int: i}})
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}

	// Multicast close 到指定 Actor
	hit, _ := actor.Multicast(mgr, ids[:2], &TestClose{})
	if hit != 2 {
		t.Errorf("expected multicast to hit 2 actors, got %d", hit)
	}

	// 第 3 个 Actor 未被关闭
	testutil.WaitCount[TestActorId](t, mgr, 1, time.Second)
}

// TestActorRequestSpawn 测试 RegisterServe（首次请求创建 Actor 并返回回复）。
func TestActorRequestSpawn(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterServeHandler[TestActorId, TestActorData, *TestLoginWithReply](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
	})

	testId := TestActorId{ServerId: 1, OpenId: "reqspawn"}

	// 首次 Call 触发 spawn 并返回回复
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestLoginWithReply{Data: TestActorData{Int: 42}})
	if err != nil {
		t.Fatalf("Call RequestSpawn failed: %v", err)
	}
	if reply.Result != 42 {
		t.Errorf("expected result 42, got %d", reply.Result)
	}

	// 确认 Actor 已创建，后续 Call 正常
	reply2, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 8})
	if err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if reply2.Result != 50 {
		t.Errorf("expected result 50, got %d", reply2.Result)
	}
}

// TestActorSpawningFlag 测试 spawning 标志的正确性：RegisterServe 首次调用 spawning=true，
// 对已存在 Actor 再次调用同一 handler 时 spawning=false。
func TestActorSpawningFlag(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		// 使用 RegisterServeHandler（allow_spawn=true, allow_query=true）才能让 handler
		// 在 Actor 已存在时也被调用，从而触发 spawning=false 分支。
		actor.RegisterServeHandler[TestActorId, TestActorData, *testSpawningLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
	})

	testId := TestActorId{ServerId: 1, OpenId: "spawning"}

	ctx := context.Background()

	// 首次 Call: spawning=true，初始化 state=1
	_, err := actor.Call(ctx, mgr, testId, &testSpawningLogin{Data: TestActorData{Int: 0}})
	if err != nil {
		t.Fatalf("first Call (spawn) failed: %v", err)
	}
	reply, _ := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if reply.Result != 1 {
		t.Errorf("expected result 1 (spawning=true initialized), got %d", reply.Result)
	}

	// 第二次 Call: spawning=false，追加 100
	_, err = actor.Call(ctx, mgr, testId, &testSpawningLogin{Data: TestActorData{Int: 0}})
	if err != nil {
		t.Fatalf("second Call (query) failed: %v", err)
	}
	reply2, _ := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if reply2.Result != 101 {
		t.Errorf("expected result 101 (spawning=false added 100), got %d", reply2.Result)
	}
}

// TestActorCallContextCancel 测试 context 取消时 Call 返回错误。
func TestActorCallContextCancel(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "ctx_cancel"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	testutil.Settle()

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	_, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

// TestActorCallTimeoutExceeded 测试超时发生时 Call 返回错误。
func TestActorCallTimeoutExceeded(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		// 注册一个会延迟的 handler
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testSlowAdd](b)
	})

	testId := TestActorId{ServerId: 1, OpenId: "timeout_exceed"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	testutil.Settle()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := actor.Call(ctx, mgr, testId, &testSlowAdd{Add: 1})
	if err == nil {
		t.Error("expected timeout error")
	}
}

// TestActorSequentialCalls 测试对同一 Actor 的连续多次 Call。
func TestActorSequentialCalls(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "sequential"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	testutil.Settle()

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: i})
		if err != nil {
			t.Fatalf("Call %d failed: %v", i, err)
		}
		expected := i * (i + 1) / 2 // 1+2+...+i
		if reply.Result != expected {
			t.Errorf("Call %d: expected result %d, got %d", i, expected, reply.Result)
		}
	}
}

// ============================================================
// 补充测试：Handler Panic、并发、未注册 Group、空 Multicast
// ============================================================

// TestActorHandlerPanic 测试 handler 中发生 panic 时，Call 能收到错误而非让 actor 崩溃。
func TestActorHandlerPanic(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *testPanicAdd](b)
	})

	id := TestActorId{ServerId: 1, OpenId: "panic_test"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, id, &testPanicAdd{Add: 1})
	if err == nil {
		t.Error("expected error when handler panics")
		return
	}
	var hce *actor.HandlerCallError
	if !errors.As(err, &hce) {
		t.Errorf("expected HandlerCallError, got %T: %v", err, err)
	}

	// Actor 应仍存活（panic 仅影响单次调用）
	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("actor should survive handler panic, expected 1 actor, got %d", count)
	}
}

// TestActorConcurrentCalls 测试并发 Call 的串行化保证：
// 100 个 goroutine 同时 Call 同一 Actor，结果应为 1..100 不重不丢。
func TestActorConcurrentCalls(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "concurrent"}
	if err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	testutil.Settle()

	ctx := context.Background()
	const N = 100
	var wg sync.WaitGroup
	results := make([]int, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
			if err != nil {
				t.Errorf("concurrent Call %d failed: %v", idx, err)
				return
			}
			results[idx] = reply.Result
		}(i)
	}
	wg.Wait()

	// 验证串行化：100 次 Call 各 +1，结果集应为 {1,2,...,100}
	seen := make(map[int]bool, N)
	for _, r := range results {
		if r < 1 || r > N {
			t.Errorf("result %d out of range [1, %d]", r, N)
		}
		if seen[r] {
			t.Errorf("duplicate result %d — actor serialization broken", r)
		}
		seen[r] = true
	}
	if len(seen) != N {
		t.Errorf("expected %d unique results, got %d", N, len(seen))
	}
}

// TestActorGroupNotFound 测试向未注册 Group 发送消息时的错误处理。
func TestActorGroupNotFound(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	// 不注册任何 Group，所有操作应返回 GroupNotFoundError

	testId := TestActorId{ServerId: 1, OpenId: "no_group"}

	// Post 应失败
	err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	if err == nil {
		t.Error("expected error when posting to unregistered group")
	}
	var gnf *actor.GroupNotFoundError
	if !errors.As(err, &gnf) {
		t.Errorf("expected GroupNotFoundError, got %T: %v", err, err)
	}

	// Call 也应失败
	ctx := context.Background()
	_, err = actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error when calling unregistered group")
	}

	// Count 应返回错误
	_, err = actor.Count[TestActorId](mgr)
	if err == nil {
		t.Error("expected error when counting unregistered group")
	}

	// Broadcast 应返回 0
	n, _ := actor.Broadcast(mgr, &TestClose{})
	if n != 0 {
		t.Errorf("expected 0 broadcast hits for unregistered group, got %d", n)
	}

	// Multicast 应返回 0
	n, _ = actor.Multicast(mgr, []TestActorId{testId}, &TestClose{})
	if n != 0 {
		t.Errorf("expected 0 multicast hits for unregistered group, got %d", n)
	}
}

// TestActorEmptyMulticast 测试空 ID 列表的 Multicast 返回 (0, nil)。
func TestActorEmptyMulticast(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	hit, err := actor.Multicast(mgr, []TestActorId{}, &TestClose{})
	if err != nil {
		t.Errorf("expected no error for empty multicast, got: %v", err)
	}
	if hit != 0 {
		t.Errorf("expected 0 hits for empty multicast, got %d", hit)
	}
}

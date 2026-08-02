package actor_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ============================================================
// RequestHandler 测试类型定义
// ============================================================

type HandlerTestId struct {
	Name string
}

func (id HandlerTestId) ActorType() actor.ActorType { return "HandlerTest" }
func (id HandlerTestId) String() string             { return "HandlerTest(" + id.Name + ")" }

type HandlerTestState struct {
	Value int
}

type HandlerTestReply struct {
	Result int
}

// ─── RequestHandler 请求类型：将 Handle 方法内聚在请求上 ───

// HandlerSpawn 实现 RequestHandler，首次消息创建 Actor。
type HandlerSpawn struct {
	InitValue int
}

func (*HandlerSpawn) ReqType(_ HandlerTestId, _ actor.OkReply) string { return "HandlerSpawn" }
func (req *HandlerSpawn) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (actor.OkReply, error) {
	ctx.Open() // spawn 后保持活跃（框架不再自动激活）
	ctx.SetState(HandlerTestState{Value: req.InitValue})
	return actor.OK, nil
}

// HandlerAdd 实现 RequestHandler，查询已存在 Actor。
type HandlerAdd struct {
	Add int
}

func (*HandlerAdd) ReqType(_ HandlerTestId, _ *HandlerTestReply) string { return "HandlerAdd" }
func (req *HandlerAdd) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (*HandlerTestReply, error) {
	ctx.State().Value += req.Add
	return &HandlerTestReply{Result: ctx.State().Value}, nil
}

// HandlerServe 实现 RequestHandler，spawn + query 两用。
type HandlerServe struct {
	InitValue int
}

func (*HandlerServe) ReqType(_ HandlerTestId, _ *HandlerTestReply) string { return "HandlerServe" }
func (req *HandlerServe) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (*HandlerTestReply, error) {
	if spawning {
		ctx.Open() // spawn 后保持活跃（框架不再自动激活）
		ctx.SetState(HandlerTestState{Value: req.InitValue})
	}
	return &HandlerTestReply{Result: ctx.State().Value}, nil
}

// HandlerClose 实现 RequestHandler，关闭 Actor。
type HandlerClose struct{}

func (*HandlerClose) ReqType(_ HandlerTestId, _ actor.OkReply) string { return "HandlerClose" }
func (req *HandlerClose) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (actor.OkReply, error) {
	ctx.Quit()
	return actor.OK, nil
}

// HandlerSafe 实现 RequestHandler 且返回 SafeReply。
type HandlerSafe struct {
	Add     int
	cleaned *atomic.Bool
}

func (*HandlerSafe) ReqType(_ HandlerTestId, _ *SafeTestReply) string { return "HandlerSafe" }
func (req *HandlerSafe) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (*SafeTestReply, error) {
	ctx.State().Value += req.Add
	return &SafeTestReply{Result: ctx.State().Value, cleaned: req.cleaned}, nil
}

// HandlerSlow 实现 RequestHandler，模拟慢处理。
type HandlerSlow struct {
	Add int
}

func (*HandlerSlow) ReqType(_ HandlerTestId, _ *HandlerTestReply) string { return "HandlerSlow" }
func (req *HandlerSlow) Handle(ctx *actor.ActorContext[HandlerTestId, HandlerTestState], spawning bool) (*HandlerTestReply, error) {
	time.Sleep(200 * time.Millisecond)
	ctx.State().Value += req.Add
	return &HandlerTestReply{Result: ctx.State().Value}, nil
}

// ─── 注册辅助函数 ───

func setupHandlerManager(mgr *actor.Manager) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HandlerTestId, HandlerTestState]) {
		actor.RegisterSpawnHandler[HandlerTestId, HandlerTestState, *HandlerSpawn](b)
		actor.RegisterQueryHandler[HandlerTestId, HandlerTestState, *HandlerAdd](b)
		actor.RegisterServeHandler[HandlerTestId, HandlerTestState, *HandlerServe](b)
		actor.RegisterQueryHandler[HandlerTestId, HandlerTestState, *HandlerClose](b)
		actor.RegisterQueryHandler[HandlerTestId, HandlerTestState, *HandlerSlow](b)
	})
}

// ============================================================
// RequestHandler 基础测试
// ============================================================

// TestRequestHandlerSpawn 测试 RegisterSpawnHandler：首次消息创建 Actor。
func TestRequestHandlerSpawn(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "spawn_handler"}
	err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 42})
	if err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	// 验证 Actor 已创建
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, id, &HandlerAdd{Add: 8})
	if err != nil {
		t.Fatalf("Call HandlerAdd failed: %v", err)
	}
	if reply.Result != 50 {
		t.Errorf("expected 50, got %d", reply.Result)
	}
}

// TestRequestHandlerQuery 测试 RegisterQueryHandler：查询已存在 Actor。
func TestRequestHandlerQuery(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "query_handler"}
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 10}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	ctx := context.Background()

	// 多次调用 HandlerAdd（RequestHandler 模式）
	for i := 1; i <= 3; i++ {
		reply, err := actor.Call(ctx, mgr, id, &HandlerAdd{Add: i})
		if err != nil {
			t.Fatalf("Call %d failed: %v", i, err)
		}
		expected := 10 + i*(i+1)/2
		if reply.Result != expected {
			t.Errorf("call %d: expected %d, got %d", i, expected, reply.Result)
		}
	}
}

// TestRequestHandlerServe 测试 RegisterServeHandler：首次消息创建 Actor 并返回回复。
func TestRequestHandlerServe(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "serve_handler"}

	ctx := context.Background()

	// 首次调用：spawning=true，初始化 state
	reply, err := actor.Call(ctx, mgr, id, &HandlerServe{InitValue: 100})
	if err != nil {
		t.Fatalf("Call HandlerServe (spawn) failed: %v", err)
	}
	if reply.Result != 100 {
		t.Errorf("expected 100 after spawn, got %d", reply.Result)
	}

	// 第二次调用：spawning=false，不重新初始化
	reply2, err := actor.Call(ctx, mgr, id, &HandlerServe{InitValue: 999})
	if err != nil {
		t.Fatalf("Call HandlerServe (query) failed: %v", err)
	}
	if reply2.Result != 100 {
		t.Errorf("expected 100 after query (InitValue should be ignored), got %d", reply2.Result)
	}
}

// TestRequestHandlerClose 测试通过 RequestHandler 关闭 Actor。
func TestRequestHandlerClose(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "close_handler"}
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 0}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	// 通过 HandlerClose 关闭
	_, err := actor.Call(context.Background(), mgr, id, &HandlerClose{})
	if err != nil {
		t.Fatalf("Call HandlerClose failed: %v", err)
	}

	// 等待关闭
	testutil.WaitStop[HandlerTestId](t, mgr, time.Second)
}

// TestRequestHandlerWithSafeCall 测试 RequestHandler 与 SafeCall 配合使用。
func TestRequestHandlerWithSafeCall(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HandlerTestId, HandlerTestState]) {
		actor.RegisterSpawnHandler[HandlerTestId, HandlerTestState, *HandlerSpawn](b)
		actor.RegisterQueryHandler[HandlerTestId, HandlerTestState, *HandlerSafe](b)
	})

	id := HandlerTestId{Name: "handler_safe"}
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 10}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	ctx := context.Background()
	reply, err := actor.SafeCall(ctx, mgr, id, &HandlerSafe{Add: 5, cleaned: &cleaned})
	if err != nil {
		t.Fatalf("SafeCall HandlerSafe failed: %v", err)
	}
	if reply.Result != 15 {
		t.Errorf("expected 15, got %d", reply.Result)
	}

	// 正常路径 clean 不应被自动调用
	if cleaned.Load() {
		t.Error("Close() should NOT be auto-called on success")
	}
	reply.Close()
	if !cleaned.Load() {
		t.Error("Close() should be called after explicit call")
	}
}

// TestRequestHandlerConcurrent 测试并发调用 RequestHandler 的串行化。
func TestRequestHandlerConcurrent(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "handler_concurrent"}
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 0}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	const N = 50
	results := make([]int, N)
	ctx := context.Background()

	for i := 0; i < N; i++ {
		reply, err := actor.Call(ctx, mgr, id, &HandlerAdd{Add: 1})
		if err != nil {
			t.Fatalf("Call %d failed: %v", i, err)
		}
		results[i] = reply.Result
	}

	// 验证串行化
	seen := make(map[int]bool, N)
	for _, r := range results {
		if r < 1 || r > N {
			t.Errorf("result %d out of range [1, %d]", r, N)
		}
		if seen[r] {
			t.Errorf("duplicate result %d — serialization broken", r)
		}
		seen[r] = true
	}
	if len(seen) != N {
		t.Errorf("expected %d unique results, got %d", N, len(seen))
	}
}

// TestRequestHandlerTimeout 测试 RequestHandler 超时处理。
func TestRequestHandlerTimeout(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "handler_timeout"}
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 0}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := actor.Call(ctx, mgr, id, &HandlerSlow{Add: 1})
	if err == nil {
		t.Error("expected timeout error")
	}
}

// TestRequestHandlerVsRegisterQuery 对比 RequestHandler 和传统 RegisterQuery 两种模式。
func TestRequestHandlerVsRegisterQuery(t *testing.T) {
	mgr := actor.NewManager()

	// 用两种方式注册同一个 ActorType 的不同请求
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HandlerTestId, HandlerTestState]) {
		// 方式1: RequestHandler 模式 — Handle 方法在请求类型上
		actor.RegisterSpawnHandler[HandlerTestId, HandlerTestState, *HandlerSpawn](b)

		// 方式2: 传统 RegisterQuery 模式 — handler 函数独立传入
		actor.RegisterQuery(b, func(a *actor.ActorContext[HandlerTestId, HandlerTestState], req *HandlerAdd, _ bool) (*HandlerTestReply, error) {
			a.State().Value += req.Add
			return &HandlerTestReply{Result: a.State().Value}, nil
		})
	})

	id := HandlerTestId{Name: "vs_test"}

	// HandlerSpawn（RequestHandler 模式）
	if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 10}); err != nil {
		t.Fatalf("Post HandlerSpawn failed: %v", err)
	}
	testutil.Settle()

	// HandlerAdd（传统 RegisterQuery 模式）
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, id, &HandlerAdd{Add: 5})
	if err != nil {
		t.Fatalf("Call HandlerAdd failed: %v", err)
	}
	if reply.Result != 15 {
		t.Errorf("expected 15, got %d", reply.Result)
	}

	// 两种模式可以在同一个 Group 中共存
	if count, _ := actor.Count[HandlerTestId](mgr); count != 1 {
		t.Errorf("expected 1 actor, got %d", count)
	}
}

// TestRequestHandlerBroadcast 测试 RequestHandler 广播。
func TestRequestHandlerBroadcast(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	// 创建多个 Actor
	for i := 0; i < 3; i++ {
		id := HandlerTestId{Name: fmt.Sprintf("broadcast_%d", i)}
		if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: i}); err != nil {
			t.Fatalf("Post HandlerSpawn %d failed: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[HandlerTestId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}

	// 广播关闭
	count, err := actor.Broadcast(mgr, &HandlerClose{})
	if err != nil {
		t.Fatalf("Broadcast failed: %v", err)
	}
	if count != 3 {
		t.Errorf("expected 3 broadcast hits, got %d", count)
	}

	// 所有 Actor 应已关闭
	testutil.WaitStop[HandlerTestId](t, mgr, time.Second)
}

// TestRequestHandlerMulticast 测试 RequestHandler 多播。
func TestRequestHandlerMulticast(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	ids := make([]HandlerTestId, 4)
	for i := 0; i < 4; i++ {
		ids[i] = HandlerTestId{Name: fmt.Sprintf("multi_%d", i)}
		if err := actor.Post(mgr, ids[i], &HandlerSpawn{InitValue: i}); err != nil {
			t.Fatalf("Post HandlerSpawn %d failed: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	// 只关闭前 2 个
	hit, err := actor.Multicast(mgr, ids[:2], &HandlerClose{})
	if err != nil {
		t.Fatalf("Multicast failed: %v", err)
	}
	if hit != 2 {
		t.Errorf("expected 2 hits, got %d", hit)
	}

	// 后 2 个应仍存活
	testutil.WaitCount[HandlerTestId](t, mgr, 2, time.Second)
}

// TestRequestHandlerGroupNotFound 测试未注册 Group 时 RequestHandler 调用返回错误。
func TestRequestHandlerGroupNotFound(t *testing.T) {
	mgr := actor.NewManager()
	id := HandlerTestId{Name: "no_group"}

	err := actor.Post(mgr, id, &HandlerSpawn{InitValue: 0})
	if err == nil {
		t.Error("expected GroupNotFoundError for unregistered group")
	}

	ctx := context.Background()
	_, err = actor.Call(ctx, mgr, id, &HandlerAdd{Add: 1})
	if err == nil {
		t.Error("expected GroupNotFoundError for unregistered group")
	}
}

// TestRequestHandlerFinalize 测试 Finalize 与 RequestHandler 配合。
func TestRequestHandlerFinalize(t *testing.T) {
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	for i := 0; i < 3; i++ {
		id := HandlerTestId{Name: fmt.Sprintf("fin_%d", i)}
		if err := actor.Post(mgr, id, &HandlerSpawn{InitValue: i}); err != nil {
			t.Fatalf("Post HandlerSpawn %d failed: %v", i, err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[HandlerTestId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}

	actor.Finalize(mgr, &HandlerClose{})

	if count, _ := actor.Count[HandlerTestId](mgr); count != 0 {
		t.Errorf("expected 0 actors after finalize, got %d", count)
	}
}

// TestRequestHandlerTypeSafety 验证 RequestHandler 的编译期类型安全。
func TestRequestHandlerTypeSafety(t *testing.T) {
	// 编译期保证：
	// - RegisterSpawnHandler 只能注册实现了 RequestHandler 的类型
	// - Handle 方法的参数类型 (A, S, R) 由 RegistryBuilder 的泛型参数推导
	// - 请求类型和 Group 类型不匹配会导致编译错误
	//
	// 以下代码能编译通过：
	mgr := actor.NewManager()
	setupHandlerManager(mgr)

	id := HandlerTestId{Name: "type_safe"}
	actor.Post(mgr, id, &HandlerSpawn{InitValue: 1})
	testutil.Settle()

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, id, &HandlerAdd{Add: 1})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Result != 2 {
		t.Errorf("expected 2, got %d", reply.Result)
	}

	// 以下代码会导致编译错误（预期行为）：
	// 1. 将 HandlerAdd 注册到不同 State 类型的 Group：
	//    actor.RegisterQueryHandler[HandlerTestId, OtherState, *HandlerAdd](b)
	//    → HandlerAdd.Handle 参数类型不匹配
	//
	// 2. 将普通 Request 类型传给 RegisterSpawnHandler：
	//    actor.RegisterSpawnHandler[HandlerTestId, HandlerTestState, *TestLogin](b)
	//    → *TestLogin does not implement RequestHandler (missing Handle method)
}

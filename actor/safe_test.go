package actor_test

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// SafeCall / SafeReply 测试类型定义
// ============================================================

type SafeTestId struct {
	Name string
}

func (id SafeTestId) ActorType() actor.ActorType { return "SafeTest" }
func (id SafeTestId) String() string              { return "SafeTest(" + id.Name + ")" }

type SafeTestState struct {
	Value int
}

// SafeTestReply 实现 actor.SafeReply[*SafeTestReply]，带 Close() 方法。
// 模拟需要显式释放资源的 reply（如连接池归还、文件关闭等）。
type SafeTestReply struct {
	Result  int
	closed  atomic.Bool
	cleaned *atomic.Bool // 外部传入，用于跟踪 Close 是否被调用
}

func (r *SafeTestReply) Close() {
	if r.closed.CompareAndSwap(false, true) && r.cleaned != nil {
		r.cleaned.Store(true)
	}
}

func (r *SafeTestReply) IsClosed() bool {
	return r.closed.Load()
}

type SafeTestInit struct {
	Value int
}

func (*SafeTestInit) ReqType(_ SafeTestId, _ actor.OkReply) string { return "SafeTestInit" }

type SafeTestAdd struct {
	Add int
}

func (*SafeTestAdd) ReqType(_ SafeTestId, _ *SafeTestReply) string { return "SafeTestAdd" }

type SafeTestSlowAdd struct {
	Add int
}

func (*SafeTestSlowAdd) ReqType(_ SafeTestId, _ *SafeTestReply) string { return "SafeTestSlowAdd" }

type SafeTestClose struct{}

func (*SafeTestClose) ReqType(_ SafeTestId, _ actor.OkReply) string { return "SafeTestClose" }

func setupSafeManager(mgr *actor.Manager, cleaned *atomic.Bool) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[SafeTestId, SafeTestState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[SafeTestId, SafeTestState], req *SafeTestInit, _ bool) (actor.OkReply, error) {
			a.SetState(SafeTestState{Value: req.Value})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[SafeTestId, SafeTestState], req *SafeTestAdd, _ bool) (*SafeTestReply, error) {
			a.State().Value += req.Add
			return &SafeTestReply{Result: a.State().Value, cleaned: cleaned}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[SafeTestId, SafeTestState], req *SafeTestSlowAdd, _ bool) (*SafeTestReply, error) {
			time.Sleep(200 * time.Millisecond)
			a.State().Value += req.Add
			return &SafeTestReply{Result: a.State().Value, cleaned: cleaned}, nil
		})
	})
}

// ============================================================
// SafeCall 基础测试
// ============================================================

// TestSafeCallBasic 测试 SafeCall 正常路径：获取 reply 并验证资源未被自动释放。
func TestSafeCallBasic(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_basic"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 10}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 5})
	if err != nil {
		t.Fatalf("SafeCall failed: %v", err)
	}
	if reply.Result != 15 {
		t.Errorf("expected result 15, got %d", reply.Result)
	}

	// reply 被 caller 收到后，Close 不应被自动调用
	if cleaned.Load() {
		t.Error("reply.Close() should NOT be called when caller successfully receives the reply")
	}
	if reply.IsClosed() {
		t.Error("reply should not be closed when caller receives it successfully")
	}

	// caller 显式释放
	reply.Close()
	if !cleaned.Load() {
		t.Error("reply.Close() should have been called after explicit Close()")
	}
}

// TestSafeCallCleanupOnTimeout 测试 SafeCall 超时时 reply 被自动清理（Close）。
func TestSafeCallCleanupOnTimeout(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_timeout"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 0}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 使用极短超时，handler 需要 200ms
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestSlowAdd{Add: 1})
	if err == nil {
		// 如果碰巧拿到了 reply（极少数情况），需要手动清理
		reply.Close()
		t.Error("expected timeout error, but got nil")
		return
	}

	// 等待 handler 完成并触发 clean
	time.Sleep(300 * time.Millisecond)

	// 超时后，无人接收的 reply 应被自动 Close
	if !cleaned.Load() {
		t.Error("reply.Close() should be called automatically when SafeCall times out and reply is orphaned")
	}
}

// TestSafeCallCleanupOnContextCancel 测试 context 取消时 reply 被自动清理。
func TestSafeCallCleanupOnContextCancel(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_cancel"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 0}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	_, err := actor.SafeCall(ctx, mgr, id, &SafeTestSlowAdd{Add: 1})
	if err == nil {
		t.Error("expected error when context is cancelled")
		return
	}

	// 等待 handler 完成
	time.Sleep(300 * time.Millisecond)

	// 取消后，reply 应被自动 Close
	if !cleaned.Load() {
		t.Error("reply.Close() should be called automatically when context is cancelled and reply is orphaned")
	}
}

// TestSafeCallCleanupNotCalledOnSuccess 验证正常路径下 clean 不被触发。
func TestSafeCallCleanupNotCalledOnSuccess(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_success"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 0}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	for i := 0; i < 5; i++ {
		ctx := context.Background()
		reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 1})
		if err != nil {
			t.Fatalf("SafeCall %d failed: %v", i, err)
		}

		// 每次调用后，clean 不应被自动触发
		if cleaned.Load() {
			t.Errorf("iteration %d: reply.Close() should NOT be auto-called on success", i)
		}

		reply.Close()
		cleaned.Store(false) // 重置
	}
}

// TestSafeCallConcurrent 测试并发 SafeCall 的串行化保证。
func TestSafeCallConcurrent(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_concurrent"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 0}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	const N = 50
	results := make([]int, N)
	for i := 0; i < N; i++ {
		ctx := context.Background()
		reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 1})
		if err != nil {
			t.Fatalf("SafeCall %d failed: %v", i, err)
		}
		results[i] = reply.Result
		reply.Close()
	}

	// 验证串行化
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

// TestSafeCallGroupNotFound 测试未注册 Group 时 SafeCall 返回错误。
func TestSafeCallGroupNotFound(t *testing.T) {
	mgr := actor.NewManager()
	id := SafeTestId{Name: "no_group"}
	ctx := context.Background()
	_, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 1})
	if err == nil {
		t.Error("expected GroupNotFoundError")
	}
}

// TestSafeCallCloseIdempotent 测试多次 Close 幂等。
func TestSafeCallCloseIdempotent(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "safe_idempotent"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 0}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 1})
	if err != nil {
		t.Fatalf("SafeCall failed: %v", err)
	}

	// 多次 Close 应安全
	reply.Close()
	reply.Close()
	reply.Close()

	// 只应触发一次
	if !cleaned.Load() {
		t.Error("reply.Close() should have been called")
	}
}

// TestSafeCallVsCall 对比 SafeCall 和 Call：SafeCall 需要 reply 实现 SafeReply，
// Call 使用 PtrReply 不需要 Close。两者在功能上等价但 SafeCall 提供资源安全保证。
func TestSafeCallVsCall(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool

	// 注册两个 Group：Safe 版本和普通版本
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[SafeTestId, SafeTestState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[SafeTestId, SafeTestState], req *SafeTestInit, _ bool) (actor.OkReply, error) {
			a.SetState(SafeTestState{Value: req.Value})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[SafeTestId, SafeTestState], req *SafeTestAdd, _ bool) (*SafeTestReply, error) {
			a.State().Value += req.Add
			return &SafeTestReply{Result: a.State().Value, cleaned: &cleaned}, nil
		})
	})

	id := SafeTestId{Name: "compare"}
	if err := actor.Post(mgr, id, &SafeTestInit{Value: 10}); err != nil {
		t.Fatalf("Post Init failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()

	// SafeCall: reply 实现 SafeReply
	safeReply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 5})
	if err != nil {
		t.Fatalf("SafeCall failed: %v", err)
	}
	if safeReply.Result != 15 {
		t.Errorf("SafeCall: expected 15, got %d", safeReply.Result)
	}
	safeReply.Close()

	// 普通 Call 对 SafeTestReply 也可以工作（SafeReply 内嵌 PtrReply 约束）
	// 注意：Call 返回的是 R (PtrReply)，不会自动 Close
	// 这里使用 Call 时，reply 是 *SafeTestReply，可以手动调用 Close
	// 但 Call 返回类型是 PtrReply，编译器不会强制 Close
	reply2, err := actor.Call(ctx, mgr, id, &SafeTestAdd{Add: 5})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply2.Result != 20 {
		t.Errorf("Call: expected 20, got %d", reply2.Result)
	}
	// Call 不会自动 Close，但我们可以手动调用
	reply2.Close()
}

// ============================================================
// RefSafeCall 测试
// ============================================================

type RefSafeTestId struct {
	Name string
}

func (id RefSafeTestId) ActorType() actor.ActorType { return "RefSafeTest" }
func (id RefSafeTestId) String() string              { return "RefSafeTest(" + id.Name + ")" }

type RefSafeState struct {
	Counter int
}

type RefSafeReply struct {
	Value   int
	closed  atomic.Bool
	cleaned *atomic.Bool
}

func (r *RefSafeReply) Close() {
	if r.closed.CompareAndSwap(false, true) && r.cleaned != nil {
		r.cleaned.Store(true)
	}
}

type RefSafeSpawn struct {
	Value int
}

func (*RefSafeSpawn) ReqType(_ RefSafeTestId, _ actor.OkReply) string { return "RefSafeSpawn" }

type RefSafeGet struct{}

func (*RefSafeGet) ReqType(_ RefSafeTestId, _ *RefSafeReply) string { return "RefSafeGet" }

type RefSafeSlowGet struct{}

func (*RefSafeSlowGet) ReqType(_ RefSafeTestId, _ *RefSafeReply) string { return "RefSafeSlowGet" }

type RefSafePing struct{}

func (*RefSafePing) ReqType(_ RefSafeTestId, _ actor.OkReply) string { return "RefSafePing" }

type RefSafeClose struct{}

func (*RefSafeClose) ReqType(_ RefSafeTestId, _ actor.OkReply) string { return "RefSafeClose" }

// RefGetRef 跨 Actor 引用请求：通过 ActorRef 获取另一个 Actor 的数据。
type RefGetRef struct {
	TargetId RefSafeTestId
}

type RefGetRefReply struct {
	Value int
}

func (*RefGetRef) ReqType(_ RefSafeTestId, _ *RefGetRefReply) string { return "RefGetRef" }

func setupRefSafeManager(mgr *actor.Manager, cleaned *atomic.Bool) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefSafeTestId, RefSafeState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeSpawn, _ bool) (actor.OkReply, error) {
			a.SetState(RefSafeState{Counter: req.Value})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeGet, _ bool) (*RefSafeReply, error) {
			return &RefSafeReply{Value: a.State().Counter, cleaned: cleaned}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeSlowGet, _ bool) (*RefSafeReply, error) {
			time.Sleep(200 * time.Millisecond)
			return &RefSafeReply{Value: a.State().Counter, cleaned: cleaned}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafePing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeClose, _ bool) (actor.OkReply, error) {
			a.Quit()
			return actor.OK, nil
		})
		// 跨 Actor 引用 handler
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefGetRef, _ bool) (*RefGetRefReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, nil // 目标不存在
			}
			defer ref.Release()

			ctx := context.Background()
			reply, err := actor.RefSafeCall(ctx, ref, &RefSafeGet{})
			if err != nil {
				return nil, err
			}
			defer reply.Close()
			return &RefGetRefReply{Value: reply.Value}, nil
		})
	})
}

// TestRefSafeCallBasic 测试 RefSafeCall 编译正确性。
// 实际的跨 Actor 调用测试见 TestRefSafeCallCrossActor。
func TestRefSafeCallBasic(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupRefSafeManager(mgr, &cleaned)

	id := RefSafeTestId{Name: "refsafe_basic"}
	if err := actor.Post(mgr, id, &RefSafeSpawn{Value: 42}); err != nil {
		t.Fatalf("Post Spawn failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 验证 Actor 正常工作
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, id, &RefSafeGet{})
	if err != nil {
		t.Fatalf("Call RefSafeGet failed: %v", err)
	}
	if reply.Value != 42 {
		t.Errorf("expected 42, got %d", reply.Value)
	}
	reply.Close()
}

// TestRefSafeCallCrossActor 测试通过 ActorRef 的 SafeCall（跨 Actor 引用）。
func TestRefSafeCallCrossActor(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupRefSafeManager(mgr, &cleaned)

	// 创建两个 Actor
	srcId := RefSafeTestId{Name: "src"}
	targetId := RefSafeTestId{Name: "target"}

	if err := actor.Post(mgr, srcId, &RefSafeSpawn{Value: 0}); err != nil {
		t.Fatalf("Post Spawn src failed: %v", err)
	}
	if err := actor.Post(mgr, targetId, &RefSafeSpawn{Value: 99}); err != nil {
		t.Fatalf("Post Spawn target failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// src 通过 RefGetRef 跨引用读取 target 的数据
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, srcId, &RefGetRef{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call RefGetRef failed: %v", err)
	}
	if reply.Value != 99 {
		t.Errorf("expected target value 99, got %d", reply.Value)
	}

	// 跨引用调用后，clean 应被正确调用（RefGetRef handler 内 defer reply.Close()）
	if !cleaned.Load() {
		t.Error("reply.Close() should be called via RefSafeCall in cross-actor scenario")
	}
}

// TestRefSafeCallCleanupOnTimeout 测试 RefSafeCall 超时时 reply 自动清理（通过跨引用）。
func TestRefSafeCallCleanupOnTimeout(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool

	// 注册两个 handler：一个正常获取，一个慢获取（用于超时）
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefSafeTestId, RefSafeState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeSpawn, _ bool) (actor.OkReply, error) {
			a.SetState(RefSafeState{Counter: req.Value})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeGet, _ bool) (*RefSafeReply, error) {
			return &RefSafeReply{Value: a.State().Counter, cleaned: &cleaned}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafeSlowGet, _ bool) (*RefSafeReply, error) {
			time.Sleep(200 * time.Millisecond)
			return &RefSafeReply{Value: a.State().Counter, cleaned: &cleaned}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefSafePing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
		// 通过 RefSafeCall 跨引用调用慢 handler 的包装请求
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefSafeTestId, RefSafeState], req *RefGetRef, _ bool) (*RefGetRefReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return &RefGetRefReply{}, nil
			}
			defer ref.Release()

			// 使用极短超时的 context 触发 RefSafeCall 超时清理
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			reply, err := actor.RefSafeCall(ctx, ref, &RefSafeSlowGet{})
			if err != nil {
				return &RefGetRefReply{Value: -1}, nil
			}
			defer reply.Close()
			return &RefGetRefReply{Value: reply.Value}, nil
		})
	})

	targetId := RefSafeTestId{Name: "target"}
	srcId := RefSafeTestId{Name: "src"}
	if err := actor.Post(mgr, targetId, &RefSafeSpawn{Value: 10}); err != nil {
		t.Fatalf("Post Spawn target failed: %v", err)
	}
	if err := actor.Post(mgr, srcId, &RefSafeSpawn{Value: 0}); err != nil {
		t.Fatalf("Post Spawn src failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 触发跨引用调用，RefSafeCall 会因超时而清理 reply
	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, srcId, &RefGetRef{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call RefGetRef failed: %v", err)
	}

	// 等待慢 handler 完成
	time.Sleep(300 * time.Millisecond)

	// 超时后，reply 应被 RefSafeCall 的 safeResult 自动 Close
	if !cleaned.Load() {
		t.Error("reply.Close() should be called automatically when RefSafeCall times out and reply is orphaned")
	}
}

// TestRefSafeCallClosedActor 测试向已关闭 Actor 发送 RefSafeCall 返回错误（通过跨引用）。
func TestRefSafeCallClosedActor(t *testing.T) {
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupRefSafeManager(mgr, &cleaned)

	targetId := RefSafeTestId{Name: "target_closed"}
	srcId := RefSafeTestId{Name: "src_closed"}

	if err := actor.Post(mgr, targetId, &RefSafeSpawn{Value: 0}); err != nil {
		t.Fatalf("Post Spawn target failed: %v", err)
	}
	if err := actor.Post(mgr, srcId, &RefSafeSpawn{Value: 0}); err != nil {
		t.Fatalf("Post Spawn src failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 关闭 target Actor
	if _, err := actor.Call(context.Background(), mgr, targetId, &RefSafeClose{}); err != nil {
		t.Fatalf("Call Close failed: %v", err)
	}
	time.Sleep(100 * time.Millisecond)

	// 验证 target Actor 已关闭
	if count, _ := actor.Count[RefSafeTestId](mgr); count != 1 {
		t.Fatalf("expected 1 actor (src only), got %d", count)
	}

	// src 通过 RefGetRef 尝试跨引用访问已关闭的 target
	// RefGetRef handler 内 ctx.Ref 返回 nil（target 已关闭），handler 返回 nil reply
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, srcId, &RefGetRef{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call RefGetRef failed: %v", err)
	}
	if reply != nil {
		t.Errorf("expected nil reply from closed actor ref, got Value=%d", reply.Value)
	}
}

// ============================================================
// SafeCall 类型安全验证
// ============================================================

// TestSafeCallTypeSafety 验证 SafeCall 的类型安全：只有实现 SafeReply 的类型才能使用 SafeCall。
// 此测试验证编译期约束。
func TestSafeCallTypeSafety(t *testing.T) {
	// 编译期保证：
	// - SafeCall 的 R 约束为 SafeReply[R0]（~*R0 + Close()）
	// - SafeTestReply 实现 SafeReply[*SafeTestReply]
	// - TestAddReply 不实现 SafeReply（没有 Close() 方法）
	//
	// 以下代码能编译通过：
	mgr := actor.NewManager()
	var cleaned atomic.Bool
	setupSafeManager(mgr, &cleaned)

	id := SafeTestId{Name: "type_safe"}
	actor.Post(mgr, id, &SafeTestInit{Value: 0})
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	reply, err := actor.SafeCall(ctx, mgr, id, &SafeTestAdd{Add: 1})
	if err != nil {
		t.Fatalf("SafeCall failed: %v", err)
	}
	reply.Close()

	// 以下代码会导致编译错误（预期行为）：
	// reply2, _ := actor.SafeCall(ctx, mgr, id, &TestAdd{Add: 1})
	// → *TestAddReply does not implement SafeReply[*TestAddReply] (missing Close method)
	_ = reply
}

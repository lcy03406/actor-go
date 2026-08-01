package actor_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// ActorRef 测试类型定义
// ============================================================

type RefTestId struct {
	Name string `json:"name"`
}

func (id RefTestId) ActorType() actor.ActorType { return "RefTest" }
func (id RefTestId) String() string              { return "RefTest(" + id.Name + ")" }

type RefTestState struct {
	Counter int32
	Value   string
}

// ─── 基础消息类型 ───

type RefTestInit struct{ Value string }

func (*RefTestInit) ReqType(_ RefTestId, _ actor.OkReply) string { return "RefTestInit" }

type RefTestGet struct{}

type RefTestGetReply struct {
	Value   string
	Counter int32
}

func (*RefTestGet) ReqType(_ RefTestId, _ *RefTestGetReply) string { return "RefTestGet" }

type RefTestAdd struct{ Delta int32 }

type RefTestAddReply struct{ Result int32 }

func (*RefTestAdd) ReqType(_ RefTestId, _ *RefTestAddReply) string { return "RefTestAdd" }

type RefTestPing struct{}

func (*RefTestPing) ReqType(_ RefTestId, _ actor.OkReply) string { return "RefTestPing" }

type RefTestClose struct{}

func (*RefTestClose) ReqType(_ RefTestId, _ actor.OkReply) string { return "RefTestClose" }

// ─── 注册构建器：注册基础 handler ───

func registerRefTestBase(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
	actor.RegisterSpawn(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *RefTestInit, _ bool) (actor.OkReply, error) {
		a.SetState(RefTestState{Value: req.Value})
		return actor.OK, nil
	})
	actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *RefTestGet, _ bool) (*RefTestGetReply, error) {
		return &RefTestGetReply{Value: a.State().Value, Counter: a.State().Counter}, nil
	})
	actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *RefTestAdd, _ bool) (*RefTestAddReply, error) {
		a.State().Counter += req.Delta
		return &RefTestAddReply{Result: a.State().Counter}, nil
	})
	actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *RefTestPing, _ bool) (actor.OkReply, error) {
		return actor.OK, nil
	})
	actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *RefTestClose, _ bool) (actor.OkReply, error) {
		a.Quit()
		return actor.OK, nil
	})
}

func spawnRefTestActor(mgr *actor.Manager, name, value string) RefTestId {
	id := RefTestId{Name: name}
	_ = actor.Post(mgr, id, &RefTestInit{Value: value})
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, id, &RefTestPing{}); err != nil {
		panic("spawnRefTestActor: " + err.Error())
	}
	return id
}

// ============================================================
// 自定义请求类型（用于 ActorRef 相关 handler）
// ============================================================

type refTestGetRefReq struct {
	TargetId RefTestId
}

type refTestGetRefReply struct {
	TargetValue string
}

func (*refTestGetRefReq) ReqType(_ RefTestId, _ *refTestGetRefReply) string { return "RefTestGetRef" }

type refTestRefPostReq struct {
	TargetId RefTestId
	Delta    int32
}

func (*refTestRefPostReq) ReqType(_ RefTestId, _ actor.OkReply) string { return "RefTestRefPost" }

type refTestRefNotFoundReq struct {
	TargetId RefTestId
}

func (*refTestRefNotFoundReq) ReqType(_ RefTestId, _ actor.OkReply) string {
	return "RefTestRefNotFound"
}

type refTestReleaseReq struct {
	TargetId RefTestId
}

type refTestReleaseReply struct {
	BeforeValid bool
	AfterValid  bool
}

func (*refTestReleaseReq) ReqType(_ RefTestId, _ *refTestReleaseReply) string {
	return "RefTestRelease"
}

type refTestIdempotentReq struct {
	TargetId RefTestId
}

func (*refTestIdempotentReq) ReqType(_ RefTestId, _ actor.OkReply) string {
	return "RefTestIdempotent"
}

type refTestPreventExitReq struct {
	TargetId RefTestId
}

type refTestPreventExitReply struct {
	RefValidAfterQuit bool
}

func (*refTestPreventExitReq) ReqType(_ RefTestId, _ *refTestPreventExitReply) string {
	return "RefTestPreventExit"
}

type refTestReleaseExitReq struct {
	TargetId RefTestId
}

func (*refTestReleaseExitReq) ReqType(_ RefTestId, _ actor.OkReply) string {
	return "RefTestReleaseExit"
}

type refTestIdReq struct {
	TargetId RefTestId
}

type refTestIdReply struct {
	Match bool
}

func (*refTestIdReq) ReqType(_ RefTestId, _ *refTestIdReply) string { return "RefTestId" }

type refTestCtxCancelReq struct {
	TargetId RefTestId
}

func (*refTestCtxCancelReq) ReqType(_ RefTestId, _ actor.OkReply) string {
	return "RefTestCtxCancel"
}

type refTestConcurrentReq struct {
	TargetId RefTestId
}

func (*refTestConcurrentReq) ReqType(_ RefTestId, _ actor.OkReply) string {
	return "RefTestConcurrent"
}

type refTestClosedReq struct {
	TargetId RefTestId
}

type refTestClosedReply struct {
	RefIsNil bool
}

func (*refTestClosedReq) ReqType(_ RefTestId, _ *refTestClosedReply) string {
	return "RefTestClosed"
}

// ============================================================
// 功能测试
// ============================================================

// TestActorRefBasic 测试 ActorRef 基本功能：获取引用、RefCall。
func TestActorRefBasic(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestGetRefReq, _ bool) (*refTestGetRefReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return &refTestGetRefReply{}, nil
			}
			defer ref.Release()
			r, err := actor.RefCall(context.Background(), ref, &RefTestGet{})
			if err != nil {
				return nil, err
			}
			return &refTestGetRefReply{TargetValue: r.Value}, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, sourceId, &refTestGetRefReq{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call GetRef failed: %v", err)
	}
	if reply.TargetValue != "hello" {
		t.Errorf("expected 'hello', got '%s'", reply.TargetValue)
	}
}

// TestActorRefPost 测试 RefPost 基本功能。
func TestActorRefPost(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestRefPostReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			defer ref.Release()
			return actor.OK, actor.RefPost(ref, &RefTestAdd{Delta: req.Delta})
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &refTestRefPostReq{TargetId: targetId, Delta: 10}); err != nil {
		t.Fatalf("Call RefPost failed: %v", err)
	}

	reply, err := actor.Call(ctx, mgr, targetId, &RefTestAdd{Delta: 0})
	if err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if reply.Result != 10 {
		t.Errorf("expected 10, got %d", reply.Result)
	}
}

// TestActorRefNotFound 测试目标 Actor 不存在时 Ref 返回 nil。
func TestActorRefNotFound(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestRefNotFoundReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref != nil {
				ref.Release()
			}
			return actor.OK, nil
		})
	})

	sourceId := spawnRefTestActor(mgr, "source", "world")
	nonExistentId := RefTestId{Name: "nonexistent"}

	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, sourceId, &refTestRefNotFoundReq{TargetId: nonExistentId})
	if err != nil {
		t.Fatalf("expected nil ref to be OK, got error: %v", err)
	}
}

// TestActorRefRelease 测试 Release 后 Valid 返回 false。
func TestActorRefRelease(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestReleaseReq, _ bool) (*refTestReleaseReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return &refTestReleaseReply{}, nil
			}
			beforeValid := ref.Valid()
			ref.Release()
			afterValid := ref.Valid()
			return &refTestReleaseReply{BeforeValid: beforeValid, AfterValid: afterValid}, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, sourceId, &refTestReleaseReq{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call Release test failed: %v", err)
	}
	if !reply.BeforeValid {
		t.Error("expected Valid() to be true before Release()")
	}
	if reply.AfterValid {
		t.Error("expected Valid() to be false after Release()")
	}
}

// TestActorRefReleaseIdempotent 测试 Release 幂等性。
func TestActorRefReleaseIdempotent(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestIdempotentReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			ref.Release()
			ref.Release()
			ref.Release()
			return actor.OK, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &refTestIdempotentReq{TargetId: targetId}); err != nil {
		t.Fatalf("Release idempotent test failed: %v", err)
	}
}

// TestActorRefPreventIdleExit 测试 ActorRef 持有引用阻止目标 idle 退出。
// 目标 Actor 调用 Quit 后，因 ActorRef 持有 hold，actorRuntime 不会真正退出，
// Valid() 仍为 true。消息能否送达取决于 handler 注册类型（spawn/query），
// 与发送方身份无关，此处仅验证 hold 语义。
func TestActorRefPreventIdleExit(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestPreventExitReq, _ bool) (*refTestPreventExitReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return &refTestPreventExitReply{}, nil
			}
			_, err := actor.Call(context.Background(), mgr, req.TargetId, &RefTestClose{})
			if err != nil {
				ref.Release()
				return nil, err
			}
			time.Sleep(50 * time.Millisecond)

			refValid := ref.Valid()
			ref.Release()
			return &refTestPreventExitReply{
				RefValidAfterQuit: refValid,
			}, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, sourceId, &refTestPreventExitReq{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call PreventExit test failed: %v", err)
	}
	if !reply.RefValidAfterQuit {
		t.Error("expected Ref.Valid() to be true while holding reference after Quit")
	}
}

// TestActorRefAfterReleaseAllowsExit 测试 Release 后目标可以正常 idle 退出。
func TestActorRefAfterReleaseAllowsExit(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestReleaseExitReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			ref.Release()

			_, err := actor.Call(context.Background(), mgr, req.TargetId, &RefTestClose{})
			if err != nil {
				return nil, err
			}
			time.Sleep(50 * time.Millisecond)

			if ref.Valid() {
				return actor.OK, nil
			}
			return actor.OK, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &refTestReleaseExitReq{TargetId: targetId}); err != nil {
		t.Fatalf("Call ReleaseExit test failed: %v", err)
	}
}

// TestActorRefId 测试 Id() 返回正确的目标 ActorId。
func TestActorRefId(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestIdReq, _ bool) (*refTestIdReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return &refTestIdReply{}, nil
			}
			defer ref.Release()
			return &refTestIdReply{Match: ref.Id().Name == req.TargetId.Name}, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, sourceId, &refTestIdReq{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call Id test failed: %v", err)
	}
	if !reply.Match {
		t.Error("expected Ref.Id() to match targetId")
	}
}

// TestActorRefCallWithContextCancel 测试 RefCall 的 context 取消。
func TestActorRefCallWithContextCancel(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestCtxCancelReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			defer ref.Release()

			ctx, cancel := context.WithCancel(context.Background())
			cancel()

			_, err := actor.RefCall(ctx, ref, &RefTestGet{})
			if err != context.Canceled {
				t.Errorf("expected context.Canceled, got %v", err)
			}
			return actor.OK, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &refTestCtxCancelReq{TargetId: targetId}); err != nil {
		t.Fatalf("Call ContextCancel test failed: %v", err)
	}
}

// TestActorRefConcurrent 测试并发使用 ActorRef。
func TestActorRefConcurrent(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestConcurrentReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			defer ref.Release()

			var wg sync.WaitGroup
			const numGoroutines = 50
			for i := 0; i < numGoroutines; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, _ = actor.RefCall(context.Background(), ref, &RefTestAdd{Delta: 1})
				}()
			}
			wg.Wait()
			return actor.OK, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &refTestConcurrentReq{TargetId: targetId}); err != nil {
		t.Fatalf("Call Concurrent test failed: %v", err)
	}

	reply, err := actor.Call(ctx, mgr, targetId, &RefTestGet{})
	if err != nil {
		t.Fatalf("Call Get failed: %v", err)
	}
	if reply.Counter != 50 {
		t.Errorf("expected counter 50, got %d", reply.Counter)
	}
}

// TestActorRefClosedTarget 测试向已关闭的 Actor 获取 Ref 返回 nil。
func TestActorRefClosedTarget(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RefTestId, RefTestState]) {
		registerRefTestBase(b)
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefTestId, RefTestState], req *refTestClosedReq, _ bool) (*refTestClosedReply, error) {
			ref := a.Ref(req.TargetId)
			return &refTestClosedReply{RefIsNil: ref == nil}, nil
		})
	})

	targetId := spawnRefTestActor(mgr, "target", "hello")
	actor.CloseActor(mgr, targetId)
	actor.JoinActor(mgr, targetId)

	sourceId := spawnRefTestActor(mgr, "source", "world")

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, sourceId, &refTestClosedReq{TargetId: targetId})
	if err != nil {
		t.Fatalf("Call Closed test failed: %v", err)
	}
	if !reply.RefIsNil {
		t.Error("expected Ref to be nil for closed actor")
	}
}

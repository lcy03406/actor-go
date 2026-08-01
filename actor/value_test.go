package actor_test

import (
	"context"
	"log/slog"
	"testing"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

// ctxValue 强类型 context 值的测试句柄。
var ctxValue = actor.DefineValue[string]()

// ── 探针请求：spawn 时从 actor ctx 读取挂载值，结果落包级变量供断言 ──

var (
	gotValue string
	gotOK    bool
)

type valueProbeReq struct{}

func (*valueProbeReq) ReqType(_ TestActorId, _ actor.OkReply) string { return "valueProbeReq" }
func (req *valueProbeReq) Handle(a *actor.ActorContext[TestActorId, TestActorData], spawning bool) (actor.OkReply, error) {
	if spawning {
		gotValue, gotOK = ctxValue.Get(a.Context())
	}
	return actor.OK, nil
}

// TestCtxValueRoundTrip 挂载后 actor 处理函数能经 Get 读到强类型值。
func TestCtxValueRoundTrip(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	actor.WithValue(mgr, ctxValue, "hello")
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *valueProbeReq](b)
	})

	if err := actor.Post(mgr, TestActorId{ServerId: 1, OpenId: "v"}, &valueProbeReq{}); err != nil {
		t.Fatalf("Post probe failed: %v", err)
	}
	testutil.Settle()

	if !gotOK || gotValue != "hello" {
		t.Fatalf("Get = (%q, %v), want (\"hello\", true)", gotValue, gotOK)
	}
}

// TestCtxValueNotBound 未挂载时 Get 返回零值与 false，而非 panic。
func TestCtxValueNotBound(t *testing.T) {
	v, ok := ctxValue.Get(context.Background())
	if ok || v != "" {
		t.Fatalf("Get = (%q, %v), want (\"\", false)", v, ok)
	}
}

// TestWithValueAfterServePanics Serve 之后挂载对已注册 Group 不可见，必须 panic。
func TestWithValueAfterServePanics(t *testing.T) {
	mgr := actor.NewManager(slog.Default())
	setupManager(mgr)

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected WithValue after Serve to panic")
		}
	}()
	actor.WithValue(mgr, ctxValue, "late")
}

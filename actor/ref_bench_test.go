package actor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// ActorRef vs Standard 对比性能测试
// 无争抢（单goroutine顺序）和有争抢（多goroutine并发同一Actor）
// ============================================================

type RefBenchId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id RefBenchId) ActorType() actor.ActorType { return "RefBench" }
func (id RefBenchId) String() string {
	if id.OpenId == "" {
		return fmt.Sprintf("RefBench(%d)", id.ServerId)
	}
	return fmt.Sprintf("RefBench(%d,%s)", id.ServerId, id.OpenId)
}

type RefBenchState struct{ N int }

type RefBenchAdd struct{ V int }

type RefBenchAddReply struct{ Result int }

func (*RefBenchAdd) ReqType(_ RefBenchId, _ *RefBenchAddReply) string { return "RefBenchAdd" }

type RefBenchPing struct{}

func (*RefBenchPing) ReqType(_ RefBenchId, _ actor.OkReply) string { return "RefBenchPing" }

type RefBenchLogin struct{ Init int }

func (*RefBenchLogin) ReqType(_ RefBenchId, _ actor.OkReply) string { return "RefBenchLogin" }

type refBenchAcquireReq struct {
	TargetId RefBenchId
}

type refBenchAcquireReply struct {
	Ref *actor.ActorRef[RefBenchId, RefBenchState]
}

func (*refBenchAcquireReq) ReqType(_ RefBenchId, _ *refBenchAcquireReply) string {
	return "RefBenchAcquire"
}

type refBenchCallReq struct {
	TargetId RefBenchId
}

type refBenchCallReply struct {
	Result int
}

func (*refBenchCallReq) ReqType(_ RefBenchId, _ *refBenchCallReply) string { return "RefBenchCall" }

type refBenchPostReq struct {
	TargetId RefBenchId
}

func (*refBenchPostReq) ReqType(_ RefBenchId, _ actor.OkReply) string { return "RefBenchPost" }

func setupRefBenchManager(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 1024}, func(b *actor.RegistryBuilder[RefBenchId, RefBenchState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *RefBenchLogin, _ bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *RefBenchAdd, _ bool) (*RefBenchAddReply, error) {
			a.State().N += req.V
			return &RefBenchAddReply{Result: a.State().N}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *RefBenchPing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *refBenchAcquireReq, _ bool) (*refBenchAcquireReply, error) {
			ref := a.Ref(req.TargetId)
			return &refBenchAcquireReply{Ref: ref}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *refBenchCallReq, _ bool) (*refBenchCallReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			defer ref.Release()
			r, err := actor.RefCall(context.Background(), ref, &RefBenchAdd{V: 1})
			if err != nil {
				return nil, err
			}
			return &refBenchCallReply{Result: r.Result}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[RefBenchId, RefBenchState], req *refBenchPostReq, _ bool) (actor.OkReply, error) {
			ref := a.Ref(req.TargetId)
			if ref == nil {
				return nil, &actor.ActorNotFoundError{}
			}
			defer ref.Release()
			return actor.OK, actor.RefPost(ref, &RefBenchPing{})
		})
	})
}

func refBenchSpawn(mgr *actor.Manager, serverId int) RefBenchId {
	id := RefBenchId{ServerId: serverId, OpenId: fmt.Sprintf("bench_%d", serverId)}
	_ = actor.Post(mgr, id, &RefBenchLogin{Init: 0})
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, id, &RefBenchPing{}); err != nil {
		panic(fmt.Sprintf("refBenchSpawn: %v", err))
	}
	return id
}

func acquireRef(mgr *actor.Manager, targetId RefBenchId) *actor.ActorRef[RefBenchId, RefBenchState] {
	sourceId := RefBenchId{ServerId: 99999, OpenId: "ref_acquirer"}
	_ = actor.Post(mgr, sourceId, &RefBenchLogin{Init: 0})
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, sourceId, &RefBenchPing{}); err != nil {
		panic(fmt.Sprintf("acquireRef ping: %v", err))
	}
	reply, err := actor.Call(ctx, mgr, sourceId, &refBenchAcquireReq{TargetId: targetId})
	if err != nil {
		panic(fmt.Sprintf("acquireRef: %v", err))
	}
	return reply.Ref
}

// ============================================================
// 无争抢：单 goroutine 顺序调用
// ============================================================

// BenchmarkCallNoContention 单 Actor 顺序 Call（对照组）。
func BenchmarkCallNoContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	id := refBenchSpawn(mgr, 1)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.Call(ctx, mgr, id, &RefBenchAdd{V: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkRefCallNoContention 通过持久的 ActorRef 顺序 Call。
func BenchmarkRefCallNoContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	ref := acquireRef(mgr, targetId)
	if ref == nil {
		b.Fatal("ref is nil")
	}
	defer ref.Release()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.RefCall(ctx, ref, &RefBenchAdd{V: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkPostNoContention 单 Actor 顺序 Post（对照组）。
func BenchmarkPostNoContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	id := refBenchSpawn(mgr, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = actor.Post(mgr, id, &RefBenchPing{})
	}
}

// BenchmarkRefPostNoContention 通过持久的 ActorRef 顺序 Post。
func BenchmarkRefPostNoContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	ref := acquireRef(mgr, targetId)
	if ref == nil {
		b.Fatal("ref is nil")
	}
	defer ref.Release()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = actor.RefPost(ref, &RefBenchPing{})
	}
}

// ============================================================
// 有争抢：多 goroutine 并发调用同一 Actor
// ============================================================

// BenchmarkCallWithContention 并发 Call 同一 Actor（对照组）。
func BenchmarkCallWithContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	id := refBenchSpawn(mgr, 1)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := actor.Call(ctx, mgr, id, &RefBenchAdd{V: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkRefCallWithContention 并发 RefCall 同一 Actor。
func BenchmarkRefCallWithContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	ref := acquireRef(mgr, targetId)
	if ref == nil {
		b.Fatal("ref is nil")
	}
	defer ref.Release()
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := actor.RefCall(ctx, ref, &RefBenchAdd{V: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkPostWithContention 并发 Post 同一 Actor（对照组）。
func BenchmarkPostWithContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	id := refBenchSpawn(mgr, 1)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = actor.Post(mgr, id, &RefBenchPing{})
		}
	})
}

// BenchmarkRefPostWithContention 并发 RefPost 同一 Actor。
func BenchmarkRefPostWithContention(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	ref := acquireRef(mgr, targetId)
	if ref == nil {
		b.Fatal("ref is nil")
	}
	defer ref.Release()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = actor.RefPost(ref, &RefBenchPing{})
		}
	})
}

// BenchmarkAcquireAndCall 完整流程：获取 Ref → RefCall → Release（模拟 handler 内临时引用）。
func BenchmarkAcquireAndCall(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	sourceId := refBenchSpawn(mgr, 2)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		reply, err := actor.Call(ctx, mgr, sourceId, &refBenchCallReq{TargetId: targetId})
		if err != nil {
			b.Fatal(err)
		}
		_ = reply
	}
}

// BenchmarkAcquireAndPost 完整流程：获取 Ref → RefPost → Release。
func BenchmarkAcquireAndPost(b *testing.B) {
	mgr := actor.NewManager()
	setupRefBenchManager(mgr)
	targetId := refBenchSpawn(mgr, 1)
	sourceId := refBenchSpawn(mgr, 2)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.Call(context.Background(), mgr, sourceId, &refBenchPostReq{TargetId: targetId}); err != nil {
			b.Fatal(err)
		}
	}
}

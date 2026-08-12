package actor_test

import (
	"context"
	"fmt"
	"sort"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// v2 性能测试
// 与 v1 同等条件下对比：单 Actor Call、Post、并发 Call、Broadcast、Spawn
// ============================================================

type BenchId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id BenchId) ActorType() actor.ActorType { return "BenchActorV2" }
func (id BenchId) String() string {
	if id.OpenId == "" {
		return fmt.Sprintf("BenchV2(%d)", id.ServerId)
	}
	return fmt.Sprintf("BenchV2(%d,%s)", id.ServerId, id.OpenId)
}

type BenchState struct {
	N int
}

type BenchAdd struct{ V int }
type BenchAddReply struct{ Result int }

func (*BenchAdd) ReqType(_ BenchId, _ *BenchAddReply) string { return "BenchAdd" }

// 用于 Post 的 OkReply request 类型（v2 Post 约束 Request[A, *Ok, Q0, Ok]）。
type BenchPing struct{}

func (*BenchPing) ReqType(_ BenchId, _ actor.OkReply) string { return "BenchPing" }

type BenchLogin struct{ Init int }

func (*BenchLogin) ReqType(_ BenchId, _ actor.OkReply) string { return "BenchLogin" }

// 用于 spawn + 返回回复 的类型。
type BenchLoginWithReply struct{ Init int }

func (*BenchLoginWithReply) ReqType(_ BenchId, _ *BenchAddReply) string { return "BenchLoginWithReply" }

var options = actor.Options{
	BufMails: 1024,
}

func setupBenchManagerV2(mgr *actor.Manager) {
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[BenchId, BenchState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchAdd, _ bool) (*BenchAddReply, error) {
			a.State().N += req.V
			return &BenchAddReply{Result: a.State().N}, nil
		})
		// Post 目标：OkReply query 类型
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
		// RegisterServe = spawn + query，用于 spawn benchmark
		actor.RegisterServe(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLoginWithReply, spawning bool) (*BenchAddReply, error) {
			a.State().N = req.Init
			return &BenchAddReply{Result: a.State().N}, nil
		})
	})
}

// 准备一个已 spawn 的 Actor，返回其 id。
// 用 Post 触发 spawn，再通过 Call 等待 spawn 完成（避免 time.Sleep）。
func benchSpawnV2(mgr *actor.Manager, serverId int) BenchId {
	id := BenchId{ServerId: serverId, OpenId: fmt.Sprintf("bench_%d", serverId)}
	_ = actor.Post(mgr, id, &BenchLogin{Init: 0})
	// 后续 Call 保证 spawn 的 Post 已被消费，即 actor 已就绪
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, id, &BenchPing{}); err != nil {
		panic(fmt.Sprintf("benchSpawnV2: %v", err))
	}
	return id
}

// BenchmarkV2Call 单 Actor 顺序 Call。
func BenchmarkV2Call(b *testing.B) {
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	id := benchSpawnV2(mgr, 1)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.Call(ctx, mgr, id, &BenchAdd{V: 1}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkV2Post 单 Actor 顺序 Post（fire-and-forget）。
func BenchmarkV2Post(b *testing.B) {
	mgr := actor.NewManager()
	optoins := actor.Options{BufMails: 1 << 20}
	actor.Serve(mgr, optoins, func(b *actor.RegistryBuilder[BenchId, BenchState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
	})
	id := benchSpawnV2(mgr, 1)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = actor.Post(mgr, id, &BenchPing{})
	}
}

// BenchmarkV2PostThenCall 先 Post 再 Call 同一 Actor，迫使 Post 处理完成才返回 Call 回复。
// 与单独 Post/Call 对比可拆出 reply 回传开销：Call ≈ Post+Call 等待+回复回传，
// PostThenCall ≈ Post+Call 全链，PostThenCall − Post ≈ 一次完整 Call 链路（处理+回复）。
func BenchmarkV2PostThenCall(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(bb *actor.RegistryBuilder[BenchId, BenchState]) {
		actor.RegisterSpawn(bb, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[BenchId, BenchState], req *BenchAdd, _ bool) (*BenchAddReply, error) {
			s := a.State()
			s.N += req.V
			return &BenchAddReply{Result: s.N}, nil
		})
	})
	id := benchSpawnV2(mgr, 1)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = actor.Post(mgr, id, &BenchPing{})
		actor.Call(ctx, mgr, id, &BenchAdd{V: 0})
	}
}

// BenchmarkV2CallParallel 并发 Call 同一 Actor。
func BenchmarkV2CallParallel(b *testing.B) {
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	id := benchSpawnV2(mgr, 1)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if _, err := actor.Call(ctx, mgr, id, &BenchAdd{V: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkV2Spawn 创建新 Actor（每轮 spawn 一个不同的 id）。
// 用 RegisterServe，Call 首次消息即 spawn 并返回。
// 测试结束后统一清理所有 actor，避免 goroutine 泄漏累积影响测量。
func BenchmarkV2Spawn(b *testing.B) {
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	ctx := context.Background()

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		// 只用整数 ID，避免 fmt.Sprintf 热路径分配干扰测量
		id := BenchId{ServerId: i}
		if _, err := actor.Call(ctx, mgr, id, &BenchLoginWithReply{Init: 0}); err != nil {
			b.Fatal(err)
		}
	}
	b.StopTimer()
	mgr.JoinManager()
}

// BenchmarkV2Broadcast 广播 N 个 Actor（含丢消息统计）。
func BenchmarkV2Broadcast(b *testing.B) {
	const numActors = 100
	mgr := actor.NewManager()

	var sent, recv atomic.Int64
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[BenchId, BenchState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchAdd, _ bool) (*BenchAddReply, error) {
			a.State().N += req.V
			return &BenchAddReply{Result: a.State().N}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
			recv.Add(1)
			return actor.OK, nil
		})
	})

	ctx := context.Background()
	ids := make([]BenchId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = BenchId{ServerId: i, OpenId: fmt.Sprintf("bcast_%d", i)}
		_ = actor.Post(mgr, ids[i], &BenchLogin{Init: 0})
		actor.Call(ctx, mgr, ids[i], &BenchAdd{V: 0})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		n, _ := actor.Broadcast(mgr, &BenchPing{})
		sent.Add(int64(n)) // 修复后 Broadcast 返回成功发送数
	}
	b.StopTimer()

	// Sync: 对每个 actor Call 一次，确保其 mailbox 中所有消息已消费完毕
	for i := 0; i < numActors; i++ {
		actor.Call(ctx, mgr, ids[i], &BenchAdd{V: 0})
	}

	totalSent := sent.Load()
	totalRecv := recv.Load()
	sendDrop := float64(int64(b.N)*numActors-totalSent) / float64(int64(b.N)*numActors) * 100
	rcvDrop := float64(totalSent-totalRecv) / float64(totalSent) * 100
	b.ReportMetric(sendDrop, "send_drop_pct")
	b.ReportMetric(rcvDrop, "recv_drop_pct")
}

// BenchmarkV2ConcurrentCalls 不同 Actor 并发 Call。
func BenchmarkV2ConcurrentCalls(b *testing.B) {
	const numActors = 64
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	ctx := context.Background()

	ids := make([]BenchId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = BenchId{ServerId: i, OpenId: fmt.Sprintf("conc_%d", i)}
		_ = actor.Post(mgr, ids[i], &BenchLogin{Init: 0})
		// Call 等待 spawn 完成，避免 time.Sleep
		actor.Call(ctx, mgr, ids[i], &BenchAdd{V: 0})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1)) % numActors
			if _, err := actor.Call(ctx, mgr, ids[i], &BenchAdd{V: 1}); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkV2PostParallel 并发 Post 同一 Actor。
func BenchmarkV2PostParallel(b *testing.B) {
	optoins := actor.Options{BufMails: 1 << 20}
	mgr := actor.NewManager()
	actor.Serve(mgr, optoins, func(b *actor.RegistryBuilder[BenchId, BenchState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
			a.State().N = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
			return actor.OK, nil
		})
	})
	id := benchSpawnV2(mgr, 1)

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = actor.Post(mgr, id, &BenchPing{})
		}
	})
}

// BenchmarkV2Multicast 向一组 Actor 发送 Multicast 消息。
func BenchmarkV2Multicast(b *testing.B) {
	const numActors = 100
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	ctx := context.Background()

	ids := make([]BenchId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = BenchId{ServerId: i, OpenId: fmt.Sprintf("mcast_%d", i)}
		_ = actor.Post(mgr, ids[i], &BenchLogin{Init: 0})
		actor.Call(ctx, mgr, ids[i], &BenchAdd{V: 0})
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = actor.Multicast(mgr, ids, &BenchPing{})
	}
}

// BenchmarkV2CallLatency 测量单 Actor Call 的延迟分布（P50/P90/P99）。
// 单次 Call 延迟低于 time.Now 分辨率，因此每轮批量执行多次取平均值作为样本。
func BenchmarkV2CallLatency(b *testing.B) {
	const batchSize = 500
	mgr := actor.NewManager()
	setupBenchManagerV2(mgr)
	id := benchSpawnV2(mgr, 1)
	ctx := context.Background()

	latencies := make([]time.Duration, b.N)

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		start := time.Now()
		for j := 0; j < batchSize; j++ {
			if _, err := actor.Call(ctx, mgr, id, &BenchAdd{V: 1}); err != nil {
				b.Fatal(err)
			}
		}
		latencies[i] = time.Since(start) / batchSize
	}
	b.StopTimer()

	// 转换为 int64 排序并计算百分位
	n := len(latencies)
	ns := make([]int64, n)
	for i, d := range latencies {
		ns[i] = int64(d)
	}
	sort.Slice(ns, func(i, j int) bool { return ns[i] < ns[j] })
	p50 := float64(ns[n*50/100])
	p90 := float64(ns[n*90/100])
	p99 := float64(ns[n*99/100])

	b.ReportMetric(p50, "p50_ns")
	b.ReportMetric(p90, "p90_ns")
	b.ReportMetric(p99, "p99_ns")
}

// BenchmarkV2PostSmallMailbox 小邮箱并发 Post，测量丢消息比例。
// 小容量 mailbox 在高并发 Post 下更容易满，本测试对比不同容量下的 drop 率。
func BenchmarkV2PostSmallMailbox(b *testing.B) {
	for _, cap := range []int{4, 16, 64} {
		optoins := actor.Options{BufMails: cap}
		b.Run(fmt.Sprintf("cap=%d", cap), func(b *testing.B) {
			mgr := actor.NewManager()
			actor.Serve(mgr, optoins, func(bb *actor.RegistryBuilder[BenchId, BenchState]) {
				actor.RegisterSpawn(bb, func(a *actor.ActorContext[BenchId, BenchState], req *BenchLogin, spawning bool) (actor.OkReply, error) {
					a.State().N = req.Init
					return actor.OK, nil
				})
				actor.RegisterQuery(bb, func(a *actor.ActorContext[BenchId, BenchState], req *BenchPing, _ bool) (actor.OkReply, error) {
					return actor.OK, nil
				})
			})
			id := benchSpawnV2(mgr, 1)

			var sent, dropped atomic.Int64

			b.ResetTimer()
			b.ReportAllocs()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					sent.Add(1)
					if err := actor.Post(mgr, id, &BenchPing{}); err != nil {
						dropped.Add(1)
					}
				}
			})
			b.StopTimer()

			s, d := sent.Load(), dropped.Load()
			b.ReportMetric(float64(d)/float64(s)*100, "drop_pct")
		})
	}
}

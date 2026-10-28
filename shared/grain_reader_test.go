package shared

import (
	"context"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
)

// 本文件钉住一件此前只在普通 actor 上验证过的事：
// **grain 读者（grain.SetupGrain + ServeReader）能否收到容器推送并更新本地缓存**。
//
// 背景：erosion 侧用同一形态让 entity / building / family / session 订阅世界时钟锚点，
// 落地后发现"订阅成功但 GM 发布后读者缓存不更新"。普通 actor 的路径由 store_test.go
// 覆盖且绿，因此怀疑点集中在 grain 组合上（grain 的激活/状态形态是否影响 refresh 投递）。

type grainReaderId struct{ Name string }

func (grainReaderId) ActorType() actor.ActorType { return "t_grain_reader" }
func (id grainReaderId) String() string          { return "reader-" + id.Name }

type grainReaderData struct {
	Value int
	// Vars 读者缓存（非持久化字段）：模拟 erosion 侧 State 里嵌 shared.Reader 的写法。
	Vars Reader
}

type grainReaderState = grain.State[grainReaderId, grainReaderData, grainReaderData, grain.ShotSelf[grainReaderData]]

// grainReadReq 读取当前缓存并可选地触发一次订阅。
type grainReadReq struct {
	Key Key[int64]
	Sub bool
}

func (*grainReadReq) ReqType(_ grainReaderId, _ *grainReadRep) string { return "grain_read" }

type grainReadRep struct {
	Version int64
	Loaded  bool
	Value   int64
}

// serveGrainReader 注册 grain 读者组（含 ServeReader + SetupGrain + 读接口）。
func serveGrainReader(t *testing.T, mgr *actor.Manager, pm *grain.PersistenceManager) {
	t.Helper()
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[grainReaderId, grainReaderState]) {
		ServeReader(b, func(s *grainReaderState) *Reader { return &s.Data.Vars })
		grain.SetupGrain(b, pm, grain.ShotSelf[grainReaderData]{}, nil)
		// Serve（而非 Query）：需要能唤起未被 spawn 的 grain 读者。
		actor.RegisterServe[grainReaderId, grainReaderState, *grainReadReq, *grainReadRep, grainReadReq, grainReadRep](b,
			func(ctx *actor.ActorContext[grainReaderId, grainReaderState], req *grainReadReq, _ bool) (*grainReadRep, error) {
				r := &ctx.State().Data.Vars
				if req.Sub {
					if _, err := Watch(ctx, r, req.Key); err != nil {
						return nil, err
					}
				}
				ver, loaded := VersionOf(r, req.Key)
				val, _ := Watch(ctx, r, req.Key)
				return &grainReadRep{Version: ver, Loaded: loaded, Value: val}, nil
			})
	})
}

// TestGrainReaderReceivesRefresh grain 读者的缓存必须随 publish 更新。
func TestGrainReaderReceivesRefresh(t *testing.T) {
	mgr := newTestManager(t, Options{})
	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver(t.TempDir())),
		grain.WithNodeId("test-node"),
	)
	serveGrainReader(t, mgr, pm)

	key := kSeed.Of("grain")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// 先发布，让读者首次订阅能拿到值（未发布时订阅会返回 NotPublished）。
	mustPubInt(t, mgr, key, 42)

	id := grainReaderId{Name: "r1"}
	rep, err := actor.Call(ctx, mgr, id, &grainReadReq{Key: key, Sub: true})
	if err != nil {
		t.Fatalf("subscribe read: %v", err)
	}
	if !rep.Loaded || rep.Value != 42 {
		t.Fatalf("首次订阅应拿到 42，实际 loaded=%v value=%d ver=%d", rep.Loaded, rep.Value, rep.Version)
	}
	firstVer := rep.Version

	// 再发布：容器应立即推送（Cooldown=0），grain 读者缓存随之更新。
	mustPubInt(t, mgr, key, 99)

	deadline := time.Now().Add(2 * time.Second)
	for {
		rep, err := actor.Call(ctx, mgr, id, &grainReadReq{Key: key})
		if err != nil {
			t.Fatalf("poll read: %v", err)
		}
		if rep.Value == 99 {
			if rep.Version <= firstVer {
				t.Fatalf("值已更新但版本未推进：first=%d now=%d", firstVer, rep.Version)
			}
			return // 通过
		}
		if time.Now().After(deadline) {
			t.Fatalf("grain 读者未收到推送：期望 value=99，实际 %d（ver=%d，首版=%d）", rep.Value, rep.Version, firstVer)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

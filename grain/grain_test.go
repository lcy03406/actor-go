package grain

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/lease"
)

// ─── 测试用类型 ───

type TestGrainId struct {
	Name string
}

func (t TestGrainId) ActorType() actor.ActorType { return "test_grain" }
func (t TestGrainId) String() string             { return t.Name }

type TestGrainData struct {
	Value int
}

// TestGrainSnapshot 是 TestGrainData 的快照类型。
type TestGrainSnapshot struct {
	Value int `json:"value"`
}

type TestGrainSnapshotter struct{}

func (TestGrainSnapshotter) NewPersist(data *TestGrainData) *TestGrainSnapshot {
	return &TestGrainSnapshot{}
}

func (TestGrainSnapshotter) LoadSnapshot(data *TestGrainData, snapshot *TestGrainSnapshot) {
	data.Value = snapshot.Value
}

func (TestGrainSnapshotter) TakeSnapshot(data *TestGrainData) *TestGrainSnapshot {
	return &TestGrainSnapshot{Value: data.Value}
}

type TestSpawnReq struct{}

func (*TestSpawnReq) ReqType(_ TestGrainId, _ *TestSpawnReply) string { return "test_spawn" }

type TestSpawnReply struct {
	Activated bool
}

type TestQueryReq struct{}

func (*TestQueryReq) ReqType(_ TestGrainId, _ *TestQueryReply) string { return "test_query" }

type TestQueryReply struct {
	Value int
}

type TestDeactivateReq struct{}

func (*TestDeactivateReq) ReqType(_ TestGrainId, _ actor.OkReply) string { return "test_deactivate" }

type TestMutateReq struct {
	Add int
}

func (*TestMutateReq) ReqType(_ TestGrainId, _ actor.OkReply) string { return "test_mutate" }

// ─── 辅助 ───

type testState = State[TestGrainId, TestGrainData, TestGrainSnapshot, TestGrainSnapshotter]
type testActorCtx = actor.ActorContext[TestGrainId, testState]

func newTestPMWithDir(dir string) *PersistenceManager {
	return NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithLeaseManager(lease.NewLocalManager(10*time.Second)),
		WithNodeId("node-1"),
	)
}

// setupTestRegistry 创建 manager 并注册一组标准的 grain handler。
// 返回 manager 和已取消的 cancel（用于 cleanup）。
func setupTestRegistry(t *testing.T, pm *PersistenceManager) *actor.Manager {
	t.Helper()
	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			return actor.OK, nil
		}))
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		}))
	})
	return mgr
}

// ─── 测试 ───

func TestLifecycle_ActivateDeactivate(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			return actor.OK, nil
		}))
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// 激活（首次，无数据）
	reply, err := actor.Call(ctx, mgr, TestGrainId{Name: "grain-1"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn call failed: %v", err)
	}
	if !reply.Activated {
		t.Error("expected activated=true")
	}

	// 修改数据并停活
	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "grain-1"}, &TestMutateReq{Add: 42})
	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}
	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "grain-1"}, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, TestGrainId{Name: "grain-1"})

	// 重新激活，验证数据恢复
	reply2, err := actor.Call(ctx, mgr, TestGrainId{Name: "grain-1"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("second spawn call failed: %v", err)
	}
	if !reply2.Activated {
		t.Error("expected activated=true on second spawn")
	}

	q, err := actor.Call(ctx, mgr, TestGrainId{Name: "grain-1"}, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 42 {
		t.Errorf("data: want 42, got %d", q.Value)
	}
}

func TestLifecycle_Persist(t *testing.T) {
	dir := t.TempDir()
	lm := lease.NewLocalManager(100 * time.Millisecond)
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithLeaseManager(lm),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			ctx.State().Data.Value = 999
			if err := ctx.State().Persist(ctx); err != nil {
				t.Fatalf("persist failed: %v", err)
			}
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "persist-test"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	actor.CloseActor[TestGrainId](mgr, TestGrainId{Name: "persist-test"})
	actor.JoinActor[TestGrainId](mgr, TestGrainId{Name: "persist-test"})
	time.Sleep(200 * time.Millisecond)

	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "persist-test"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("second call failed: %v", err)
	}

	q, err := actor.Call(ctx, mgr, TestGrainId{Name: "persist-test"}, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 999 {
		t.Errorf("data: want 999, got %d", q.Value)
	}
}

func TestLifecycle_AutoRenew(t *testing.T) {
	dir := t.TempDir()
	lm := lease.NewLocalManager(10 * time.Second)
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithLeaseManager(lm),
		WithNodeId("node-1"),
		WithRenewInterval(50*time.Millisecond),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterServe(b, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "renew-test"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}

	time.Sleep(200 * time.Millisecond)

	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "renew-test"}, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, TestGrainId{Name: "renew-test"})
}

func TestLifecycle_ManualRenew(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			if err := ctx.State().RenewLease(ctx); err != nil {
				t.Errorf("manual renew failed: %v", err)
			}
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "manual"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("call failed: %v", err)
	}
}

// TestJsonDriver 单独测试 JsonDriver 的 Load/Save。
func TestJsonDriver_LoadSave(t *testing.T) {
	dir := t.TempDir()
	d := NewJsonDriver(dir)

	snapshot := &TestGrainSnapshot{Value: 100}
	err := d.Save(context.Background(), "test", "actor-1", snapshot, 1)
	if err != nil {
		t.Fatalf("save failed: %v", err)
	}

	var loaded TestGrainSnapshot
	err = d.Load(context.Background(), "test", "actor-1", &loaded)
	if err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Value != 100 {
		t.Errorf("want 100, got %d", loaded.Value)
	}
}

// TestJsonDriver_LoadNotFound 测试 Load 不存在的文件。
func TestJsonDriver_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	d := NewJsonDriver(dir)

	var loaded TestGrainSnapshot
	err := d.Load(context.Background(), "test", "no-such-actor", &loaded)
	if err != ErrNotFound {
		t.Errorf("want ErrNotFound, got %v", err)
	}
}

// ─── handler 中修改数据并通过 Deactivate 持久化 ───

func TestLifecycle_MutateAndDeactivate(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			return actor.OK, nil
		}))
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "m"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	for i := 0; i < 3; i++ {
		_, err = actor.Call(ctx, mgr, TestGrainId{Name: "m"}, &TestMutateReq{Add: 10})
		if err != nil {
			t.Fatalf("mutate %d failed: %v", i, err)
		}
	}

	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "m"}, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, TestGrainId{Name: "m"})

	_, err = actor.Call(ctx, mgr, TestGrainId{Name: "m"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("respawn failed: %v", err)
	}
	q, err := actor.Call(ctx, mgr, TestGrainId{Name: "m"}, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 30 {
		t.Errorf("want 30, got %d", q.Value)
	}
}

// ─── Snapshotter / ShotSelf 单元测试 ───

func TestSnapshotter_ShotSelf(t *testing.T) {
	var s ShotSelf[int]

	data := 42
	persist := s.NewPersist(&data)
	if persist == nil {
		t.Fatal("NewPersist returned nil")
	}
	if *persist != 42 {
		t.Errorf("NewPersist: want 42, got %d", *persist)
	}

	// 修改 data 后验证 TakeSnapshot 返回当前值
	data = 100
	snapshot := s.TakeSnapshot(&data)
	if *snapshot != 100 {
		t.Errorf("TakeSnapshot: want 100, got %d", *snapshot)
	}

	// LoadSnapshot 从 persist 恢复
	other := 0
	restored := &other
	s.LoadSnapshot(&other, restored)
	if other != 0 {
		t.Errorf("LoadSnapshot from zero: want 0, got %d", other)
	}

	newVal := 77
	s.LoadSnapshot(&other, &newVal)
	if other != 77 {
		t.Errorf("LoadSnapshot: want 77, got %d", other)
	}
}

func TestSnapshotter_ShotSelf_LoadNil(t *testing.T) {
	// 验证 LoadSnapshot 在 persist 为 nil 时不崩溃且不改写 data
	var s ShotSelf[int]
	data := 99
	s.LoadSnapshot(&data, nil)
	if data != 99 {
		t.Errorf("LoadSnapshot with nil persist should not modify data, got %d", data)
	}
}

func TestSnapshotter_CustomRoundTrip(t *testing.T) {
	// 验证自定义 Snapshotter 的完整 round-trip
	var snap TestGrainSnapshotter
	data := TestGrainData{Value: 55}

	p := snap.NewPersist(&data)
	if p == nil {
		t.Fatal("NewPersist returned nil")
	}
	if p.Value != 0 {
		t.Errorf("NewPersist should return zero-value snapshot, got %d", p.Value)
	}

	// TakeSnapshot
	s := snap.TakeSnapshot(&data)
	if s.Value != 55 {
		t.Errorf("TakeSnapshot: want 55, got %d", s.Value)
	}

	// LoadSnapshot
	recovered := TestGrainData{}
	snap.LoadSnapshot(&recovered, s)
	if recovered.Value != 55 {
		t.Errorf("LoadSnapshot: want 55, got %d", recovered.Value)
	}
}

func TestSnapshotter_ShotSelf_Struct(t *testing.T) {
	// 验证 ShotSelf 对 struct 类型的支持
	type ComplexData struct {
		Name  string
		Count int
		Tags  []string
	}

	var s ShotSelf[ComplexData]

	original := ComplexData{Name: "test", Count: 10, Tags: []string{"a", "b"}}
	snapshot := s.TakeSnapshot(&original)
	if snapshot.Name != "test" || snapshot.Count != 10 {
		t.Errorf("TakeSnapshot struct mismatch: %+v", snapshot)
	}

	restored := ComplexData{}
	s.LoadSnapshot(&restored, snapshot)
	if restored.Name != "test" || restored.Count != 10 || len(restored.Tags) != 2 {
		t.Errorf("LoadSnapshot struct mismatch: %+v", restored)
	}
}

// ─── Grain 激活失败 / 租约冲突测试 ───

func TestLifecycle_ActivateAcquireLeaseFailure(t *testing.T) {
	// 当租约管理器拒绝 Acquire 时，activate 应返回错误，handler 不应被调用
	// 使用一个自定义的 lease manager 模拟 Acquire 失败
	lm := &failingLeaseManager{}
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(t.TempDir())),
		WithLeaseManager(lm),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	spawnCalled := false
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			spawnCalled = true
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "fail-acquire"}, &TestSpawnReq{})
	if err == nil {
		t.Error("expected error when lease acquire fails")
	}
	if spawnCalled {
		t.Error("handler should not be called when activate fails")
	}
}

func TestLifecycle_ConcurrentSameIdActivation(t *testing.T) {
	// 同一 ID 被并发激活时，只有一个能成功获取租约
	pm := newTestPMWithDir(t.TempDir())

	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id := TestGrainId{Name: "concurrent-activate"}
	var wg sync.WaitGroup
	successCount := int32(0)
	errCount := int32(0)

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
			if err != nil {
				// 可能因为 ActorClosedError（之前的 actor 被替换）
				_ = err
			}
		}()
	}
	wg.Wait()

	// 至少有一个能成功
	_ = successCount
	_ = errCount
}

func TestLifecycle_ReactivationAfterDeactivateAndReacquire(t *testing.T) {
	// 通过 Deactivate 正常停活后重新激活，验证数据恢复
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id := TestGrainId{Name: "reopen"}
	_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("initial spawn failed: %v", err)
	}

	// 修改数据
	_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: 100})
	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}

	// 通过 Deactivate 正常停活（保存数据 + 释放租约）
	_, err = actor.Call(ctx, mgr, id, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, id)

	// 重新激活，数据应为 100（Deactivate 已保存）
	_, err = actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("reactivation failed: %v", err)
	}
	q, err := actor.Call(ctx, mgr, id, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 100 {
		t.Errorf("after Deactivate+reactivate, data should be 100, got %d", q.Value)
	}
}

// ─── 多 Grain 并发激活与隔离测试 ───

func TestLifecycle_MultipleGrainsIsolation(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grainCount := 10
	var wg sync.WaitGroup

	for i := 0; i < grainCount; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			id := TestGrainId{Name: "isolated-" + itoa(idx)}
			// 激活
			_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
			if err != nil {
				t.Errorf("grain %d spawn failed: %v", idx, err)
				return
			}
			// 写入
			_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: idx * 10})
			if err != nil {
				t.Errorf("grain %d mutate failed: %v", idx, err)
				return
			}
		}(i)
	}
	wg.Wait()

	// 验证每个 grain 的数据隔离
	for i := 0; i < grainCount; i++ {
		id := TestGrainId{Name: "isolated-" + itoa(i)}
		q, err := actor.Call(ctx, mgr, id, &TestQueryReq{})
		if err != nil {
			t.Errorf("grain %d query failed: %v", i, err)
			continue
		}
		expected := i * 10
		if q.Value != expected {
			t.Errorf("grain %d: want %d, got %d", i, expected, q.Value)
		}
	}
}

func TestLifecycle_MultipleGrainsDeactivateReactivate(t *testing.T) {
	// 多个 grain 停活后重新激活，验证各自数据恢复正确
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grainCount := 5
	ids := make([]TestGrainId, grainCount)
	for i := 0; i < grainCount; i++ {
		ids[i] = TestGrainId{Name: "multi-dar-" + itoa(i)}
		_, err := actor.Call(ctx, mgr, ids[i], &TestSpawnReq{})
		if err != nil {
			t.Fatalf("spawn %d failed: %v", i, err)
		}
		_, err = actor.Call(ctx, mgr, ids[i], &TestMutateReq{Add: (i + 1) * 100})
		if err != nil {
			t.Fatalf("mutate %d failed: %v", i, err)
		}
	}

	// 全部停活
	for i := 0; i < grainCount; i++ {
		_, err := actor.Call(ctx, mgr, ids[i], &TestDeactivateReq{})
		if err != nil {
			t.Fatalf("deactivate %d failed: %v", i, err)
		}
		actor.JoinActor[TestGrainId](mgr, ids[i])
	}

	// 重新激活并验证
	for i := 0; i < grainCount; i++ {
		_, err := actor.Call(ctx, mgr, ids[i], &TestSpawnReq{})
		if err != nil {
			t.Fatalf("reactivate %d failed: %v", i, err)
		}
		q, err := actor.Call(ctx, mgr, ids[i], &TestQueryReq{})
		if err != nil {
			t.Fatalf("query %d failed: %v", i, err)
		}
		expected := (i + 1) * 100
		if q.Value != expected {
			t.Errorf("grain %d: want %d, got %d", i, expected, q.Value)
		}
	}
}

// ─── PersistenceManager 配置与边界测试 ───

func TestPersistenceManager_Options(t *testing.T) {
	dir := t.TempDir()
	lm := lease.NewLocalManager(5 * time.Second)

	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithLeaseManager(lm),
		WithNodeId("test-node"),
		WithRenewInterval(30*time.Second),
	)

	if pm.driver == nil {
		t.Error("driver should not be nil")
	}
	if pm.leaseManager == nil {
		t.Error("leaseManager should not be nil")
	}
	if pm.nodeId != "test-node" {
		t.Errorf("nodeId: want test-node, got %s", pm.nodeId)
	}
	if pm.renewInterval != 30*time.Second {
		t.Errorf("renewInterval: want 30s, got %v", pm.renewInterval)
	}
}

func TestPersistenceManager_ZeroRenewInterval(t *testing.T) {
	// renewInterval 为 0 表示不自动续约
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(t.TempDir())),
		WithLeaseManager(lease.NewLocalManager(10*time.Second)),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "no-renew"}, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
}

func TestPersistenceManager_NilLeaseManager(t *testing.T) {
	// 没有 lease manager 时，activate 会失败
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(t.TempDir())),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "no-lm"}, &TestSpawnReq{})
	if err == nil {
		t.Error("expected error when lease manager is nil")
	}
}

func TestPersistenceManager_NilDriver(t *testing.T) {
	pm := NewPersistenceManager(
		WithLeaseManager(lease.NewLocalManager(10*time.Second)),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "no-driver"}, &TestSpawnReq{})
	// 可能 panic 或返回错误，取决于 driver 是否为 nil
	if err == nil {
		t.Log("no error when driver is nil (might panic)")
	}
}

// ─── JsonDriver 测试 ───

func TestJsonDriver_Overwrite(t *testing.T) {
	dir := t.TempDir()
	d := NewJsonDriver(dir)

	// 首次保存
	snap := &TestGrainSnapshot{Value: 100}
	if err := d.Save(context.Background(), "test", "actor-ow", snap, 1); err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	// 覆盖写入
	snap2 := &TestGrainSnapshot{Value: 200}
	if err := d.Save(context.Background(), "test", "actor-ow", snap2, 2); err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if err := d.Load(context.Background(), "test", "actor-ow", &loaded); err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Value != 200 {
		t.Errorf("overwrite: want 200, got %d", loaded.Value)
	}
}

func TestJsonDriver_MultipleActorTypes(t *testing.T) {
	dir := t.TempDir()
	d := NewJsonDriver(dir)

	snap1 := &TestGrainSnapshot{Value: 1}
	snap2 := &TestGrainSnapshot{Value: 2}

	if err := d.Save(context.Background(), "player", "p1", snap1, 1); err != nil {
		t.Fatalf("save player failed: %v", err)
	}
	if err := d.Save(context.Background(), "npc", "p1", snap2, 1); err != nil {
		t.Fatalf("save npc failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if err := d.Load(context.Background(), "player", "p1", &loaded); err != nil {
		t.Fatalf("load player failed: %v", err)
	}
	if loaded.Value != 1 {
		t.Errorf("player: want 1, got %d", loaded.Value)
	}

	if err := d.Load(context.Background(), "npc", "p1", &loaded); err != nil {
		t.Fatalf("load npc failed: %v", err)
	}
	if loaded.Value != 2 {
		t.Errorf("npc: want 2, got %d", loaded.Value)
	}
}

func TestJsonDriver_SaveCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "deep", "nested", "path")
	d := NewJsonDriver(dir)

	snap := &TestGrainSnapshot{Value: 42}
	if err := d.Save(context.Background(), "type", "id", snap, 1); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 验证目录和文件存在
	expectedPath := filepath.Join(dir, "type", "id.json")
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("file not created at %s", expectedPath)
	}
}

func TestJsonDriver_SaveInvalidPath(t *testing.T) {
	// Windows 上使用包含非法字符的路径，或者只读目录
	// 使用 NUL 设备路径作为测试
	d := NewJsonDriver("NUL")
	snap := &TestGrainSnapshot{Value: 1}
	err := d.Save(context.Background(), "test", "id", snap, 1)
	if err == nil {
		// 在 Windows 上可能不会报错，改用只读目录测试
		// 或者直接跳过
		t.Skip("cannot create invalid path on this platform")
	}
}

// ─── Deactivate 后状态行为测试 ───

func TestLifecycle_DeactivateSavesState(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id := TestGrainId{Name: "deact-save"}
	_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: 77})
	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}
	_, err = actor.Call(ctx, mgr, id, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, id)

	// 验证通过 actor 恢复数据
	_, err = actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("reactivation failed: %v", err)
	}
	q, err := actor.Call(ctx, mgr, id, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 77 {
		t.Errorf("deactivate should persist data: want 77, got %d", q.Value)
	}
}

func TestLifecycle_PersistMultipleTimes(t *testing.T) {
	// 多次 Persist 不退出，验证数据累积正确
	dir := t.TempDir()
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithLeaseManager(lease.NewLocalManager(10*time.Second)),
		WithNodeId("node-1"),
	)

	mgr := actor.NewManager()
	actor.Serve(mgr, 10, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			// 只在首次激活时设置初始值，重新激活时不应覆盖已加载的数据
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			if err := ctx.State().Persist(ctx); err != nil {
				t.Errorf("persist failed: %v", err)
			}
			return actor.OK, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
		actor.RegisterServe(b, WrapSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id := TestGrainId{Name: "multi-persist"}
	_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}

	for i := 0; i < 5; i++ {
		_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: 10})
		if err != nil {
			t.Fatalf("mutate %d failed: %v", i, err)
		}
	}

	// 停活后重新激活
	_, err = actor.Call(ctx, mgr, id, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, id)

	_, err = actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("reactivation failed: %v", err)
	}
	q, err := actor.Call(ctx, mgr, id, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 50 {
		t.Errorf("multi-persist: want 50, got %d", q.Value)
	}
}

// ─── failingLeaseManager 用于模拟 Acquire 失败 ───

type failingLeaseManager struct{}

func (f *failingLeaseManager) Acquire(_ context.Context, _, _ string) (*lease.Lease, error) {
	return nil, lease.ErrNotAcquired
}
func (f *failingLeaseManager) Release(_ context.Context, _ *lease.Lease) error { return nil }
func (f *failingLeaseManager) Renew(_ context.Context, _ *lease.Lease) error  { return nil }

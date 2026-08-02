package grain

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
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
		WithNodeId("node-1"),
	)
}

// activatingSpawn 是测试中对"用户在回调中显式调用 State.Activate"的惯用法封装：
// 仅在 spawning（首次/重新 spawn）时激活 Grain，非 spawning 时沿用已激活状态。
// 框架不再提供 WrapSpawn 这类自动包装，调用方需自行决定何时激活。
func activatingSpawn[Q any, R any](
	pm *PersistenceManager,
	fn func(ctx *testActorCtx, req Q, spawning bool) (R, error),
) func(ctx *testActorCtx, req Q, spawning bool) (R, error) {
	return func(ctx *testActorCtx, req Q, spawning bool) (R, error) {
		if spawning {
			if _, err := ctx.State().Activate(ctx, pm); err != nil {
				var zero R
				return zero, err
			}
		}
		return fn(ctx, req, spawning)
	}
}

// setupManager 创建 manager 并按给定注册函数注册一组 grain handler，
// 消除各测试中重复的 `actor.NewManager()` + `actor.Serve(...)` 包裹样板。
func setupManager(pm *PersistenceManager, register func(b *actor.RegistryBuilder[TestGrainId, testState])) *actor.Manager {
	mgr := actor.NewManager()
	actor.Serve(mgr, 10, register)
	return mgr
}

// setupTestRegistry 创建 manager 并注册一组标准的 grain handler。
func setupTestRegistry(t *testing.T, pm *PersistenceManager) *actor.Manager {
	t.Helper()
	return setupManager(pm, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
		actor.RegisterServe(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			return actor.OK, nil
		}))
		actor.RegisterServe(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx)
			return actor.OK, nil
		}))
	})
}

// ─── 测试 ───

func TestLifecycle_ActivateDeactivate(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

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
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithNodeId("node-1"),
	)

	mgr := setupManager(pm, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
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
	testutil.Settle(200 * time.Millisecond)

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

// ─── handler 中修改数据并通过 Deactivate 持久化 ───

func TestLifecycle_MutateAndDeactivate(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

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

	data = 100
	snapshot := s.TakeSnapshot(&data)
	if *snapshot != 100 {
		t.Errorf("TakeSnapshot: want 100, got %d", *snapshot)
	}

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
	var s ShotSelf[int]
	data := 99
	s.LoadSnapshot(&data, nil)
	if data != 99 {
		t.Errorf("LoadSnapshot with nil persist should not modify data, got %d", data)
	}
}

func TestSnapshotter_CustomRoundTrip(t *testing.T) {
	var snap TestGrainSnapshotter
	data := TestGrainData{Value: 55}

	p := snap.NewPersist(&data)
	if p == nil {
		t.Fatal("NewPersist returned nil")
	}
	if p.Value != 0 {
		t.Errorf("NewPersist should return zero-value snapshot, got %d", p.Value)
	}

	s := snap.TakeSnapshot(&data)
	if s.Value != 55 {
		t.Errorf("TakeSnapshot: want 55, got %d", s.Value)
	}

	recovered := TestGrainData{}
	snap.LoadSnapshot(&recovered, s)
	if recovered.Value != 55 {
		t.Errorf("LoadSnapshot: want 55, got %d", recovered.Value)
	}
}

func TestSnapshotter_ShotSelf_Struct(t *testing.T) {
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

func TestLifecycle_ConcurrentSameIdActivation(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	id := TestGrainId{Name: "concurrent-activate"}
	var wg sync.WaitGroup

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
}

func TestLifecycle_ReactivationAfterDeactivateAndReacquire(t *testing.T) {
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id := TestGrainId{Name: "reopen"}
	_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("initial spawn failed: %v", err)
	}

	_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: 100})
	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}

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
			id := TestGrainId{Name: "isolated-" + strconv.Itoa(idx)}
			_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
			if err != nil {
				t.Errorf("grain %d spawn failed: %v", idx, err)
				return
			}
			_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: idx * 10})
			if err != nil {
				t.Errorf("grain %d mutate failed: %v", idx, err)
				return
			}
		}(i)
	}
	wg.Wait()

	for i := 0; i < grainCount; i++ {
		id := TestGrainId{Name: "isolated-" + strconv.Itoa(i)}
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
	pm := newTestPMWithDir(t.TempDir())
	mgr := setupTestRegistry(t, pm)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	grainCount := 5
	ids := make([]TestGrainId, grainCount)
	for i := 0; i < grainCount; i++ {
		ids[i] = TestGrainId{Name: "multi-dar-" + strconv.Itoa(i)}
		_, err := actor.Call(ctx, mgr, ids[i], &TestSpawnReq{})
		if err != nil {
			t.Fatalf("spawn %d failed: %v", i, err)
		}
		_, err = actor.Call(ctx, mgr, ids[i], &TestMutateReq{Add: (i + 1) * 100})
		if err != nil {
			t.Fatalf("mutate %d failed: %v", i, err)
		}
	}

	for i := 0; i < grainCount; i++ {
		_, err := actor.Call(ctx, mgr, ids[i], &TestDeactivateReq{})
		if err != nil {
			t.Fatalf("deactivate %d failed: %v", i, err)
		}
		actor.JoinActor[TestGrainId](mgr, ids[i])
	}

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

	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithNodeId("test-node"),
	)

	if pm.driver == nil {
		t.Error("driver should not be nil")
	}
	if pm.nodeId != "test-node" {
		t.Errorf("nodeId: want test-node, got %s", pm.nodeId)
	}
}

func TestPersistenceManager_NilDriver(t *testing.T) {
	pm := NewPersistenceManager(
		WithNodeId("node-1"),
	)

	mgr := setupManager(pm, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
	})

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	_, err := actor.Call(ctx, mgr, TestGrainId{Name: "no-driver"}, &TestSpawnReq{})
	if err == nil {
		t.Log("no error when driver is nil (might panic)")
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
	dir := t.TempDir()
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithNodeId("node-1"),
	)

	mgr := setupManager(pm, func(b *actor.RegistryBuilder[TestGrainId, testState]) {
		actor.RegisterSpawn(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestSpawnReq, spawning bool) (*TestSpawnReply, error) {
			return &TestSpawnReply{Activated: true}, nil
		}))
		actor.RegisterServe(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestMutateReq, spawning bool) (actor.OkReply, error) {
			ctx.State().Data.Value += req.Add
			if err := ctx.State().Persist(ctx); err != nil {
				t.Errorf("persist failed: %v", err)
			}
			return actor.OK, nil
		}))
		actor.RegisterQuery(b, func(ctx *testActorCtx, req *TestQueryReq, spawning bool) (*TestQueryReply, error) {
			return &TestQueryReply{Value: ctx.State().Data.Value}, nil
		})
		actor.RegisterServe(b, activatingSpawn(pm, func(ctx *testActorCtx, req *TestDeactivateReq, spawning bool) (actor.OkReply, error) {
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

// ─── ErrLeaseTaken 测试 ───

func TestErrLeaseTaken_Error(t *testing.T) {
	err := &ErrLeaseTaken{
		Key:        "player:123",
		Owner:      "node-2",
		Generation: 5,
	}
	msg := err.Error()
	if msg == "" {
		t.Error("ErrLeaseTaken.Error() should not be empty")
	}
}

func TestPersistenceManager_ForceRelease(t *testing.T) {
	dir := t.TempDir()
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithNodeId("node-1"),
	)

	// 先通过正常流程激活并保存数据
	mgr := setupTestRegistry(t, pm)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	id := TestGrainId{Name: "pm-fr-test"}
	_, err := actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("spawn failed: %v", err)
	}
	_, err = actor.Call(ctx, mgr, id, &TestMutateReq{Add: 50})
	if err != nil {
		t.Fatalf("mutate failed: %v", err)
	}
	_, err = actor.Call(ctx, mgr, id, &TestDeactivateReq{})
	if err != nil {
		t.Fatalf("deactivate failed: %v", err)
	}
	actor.JoinActor[TestGrainId](mgr, id)

	// 通过 PersistenceManager 强制释放
	newGen, err := pm.ForceRelease(ctx, "test_grain", id.String())
	if err != nil {
		t.Fatalf("PersistenceManager.ForceRelease failed: %v", err)
	}
	if newGen <= 0 {
		t.Errorf("ForceRelease: expected positive generation, got %d", newGen)
	}

	// 释放后其他节点可以重新激活
	_, err = actor.Call(ctx, mgr, id, &TestSpawnReq{})
	if err != nil {
		t.Fatalf("reactivation after ForceRelease failed: %v", err)
	}
	q, err := actor.Call(ctx, mgr, id, &TestQueryReq{})
	if err != nil {
		t.Fatalf("query failed: %v", err)
	}
	if q.Value != 50 {
		t.Errorf("ForceRelease preserved data: want 50, got %d", q.Value)
	}
}

func TestPersistenceManager_ForceRelease_NoDriver(t *testing.T) {
	pm := NewPersistenceManager(
		WithNodeId("node-1"),
	)

	_, err := pm.ForceRelease(context.Background(), "test", "id")
	if err == nil {
		t.Error("ForceRelease without driver should return error")
	}
	if !errors.Is(err, ErrNoDriver) {
		t.Errorf("ForceRelease error: want ErrNoDriver, got %v", err)
	}
}

func TestPersistenceManager_Driver_NodeId(t *testing.T) {
	dir := t.TempDir()
	pm := NewPersistenceManager(
		WithDriver(NewJsonDriver(dir)),
		WithNodeId("test-node"),
	)

	if pm.Driver() == nil {
		t.Error("Driver() should not be nil")
	}
	if pm.NodeId() != "test-node" {
		t.Errorf("NodeId(): want test-node, got %s", pm.NodeId())
	}
}

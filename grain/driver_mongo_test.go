package grain

import (
	"context"
	"errors"
	"os"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/v2/mongo"
	"go.mongodb.org/mongo-driver/v2/mongo/options"
)

// newTestMongoDriver 连接 MongoDB 用于测试。
// 优先使用环境变量 MONGO_URI；未设置时回退到本机默认地址 mongodb://localhost:27017。
// 若连接/可达性校验失败则 t.Skip（避免无可用实例时直接失败）。
func newTestMongoDriver(t *testing.T) *MongoDriver {
	t.Helper()
	uri := os.Getenv("MONGO_URI")
	if uri == "" {
		uri = "mongodb://localhost:27017"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	client, err := mongo.Connect(options.Client().ApplyURI(uri))
	if err != nil {
		t.Skipf("mongo connect failed (uri=%s): %v", uri, err)
	}
	// 验证可达性
	if err := client.Ping(ctx, nil); err != nil {
		t.Skipf("mongo ping failed (uri=%s): %v", uri, err)
	}
	t.Cleanup(func() { _ = client.Disconnect(context.Background()) })
	db := client.Database("actor_go_test")
	// 隔离：每个测试开始时清空其用到的集合，避免跨用例/多次运行残留导致 generation 不匹配。
	for _, c := range []string{"test", "custom"} {
		if err := db.Collection(c).Drop(ctx); err != nil {
			t.Fatalf("drop collection %s failed: %v", c, err)
		}
	}
	return NewMongoDriver(db, "node-1", DefaultLeaseTimeout)
}

func TestMongoDriver_LoadSave(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-1"

	// 首次 Load：文档不存在，upsert 创建，generation=1，返回 ErrNotFound
	var loaded TestGrainSnapshot
	lease, err := d.Load(ctx, "test", id, "node-1", &loaded)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("first Load: want ErrNotFound, got %v", err)
	}
	if lease == nil || lease.Generation != 1 {
		t.Errorf("first Load: want generation=1, got %v", lease)
	}

	// Save 数据
	snapshot := &TestGrainSnapshot{Value: 100}
	if err := d.Save(ctx, "test", id, "node-1", snapshot, 1); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// 再次 Load：应返回数据
	var loaded2 TestGrainSnapshot
	lease2, err := d.Load(ctx, "test", id, "node-1", &loaded2)
	if err != nil {
		t.Fatalf("second Load failed: %v", err)
	}
	if lease2 == nil {
		t.Fatal("lease should not be nil")
	}
	if loaded2.Value != 100 {
		t.Errorf("want 100, got %d", loaded2.Value)
	}
}

func TestMongoDriver_LoadNotFound(t *testing.T) {
	d := newTestMongoDriver(t)
	var loaded TestGrainSnapshot
	lease, err := d.Load(context.Background(), "test", "no-such-actor", "node-1", &loaded)
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("want ErrNotFound, got %v", err)
	}
	if lease == nil {
		t.Error("lease should not be nil even on ErrNotFound (first activation)")
	}
}

func TestMongoDriver_Overwrite(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-ow"

	// 先 Load 获取租约（首次激活返回 ErrNotFound 属正常）
	var loaded0 TestGrainSnapshot
	lease, err := d.Load(ctx, "test", id, "node-1", &loaded0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}

	snap := &TestGrainSnapshot{Value: 100}
	if err := d.Save(ctx, "test", id, "node-1", snap, lease.Generation); err != nil {
		t.Fatalf("first save failed: %v", err)
	}
	snap2 := &TestGrainSnapshot{Value: 200}
	if err := d.Save(ctx, "test", id, "node-1", snap2, lease.Generation); err != nil {
		t.Fatalf("second save failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if _, err := d.Load(ctx, "test", id, "node-1", &loaded); err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Value != 200 {
		t.Errorf("overwrite: want 200, got %d", loaded.Value)
	}
}

func TestMongoDriver_LeaseTaken(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-taken"

	// node-1 抢占租约（首次激活返回 ErrNotFound 属正常，租约已获取）
	if _, err := d.Load(ctx, "test", id, "node-1", &TestGrainSnapshot{}); err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}

	// node-2 立刻 Load（租约未过期）→ ErrLeaseTaken
	var loaded TestGrainSnapshot
	_, leaseErr := d.Load(ctx, "test", id, "node-2", &loaded)
	var leaseTaken *ErrLeaseTaken
	if !errors.As(leaseErr, &leaseTaken) {
		t.Errorf("want ErrLeaseTaken, got %v", leaseErr)
	}
}

func TestMongoDriver_SaveNilRenewLease(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-nil"

	// 先 Load 获取租约（首次激活返回 ErrNotFound 属正常）
	var loaded0 TestGrainSnapshot
	lease, err := d.Load(ctx, "test", id, "node-1", &loaded0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}

	snap := &TestGrainSnapshot{Value: 100}
	if err := d.Save(ctx, "test", id, "node-1", snap, lease.Generation); err != nil {
		t.Fatalf("first save failed: %v", err)
	}

	// snapshot 为 nil：仅续租，不覆盖数据
	if err := d.Save(ctx, "test", id, "node-1", nil, lease.Generation); err != nil {
		t.Fatalf("nil save (renew lease) failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if _, err := d.Load(ctx, "test", id, "node-1", &loaded); err != nil {
		t.Fatalf("load failed: %v", err)
	}
	if loaded.Value != 100 {
		t.Errorf("nil save must not overwrite data: want 100, got %d", loaded.Value)
	}
}

func TestMongoDriver_Release(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-rel"

	// 先 Load 获取租约（首次激活返回 ErrNotFound 属正常）
	var loaded0 TestGrainSnapshot
	lease, err := d.Load(ctx, "test", id, "node-1", &loaded0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}

	snap := &TestGrainSnapshot{Value: 42}
	if err := d.Save(ctx, "test", id, "node-1", snap, lease.Generation); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	// Release 不删除数据
	if err := d.Release(ctx, "test", id, "node-1", lease.Generation); err != nil {
		t.Fatalf("Release failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if _, err := d.Load(ctx, "test", id, "node-1", &loaded); err != nil {
		t.Fatalf("Load after Release failed: %v", err)
	}
	if loaded.Value != 42 {
		t.Errorf("Release should not delete data: want 42, got %d", loaded.Value)
	}
}

func TestMongoDriver_ForceRelease(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	id := "actor-fr"

	// 先 Load 获取租约（首次激活返回 ErrNotFound 属正常）
	var loaded0 TestGrainSnapshot
	lease, err := d.Load(ctx, "test", id, "node-1", &loaded0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}

	snapshot := &TestGrainSnapshot{Value: 42}
	if err := d.Save(ctx, "test", id, "node-1", snapshot, lease.Generation); err != nil {
		t.Fatalf("save failed: %v", err)
	}

	newGen, err := d.ForceRelease(ctx, "test", id)
	if err != nil {
		t.Fatalf("ForceRelease failed: %v", err)
	}
	if newGen != 2 {
		t.Errorf("ForceRelease: want newGen=2, got %d", newGen)
	}

	// 释放后其他节点可重新 Load，数据保留
	var loaded TestGrainSnapshot
	lease, err = d.Load(ctx, "test", id, "node-2", &loaded)
	if err != nil {
		t.Fatalf("Load after ForceRelease failed: %v", err)
	}
	if lease.Generation != 3 {
		t.Errorf("Load after ForceRelease: want generation=3, got %d", lease.Generation)
	}
	if loaded.Value != 42 {
		t.Errorf("Load after ForceRelease: data preserved, want 42, got %d", loaded.Value)
	}
}

func TestMongoDriver_RegisterCollection(t *testing.T) {
	d := newTestMongoDriver(t)
	ctx := context.Background()
	d.RegisterCollection("custom", "my_custom_coll")

	id := "actor-custom"
	// 先 Load 获取租约（首次激活返回 ErrNotFound 属正常）
	var loaded0 TestGrainSnapshot
	lease, err := d.Load(ctx, "custom", id, "node-1", &loaded0)
	if err != nil && !errors.Is(err, ErrNotFound) {
		t.Fatalf("first load failed: %v", err)
	}
	snap := &TestGrainSnapshot{Value: 99}
	if err := d.Save(ctx, "custom", id, "node-1", snap, lease.Generation); err != nil {
		t.Fatalf("save to custom collection failed: %v", err)
	}

	var loaded TestGrainSnapshot
	if _, err := d.Load(ctx, "custom", id, "node-1", &loaded); err != nil {
		t.Fatalf("load from custom collection failed: %v", err)
	}
	if loaded.Value != 99 {
		t.Errorf("custom collection: want 99, got %d", loaded.Value)
	}
}

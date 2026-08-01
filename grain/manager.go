package grain

import (
	"context"
	"time"
)

// PersistenceManager 是 manager 级别的持久化管理器。
// 每个 Manager 创建一个实例，所有 Grain 类型共享。
//
// 租约管理已内置在 Driver 中，不再需要单独的 lease.Manager。
// 续租由 Persist/Save 操作顺带完成，不再有独立的自动续约定时器。
//
// 用法：
//
//	pm := grain.NewPersistenceManager(
//	    grain.WithDriver(grain.NewJsonDriver("./data")),
//	    grain.WithNodeId("node-1"),
//	    grain.WithAutoPersistInterval(30*time.Second),
//	)
type PersistenceManager struct {
	driver          Driver
	nodeId          string
	persistInterval time.Duration
}

// PersistenceManagerOption 是 PersistenceManager 的配置选项。
type PersistenceManagerOption func(*PersistenceManager)

// WithDriver 设置持久化驱动。
func WithDriver(d Driver) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.driver = d
	}
}

// WithNodeId 设置节点 ID。
func WithNodeId(id string) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.nodeId = id
	}
}

// WithAutoPersistInterval 设置自动定时存盘间隔。
// 通过 grain.SetupGrain 接入生命周期后，Grain 会以该间隔自动 Persist（续租 + 落盘）。
// 设为 0 或负数表示关闭自动存盘（默认），需由业务主动调用 Persist。
func WithAutoPersistInterval(d time.Duration) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.persistInterval = d
	}
}

// NewPersistenceManager 创建持久化管理器。
func NewPersistenceManager(opts ...PersistenceManagerOption) *PersistenceManager {
	pm := &PersistenceManager{}
	for _, opt := range opts {
		opt(pm)
	}
	return pm
}

// Driver 返回底层 Driver 实例。
func (pm *PersistenceManager) Driver() Driver {
	return pm.driver
}

// NodeId 返回节点 ID。
func (pm *PersistenceManager) NodeId() string {
	return pm.nodeId
}

// ForceRelease 强制释放租约，不检查 owner 和 generation。
// 实现 cluster.LeaseForceReleaser 接口，供 cluster.Router 通过 WithForceReleaser 使用。
func (pm *PersistenceManager) ForceRelease(ctx context.Context, actorType string, id string) (int64, error) {
	if pm.driver == nil {
		return 0, ErrNoDriver
	}
	return pm.driver.ForceRelease(ctx, actorType, id)
}

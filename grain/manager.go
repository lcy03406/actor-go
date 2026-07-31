package grain

import (
	"time"

	"github.com/lcy03406/actor-go/lease"
)

// PersistenceManager 是 manager 级别的持久化管理器。
// 每个 Manager 创建一个实例，所有 Grain 类型共享。
//
// 用法：
//
//	pm := grain.NewPersistenceManager(
//	    grain.WithDriver(grain.NewJsonDriver("./data")),
//	    grain.WithLeaseManager(lease.NewLocalManager()),
//	    grain.WithNodeId("node-1"),
//	    grain.WithRenewInterval(30*time.Second),
//	)
type PersistenceManager struct {
	driver        Driver
	leaseManager  lease.Manager
	nodeId        string
	renewInterval time.Duration
}

// PersistenceManagerOption 是 PersistenceManager 的配置选项。
type PersistenceManagerOption func(*PersistenceManager)

// WithDriver 设置持久化驱动。
func WithDriver(d Driver) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.driver = d
	}
}

// WithLeaseManager 设置租约管理器。
func WithLeaseManager(lm lease.Manager) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.leaseManager = lm
	}
}

// WithNodeId 设置节点 ID。
func WithNodeId(id string) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.nodeId = id
	}
}

// WithRenewInterval 设置自动续约间隔。0 表示不自动续约。
func WithRenewInterval(d time.Duration) PersistenceManagerOption {
	return func(pm *PersistenceManager) {
		pm.renewInterval = d
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

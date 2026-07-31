package lease

import (
	"context"
	"sync"
	"time"
)

// localLeaseRecord 本地租约记录。
type localLeaseRecord struct {
	owner      string
	generation int64
	expireAt   time.Time
}

// localLeaseManager 基于本地内存的租约管理器实现。
//
// 适用于单进程场景，使用 sync.Mutex 保证并发安全。
// 租约过期后自动释放，可被其他 goroutine 抢占。
//
// 注意：
//   - 仅适用于单进程，不适用于分布式环境
//   - 进程重启后所有租约丢失
//   - 不支持持久化
type localLeaseManager struct {
	mu           sync.Mutex
	leases       map[string]*localLeaseRecord
	leaseTimeout time.Duration
}

// NewLocalManager 创建一个基于本地内存的租约管理器。
// leaseTimeout 指定租约过期时间：超过此时间未续约的租约可以被其他 owner 抢占。
func NewLocalManager(leaseTimeout time.Duration) Manager {
	return &localLeaseManager{
		leases:       make(map[string]*localLeaseRecord),
		leaseTimeout: leaseTimeout,
	}
}

func (m *localLeaseManager) Acquire(ctx context.Context, key, owner string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	record, exists := m.leases[key]

	if !exists || record.owner == "" || now.After(record.expireAt) {
		gen := int64(1)
		if exists {
			gen = record.generation + 1
		}
		m.leases[key] = &localLeaseRecord{
			owner:      owner,
			generation: gen,
			expireAt:   now.Add(m.leaseTimeout),
		}
		return &Lease{
			Key:        key,
			Owner:      owner,
			Generation: gen,
		}, nil
	}

	return nil, ErrNotAcquired
}

func (m *localLeaseManager) Release(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, exists := m.leases[lease.Key]
	if !exists || record.owner != lease.Owner || record.generation != lease.Generation {
		return ErrLeaseExpired
	}

	record.owner = ""
	return nil
}

func (m *localLeaseManager) Renew(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	record, exists := m.leases[lease.Key]
	if !exists || record.owner != lease.Owner || record.generation != lease.Generation {
		return ErrLeaseExpired
	}

	now := time.Now()
	if now.After(record.expireAt) {
		return ErrLeaseExpired
	}

	record.expireAt = now.Add(m.leaseTimeout)
	return nil
}

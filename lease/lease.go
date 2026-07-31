// Package lease 提供分布式租约（fencing token）抽象。
//
// 租约确保在集群环境中同一时刻只有一个持有者拥有对某个资源的操作权。
// 每次 Acquire 返回单调递增的 Generation，写回时携带 Generation 可防止
// 旧持有者的过期写入（fencing）。
//
// 本包不依赖 actor-go 的任何其他包，可独立使用。
package lease

import (
	"context"
	"errors"
)

// ErrNotAcquired 表示抢锁失败，资源当前被其他持有者占用。
var ErrNotAcquired = errors.New("lease: not acquired, held by another owner")

// ErrLeaseExpired 表示租约已过期（generation 不匹配），通常出现在写回时。
var ErrLeaseExpired = errors.New("lease: generation mismatch, lease expired")

// Lease 表示一个已获取的租约。
type Lease struct {
	// Key 是资源的唯一标识，例如 "player:123"。
	Key string
	// Owner 是当前持有者的标识，例如节点 ID "node-3"。
	Owner string
	// Generation 是单调递增的版本号。
	// 每次 Acquire 成功后递增，用于 fencing 校验。
	Generation int64
}

// Manager 是分布式租约管理器接口。
//
// 典型使用流程：
//
//	lease, err := mgr.Acquire(ctx, "player:123", "node-3")
//	// ... 执行业务操作 ...
//	err = mgr.Release(ctx, lease)
//
// 对于长生命周期租约，定期调用 Renew 防止被认为失联：
//
//	lease, _ := mgr.Acquire(ctx, key, owner)
//	go func() {
//	    ticker := time.NewTicker(30 * time.Second)
//	    for range ticker.C {
//	        mgr.Renew(ctx, lease)
//	    }
//	}()
type Manager interface {
	// Acquire 尝试获取租约。成功返回 Lease，失败返回 ErrNotAcquired。
	// 每次成功调用都会使 Generation 递增。
	Acquire(ctx context.Context, key, owner string) (*Lease, error)

	// Release 释放租约。只有持有者才能释放。
	// 通过比对 Generation 确保不会误释放已被抢占的租约。
	Release(ctx context.Context, lease *Lease) error

	// Renew 续约租约，向存储后端证明持有者仍然存活。
	// 如果 Generation 不匹配（租约已被抢占），返回 ErrLeaseExpired。
	Renew(ctx context.Context, lease *Lease) error
}

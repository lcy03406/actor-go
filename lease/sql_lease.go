package lease

import (
	"context"
	"database/sql"
	"sync"
	"time"
)

// sqlLeaseManager 基于 SQL 数据库的租约管理器实现。
//
// 使用 PostgreSQL INSERT ... ON CONFLICT ... RETURNING 语句原子完成抢锁和 generation 递增：
//
//	INSERT INTO leases (key, owner, generation, updated_at)
//	VALUES ($1, $2, 1, now())
//	ON CONFLICT (key) DO UPDATE
//	  SET owner = CASE WHEN owner = '' OR owner = $2
//	                        OR updated_at < now() - lease_timeout
//	                   THEN $2 ELSE owner END,
//	      generation = CASE WHEN owner = '' OR owner = $2
//	                             OR updated_at < now() - lease_timeout
//	                       THEN generation + 1 ELSE generation END,
//	      updated_at = CASE WHEN owner = '' OR owner = $2
//	                             OR updated_at < now() - lease_timeout
//	                       THEN now() ELSE updated_at END
//	RETURNING owner, generation
//
// 表结构：
//
//	CREATE TABLE leases (
//	    key        TEXT PRIMARY KEY,
//	    owner      TEXT NOT NULL DEFAULT '',
//	    generation BIGINT NOT NULL DEFAULT 0,
//	    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
//	);
//
// 断线重连：
//
//	database/sql 内置连接池管理，配合 SetConnMaxLifetime 可在连接断开后自动建立新连接。
//	本实现额外提供了 retryAttempts 参数，对瞬态网络错误（driver.ErrBadConn 等）进行指数退避重试。
//	逻辑错误（ErrNotAcquired / ErrLeaseExpired）不重试。
//	Acquire 调用是幂等的：如果 key 已被同一 owner 持有，直接返回现有租约并更新 updated_at。
type sqlLeaseManager struct {
	db            *sql.DB
	leaseTimeout  time.Duration
	retryAttempts int
	mu            sync.Mutex
}

// NewSqlManager 创建一个基于 SQL 的租约管理器。
// leaseTimeout 指定租约过期时间：超过此时间未续约的租约可以被其他 owner 抢占。
// retryAttempts 指定瞬态网络错误的最大重试次数（含首次），设为 0 表示不重试。
// 建议设置为核心业务超时时间的 2-3 倍，例如 30-60 秒。
func NewSqlManager(db *sql.DB, leaseTimeout time.Duration, retryAttempts int) Manager {
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	return &sqlLeaseManager{
		db:            db,
		leaseTimeout:  leaseTimeout,
		retryAttempts: retryAttempts,
	}
}

func (m *sqlLeaseManager) Acquire(ctx context.Context, key, owner string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var (
		lease *Lease
		err   error
	)
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		lease, err = m.acquireOnce(ctx, key, owner)
		if err == nil {
			return lease, nil
		}
		if !isTransientNetErrBase(err) {
			return nil, err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lease, err
}

func (m *sqlLeaseManager) acquireOnce(ctx context.Context, key, owner string) (*Lease, error) {
	lease := &Lease{Key: key, Owner: owner}

	// Acquire 幂等：若已被同一 owner 持有，直接返回现有租约
	err := m.db.QueryRowContext(ctx, `
		INSERT INTO leases (key, owner, generation, updated_at)
		VALUES ($1, $2, 1, now())
		ON CONFLICT (key) DO UPDATE
			SET owner = CASE
					WHEN leases.owner = ''
						OR leases.owner = $2
						OR leases.updated_at < now() - ($3 * interval '1 millisecond')
					THEN $2
					ELSE leases.owner
				END,
				generation = CASE
					WHEN leases.owner = ''
						OR leases.owner = $2
						OR leases.updated_at < now() - ($3 * interval '1 millisecond')
					THEN leases.generation + 1
					ELSE leases.generation
				END,
				updated_at = CASE
					WHEN leases.owner = ''
						OR leases.owner = $2
						OR leases.updated_at < now() - ($3 * interval '1 millisecond')
					THEN now()
					ELSE leases.updated_at
				END
		RETURNING owner, generation
	`, key, owner, m.leaseTimeout.Milliseconds()).Scan(&lease.Owner, &lease.Generation)
	if err != nil {
		return nil, err
	}

	if lease.Owner != owner {
		return nil, ErrNotAcquired
	}

	return lease, nil
}

func (m *sqlLeaseManager) Release(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.releaseOnce(ctx, lease)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientNetErrBase(err) {
			return err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lastErr
}

func (m *sqlLeaseManager) releaseOnce(ctx context.Context, lease *Lease) error {
	result, err := m.db.ExecContext(ctx, `
		UPDATE leases
		SET owner = '', updated_at = now()
		WHERE key = $1 AND owner = $2 AND generation = $3
	`, lease.Key, lease.Owner, lease.Generation)
	if err != nil {
		return err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseExpired
	}
	return nil
}

func (m *sqlLeaseManager) Renew(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.renewOnce(ctx, lease)
		if err == nil {
			return nil
		}
		lastErr = err
		if !isTransientNetErrBase(err) {
			return err
		}
		if attempt < m.retryAttempts-1 {
			sleepBackoff(ctx, attempt)
		}
	}
	return lastErr
}

func (m *sqlLeaseManager) renewOnce(ctx context.Context, lease *Lease) error {
	result, err := m.db.ExecContext(ctx, `
		UPDATE leases
		SET updated_at = now()
		WHERE key = $1 AND owner = $2 AND generation = $3
	`, lease.Key, lease.Owner, lease.Generation)
	if err != nil {
		return err
	}

	n, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrLeaseExpired
	}
	return nil
}

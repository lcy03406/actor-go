package lease

import (
	"context"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// redisLeaseManager 基于 Redis 的租约管理器实现。
//
// 使用 Lua 脚本保证 Acquire/Release/Renew 的原子性。
// 租约数据以 Hash 结构存储在 Redis 中，包含 owner、generation 字段，并通过 TTL 控制过期。
//
// Redis 数据结构 (Hash)：
//
//	HSET lease:{key} owner {owner} generation {gen}
//	PEXPIRE lease:{key} {leaseTimeout_ms}
//
// 断线重连：
//
//	go-redis 内置连接池管理和自动重试（MaxRetries），网络中断恢复后自动重连。
//	本实现额外提供了 retryAttempts 参数，对瞬态网络错误进行指数退避重试。
//	逻辑错误（ErrNotAcquired / ErrLeaseExpired）不重试。
//	Acquire 调用是幂等的：如果 key 已被同一 owner 持有，直接返回现有租约并续期 TTL。
type redisLeaseManager struct {
	client        *redis.Client
	leaseTimeout  time.Duration
	retryAttempts int
	mu            sync.Mutex

	acquireScript *redis.Script
	releaseScript *redis.Script
	renewScript   *redis.Script
}

// NewRedisManager 创建一个基于 Redis 的租约管理器。
// leaseTimeout 指定租约过期时间：超过此时间未续约的租约可以被其他 owner 抢占。
// retryAttempts 指定瞬态网络错误的最大重试次数（含首次），设为 0 表示不重试。
// 注意：go-redis 自身也有重试机制，本 retryAttempts 是额外的一层保护。
func NewRedisManager(client *redis.Client, leaseTimeout time.Duration, retryAttempts int) Manager {
	if retryAttempts <= 0 {
		retryAttempts = 1
	}
	m := &redisLeaseManager{
		client:        client,
		leaseTimeout:  leaseTimeout,
		retryAttempts: retryAttempts,
		acquireScript: redis.NewScript(`
			local key = KEYS[1]
			local owner = ARGV[1]
			local ttl_ms = tonumber(ARGV[2])

			local current = redis.call('HGETALL', key)
			if #current == 0 then
				redis.call('HSET', key, 'owner', owner, 'generation', 1)
				redis.call('PEXPIRE', key, ttl_ms)
				return {1, owner, 1}
			end

			local cur_owner = ''
			local cur_gen = 0
			for i = 1, #current, 2 do
				if current[i] == 'owner' then
					cur_owner = current[i + 1]
				elseif current[i] == 'generation' then
					cur_gen = tonumber(current[i + 1])
				end
			end

			-- 幂等：同一 owner 重复 Acquire，返回现有租约并续期
			if cur_owner == owner then
				redis.call('PEXPIRE', key, ttl_ms)
				return {1, owner, cur_gen}
			end

			local ttl = redis.call('PTTL', key)
			if cur_owner == '' or ttl <= 0 then
				local new_gen = cur_gen + 1
				redis.call('HSET', key, 'owner', owner, 'generation', new_gen)
				redis.call('PEXPIRE', key, ttl_ms)
				return {1, owner, new_gen}
			end

			return {0, cur_owner, cur_gen}
		`),
		releaseScript: redis.NewScript(`
			local key = KEYS[1]
			local owner = ARGV[1]
			local gen = tonumber(ARGV[2])

			local cur_owner = redis.call('HGET', key, 'owner')
			local cur_gen = tonumber(redis.call('HGET', key, 'generation') or 0)

			if cur_owner == owner and cur_gen == gen then
				redis.call('HSET', key, 'owner', '')
				return 1
			end
			return 0
		`),
		renewScript: redis.NewScript(`
			local key = KEYS[1]
			local owner = ARGV[1]
			local gen = tonumber(ARGV[2])
			local ttl_ms = tonumber(ARGV[3])

			local cur_owner = redis.call('HGET', key, 'owner')
			local cur_gen = tonumber(redis.call('HGET', key, 'generation') or 0)

			if cur_owner == owner and cur_gen == gen then
				redis.call('PEXPIRE', key, ttl_ms)
				return 1
			end
			return 0
		`),
	}
	return m
}

func (m *redisLeaseManager) Acquire(ctx context.Context, key, owner string) (*Lease, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	redisKey := "lease:" + key
	ttlMs := m.leaseTimeout.Milliseconds()

	var (
		lease *Lease
		err   error
	)
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		lease, err = m.acquireOnce(ctx, redisKey, owner, ttlMs)
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

func (m *redisLeaseManager) acquireOnce(ctx context.Context, redisKey, owner string, ttlMs int64) (*Lease, error) {
	result, err := m.acquireScript.Run(ctx, m.client, []string{redisKey}, owner, ttlMs).Slice()
	if err != nil {
		return nil, err
	}

	success, ok := result[0].(int64)
	if !ok || success == 0 {
		return nil, ErrNotAcquired
	}

	leaseOwner, _ := result[1].(string)
	generation, _ := result[2].(int64)

	return &Lease{
		Key:        redisKey[6:], // 去掉 "lease:" 前缀
		Owner:      leaseOwner,
		Generation: generation,
	}, nil
}

func (m *redisLeaseManager) Release(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	redisKey := "lease:" + lease.Key

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.releaseOnce(ctx, redisKey, lease)
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

func (m *redisLeaseManager) releaseOnce(ctx context.Context, redisKey string, lease *Lease) error {
	result, err := m.releaseScript.Run(ctx, m.client, []string{redisKey}, lease.Owner, lease.Generation).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrLeaseExpired
	}
	return nil
}

func (m *redisLeaseManager) Renew(ctx context.Context, lease *Lease) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	redisKey := "lease:" + lease.Key
	ttlMs := m.leaseTimeout.Milliseconds()

	var lastErr error
	for attempt := 0; attempt < m.retryAttempts; attempt++ {
		err := m.renewOnce(ctx, redisKey, lease, ttlMs)
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

func (m *redisLeaseManager) renewOnce(ctx context.Context, redisKey string, lease *Lease, ttlMs int64) error {
	result, err := m.renewScript.Run(ctx, m.client, []string{redisKey}, lease.Owner, lease.Generation, ttlMs).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return ErrLeaseExpired
	}
	return nil
}

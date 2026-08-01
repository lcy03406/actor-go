package grain

import (
	"context"
	"encoding"
	"errors"
	"time"

	"github.com/redis/go-redis/v9"
)

// ErrRedisUnmarshalFailed 表示 Redis 驱动反序列化失败：dst 类型不满足要求。
var ErrRedisUnmarshalFailed = errors.New("grain: RedisDriver: dst must implement encoding.BinaryUnmarshaler, json.Unmarshaler, or be *[]byte")

// ErrRedisMarshalFailed 表示 Redis 驱动序列化失败：src 类型不满足要求。
var ErrRedisMarshalFailed = errors.New("grain: RedisDriver: src must implement encoding.BinaryMarshaler, json.Marshaler, or be []byte")

// RedisDriver 基于 Redis 的持久化驱动，内置租约管理。
// Key 格式：{prefix}{actorType}:{id}
// 使用 Lua 脚本保证 Load/Save/Release 的原子性。
//
// Redis Hash 结构：
//
//	HSET {prefix}{actorType}:{id} owner {nodeId} generation {gen} data {binary}
//	EXPIRE {prefix}{actorType}:{id} {leaseTimeout}
//
// 要求快照类型 S 实现 encoding.BinaryMarshaler / encoding.BinaryUnmarshaler，
// 或为可直接被 go-redis 序列化的基本类型。推荐使用 JSON/MessagePack 编解码。
type RedisDriver struct {
	client       redis.UniversalClient
	prefix       string
	nodeId       string
	leaseTimeout time.Duration
	dataTTL      time.Duration // 数据 TTL，0 表示永不过期（与租约 TTL 相同）

	loadScript    *redis.Script
	saveScript    *redis.Script
	releaseScript *redis.Script
}

// NewRedisDriver 创建 Redis 驱动。
// prefix 为 key 前缀（如 "grain:"）。
// nodeId 是当前节点标识。
// leaseTimeout 是租约超时时间（默认 DefaultLeaseTimeout）。
// dataTTL 是数据过期时间，0 表示与 leaseTimeout 相同。
func NewRedisDriver(client redis.UniversalClient, prefix string, nodeId string, leaseTimeout time.Duration, dataTTL time.Duration) *RedisDriver {
	if leaseTimeout <= 0 {
		leaseTimeout = DefaultLeaseTimeout
	}
	if dataTTL <= 0 {
		dataTTL = leaseTimeout
	}
	d := &RedisDriver{
		client:       client,
		prefix:       prefix,
		nodeId:       nodeId,
		leaseTimeout: leaseTimeout,
		dataTTL:      dataTTL,
	}
	d.loadScript = redis.NewScript(d.loadLua())
	d.saveScript = redis.NewScript(d.saveLua())
	d.releaseScript = redis.NewScript(d.releaseLua())
	return d
}

func (d *RedisDriver) loadLua() string {
	return `
		local key = KEYS[1]
		local owner = ARGV[1]
		local ttl_ms = tonumber(ARGV[2])

		local exists = redis.call('EXISTS', key)
		if exists == 0 then
			redis.call('HSET', key, 'owner', owner, 'generation', 1, 'data', '')
			redis.call('PEXPIRE', key, ttl_ms)
			return {1, owner, 1, 0}  -- ok, owner, gen, not_found
		end

		local cur_owner = redis.call('HGET', key, 'owner') or ''
		local cur_gen = tonumber(redis.call('HGET', key, 'generation') or '0')

		-- 幂等：同一 owner 重复 Load，返回现有数据并续租
		if cur_owner == owner then
			redis.call('PEXPIRE', key, ttl_ms)
			local data = redis.call('HGET', key, 'data') or ''
			return {1, owner, cur_gen, 0, data}
		end

		-- 无主或租约已过期 → 抢占
		local ttl = redis.call('PTTL', key)
		if cur_owner == '' or ttl <= 0 then
			local new_gen = cur_gen + 1
			redis.call('HSET', key, 'owner', owner, 'generation', new_gen)
			redis.call('PEXPIRE', key, ttl_ms)
			local data = redis.call('HGET', key, 'data') or ''
			return {1, owner, new_gen, 0, data}
		end

		-- 被其他节点持有且未过期
		return {0, cur_owner, cur_gen}  -- taken
	`
}

func (d *RedisDriver) saveLua() string {
	return `
		local key = KEYS[1]
		local owner = ARGV[1]
		local gen = tonumber(ARGV[2])
		local data = ARGV[3]
		local ttl_ms = tonumber(ARGV[4])

		local cur_owner = redis.call('HGET', key, 'owner') or ''
		local cur_gen = tonumber(redis.call('HGET', key, 'generation') or '0')

		if cur_owner ~= owner or cur_gen ~= gen then
			return 0
		end

		redis.call('HSET', key, 'owner', owner, 'generation', gen, 'data', data)
		if ttl_ms > 0 then
			redis.call('PEXPIRE', key, ttl_ms)
		end
		return 1
	`
}

func (d *RedisDriver) releaseLua() string {
	return `
		local key = KEYS[1]
		local owner = ARGV[1]
		local gen = tonumber(ARGV[2])

		local cur_owner = redis.call('HGET', key, 'owner') or ''
		local cur_gen = tonumber(redis.call('HGET', key, 'generation') or '0')

		if cur_owner == owner and cur_gen == gen then
			redis.call('HSET', key, 'owner', '')
			return 1
		end
		return 0
	`
}

// Load 加载快照并获取租约，Lua 脚本原子操作。
func (d *RedisDriver) Load(ctx context.Context, actorType string, id string, owner string, dst any) (*LeaseInfo, error) {
	key := d.key(actorType, id)
	ttlMs := d.leaseTimeout.Milliseconds()

	result, err := d.loadScript.Run(ctx, d.client, []string{key}, owner, ttlMs).Slice()
	if err != nil {
		return nil, err
	}

	success, _ := result[0].(int64)
	if success == 0 {
		curOwner, _ := result[1].(string)
		curGen, _ := result[2].(int64)
		return nil, &ErrLeaseTaken{
			Key:        id,
			Owner:      curOwner,
			Generation: curGen,
		}
	}

	leaseOwner, _ := result[1].(string)
	generation, _ := result[2].(int64)
	notFound, _ := result[3].(int64)

	lease := &LeaseInfo{Key: id, Owner: leaseOwner, Generation: generation}

	// 如果文档之前不存在，返回 ErrNotFound
	if notFound == 1 {
		return lease, ErrNotFound
	}

	// 解码数据
	data, _ := result[4].(string)
	if err := d.unmarshalData([]byte(data), dst); err != nil {
		return lease, err
	}

	return lease, nil
}

// Save 保存快照并续租。
func (d *RedisDriver) Save(ctx context.Context, actorType string, id string, owner string, src any, gen int64) error {
	key := d.key(actorType, id)

	data, err := d.marshalData(src)
	if err != nil {
		return err
	}

	ttlMs := d.dataTTL.Milliseconds()
	if ttlMs <= 0 {
		ttlMs = d.leaseTimeout.Milliseconds()
	}

	result, err := d.saveScript.Run(ctx, d.client, []string{key}, owner, gen, string(data), ttlMs).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return errors.New("grain: save failed, lease expired or taken by another owner")
	}
	return nil
}

// Release 释放租约（清空 owner），不删除数据。
func (d *RedisDriver) Release(ctx context.Context, actorType string, id string, owner string, gen int64) error {
	key := d.key(actorType, id)
	result, err := d.releaseScript.Run(ctx, d.client, []string{key}, owner, gen).Int64()
	if err != nil {
		return err
	}
	if result == 0 {
		return errors.New("grain: release failed, lease expired or taken by another owner")
	}
	return nil
}

func (d *RedisDriver) marshalData(src any) ([]byte, error) {
	switch v := src.(type) {
	case encoding.BinaryMarshaler:
		return v.MarshalBinary()
	case interface{ MarshalJSON() ([]byte, error) }:
		return v.MarshalJSON()
	case []byte:
		return v, nil
	default:
		return nil, ErrRedisMarshalFailed
	}
}

func (d *RedisDriver) unmarshalData(data []byte, dst any) error {
	if len(data) == 0 {
		return nil
	}
	if u, ok := dst.(encoding.BinaryUnmarshaler); ok {
		return u.UnmarshalBinary(data)
	}
	if u, ok := dst.(interface{ UnmarshalJSON([]byte) error }); ok {
		return u.UnmarshalJSON(data)
	}
	if b, ok := dst.(*[]byte); ok {
		*b = data
		return nil
	}
	return ErrRedisUnmarshalFailed
}

func (d *RedisDriver) key(actorType string, id string) string {
	return d.prefix + actorType + ":" + id
}



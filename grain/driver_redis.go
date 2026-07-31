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

// RedisDriver 基于 Redis 的持久化驱动。
// Key 格式：{prefix}{actorType}:{id}
// 使用 SETNX + generation 实现 fencing token 防止过期写入。
//
// 要求快照类型 S 实现 encoding.BinaryMarshaler / encoding.BinaryUnmarshaler，
// 或为可直接被 go-redis 序列化的基本类型。推荐使用 JSON/MessagePack 编解码。
type RedisDriver struct {
	client redis.UniversalClient
	prefix string
	ttl    time.Duration // 0 表示永不过期
}

// NewRedisDriver 创建 Redis 驱动。
// prefix 为 key 前缀（如 "grain:"），ttl 为数据过期时间（0 表示永不过期）。
func NewRedisDriver(client redis.UniversalClient, prefix string, ttl time.Duration) *RedisDriver {
	return &RedisDriver{
		client: client,
		prefix: prefix,
		ttl:    ttl,
	}
}

func (d *RedisDriver) Load(ctx context.Context, actorType string, id string, dst any) error {
	key := d.key(actorType, id)

	// 先获取 generation
	gen, err := d.client.HGet(ctx, key, "generation").Int64()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotFound
		}
		return err
	}

	// 获取快照数据
	data, err := d.client.HGet(ctx, key, "data").Bytes()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return ErrNotFound
		}
		return err
	}

	// 解码
	if u, ok := dst.(encoding.BinaryUnmarshaler); ok {
		return u.UnmarshalBinary(data)
	}

	// 尝试 JSON Unmarshaler（通过反射）
	if u, ok := dst.(interface{ UnmarshalJSON([]byte) error }); ok {
		return u.UnmarshalJSON(data)
	}

	// 回退：直接赋值字节（仅适用于 *[]byte 类型）
	if b, ok := dst.(*[]byte); ok {
		*b = data
		return nil
	}

	_ = gen
	return ErrRedisUnmarshalFailed
}

func (d *RedisDriver) Save(ctx context.Context, actorType string, id string, src any, gen int64) error {
	key := d.key(actorType, id)

	var data []byte

	switch v := src.(type) {
	case encoding.BinaryMarshaler:
		var err error
		data, err = v.MarshalBinary()
		if err != nil {
			return err
		}
	case interface{ MarshalJSON() ([]byte, error) }:
		var err error
		data, err = v.MarshalJSON()
		if err != nil {
			return err
		}
	case []byte:
		data = v
	default:
		return ErrRedisMarshalFailed
	}

	pipe := d.client.TxPipeline()

	// Lua 脚本保证原子性：仅当 key 不存在或 generation <= gen 时写入
	script := redis.NewScript(`
		local current_gen = redis.call('HGET', KEYS[1], 'generation')
		if current_gen and tonumber(current_gen) > tonumber(ARGV[2]) then
			return 0
		end
		redis.call('HSET', KEYS[1], 'data', ARGV[1], 'generation', ARGV[2])
		if ARGV[3] ~= '0' then
			redis.call('EXPIRE', KEYS[1], ARGV[3])
		end
		return 1
	`)

	ttlSec := "0"
	if d.ttl > 0 {
		ttlSec = formatDurationSec(d.ttl)
	}

	_ = pipe
	keys := []string{key}
	args := []any{string(data), gen, ttlSec}
	return script.Run(ctx, d.client, keys, args...).Err()
}

func (d *RedisDriver) key(actorType string, id string) string {
	return d.prefix + actorType + ":" + id
}

func formatDurationSec(d time.Duration) string {
	return itoa(int(d.Seconds()))
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	s := ""
	for i > 0 {
		s = string(rune('0'+i%10)) + s
		i /= 10
	}
	return s
}

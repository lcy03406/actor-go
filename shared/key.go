package shared

import (
	"fmt"
	"sync"
	"time"
)

// defInfo 是 key 定义的运行时信息，与值类型 V 无关，可安全存放在容器内。
type defInfo struct {
	pattern   string
	cooldown  time.Duration // 该 key 的推送冷却，0 表示用容器默认
	ttl       time.Duration // 读者缓存有效期，0 表示不过期
	clone     func(any) any // 发布时克隆值，nil 表示不克隆
	immutable bool          // 值为不可变（只读配置等），owner 发布后不应再修改
}

// defRegistry 保证 pattern 全局唯一：两个 Def 用了同一个 pattern 会导致
// 不同类型的 key 互相覆盖，属于定义期错误，直接 panic。
type defRegistry struct {
	mu   sync.Mutex
	seen map[string]string
}

var defs = defRegistry{seen: make(map[string]string)}

// Def 是一个 key 定义，绑定值类型 V。由 Define 创建，应当在包初始化期定义。
type Def[V any] struct {
	info *defInfo
}

// Key 是 Def 的具体实例：pattern 中的占位符已展开为完整 key 名。
type Key[V any] struct {
	name string
	info *defInfo
}

// Name 返回完整 key 名。
func (k Key[V]) Name() string { return k.name }

// Define 定义一个共享 key。pattern 全局唯一，重复定义 panic。
//
// pattern 可含 fmt 占位符，用 Of 展开为实例：
//
//	var WorldSeed = shared.Define[int64]("world/%s/seed")
//	seed, err := shared.Watch(ctx, vars, WorldSeed.Of(worldID))
func Define[V any](pattern string, opts ...KeyOption) Def[V] {
	info := &defInfo{pattern: pattern}
	for _, opt := range opts {
		opt(info)
	}
	defs.mu.Lock()
	defer defs.mu.Unlock()
	if _, ok := defs.seen[pattern]; ok {
		panic("shared: duplicate key pattern: " + pattern)
	}
	defs.seen[pattern] = pattern
	return Def[V]{info: info}
}

// Of 展开 pattern 生成具体 key 实例。
func (d Def[V]) Of(args ...any) Key[V] {
	name := d.info.pattern
	if len(args) > 0 {
		name = fmt.Sprintf(d.info.pattern, args...)
	}
	return Key[V]{name: name, info: d.info}
}

// KeyOption 配置单个 key 的行为。
type KeyOption func(*defInfo)

// WithCooldown 设置该 key 的推送冷却，覆盖容器默认值。
// 冷却期内多次发布合并为一次推送，读者最终拿到最新值。
func WithCooldown(d time.Duration) KeyOption {
	return func(i *defInfo) { i.cooldown = d }
}

// WithTTL 设置读者缓存有效期。超过 TTL 未收到推送时，Watch 会重新向容器拉取（对账自愈）。
// 0 表示不过期：适合写一次、几乎不再变更的数据（如世界种子）。
func WithTTL(d time.Duration) KeyOption {
	return func(i *defInfo) { i.ttl = d }
}

// Immutable 标记值为不可变：通常是指向只读配置的指针（如 *WorldDef），
// 容器与读者共享同一份数据，owner 发布后不应再修改其内容。
func Immutable() KeyOption {
	return func(i *defInfo) { i.immutable = true }
}

// WithClone 在发布时克隆值，用于 V 含引用语义且 owner 会在发布后继续修改的场景。
// 克隆只在发布时发生一次，推送给读者时不再克隆。
func WithClone[V any](fn func(V) V) KeyOption {
	return func(i *defInfo) {
		i.clone = func(v any) any { return fn(v.(V)) }
	}
}

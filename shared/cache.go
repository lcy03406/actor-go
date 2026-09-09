// Package shared 提供 actor 之间的共享数据框架:按需注册(订阅) + 刷新推送 + 冷却合并。
//
// 模型:
//   - 写方(某个 writer actor)拥有数据,维护单调递增版本;
//   - 读者在首次读取时向写方按需注册(Subscribe),写方登记后只向已注册读者推送(非广播);
//   - 写方写入(标准 Write 消息)后,框架按冷却策略(WithCooldown)自动把最新数据快照
//     (携带数据指针)推送给已注册读者(Refresh),读者直接替换本地缓存;
//   - 读者被驱逐/消失时,写方在推送中自动清理其注册;读者重建后首次读取会重新注册并取当前快照。
//
// 一致性语义(最终一致):
//   - 刷新推送是尽力投递,读者邮箱繁忙时可能丢失,靠下一次推送或重新订阅自愈;
//   - 冷却合并意味着读者可能跳过中间版本,但最终拿到最新数据;
//   - 跨节点推送(rpc/cluster)可作为扩展方向。
//
// 线程模型:Cache / Versioned 非线程安全,只允许在所属 actor 的 run goroutine 内访问
// (handler / Timer 回调),不应放入 State 之外的其他 goroutine。
package shared

import (
	"context"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// fetchTimeout 是读者注册订阅时等待写方快照回复的默认超时。
const fetchTimeout = 2 * time.Second

// Snapshot 是带版本的数据快照,由写方返回给读者,作为"权威版本"。
type Snapshot[T any] struct {
	Version int64
	Data    T
}

// Cache 是读者 Actor 持有的共享数据缓存。
//   - 首次 Get 时向写方按需注册(Subscribe 请求),写方登记该读者并返回当前快照;
//   - 此后写方通过 Refresh 推送直接更新缓存,Get 不再触网;
//   - 读者被驱逐后重建,缓存清空,首次 Get 会重新注册并取当前快照(自愈)。
//
// R 读者 ActorId,S 读者 State,A 写方 ActorId,T 数据类型。零值可用,
// 仅由该 reader actor 的 run goroutine 访问。
type Cache[R actor.ActorId, A actor.ActorId, S any, T any] struct {
	dataVersion int64
	data        T
	loaded      bool
}

// Get 返回缓存数据。首次调用(缓存为空)时向写方按需注册并取得当前快照;
// 已加载后直接返回本地缓存。ctx 是当前 handler 的 ActorContext;writer 是写方 ActorId。
func (c *Cache[R, A, S, T]) Get(ctx *actor.ActorContext[R, S], writer A) (T, error) {
	if c.loaded {
		return c.data, nil
	}
	callCtx, cancel := context.WithTimeout(ctx.Context(), fetchTimeout)
	defer cancel()
	rep, err := actor.Call[A](callCtx, ctx.Manager(), writer, &Subscribe[A, R, T]{Reader: ctx.Id()})
	if err != nil {
		return c.data, err
	}
	c.Set(Snapshot[T]{Version: rep.Version, Data: rep.Data})
	return c.data, nil
}

// Set 用一份权威快照替换缓存,由 Refresh 推送处理器调用。
func (c *Cache[R, A, S, T]) Set(snap Snapshot[T]) {
	c.data = snap.Data
	c.dataVersion = snap.Version
	c.loaded = true
}

// Data 返回缓存数据(已加载时)。
func (c *Cache[R, A, S, T]) Data() T {
	return c.data
}

// Version 返回本地数据的权威版本。
func (c *Cache[R, A, S, T]) Version() int64 {
	return c.dataVersion
}

// Stale 报告缓存是否为空(尚未订阅或尚未收到任何数据)。
func (c *Cache[R, A, S, T]) Stale() bool {
	return !c.loaded
}

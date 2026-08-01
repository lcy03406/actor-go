package shared

import (
	"context"

	"github.com/lcy03406/actor-go/actor"
)

// Publish 发布（更新）一个 key 的值，由权威 owner 调用。
// 容器更新版本后按冷却策略向已注册读者推送。发后不理，返回入队错误。
//
//	shared.Publish(ctx, WorldSeed.Of(worldID), s.Seed)
func Publish[R actor.ActorId, S any, V any](ctx *actor.ActorContext[R, S], k Key[V], v V) error {
	return PublishFrom(ctx.Manager(), k, v)
}

// PublishFrom 是 Publish 的变体，供只有 Manager 的场合使用。
func PublishFrom[V any](mgr *actor.Manager, k Key[V], v V) error {
	return actor.Post[storeId](mgr, shardOf(k.name), publishMessage(k, v))
}

// PublishWait 同 Publish，但等待容器处理完成（用于需要确认生效的写入）。
func PublishWait[R actor.ActorId, S any, V any](ctx *actor.ActorContext[R, S], k Key[V], v V) error {
	return PublishWaitFrom(ctx.Context(), ctx.Manager(), k, v)
}

// PublishWaitFrom 是 PublishWait 的变体，供只有 Manager 的场合使用。
func PublishWaitFrom[V any](ctx context.Context, mgr *actor.Manager, k Key[V], v V) error {
	callCtx, cancel := context.WithTimeout(ctx, fetchTimeout())
	defer cancel()
	_, err := actor.Call[storeId](callCtx, mgr, shardOf(k.name), publishMessage(k, v))
	return err
}

// Unpublish 删除一个 key。owner 下线（如家族销毁）时调用，避免容器里的 key 无限累积。
func Unpublish[R actor.ActorId, S any, V any](ctx *actor.ActorContext[R, S], k Key[V]) error {
	return UnpublishFrom(ctx.Manager(), k)
}

// UnpublishFrom 是 Unpublish 的变体，供只有 Manager 的场合使用。
func UnpublishFrom[V any](mgr *actor.Manager, k Key[V]) error {
	return actor.Post[storeId](mgr, shardOf(k.name), &unpublishReq{Name: k.name})
}

func publishMessage[V any](k Key[V], v V) *publishReq {
	value := any(v)
	if k.info != nil && k.info.clone != nil {
		value = k.info.clone(value)
	}
	return &publishReq{Name: k.name, Value: value, Info: k.info}
}

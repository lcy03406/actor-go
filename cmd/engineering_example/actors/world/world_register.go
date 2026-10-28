package world

import (
	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/shared"

	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// RegisterHandlers 注册 world actor。
//
// 普通 actor（非 grain）也要在 spawn 时显式 Open，否则处理完首条消息就回到空闲池。
// "谁持有全服唯一的权威数据"适合做成单例 actor —— 便于序列化写入，不必加锁。
func RegisterHandlers(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 64}, func(b *actor.RegistryBuilder[ids.WorldId, State]) {
		actor.RegisterSpawn(b, handleInit)
		actor.RegisterQuery(b, handleSetConfig)
	})
}

// handleInit 初始化世界并把配置发布到容器。
func handleInit(ctx *actor.ActorContext[ids.WorldId, State], _ *worldapi.Init, spawning bool) (actor.OkReply, error) {
	if spawning {
		ctx.Open()
		ctx.State().Config = worldapi.DefaultConfig
	}
	ctx.Logger().Info("world init", "config", ctx.State().Config)
	return actor.OK, publish(ctx)
}

// handleSetConfig 热更配置。PublishWait 等容器确认收到再返回：
// 调用方需要"发布已生效"的语义时不能只 Post。
func handleSetConfig(ctx *actor.ActorContext[ids.WorldId, State], req *worldapi.SetConfig, _ bool) (actor.OkReply, error) {
	if req.ExpRate > 0 {
		ctx.State().Config.ExpRate = req.ExpRate
	}
	ctx.Logger().Info("world config updated", "config", ctx.State().Config)
	return actor.OK, publish(ctx)
}

// publish 把权威配置发布进容器：容器只持副本，权威数据始终在本 actor 的 State 里。
func publish(ctx *actor.ActorContext[ids.WorldId, State]) error {
	return shared.PublishWait(ctx, worldapi.ConfigKey, ctx.State().Config)
}

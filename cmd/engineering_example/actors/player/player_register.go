package player

import (
	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/shared"

	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// RegisterHandlers 注册玩家 actor。
//
// 注册语义（哪种消息可以把 actor 叫起来、能不能回复）就是调度策略：
//   - Login：RegisterServe —— 允许创建，"第一次登录"才建出一个玩家；
//   - 其余意图 / 事件：RegisterQuery —— 只处理已存在的玩家，
//     打到不存在的玩家直接报错，不会凭空建出一个 0 级空壳。
//
// 事件也用 Query 而不是 Spawn：给一个不存在的玩家 Post 伤害，本来就该失败，
// 而不是造一个空角色把伤害记在零值上。
func RegisterHandlers(mgr *actor.Manager, pm *grain.PersistenceManager) {
	actor.Serve(mgr, actor.Options{BufMails: 128}, func(b *actor.RegistryBuilder[ids.PlayerId, PlayerGrainState]) {
		// 读者注册必须先于任何 Watch：它同时是容器回调本 actor 的入站通道。
		shared.ServeReader(b, func(s *PlayerGrainState) *shared.Reader { return &s.Data.Vars })

		// grain 托管整个生命周期：OnSpawn 自动激活（加载或新建）、定时存盘、
		// 退出时落盘并释放租约。handler 里看不到 Activate / Persist。
		grain.SetupGrain(b, pm, grain.ShotSelf[PlayerState]{}, handleSpawn)

		// 客户端意图
		actor.RegisterServe(b, handleLogin)
		actor.RegisterQuery(b, handleJoinRoom)
		actor.RegisterQuery(b, handleSay)
		actor.RegisterQuery(b, handleAttack)
		actor.RegisterQuery(b, handleStatus)

		// 跨 actor 事件
		actor.RegisterQuery(b, handleDamage)
		actor.RegisterQuery(b, handleCombatResult)
		actor.RegisterQuery(b, handleRoomEvent)
	})
}

// handleSpawn 在玩家首次被创建时、任何 handler 之前调用一次。
// created 为 true 表示磁盘上没有数据，需要初始化新角色。
func handleSpawn(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState], created bool) error {
	if created {
		ctx.State().Data = newPlayer()
	}
	ctx.Logger().Info("player activated", "id", ctx.Id(), "created", created)
	return nil
}

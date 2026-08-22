package grain

import (
	"context"

	"github.com/lcy03406/actor-go/actor"
)

// OnActivateFn 是可选的激活回调：Grain 在 OnSpawn 阶段自动 Activate 后调用。
// created 为 true 表示数据首次创建（ActivateCreated），调用方可据此初始化业务数据；
// 为 false 表示数据从存储加载（ActivateLoaded）。
// 返回 error 会沿 OnSpawn 传播，导致 spawn 失败。
type OnActivateFn[A actor.ActorId, S any, P any, K Snapshotter[S, P]] func(
	ctx *actor.ActorContext[A, State[A, S, P, K]],
	created bool,
) error

// SetupGrain 将 Grain 的生命周期托管给框架，利用 actor 的 OnSpawn / OnQuit 钩子
// 自动完成"激活 + 定时存盘 + 退出时存盘释放"，业务 handler 不再需要手动调用
// Activate / Deactivate / Persist。
//
// 行为：
//   - OnSpawn：自动 Activate（若尚未激活），调用可选的 onActivate 回调完成首次初始化，
//     并按 PersistenceManager 配置的 WithAutoPersistInterval 启动定时存盘定时器；
//     同时通过 PushOnQuit 注册退出钩子。
//   - 退出（Quit / CloseActor / KillActor）：OnQuit 钩子先 Persist（落盘或跳过 nil），
//     再 release 释放租约。定时存盘定时器会在退出时被框架自动取消。
//
// 用法：
//
//	pm := grain.NewPersistenceManager(
//	    grain.WithDriver(grain.NewJsonDriver("./data")),
//	    grain.WithNodeId("node-1"),
//	    grain.WithAutoPersistInterval(30*time.Second),
//	)
//
//	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, *grain.State[PlayerId, PlayerData, PlayerSnapshot]]) {
//	    grain.SetupGrain(b, pm, func(ctx, created bool) error {
//	        if created {
//	            ctx.State().Data.HP = 100 // 首次创建初始化
//	        }
//	        return nil
//	    })
//	    actor.RegisterSpawn(b, handleLogin)
//	})
func SetupGrain[A actor.ActorId, S any, P any, K Snapshotter[S, P]](
	b *actor.RegistryBuilder[A, State[A, S, P, K]],
	pm *PersistenceManager,
	onActivate OnActivateFn[A, S, P, K],
) {
	b.SetOnSpawn(func(ctx *actor.ActorContext[A, State[A, S, P, K]]) error {
		created, err := ctx.State().Activate(ctx, pm)
		if err != nil {
			ctx.Logger().Warn("grain activate failed", "err", err)
			return err
		}

		if onActivate != nil {
			if err := onActivate(ctx, created == ActivateCreated); err != nil {
				ctx.Logger().Warn("grain onActivate failed", "err", err)
				return err
			}
		}

		// 启动定时存盘（续租 + 落盘）。退出时框架自动取消定时器。
		if pm != nil && pm.persistInterval > 0 {
			ctx.Timer(pm.persistInterval, func() {
				if err := ctx.State().Persist(ctx); err != nil {
					ctx.Logger().Warn("grain auto persist failed", "err", err)
				}
			})
		}

		// 退出钩子：先存盘（nil snapshot 跳过写但仍续租），再释放租约。
		// 退出路径上 actor ctx 已被取消，落盘/释放租约使用 context.WithoutCancel
		// 派生的 context，避免 Mongo/Redis 驱动因 context canceled 拒绝写入，
		// 导致最终状态丢失（grain persist on quit failed）。
		ctx.Control().PushOnQuit(func() {
			quitCtx := context.WithoutCancel(ctx.Context())
			if err := ctx.State().persist(ctx, quitCtx); err != nil {
				ctx.Logger().Warn("grain persist on quit failed", "err", err)
			}
			ctx.State().release(ctx, string(ctx.Id().ActorType()), quitCtx)
		})

		return nil
	})
}

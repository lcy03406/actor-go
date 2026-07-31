// Package grain 提供带租约管理的持久化 Actor 工具。
//
// State[A, D, S, T] 封装租约和业务数据，D 需实现 Snapshotter[S]。
// 方法 Deactivate/Persist/RenewLease 直接挂在 State 上，
// handler 中通过 ctx.State() 即可调用。
//
// 用法：
//
//	pm := grain.NewPersistenceManager(
//	    grain.WithDriver(grain.NewJsonDriver("./data")),
//	    grain.WithLeaseManager(lease.NewLocalManager()),
//	    grain.WithNodeId("node-1"),
//	)
//
//	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, *grain.State[PlayerId, PlayerData, PlayerSnapshot]]) {
//	    actor.RegisterSpawn(b, grain.WrapSpawn(pm, handleLogin))
//	    actor.RegisterQuery(b, handlePing)
//	})
//
//	func handleLogin(ctx *ActorContext[PlayerId, *grain.State[PlayerId, PlayerData, PlayerSnapshot]], ...) {
//	    ctx.State().Data.HP = 100
//	    ctx.State().Persist(ctx)
//	}
package grain

import (
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/lease"
)

// ─── State ───

// State 封装 Grain 运行时状态。
// A 是 ActorId 类型，D 是业务数据类型（值类型），S 是快照类型指针。
type State[A actor.ActorId, D any, S any, T Snapshotter[D, S]] struct {
	Data  D
	lease *lease.Lease
	pm    *PersistenceManager
}

// Deactivate 保存 Data、释放租约、退出 Grain。
func (s *State[A, D, S, T]) Deactivate(ctx *actor.ActorContext[A, State[A, D, S, T]]) {
	id := ctx.Id()
	actorType := string(id.ActorType())

	var gen int64
	if s.lease != nil {
		gen = s.lease.Generation
	}

	if err := s.pm.driver.Save(ctx.Context(), actorType, id.String(), s.toSnapshot(), gen); err != nil {
		ctx.Logger().Error("grain deactivate: save failed", "id", id, "err", err)
	} else {
		ctx.Logger().Info("grain deactivate: saved", "id", id)
	}

	if s.lease != nil {
		if err := s.pm.leaseManager.Release(ctx.Context(), s.lease); err != nil {
			ctx.Logger().Warn("grain deactivate: release lease failed", "id", id, "err", err)
		}
	}

	ctx.Quit()
}

// Persist 主动保存 Data，不退出 Grain。
func (s *State[A, D, S, T]) Persist(ctx *actor.ActorContext[A, State[A, D, S, T]]) error {
	var gen int64
	if s.lease != nil {
		gen = s.lease.Generation
	}
	return s.pm.driver.Save(ctx.Context(), string(ctx.Id().ActorType()), ctx.Id().String(), s.toSnapshot(), gen)
}

// RenewLease 手动续约。RenewInterval > 0 时框架自动续约。
func (s *State[A, D, S, T]) RenewLease(ctx *actor.ActorContext[A, State[A, D, S, T]]) error {
	if s.lease != nil {
		return s.pm.leaseManager.Renew(ctx.Context(), s.lease)
	}
	return nil
}

func (s *State[A, D, S, T]) toSnapshot() *S {
	var t T
	return t.TakeSnapshot(&s.Data)
}

// ─── 生命周期内部函数 ───

func activate[A actor.ActorId, D any, S any, T Snapshotter[D, S]](ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager) error {
	id := ctx.Id()
	actorType := string(id.ActorType())

	le, err := pm.leaseManager.Acquire(ctx.Context(), id.String(), pm.nodeId)
	if err != nil {
		ctx.Logger().Warn("grain activate: acquire failed", "id", id, "err", err)
		return err
	}

	state := ctx.State()
	var t T
	snapshot := t.NewPersist(&state.Data)
	err = pm.driver.Load(ctx.Context(), actorType, id.String(), snapshot)
	if err != nil && err != ErrNotFound {
		ctx.Logger().Error("grain activate: load failed", "id", id, "err", err)
		_ = pm.leaseManager.Release(ctx.Context(), le)
		return err
	}

	if err == nil {
		t.LoadSnapshot(&state.Data, snapshot)
	}
	state.pm = pm
	state.lease = le
	ctx.Logger().Info("grain activated", "id", id, "generation", le.Generation)

	if pm.renewInterval > 0 {
		scheduleRenew(ctx, pm.renewInterval)
	}
	return nil
}

func scheduleRenew[A actor.ActorId, D any, S any, T Snapshotter[D, S]](ctx *actor.ActorContext[A, State[A, D, S, T]], interval time.Duration) {
	ctx.Timer(interval, func() {
		state := ctx.State()
		if state.lease != nil {
			if err := state.pm.leaseManager.Renew(ctx.Context(), state.lease); err != nil {
				ctx.Logger().Warn("lease renew failed, deactivating",
					"id", ctx.Id(), "err", err)
				state.Deactivate(ctx)
				return
			}
			scheduleRenew(ctx, interval)
		}
	})
}

// ─── handler 包装 ───

// WrapSpawn 包装 handler，spawning 时自动执行激活（抢租约 + 加载数据 + 启动续约）。
// 用于 actor.RegisterSpawn / RegisterServe。
// D 需实现 Snapshotter[S]，由 State[A, D, S, T] 的 D 推导。
func WrapSpawn[A actor.ActorId, D any, S any, T Snapshotter[D, S], Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	pm *PersistenceManager,
	fn func(*actor.ActorContext[A, State[A, D, S, T]], Q, bool) (R, error),
) func(*actor.ActorContext[A, State[A, D, S, T]], Q, bool) (R, error) {
	return func(actx *actor.ActorContext[A, State[A, D, S, T]], req Q, spawning bool) (R, error) {
		if spawning {
			if err := activate[A, D, S, T](actx, pm); err != nil {
				var zero R
				return zero, err
			}
		}
		return fn(actx, req, spawning)
	}
}

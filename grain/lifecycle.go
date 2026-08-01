// Package grain 提供带租约管理的持久化 Actor 工具。
//
// State[A, D, S, T] 封装租约和业务数据，D 需实现 Snapshotter[S]。
// 方法 Deactivate/Persist 直接挂在 State 上，
// handler 中通过 ctx.State() 即可调用。
//
// 租约已内置在 Driver 中，Load 时自动获取租约，Save/Persist 时自动续租。
// 续租不再由框架定时器驱动，而是由用户逻辑主动调用 Persist 时顺带完成。
//
// 用法：
//
//	pm := grain.NewPersistenceManager(
//	    grain.WithDriver(grain.NewJsonDriver("./data")),
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
//	    ctx.State().Persist(ctx) // 保存数据 + 续租
//	}
package grain

import (
	"errors"

	"github.com/lcy03406/actor-go/actor"
)

// ─── State ───

// State 封装 Grain 运行时状态。
// A 是 ActorId 类型，D 是业务数据类型（值类型），S 是快照类型指针。
type State[A actor.ActorId, D any, S any, T Snapshotter[D, S]] struct {
	Data  D
	lease *LeaseInfo
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

	if err := s.pm.driver.Save(ctx.Context(), actorType, id.String(), s.pm.nodeId, s.toSnapshot(), gen); err != nil {
		ctx.Logger().Error("grain deactivate: save failed", "id", id, "err", err)
	} else {
		ctx.Logger().Info("grain deactivate: saved", "id", id)
	}

	if s.lease != nil {
		if err := s.pm.driver.Release(ctx.Context(), actorType, id.String(), s.pm.nodeId, gen); err != nil {
			ctx.Logger().Warn("grain deactivate: release lease failed", "id", id, "err", err)
		}
	}

	ctx.Quit()
}

// Persist 主动保存 Data 并续租，不退出 Grain。
// 每次调用都会续租，重置租约 TTL。
func (s *State[A, D, S, T]) Persist(ctx *actor.ActorContext[A, State[A, D, S, T]]) error {
	var gen int64
	if s.lease != nil {
		gen = s.lease.Generation
	}
	return s.pm.driver.Save(ctx.Context(), string(ctx.Id().ActorType()), ctx.Id().String(), s.pm.nodeId, s.toSnapshot(), gen)
}

func (s *State[A, D, S, T]) toSnapshot() *S {
	var t T
	return t.TakeSnapshot(&s.Data)
}

// ─── 生命周期内部函数 ───

// activate 激活 Grain：通过 Driver.Load 原子完成"获取租约 + 加载快照"。
func activate[A actor.ActorId, D any, S any, T Snapshotter[D, S]](ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager) error {
	id := ctx.Id()
	actorType := string(id.ActorType())

	state := ctx.State()
	var t T
	snapshot := t.NewPersist(&state.Data)
	lease, err := pm.driver.Load(ctx.Context(), actorType, id.String(), pm.nodeId, snapshot)
	if err != nil {
		var taken *ErrLeaseTaken
		if errors.As(err, &taken) {
			ctx.Logger().Warn("grain activate: lease taken by another owner",
				"id", id, "owner", taken.Owner, "generation", taken.Generation)
		} else if errors.Is(err, ErrNotFound) {
			// 首次激活，lease 仍然有效，数据用零值
		} else {
			ctx.Logger().Error("grain activate: load failed", "id", id, "err", err)
			return err
		}
	}

	if err == nil {
		t.LoadSnapshot(&state.Data, snapshot)
	}
	state.pm = pm
	state.lease = lease
	ctx.Logger().Info("grain activated", "id", id, "generation", lease.Generation)
	return nil
}

// ─── handler 包装 ───

// WrapSpawn 包装 handler，spawning 时自动执行激活（获取租约 + 加载数据）。
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

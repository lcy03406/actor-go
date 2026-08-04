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
//	    actor.RegisterSpawn(b, func(ctx *ActorContext[PlayerId, *grain.State[...]], req *Login, spawning bool) (actor.OkReply, error) {
//	        res, err := ctx.State().Activate(ctx, pm)
//	        if err != nil {
//	            return actor.OK, err
//	        }
//	        if res == grain.ActivateCreated {
//	            ctx.State().Data.HP = req.InitHP // 首次激活：初始化数据
//	        }
//	        return actor.OK, nil
//	    })
//	    actor.RegisterQuery(b, handlePing)
//	})
//
//	func handlePing(ctx *ActorContext[PlayerId, *grain.State[...]], ...) { ... }
package grain

import (
	"errors"

	"github.com/lcy03406/actor-go/actor"
)

// ─── ActivateResult ───

// ActivateResult 表示 Activate 的结果：数据是新建的还是从存储加载的。
type ActivateResult int

const (
	// ActivateCreated 表示数据不存在，使用零值初始化（首次激活）。
	ActivateCreated ActivateResult = iota
	// ActivateLoaded 表示数据已从存储加载成功。
	ActivateLoaded
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
// 必须在 Grain 激活后调用（State.pm 不为 nil），否则 panic。
// 激活方式：在 spawn/serve 回调中调用 State.Activate(ctx, pm)。
func (s *State[A, D, S, T]) Deactivate(ctx *actor.ActorContext[A, State[A, D, S, T]]) {
	if s.pm == nil {
		panic("grain: Deactivate called without PersistenceManager. " +
			"Activate the Grain first via State.Activate(ctx, pm) in a spawn/serve handler.")
	}
	id := ctx.Id()
	actorType := string(id.ActorType())

	gen := s.lease.Generation
	var t T
	snap := t.TakeSnapshot(&s.Data)
	if snap != nil {
		if err := s.pm.driver.Save(ctx.Context(), actorType, id.String(), s.pm.nodeId, snap, gen); err != nil {
			ctx.Logger().Error("grain deactivate: save failed", "id", id, "err", err)
		} else {
			ctx.Logger().Info("grain deactivate: saved", "id", id)
		}
	} else {
		// 快照为 nil：跳过存盘（不写文件），但仍通过 Save 续租以保活租约。
		if err := s.pm.driver.Save(ctx.Context(), actorType, id.String(), s.pm.nodeId, nil, gen); err != nil {
			ctx.Logger().Error("grain deactivate: renew lease failed", "id", id, "err", err)
		}
	}

	s.release(ctx, actorType)
	ctx.Quit()
}

// release 释放租约（清空 owner），不写数据、不退出 Grain。
// 供 Deactivate 与生命周期钩子（OnQuit）复用。
func (s *State[A, D, S, T]) release(ctx *actor.ActorContext[A, State[A, D, S, T]], actorType string) {
	if s.lease == nil {
		return
	}
	if err := s.pm.driver.Release(ctx.Context(), actorType, ctx.Id().String(), s.pm.nodeId, s.lease.Generation); err != nil {
		ctx.Logger().Warn("grain: release lease failed", "id", ctx.Id(), "err", err)
	}
}

// Persist 主动保存 Data 并续租，不退出 Grain。
// 每次调用都会续租，重置租约 TTL。
// 必须在 Grain 激活后调用（State.pm 不为 nil），否则 panic。
func (s *State[A, D, S, T]) Persist(ctx *actor.ActorContext[A, State[A, D, S, T]]) error {
	if s.pm == nil {
		panic("grain: Persist called without PersistenceManager. " +
			"Activate the Grain first via State.Activate(ctx, pm) in a spawn/serve handler.")
	}
	gen := s.lease.Generation
	var t T
	snap := t.TakeSnapshot(&s.Data)
	if snap == nil {
		// 快照为 nil：本次不存盘（例如状态无变化），但仍续租以保活租约。
		ctx.Logger().Debug("grain persist: snapshot nil, skip save but renew lease", "id", ctx.Id())
		return s.pm.driver.Save(ctx.Context(), string(ctx.Id().ActorType()), ctx.Id().String(), s.pm.nodeId, nil, gen)
	}
	return s.pm.driver.Save(ctx.Context(), string(ctx.Id().ActorType()), ctx.Id().String(), s.pm.nodeId, snap, gen)
}

// ─── 生命周期 ───

// Activate 续租 / 加载或创建数据，并将 Actor 置为活跃状态。
//
// 必须在 spawn/serve 回调中显式调用（框架不再自动激活）。
// 它内部完成：
//  1. 通过 Driver.Load 原子地"获取租约 + 加载快照"（或 ErrNotFound 时用零值创建）
//  2. 调用 ActorContext.Open 将 Actor 唤醒为活跃态
//
// 返回值表明数据是首次创建（ActivateCreated）还是从存储加载（ActivateLoaded）。
// 调用方据此决定是否需要初始化业务数据。
//
// 若 Actor 已通过其他方式 Open（例如非 Grain 场景），Activate 仍会执行加载与续租，
// 并安全地成为活跃态的无操作 Open。
//
// 示例：
//
//	res, err := ctx.State().Activate(ctx, pm)
//	if err != nil { return ..., err }
//	if res == grain.ActivateCreated {
//	    ctx.State().Data.HP = req.InitHP // 首次激活初始化
//	}
func (s *State[A, D, S, T]) Activate(ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager) (ActivateResult, error) {
	res, err := activate(ctx, pm)
	if err != nil {
		return res, err
	}
	ctx.Open()
	return res, nil
}

// Activated 返回 Grain 是否已激活（已获取租约）。
func (s *State[A, D, S, T]) Activated() bool {
	return s.pm != nil && s.lease != nil
}

// activate 激活 Grain：通过 Driver.Load 原子完成"获取租约 + 加载快照"。
// 返回数据是新建的还是加载的。
func activate[A actor.ActorId, D any, S any, T Snapshotter[D, S]](ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager) (ActivateResult, error) {
	// 幂等：已激活（已持有 PersistenceManager）则直接返回，避免重复 Load 造成租约竞态。
	if ctx.State().pm != nil {
		return ActivateLoaded, nil
	}
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
			return ActivateLoaded, err
		} else if errors.Is(err, ErrNotFound) {
			// 首次激活，lease 仍然有效，数据用零值
			state.pm = pm
			state.lease = lease
			ctx.Logger().Info("grain activated (created)", "id", id, "generation", lease.Generation)
			return ActivateCreated, nil
		}
		ctx.Logger().Error("grain activate: load failed", "id", id, "err", err)
		return ActivateLoaded, err
	}

	t.LoadSnapshot(&state.Data, snapshot)
	state.pm = pm
	state.lease = lease
	ctx.Logger().Info("grain activated (loaded)", "id", id, "generation", lease.Generation)
	return ActivateLoaded, nil
}

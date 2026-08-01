// Package grain 提供带租约管理的持久化 Actor 工具。
//
// State[A, D, S, T] 封装租约和业务数据：D 是业务数据，S 是快照类型，
// T 是 Snapshotter[D, S] 实现（值类型或指针类型均可）。
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
//	type GrainState = grain.State[PlayerId, PlayerData, PlayerData, grain.ShotSelf[PlayerData]]
//
//	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
//	    actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, GrainState], req *Login, spawning bool) (actor.OkReply, error) {
//	        // 快照器实例由调用方显式传入（ShotSelf 无状态，传零值即可）
//	        res, err := ctx.State().Activate(ctx, pm, grain.ShotSelf[PlayerData]{})
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
//	func handlePing(ctx *actor.ActorContext[PlayerId, GrainState], ...) { ... }
package grain

import (
	"context"
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
// A 是 ActorId 类型，D 是业务数据类型（值类型），S 是快照类型，T 是快照器实现。
// T 只负责类型层面（锚定 S 并校验实例形态），实例由 Activate 显式传入。
type State[A actor.ActorId, D any, S any, T Snapshotter[D, S]] struct {
	Data  D
	lease *LeaseInfo
	pm    *PersistenceManager
	// snap 是调用方在 Activate 时传入的快照器实例，Persist / Deactivate 复用它。
	snap T
}

// Deactivate 保存 Data、释放租约、退出 Grain。
// 必须在 Grain 激活后调用（State.pm 不为 nil），否则 panic。
// 激活方式：在 spawn/serve 回调中调用 State.Activate(ctx, pm, snap)。
func (s *State[A, D, S, T]) Deactivate(ctx *actor.ActorContext[A, State[A, D, S, T]]) {
	if s.pm == nil {
		panic("grain: Deactivate called without PersistenceManager. " +
			"Activate the Grain first via State.Activate(ctx, pm, snap) in a spawn/serve handler.")
	}
	id := ctx.Id()
	actorType := string(id.ActorType())

	gen := s.lease.Generation
	snapshot := s.snap.TakeSnapshot(&s.Data)
	if snapshot != nil {
		if err := s.pm.driver.Save(ctx.Context(), actorType, id.String(), s.pm.nodeId, snapshot, gen); err != nil {
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

	s.release(ctx, actorType, ctx.Context())
	ctx.Quit()
}

// release 释放租约（清空 owner），不写数据、不退出 Grain。
// 供 Deactivate 与生命周期钩子（OnQuit）复用。c 为执行 Release 所用的 context，
// 退出钩子场景需传入不受 actor 取消影响的 context（见 persist 的调用方）。
func (s *State[A, D, S, T]) release(ctx *actor.ActorContext[A, State[A, D, S, T]], actorType string, c context.Context) {
	if s.lease == nil {
		return
	}
	if err := s.pm.driver.Release(c, actorType, ctx.Id().String(), s.pm.nodeId, s.lease.Generation); err != nil {
		ctx.Logger().Warn("grain: release lease failed", "id", ctx.Id(), "err", err)
	}
}

// Persist 主动保存 Data 并续租，不退出 Grain。
// 每次调用都会续租，重置租约 TTL。
// 必须在 Grain 激活后调用（State.pm 不为 nil），否则 panic。
func (s *State[A, D, S, T]) Persist(ctx *actor.ActorContext[A, State[A, D, S, T]]) error {
	return s.persist(ctx, ctx.Context())
}

// persist 落盘 + 续租核心实现，使用调用方指定的 context。
// 退出钩子（OnQuit）场景下 actor ctx 已被取消，须传入不受取消影响的
// context（context.WithoutCancel），否则 Mongo/Redis 驱动会返回 context canceled，
// 导致最终状态无法落盘。
func (s *State[A, D, S, T]) persist(ctx *actor.ActorContext[A, State[A, D, S, T]], c context.Context) error {
	if s.pm == nil {
		panic("grain: Persist called without PersistenceManager. " +
			"Activate the Grain first via State.Activate(ctx, pm, snap) in a spawn/serve handler.")
	}
	gen := s.lease.Generation
	snapshot := s.snap.TakeSnapshot(&s.Data)
	if snapshot == nil {
		// 快照为 nil：本次不存盘（例如状态无变化），但仍续租以保活租约。
		ctx.Logger().Debug("grain persist: snapshot nil, skip save but renew lease", "id", ctx.Id())
		return s.pm.driver.Save(c, string(ctx.Id().ActorType()), ctx.Id().String(), s.pm.nodeId, nil, gen)
	}
	return s.pm.driver.Save(c, string(ctx.Id().ActorType()), ctx.Id().String(), s.pm.nodeId, snapshot, gen)
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
// snap 是调用方给出的快照器实例，保存在 State 上供 Persist / Deactivate 复用。
// 实例类型由 snap 参数与 T 在编译期对齐，因此框架不做任何反射或判空：
//   - T 为值类型时传零值字面量，如 grain.ShotSelf[PlayerData]{}；
//   - T 为指针类型时传实例地址，如 &MySnapshotter{}；
//   - 仅当快照器的方法用指针接收者且方法体不访问接收者时，传 nil 才是安全的
//     （ShotSelf 是值接收者，写成 *grain.ShotSelf[D] 时必须传 &grain.ShotSelf[D]{}）。
//
// 示例：
//
//	res, err := ctx.State().Activate(ctx, pm, grain.ShotSelf[PlayerData]{})
//	if err != nil { return ..., err }
//	if res == grain.ActivateCreated {
//	    ctx.State().Data.HP = req.InitHP // 首次激活初始化
//	}
func (s *State[A, D, S, T]) Activate(ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager, snap T) (ActivateResult, error) {
	res, err := activate(ctx, pm, snap)
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
// snap 是调用方给出的快照器实例；已激活时本次传入的实例被忽略（沿用首次激活的那个）。
// 返回数据是新建的还是加载的。
func activate[A actor.ActorId, D any, S any, T Snapshotter[D, S]](ctx *actor.ActorContext[A, State[A, D, S, T]], pm *PersistenceManager, snap T) (ActivateResult, error) {
	// 幂等：已激活（已持有 PersistenceManager）则直接返回，避免重复 Load 造成租约竞态。
	if ctx.State().pm != nil {
		return ActivateLoaded, nil
	}
	id := ctx.Id()
	actorType := string(id.ActorType())

	state := ctx.State()
	snapshot := snap.NewPersist(&state.Data)
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
			state.snap = snap
			ctx.Logger().Info("grain activated (created)", "id", id, "generation", lease.Generation)
			return ActivateCreated, nil
		}
		ctx.Logger().Error("grain activate: load failed", "id", id, "err", err)
		return ActivateLoaded, err
	}

	snap.LoadSnapshot(&state.Data, snapshot)
	state.pm = pm
	state.lease = lease
	state.snap = snap
	ctx.Logger().Info("grain activated (loaded)", "id", id, "generation", lease.Generation)
	return ActivateLoaded, nil
}

// ControlState 获取Control和State的便捷函数
func ControlState[A actor.ActorId, D any, S any, T Snapshotter[D, S]](a *actor.ActorContext[A, State[A, D, S, T]]) (*actor.ActorControl, A, *D) {
	return a.Control(), a.Id(), &a.State().Data
}

// handlerCISFunc 是接受ActorControl的Handler形式
type HandlerCISFunc[A actor.ActorId, D any, S any, T Snapshotter[D, S], Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any] func(a *actor.ActorControl, id A, s *D, req Q, spawning bool) (R, error)

// CIS 将handlerCISFunc包装成Handler
func CIS[A actor.ActorId, D any, S any, T Snapshotter[D, S], Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](cis HandlerCISFunc[A, D, S, T, Q, R, Q0, R0]) actor.HandlerFunc[A, State[A, D, S, T], Q, R, Q0, R0] {
	return func(a *actor.ActorContext[A, State[A, D, S, T]], req Q, spawning bool) (R, error) {
		c, i, s := ControlState(a)
		return cis(c, i, s, req, spawning)
	}
}

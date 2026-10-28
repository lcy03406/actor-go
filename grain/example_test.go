package grain_test

// 本文件是 README「Grain」一节的示例来源：region 片段由 cmd/readmegen 注入。

import (
	"fmt"
	"log/slog"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
)

// ─── 示例业务类型 ───

type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "GrainPlayer" }
func (id PlayerId) String() string {
	return fmt.Sprintf("GrainPlayer(%d,%s)", id.ServerId, id.OpenId)
}

// PlayerData 是业务数据（既当持久化快照，用 ShotSelf 直接序列化）。
type PlayerData struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}

// GrainState 的最后一个类型参数是 Snapshotter 实现，这里用值类型 grain.ShotSelf[PlayerData]，
// 注册时传零值 grain.ShotSelf[PlayerData]{}；写成指针类型则需传 &grain.ShotSelf[PlayerData]{}。
type (
	GrainState = grain.State[PlayerId, PlayerData, PlayerData, grain.ShotSelf[PlayerData]]
	GrainCtx   = actor.ActorContext[PlayerId, GrainState]
)

type Login struct {
	InitHP int `json:"initHP"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
	Damage int `json:"damage"`
}

type AttackReply struct {
	RemainingHP int `json:"remainingHP"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type SaveNow struct{}

func (*SaveNow) ReqType(_ PlayerId, _ actor.OkReply) string { return "SaveNow" }

func Example_setup() {
	mgr := actor.NewManager(slog.Default())

	// region: setup
	// PersistenceManager 聚合 driver + 租约；租约内建于 driver，无需单独的租约管理器。
	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver("./grain_data")),
		grain.WithNodeId("node-1"),
		grain.WithAutoPersistInterval(5*time.Second),
	)

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
		// 生命周期托管给框架：OnSpawn 自动 Activate + 定时存盘，退出时自动 Persist + 释放租约。
		grain.SetupGrain(b, pm, grain.ShotSelf[PlayerData]{}, func(ctx *GrainCtx, created bool) error {
			if created { // 仅首次创建时初始化业务数据
				ctx.State().Data.HP = 100
				ctx.State().Data.Level = 1
			}
			return nil
		})

		// 业务 handler 只关心业务：不再手动 Activate / Persist / Deactivate。
		actor.RegisterSpawn(b, func(ctx *GrainCtx, req *Login, _ bool) (actor.OkReply, error) {
			ctx.State().Data.HP = req.InitHP
			return actor.OK, nil
		})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().Data.HP -= req.Damage
			return &AttackReply{RemainingHP: ctx.State().Data.HP}, nil
		})
	})
	// endregion: setup

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_state() {
	mgr := actor.NewManager(slog.Default())
	pm := grain.NewPersistenceManager(grain.WithNodeId("node-1"))

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, GrainState]) {
		grain.SetupGrain(b, pm, grain.ShotSelf[PlayerData]{}, nil)

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *SaveNow, _ bool) (actor.OkReply, error) {
			// region: state
			state := ctx.State()
			ctx.Logger().Info("hp", "hp", state.Data.HP) // Data：业务数据（D）

			if err := state.Persist(ctx); err != nil { // 立即存盘并续租，Actor 继续运行
				return actor.OK, err
			}
			state.Deactivate(ctx) // 存盘 + 释放租约 + 退出
			// endregion: state
			return actor.OK, nil
		})
	})

	mgr.CloseManager()
	mgr.JoinManager()
}

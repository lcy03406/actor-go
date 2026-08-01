package shared_test

// 本文件是 README「共享数据」一节的示例来源：region 片段由 cmd/readmegen 注入。

import (
	"log/slog"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/shared"
)

// ─── 示例业务类型 ───

type WorldId struct {
	WorldId string `json:"worldId"`
}

func (id WorldId) ActorType() actor.ActorType { return "World" }
func (id WorldId) String() string             { return "World(" + id.WorldId + ")" }

type WorldState struct {
	Seed int64 `json:"seed"`
}

type InitWorld struct {
	Seed int64 `json:"seed"`
}

func (*InitWorld) ReqType(_ WorldId, _ actor.OkReply) string { return "InitWorld" }

type RegionId struct {
	RegionId string `json:"regionId"`
}

func (id RegionId) ActorType() actor.ActorType { return "Region" }
func (id RegionId) String() string             { return "Region(" + id.RegionId + ")" }

// RegionState 内嵌 shared.Reader：读者的本地缓存表，只允许在该 actor 的 run goroutine 内访问。
type RegionState struct {
	Vars shared.Reader
	Seed int64
}

type EnterRegion struct {
	World WorldId
}

func (*EnterRegion) ReqType(_ RegionId, _ actor.OkReply) string { return "EnterRegion" }

type TechState struct {
	Level int `json:"level"`
}

// region: keys
// 定义 key：值类型在编译期与 key 绑定，pattern 全局唯一，可带占位符。
var (
	WorldSeed  = shared.Define[int64]("world/%s/seed", shared.Immutable())
	FamilyTech = shared.Define[TechState]("family/%s/tech",
		shared.WithCooldown(50*time.Millisecond), shared.WithTTL(30*time.Second))
)

// endregion: keys

func Example_usage() {
	mgr := actor.NewManager(slog.Default())
	worldID := WorldId{WorldId: "w1"}
	regionID := RegionId{RegionId: "r1"}

	// region: usage
	// 启动：注册容器 Group（分片数在进程内必须一致，重复 Serve 会 panic）
	shared.Serve(mgr, shared.Options{Shards: 4, Cooldown: 50 * time.Millisecond})

	// owner：权威数据仍在自己手里，变更时 Publish，容器只负责分发
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[WorldId, WorldState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[WorldId, WorldState], req *InitWorld, _ bool) (actor.OkReply, error) {
			ctx.Open()
			ctx.State().Seed = req.Seed
			return actor.OK, shared.Publish(ctx, WorldSeed.Of(ctx.Id()), ctx.State().Seed)
		})
	})

	// 读者：Group 只需注册一次 ServeReader，之后可 Watch 任意 key
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[RegionId, RegionState]) {
		shared.ServeReader(b, func(s *RegionState) *shared.Reader { return &s.Vars })

		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[RegionId, RegionState], req *EnterRegion, _ bool) (actor.OkReply, error) {
			ctx.Open()

			// 首次 Watch 订阅并取回快照，之后命中本地缓存（由容器推送更新）
			seed, err := shared.Watch(ctx, &ctx.State().Vars, WorldSeed.Of(req.World))
			if err != nil {
				return actor.OK, err
			}
			ctx.State().Seed = seed

			// 需要"必须最新"的读取用 Fetch：每次触网、不订阅、不写缓存
			latest, err := shared.Fetch(ctx, WorldSeed.Of(req.World))
			if err != nil {
				return actor.OK, err
			}
			ctx.Logger().Info("joined", "seed", latest)
			return actor.OK, nil
		})
	})

	// 业务侧：先由 owner 发布，再让读者订阅
	_ = actor.Post(mgr, worldID, &InitWorld{Seed: 42})
	_ = actor.Post(mgr, regionID, &EnterRegion{World: worldID})
	// endregion: usage

	mgr.CloseManager()
	mgr.JoinManager()
}

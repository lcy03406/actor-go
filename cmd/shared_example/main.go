// 共享数据容器示例：owner 按 key 发布，读者按 key 订阅，容器负责版本推进与冷却合并推送。
//
// 场景：World 是权威数据源，Region 只读 —— 它不 Call World，而是订阅 World 发布的 key，
// 由容器把变更推送到读者的本地缓存里。
//
// 运行：go run ./cmd/shared_example/
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/shared"
)

// ─── Key 定义：值类型与 key 在编译期绑定 ───

var (
	// Immutable：值发布后不再修改（只读配置、种子等），容器可以把同一份值分发给所有读者。
	DemoWorldSeed = shared.Define[int64]("demo/world/%s/seed", shared.Immutable())
	// WithCooldown：高频变更的 key 在冷却窗口内合并推送，避免刷爆读者邮箱。
	// WithTTL：读者缓存的过期时间，过期后下一次 Watch 会重新拉取。
	DemoWorldWeather = shared.Define[string]("demo/world/%s/weather",
		shared.WithCooldown(100*time.Millisecond), shared.WithTTL(30*time.Second))
)

// ─── World：owner ───

type WorldId struct{ Name string }

func (id WorldId) ActorType() actor.ActorType { return "DemoWorld" }
func (id WorldId) String() string             { return "DemoWorld(" + id.Name + ")" }

type WorldState struct {
	Seed    int64
	Weather string
}

type InitWorld struct{ Seed int64 }

func (*InitWorld) ReqType(_ WorldId, _ actor.OkReply) string { return "InitWorld" }

type SetWeather struct{ Weather string }

func (*SetWeather) ReqType(_ WorldId, _ actor.OkReply) string { return "SetWeather" }

// ─── Region：读者 ───

type RegionId struct{ Name string }

func (id RegionId) ActorType() actor.ActorType { return "DemoRegion" }
func (id RegionId) String() string             { return "DemoRegion(" + id.Name + ")" }

// RegionState 嵌入 shared.Reader：读者本地缓存，只在该 actor 的 goroutine 内访问。
type RegionState struct {
	Vars     shared.Reader
	World    WorldId
	Seed     int64
	Weather  string
	PushSeen int
}

type EnterRegion struct{ World WorldId }

func (*EnterRegion) ReqType(_ RegionId, _ actor.OkReply) string { return "EnterRegion" }

type ShowState struct{}

type StateReply struct {
	Seed    int64  `json:"seed"`
	Weather string `json:"weather"`
}

func (*ShowState) ReqType(_ RegionId, _ *StateReply) string { return "ShowState" }

// ─── 注册 ───

var options100 = actor.Options{BufMails: 100}

func setupWorld(mgr *actor.Manager) {
	actor.Serve(mgr, options100, func(b *actor.RegistryBuilder[WorldId, WorldState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[WorldId, WorldState], req *InitWorld, _ bool) (actor.OkReply, error) {
			ctx.Open()
			ctx.SetState(WorldState{Seed: req.Seed, Weather: "sunny"})

			// 发布：容器只持有副本，权威数据仍在本 actor 的 State 里。
			key := DemoWorldSeed.Of(ctx.Id().Name)
			if err := shared.Publish(ctx, key, ctx.State().Seed); err != nil {
				return actor.OK, err
			}
			return actor.OK, shared.Publish(ctx, DemoWorldWeather.Of(ctx.Id().Name), ctx.State().Weather)
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[WorldId, WorldState], req *SetWeather, _ bool) (actor.OkReply, error) {
			ctx.State().Weather = req.Weather
			// PublishWait：等容器确认收到，用于需要"发布已生效"的场景。
			return actor.OK, shared.PublishWait(ctx, DemoWorldWeather.Of(ctx.Id().Name), req.Weather)
		})
	})
}

func setupRegion(mgr *actor.Manager) {
	actor.Serve(mgr, options100, func(b *actor.RegistryBuilder[RegionId, RegionState]) {
		// 每个读者 Group 只需注册一次 ServeReader，之后可 Watch 任意 key。
		shared.ServeReader(b, func(s *RegionState) *shared.Reader { return &s.Vars })

		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[RegionId, RegionState], req *EnterRegion, _ bool) (actor.OkReply, error) {
			ctx.Open()
			ctx.State().World = req.World

			// 首次 Watch：订阅 + 取回快照；此后命中本地缓存（由容器推送更新）。
			seed, err := shared.Watch(ctx, &ctx.State().Vars, DemoWorldSeed.Of(req.World.Name))
			if err != nil {
				return actor.OK, err
			}
			weather, err := shared.Watch(ctx, &ctx.State().Vars, DemoWorldWeather.Of(req.World.Name))
			if err != nil {
				return actor.OK, err
			}
			ctx.State().Seed, ctx.State().Weather = seed, weather
			ctx.Logger().Info("region entered", "seed", seed, "weather", weather)
			return actor.OK, nil
		})

		// 再次 Watch 不会触网（缓存已加载），可用于读当前缓存值。
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[RegionId, RegionState], req *ShowState, _ bool) (*StateReply, error) {
			seed, err := shared.Watch(ctx, &ctx.State().Vars, DemoWorldSeed.Of(ctx.State().World.Name))
			if err != nil {
				return nil, err
			}
			weather, err := shared.Watch(ctx, &ctx.State().Vars, DemoWorldWeather.Of(ctx.State().World.Name))
			if err != nil {
				return nil, err
			}
			ctx.State().Seed, ctx.State().Weather = seed, weather
			return &StateReply{Seed: seed, Weather: weather}, nil
		})
	})
}

// ─── main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mgr := actor.NewManager(slog.Default())

	// 容器本身也是 Group，分片数在进程内必须一致。
	shared.Serve(mgr, shared.Options{Shards: 4, Cooldown: 100 * time.Millisecond})

	setupWorld(mgr)
	setupRegion(mgr)

	ctx := context.Background()
	world := WorldId{Name: "w1"}
	region := RegionId{Name: "r1"}

	fmt.Println("\n=== 1. World 发布、Region 订阅 ===")
	_ = actor.Post(mgr, world, &InitWorld{Seed: 42})
	time.Sleep(50 * time.Millisecond)
	_ = actor.Post(mgr, region, &EnterRegion{World: world})
	time.Sleep(100 * time.Millisecond)

	reply, err := actor.Call(ctx, mgr, region, &ShowState{})
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
	} else {
		fmt.Printf("r1 缓存：seed=%d weather=%s\n", reply.Seed, reply.Weather)
	}

	fmt.Println("\n=== 2. World 改天气 —— 容器推送到读者缓存 ===")
	if _, err := actor.Call(ctx, mgr, world, &SetWeather{Weather: "rain"}); err != nil {
		fmt.Printf("改天气失败: %v\n", err)
	}
	time.Sleep(300 * time.Millisecond) // 等冷却窗口合并后的推送到达

	reply, err = actor.Call(ctx, mgr, region, &ShowState{})
	if err != nil {
		fmt.Printf("查询失败: %v\n", err)
	} else {
		fmt.Printf("r1 缓存（已由推送更新）：seed=%d weather=%s\n", reply.Seed, reply.Weather)
	}

	fmt.Println("\n=== 3. Fetch：不读缓存，每次向容器取最新值 ===")
	latest, err := shared.FetchFrom[string](ctx, mgr, DemoWorldWeather.Of(world.Name))
	if err != nil {
		fmt.Printf("Fetch 失败: %v\n", err)
	} else {
		fmt.Printf("容器中的 weather=%s\n", latest)
	}

	mgr.CloseManager()
	mgr.JoinManager()
	fmt.Println("\n=== Done ===")
}

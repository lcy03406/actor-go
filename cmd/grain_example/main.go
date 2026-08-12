// actor-go grain example: demonstrates persistent Actor with lease management.
// Each grain Actor is automatically activated (acquire lease + load persisted state)
// on first message, and can persist state or deactivate at any time.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
)

// ─── Type Aliases (for readability) ───

type (
	PlayerId   = grainPlayerId
	GrainState = grain.State[PlayerId, PlayerData, PlayerData, *grain.ShotSelf[PlayerData]]
	GrainCtx   = actor.ActorContext[PlayerId, GrainState]
	RegBuilder = actor.RegistryBuilder[PlayerId, GrainState]
)

// ─── Type Definitions ───

type grainPlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id grainPlayerId) ActorType() actor.ActorType { return "GrainPlayer" }
func (id grainPlayerId) String() string {
	return fmt.Sprintf("GrainPlayer(%d,%s)", id.ServerId, id.OpenId)
}

type PlayerData struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}

// ─── Requests ───

type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
	Damage int `json:"damage"`
}

type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type SaveAndQuit struct{}

func (*SaveAndQuit) ReqType(_ PlayerId, _ actor.OkReply) string { return "SaveAndQuit" }

type ForceSave struct{}

type SaveReply struct {
	HP int `json:"hp"`
}

func (*ForceSave) ReqType(_ PlayerId, _ *SaveReply) string { return "ForceSave" }

// ─── Handlers ───

func setupGrainPlayer(mgr *actor.Manager, pm *grain.PersistenceManager) {
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *RegBuilder) {
		// 使用 SetupGrain 将生命周期托管给框架：
		// OnSpawn 自动 Activate 并按 onActivate 回调初始化，
		// OnQuit 自动 Persist + 释放租约，并按 WithAutoPersistInterval 定时存盘。
		grain.SetupGrain(b, pm, func(ctx *GrainCtx, created bool) error {
			if created {
				// 首次创建时初始化数据（initialized HP/Level 由 Login 传入）。
				ctx.State().Data.HP = 100
				ctx.State().Data.Level = 1
				ctx.Logger().Info("grain created")
			} else {
				ctx.Logger().Info("grain loaded from disk")
			}
			return nil
		})

		// 注意：不再需要在 spawn/serve 中手动 Activate / Persist / Deactivate，
		// 框架已通过 OnSpawn / OnQuit 自动管理。下面 handler 只关心业务。
		actor.RegisterSpawn(b,
			func(ctx *GrainCtx, req *Login, spawning bool) (actor.OkReply, error) {
				// 若首次创建，用 Login 参数覆盖默认初始化值。
				if ctx.State().Data.HP == 0 {
					ctx.State().Data.HP = req.InitHP
					ctx.State().Data.Level = req.InitLevel
				}
				ctx.Logger().Info("grain login", "hp", ctx.State().Data.HP, "level", ctx.State().Data.Level)
				return actor.OK, nil
			})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().Data.HP -= req.Damage
			alive := ctx.State().Data.HP > 0
			ctx.Logger().Info("grain attacked", "damage", req.Damage, "remainingHP", ctx.State().Data.HP)
			return &AttackReply{RemainingHP: ctx.State().Data.HP, Alive: alive}, nil
		})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *SaveAndQuit, _ bool) (actor.OkReply, error) {
			ctx.Quit() // 触发 OnQuit 自动 Persist + 释放租约
			ctx.Logger().Info("grain save and quit")
			return actor.OK, nil
		})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *ForceSave, _ bool) (*SaveReply, error) {
			ctx.State().Persist(ctx) // 主动存盘 + 续租（可选，定时存盘已自动进行）
			return &SaveReply{HP: ctx.State().Data.HP}, nil
		})
	})
}

// ─── Main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Create PersistenceManager with JSON file driver
	// Lease is now built into the driver, no separate lease.Manager needed.
	// WithAutoPersistInterval 让框架每隔 5s 自动存盘续租。
	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver("./grain_data")),
		grain.WithNodeId("node-1"),
		grain.WithAutoPersistInterval(5*time.Second),
	)

	mgr := actor.NewManager()
	setupGrainPlayer(mgr, pm)

	ctx := context.Background()
	player := PlayerId{ServerId: 1, OpenId: "alice"}

	// 1. Spawn: first message triggers activation (acquire lease + load state)
	fmt.Println("=== 1. Spawn (activate) ===")
	actor.Post(mgr, player, &Login{InitHP: 100, InitLevel: 1})
	time.Sleep(200 * time.Millisecond)

	// 2. Call: attack
	fmt.Println("\n=== 2. Call: Attack ===")
	reply, err := actor.Call(ctx, mgr, player, &Attack{Damage: 30})
	if err != nil {
		fmt.Printf("Attack error: %v\n", err)
	} else {
		fmt.Printf("After attack: remainingHP=%d, alive=%v\n", reply.RemainingHP, reply.Alive)
	}

	// 3. Call: force save (also renews lease)
	fmt.Println("\n=== 3. Call: Force Save ===")
	saveReply, err := actor.Call(ctx, mgr, player, &ForceSave{})
	if err != nil {
		fmt.Printf("ForceSave error: %v\n", err)
	} else {
		fmt.Printf("Saved state: HP=%d\n", saveReply.HP)
	}

	// 4. Deactivate: save + release lease + quit
	fmt.Println("\n=== 4. Deactivate ===")
	actor.Post(mgr, player, &SaveAndQuit{})
	time.Sleep(200 * time.Millisecond)

	// 5. Re-activate: state should be restored from disk
	fmt.Println("\n=== 5. Re-activate (load from disk) ===")
	actor.Post(mgr, player, &Login{InitHP: 999, InitLevel: 99})
	time.Sleep(200 * time.Millisecond)

	reply2, err := actor.Call(ctx, mgr, player, &Attack{Damage: 0})
	if err != nil {
		fmt.Printf("Attack error: %v\n", err)
	} else {
		fmt.Printf("After re-activation: HP=%d\n", reply2.RemainingHP)
	}

	// Cleanup
	actor.Post(mgr, player, &SaveAndQuit{})
	time.Sleep(200 * time.Millisecond)

	fmt.Println("\n=== Done ===")
	// Remove generated data directory
	os.RemoveAll("./grain_data")
}

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
	actor.Serve(mgr, 100, func(b *RegBuilder) {
		// WrapSpawn: on first message, acquires lease + loads persisted state,
		// then calls the handler. If state doesn't exist, starts with zero-value PlayerData.
		actor.RegisterSpawn(b, grain.WrapSpawn(pm,
			func(ctx *GrainCtx, req *Login, _ bool) (actor.OkReply, error) {
				ctx.State().Data.HP = req.InitHP
				ctx.State().Data.Level = req.InitLevel
				ctx.State().Persist(ctx) // save + renew lease
				ctx.Logger().Info("grain login", "hp", req.InitHP, "level", req.InitLevel)
				return actor.OK, nil
			}))

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().Data.HP -= req.Damage
			alive := ctx.State().Data.HP > 0
			ctx.Logger().Info("grain attacked", "damage", req.Damage, "remainingHP", ctx.State().Data.HP)
			return &AttackReply{RemainingHP: ctx.State().Data.HP, Alive: alive}, nil
		})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *SaveAndQuit, _ bool) (actor.OkReply, error) {
			ctx.State().Deactivate(ctx) // save + release lease + quit
			ctx.Logger().Info("grain deactivated")
			return actor.OK, nil
		})

		actor.RegisterQuery(b, func(ctx *GrainCtx, req *ForceSave, _ bool) (*SaveReply, error) {
			ctx.State().Persist(ctx) // save + renew lease, without quitting
			return &SaveReply{HP: ctx.State().Data.HP}, nil
		})
	})
}

// ─── Main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	// Create PersistenceManager with JSON file driver
	// Lease is now built into the driver, no separate lease.Manager needed
	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver("./grain_data")),
		grain.WithNodeId("node-1"),
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

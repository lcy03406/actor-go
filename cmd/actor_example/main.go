// actor-go basic example: demonstrates Actor spawn, Call, Post, Broadcast, Timer, and lifecycle.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ─── Type Definitions ───

type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
	return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

type PlayerState struct {
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

type Heal struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ PlayerId, _ *HealReply) string { return "Heal" }

type Logout struct{}

func (*Logout) ReqType(_ PlayerId, _ actor.OkReply) string { return "Logout" }

type Close struct{}

func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }

// ─── Handlers ───

var options100 = actor.Options{BufMails: 100}

func setupPlayer(mgr *actor.Manager) {
	actor.Serve(mgr, options100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
			ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
			ctx.Logger().Info("player login", "hp", req.InitHP, "level", req.InitLevel)
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().HP -= req.Damage
			alive := ctx.State().HP > 0
			ctx.Logger().Info("player attacked", "damage", req.Damage, "remainingHP", ctx.State().HP)
			return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, _ bool) (*HealReply, error) {
			ctx.State().HP += req.Amount
			return &HealReply{NewHP: ctx.State().HP}, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Logout, _ bool) (actor.OkReply, error) {
			ctx.Logger().Info("player logout")
			ctx.Quit()
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Close, _ bool) (actor.OkReply, error) {
			ctx.Quit()
			return actor.OK, nil
		})
	})
}

// ─── Main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	mgr := actor.NewManager(slog.Default())
	setupPlayer(mgr)

	ctx := context.Background()
	player1 := PlayerId{ServerId: 1, OpenId: "alice"}
	player2 := PlayerId{ServerId: 1, OpenId: "bob"}

	// 1. Post: spawn players (fire-and-forget)
	fmt.Println("=== 1. Spawn Players ===")
	actor.Post(mgr, player1, &Login{InitHP: 100, InitLevel: 1})
	actor.Post(mgr, player2, &Login{InitHP: 80, InitLevel: 2})
	time.Sleep(100 * time.Millisecond)

	// 2. Call: attack player1, get reply
	fmt.Println("\n=== 2. Call: Attack ===")
	reply, err := actor.Call(ctx, mgr, player1, &Attack{Damage: 30})
	if err != nil {
		fmt.Printf("Attack error: %v\n", err)
	} else {
		fmt.Printf("Player1 attacked: remainingHP=%d, alive=%v\n", reply.RemainingHP, reply.Alive)
	}

	// 3. Call: heal player1
	fmt.Println("\n=== 3. Call: Heal ===")
	healReply, err := actor.Call(ctx, mgr, player1, &Heal{Amount: 20})
	if err != nil {
		fmt.Printf("Heal error: %v\n", err)
	} else {
		fmt.Printf("Player1 healed: newHP=%d\n", healReply.NewHP)
	}

	// 4. Post: logout player2
	fmt.Println("\n=== 4. Post: Logout ===")
	actor.Post(mgr, player2, &Logout{})
	time.Sleep(100 * time.Millisecond)

	// 5. Count and Broadcast
	fmt.Println("\n=== 5. Count & Broadcast ===")
	count, _ := actor.Count[PlayerId](mgr)
	fmt.Printf("Actors before broadcast: %d\n", count)

	actor.Broadcast(mgr, &Close{})
	time.Sleep(200 * time.Millisecond)

	count, _ = actor.Count[PlayerId](mgr)
	fmt.Printf("Actors after broadcast: %d\n", count)

	fmt.Println("\n=== Done ===")
}

// actor-go RPC example: demonstrates remote Actor communication over WebSocket.
// Run this single binary to start both server and client in one process.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/rpc"
)

// ─── Type Aliases ───

type (
	JsonServer = rpc.Server[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
	JsonClient = rpc.Client[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
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

type Close struct{}

func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }

// ─── Server ───

func runServer(addr string) (*JsonServer, *actor.Manager) {
	mgr := actor.NewManager()

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
			ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
			ctx.Logger().Info("player login", "hp", req.InitHP, "level", req.InitLevel)
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().HP -= req.Damage
			alive := ctx.State().HP > 0
			return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, _ bool) (*HealReply, error) {
			ctx.State().HP += req.Amount
			return &HealReply{NewHP: ctx.State().HP}, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Close, _ bool) (actor.OkReply, error) {
			ctx.Quit()
			return actor.OK, nil
		})
	})

	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		addr,
		mgr,
		func(b *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]) {
			rpc.RegisterRequest(b, &Login{})
			rpc.RegisterRequest(b, &Attack{})
			rpc.RegisterRequest(b, &Heal{})
			rpc.RegisterRequest(b, &Close{})
		},
	)
	if err := server.Start(); err != nil {
		log.Fatalf("server start failed: %v", err)
	}
	fmt.Printf("[Server] started on %s\n", addr)
	return server, mgr
}

// ─── Client ───

func runClient(addr string) {
	client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](addr)
	if err := client.Connect(); err != nil {
		log.Fatalf("client connect failed: %v", err)
	}
	defer client.Close()
	fmt.Printf("[Client] connected to %s\n", addr)

	ctx := context.Background()
	player1 := PlayerId{ServerId: 1, OpenId: "alice"}
	player2 := PlayerId{ServerId: 1, OpenId: "bob"}

	// 1. Spawn players via Post
	fmt.Println("\n=== 1. Post: Spawn Players ===")
	rpc.Post(client, player1, &Login{InitHP: 100, InitLevel: 1})
	fmt.Println("  Player1 (alice) logged in")
	rpc.Post(client, player2, &Login{InitHP: 80, InitLevel: 2})
	fmt.Println("  Player2 (bob) logged in")
	time.Sleep(200 * time.Millisecond)

	// 2. Call: attack player1
	fmt.Println("\n=== 2. Call: Attack ===")
	reply, err := rpc.Call(ctx, client, player1, &Attack{Damage: 30})
	if err != nil {
		log.Fatalf("Call Attack failed: %v", err)
	}
	fmt.Printf("  Player1 attacked: damage=30, remainingHP=%d, alive=%v\n", reply.RemainingHP, reply.Alive)

	// 3. Call: heal player1
	fmt.Println("\n=== 3. Call: Heal ===")
	healReply, err := rpc.Call(ctx, client, player1, &Heal{Amount: 20})
	if err != nil {
		log.Fatalf("Call Heal failed: %v", err)
	}
	fmt.Printf("  Player1 healed: amount=20, newHP=%d\n", healReply.NewHP)

	// 4. CallTimeout: attack with timeout
	fmt.Println("\n=== 4. CallTimeout: Attack ===")
	reply2, err := rpc.CallTimeout(ctx, client, player1, &Attack{Damage: 50}, 5*time.Second)
	if err != nil {
		log.Fatalf("CallTimeout Attack failed: %v", err)
	}
	fmt.Printf("  Player1 attacked: damage=50, remainingHP=%d, alive=%v\n", reply2.RemainingHP, reply2.Alive)

	// 5. Broadcast: close all players
	fmt.Println("\n=== 5. Broadcast: Close All ===")
	rpc.Broadcast(client, &Close{})
	fmt.Println("  Broadcast sent, waiting for actors to close...")
	time.Sleep(300 * time.Millisecond)
}

// ─── Main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	addr := "localhost:8080"

	// Start server
	server, _ := runServer(addr)
	time.Sleep(300 * time.Millisecond)

	// Run client
	runClient(addr)

	// Shutdown
	fmt.Println("\n--- Shutting down ---")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(ctx)
	fmt.Println("Done.")
}
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"os/signal"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/rpc"
)

// ============================================================
// 便捷类型别名
// ============================================================

type jsonServer = rpc.Server[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
type jsonClient = rpc.Client[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
type jsonRegBuilder = rpc.RegustryBuilder[json.RawMessage, rpc.JsonCodec]

// ============================================================
// 类型定义
// ============================================================

type GameId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id GameId) ActorType() actor.ActorType { return "Game" }
func (id GameId) String() string             { return fmt.Sprintf("Game(%d,%s)", id.ServerId, id.OpenId) }

type GameState struct {
	HP    int
	Level int
}

type LoginReq struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*LoginReq) ReqType(_ GameId, _ actor.OkReply) string { return "Login" }

type AttackReq struct {
	Damage int `json:"damage"`
}

type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

func (*AttackReq) ReqType(_ GameId, _ *AttackReply) string { return "Attack" }

type HealReq struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*HealReq) ReqType(_ GameId, _ *HealReply) string { return "Heal" }

type CloseReq struct{}

func (*CloseReq) ReqType(_ GameId, _ actor.OkReply) string { return "Close" }

// ============================================================
// 服务端
// ============================================================

func runServer(addr string) (*jsonServer, *actor.Manager) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[GameId, GameState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[GameId, GameState], req *LoginReq, spawning bool) (actor.OkReply, error) {
			a.SetState(GameState{HP: req.InitHP, Level: req.InitLevel})
			a.Logger().Info("player logged in", "hp", req.InitHP, "level", req.InitLevel)
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GameId, GameState], req *AttackReq, _ bool) (*AttackReply, error) {
			state := a.State()
			state.HP -= req.Damage
			alive := state.HP > 0
			a.Logger().Info("player attacked", "damage", req.Damage, "remainingHP", state.HP, "alive", alive)
			return &AttackReply{RemainingHP: state.HP, Alive: alive}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GameId, GameState], req *HealReq, _ bool) (*HealReply, error) {
			state := a.State()
			state.HP += req.Amount
			a.Logger().Info("player healed", "amount", req.Amount, "newHP", state.HP)
			return &HealReply{NewHP: state.HP}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GameId, GameState], req *CloseReq, _ bool) (actor.OkReply, error) {
			a.Logger().Info("player closing")
			a.Quit()
			return actor.OK, nil
		})
	})

	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		addr,
		mgr,
		func(b *jsonRegBuilder) {
			rpc.RegisterRequest(b, &LoginReq{})
			rpc.RegisterRequest(b, &AttackReq{})
			rpc.RegisterRequest(b, &HealReq{})
			rpc.RegisterRequest(b, &CloseReq{})
		},
	)
	if err := server.Start(); err != nil {
		log.Fatalf("server start failed: %v", err)
	}
	fmt.Printf("[Server] started on %s\n", addr)
	return server, mgr
}

// ============================================================
// 客户端
// ============================================================

func runClient(addr string, mgr *actor.Manager) {
	client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](addr)
	if err := client.Connect(); err != nil {
		log.Fatalf("client connect failed: %v", err)
	}
	defer client.Close()
	fmt.Printf("[Client] connected to %s\n", addr)

	ctx := context.Background()

	player1 := GameId{ServerId: 1, OpenId: "player_1"}
	player2 := GameId{ServerId: 1, OpenId: "player_2"}

	// 1. Post: 创建玩家（登录）
	fmt.Println("\n--- 1. Post: 玩家登录 ---")
	if err := rpc.Post(client, player1, &LoginReq{InitHP: 100, InitLevel: 1}); err != nil {
		log.Fatalf("Post player1 failed: %v", err)
	}
	fmt.Println("  Player1 logged in (HP=100, Level=1)")
	if err := rpc.Post(client, player2, &LoginReq{InitHP: 80, InitLevel: 2}); err != nil {
		log.Fatalf("Post player2 failed: %v", err)
	}
	fmt.Println("  Player2 logged in (HP=80, Level=2)")
	time.Sleep(200 * time.Millisecond)

	// 2. Call: 攻击玩家
	fmt.Println("\n--- 2. Call: 攻击玩家 ---")
	reply, err := rpc.Call(ctx, client, player1, &AttackReq{Damage: 30})
	if err != nil {
		log.Fatalf("Call Attack failed: %v", err)
	}
	fmt.Printf("  Player1 attacked: damage=30, remainingHP=%d, alive=%v\n", reply.RemainingHP, reply.Alive)

	// 3. Call: 治疗玩家
	fmt.Println("\n--- 3. Call: 治疗玩家 ---")
	healReply, err := rpc.Call(ctx, client, player1, &HealReq{Amount: 20})
	if err != nil {
		log.Fatalf("Call Heal failed: %v", err)
	}
	fmt.Printf("  Player1 healed: amount=20, newHP=%d\n", healReply.NewHP)

	// 4. CallTimeout: 带超时的调用
	fmt.Println("\n--- 4. CallTimeout: 带超时攻击 ---")
	reply2, err := rpc.CallTimeout(ctx, client, player1, &AttackReq{Damage: 50}, 5*time.Second)
	if err != nil {
		log.Fatalf("CallTimeout Attack failed: %v", err)
	}
	fmt.Printf("  Player1 attacked: damage=50, remainingHP=%d, alive=%v\n", reply2.RemainingHP, reply2.Alive)

	// 5. Broadcast: 广播关闭所有玩家
	fmt.Println("\n--- 5. Broadcast: 关闭所有玩家 ---")
	if err := rpc.Broadcast(client, &CloseReq{}); err != nil {
		log.Fatalf("Broadcast Close failed: %v", err)
	}
	fmt.Println("  Broadcast sent, waiting for actors to close...")
	time.Sleep(300 * time.Millisecond)

	count, _ := actor.Count[GameId](mgr)
	fmt.Printf("  Actors remaining: %d\n", count)
}

// ============================================================
// main
// ============================================================

func main() {
	addr := "localhost:8080"

	// 启动服务端
	server, mgr := runServer(addr)

	// 等待服务端就绪
	time.Sleep(300 * time.Millisecond)

	// 运行客户端
	runClient(addr, mgr)

	// 优雅关闭
	fmt.Println("\n--- Shutting down ---")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt)

	select {
	case <-sigCh:
		fmt.Println("Received interrupt signal")
	case <-time.After(1 * time.Second):
	}

	server.Shutdown(ctx)
	fmt.Println("Done.")
}

package rpc_test

// 本文件是 README「RPC」一节的示例来源：region 片段由 cmd/readmegen 注入。

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/rpc"
)

// ─── 示例业务类型 ───

type DemoPlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id DemoPlayerId) ActorType() actor.ActorType { return "DemoPlayer" }
func (id DemoPlayerId) String() string             { return "DemoPlayer(" + id.OpenId + ")" }

type DemoState struct {
	HP int `json:"hp"`
}

type DemoLogin struct {
	InitHP int `json:"initHP"`
}

func (*DemoLogin) ReqType(_ DemoPlayerId, _ actor.OkReply) string { return "Login" }

type DemoAttack struct {
	Damage int `json:"damage"`
}

type DemoAttackReply struct {
	RemainingHP int `json:"remainingHP"`
}

func (*DemoAttack) ReqType(_ DemoPlayerId, _ *DemoAttackReply) string { return "Attack" }

type DemoClose struct{}

func (*DemoClose) ReqType(_ DemoPlayerId, _ actor.OkReply) string { return "Close" }

func Example_server() {
	mgr := actor.NewManager(slog.Default())
	serveDemoHandlers(mgr)

	// region: server
	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		":8080", mgr,
		func(b *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]) {
			rpc.RegisterRequest(b, &DemoLogin{})
			rpc.RegisterRequest(b, &DemoAttack{})
			rpc.RegisterRequest(b, &DemoClose{})
		},
	)
	if err := server.Start(); err != nil { // 非阻塞；需要阻塞时改用 server.Run()
		slog.Error("server start failed", "err", err)
	}

	// 优雅关闭：等待在途请求处理完
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		slog.Error("server shutdown failed", "err", err)
	}
	// endregion: server
}

func Example_client() {
	client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]("localhost:8080")
	if err := client.Connect(); err != nil {
		slog.Error("client connect failed", "err", err)
	}
	defer client.Close()

	playerId := DemoPlayerId{ServerId: 1, OpenId: "alice"}
	ctx := context.Background()

	// region: client
	// 远程 Post（发后不理）
	if err := rpc.Post(client, playerId, &DemoLogin{InitHP: 100}); err != nil {
		slog.Error("post login failed", "err", err)
	}

	// 远程 Call
	reply, err := rpc.Call(ctx, client, playerId, &DemoAttack{Damage: 30})
	if err != nil {
		slog.Error("call attack failed", "err", err)
	}
	slog.Info("attacked", "remainingHP", reply.RemainingHP)

	// 带超时的远程 Call
	reply, err = rpc.CallTimeout(ctx, client, playerId, &DemoAttack{Damage: 10}, 5*time.Second)
	if err != nil {
		slog.Error("call attack with timeout failed", "err", err)
	}
	slog.Info("attacked", "remainingHP", reply.RemainingHP)

	// 远程 Broadcast
	if err := rpc.Broadcast(client, &DemoClose{}); err != nil {
		slog.Error("broadcast failed", "err", err)
	}
	// endregion: client
}

// serveDemoHandlers 注册示例用的 Actor Group（与服务端注册的请求类型一一对应）。
func serveDemoHandlers(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[DemoPlayerId, DemoState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[DemoPlayerId, DemoState], req *DemoLogin, _ bool) (actor.OkReply, error) {
			ctx.Open()
			ctx.State().HP = req.InitHP
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[DemoPlayerId, DemoState], req *DemoAttack, _ bool) (*DemoAttackReply, error) {
			ctx.State().HP -= req.Damage
			return &DemoAttackReply{RemainingHP: ctx.State().HP}, nil
		})
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[DemoPlayerId, DemoState], req *DemoClose, _ bool) (actor.OkReply, error) {
			ctx.Quit()
			return actor.OK, nil
		})
	})
}

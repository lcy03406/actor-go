// handler.go 是 Chat Actor 的 handler 注册入口。
//
// Register(mgr, rpcBld, placement, selfID)  一次性注册 handler + RPC + CheckOwnership。
package chat

import (
	"encoding/json"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/rpc"
)

// Register 一次性注册 handler + RPC 入口 + 集群 CheckOwnership。
func Register(mgr *actor.Manager, rpcBld *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec], placement cluster.PlacementStrategy, selfID string) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[ChatId, ChatState]) {
		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, (*SendMessage)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, &CheckOwnership{placement: placement, selfID: selfID}))
	})
}

// handler.go 是 Room Actor 的 handler 注册入口。
//
// Register(mgr, rpcBld, placement, selfID)  一次性注册 handler + RPC + CheckOwnership。
package room

import (
	"encoding/json"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/rpc"
)

// Register 一次性注册 handler + RPC 入口 + 集群 CheckOwnership。
func Register(mgr *actor.Manager, rpcBld *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec], placement cluster.PlacementStrategy, selfID string) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RoomId, RoomState]) {
		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, (*CreateRoom)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*JoinRoom)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*RoomInfo)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, &CheckOwnership{placement: placement, selfID: selfID}))
	})
}

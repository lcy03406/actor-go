// handler.go 是 Player Actor 的 handler 注册入口。
//
// Register(mgr, rpcBld, pm, placement, selfID)  一次性注册 handler + RPC + CheckOwnership。
package player

import (
	"encoding/json"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/rpc"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/attr"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/inventory"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/skill"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// Register 一次性注册 handler + RPC 入口 + 集群 CheckOwnership。
func Register(mgr *actor.Manager, rpcBld *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec], pm *grain.PersistenceManager, placement cluster.PlacementStrategy, selfID string) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[types.PlayerId, types.GrainState]) {
		actor.RegisterSpawn(b, grain.WrapSpawnHandler2(pm, (*Login)(nil)))
		rpc.RegisterRequest(rpcBld, &Login{})

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*Attack)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*Heal)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*AddGold)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*Close)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*PlayerStatusReq)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.AddExp)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.QueryAttr)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.UpgradeAttr)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.AddItem)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.RemoveItem)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.ListItems)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.UseItem)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.LearnSkill)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.CastSkill)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.ListSkills)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, &CheckOwnership{placement: placement, selfID: selfID}))
	})
}

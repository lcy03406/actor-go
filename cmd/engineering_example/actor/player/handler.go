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

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/combat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/notify"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/attr"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/inventory"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/skill"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

// Register 一次性注册 handler + RPC 入口 + 集群 CheckOwnership。
func Register(mgr *actor.Manager, rpcBld *rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec], pm *grain.PersistenceManager, placement cluster.PlacementStrategy, selfID string) {
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[types.PlayerId, types.GrainState]) {
		// 玩家登录：Grain 模式。不再使用 WrapSpawn 自动激活，
		// 改为在回调中显式 Activate，并根据返回值（创建/加载）决定初始化逻辑。
		actor.RegisterSpawn(b, func(ctx *types.PlayerActorCtx, req *Login, spawning bool) (actor.OkReply, error) {
			res, err := ctx.State().Activate(ctx, pm)
			if err != nil {
				return actor.OK, err
			}
			return req.Handle(ctx, spawning, res)
		})
		rpc.RegisterRequest(rpcBld, &Login{})

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*ControlAttack)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*ControlHeal)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*AddGold)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*Close)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*PlayerStatusReq)(nil)))

		// ★ 跨 Actor 通信：Player → Room
		// Player 加入房间后，可在房间内与其他玩家聊天 / 战斗。
		// Handler 中通过 ctx.Manager() 获取 Manager，
		// 使用 actor.Post / actor.Call 向其他 Group 的 Actor 发送请求。
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*ControlJoinRoom)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*PlayerRoomChat)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*ControlLeaveRoom)(nil)))

		// 以下为「落到本 Player 的事件」(Player*)，由其他 actor post 过来：
		//   - combat.PlayerDamage    ：被攻击方受击（跨 actor 受击协议）
		//   - combat.PlayerCombatResult：攻击方收到被攻击方回传的战斗结果 → 结算奖励
		// 二者均注册 handler 以支持分发，但不通过 rpc.RegisterRequest 暴露为对外 RPC。
		// 类型定义在 combat 包（跨 actor 协议），结算逻辑（PlayerCombatResult）在本包。
		actor.RegisterQueryHandler2(b, (*combat.PlayerDamage)(nil))
		actor.RegisterQueryHandler2(b, (*combat.PlayerCombatResult)(nil))
		actor.RegisterServeHandler2(b, (*notify.ReceiveChat)(nil))
		actor.RegisterServeHandler2(b, (*notify.ReceiveBattle)(nil))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.AddExp)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.QueryAttr)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*attr.UpgradeAttr)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.AddItem)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.RemoveItem)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.ListItems)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*inventory.UseItem)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.ControlLearn)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.ControlCast)(nil)))
		rpc.RegisterRequest(rpcBld, actor.RegisterQueryHandler2(b, (*skill.ListSkills)(nil)))

		rpc.RegisterRequest(rpcBld, actor.RegisterServeHandler2(b, &CheckOwnership{placement: placement, selfID: selfID}))
	})
}

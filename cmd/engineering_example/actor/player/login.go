package player

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/grain"
)

// Login 是玩家登录（创建 Actor）的请求。
// 作为 spawn handler，由 handler 注册处的闭包在 spawning 时调用
// ctx.State().Activate(ctx, pm) 激活，并据返回结果初始化数据。
type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ types.PlayerId, _ actor.OkReply) string { return "Login" }

// Handle 在已激活（Activate）后执行。
// res 表示本次是首次创建（grain.ActivateCreated）还是从存储加载（grain.ActivateLoaded），
// 调用方据此决定是否需要初始化业务数据。
func (req *Login) Handle(ctx *types.PlayerActorCtx, spawning bool, res grain.ActivateResult) (actor.OkReply, error) {
	if res == grain.ActivateCreated {
		// 首次创建：初始化状态（如果从持久化恢复，Activate 已完成加载）
		ctx.State().Data = types.PlayerState{
			HP:    req.InitHP,
			MaxHP: req.InitHP, // 初始生命上限与初始 HP 相同
			Level: req.InitLevel,
			Attr: types.AttrState{
				Exp: 0, Gold: 100, Atk: 10 + req.InitLevel*2, Def: 5 + req.InitLevel, Speed: 5,
			},
			Inventory: types.InventoryState{Items: []types.Item{}, Capacity: 50},
			Skill:     types.SkillState{Learned: []types.Skill{}, MaxSlots: 6},
		}
		ctx.State().Persist(ctx) // 首次持久化
	}
	log.Printf("[Player] %s 登录 HP=%d Level=%d Gold=%d (spawning=%v, created=%v)",
		ctx.Id(), ctx.State().Data.HP, ctx.State().Data.Level, ctx.State().Data.Attr.Gold, spawning, res == grain.ActivateCreated)
	return actor.OK, nil
}

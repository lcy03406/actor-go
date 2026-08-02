package player

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
)

type Attack struct {
	Damage int `json:"damage"`
}

type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

func (*Attack) ReqType(_ types.PlayerId, _ *AttackReply) string { return "Attack" }

func (req *Attack) Handle(ctx *types.PlayerActorCtx, spawning bool) (*AttackReply, error) {
	data := &ctx.State().Data

	// 使用 logic 包计算实际伤害（攻击方攻击力 vs 防御方防御力）
	result := logic.CalcDamage(data.Attr.Atk, data.Attr.Def, data.Level, float64(req.Damage), 0.5, 5, 0.1)
	dmg := result.Mitigated
	if result.Critical {
		log.Printf("[Player] %s 暴击!", ctx.Id())
	}

	data.HP -= dmg
	alive := data.HP > 0

	log.Printf("[Player] %s 受到 %d 伤害(raw=%d, mit=%d), HP=%d",
		ctx.Id(), dmg, result.Raw, result.Mitigated, data.HP)

	if !alive {
		ctx.State().Deactivate(ctx) // 死亡 → 停用 + 释放租约
		log.Printf("[Player] %s 已死亡，释放租约", ctx.Id())
	}
	return &AttackReply{RemainingHP: data.HP, Alive: alive}, nil
}

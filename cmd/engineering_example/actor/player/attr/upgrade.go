package attr

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type UpgradeAttr struct {
	Stat string `json:"stat"`
}

type UpgradeAttrReply struct {
	Stat  string `json:"stat"`
	Value int    `json:"value"`
	Cost  int    `json:"cost"`
}

func (*UpgradeAttr) ReqType(_ types.PlayerId, _ *UpgradeAttrReply) string { return "UpgradeAttr" }

func (req *UpgradeAttr) Handle(ctx *types.PlayerActorCtx, spawning bool) (*UpgradeAttrReply, error) {
	data := &ctx.State().Data
	cost := 50
	if data.Gold < cost {
		log.Printf("[Player.Attr] %s 金币不足, 需要 %d, 当前 %d", ctx.Id(), cost, data.Gold)
		return nil, nil
	}

	attr := &data.Attr
	data.Gold -= cost

	var newVal int
	switch req.Stat {
	case "atk":
		attr.Atk += 2
		newVal = attr.Atk
	case "def":
		attr.Def += 2
		newVal = attr.Def
	case "speed":
		attr.Speed += 2
		newVal = attr.Speed
	default:
		log.Printf("[Player.Attr] %s 未知属性: %s", ctx.Id(), req.Stat)
		return nil, nil
	}

	ctx.State().Persist(ctx) // 属性升级后持久化
	log.Printf("[Player.Attr] %s 升级 %s=%d 消耗 %d 金币", ctx.Id(), req.Stat, newVal, cost)
	return &UpgradeAttrReply{Stat: req.Stat, Value: newVal, Cost: cost}, nil
}

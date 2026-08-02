// upgrade.go 定义 UpgradeAttr 请求的薄入口（handler）。
//
// 业务规则已下沉到 AttrState.Upgrade（见 types/methods.go）。本 handler 仅做：
// 取参数 → 调 Upgrade → 持久化 → 返回。
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
	newVal, cost, ok := data.Attr.Upgrade(req.Stat)
	if !ok {
		log.Printf("[Player.Attr] %s 升级 %s 失败 (需要 %d 金币, 当前 %d)", ctx.Id(), req.Stat, cost, data.Attr.Gold)
		return nil, nil
	}
	ctx.State().Persist(ctx)
	log.Printf("[Player.Attr] %s 升级 %s=%d 消耗 %d 金币", ctx.Id(), req.Stat, newVal, cost)
	return &UpgradeAttrReply{Stat: req.Stat, Value: newVal, Cost: cost}, nil
}

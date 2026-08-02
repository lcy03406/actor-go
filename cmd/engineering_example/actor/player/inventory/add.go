// inventory 子包定义道具/背包模块的所有请求。
//
// 【依赖】types/ + actor + logic/，不依赖 player 包。
package inventory

import (
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type AddItem struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
	Type  string `json:"type"`
}

type AddItemReply struct {
	Added      bool `json:"added"`
	TotalCount int  `json:"totalCount"`
}

func (*AddItem) ReqType(_ types.PlayerId, _ *AddItemReply) string { return "AddItem" }

func (req *AddItem) Handle(ctx *types.PlayerActorCtx, spawning bool) (*AddItemReply, error) {
	inv := &ctx.State().Data.Inventory

	currentCount := 0
	for _, item := range inv.Items {
		currentCount += item.Count
	}
	if currentCount+req.Count > inv.Capacity {
		log.Printf("[Player.Inventory] %s 背包已满 (容量=%d)", ctx.Id(), inv.Capacity)
		return &AddItemReply{Added: false}, nil
	}

	for i := range inv.Items {
		if inv.Items[i].ID == req.ID {
			inv.Items[i].Count += req.Count
			log.Printf("[Player.Inventory] %s 获得 %dx%s (已有, 总计=%d)",
				ctx.Id(), req.Count, req.Name, inv.Items[i].Count)
			return &AddItemReply{Added: true, TotalCount: inv.Items[i].Count}, nil
		}
	}

	inv.Items = append(inv.Items, types.Item{
		ID: req.ID, Name: req.Name, Count: req.Count, Type: req.Type,
	})
	log.Printf("[Player.Inventory] %s 获得 %dx%s (新)", ctx.Id(), req.Count, req.Name)
	return &AddItemReply{Added: true, TotalCount: req.Count}, nil
}

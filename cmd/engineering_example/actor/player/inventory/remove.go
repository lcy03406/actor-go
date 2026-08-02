package inventory

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type RemoveItem struct {
	ID    int `json:"id"`
	Count int `json:"count"`
}

type RemoveItemReply struct {
	Removed   bool   `json:"removed"`
	ItemName  string `json:"itemName"`
	Remaining int    `json:"remaining"`
}

func (*RemoveItem) ReqType(_ types.PlayerId, _ *RemoveItemReply) string { return "RemoveItem" }

func (req *RemoveItem) Handle(ctx *types.PlayerActorCtx, spawning bool) (*RemoveItemReply, error) {
	inv := &ctx.State().Data.Inventory

	for i := range inv.Items {
		if inv.Items[i].ID == req.ID {
			if inv.Items[i].Count < req.Count {
				return nil, fmt.Errorf("道具 %s 数量不足: 需要 %d, 现有 %d",
					inv.Items[i].Name, req.Count, inv.Items[i].Count)
			}
			inv.Items[i].Count -= req.Count
			name := inv.Items[i].Name
			remaining := inv.Items[i].Count

			if inv.Items[i].Count == 0 {
				inv.Items = append(inv.Items[:i], inv.Items[i+1:]...)
				remaining = 0
			}

			log.Printf("[Player.Inventory] %s 消耗 %dx%s (剩余=%d)", ctx.Id(), req.Count, name, remaining)
			return &RemoveItemReply{Removed: true, ItemName: name, Remaining: remaining}, nil
		}
	}

	return nil, fmt.Errorf("未找到道具 ID=%d", req.ID)
}

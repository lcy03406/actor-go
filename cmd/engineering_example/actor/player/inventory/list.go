package inventory

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type ListItems struct{}

type ListItemsReply struct {
	Items    []types.Item `json:"items"`
	Capacity int          `json:"capacity"`
	Used     int          `json:"used"`
}

func (*ListItems) ReqType(_ types.PlayerId, _ *ListItemsReply) string { return "ListItems" }

func (req *ListItems) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ListItemsReply, error) {
	inv := ctx.State().Data.Inventory
	used := 0
	for _, item := range inv.Items {
		used += item.Count
	}
	return &ListItemsReply{Items: inv.Items, Capacity: inv.Capacity, Used: used}, nil
}

package inventory

import (
	"fmt"
	"log"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type UseItem struct {
	ID int `json:"id"`
}

type UseItemReply struct {
	Used     bool   `json:"used"`
	ItemName string `json:"itemName"`
	Effect   string `json:"effect"`
}

func (*UseItem) ReqType(_ types.PlayerId, _ *UseItemReply) string { return "UseItem" }

func (req *UseItem) Handle(ctx *types.PlayerActorCtx, spawning bool) (*UseItemReply, error) {
	data := &ctx.State().Data
	inv := &data.Inventory

	for i := range inv.Items {
		if inv.Items[i].ID == req.ID {
			item := &inv.Items[i]
			if item.Count < 1 {
				return nil, fmt.Errorf("道具 %s 数量不足", item.Name)
			}

			effect := ""
			switch item.Type {
			case "potion":
				heal := 30
				// 受 MaxHP 上限约束（与 Heal 一致），避免溢出上限
				oldHP := data.HP
				data.HP += heal
				if data.HP > data.MaxHP {
					data.HP = data.MaxHP
				}
				realHeal := data.HP - oldHP
				effect = fmt.Sprintf("回复 %d HP (当前 HP=%d)", realHeal, data.HP)
			case "material":
				effect = "使用材料，无直接效果"
			case "weapon":
				data.Attr.Atk += 3
				effect = fmt.Sprintf("装备武器，攻击力 +3 (当前 Atk=%d)", data.Attr.Atk)
			default:
				effect = "无效果"
			}

			item.Count--
			name := item.Name
			if item.Count == 0 {
				inv.Items = append(inv.Items[:i], inv.Items[i+1:]...)
			}

			ctx.State().Persist(ctx) // 使用道具后持久化
			log.Printf("[Player.Inventory] %s 使用 %s → %s", ctx.Id(), name, effect)
			return &UseItemReply{Used: true, ItemName: name, Effect: effect}, nil
		}
	}

	return nil, fmt.Errorf("未找到道具 ID=%d", req.ID)
}

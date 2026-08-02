package player

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type PlayerStatusReq struct{}

type PlayerStatusReply struct {
	HP    int  `json:"hp"`
	MaxHP int  `json:"maxHP"`
	Level int  `json:"level"`
	Gold  int  `json:"gold"`

	CurrentRoom int `json:"currentRoom"`

	Attr      types.AttrState      `json:"attr"`
	Inventory types.InventoryState `json:"inventory"`
	Skill     types.SkillState     `json:"skill"`
}

func (*PlayerStatusReq) ReqType(_ types.PlayerId, _ *PlayerStatusReply) string { return "PlayerStatus" }

func (req *PlayerStatusReq) Handle(ctx *types.PlayerActorCtx, spawning bool) (*PlayerStatusReply, error) {
	d := ctx.State().Data
	return &PlayerStatusReply{
		HP: d.HP, MaxHP: d.MaxHP, Level: d.Level, Gold: d.Attr.Gold, CurrentRoom: d.CurrentRoom,
		Attr: d.Attr, Inventory: d.Inventory, Skill: d.Skill,
	}, nil
}

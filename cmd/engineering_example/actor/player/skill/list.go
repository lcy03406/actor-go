package skill

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type ListSkills struct{}

type ListSkillsReply struct {
	Skills   []types.Skill `json:"skills"`
	MaxSlots int           `json:"maxSlots"`
}

func (*ListSkills) ReqType(_ types.PlayerId, _ *ListSkillsReply) string { return "ListSkills" }

func (req *ListSkills) Handle(ctx *types.PlayerActorCtx, spawning bool) (*ListSkillsReply, error) {
	sk := ctx.State().Data.Skill
	return &ListSkillsReply{Skills: sk.Learned, MaxSlots: sk.MaxSlots}, nil
}

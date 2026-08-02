package attr

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
)

type QueryAttr struct{}

type QueryAttrReply struct {
	Level int `json:"level"`
	Exp   int `json:"exp"`
	Atk   int `json:"atk"`
	Def   int `json:"def"`
	Speed int `json:"speed"`
}

func (*QueryAttr) ReqType(_ types.PlayerId, _ *QueryAttrReply) string { return "QueryAttr" }

func (req *QueryAttr) Handle(ctx *types.PlayerActorCtx, spawning bool) (*QueryAttrReply, error) {
	d := ctx.State().Data
	return &QueryAttrReply{
		Level: d.Level, Exp: d.Attr.Exp,
		Atk: d.Attr.Atk, Def: d.Attr.Def, Speed: d.Attr.Speed,
	}, nil
}

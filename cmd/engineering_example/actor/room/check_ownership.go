// check_ownership.go 定义 Room 迁移检查请求：结构体 + ReqType + Handle 写在同一文件。
package room

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
)

// CheckOwnership 用于集群拓扑变化时检查 Room Actor 归属。
type CheckOwnership struct {
	placement cluster.PlacementStrategy
	selfID    string
}

func (*CheckOwnership) ReqType(_ RoomId, _ actor.OkReply) string { return "CheckOwnership" }

func (req *CheckOwnership) Handle(ctx *actor.ActorContext[RoomId, RoomState], spawning bool) (actor.OkReply, error) {
	if req.placement == nil {
		return actor.OK, nil
	}
	_, leave := cluster.CheckOwnership(req.placement, nil, req.selfID, "Room", ctx.Id().String())
	if leave {
		log.Printf("[迁移] Room %s 应迁移到其他节点 (玩家数=%d)",
			ctx.Id(), len(ctx.State().PlayerIds))
	}
	return actor.OK, nil
}

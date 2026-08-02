// check_ownership.go 定义 Chat 迁移检查请求：结构体 + ReqType + Handle 写在同一文件。
package chat

import (
	"log"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
)

// CheckOwnership 用于集群拓扑变化时检查 Chat Actor 归属。
type CheckOwnership struct {
	placement cluster.PlacementStrategy
	selfID    string
}

func (*CheckOwnership) ReqType(_ ChatId, _ actor.OkReply) string { return "CheckOwnership" }

func (req *CheckOwnership) Handle(ctx *actor.ActorContext[ChatId, ChatState], spawning bool) (actor.OkReply, error) {
	if req.placement == nil {
		return actor.OK, nil
	}
	_, leave := cluster.CheckOwnership(req.placement, nil, req.selfID, "Chat", ctx.Id().String())
	if leave {
		log.Printf("[迁移] Chat %s 应迁移到其他节点 (消息数=%d)",
			ctx.Id(), len(ctx.State().Messages))
	}
	return actor.OK, nil
}

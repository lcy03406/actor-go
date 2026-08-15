// Package cluster 提供 Actor 节点迁移支持。
//
// # 设计思路
//
// 框架不决定迁移策略，只负责在集群拓扑变化时发送通知。
// 用户实现 CheckOwnership 消息的 handler，自行判断是否 Deactivate。
//
// # 通知机制
//
// CheckOwnership 是一个标准化的广播消息，用户约定实现其 handler。
// 当集群拓扑变化时（MemberJoined/MemberLeft），MigrationCoordinator
// 对本地每个 Actor Group 广播 CheckOwnership。
//
// 用户 handler 中：
//   - 调用 cluster.ShouldOwn 检查自己是否还在偏好节点
//   - 若不在且业务允许退出，调用 ctx.State().Deactivate()（Save + Release）
//   - 若业务不允许退出（如正在处理关键逻辑），忽略本次通知
//
// # 优雅退出
//
// 节点关闭前调用 actor.Finalize，Actor 的 Quit/Close 生命周期钩子中
// 调用 Deactivate 释放租约。无需额外的 GracefulShutdown。
//
// # 使用方式
//
//	type CheckOwnership struct{}
//	func (*CheckOwnership) ReqType(_ MyId, _ actor.OkReply) string { return "CheckOwnership" }
//
//	actor.RegisterServe(b, func(ctx *ActorCtx, req *CheckOwnership, _ bool) (actor.OkReply, error) {
//	    if !cluster.ShouldOwn(placement, members, selfID, actorType, ctx.Id().String()) {
//	        if canDeactivate() {
//	            ctx.State().Deactivate()
//	        }
//	    }
//	    return actor.OK, nil
//	})
//
//	coord := cluster.NewMigrationCoordinator(mgr, placement, membership)
//	go coord.Run(ctx, membership.Events())
package cluster

import (
	"context"
	"log/slog"

	"github.com/lcy03406/actor-go/actor"
)

// ─── ActorRef ───

// ActorRef 标识一个 Actor。
type ActorRef struct {
	Type string
	ID   string
}

// String 返回人类可读的标识。
func (a ActorRef) String() string {
	return a.Type + ":" + a.ID
}

// ─── MigrationCoordinator ───

// MigrationCoordinator 监听集群拓扑变化，向本地所有 Actor 广播 CheckOwnership 通知。
//
// 职责：
//   - 监听 MemberEvent
//   - 集群变化时调用所有已注册的 NotifyFunc（用户在这些回调中广播 CheckOwnership）
//
// 不做的事：
//   - 不自动 Deactivate
//   - 不规定迁移策略
//   - 不做 ForceRelease
type MigrationCoordinator struct {
	mgr        *actor.Manager
	placement  PlacementStrategy
	membership Membership
	notifiers  []NotifyFunc
	logger     *slog.Logger
}

// NewMigrationCoordinator 创建迁移协调器。
func NewMigrationCoordinator(mgr *actor.Manager, placement PlacementStrategy, membership Membership) *MigrationCoordinator {
	return &MigrationCoordinator{
		mgr:        mgr,
		placement:  placement,
		membership: membership,
		logger:     mgr.RootLogger().With("component", "MigrationCoordinator"),
	}
}

// RegisterNotify 注册一个 Group 的通知回调。
// 用户在 RegisterServe 时调用：
//
//	coord.RegisterNotify(func() {
//	    actor.Broadcast[MyId](mgr, &CheckOwnership{})
//	})
func (mc *MigrationCoordinator) RegisterNotify(fn NotifyFunc) {
	mc.notifiers = append(mc.notifiers, fn)
}

// Run 启动主循环，监听成员变更事件。
func (mc *MigrationCoordinator) Run(ctx context.Context, events <-chan MemberEvent) {
	mc.logger.Info("migration coordinator started", "self", mc.membership.Self().ID)

	for {
		select {
		case <-ctx.Done():
			mc.logger.Info("migration coordinator stopped")
			return
		case evt, ok := <-events:
			if !ok {
				return
			}
			mc.handleEvent(evt)
		}
	}
}

func (mc *MigrationCoordinator) handleEvent(evt MemberEvent) {
	members := mc.membership.Members()
	selfID := mc.membership.Self().ID

	mc.logger.Info("member event",
		"type", evt.Type,
		"node", evt.Node.ID,
		"nodeType", evt.Node.Type,
		"self", selfID,
		"members", len(members),
		"notifiers", len(mc.notifiers),
	)

	// 调用所有已注册的通知回调
	for _, fn := range mc.notifiers {
		fn()
	}
}

// ─── NotifyFunc ───

// NotifyFunc 是通知回调函数类型。调用时会向对应的 Actor Group 广播 CheckOwnership。
type NotifyFunc func()

// ─── Placement 辅助 ───

// ShouldOwn 判断指定 Actor 是否应该由当前节点拥有。
func ShouldOwn(placement PlacementStrategy, members NodeSet, selfID, actorType, actorId string) bool {
	preferred := placement.Place(actorType, actorId, members)
	return preferred.ID == selfID
}

// CheckOwnership 检查 Actor 归属。若不再拥有，返回目标节点 ID。
//
// 用法（在 CheckOwnership handler 中）：
//
//	if target, leave := cluster.CheckOwnership(placement, members, selfID, actorType, ctx.Id().String()); leave {
//	    ctx.State().Deactivate()
//	}
func CheckOwnership(placement PlacementStrategy, members NodeSet, selfID, actorType, actorId string) (targetNodeID string, shouldLeave bool) {
	preferred := placement.Place(actorType, actorId, members)
	if preferred.ID == "" {
		return "", false
	}
	if preferred.ID != selfID {
		return preferred.ID, true
	}
	return "", false
}

// snapshot.go 定义 PlayerData 的快照/持久化支持。
//
// 【设计说明】
//   PlayerState 同时作为业务数据和快照类型（使用 ShotSelf），
//   持久化时完整保存整个 PlayerState（含所有子模块数据）。
//   如果需要对不同子模块做不同粒度的持久化，可以定义独立的 Snapshot 结构体。
package types

import (
	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
)

// PlayerSnapshotter 使用 ShotSelf，PlayerState 自身就是快照格式。
// 持久化时保存完整状态，恢复时完整覆盖。
type PlayerSnapshotter = grain.ShotSelf[PlayerState]

// GrainState 是 Player Actor 在 grain 模式下的 State 类型别名。
// handler 中通过 ctx.State() 获取 *grain.State，通过 .Data 访问 PlayerState。
//
//	func handle(ctx *ActorContext[PlayerId, GrainState], ...) {
//	    ctx.State().Data.HP += 10
//	    ctx.State().Persist(ctx)  // 持久化
//	    ctx.State().Deactivate(ctx) // 停用 + 释放租约
//	}
//
// T 参数是 *ShotSelf[PlayerState]（指针类型），因为 ShotSelf 的方法是指针接收者。
type GrainState = grain.State[PlayerId, PlayerState, PlayerState, *PlayerSnapshotter]

// PlayerActorCtx 是 Player Actor 在 grain 模式下的 ActorContext 类型别名。
type PlayerActorCtx = actor.ActorContext[PlayerId, GrainState]

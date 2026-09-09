package shared

import "github.com/lcy03406/actor-go/actor"

// RefreshReply 是刷新消息的占位回复(仅满足框架 PtrReply 约束,Multicast 不等待回复)。
type RefreshReply struct{}

// Refresh 是写方向已注册读者推送的刷新消息,直接携带数据指针。
// 读者收到后把数据替换进本地缓存,无需再向写方拉取。
type Refresh[R actor.ActorId, T any] struct {
	Version int64
	Data    *T
}

// ReqType 标识刷新请求。使用双下划线前缀避免与业务请求类型撞名。
func (*Refresh[R, T]) ReqType(_ R, _ *RefreshReply) string {
	return "__shared_refresh__"
}

// RegisterRefresh 在读者 Group 注册刷新处理器,并绑定读者 State 中的缓存。
// 收到 Refresh 时,框架把推送的数据指针解引用后替换进缓存。
//
// 全部类型参数均可从参数推导,调用处无需显式指定:
//
//	actor.Serve(mgr, opts, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
//	    shared.RegisterRefresh(b, func(s *PlayerState) *shared.Cache[PlayerId, BoardId, PlayerState, BoardData] {
//	        return &s.Board
//	    })
//	})
//
// 处理器注册为查询(allow_query=true, allow_spawn=false):刷新只送达已存在的读者。
func RegisterRefresh[R actor.ActorId, S any, A actor.ActorId, T any](
	b *actor.RegistryBuilder[R, S],
	accessor func(*S) *Cache[R, A, S, T],
) {
	actor.RegisterQuery[R, S, *Refresh[R, T], *RefreshReply, Refresh[R, T], RefreshReply](
		b,
		func(ctx *actor.ActorContext[R, S], req *Refresh[R, T], _ bool) (*RefreshReply, error) {
			if req.Data != nil {
				accessor(ctx.State()).Set(Snapshot[T]{Version: req.Version, Data: *req.Data})
			}
			return &RefreshReply{}, nil
		},
	)
}

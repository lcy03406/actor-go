package cluster

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/rpc"
)

// ─── 租约重试配置 ───

// LeaseForceReleaser 是强制释放租约的接口。
// PersistenceManager 通过 Driver 实现此接口。
type LeaseForceReleaser interface {
	ForceRelease(ctx context.Context, actorType string, id string) (int64, error)
}

// RouterConfig 是 Router 的租约重试配置。
type RouterConfig struct {
	LeaseRetry    bool
	ForceReleaser LeaseForceReleaser
}

// RouterOption 是 Router 的配置选项。
type RouterOption func(*RouterConfig)

// WithLeaseRetry 启用租约失败自动重试。
func WithLeaseRetry(enabled bool) RouterOption {
	return func(c *RouterConfig) { c.LeaseRetry = enabled }
}

// WithForceReleaser 设置强制释放租约的接口，自动启用 LeaseRetry。
func WithForceReleaser(releaser LeaseForceReleaser) RouterOption {
	return func(c *RouterConfig) {
		c.ForceReleaser = releaser
		c.LeaseRetry = true
	}
}

// ─── Router 路由分发层 ───

// Router 封装集群路由分发逻辑。内置 Membership + Placement 拓扑和 rpc.Client 连接池。
// 根据 Placement 结果自动决定本地处理（通过 actor.Manager）还是远程转发（通过 rpc.Client）。
//
// 用法：
//
//	router := cluster.NewRouter[D, C, T](membership, placement, mgr)
//
//	// 带租约重试 + 强制释放
//	router := cluster.NewRouter[D, C, T](membership, placement, mgr,
//	    cluster.WithForceReleaser(pm),
//	)
//
//	reply, err := cluster.Call(ctx, router, playerId, &Login{...})
//	cluster.Post(router, playerId, &SaveAndQuit{})
//	cluster.Broadcast(router, &KickAll{})
//	cluster.Multicast(router, []PlayerId{id1, id2}, &SyncState{...})
type Router[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] struct {
	membership Membership
	placement  PlacementStrategy
	mgr        *actor.Manager
	clientPool *clientPool[M, C, T]
	cfg        RouterConfig
}

// NewRouter 创建一个路由分发器。
func NewRouter[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]](
	membership Membership,
	placement PlacementStrategy,
	mgr *actor.Manager,
	opts ...RouterOption,
) *Router[M, C, T] {
	r := &Router[M, C, T]{
		membership: membership,
		placement:  placement,
		mgr:        mgr,
		clientPool: newClientPool[M, C, T](),
	}
	for _, opt := range opts {
		opt(&r.cfg)
	}
	return r
}

// Self 返回本地节点信息。
func (r *Router[M, C, T]) Self() Node {
	return r.membership.Self()
}

// Members 返回当前集群成员列表。
func (r *Router[M, C, T]) Members() NodeSet {
	return r.membership.Members()
}

// Events 返回成员变更事件 channel。
func (r *Router[M, C, T]) Events() <-chan MemberEvent {
	return r.membership.Events()
}

// Place 返回指定 Actor 的偏好节点。
func (r *Router[M, C, T]) Place(actorType, actorId string) Node {
	return r.placement.Place(actorType, actorId, r.membership.Members())
}

// IsLocal 判断指定 Actor 的偏好节点是否为本地。
func (r *Router[M, C, T]) IsLocal(actorType, actorId string) bool {
	preferred := r.Place(actorType, actorId)
	return preferred.ID == r.Self().ID
}

// GetClient 获取或创建到目标节点的 rpc.Client 连接。
func (r *Router[M, C, T]) GetClient(target Node) (*rpc.Client[M, C, T], error) {
	return r.clientPool.getOrDial(target)
}

// RemoveClient 移除并关闭到指定节点的连接。
func (r *Router[M, C, T]) RemoveClient(addr string) {
	r.clientPool.remove(addr)
}

// Close 关闭 Router，释放所有连接和 Membership。
func (r *Router[M, C, T]) Close() error {
	r.clientPool.closeAll()
	return r.membership.Close()
}

// ─── 调用方 API ───

// Post 向指定 Actor 发送 fire-and-forget 消息。
func Post[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
) error {
	err := postOnce(r, id, req)
	if err == nil {
		return nil
	}
	return handleLeasePost(r, id, req, err)
}

// Call 向指定 Actor 发送请求，等待回复。
func Call[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
) (R, error) {
	reply, err := callOnce(ctx, r, id, req)
	if err == nil {
		return reply, nil
	}
	return handleLeaseCall(ctx, r, id, req, reply, err)
}

// Broadcast 向所有同类 Actor 广播消息。
func Broadcast[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	req Q,
) error {
	self := r.Self()
	allNodes := r.Members()

	if _, err := actor.Broadcast(r.mgr, req); err != nil {
		return err
	}

	var lastErr error
	for _, node := range allNodes {
		if node.ID == self.ID {
			continue
		}
		client, err := r.clientPool.getOrDial(node)
		if err != nil {
			lastErr = err
			continue
		}
		if err := rpc.Broadcast[M, C, T](client, req); err != nil {
			lastErr = err
		}
	}
	return lastErr
}

// Multicast 向一组 Actor 发送消息。
func Multicast[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	ids []A,
	req Q,
) (int, error) {
	if len(ids) == 0 {
		return 0, nil
	}

	self := r.Self()
	actorType := string(ids[0].ActorType())

	nodeGroups := make(map[string]struct {
		node Node
		ids  []A
	})
	var localIds []A

	for _, id := range ids {
		preferred := r.Place(actorType, id.String())
		if preferred.ID == self.ID {
			localIds = append(localIds, id)
		} else {
			g, ok := nodeGroups[preferred.ID]
			if !ok {
				g.node = preferred
			}
			g.ids = append(g.ids, id)
			nodeGroups[preferred.ID] = g
		}
	}

	total := 0

	if len(localIds) > 0 {
		n, err := actor.Multicast(r.mgr, localIds, req)
		if err != nil {
			return total, err
		}
		total += n
	}

	for _, g := range nodeGroups {
		client, err := r.clientPool.getOrDial(g.node)
		if err != nil {
			continue
		}
		if err := rpc.Multicast[M, C, T](client, g.ids, req); err != nil {
			continue
		}
		total += len(g.ids)
	}

	return total, nil
}

// ─── 租约重试逻辑 ───

func postOnce[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
) error {
	preferred := r.Place(string(id.ActorType()), id.String())
	if preferred.ID == r.Self().ID {
		return actor.Post(r.mgr, id, req)
	}
	client, err := r.clientPool.getOrDial(preferred)
	if err != nil {
		return err
	}
	return rpc.Post[M, C, T](client, id, req)
}

func callOnce[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
) (R, error) {
	preferred := r.Place(string(id.ActorType()), id.String())
	if preferred.ID == r.Self().ID {
		return actor.Call(ctx, r.mgr, id, req)
	}
	client, err := r.clientPool.getOrDial(preferred)
	if err != nil {
		var zero R
		return zero, err
	}
	return rpc.Call[M, C, T](ctx, client, id, req)
}

func handleLeasePost[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
	origErr error,
) error {
	if !r.cfg.LeaseRetry {
		return origErr
	}

	taken := isLeaseTaken(origErr)
	if taken == nil {
		return origErr
	}

	if tryForwardPost(r, id, req, taken) == nil {
		return nil
	}

	if r.cfg.ForceReleaser != nil {
		actorType := string(id.ActorType())
		if _, forceErr := r.cfg.ForceReleaser.ForceRelease(context.Background(), actorType, id.String()); forceErr != nil {
			return fmt.Errorf("lease taken and force release failed: %w (original: %w)", forceErr, origErr)
		}
		return postOnce(r, id, req)
	}

	return origErr
}

func handleLeaseCall[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
	zero R,
	origErr error,
) (R, error) {
	if !r.cfg.LeaseRetry {
		return zero, origErr
	}

	taken := isLeaseTaken(origErr)
	if taken == nil {
		return zero, origErr
	}

	if reply, ok := tryForwardCall(ctx, r, id, req, taken); ok {
		return reply, nil
	}

	if r.cfg.ForceReleaser != nil {
		actorType := string(id.ActorType())
		if _, forceErr := r.cfg.ForceReleaser.ForceRelease(ctx, actorType, id.String()); forceErr != nil {
			return zero, fmt.Errorf("lease taken and force release failed: %w (original: %w)", forceErr, origErr)
		}
		return callOnce(ctx, r, id, req)
	}

	return zero, origErr
}

func tryForwardPost[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
	taken *grain.ErrLeaseTaken,
) error {
	if taken.Owner == "" || taken.Owner == r.Self().ID {
		return fmt.Errorf("cannot forward to self")
	}

	ownerNode := r.Members().Lookup(taken.Owner)
	if ownerNode == nil {
		return fmt.Errorf("lease owner %s not in cluster", taken.Owner)
	}

	client, err := r.GetClient(*ownerNode)
	if err != nil {
		return err
	}

	if err := rpc.Post[M, C, T](client, id, req); err != nil {
		r.RemoveClient(ownerNode.Addr)
		return err
	}
	return nil
}

func tryForwardCall[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
	taken *grain.ErrLeaseTaken,
) (R, bool) {
	var zero R
	if taken.Owner == "" || taken.Owner == r.Self().ID {
		return zero, false
	}

	ownerNode := r.Members().Lookup(taken.Owner)
	if ownerNode == nil {
		return zero, false
	}

	client, err := r.GetClient(*ownerNode)
	if err != nil {
		return zero, false
	}

	reply, err := rpc.Call[M, C, T](ctx, client, id, req)
	if err != nil {
		r.RemoveClient(ownerNode.Addr)
		return zero, false
	}
	return reply, true
}

func isLeaseTaken(err error) *grain.ErrLeaseTaken {
	var taken *grain.ErrLeaseTaken
	if errors.As(err, &taken) {
		return taken
	}
	return nil
}

// ─── ClientPool 连接池 ───

type clientPool[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client[M, C, T]
}

func newClientPool[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]]() *clientPool[M, C, T] {
	return &clientPool[M, C, T]{
		clients: make(map[string]*rpc.Client[M, C, T]),
	}
}

func (p *clientPool[M, C, T]) getOrDial(target Node) (*rpc.Client[M, C, T], error) {
	p.mu.Lock()
	if client, ok := p.clients[target.Addr]; ok {
		p.mu.Unlock()
		return client, nil
	}
	p.mu.Unlock()

	client := rpc.NewClient[M, C, T](target.Addr)
	if err := client.Connect(); err != nil {
		return nil, &RouteError{
			ActorType: "",
			ActorId:   "",
			Owner:     target.ID,
			Reason:    "failed to connect to remote node: " + err.Error(),
		}
	}

	p.mu.Lock()
	if existing, ok := p.clients[target.Addr]; ok {
		p.mu.Unlock()
		client.Close()
		return existing, nil
	}
	p.clients[target.Addr] = client
	p.mu.Unlock()

	return client, nil
}

func (p *clientPool[M, C, T]) remove(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if client, ok := p.clients[addr]; ok {
		client.Close()
		delete(p.clients, addr)
	}
}

func (p *clientPool[M, C, T]) closeAll() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr, client := range p.clients {
		client.Close()
		delete(p.clients, addr)
	}
}

// ─── RouteError 路由错误 ───

type RouteError struct {
	ActorType string
	ActorId   string
	Owner     string
	Reason    string
}

func (e *RouteError) Error() string {
	return "route error: actor " + e.ActorType + ":" + e.ActorId +
		" should be on node " + e.Owner + ": " + e.Reason
}

// ─── 工具函数 ───

func MembersToNodeSet(members []Node) NodeSet {
	sorted := make([]Node, len(members))
	copy(sorted, members)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	return NodeSet(sorted)
}

func MemberDiff(old, new NodeSet) (joined, left NodeSet) {
	oldMap := make(map[string]Node)
	for _, n := range old {
		oldMap[n.ID] = n
	}
	newMap := make(map[string]Node)
	for _, n := range new {
		newMap[n.ID] = n
	}
	for id, n := range newMap {
		if _, ok := oldMap[id]; !ok {
			joined = append(joined, n)
		}
	}
	for id, n := range oldMap {
		if _, ok := newMap[id]; !ok {
			left = append(left, n)
		}
	}
	return
}

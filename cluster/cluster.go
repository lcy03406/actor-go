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

// ─── Cluster 拓扑层 ───

// Cluster 是集群管理器，组合 Membership 和 Placement。
// 提供节点拓扑信息，不涉及传输和路由决策。
type Cluster struct {
	membership Membership
	placement  PlacementStrategy
	self       Node
}

// New 创建一个新的 Cluster 实例。
func New(membership Membership, placement PlacementStrategy) *Cluster {
	return &Cluster{
		membership: membership,
		placement:  placement,
		self:       membership.Self(),
	}
}

// Self 返回本地节点信息。
func (c *Cluster) Self() Node {
	return c.self
}

// Members 返回当前集群成员列表。
func (c *Cluster) Members() NodeSet {
	return c.membership.Members()
}

// Events 返回成员变更事件 channel。
func (c *Cluster) Events() <-chan MemberEvent {
	return c.membership.Events()
}

// Place 返回指定 Actor 的偏好节点。
func (c *Cluster) Place(actorType, actorId string) Node {
	return c.placement.Place(actorType, actorId, c.membership.Members())
}

// Close 关闭集群。
func (c *Cluster) Close() error {
	return c.membership.Close()
}

// ─── Router 路由分发层 ───

// Dialer 是建立到目标节点的 rpc.Client 的函数类型。
type Dialer[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] func(addr string) *rpc.Client[M, C, T]

// LeaseForceReleaser 是强制释放租约的接口。
// PersistenceManager 通过 Driver 实现此接口。
type LeaseForceReleaser interface {
	ForceRelease(ctx context.Context, actorType string, id string) (int64, error)
}

// RouterConfig 是 Router 的租约重试配置，不依赖泛型参数。
type RouterConfig struct {
	LeaseRetry    bool
	ForceReleaser LeaseForceReleaser
}

// RouterOption 是 Router 的配置选项（非泛型，直接用于 RouterConfig）。
type RouterOption func(*RouterConfig)

// WithLeaseRetry 启用租约失败自动重试。
// 启用后，Call/Post 遇到 *grain.ErrLeaseTaken 时会先尝试转发到持有者节点。
func WithLeaseRetry(enabled bool) RouterOption {
	return func(c *RouterConfig) { c.LeaseRetry = enabled }
}

// WithForceReleaser 设置强制释放租约的接口。
// 设置后，租约持有者不可达时自动强制释放租约后本地重试。
// 通常传入 grain.PersistenceManager，它实现了 LeaseForceReleaser 接口。
// 同时自动启用 LeaseRetry。
func WithForceReleaser(releaser LeaseForceReleaser) RouterOption {
	return func(c *RouterConfig) {
		c.ForceReleaser = releaser
		c.LeaseRetry = true
	}
}

// Router 封装集群路由分发逻辑，M/C/T 与 rpc.Client 泛型参数对应。
// 根据 Placement 结果自动决定本地处理还是远程转发。
//
// 用法：
//
//	// 基础用法（无租约重试）
//	router := cluster.NewRouter(cluster, mgr, dialer)
//
//	// 带租约重试 + 强制释放
//	router := cluster.NewRouter(cluster, mgr, dialer,
//	    cluster.WithForceReleaser(pm),
//	)
//
//	reply, err := cluster.Call(ctx, router, playerId, &Login{...})
//	cluster.Post(router, playerId, &SaveAndQuit{})
//	cluster.Broadcast(router, &KickAll{})
//	cluster.Multicast(router, []PlayerId{id1, id2}, &SyncState{...})
type Router[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] struct {
	cluster    *Cluster
	mgr        *actor.Manager
	dialer     Dialer[M, C, T]
	clientPool *clientPool[M, C, T]
	cfg        RouterConfig
}

// NewRouter 创建一个路由分发器。
func NewRouter[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]](
	cluster *Cluster,
	mgr *actor.Manager,
	dialer Dialer[M, C, T],
	opts ...RouterOption,
) *Router[M, C, T] {
	r := &Router[M, C, T]{
		cluster:    cluster,
		mgr:        mgr,
		dialer:     dialer,
		clientPool: newClientPool[M, C, T](),
	}
	for _, opt := range opts {
		opt(&r.cfg)
	}
	return r
}

// Self 返回本地节点信息。
func (r *Router[M, C, T]) Self() Node {
	return r.cluster.Self()
}

// Members 返回当前集群成员列表。
func (r *Router[M, C, T]) Members() NodeSet {
	return r.cluster.Members()
}

// Events 返回成员变更事件 channel。
func (r *Router[M, C, T]) Events() <-chan MemberEvent {
	return r.cluster.Events()
}

// Place 返回指定 Actor 的偏好节点。
func (r *Router[M, C, T]) Place(actorType, actorId string) Node {
	return r.cluster.Place(actorType, actorId)
}

// IsLocal 判断指定 Actor 的偏好节点是否为本地。
func (r *Router[M, C, T]) IsLocal(actorType, actorId string) bool {
	preferred := r.cluster.Place(actorType, actorId)
	return preferred.ID == r.cluster.Self().ID
}

// GetClient 获取或创建到目标节点的 rpc.Client 连接。
func (r *Router[M, C, T]) GetClient(target Node) (*rpc.Client[M, C, T], error) {
	return r.clientPool.getOrDial(target, r.dialer)
}

// RemoveClient 移除并关闭到指定节点的连接（节点离开时调用）。
func (r *Router[M, C, T]) RemoveClient(addr string) {
	r.clientPool.remove(addr)
}

// Close 关闭 Router，释放所有连接。
func (r *Router[M, C, T]) Close() error {
	r.clientPool.closeAll()
	return r.cluster.Close()
}

// ─── 调用方 API ───

// Post 向指定 Actor 发送 fire-and-forget 消息。
// 自动根据 Place 结果选择本地执行还是远程转发。
//
// 当 Router 配置了租约重试选项时，遇到 *grain.ErrLeaseTaken 会：
//  1. 尝试转发到租约持有者节点
//  2. 若持有者不可达且有 ForceReleaser，强制释放租约后本地重试
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
// 自动根据 Place 结果选择本地执行还是远程转发。
//
// 当 Router 配置了租约重试选项时，遇到 *grain.ErrLeaseTaken 会：
//  1. 尝试转发到租约持有者节点
//  2. 若持有者不可达且有 ForceReleaser，强制释放租约后本地重试
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
	self := r.cluster.Self()
	allNodes := r.cluster.Members()

	// 本地广播
	if _, err := actor.Broadcast(r.mgr, req); err != nil {
		return err
	}

	// 向所有其他节点远程广播
	var lastErr error
	for _, node := range allNodes {
		if node.ID == self.ID {
			continue
		}
		client, err := r.clientPool.getOrDial(node, r.dialer)
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

	self := r.cluster.Self()
	actorType := string(ids[0].ActorType())

	// 按节点分组
	nodeGroups := make(map[string]struct {
		node Node
		ids  []A
	})
	var localIds []A

	for _, id := range ids {
		preferred := r.cluster.Place(actorType, id.String())
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

	// 本地发送
	if len(localIds) > 0 {
		n, err := actor.Multicast(r.mgr, localIds, req)
		if err != nil {
			return total, err
		}
		total += n
	}

	// 按节点远程发送
	for _, g := range nodeGroups {
		client, err := r.clientPool.getOrDial(g.node, r.dialer)
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

// postOnce 执行单次 Post 路由。
func postOnce[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
) error {
	preferred := r.cluster.Place(string(id.ActorType()), id.String())
	if preferred.ID == r.cluster.Self().ID {
		return actor.Post(r.mgr, id, req)
	}
	client, err := r.clientPool.getOrDial(preferred, r.dialer)
	if err != nil {
		return err
	}
	return rpc.Post[M, C, T](client, id, req)
}

// callOnce 执行单次 Call 路由。
func callOnce[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
) (R, error) {
	preferred := r.cluster.Place(string(id.ActorType()), id.String())
	if preferred.ID == r.cluster.Self().ID {
		return actor.Call(ctx, r.mgr, id, req)
	}
	client, err := r.clientPool.getOrDial(preferred, r.dialer)
	if err != nil {
		var zero R
		return zero, err
	}
	return rpc.Call[M, C, T](ctx, client, id, req)
}

// handleLeasePost 处理 Post 的租约重试逻辑。
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

	// 尝试转发到租约持有者节点
	if tryForwardPost(r, id, req, taken) == nil {
		return nil
	}

	// 强制释放租约后本地重试
	if r.cfg.ForceReleaser != nil {
		actorType := string(id.ActorType())
		if _, forceErr := r.cfg.ForceReleaser.ForceRelease(context.Background(), actorType, id.String()); forceErr != nil {
			return fmt.Errorf("lease taken and force release failed: %w (original: %w)", forceErr, origErr)
		}
		return postOnce(r, id, req)
	}

	return origErr
}

// handleLeaseCall 处理 Call 的租约重试逻辑。
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

	// 尝试转发到租约持有者节点
	if reply, ok := tryForwardCall(ctx, r, id, req, taken); ok {
		return reply, nil
	}

	// 强制释放租约后本地重试
	if r.cfg.ForceReleaser != nil {
		actorType := string(id.ActorType())
		if _, forceErr := r.cfg.ForceReleaser.ForceRelease(ctx, actorType, id.String()); forceErr != nil {
			return zero, fmt.Errorf("lease taken and force release failed: %w (original: %w)", forceErr, origErr)
		}
		return callOnce(ctx, r, id, req)
	}

	return zero, origErr
}

// tryForwardPost 尝试将 Post 请求转发到租约持有者节点。
func tryForwardPost[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	r *Router[M, C, T],
	id A,
	req Q,
	taken *grain.ErrLeaseTaken,
) error {
	if taken.Owner == "" || taken.Owner == r.cluster.Self().ID {
		return fmt.Errorf("cannot forward to self")
	}

	ownerNode := r.cluster.Members().Lookup(taken.Owner)
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

// tryForwardCall 尝试将 Call 请求转发到租约持有者节点。
// 返回 (reply, ok)，ok=true 表示转发成功。
func tryForwardCall[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
	ctx context.Context,
	r *Router[M, C, T],
	id A,
	req Q,
	taken *grain.ErrLeaseTaken,
) (R, bool) {
	var zero R
	if taken.Owner == "" || taken.Owner == r.cluster.Self().ID {
		return zero, false
	}

	ownerNode := r.cluster.Members().Lookup(taken.Owner)
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

// isLeaseTaken 判断错误是否为租约被占用错误。
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

func (p *clientPool[M, C, T]) getOrDial(target Node, dialer Dialer[M, C, T]) (*rpc.Client[M, C, T], error) {
	p.mu.Lock()
	if client, ok := p.clients[target.Addr]; ok {
		p.mu.Unlock()
		return client, nil
	}
	p.mu.Unlock()

	if dialer == nil {
		return nil, &RouteError{
			ActorType: "",
			ActorId:   "",
			Owner:     target.ID,
			Reason:    "no dialer set, cannot connect to remote node",
		}
	}

	client := dialer(target.Addr)
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

// RouteError 表示路由失败，携带目标节点信息供调用方处理。
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

// MembersToNodeSet 将成员列表转换为有序 NodeSet。
func MembersToNodeSet(members []Node) NodeSet {
	sorted := make([]Node, len(members))
	copy(sorted, members)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].ID < sorted[j].ID
	})
	return NodeSet(sorted)
}

// MemberDiff 比较新旧成员列表，返回加入和离开的节点。
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

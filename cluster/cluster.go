package cluster

import (
	"context"
	"sort"
	"sync"

	"github.com/lcy03406/actor-go/actor"
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

// Router 封装集群路由分发逻辑，M/C/T 与 rpc.Client 泛型参数对应。
// 根据 Placement 结果自动决定本地处理（通过 actor.Manager）还是远程转发（通过 rpc.Client）。
//
// 用法：
//
//	router := cluster.NewRouter(cluster, mgr, func(addr string) *rpc.Client[...] {
//	    return rpc.NewClient[...](addr)
//	})
//
//	reply, err := router.Call(ctx, playerId, &Login{...})
//	router.Post(playerId, &SaveAndQuit{})
//	router.Broadcast(&KickAll{})
//	router.Multicast([]PlayerId{id1, id2}, &SyncState{...})
type Router[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] struct {
	cluster    *Cluster
	mgr        *actor.Manager
	dialer     Dialer[M, C, T]
	clientPool *clientPool[M, C, T]
}

// NewRouter 创建一个路由分发器。
func NewRouter[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]](cluster *Cluster, mgr *actor.Manager, dialer Dialer[M, C, T]) *Router[M, C, T] {
	return &Router[M, C, T]{
		cluster:    cluster,
		mgr:        mgr,
		dialer:     dialer,
		clientPool: newClientPool[M, C, T](),
	}
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
func Post[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
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

// Call 向指定 Actor 发送请求，等待回复。
// 自动根据 Place 结果选择本地执行还是远程转发。
func Call[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](
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

// Broadcast 向所有同类 Actor 广播消息。
// 本地：通过 actor.Manager 广播本地 Actor；
// 远程：向所有其他节点发送 Broadcast。
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
// 按 Place 结果将 Actor 按目标节点分组，本地部分通过 actor.Manager 发送，
// 远程部分按节点聚合后通过 rpc.Multicast 发送。
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

// ─── ClientPool 连接池 ───

// clientPool 管理到各远程节点的 rpc.Client 连接。
// 懒加载，按需建立连接，线程安全。
type clientPool[M rpc.Message, C rpc.Codec[M], T rpc.Transport[M]] struct {
	mu      sync.Mutex
	clients map[string]*rpc.Client[M, C, T] // addr -> client
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
	// 双重检查：可能另一个 goroutine 已经建立了连接
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
	Owner     string // 偏好节点 ID
	Reason    string // 失败原因
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

package cluster

import (
	"sort"
)

// StaticMembership 是一个静态集群成员实现，用于测试和单节点部署。
//
// 不支持动态加入/离开，所有节点通过初始化时指定。
type StaticMembership struct {
	self    Node
	members NodeSet
	events  chan MemberEvent
	closed  bool
}

// NewStaticMembership 创建一个静态成员管理。
// nodes 是所有节点列表，selfId 是本地节点 ID。
func NewStaticMembership(selfId string, nodes NodeSet) *StaticMembership {
	var self Node
	members := make(NodeSet, 0, len(nodes))
	for _, n := range nodes {
		if n.ID == selfId {
			self = n
		}
		members = append(members, n)
	}
	return &StaticMembership{
		self:    self,
		members: members,
		events:  make(chan MemberEvent, 16),
	}
}

func (m *StaticMembership) Self() Node {
	return m.self
}

func (m *StaticMembership) Members() NodeSet {
	return m.members
}

func (m *StaticMembership) Events() <-chan MemberEvent {
	return m.events
}

func (m *StaticMembership) Join(seeds []string) error {
	return nil
}

func (m *StaticMembership) Leave() error {
	return nil
}

func (m *StaticMembership) Close() error {
	if !m.closed {
		m.closed = true
		close(m.events)
	}
	return nil
}

// ─────────────────────────────────────────────
// Cluster 是集群的顶层入口，组合成员管理、放置策略和本地节点信息。
// ─────────────────────────────────────────────

// Cluster 是集群管理器，组合 Membership、Placement 和 Transport。
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

// Resolve 对一条消息做完整的路由决策。
//
// actorType + actorId 用于定位偏好节点，
// allowSpawn + allowQuery 决定路由模式（与 actor.RegisterSpawn/Query/Serve 对应）。
func (c *Cluster) Resolve(actorType, actorId string, allowSpawn, allowQuery bool) RouteResult {
	preferred := c.Place(actorType, actorId)
	return Route(c.self, preferred, allowSpawn, allowQuery)
}

// IsLocal 判断指定 Actor 的偏好节点是否为本地。
func (c *Cluster) IsLocal(actorType, actorId string) bool {
	preferred := c.Place(actorType, actorId)
	return preferred.ID == c.self.ID
}

// Close 关闭集群。
func (c *Cluster) Close() error {
	return c.membership.Close()
}

// ─────────────────────────────────────────────
// 工具函数
// ─────────────────────────────────────────────

// MembersToNodeSet 将成员列表（通过哈希环排序后）转换为 NodeSet，
// 保证确定性顺序。
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

// membership.go 实现 DynamicMembership，支持模拟节点加入/离开。
// 放在 setup 包而非 cluster 包，因为它只用于示例/测试。
package setup

import (
	"sync"

	"github.com/lcy03406/actor-go/cluster"
)

// DynamicMembership 是一个支持动态添加/删除节点的内存 Membership 实现。
// 用于示例和测试中模拟集群拓扑变化。
type DynamicMembership struct {
	mu      sync.Mutex
	self    cluster.Node
	members cluster.NodeSet
	events  chan cluster.MemberEvent
}

func newDynamicMembership(self cluster.Node, members ...cluster.Node) *DynamicMembership {
	return &DynamicMembership{
		self:    self,
		members: cluster.NodeSet(members),
		events:  make(chan cluster.MemberEvent, 100),
	}
}

func (d *DynamicMembership) Self() cluster.Node {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.self
}

func (d *DynamicMembership) Members() cluster.NodeSet {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(cluster.NodeSet, len(d.members))
	copy(result, d.members)
	return result
}

func (d *DynamicMembership) Events() <-chan cluster.MemberEvent {
	return d.events
}

func (d *DynamicMembership) Join(seeds []string) error { return nil }
func (d *DynamicMembership) Leave() error              { return nil }
func (d *DynamicMembership) Close() error              { return nil }

// updateSelf 更新本地节点信息（用于端口 0 场景，Server 绑定后更新实际地址）。
// oldID 用于在 members 中定位旧节点记录。
func (d *DynamicMembership) updateSelf(oldID string, self cluster.Node) {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.self = self
	for i, m := range d.members {
		if m.ID == oldID {
			d.members[i] = self
			return
		}
	}
}

// AddNode 模拟节点加入集群，触发 MemberJoined 事件。
func (d *DynamicMembership) AddNode(n cluster.Node) {
	d.mu.Lock()
	for _, m := range d.members {
		if m.ID == n.ID {
			d.mu.Unlock()
			return
		}
	}
	d.members = append(d.members, n)
	membersCopy := make(cluster.NodeSet, len(d.members))
	copy(membersCopy, d.members)
	d.mu.Unlock()

	d.events <- cluster.MemberEvent{
		Type:  cluster.MemberJoined,
		Node:  n,
		Nodes: membersCopy,
	}
}

// RemoveNode 模拟节点离开集群，触发 MemberLeft 事件。
func (d *DynamicMembership) RemoveNode(nodeID string) {
	d.mu.Lock()
	var removed cluster.Node
	newMembers := make(cluster.NodeSet, 0, len(d.members))
	for _, m := range d.members {
		if m.ID == nodeID {
			removed = m
		} else {
			newMembers = append(newMembers, m)
		}
	}
	if removed.ID == "" {
		d.mu.Unlock()
		return
	}
	d.members = newMembers
	d.mu.Unlock()

	d.events <- cluster.MemberEvent{
		Type:  cluster.MemberLeft,
		Node:  removed,
		Nodes: newMembers,
	}
}

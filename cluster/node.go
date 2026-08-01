// Package cluster 提供集群成员管理、Actor 放置策略和路由决策。
//
// 本包不依赖 actor-go 的其他包，只做路由决策，不做消息分发。
// 消息分发由 grain 层或用户代码完成。
package cluster

// Node 表示集群中的一个节点。
type Node struct {
	// ID 是节点的唯一标识。
	ID string
	// Addr 是节点的通信地址，例如 "192.168.1.10:9000"。
	Addr string
	// Type 是节点的角色/类型（如 "player-server", "chat-server", "worker"）。
	// 结合 GroupMapping 决定该节点承载哪些 Actor 类型。
	// 为空时表示同构节点，承载所有 Actor 类型（向后兼容）。
	Type string
	// Meta 是节点的元数据（标签、权重等），可用于自定义放置策略。
	Meta map[string]string
}

// NodeSet 表示集群中所有节点的集合。
type NodeSet []Node

// Lookup 按 ID 查找节点，返回节点指针，不存在返回 nil。
func (ns NodeSet) Lookup(id string) *Node {
	for i := range ns {
		if ns[i].ID == id {
			return &ns[i]
		}
	}
	return nil
}

// Contains 判断节点 ID 是否在集合中。
func (ns NodeSet) Contains(id string) bool {
	return ns.Lookup(id) != nil
}

// ─── GroupMapping: NodeType → ActorType 映射 ───

// GroupMapping 定义节点类型与 Actor 类型的映射关系。
// 一个节点类型可以承载多个 Actor 类型，一个 Actor 类型也可以被多种节点类型承载。
//
// 用法：
//
//	mapping := cluster.GroupMapping{
//	    "player-server": {"Player"},
//	    "room-server":   {"Room"},
//	    "chat-server":   {"Chat"},
//	    "all-in-one":    {"Player", "Room", "Chat"},
//	}
//
// 节点通过 Node.Type 声明自己的角色，运行时通过 GroupMapping 查询该节点能否承载指定 ActorType。
type GroupMapping map[string][]string

// HasGroup 判断指定节点类型的节点是否能承载给定的 ActorType。
// 若 nodeType 为空（同构模式），始终返回 true。
// 若 nodeType 不在 mapping 中，返回 false。
func (m GroupMapping) HasGroup(nodeType, actorType string) bool {
	if nodeType == "" {
		return true
	}
	groups, ok := m[nodeType]
	if !ok {
		return false
	}
	for _, g := range groups {
		if g == actorType {
			return true
		}
	}
	return false
}

// NodeCanHost 判断指定节点是否能承载给定的 ActorType。
func (m GroupMapping) NodeCanHost(n Node, actorType string) bool {
	return m.HasGroup(n.Type, actorType)
}

// FilterByGroup 过滤出能承载指定 ActorType 的节点。
func (m GroupMapping) FilterByGroup(members NodeSet, actorType string) NodeSet {
	if len(members) == 0 {
		return members
	}
	filtered := make(NodeSet, 0, len(members))
	for _, n := range members {
		if m.NodeCanHost(n, actorType) {
			filtered = append(filtered, n)
		}
	}
	return filtered
}

// GroupsOf 返回指定节点类型承载的 Actor 类型列表。
// 若 nodeType 为空，返回 nil（表示同构）。
func (m GroupMapping) GroupsOf(nodeType string) []string {
	return m[nodeType]
}

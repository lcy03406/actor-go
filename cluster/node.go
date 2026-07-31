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

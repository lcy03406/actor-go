package cluster

// Membership 表示集群成员管理接口。
//
// 实现可以是基于 gossip（memberlist）、基于 etcd、基于 consul 等。
// 当集群成员变化时，通过 Events() channel 通知。
type Membership interface {
	// Self 返回本地节点信息。
	Self() Node

	// Members 返回当前集群所有节点列表。
	Members() NodeSet

	// Events 返回成员变更事件 channel。
	// 当节点加入或离开时触发。
	Events() <-chan MemberEvent

	// Join 加入集群，传入种子节点地址列表。
	Join(seeds []string) error

	// Leave 优雅离开集群。
	Leave() error

	// Close 关闭成员管理。
	Close() error
}

// MemberEventType 表示成员变更类型。
type MemberEventType int

const (
	MemberJoined MemberEventType = iota
	MemberLeft
)

// MemberEvent 是成员变更事件。
type MemberEvent struct {
	Type  MemberEventType
	Node  Node
	Nodes NodeSet // 变更后的完整成员列表
}

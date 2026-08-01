package cluster

import (
	"fmt"
	"hash/crc32"
	"sort"
)

// PlacementStrategy 决定一个 Actor 应该被放置在哪个节点上。
//
// 放置结果称为"偏好节点"（preferred node）。如果偏好节点不可用
// （网络分区、宕机），实际激活位置通过 lease 竞争决定。
type PlacementStrategy interface {
	// Place 根据 Actor 类型和 ID 返回偏好节点。
	// members 是当前集群成员列表。
	Place(actorType, actorId string, members NodeSet) Node
}

// ConsistentHashPlacement 基于一致性哈希的放置策略。
//
// 使用 CRC32 计算哈希值，每个物理节点创建 virtualNodes 个虚拟节点。
// 虚拟节点越多分布越均匀，但哈希环重建成本越高。
//
// 若设置了 GroupMapping，则只会在能承载该 ActorType 的节点间进行哈希放置。
type ConsistentHashPlacement struct {
	VirtualNodes int
	// Mapping 定义节点类型到 Actor 类型的映射。
	// 为 nil 时表示同构集群（向后兼容）。
	Mapping GroupMapping
}

// NewConsistentHashPlacement 创建一个一致性哈希放置策略。
// virtualNodes 默认为 128。
func NewConsistentHashPlacement(virtualNodes int) *ConsistentHashPlacement {
	if virtualNodes <= 0 {
		virtualNodes = 128
	}
	return &ConsistentHashPlacement{VirtualNodes: virtualNodes}
}

// WithGroupMapping 设置节点类型到 Actor 类型的映射。
func (p *ConsistentHashPlacement) WithGroupMapping(m GroupMapping) *ConsistentHashPlacement {
	p.Mapping = m
	return p
}

// hashRingEntry 是哈希环上的一个条目。
type hashRingEntry struct {
	hash uint32
	node Node
}

// Place 使用一致性哈希选择偏好节点。
//
// 算法：对每个节点创建 virtualNodes 个虚拟节点，所有虚拟节点按哈希值排序形成环，
// 目标 key 的哈希在环上顺时针找到的第一个节点即为偏好节点。
// 若存在异构节点（设置了 Mapping），则仅在有对应 Group 的节点间进行哈希放置。
func (p *ConsistentHashPlacement) Place(actorType, actorId string, members NodeSet) Node {
	if len(members) == 0 {
		return Node{}
	}

	// 异构感知：仅考虑能承载该 ActorType 的节点
	eligible := p.filterEligible(actorType, members)
	if len(eligible) == 0 {
		return Node{}
	}
	if len(eligible) == 1 {
		return eligible[0]
	}

	key := actorType + ":" + actorId
	return p.placeOnRing(key, eligible)
}

func (p *ConsistentHashPlacement) filterEligible(actorType string, members NodeSet) NodeSet {
	if p.Mapping == nil {
		return members
	}
	return p.Mapping.FilterByGroup(members, actorType)
}

func (p *ConsistentHashPlacement) placeOnRing(key string, members NodeSet) Node {
	ring := make([]hashRingEntry, 0, len(members)*p.VirtualNodes)

	for _, node := range members {
		for i := 0; i < p.VirtualNodes; i++ {
			virtualKey := node.ID + "-" + itoa(i)
			ring = append(ring, hashRingEntry{
				hash: crc32.ChecksumIEEE([]byte(virtualKey)),
				node: node,
			})
		}
	}

	sort.Slice(ring, func(i, j int) bool {
		return ring[i].hash < ring[j].hash
	})

	keyHash := crc32.ChecksumIEEE([]byte(key))

	// 二分查找顺时针第一个节点
	idx := sort.Search(len(ring), func(i int) bool {
		return ring[i].hash >= keyHash
	})
	if idx == len(ring) {
		idx = 0 // wrap around
	}

	return ring[idx].node
}

// ─── PlacementError ───

// PlacementError 表示没有可用节点放置 Actor。
type PlacementError struct {
	ActorType string
	ActorId   string
}

func (e *PlacementError) Error() string {
	return fmt.Sprintf("no eligible node for actor %s:%s", e.ActorType, e.ActorId)
}

// ─── GroupAwarePlacement ───

// GroupAwarePlacement 是 PlacementStrategy 的包装器，
// 通过 GroupMapping 在调用底层策略前自动过滤掉不承载指定 ActorType 的节点。
// 这允许任何 PlacementStrategy 在异构集群中正确工作。
//
// 如果所有节点 Type 为空（同构模式），此包装器透明传递，不影响原有行为。
type GroupAwarePlacement struct {
	inner   PlacementStrategy
	mapping GroupMapping
}

// NewGroupAwarePlacement 创建一个支持异构感知的放置策略包装器。
func NewGroupAwarePlacement(inner PlacementStrategy, mapping GroupMapping) *GroupAwarePlacement {
	return &GroupAwarePlacement{inner: inner, mapping: mapping}
}

// Place 先过滤出承载指定 ActorType 的节点，再委托给底层策略。
func (g *GroupAwarePlacement) Place(actorType, actorId string, members NodeSet) Node {
	eligible := g.mapping.FilterByGroup(members, actorType)
	if len(eligible) == 0 {
		return Node{}
	}
	return g.inner.Place(actorType, actorId, eligible)
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 12)
	for n > 0 {
		buf = append(buf, byte('0'+n%10))
		n /= 10
	}
	for i, j := 0, len(buf)-1; i < j; i, j = i+1, j-1 {
		buf[i], buf[j] = buf[j], buf[i]
	}
	return string(buf)
}

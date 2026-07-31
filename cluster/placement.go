package cluster

import (
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
type ConsistentHashPlacement struct {
	VirtualNodes int
}

// NewConsistentHashPlacement 创建一个一致性哈希放置策略。
// virtualNodes 默认为 128。
func NewConsistentHashPlacement(virtualNodes int) *ConsistentHashPlacement {
	if virtualNodes <= 0 {
		virtualNodes = 128
	}
	return &ConsistentHashPlacement{VirtualNodes: virtualNodes}
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
func (p *ConsistentHashPlacement) Place(actorType, actorId string, members NodeSet) Node {
	if len(members) == 0 {
		return Node{}
	}
	if len(members) == 1 {
		return members[0]
	}

	key := actorType + ":" + actorId
	return p.placeOnRing(key, members)
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

// Package room 是房间 Actor：持有"房间里有哪些玩家、他们长什么样"的成员缓存。
//
// 房间不持有任何权威数据：等级 / 血量归玩家自己，变更时由玩家 Post 摘要上来。
// 这条约定让房间永远不需要反向 Call 玩家 —— 缓存替代反向调用，是避免调用环的基本手段。
package room

import (
	"slices"
	"strings"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// State 是房间状态。房间是纯内存缓存，不持久化：进程重启后房间与成员一起从零开始。
type State struct {
	Members map[ids.PlayerId]playerapi.Brief
}

func (s *State) ensure() {
	if s.Members == nil {
		s.Members = make(map[ids.PlayerId]playerapi.Brief)
	}
}

// members 返回按 OpenID 稳定排序的成员列表：map 迭代无序，广播与展示都需要稳定顺序。
func (s *State) members() []ids.PlayerId {
	list := make([]ids.PlayerId, 0, len(s.Members))
	for id := range s.Members {
		list = append(list, id)
	}
	slices.SortFunc(list, func(a, b ids.PlayerId) int { return strings.Compare(a.OpenID, b.OpenID) })
	return list
}

// briefs 返回当前缓存的成员摘要（房间视角的玩家视图）。
func (s *State) briefs() []playerapi.Brief {
	list := s.members()
	out := make([]playerapi.Brief, 0, len(list))
	for _, id := range list {
		out = append(out, s.Members[id])
	}
	return out
}

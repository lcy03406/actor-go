// types 包定义 Player Actor 的核心类型：ActorId 和 State。
//
// 【依赖方向】
//
//   types/  ──→ actor 包（框架）          ← 零业务依赖
//   attr/   ──→ types/                    ← 子模块只依赖类型
//   inventory/ ──→ types/
//   skill/  ──→ types/
//   player/ ──→ types/ + attr/ + inventory/ + skill/   ← player 聚合所有子模块
//   setup/  ──→ player/                   ← setup 只依赖一层
//
// player 包是唯一的组装点：负责 Manager 注册 + RPC 注册。
// 子模块只定义请求和 Handle，不关心注册。
package types

import (
	"fmt"

	"github.com/lcy03406/actor-go/actor"
)

// ─── ActorId ───

type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
	return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

// ─── State ───

type PlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
	Gold  int `json:"gold"`

	Attr      AttrState      `json:"attr"`
	Inventory InventoryState `json:"inventory"`
	Skill     SkillState     `json:"skill"`
}

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
	MaxHP int `json:"maxHP"` // 生命上限：升级（attr.AddExp）会随之提升，治疗/回血受此约束
	Level int `json:"level"`

	// CurrentRoom 记录玩家当前所在的房间（0 表示不在任何房间）。
	CurrentRoom int `json:"currentRoom"`

	Attr      AttrState      `json:"attr"`
	Inventory InventoryState `json:"inventory"`
	Skill     SkillState     `json:"skill"`
}

// ─── PlayerState 聚合级领域方法 ───
// 这些方法跨子 state 字段做协调（如 ApplyHeal 同时动 HP 与 Gold），
// 故定义在聚合根 PlayerState 上，直接读写各子 state 字段（同 component 内合法）。
// 这些方法不依赖 actor / manager，保持 types 包零业务依赖。

// TakeDamage 受击方在本 actor 内原子执行的伤害结算：
// 校验「双方同处一室」，用 logic 规则计算实际伤害并扣血，返回扣血值与是否死亡。
// 注意：同房间校验与本 actor 状态变更在同一调用内完成，避免 TOCTOU。
//
// attackerRoom 由攻击方在跨 actor 投递时一并带来；本方法只信任自身 CurrentRoom。
// 返回 (mitigated 实际扣血, dead 是否死亡)。
func (s *PlayerState) TakeDamage(rawDamage, attackerRoom int, attackerId PlayerId) (mitigated int, dead bool) {
	// ★ 同处一室校验（原子，避免 TOCTOU）
	if s.CurrentRoom == 0 || s.CurrentRoom != attackerRoom {
		return 0, false
	}
	// 实际伤害 = 攻击方伤害 vs 自身防御（系数沿用全局约定）。
	// 这里直接内联简化公式，避免 types 依赖 logic 包：
	//   mit = max(1, rawDamage - Def/2)，并附带 10% 暴击翻倍。
	def := s.Attr.Def
	mit := rawDamage - def/2
	if mit < 1 {
		mit = 1
	}
	// 暴击：简化随机（示例用确定性规则：伤害为奇数视为暴击）
	if rawDamage%2 == 1 {
		mit *= 2
	}
	s.HP -= mit
	if s.HP <= 0 {
		s.HP = 0
		return mit, true
	}
	return mit, false
}

// ApplyHeal 在本 actor 内执行的治疗结算：受 MaxHP 上限与 Gold 约束。
// 返回 (healed 实际恢复, goldSpent 消耗金币, reason 未治疗原因)。
// costPerHP 为每点治疗消耗的金币，由调用方（handler）传入以便调参。
func (s *PlayerState) ApplyHeal(amount, costPerHP int) (healed, goldSpent int, reason string) {
	if s.HP >= s.MaxHP {
		return 0, 0, "已处于满血"
	}
	if amount <= 0 {
		return 0, 0, "治疗量无效"
	}
	need := s.MaxHP - s.HP
	healable := amount
	if need < healable {
		healable = need
	}
	maxByGold := s.Attr.Gold / costPerHP
	if maxByGold < healable {
		healable = maxByGold
	}
	if healable <= 0 {
		return 0, 0, "金币不足"
	}
	oldHP := s.HP
	s.HP += healable
	realHeal := s.HP - oldHP
	spent := healable * costPerHP
	s.Attr.Gold -= spent
	return realHeal, spent, ""
}

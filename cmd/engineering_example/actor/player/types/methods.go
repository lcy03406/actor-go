// methods.go 承载 PlayerState / AttrState 的领域方法（状态逻辑）。
//
// 设计约束：Go 不允许在其他包给非本包类型定义方法，因此这些方法必须与
// PlayerState / AttrState 同处 types 包。这样「状态逻辑」就挂在状态自身上
// （state 带逻辑），handler 退化为入口。
//
// 分层：
//   - AttrState 的方法（AddGold / Upgrade）：只动属性与金币（成长资源）。
//   - PlayerState 的方法（AddExp / TakeDamage / ApplyHeal）：跨子 state 的聚合级行为
//     （升级会同时提升 Attr.Atk/Def、PlayerState.Level/MaxHP/HP；受击/治疗动 HP/Gold）。
package types

import (
	"github.com/lcy03406/actor-go/cmd/engineering_example/logic"
)

// ─── AttrState 方法（仅属性与金币） ───

// AddGold 增加金币。
func (a *AttrState) AddGold(amount int) {
	a.Gold += amount
}

// Upgrade 消耗金币升级某项属性。返回 (newValue, cost, ok)。
// stat ∈ {atk, def, speed}。
func (a *AttrState) Upgrade(stat string) (newValue, cost int, ok bool) {
	const upgradeCost = 50
	if a.Gold < upgradeCost {
		return 0, upgradeCost, false
	}
	a.Gold -= upgradeCost
	switch stat {
	case "atk":
		a.Atk += 2
		return a.Atk, upgradeCost, true
	case "def":
		a.Def += 2
		return a.Def, upgradeCost, true
	case "speed":
		a.Speed += 2
		return a.Speed, upgradeCost, true
	default:
		return 0, upgradeCost, false
	}
}

// ─── PlayerState 方法（聚合级） ───

// AddExp 增加经验并按需升级（循环直至经验不足）。
// 升级时提升 Attr.Atk/Def 与 MaxHP，并回满血——把「属性成长」与「生命值」结合。
// 返回最终 (exp, level, levelUp)。
func (s *PlayerState) AddExp(amount int) (exp, level int, levelUp bool) {
	s.Attr.Exp += amount
	for s.Attr.Exp >= logic.CalcExpToLevel(s.Level) {
		s.Attr.Exp -= logic.CalcExpToLevel(s.Level)
		s.Level++
		s.Attr.Atk += 5
		s.Attr.Def += 3
		s.MaxHP += 20
		s.HP = s.MaxHP // 升级回满血
		levelUp = true
	}
	return s.Attr.Exp, s.Level, levelUp
}

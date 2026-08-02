// logic 包定义与 Actor 框架无关的公共游戏逻辑。
//
// 放置位置：与 actor/ 平级，不依赖 actor 框架的任何包。
// 所有纯计算、纯算法的代码放在这里，可以被任何 actor 子模块引用。
//
//	actor/player/skill/ → logic/     ← skill 引用伤害公式
//	actor/player/attr/  → logic/     ← attr 引用战斗公式
//	actor/room/         → logic/     ← room 也可以引用寻路
//
//	logic/ 零依赖（只依赖标准库）
package logic

import "math"

// ─── 伤害公式 ───

// DamageResult 伤害计算结果。
type DamageResult struct {
	Raw       int     `json:"raw"`       // 原始伤害
	Mitigated int     `json:"mitigated"` // 减免后伤害
	Critical  bool    `json:"critical"`  // 是否暴击
	Blocked   float64 `json:"blocked"`   // 格挡减免百分比
}

// CalcDamage 计算技能伤害。
//
// 公式: 基础伤害 + 攻击力*系数 + 技能等级*成长 - 防御力*减免
// 暴击率 = critChance, 暴击伤害 = 1.5x
//
// 这是纯函数，不依赖任何 actor 状态，可以独立测试。
func CalcDamage(atk, def, skillLevel int, baseDmg, atkCoef, growth float64, critChance float64) DamageResult {
	raw := int(baseDmg + float64(atk)*atkCoef + float64(skillLevel)*growth)
	mitigated := raw - int(float64(def)*0.3)
	if mitigated < 1 {
		mitigated = 1
	}

	result := DamageResult{
		Raw:       raw,
		Mitigated: mitigated,
	}

	// 暴击判定
	if critChance > 0 && critChance >= math.Abs(float64(skillLevel)*0.05) {
		result.Critical = true
		result.Mitigated = int(float64(mitigated) * 1.5)
	}

	return result
}

// CalcHeal 计算治疗量。
func CalcHeal(baseHeal, bonus int) int {
	return baseHeal + bonus
}

// CalcExpToLevel 计算升级所需经验。
func CalcExpToLevel(level int) int {
	return level * 100
}

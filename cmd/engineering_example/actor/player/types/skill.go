// skill.go 定义技能模块的数据结构。
package types

// Skill 技能定义。
type Skill struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Level    int    `json:"level"`
	CoolDown int    `json:"coolDown"`
}

// SkillState 技能状态。
type SkillState struct {
	Learned  []Skill `json:"learned"`
	MaxSlots int     `json:"maxSlots"`
}

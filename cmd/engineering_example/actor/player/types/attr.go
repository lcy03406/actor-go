// attr.go 定义属性模块的数据结构。
package types

// AttrState 属性模块状态。
// Gold 作为成长资源归属 AttrState（加金币 / 加经验 / 升级属性都围绕它）。
type AttrState struct {
	Exp   int `json:"exp"`
	Gold  int `json:"gold"`
	Atk   int `json:"atk"`
	Def   int `json:"def"`
	Speed int `json:"speed"`
}

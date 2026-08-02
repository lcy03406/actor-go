// attr.go 定义属性模块的数据结构。
package types

// AttrState 属性模块状态。
type AttrState struct {
	Exp   int `json:"exp"`
	Atk   int `json:"atk"`
	Def   int `json:"def"`
	Speed int `json:"speed"`
}

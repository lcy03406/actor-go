// inventory.go 定义道具模块的数据结构。
package types

// Item 道具定义。
type Item struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Count int    `json:"count"`
	Type  string `json:"type"` // potion, weapon, material
}

// InventoryState 背包状态。
type InventoryState struct {
	Items    []Item `json:"items"`
	Capacity int    `json:"capacity"`
}

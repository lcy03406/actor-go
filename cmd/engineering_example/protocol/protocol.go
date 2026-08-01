// Package protocol 定义服务端推给客户端的报文。
//
// 实际项目里这一层是 WebSocket 上的 JSON 契约（由协议生成器从 Go struct 产出客户端代码）；
// 本示例省略传输层，报文由 client actor 接收并打印，等价于客户端的 UI 渲染。
//
// 与 actors/*/api 的分工：
//   - api        = 服务端内部的 actor 间契约；
//   - protocol   = 面向客户端的报文。
//
// 两者不要混用：内部结构变化不该牵动客户端契约。
package protocol

// Msg 是所有下行报文的联合类型。isMsg 是未导出方法，包外无法实现，
// 因此新增报文必须落在这个包里（联合类型封闭）。
type Msg interface{ isMsg() }

// LoginOK 登录首帧：全量视图。
//
// 首帧全量、之后增量，是下行的基本纪律：新上场 / 重连的会话必须由权威补一次全量基准，
// 否则字段停在零值且不报错（"活着但变傻"）。
type LoginOK struct {
	Player  string `json:"player"`
	Level   int    `json:"level"`
	HP      int    `json:"hp"`
	MaxHP   int    `json:"maxHp"`
	Atk     int    `json:"atk"`
	Gold    int    `json:"gold"`
	Exp     int    `json:"exp"`
	Room    int    `json:"room"`
	ExpRate int    `json:"expRate"`
}

// StateUpdate 是玩家自身状态的增量（受伤 / 奖励 / 升级）。
type StateUpdate struct {
	Level  int    `json:"level"`
	HP     int    `json:"hp"`
	MaxHP  int    `json:"maxHp"`
	Gold   int    `json:"gold"`
	Exp    int    `json:"exp"`
	Reason string `json:"reason"`
}

// RoomEvent 是房间广播（聊天 / 战报 / 进出房）。
type RoomEvent struct {
	Room int    `json:"room"`
	Kind string `json:"kind"`
	Text string `json:"text"`
}

func (LoginOK) isMsg()     {}
func (StateUpdate) isMsg() {}
func (RoomEvent) isMsg()   {}

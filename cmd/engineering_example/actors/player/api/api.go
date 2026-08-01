// Package api 是 player actor 的对外契约：客户端意图 + 落到玩家身上的跨 actor 事件。
//
// 方向约定（比"内外"更可靠，因为分布式下没有稳定的内外边界）：
//   - client 沿链 Call 进来的是"意图"：Login / JoinRoom / Say / Attack / Status；
//   - 别的 actor Post 过来的是"事件"：Damage / CombatResult / RoomEvent。
//
// 两者的区别不靠命名前缀，而靠注册语义：意图用 Query/Serve 处理并允许回复，
// 事件一律 Post（不需要回复，也不该阻塞投递方）。
package api

import (
	"github.com/lcy03406/actor-go/actor"

	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// ─── 客户端意图 ───

// Login 登录：绑定下行出口（连接），返回首帧全量。
type Login struct {
	Client ids.ClientId `json:"client"`
}

func (*Login) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "Login" }

// JoinRoom 加入房间。
type JoinRoom struct {
	Room ids.RoomId `json:"room"`
}

func (*JoinRoom) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "JoinRoom" }

// Say 在房间内发言。
type Say struct {
	Text string `json:"text"`
}

func (*Say) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "Say" }

// Attack 攻击另一名玩家。
type Attack struct {
	Target ids.PlayerId `json:"target"`
}

func (*Attack) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "Attack" }

// Status 查询玩家全量快照。
type Status struct{}

func (*Status) ReqType(_ ids.PlayerId, _ *PlayerView) string { return "Status" }

// ─── 落到玩家身上的事件（Post，不回复）───

// Damage 受击事件。
//
// 攻击方与被攻击方都是 player（同一个 Group）：同组 peer 之间只 Post。
// 若用 Call，A 的 handler 等 B 的回复，而 B 的 handler 又要通知 A，就构成 A→B→A 的
// 死锁环；Post 不阻塞、也不需要回复。
type Damage struct {
	From   ids.PlayerId `json:"from"`
	Room   ids.RoomId   `json:"room"`
	Amount int          `json:"amount"` // 攻击方攻击力；防御由受击方自己结算
}

func (*Damage) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "Damage" }

// CombatResult 被击方回传攻击方的结算回执：攻击方收到它之后才发奖。
//
// Level 是被击方等级——击杀奖励按等级换算，被击方报价比攻击方去查人便宜：
// 数据随消息带，不必为读一个字段而反向 Call。
type CombatResult struct {
	Target ids.PlayerId `json:"target"`
	Level  int          `json:"level"`
	Amount int          `json:"amount"`
	Slain  bool         `json:"slain"`
}

func (*CombatResult) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "CombatResult" }

// RoomEvent 房间广播（room Post 过来），player 原样转给连接。
type RoomEvent struct {
	Room ids.RoomId `json:"room"`
	Kind string     `json:"kind"`
	Text string     `json:"text"`
}

func (*RoomEvent) ReqType(_ ids.PlayerId, _ actor.OkReply) string { return "RoomEvent" }

// ─── 对外数据 ───

// Brief 玩家摘要：player 主动上报给 room 做缓存（room 不反向 Call 取值）。
type Brief struct {
	OpenID string `json:"openId"`
	Level  int    `json:"level"`
	HP     int    `json:"hp"`
}

// PlayerView 玩家全量快照（登录首帧 / 状态查询共用同一构造器）。
type PlayerView struct {
	Player  string `json:"player"`
	Level   int    `json:"level"`
	HP      int    `json:"hp"`
	MaxHP   int    `json:"maxHp"`
	Atk     int    `json:"atk"`
	Gold    int    `json:"gold"`
	Exp     int    `json:"exp"`
	Room    int    `json:"room"`
	ExpRate int    `json:"expRate"` // 世界配置的本地缓存值：证明 shared 读路径打通
}

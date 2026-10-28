// Package player 是玩家 Actor：玩家的权威数据（等级 / 血量 / 金币 / 经验）只有它写。
//
// 它同时是 grain（持久化）：重启后等级、金币、经验从磁盘恢复；
// 连接与房间归属是运行时事实，不进快照 —— 见 PlayerState 的两段字段。
package player

import (
	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/shared"

	playerapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/player/api"
	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/game"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// PlayerGrainState 是 grain 状态：业务数据与快照同构（ShotSelf），框架托管租约与存盘。
type PlayerGrainState = grain.State[ids.PlayerId, PlayerState, PlayerState, grain.ShotSelf[PlayerState]]

// PlayerState 玩家数据。字段分两段，区别就是"重启后还要不要它"：
//
//   - 持久化段：角色成长，进程重启必须原样恢复；
//   - 运行时段（json:"-"）：会话与缓存，重启后重新建立比持久化更正确 ——
//     连接对象已经失效，世界配置应当重新订阅，房间成员表也在房间那边重建。
type PlayerState struct {
	Level int `json:"level"`
	HP    int `json:"hp"`
	Gold  int `json:"gold"`
	Exp   int `json:"exp"`

	Client ids.ClientId  `json:"-"` // 下行出口
	Room   ids.RoomId    `json:"-"` // 当前房间
	Vars   shared.Reader `json:"-"` // 世界配置的本地缓存
}

// newPlayer 新角色的初始状态：1 级满血。
func newPlayer() PlayerState {
	return PlayerState{Level: 1, HP: game.MaxHPOf(1)}
}

// ─── 派生属性：只由等级决定，不单独存，避免两份真相 ───

func (s *PlayerState) maxHP() int { return game.MaxHPOf(s.Level) }
func (s *PlayerState) atk() int   { return game.AtkOf(s.Level) }
func (s *PlayerState) def() int   { return game.DefOf(s.Level) }

// ─── 状态变更 ───

// applyDamage 结算受击。攻击方只报攻击力，防御与生死由受击方裁决：
// 每个 actor 只改自己的数据，别人的结论靠消息回执。
//
// 已经倒地时不再结算（ok=false）：重复打击必须幂等。
func (s *PlayerState) applyDamage(atk int) (taken int, slain, ok bool) {
	if s.HP <= 0 {
		return 0, false, false
	}
	taken = game.Damage(atk, s.def())
	s.HP -= taken
	if s.HP <= 0 {
		s.HP = 0
		return taken, true, true
	}
	return taken, false, true
}

// applyReward 记账并按需连升，返回是否升过级。
func (s *PlayerState) applyReward(gold, exp int) (leveled bool) {
	s.Gold += gold
	s.Exp += exp
	for s.Exp >= game.ExpToLevel(s.Level) {
		s.Exp -= game.ExpToLevel(s.Level)
		s.Level++
		s.HP = s.maxHP() // 升级回满，让"升级"在血量上也看得出来
		leveled = true
	}
	return leveled
}

// ─── 对外视图 ───

// brief 摘要：上报给房间做缓存的最小字段集。
func (s *PlayerState) brief(id ids.PlayerId) playerapi.Brief {
	return playerapi.Brief{OpenID: id.OpenID, Level: s.Level, HP: s.HP}
}

// view 全量快照：登录首帧与状态查询共用同一个构造器，避免两处字段漂移。
func (s *PlayerState) view(id ids.PlayerId, expRate int) *playerapi.PlayerView {
	return &playerapi.PlayerView{
		Player:  id.OpenID,
		Level:   s.Level,
		HP:      s.HP,
		MaxHP:   s.maxHP(),
		Atk:     s.atk(),
		Gold:    s.Gold,
		Exp:     s.Exp,
		Room:    s.Room.Seq,
		ExpRate: expRate,
	}
}

// expRate 读世界配置：本地缓存命中就直接用，首次读取会顺带向容器订阅。
// 读不到就退回默认值 —— 世界还没初始化不该让玩家算错账。
func expRate(ctx *actor.ActorContext[ids.PlayerId, PlayerGrainState]) int {
	cfg, err := shared.Watch(ctx, &ctx.State().Data.Vars, worldapi.ConfigKey)
	if err != nil {
		ctx.Logger().Debug("player: world config not ready", "id", ctx.Id(), "err", err)
		return worldapi.DefaultConfig.ExpRate
	}
	return cfg.ExpRate
}

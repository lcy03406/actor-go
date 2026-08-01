// Package api 是 world actor 的对外契约。
//
// 契约包约定（全部 actors/*/api 通用）：
//   - 只依赖 ids / protocol / 其它 actor 的 api，绝不 import 实现包 —— 这是防循环依赖的唯一规则；
//   - "发给本 actor 的请求"与"本 actor 发给别人的通知"都放本包；
//   - 实现包（world/）import 契约包，注册 handler 时把契约类型落到具体函数上。
package api

import (
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/shared"

	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// Init 初始化世界。由装配层在启动时同步 Call 一次：
// 世界配置必须先进入 shared 容器，玩家登录时才能 Watch 到。
type Init struct{}

func (*Init) ReqType(_ ids.WorldId, _ actor.OkReply) string { return "Init" }

// SetConfig 热更世界配置（运营 / GM 入口）。
type SetConfig struct {
	ExpRate int `json:"expRate"` // 经验倍率（百分比）
}

func (*SetConfig) ReqType(_ ids.WorldId, _ actor.OkReply) string { return "SetConfig" }

// Config 世界配置：对全服只读。玩家经 shared 容器读取，不 Call world。
//
// 为什么用 shared 而不用 Call：这是"读多写少"的全局事实，每个玩家都要读；
// 容器负责订阅、冷却合并与本地缓存，读者不必每次触网，world 也不会被扇入打爆。
type Config struct {
	ExpRate int `json:"expRate"`
}

// DefaultConfig 是新建世界的初始配置。
var DefaultConfig = Config{ExpRate: 100}

// configKeyDef 是 key 定义（pattern 全局唯一，重复定义会 panic）。
// 世界是单例，pattern 不含占位符；多实例场景写成 "example/world/%s/config" 再
// 用 Of(世界 ID) 展开即可。
//
// TTL 让读者缓存自愈：推送是尽力投递，超过 TTL 未收到推送就重新向容器拉取。
var configKeyDef = shared.Define[Config]("example/world/config", shared.WithTTL(time.Minute))

// ConfigKey 是容器里的 key 实例：owner = world，reader = player。
var ConfigKey = configKeyDef.Of()

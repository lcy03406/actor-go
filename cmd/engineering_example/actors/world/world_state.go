// Package world 是世界 Actor：全服单例，持有世界配置并把它发布到 shared 容器。
//
// 它是容器的 owner（唯一发布者），所有玩家都是 reader。
// 世界不需要持久化 —— 配置应在启动时由装配层重新 Init 一次。
package world

import (
	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
)

// State 是世界的权威状态：这份配置只有 world 写，其他人只能经容器读到副本。
type State struct {
	Config worldapi.Config
}

// Package setup 是装配层：把 Manager、共享容器、各 actor 的注册和世界初始化拼在一起。
//
// 只有这里同时 import 全部 actor 实现包；其余任何地方都只依赖 ids + 契约包。
// 于是"新增一个 actor"的改动面就固定为两处：ids 加一个 ID 类型、本文件加一行注册。
package setup

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/shared"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actors/client"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actors/player"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actors/room"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actors/world"
	worldapi "github.com/lcy03406/actor-go/cmd/engineering_example/actors/world/api"
	"github.com/lcy03406/actor-go/cmd/engineering_example/ids"
)

// Config 装配参数。
type Config struct {
	DataDir string        // grain 落盘目录，空则 ./data
	NodeID  string        // 租约持有者标识，空则 example-node
	Persist time.Duration // 定时存盘间隔，0 表示只在退出时存盘
	Logger  *slog.Logger  // nil 则丢弃全部日志
}

// Server 是一次装配的产物。调用方只需要 Manager 和 Close。
type Server struct {
	Mgr *actor.Manager
}

// New 装配全部 actor，并把世界初始化好。
//
// 世界初始化必须同步完成：配置先进容器，玩家登录时才读得到 ——
// "先发布、后使用"的启动顺序是 shared 唯一需要关心的时序。
func New(cfg Config) (*Server, error) {
	if cfg.DataDir == "" {
		cfg.DataDir = "./data"
	}
	if cfg.NodeID == "" {
		cfg.NodeID = "example-node"
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}

	mgr := actor.NewManager(logger)
	srv := &Server{Mgr: mgr}

	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver(cfg.DataDir)),
		grain.WithNodeId(cfg.NodeID),
		grain.WithAutoPersistInterval(cfg.Persist),
	)

	// 1. 容器先于读者注册：读者首次 Watch 就要能路由到容器分片。
	shared.Serve(mgr, shared.Options{Shards: 1})

	// 2. 各 actor 注册。彼此没有顺序要求 —— 每个包只暴露一个 RegisterHandlers。
	world.RegisterHandlers(mgr)
	room.RegisterHandlers(mgr)
	player.RegisterHandlers(mgr, pm)
	client.RegisterHandlers(mgr)

	// 3. 初始化世界：写配置 → 发布进容器。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := actor.Call(ctx, mgr, ids.TheWorld(), &worldapi.Init{}); err != nil {
		srv.Close()
		return nil, fmt.Errorf("setup: 初始化世界失败: %w", err)
	}
	return srv, nil
}

// Close 优雅停机。
//
// CloseManager 触发每个 actor 的退出钩子（grain 在这里落盘并释放租约），
// JoinManager 等它们真的退出。少了这两步，进程直接退出会丢掉最后一次写入。
func (s *Server) Close() {
	s.Mgr.CloseManager()
	s.Mgr.JoinManager()
}

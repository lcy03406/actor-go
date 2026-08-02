// setup 包负责组装所有 Actor Group、RPC 服务器和集群配置。
//
// 【合并注册】
//
//	player.Register(mgr, rpcBld, pm, placement, selfID) 一次调用完成 handler + RPC + CheckOwnership。
//	setup 不再需要分别调用多次。
//
// 【依赖方向】
//
//	player/ ──→ types/ + attr/ + inventory/ + skill/ + actor + rpc + cluster
//	room/   ──→ actor 包
//	chat/   ──→ actor 包
//	  ↑
//	  │  setup 只依赖一层子包
//	  │
//	main.go ──→ setup + player + room + chat
package setup

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/grain"
	"github.com/lcy03406/actor-go/rpc"

	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/chat"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/player"
	playertypes "github.com/lcy03406/actor-go/cmd/engineering_example/actor/player/types"
	"github.com/lcy03406/actor-go/cmd/engineering_example/actor/room"
)

// ─── 类型别名 ───

type (
	JsonMsg = json.RawMessage
	JsonC   = rpc.JsonCodec
	JsonT   = rpc.JsonTransport
	JsonSrv = rpc.Server[JsonMsg, JsonC, JsonT]
	Router  = cluster.Router[JsonMsg, JsonC, JsonT]
	RegBld  = rpc.RegistryBuilder[JsonMsg, JsonC]
)

// ─── GroupMapping ───

var GroupMapping = cluster.GroupMapping{
	"player-server": {"Player"},
	"room-server":   {"Room"},
	"chat-server":   {"Chat"},
	"all-in-one":    {"Player", "Room", "Chat"},
}

// ─── NodeConfig ───

type NodeConfig struct {
	NodeType string
	NodeID   string
	Addr     string
	Seeds    string
}

// ─── StartNode ───

func StartNode(ctx context.Context, cfg NodeConfig) (*Router, *DynamicMembership, error) {
	mgr := actor.NewManager()

	pm := grain.NewPersistenceManager(
		grain.WithDriver(grain.NewJsonDriver("./data")),
		grain.WithNodeId(cfg.NodeID),
	)

	hasPlayer := cfg.NodeType == "player-server" || cfg.NodeType == "all-in-one"
	hasRoom := cfg.NodeType == "room-server" || cfg.NodeType == "all-in-one"
	hasChat := cfg.NodeType == "chat-server" || cfg.NodeType == "all-in-one"

	// 构建集群拓扑
	self := cluster.Node{ID: cfg.NodeID, Addr: cfg.Addr, Type: cfg.NodeType}
	members := []cluster.Node{self}
	if cfg.Seeds != "" {
		for _, seed := range strings.Split(cfg.Seeds, ",") {
			seed = strings.TrimSpace(seed)
			if seed == "" || seed == cfg.Addr {
				continue
			}
			seedType := inferTypeFromAddr(seed)
			members = append(members, cluster.Node{
				ID:   fmt.Sprintf("%s-%s", seedType, seed),
				Addr: seed,
				Type: seedType,
			})
		}
	}

	mem := newDynamicMembership(self, members...)
	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(GroupMapping)
	router := cluster.NewRouter[JsonMsg, JsonC, JsonT](mem, placement, mgr)

	// RPC Server + Actor Group 一次性注册（含集群 CheckOwnership）
	server := rpc.NewServer[JsonMsg, JsonC, JsonT](cfg.Addr, mgr, func(b *RegBld) {
		if hasPlayer {
			player.Register(mgr, b, pm, placement, self.ID)
		}
		if hasRoom {
			room.Register(mgr, b, placement, self.ID)
		}
		if hasChat {
			chat.Register(mgr, b, placement, self.ID)
		}
	})
	if err := server.Start(); err != nil {
		return nil, nil, fmt.Errorf("启动 RPC Server 失败: %w", err)
	}
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	// 迁移协调器
	coord := cluster.NewMigrationCoordinator(mgr, placement, mem)
	if hasPlayer {
		coord.RegisterNotify(func() {
			actor.Broadcast[playertypes.PlayerId](mgr, &player.CheckOwnership{})
		})
	}
	if hasRoom {
		coord.RegisterNotify(func() {
			actor.Broadcast[room.RoomId](mgr, &room.CheckOwnership{})
		})
	}
	if hasChat {
		coord.RegisterNotify(func() {
			actor.Broadcast[chat.ChatId](mgr, &chat.CheckOwnership{})
		})
	}
	go coord.Run(ctx, mem.Events())

	log.Printf("节点已启动: %s (%s) 监听 %s, 成员 %d",
		cfg.NodeID, cfg.NodeType, cfg.Addr, len(members))
	return router, mem, nil
}

func inferTypeFromAddr(addr string) string {
	switch {
	case strings.Contains(addr, "8002"):
		return "room-server"
	case strings.Contains(addr, "8003"):
		return "chat-server"
	case strings.Contains(addr, "8004"):
		return "all-in-one"
	default:
		return "player-server"
	}
}

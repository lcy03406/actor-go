// 异构集群示例：按节点类型启动不同类型的服务节点，不同节点承载不同的 Actor Group。
//
// 节点类型：
//   - player-server：承载 Player Actor（玩家登录、攻击、治疗）
//   - room-server：  承载 Room Actor（创建房间、查询房间）
//   - chat-server：  承载 Chat Actor（收发聊天消息）
//   - all-in-one：   承载全部 Actor（等同于同构节点，用于向后兼容）
//
// 启动方式：
//
//	# 启动 player-server 节点
//	go run main.go -type player-server -addr localhost:8001
//
//	# 启动 room-server 节点，种子节点为 player-server
//	go run main.go -type room-server -addr localhost:8002 -seeds localhost:8001
//
//	# 启动 chat-server 节点
//	go run main.go -type chat-server -addr localhost:8003 -seeds localhost:8001
//
//	# 启动 all-in-one 节点（同构模式，向后兼容）
//	go run main.go -type all-in-one -addr localhost:8004 -seeds localhost:8001
//
// 交互命令：
//
//	login <id> <hp>     - 创建一个 Player Actor（仅 player-server / all-in-one）
//	attack <id> <dmg>   - 攻击指定 Player
//	heal <id> <amount>  - 治疗指定 Player
//	room <id> <max>     - 创建房间（仅 room-server / all-in-one）
//	roominfo <id>       - 查询房间信息
//	chat <ch> <msg>     - 发送聊天消息（仅 chat-server / all-in-one）
//	info                - 显示集群拓扑信息
//	status              - 显示本地节点信息
//	migrate join <type> <addr>  - 模拟节点加入，触发 Actor 迁移
//	migrate leave <id>          - 模拟节点离开，触发 Actor 迁移
//	quit                - 退出
package main

import (
	"bufio"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/rpc"
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

// ─── Actor ID 类型 ───

type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string              { return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId) }

type RoomId struct {
	RoomId int `json:"roomId"`
}

func (id RoomId) ActorType() actor.ActorType { return "Room" }
func (id RoomId) String() string             { return fmt.Sprintf("Room(%d)", id.RoomId) }

type ChatId struct {
	Channel string `json:"channel"`
}

func (id ChatId) ActorType() actor.ActorType { return "Chat" }
func (id ChatId) String() string             { return fmt.Sprintf("Chat(%s)", id.Channel) }

// ─── 请求类型 ───

// --- Player ---
type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
	Damage int `json:"damage"`
}
type AttackReply struct {
	RemainingHP int `json:"remainingHP"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type Heal struct {
	Amount int `json:"amount"`
}
type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ PlayerId, _ *HealReply) string { return "Heal" }

type ClosePlayer struct{}

func (*ClosePlayer) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }

// CheckOwnership 是节点迁移的核心消息。
// 当集群拓扑变化时，MigrationCoordinator 会广播此消息，
// 每个 Actor 在 handler 中检查自己是否还应留在当前节点。
type CheckOwnership struct{}

func (*CheckOwnership) ReqType(_ PlayerId, _ actor.OkReply) string { return "CheckOwnership" }

// RoomCheckOwnership Room Actor 的迁移检查消息
type RoomCheckOwnership struct{}

func (*RoomCheckOwnership) ReqType(_ RoomId, _ actor.OkReply) string { return "CheckOwnership" }

// ChatCheckOwnership Chat Actor 的迁移检查消息
type ChatCheckOwnership struct{}

func (*ChatCheckOwnership) ReqType(_ ChatId, _ actor.OkReply) string { return "CheckOwnership" }

// PlayerStateReply 返回 Player 的当前状态
type PlayerStateReq struct{}

type PlayerStateReply struct {
	HP    int  `json:"hp"`
	Level int  `json:"level"`
	Owned bool `json:"owned"` // 是否由当前节点拥有
}

func (*PlayerStateReq) ReqType(_ PlayerId, _ *PlayerStateReply) string { return "PlayerState" }

// --- Room ---
type CreateRoom struct {
	MaxPlayers int `json:"maxPlayers"`
}

func (*CreateRoom) ReqType(_ RoomId, _ actor.OkReply) string { return "CreateRoom" }

type RoomInfo struct{}
type RoomInfoReply struct {
	MaxPlayers int `json:"maxPlayers"`
}

func (*RoomInfo) ReqType(_ RoomId, _ *RoomInfoReply) string { return "RoomInfo" }

// --- Chat ---
type SendMessage struct {
	Text string `json:"text"`
}
type SendMessageReply struct {
	Echo string `json:"echo"`
}

func (*SendMessage) ReqType(_ ChatId, _ *SendMessageReply) string { return "SendMessage" }

// ─── State 类型 ───

type PlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}
type RoomState struct {
	MaxPlayers int `json:"maxPlayers"`
}
type ChatState struct {
	Messages []string `json:"messages"`
}

// ─── GroupMapping ───

var groupMapping = cluster.GroupMapping{
	"player-server": {"Player"},
	"room-server":   {"Room"},
	"chat-server":   {"Chat"},
	"all-in-one":    {"Player", "Room", "Chat"},
}

// nodeId 全局节点 ID（由命令行参数构建）
var nodeId string

// globalMembership 全局动态成员管理器，用于模拟节点加入/离开
var globalMembership *dynamicMembership

// ─── main ───

func main() {
	nodeType := flag.String("type", "all-in-one", "节点类型: player-server, room-server, chat-server, all-in-one")
	addr := flag.String("addr", "localhost:8001", "监听地址")
	seeds := flag.String("seeds", "", "种子节点地址，逗号分隔")
	flag.Parse()

	if _, ok := groupMapping[*nodeType]; !ok {
		log.Fatalf("未知节点类型: %s，支持: player-server, room-server, chat-server, all-in-one", *nodeType)
	}

	nodeId = fmt.Sprintf("%s-%s", *nodeType, *addr)
	log.SetFlags(log.Ltime | log.Lmicroseconds)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 启动节点
	router := startNode(ctx, nodeId, *nodeType, *addr, *seeds)

	// 等待就绪
	time.Sleep(500 * time.Millisecond)

	printBanner(*nodeType, *addr, router)

	// 交互式命令行
	go repl(ctx, router)

	// 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	<-sigCh

	log.Println("正在关闭...")
	router.Close()
	log.Println("已退出")
}

// ─── 节点启动 ───

func startNode(ctx context.Context, id, nodeType, addr string, seeds string) *Router {
	mgr := actor.NewManager()

	// 根据节点类型注册 Actor Group
	switch nodeType {
	case "player-server", "all-in-one":
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
				ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
				log.Printf("[Player] %s 登录 HP=%d Level=%d", ctx.Id().String(), req.InitHP, req.InitLevel)
				return actor.OK, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
				ctx.State().HP -= req.Damage
				log.Printf("[Player] %s 受到 %d 伤害, HP=%d", ctx.Id().String(), req.Damage, ctx.State().HP)
				return &AttackReply{RemainingHP: ctx.State().HP}, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, _ bool) (*HealReply, error) {
				ctx.State().HP += req.Amount
				log.Printf("[Player] %s 治疗 %d, HP=%d", ctx.Id().String(), req.Amount, ctx.State().HP)
				return &HealReply{NewHP: ctx.State().HP}, nil
			})
			actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *ClosePlayer, _ bool) (actor.OkReply, error) {
				log.Printf("[Player] %s 退出", ctx.Id().String())
				ctx.Quit()
				return actor.OK, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *PlayerStateReq, _ bool) (*PlayerStateReply, error) {
				return &PlayerStateReply{
					HP:    ctx.State().HP,
					Level: ctx.State().Level,
				}, nil
			})
		})
	}
	if nodeType == "room-server" || nodeType == "all-in-one" {
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[RoomId, RoomState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[RoomId, RoomState], req *CreateRoom, _ bool) (actor.OkReply, error) {
				ctx.SetState(RoomState{MaxPlayers: req.MaxPlayers})
				log.Printf("[Room] %s 创建 最大人数=%d", ctx.Id().String(), req.MaxPlayers)
				return actor.OK, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[RoomId, RoomState], req *RoomInfo, _ bool) (*RoomInfoReply, error) {
				return &RoomInfoReply{MaxPlayers: ctx.State().MaxPlayers}, nil
			})
		})
	}
	if nodeType == "chat-server" || nodeType == "all-in-one" {
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[ChatId, ChatState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[ChatId, ChatState], req *SendMessage, _ bool) (*SendMessageReply, error) {
				ctx.State().Messages = append(ctx.State().Messages, req.Text)
				log.Printf("[Chat] %s 消息: %s", ctx.Id().String(), req.Text)
				return &SendMessageReply{Echo: req.Text}, nil
			})
		})
	}

	// RPC Server：注册对应类型的请求
	server := rpc.NewServer[JsonMsg, JsonC, JsonT](addr, mgr, func(b *RegBld) {
		if nodeType == "player-server" || nodeType == "all-in-one" {
			rpc.RegisterRequest(b, &Login{})
			rpc.RegisterRequest(b, &Attack{})
			rpc.RegisterRequest(b, &Heal{})
			rpc.RegisterRequest(b, &ClosePlayer{})
			rpc.RegisterRequest(b, &PlayerStateReq{})
		}
		if nodeType == "room-server" || nodeType == "all-in-one" {
			rpc.RegisterRequest(b, &CreateRoom{})
			rpc.RegisterRequest(b, &RoomInfo{})
		}
		if nodeType == "chat-server" || nodeType == "all-in-one" {
			rpc.RegisterRequest(b, &SendMessage{})
		}
	})
	if err := server.Start(); err != nil {
		log.Fatalf("启动 RPC Server 失败: %v", err)
	}
	go func() {
		<-ctx.Done()
		server.Shutdown(context.Background())
	}()

	// 构建集群拓扑
	self := cluster.Node{ID: id, Addr: addr, Type: nodeType}
	members := []cluster.Node{self}
	if seeds != "" {
		for _, seed := range strings.Split(seeds, ",") {
			seed = strings.TrimSpace(seed)
			if seed == "" || seed == addr {
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
	globalMembership = mem // 保存全局引用，供 migrate 命令使用
	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(groupMapping)
	router := cluster.NewRouter[JsonMsg, JsonC, JsonT](mem, placement, mgr)

	// ─── 节点迁移支持 ───
	// 初始化 MigrationCoordinator，监听集群拓扑变化
	coord := cluster.NewMigrationCoordinator(mgr, placement, mem)

	// 注册各 Actor Group 的 CheckOwnership handler 和通知回调
	if nodeType == "player-server" || nodeType == "all-in-one" {
		// 延迟注册 CheckOwnership handler（需要在 Serve 之后）
		registerPlayerMigrationHandlers(mgr, placement, mem)
		coord.RegisterNotify(func() {
			actor.Broadcast[PlayerId](mgr, &CheckOwnership{})
		})
	}
	if nodeType == "room-server" || nodeType == "all-in-one" {
		registerRoomMigrationHandlers(mgr, placement, mem)
		coord.RegisterNotify(func() {
			actor.Broadcast[RoomId](mgr, &RoomCheckOwnership{})
		})
	}
	if nodeType == "chat-server" || nodeType == "all-in-one" {
		registerChatMigrationHandlers(mgr, placement, mem)
		coord.RegisterNotify(func() {
			actor.Broadcast[ChatId](mgr, &ChatCheckOwnership{})
		})
	}

	// 启动迁移协调器
	go coord.Run(ctx, mem.Events())

	log.Printf("节点已启动: %s (%s) 监听 %s, 成员 %d, 迁移协调器已启用", id, nodeType, addr, len(members))
	return router
}

func inferTypeFromAddr(addr string) string {
	// 简单推断：从种子地址只能推测，实际生产环境通过 Membership 协议获取
	// 此处做合理假设：若地址包含 8002 则为 room-server，8003 为 chat-server
	if strings.Contains(addr, "8002") {
		return "room-server"
	}
	if strings.Contains(addr, "8003") {
		return "chat-server"
	}
	if strings.Contains(addr, "8004") {
		return "all-in-one"
	}
	return "player-server"
}

// ─── DynamicMembership（支持动态节点加入/离开） ───

type dynamicMembership struct {
	mu      sync.Mutex
	self    cluster.Node
	members cluster.NodeSet
	events  chan cluster.MemberEvent
}

func newDynamicMembership(self cluster.Node, members ...cluster.Node) *dynamicMembership {
	return &dynamicMembership{
		self:    self,
		members: cluster.NodeSet(members),
		events:  make(chan cluster.MemberEvent, 100),
	}
}

func (d *dynamicMembership) Self() cluster.Node {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.self
}

func (d *dynamicMembership) Members() cluster.NodeSet {
	d.mu.Lock()
	defer d.mu.Unlock()
	result := make(cluster.NodeSet, len(d.members))
	copy(result, d.members)
	return result
}

func (d *dynamicMembership) Events() <-chan cluster.MemberEvent {
	return d.events
}

func (d *dynamicMembership) Join(seeds []string) error { return nil }
func (d *dynamicMembership) Leave() error              { return nil }
func (d *dynamicMembership) Close() error              { return nil }

// AddNode 模拟节点加入集群，触发 MemberJoined 事件。
func (d *dynamicMembership) AddNode(n cluster.Node) {
	d.mu.Lock()
	// 检查是否已存在
	for _, m := range d.members {
		if m.ID == n.ID {
			d.mu.Unlock()
			return
		}
	}
	d.members = append(d.members, n)
	membersCopy := make(cluster.NodeSet, len(d.members))
	copy(membersCopy, d.members)
	d.mu.Unlock()

	d.events <- cluster.MemberEvent{
		Type:  cluster.MemberJoined,
		Node:  n,
		Nodes: membersCopy,
	}
}

// RemoveNode 模拟节点离开集群，触发 MemberLeft 事件。
func (d *dynamicMembership) RemoveNode(nodeID string) {
	d.mu.Lock()
	var removed cluster.Node
	newMembers := make(cluster.NodeSet, 0, len(d.members))
	for _, m := range d.members {
		if m.ID == nodeID {
			removed = m
		} else {
			newMembers = append(newMembers, m)
		}
	}
	if removed.ID == "" {
		d.mu.Unlock()
		return
	}
	d.members = newMembers
	d.mu.Unlock()

	d.events <- cluster.MemberEvent{
		Type:  cluster.MemberLeft,
		Node:  removed,
		Nodes: newMembers,
	}
}

// ─── 迁移 Handler 注册 ───

func registerPlayerMigrationHandlers(mgr *actor.Manager, placement cluster.PlacementStrategy, mem *dynamicMembership) {
	// 注册 CheckOwnership handler 到已存在的 Player group
	// 注意：这里使用 Post + 空 ActorId 的方式无法直接注册到已有 group，
	// 所以通过 serve 的方式，manager 内部会找到对应 group 并注册 handler。
	// 实际上 Serve 已经在 startNode 中调用过了，这里需要额外的 handler 注册方式。
	// 我们利用一个临时的 Actor 来触发 handler 查找：
	// 实际上 RegisterServe 是通过 RegistryBuilder 调用的，这里改用直接方式。
	// 因为 Serve 已经调用过了，我们需要在已有 group 上添加 handler。
	// 最简单的方式是用一个新的 Serve 调用（会追加 handler 到已有 group）。
	actor.Serve(mgr, 0, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *CheckOwnership, _ bool) (actor.OkReply, error) {
			selfID := mem.Self().ID
			members := mem.Members()
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Player", ctx.Id().String())
			if leave {
				log.Printf("[迁移] Player %s 应迁移到 %s, 当前 HP=%d Level=%d",
					ctx.Id().String(), target, ctx.State().HP, ctx.State().Level)
				// 实际场景中应调用 Deactivate 释放租约
				// ctx.State().Deactivate()
			} else {
				log.Printf("[迁移] Player %s 仍属于当前节点 %s", ctx.Id().String(), selfID)
			}
			return actor.OK, nil
		})
	})
}

func registerRoomMigrationHandlers(mgr *actor.Manager, placement cluster.PlacementStrategy, mem *dynamicMembership) {
	actor.Serve(mgr, 0, func(b *actor.RegistryBuilder[RoomId, RoomState]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[RoomId, RoomState], req *RoomCheckOwnership, _ bool) (actor.OkReply, error) {
			selfID := mem.Self().ID
			members := mem.Members()
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Room", ctx.Id().String())
			if leave {
				log.Printf("[迁移] Room %s 应迁移到 %s, 当前 MaxPlayers=%d",
					ctx.Id().String(), target, ctx.State().MaxPlayers)
			} else {
				log.Printf("[迁移] Room %s 仍属于当前节点 %s", ctx.Id().String(), selfID)
			}
			return actor.OK, nil
		})
	})
}

func registerChatMigrationHandlers(mgr *actor.Manager, placement cluster.PlacementStrategy, mem *dynamicMembership) {
	actor.Serve(mgr, 0, func(b *actor.RegistryBuilder[ChatId, ChatState]) {
		actor.RegisterServe(b, func(ctx *actor.ActorContext[ChatId, ChatState], req *ChatCheckOwnership, _ bool) (actor.OkReply, error) {
			selfID := mem.Self().ID
			members := mem.Members()
			target, leave := cluster.CheckOwnership(placement, members, selfID, "Chat", ctx.Id().String())
			if leave {
				log.Printf("[迁移] Chat %s 应迁移到 %s, 消息数=%d",
					ctx.Id().String(), target, len(ctx.State().Messages))
			} else {
				log.Printf("[迁移] Chat %s 仍属于当前节点 %s", ctx.Id().String(), selfID)
			}
			return actor.OK, nil
		})
	})
}

// ─── Banner ───

func printBanner(nodeType, addr string, router *Router) {
	groups := groupMapping[nodeType]
	groupNames := "所有"
	if len(groups) > 0 && nodeType != "all-in-one" {
		groupNames = strings.Join(groups, ", ")
	}

	fmt.Println()
	fmt.Println("╔══════════════════════════════════════════════════════╗")
	fmt.Println("║          Actor-Go 异构集群示例                         ║")
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Printf("║  节点 ID:   %-42s ║\n", router.Self().ID)
	fmt.Printf("║  节点类型:  %-42s ║\n", nodeType)
	fmt.Printf("║  监听地址:  %-42s ║\n", addr)
	fmt.Printf("║  承载 Group: %-41s ║\n", groupNames)
	fmt.Printf("║  集群成员:  %-41d ║\n", len(router.Members()))
	fmt.Println("╠══════════════════════════════════════════════════════╣")
	fmt.Println("║  命令:                                                ║")
	if nodeType == "player-server" || nodeType == "all-in-one" {
		fmt.Println("║    login <id> <hp>    - 创建玩家 (本地)               ║")
	}
	fmt.Println("║    attack <id> <dmg>  - 攻击玩家                       ║")
	fmt.Println("║    heal <id> <amt>    - 治疗玩家                       ║")
	if nodeType == "room-server" || nodeType == "all-in-one" {
		fmt.Println("║    room <id> <max>    - 创建房间 (本地)               ║")
	}
	fmt.Println("║    roominfo <id>      - 查询房间                       ║")
	if nodeType == "chat-server" || nodeType == "all-in-one" {
		fmt.Println("║    chat <ch> <msg>    - 发送消息 (本地)               ║")
	}
	fmt.Println("║    info               - 集群拓扑                       ║")
	fmt.Println("║    status             - 节点信息                       ║")
	fmt.Println("║    migrate join <t> <a> - 模拟节点加入，触发迁移       ║")
	fmt.Println("║    migrate leave <id>   - 模拟节点离开，触发迁移       ║")
	fmt.Println("║    quit               - 退出                           ║")
	fmt.Println("╚══════════════════════════════════════════════════════╝")
	fmt.Println()
}

// ─── REPL 交互式命令行 ───

func repl(ctx context.Context, router *Router) {
	scanner := bufio.NewScanner(os.Stdin)
	fmt.Print("> ")

	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			fmt.Print("> ")
			continue
		}

		parts := strings.Fields(line)
		if len(parts) == 0 {
			fmt.Print("> ")
			continue
		}

		cmd := strings.ToLower(parts[0])
		args := parts[1:]

		switch cmd {
		case "login":
			if len(args) < 2 {
				fmt.Println("用法: login <id> <hp>")
				break
			}
			hp, _ := strconv.Atoi(args[1])
			id := PlayerId{ServerId: 1, OpenId: args[0]}
			err := cluster.Post[JsonMsg, JsonC, JsonT](router, id, &Login{InitHP: hp, InitLevel: 1})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ Player %s 已创建 (HP=%d)\n", args[0], hp)
			}

		case "attack":
			if len(args) < 2 {
				fmt.Println("用法: attack <id> <damage>")
				break
			}
			dmg, _ := strconv.Atoi(args[1])
			id := PlayerId{ServerId: 1, OpenId: args[0]}
			reply, err := cluster.Call(ctx, router, id, &Attack{Damage: dmg})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ Player %s 受到 %d 伤害, 剩余 HP=%d\n", args[0], dmg, reply.RemainingHP)
			}

		case "heal":
			if len(args) < 2 {
				fmt.Println("用法: heal <id> <amount>")
				break
			}
			amt, _ := strconv.Atoi(args[1])
			id := PlayerId{ServerId: 1, OpenId: args[0]}
			reply, err := cluster.Call(ctx, router, id, &Heal{Amount: amt})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ Player %s 治疗 %d, 新 HP=%d\n", args[0], amt, reply.NewHP)
			}

		case "room":
			if len(args) < 2 {
				fmt.Println("用法: room <id> <max_players>")
				break
			}
			rid, _ := strconv.Atoi(args[0])
			max, _ := strconv.Atoi(args[1])
			id := RoomId{RoomId: rid}
			err := cluster.Post[JsonMsg, JsonC, JsonT](router, id, &CreateRoom{MaxPlayers: max})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ Room %d 已创建 (最大 %d 人)\n", rid, max)
			}

		case "roominfo":
			if len(args) < 1 {
				fmt.Println("用法: roominfo <id>")
				break
			}
			rid, _ := strconv.Atoi(args[0])
			id := RoomId{RoomId: rid}
			reply, err := cluster.Call(ctx, router, id, &RoomInfo{})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ Room %d 最大人数=%d\n", rid, reply.MaxPlayers)
			}

		case "chat":
			if len(args) < 2 {
				fmt.Println("用法: chat <channel> <message>")
				break
			}
			id := ChatId{Channel: args[0]}
			msg := strings.Join(args[1:], " ")
			reply, err := cluster.Call(ctx, router, id, &SendMessage{Text: msg})
			if err != nil {
				fmt.Printf("错误: %v\n", err)
			} else {
				fmt.Printf("✓ [%s] 发送: %s → echo: %s\n", args[0], msg, reply.Echo)
			}

		case "info":
			printClusterInfo(router)

		case "status":
			printStatus(router)

		case "migrate":
			if len(args) < 1 {
				fmt.Println("用法: migrate join <nodeType> <addr>  或  migrate leave <nodeId>")
				break
			}
			handleMigrate(args, router)

		case "quit", "exit":
			fmt.Println("再见!")
			os.Exit(0)

		default:
			fmt.Printf("未知命令: %s\n", cmd)
			fmt.Println("可用命令: login, attack, heal, room, roominfo, chat, info, status, migrate, quit")
		}

		fmt.Print("> ")
	}
}

func printClusterInfo(router *Router) {
	members := router.Members()
	self := router.Self()

	fmt.Println("═══════════════════════════════════════════════════════")
	fmt.Printf("  集群成员 (%d 个节点):\n", len(members))
	for _, n := range members {
		marker := " "
		if n.ID == self.ID {
			marker = "*"
		}
		groups := groupMapping.GroupsOf(n.Type)
		groupStr := "全部"
		if len(groups) > 0 && n.Type != "all-in-one" {
			groupStr = strings.Join(groups, ", ")
		}
		fmt.Printf("  %s %s\n", marker, n.ID)
		fmt.Printf("     地址: %s  类型: %s  Group: %s\n", n.Addr, n.Type, groupStr)
	}
	fmt.Println("═══════════════════════════════════════════════════════")
}

func printStatus(router *Router) {
	self := router.Self()
	groups := groupMapping.GroupsOf(self.Type)
	groupStr := "全部"
	if len(groups) > 0 && self.Type != "all-in-one" {
		groupStr = strings.Join(groups, ", ")
	}

	fmt.Println("───────────────────────────────────────────────────────")
	fmt.Printf("  节点 ID:   %s\n", self.ID)
	fmt.Printf("  节点类型:  %s\n", self.Type)
	fmt.Printf("  监听地址:  %s\n", self.Addr)
	fmt.Printf("  承载 Group: %s\n", groupStr)
	fmt.Printf("  集群成员:  %d\n", len(router.Members()))

	// 检查各 ActorType 的 Placement
	fmt.Println("  Placement 分布 (前 10 个 key):")
	for _, at := range []string{"Player", "Room", "Chat"} {
		if !groupMapping.HasGroup(self.Type, at) {
			fmt.Printf("    %s: 不承载\n", at)
			continue
		}
		local := 0
		for i := 0; i < 10; i++ {
			if router.IsLocal(at, fmt.Sprintf("%s-%d", strings.ToLower(at), i)) {
				local++
			}
		}
		fmt.Printf("    %s: 本地 %d/10\n", at, local)
	}
	fmt.Println("───────────────────────────────────────────────────────")
}

// ─── 迁移命令处理 ───

func handleMigrate(args []string, router *Router) {
	if globalMembership == nil {
		fmt.Println("错误: 当前 Membership 不支持动态变更")
		return
	}

	subCmd := strings.ToLower(args[0])

	switch subCmd {
	case "join":
		if len(args) < 3 {
			fmt.Println("用法: migrate join <nodeType> <addr>")
			fmt.Println("示例: migrate join player-server localhost:8005")
			return
		}
		nodeType := args[1]
		addr := args[2]
		nodeID := fmt.Sprintf("%s-%s", nodeType, addr)

		n := cluster.Node{
			ID:   nodeID,
			Addr: addr,
			Type: nodeType,
		}

		fmt.Printf(">>> 模拟节点加入: %s (类型=%s, 地址=%s)\n", nodeID, nodeType, addr)
		globalMembership.AddNode(n)

		// 等待迁移协调器处理
		time.Sleep(300 * time.Millisecond)

		fmt.Println(">>> 节点加入完成，查看 info 确认集群拓扑变化")
		fmt.Println(">>> 注意观察日志中的 [迁移] 信息，了解哪些 Actor 应迁移到新节点")

	case "leave":
		if len(args) < 2 {
			fmt.Println("用法: migrate leave <nodeId>")
			fmt.Println("示例: migrate leave player-server-localhost:8005")
			return
		}
		nodeID := args[1]

		// 不允许移除自己
		if nodeID == router.Self().ID {
			fmt.Println("错误: 不能移除当前节点自身，请在其他节点上执行此操作")
			return
		}

		fmt.Printf(">>> 模拟节点离开: %s\n", nodeID)
		globalMembership.RemoveNode(nodeID)

		// 等待迁移协调器处理
		time.Sleep(300 * time.Millisecond)

		fmt.Println(">>> 节点离开完成，查看 info 确认集群拓扑变化")
		fmt.Println(">>> 注意观察日志中的 [迁移] 信息，了解 Actor 重新分配情况")

	default:
		fmt.Printf("未知的 migrate 子命令: %s\n", subCmd)
		fmt.Println("可用子命令: join, leave")
	}
}

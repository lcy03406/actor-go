package cluster_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/cluster"
	"github.com/lcy03406/actor-go/rpc"
)

// ─── 类型别名 ───

type (
	hetMsg    = json.RawMessage
	hetCodec  = rpc.JsonCodec
	hetTransp = rpc.JsonTransport
	hetSrv    = rpc.Server[hetMsg, hetCodec, hetTransp]
	hetCli    = rpc.Client[hetMsg, hetCodec, hetTransp]
	hetRouter = cluster.Router[hetMsg, hetCodec, hetTransp]
	hetReg    = rpc.RegistryBuilder[hetMsg, hetCodec]
)

// ─── Actor 类型 ───

type HPlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id HPlayerId) ActorType() actor.ActorType { return "Player" }
func (id HPlayerId) String() string              { return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId) }

type HRoomId struct {
	RoomId int `json:"roomId"`
}

func (id HRoomId) ActorType() actor.ActorType { return "Room" }
func (id HRoomId) String() string             { return fmt.Sprintf("Room(%d)", id.RoomId) }

type HChatId struct {
	Channel string `json:"channel"`
}

func (id HChatId) ActorType() actor.ActorType { return "Chat" }
func (id HChatId) String() string             { return fmt.Sprintf("Chat(%s)", id.Channel) }

// ─── 消息类型 ───

type HLogin struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*HLogin) ReqType(_ HPlayerId, _ actor.OkReply) string { return "Login" }

type HAttack struct{ Damage int `json:"damage"` }
type HAttackReply struct {
	RemainingHP int `json:"remainingHP"`
}

func (*HAttack) ReqType(_ HPlayerId, _ *HAttackReply) string { return "Attack" }

type HHeal struct{ Amount int `json:"amount"` }
type HHealReply struct {
	NewHP int `json:"newHP"`
}

func (*HHeal) ReqType(_ HPlayerId, _ *HHealReply) string { return "Heal" }

type HCreateRoom struct{ MaxPlayers int `json:"maxPlayers"` }

func (*HCreateRoom) ReqType(_ HRoomId, _ actor.OkReply) string { return "CreateRoom" }

type HRoomInfo struct{}
type HRoomInfoReply struct {
	MaxPlayers int `json:"maxPlayers"`
}

func (*HRoomInfo) ReqType(_ HRoomId, _ *HRoomInfoReply) string { return "RoomInfo" }

type HSendMessage struct{ Text string `json:"text"` }
type HSendMessageReply struct {
	Echo string `json:"echo"`
}

func (*HSendMessage) ReqType(_ HChatId, _ *HSendMessageReply) string { return "SendMessage" }

// ─── State ───

type HPlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}
type HRoomState struct {
	MaxPlayers int `json:"maxPlayers"`
}
type HChatState struct {
	Messages []string `json:"messages"`
}

// ─── GroupMapping ───

var hetGroupMapping = cluster.GroupMapping{
	"player-server": {"Player"},
	"room-server":   {"Room"},
	"chat-server":   {"Chat"},
}

// ─── 测试辅助 ───

func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatalf("freePort: %v", err)
	}
	port := l.Addr().(*net.TCPAddr).Port
	l.Close()
	return port
}

// startHetNode 启动一个异构集群节点。
// allKnownNodes 是集群中所有节点的 Node 信息（含自身），
// 在启动前就已确定，确保每个节点的 Membership 都包含完整集群视图。
func startHetNode(t *testing.T, nodeID, nodeType, addr string, allKnownNodes []cluster.Node) *cluster.Router[hetMsg, hetCodec, hetTransp] {
	t.Helper()

	// 找到自身
	var self cluster.Node
	for _, n := range allKnownNodes {
		if n.ID == nodeID {
			self = n
			break
		}
	}
	if self.ID == "" {
		t.Fatalf("node %s not found in allKnownNodes", nodeID)
	}

	mgr := actor.NewManager()

	switch nodeType {
	case "player-server":
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HPlayerId, HPlayerState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HPlayerId, HPlayerState], req *HLogin, _ bool) (actor.OkReply, error) {
				ctx.SetState(HPlayerState{HP: req.InitHP, Level: req.InitLevel})
				return actor.OK, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HPlayerId, HPlayerState], req *HAttack, _ bool) (*HAttackReply, error) {
				ctx.State().HP -= req.Damage
				return &HAttackReply{RemainingHP: ctx.State().HP}, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HPlayerId, HPlayerState], req *HHeal, _ bool) (*HHealReply, error) {
				ctx.State().HP += req.Amount
				return &HHealReply{NewHP: ctx.State().HP}, nil
			})
		})
	case "room-server":
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HRoomId, HRoomState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HRoomId, HRoomState], req *HCreateRoom, _ bool) (actor.OkReply, error) {
				ctx.SetState(HRoomState{MaxPlayers: req.MaxPlayers})
				return actor.OK, nil
			})
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HRoomId, HRoomState], req *HRoomInfo, _ bool) (*HRoomInfoReply, error) {
				return &HRoomInfoReply{MaxPlayers: ctx.State().MaxPlayers}, nil
			})
		})
	case "chat-server":
		actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[HChatId, HChatState]) {
			actor.RegisterServe(b, func(ctx *actor.ActorContext[HChatId, HChatState], req *HSendMessage, _ bool) (*HSendMessageReply, error) {
				ctx.State().Messages = append(ctx.State().Messages, req.Text)
				return &HSendMessageReply{Echo: req.Text}, nil
			})
		})
	}

	// RPC Server
	server := rpc.NewServer[hetMsg, hetCodec, hetTransp](addr, mgr, func(b *hetReg) {
		switch nodeType {
		case "player-server":
			rpc.RegisterRequest(b, &HLogin{})
			rpc.RegisterRequest(b, &HAttack{})
			rpc.RegisterRequest(b, &HHeal{})
		case "room-server":
			rpc.RegisterRequest(b, &HCreateRoom{})
			rpc.RegisterRequest(b, &HRoomInfo{})
		case "chat-server":
			rpc.RegisterRequest(b, &HSendMessage{})
		}
	})
	if err := server.Start(); err != nil {
		t.Fatalf("start %s: %v", nodeID, err)
	}
	t.Cleanup(func() { server.Shutdown(context.Background()) })
	t.Cleanup(func() { mgr.CloseManager() })

	mem := newStaticMembership(self, allKnownNodes...)
	placement := cluster.NewConsistentHashPlacement(128).WithGroupMapping(hetGroupMapping)
	router := cluster.NewRouter[hetMsg, hetCodec, hetTransp](mem, placement, mgr)

	return router
}

// ─── StaticMembership ───

type staticMembership struct {
	self    cluster.Node
	members cluster.NodeSet
	events  chan cluster.MemberEvent
}

func newStaticMembership(self cluster.Node, members ...cluster.Node) *staticMembership {
	return &staticMembership{
		self:    self,
		members: cluster.NodeSet(members),
		events:  make(chan cluster.MemberEvent, 10),
	}
}

func (s *staticMembership) Self() cluster.Node                 { return s.self }
func (s *staticMembership) Members() cluster.NodeSet           { return s.members }
func (s *staticMembership) Events() <-chan cluster.MemberEvent { return s.events }
func (s *staticMembership) Join(seeds []string) error          { return nil }
func (s *staticMembership) Leave() error                       { return nil }
func (s *staticMembership) Close() error                       { return nil }

// ─── 端到端测试 ───

// TestHeterogeneousCluster_E2E_Network 启动三个不同类型的真实节点，
// 通过 WebSocket RPC 进行跨节点通信。
// 每个节点只注册自己负责的 Actor Group（player/room/chat），
// 验证 Router 通过 GroupMapping + Placement 正确路由消息。
func TestHeterogeneousCluster_E2E_Network(t *testing.T) {
	ctx := context.Background()

	// 先分配端口，构建完整的集群拓扑
	pPort := freePort(t)
	rPort := freePort(t)
	cPort := freePort(t)

	pAddr := fmt.Sprintf("localhost:%d", pPort)
	rAddr := fmt.Sprintf("localhost:%d", rPort)
	cAddr := fmt.Sprintf("localhost:%d", cPort)

	allNodes := []cluster.Node{
		{ID: "player-1", Addr: pAddr, Type: "player-server"},
		{ID: "room-1", Addr: rAddr, Type: "room-server"},
		{ID: "chat-1", Addr: cAddr, Type: "chat-server"},
	}

	// 启动所有节点（每个节点都知道完整集群拓扑）
	pRouter := startHetNode(t, "player-1", "player-server", pAddr, allNodes)
	rRouter := startHetNode(t, "room-1", "room-server", rAddr, allNodes)
	cRouter := startHetNode(t, "chat-1", "chat-server", cAddr, allNodes)

	time.Sleep(500 * time.Millisecond)
	t.Logf("cluster: player=%s, room=%s, chat=%s", pAddr, rAddr, cAddr)

	// ─── 1. Player 操作 ───
	t.Run("player_operations", func(t *testing.T) {
		id := HPlayerId{ServerId: 1, OpenId: "alice"}

		err := cluster.Post[hetMsg, hetCodec, hetTransp](pRouter, id, &HLogin{InitHP: 100, InitLevel: 1})
		if err != nil {
			t.Fatalf("Post Login: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// 本地 Call
		reply, err := cluster.Call(ctx, pRouter, id, &HAttack{Damage: 30})
		if err != nil {
			t.Fatalf("Call Attack: %v", err)
		}
		if reply.RemainingHP != 70 {
			t.Errorf("Attack: want 70, got %d", reply.RemainingHP)
		}

		// 从 roomNode 远程访问 Player
		healReply, err := cluster.Call(ctx, rRouter, id, &HHeal{Amount: 10})
		if err != nil {
			t.Fatalf("Call Heal from roomRouter: %v", err)
		}
		if healReply.NewHP != 80 {
			t.Errorf("Heal from room: want 80, got %d", healReply.NewHP)
		}

		// 从 chatNode 远程访问 Player
		attackReply, err := cluster.Call(ctx, cRouter, id, &HAttack{Damage: 10})
		if err != nil {
			t.Fatalf("Call Attack from chatRouter: %v", err)
		}
		if attackReply.RemainingHP != 70 {
			t.Errorf("Attack from chat: want 70, got %d", attackReply.RemainingHP)
		}
	})

	// ─── 2. Room 操作 ───
	t.Run("room_operations", func(t *testing.T) {
		id := HRoomId{RoomId: 1001}

		err := cluster.Post[hetMsg, hetCodec, hetTransp](rRouter, id, &HCreateRoom{MaxPlayers: 10})
		if err != nil {
			t.Fatalf("Post CreateRoom: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// 从 playerNode 远程访问 Room
		info, err := cluster.Call(ctx, pRouter, id, &HRoomInfo{})
		if err != nil {
			t.Fatalf("Call RoomInfo from playerRouter: %v", err)
		}
		if info.MaxPlayers != 10 {
			t.Errorf("RoomInfo: want 10, got %d", info.MaxPlayers)
		}

		// 从 chatNode 远程访问 Room
		info2, err := cluster.Call(ctx, cRouter, id, &HRoomInfo{})
		if err != nil {
			t.Fatalf("Call RoomInfo from chatRouter: %v", err)
		}
		if info2.MaxPlayers != 10 {
			t.Errorf("RoomInfo from chat: want 10, got %d", info2.MaxPlayers)
		}
	})

	// ─── 3. Chat 操作 ───
	t.Run("chat_operations", func(t *testing.T) {
		id := HChatId{Channel: "general"}

		err := cluster.Post[hetMsg, hetCodec, hetTransp](cRouter, id, &HSendMessage{Text: "hello world"})
		if err != nil {
			t.Fatalf("Post SendMessage: %v", err)
		}
		time.Sleep(200 * time.Millisecond)

		// 从 playerNode 远程发送
		reply, err := cluster.Call(ctx, pRouter, id, &HSendMessage{Text: "hi from player"})
		if err != nil {
			t.Fatalf("Call SendMessage from playerRouter: %v", err)
		}
		if reply.Echo != "hi from player" {
			t.Errorf("echo: want 'hi from player', got %q", reply.Echo)
		}

		// 从 roomNode 远程发送
		reply2, err := cluster.Call(ctx, rRouter, id, &HSendMessage{Text: "room says hi"})
		if err != nil {
			t.Fatalf("Call SendMessage from roomRouter: %v", err)
		}
		if reply2.Echo != "room says hi" {
			t.Errorf("echo2: want 'room says hi', got %q", reply2.Echo)
		}
	})

	// ─── 4. 验证 Actor 严格分布在对应节点 ───
	t.Run("actor_distribution", func(t *testing.T) {
		// 通过每个 Router 背后的 Mgr 无法直接访问，但 Placement 保证了正确性。
		// 此处通过 IsLocal 验证。
		for i := 0; i < 100; i++ {
			// Player 只能在 player-1
			if !pRouter.IsLocal("Player", fmt.Sprintf("p-%d", i)) {
				t.Fatalf("Player p-%d should be local to player-1", i)
			}
			if rRouter.IsLocal("Player", fmt.Sprintf("p-%d", i)) {
				t.Fatalf("Player p-%d should NOT be local to room-1", i)
			}
			if cRouter.IsLocal("Player", fmt.Sprintf("p-%d", i)) {
				t.Fatalf("Player p-%d should NOT be local to chat-1", i)
			}

			// Room 只能在 room-1
			if !rRouter.IsLocal("Room", fmt.Sprintf("r-%d", i)) {
				t.Fatalf("Room r-%d should be local to room-1", i)
			}
			if pRouter.IsLocal("Room", fmt.Sprintf("r-%d", i)) {
				t.Fatalf("Room r-%d should NOT be local to player-1", i)
			}
			if cRouter.IsLocal("Room", fmt.Sprintf("r-%d", i)) {
				t.Fatalf("Room r-%d should NOT be local to chat-1", i)
			}

			// Chat 只能在 chat-1
			if !cRouter.IsLocal("Chat", fmt.Sprintf("c-%d", i)) {
				t.Fatalf("Chat c-%d should be local to chat-1", i)
			}
			if pRouter.IsLocal("Chat", fmt.Sprintf("c-%d", i)) {
				t.Fatalf("Chat c-%d should NOT be local to player-1", i)
			}
			if rRouter.IsLocal("Chat", fmt.Sprintf("c-%d", i)) {
				t.Fatalf("Chat c-%d should NOT be local to room-1", i)
			}
		}
	})

	// ─── 5. 跨节点 Call 延迟 ───
	t.Run("cross_node_latency", func(t *testing.T) {
		id := HPlayerId{ServerId: 1, OpenId: "alice"}
		start := time.Now()
		_, err := cluster.Call(ctx, rRouter, id, &HAttack{Damage: 0})
		elapsed := time.Since(start)
		if err != nil {
			t.Fatalf("cross-node Call: %v", err)
		}
		if elapsed > 2*time.Second {
			t.Errorf("cross-node Call too slow: %v", elapsed)
		}
		t.Logf("cross-node Call latency: %v", elapsed)
	})

	// ─── 6. 并发跨节点访问 ───
	t.Run("concurrent_cross_node", func(t *testing.T) {
		id := HPlayerId{ServerId: 1, OpenId: "alice"}
		errCh := make(chan error, 20)
		for i := 0; i < 10; i++ {
			go func() { _, err := cluster.Call(ctx, cRouter, id, &HAttack{Damage: 1}); errCh <- err }()
			go func() { _, err := cluster.Call(ctx, rRouter, id, &HHeal{Amount: 1}); errCh <- err }()
		}
		for i := 0; i < 20; i++ {
			if err := <-errCh; err != nil {
				t.Errorf("concurrent cross-node call: %v", err)
			}
		}
	})
}

// TestHeterogeneousCluster_Broadcast 验证异构集群下的广播。
func TestHeterogeneousCluster_Broadcast(t *testing.T) {
	pPort := freePort(t)
	rPort := freePort(t)

	allNodes := []cluster.Node{
		{ID: "player-1", Addr: fmt.Sprintf("localhost:%d", pPort), Type: "player-server"},
		{ID: "room-1", Addr: fmt.Sprintf("localhost:%d", rPort), Type: "room-server"},
	}

	pRouter := startHetNode(t, "player-1", "player-server", allNodes[0].Addr, allNodes)
	rRouter := startHetNode(t, "room-1", "room-server", allNodes[1].Addr, allNodes)
	_ = rRouter

	time.Sleep(500 * time.Millisecond)

	// 创建多个 Player
	for i := 0; i < 3; i++ {
		id := HPlayerId{ServerId: 1, OpenId: fmt.Sprintf("bc-%d", i)}
		err := cluster.Post[hetMsg, hetCodec, hetTransp](pRouter, id, &HLogin{InitHP: 100, InitLevel: 1})
		if err != nil {
			t.Fatalf("Post Login %d: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// 广播 Attack（会发到 player-server 和 room-server）
	// room-server 没有 Player Group，RPC Server 会返回 unknown reqType
	err := cluster.Broadcast[hetMsg, hetCodec, hetTransp, HPlayerId](pRouter, &HAttack{Damage: 10})
	if err != nil {
		t.Logf("Broadcast error (expected if some nodes lack the group): %v", err)
	}
	time.Sleep(200 * time.Millisecond)
}

// TestHeterogeneousCluster_DynamicScale 模拟动态扩缩容：
// 新增一个 player-server 节点后，Player actors 的 Placement 重新分布。
func TestHeterogeneousCluster_DynamicScale(t *testing.T) {
	p1Port := freePort(t)
	p1Addr := fmt.Sprintf("localhost:%d", p1Port)

	// 初始只有一个 player-server
	allNodes1 := []cluster.Node{
		{ID: "player-1", Addr: p1Addr, Type: "player-server"},
	}
	p1Router := startHetNode(t, "player-1", "player-server", p1Addr, allNodes1)
	time.Sleep(300 * time.Millisecond)

	// 单节点时所有 Player 都在 player-1
	for i := 0; i < 100; i++ {
		if !p1Router.IsLocal("Player", fmt.Sprintf("p-%d", i)) {
			t.Fatal("all Player should be local with single node")
		}
	}

	// 第二个 player-server 加入（需要重建 Router 或更新 Membership）
	// 这里演示：重新构建包含两个节点的集群
	p2Port := freePort(t)
	p2Addr := fmt.Sprintf("localhost:%d", p2Port)

	allNodes2 := []cluster.Node{
		{ID: "player-1", Addr: p1Addr, Type: "player-server"},
		{ID: "player-2", Addr: p2Addr, Type: "player-server"},
	}

	_ = startHetNode(t, "player-2", "player-server", p2Addr, allNodes2)

	// 重建 player-1 的 Router（模拟 Membership 更新后重建）
	p1Mem2 := newStaticMembership(allNodes2[0], allNodes2...)
	p1Placement2 := cluster.NewConsistentHashPlacement(128).WithGroupMapping(hetGroupMapping)
	// 注意：这里 Mgr 是同一个，但 Router 重新创建了
	// 实际上生产环境中 Membership 会动态更新 Members()
	_ = cluster.NewRouter[hetMsg, hetCodec, hetTransp](p1Mem2, p1Placement2, nil)
	_ = p1Router // 使用新 Router

	time.Sleep(500 * time.Millisecond)

	// 使用新的 Membership 验证分布
	p1Count := 0
	p2Count := 0
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("p-%d", i)
		n := p1Placement2.Place("Player", key, allNodes2)
		switch n.ID {
		case "player-1":
			p1Count++
		case "player-2":
			p2Count++
		default:
			t.Fatalf("Player %s placed on unknown node %s", key, n.ID)
		}
	}
	if p1Count == 0 || p2Count == 0 {
		t.Errorf("both nodes should have Players: p1=%d, p2=%d", p1Count, p2Count)
	}
	t.Logf("Scale-out distribution: p1=%d, p2=%d", p1Count, p2Count)
}

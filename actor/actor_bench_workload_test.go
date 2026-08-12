package actor_test

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// v2 有实际负载的基准测试
// ============================================================

// ---- 类型定义 ----

// WorkloadId 用于 CPU/内存负载测试
type WorkloadId struct {
	Id int
}

func (id WorkloadId) ActorType() actor.ActorType { return "WorkloadV2" }
func (id WorkloadId) String() string             { return fmt.Sprintf("WorkloadV2(%d)", id.Id) }

type WorkloadState struct {
	Value    int
	Data     map[string]int
	ReqCount int // Churn 场景：记录 actor 已处理请求数，达到阈值后退出
}

type WorkloadSpawn struct{ Init int }

func (*WorkloadSpawn) ReqType(_ WorkloadId, _ actor.OkReply) string { return "WorkloadSpawn" }

// ---- CPU 密集型 ----

type FibReq struct{ N int }
type FibReply struct{ Result int }

func (*FibReq) ReqType(_ WorkloadId, _ *FibReply) string { return "FibReq" }

func fib(n int) int {
	if n <= 1 {
		return n
	}
	return fib(n-1) + fib(n-2)
}

// BenchmarkV2CallCPUWorkload 单 Actor Call 含 CPU 密集型计算（斐波那契 fib(20)）。
// 测试 handler 中有实际计算开销时的吞吐量。
func BenchmarkV2CallCPUWorkload(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(bb *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			*a.State() = WorkloadState{Value: req.Init, Data: make(map[string]int)}
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *FibReq, _ bool) (*FibReply, error) {
			return &FibReply{Result: fib(req.N)}, nil
		})
	})
	id := WorkloadId{Id: 1}
	_ = actor.Post(mgr, id, &WorkloadSpawn{Init: 0})
	actor.Call(context.Background(), mgr, id, &FibReq{N: 1})

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.Call(ctx, mgr, id, &FibReq{N: 20}); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- 字符串处理 ----

type StrProcReq struct{ Input string }
type StrProcReply struct {
	Reversed  string
	WordCount int
}

func (*StrProcReq) ReqType(_ WorkloadId, _ *StrProcReply) string { return "StrProcReq" }

// BenchmarkV2CallStringWorkload 单 Actor Call 含字符串处理（反转、分词计数）。
func BenchmarkV2CallStringWorkload(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(bb *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			a.State().Value = req.Init
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *StrProcReq, _ bool) (*StrProcReply, error) {
			runes := []rune(req.Input)
			n := len(runes)
			reversed := make([]rune, n)
			for i, r := range runes {
				reversed[n-1-i] = r
			}
			words := 0
			inWord := false
			for _, r := range runes {
				if r == ' ' {
					inWord = false
				} else if !inWord {
					words++
					inWord = true
				}
			}
			return &StrProcReply{Reversed: string(reversed), WordCount: words}, nil
		})
	})
	id := WorkloadId{Id: 1}
	_ = actor.Post(mgr, id, &WorkloadSpawn{Init: 0})
	actor.Call(context.Background(), mgr, id, &StrProcReq{Input: "hello"})

	input := "the quick brown fox jumps over the lazy dog"
	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if _, err := actor.Call(ctx, mgr, id, &StrProcReq{Input: input}); err != nil {
			b.Fatal(err)
		}
	}
}

// ---- Map 操作 ----

type MapOpsReq struct {
	Op    string
	Key   string
	Value int
}
type MapOpsReply struct {
	Value int
	Found bool
	Size  int
}

func (*MapOpsReq) ReqType(_ WorkloadId, _ *MapOpsReply) string { return "MapOpsReq" }

// BenchmarkV2CallMapWorkload 单 Actor Call 含 Map 操作（插入/查找/删除）。
func BenchmarkV2CallMapWorkload(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(bb *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			*a.State() = WorkloadState{Value: req.Init, Data: make(map[string]int)}
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MapOpsReq, _ bool) (*MapOpsReply, error) {
			s := a.State()
			switch req.Op {
			case "set":
				s.Data[req.Key] = req.Value
			case "get":
				v, found := s.Data[req.Key]
				return &MapOpsReply{Value: v, Found: found, Size: len(s.Data)}, nil
			case "del":
				delete(s.Data, req.Key)
			}
			return &MapOpsReply{Size: len(s.Data)}, nil
		})
	})
	id := WorkloadId{Id: 1}
	_ = actor.Post(mgr, id, &WorkloadSpawn{Init: 0})
	actor.Call(context.Background(), mgr, id, &MapOpsReq{Op: "set", Key: "init", Value: 0})

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := fmt.Sprintf("key_%d", i%100)
		op := "get"
		if i%3 == 0 {
			op = "set"
		}
		if _, err := actor.Call(ctx, mgr, id, &MapOpsReq{Op: op, Key: key, Value: i}); err != nil {
			b.Fatal(err)
		}
	}
}

// ============================================================
// v2 模拟真实场景的基准测试
// ============================================================

// ---- 游戏服务器场景 ----

type GamePlayerId struct {
	PlayerId int
}

func (id GamePlayerId) ActorType() actor.ActorType { return "GamePlayerV2" }
func (id GamePlayerId) String() string             { return fmt.Sprintf("GamePlayerV2(%d)", id.PlayerId) }

type Position struct{ X, Y float64 }

type GamePlayerState struct {
	HP       int
	MaxHP    int
	Level    int
	XP       int
	Pos      Position
	Status   string
	StatsHit int
}

type GameLoginReq struct{ InitHP int }

func (*GameLoginReq) ReqType(_ GamePlayerId, _ actor.OkReply) string { return "GameLoginReq" }

type GameMoveReq struct{ DX, DY float64 }

func (*GameMoveReq) ReqType(_ GamePlayerId, _ actor.OkReply) string { return "GameMoveReq" }

type GameAttackReq struct {
	TargetId int
	Damage   int
}
type GameAttackReply struct{ TargetHP int }

func (*GameAttackReq) ReqType(_ GamePlayerId, _ *GameAttackReply) string { return "GameAttackReq" }

type GameHealReq struct{ Amount int }
type GameHealReply struct{ HP int }

func (*GameHealReq) ReqType(_ GamePlayerId, _ *GameHealReply) string { return "GameHealReq" }

type GameGetInfoReq struct{}
type GameGetInfoReply struct {
	HP    int
	Level int
	XP    int
}

func (*GameGetInfoReq) ReqType(_ GamePlayerId, _ *GameGetInfoReply) string { return "GameGetInfoReq" }

// setupGameManagerV2 注册游戏玩家 Actor 的所有 handler。
func setupGameManagerV2(mgr *actor.Manager) {
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[GamePlayerId, GamePlayerState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[GamePlayerId, GamePlayerState], req *GameLoginReq, spawning bool) (actor.OkReply, error) {
			a.SetState(GamePlayerState{
				HP:     req.InitHP,
				MaxHP:  req.InitHP,
				Level:  1,
				Pos:    Position{X: 0, Y: 0},
				Status: "idle",
			})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GamePlayerId, GamePlayerState], req *GameMoveReq, _ bool) (actor.OkReply, error) {
			s := a.State()
			s.Pos.X += req.DX
			s.Pos.Y += req.DY
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GamePlayerId, GamePlayerState], req *GameAttackReq, _ bool) (*GameAttackReply, error) {
			s := a.State()
			s.StatsHit++
			s.HP -= req.Damage
			if s.HP < 0 {
				s.HP = 0
				s.Status = "dead"
			}
			return &GameAttackReply{TargetHP: s.HP}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GamePlayerId, GamePlayerState], req *GameHealReq, _ bool) (*GameHealReply, error) {
			s := a.State()
			s.HP += req.Amount
			if s.HP > s.MaxHP {
				s.HP = s.MaxHP
			}
			return &GameHealReply{HP: s.HP}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[GamePlayerId, GamePlayerState], req *GameGetInfoReq, _ bool) (*GameGetInfoReply, error) {
			s := a.State()
			return &GameGetInfoReply{HP: s.HP, Level: s.Level, XP: s.XP}, nil
		})
	})
}

// BenchmarkV2GameServerScenario 模拟游戏服务器场景：
// 32 个玩家 Actor，并发执行移动、攻击、治疗、查询操作的混合负载。
func BenchmarkV2GameServerScenario(b *testing.B) {
	const numPlayers = 32
	mgr := actor.NewManager()
	setupGameManagerV2(mgr)
	ctx := context.Background()

	ids := make([]GamePlayerId, numPlayers)
	for i := 0; i < numPlayers; i++ {
		ids[i] = GamePlayerId{PlayerId: i + 1}
		_ = actor.Post(mgr, ids[i], &GameLoginReq{InitHP: 100})
		actor.Call(ctx, mgr, ids[i], &GameGetInfoReq{})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1))
			playerIdx := i % numPlayers
			opType := i % 100

			switch {
			case opType < 40: // 40% 移动操作
				actor.Call(ctx, mgr, ids[playerIdx], &GameMoveReq{DX: float64(i % 10), DY: float64(i % 5)})
			case opType < 60: // 20% 攻击操作
				actor.Call(ctx, mgr, ids[playerIdx], &GameAttackReq{Damage: (i % 10) + 1})
			case opType < 75: // 15% 治疗操作
				actor.Call(ctx, mgr, ids[playerIdx], &GameHealReq{Amount: (i % 5) + 1})
			case opType < 80: // 5% 查询操作
				actor.Call(ctx, mgr, ids[playerIdx], &GameGetInfoReq{})
			default: // 20% Post 移动（fire-and-forget）
				_ = actor.Post(mgr, ids[playerIdx], &GameMoveReq{DX: 0, DY: 0})
			}
		}
	})
}

// ---- 聊天室场景 ----

type ChatRoomId struct {
	RoomId int
}

func (id ChatRoomId) ActorType() actor.ActorType { return "ChatRoomV2" }
func (id ChatRoomId) String() string             { return fmt.Sprintf("ChatRoomV2(%d)", id.RoomId) }

type ChatRoomState struct {
	Members  map[int]string
	MsgCount int
}

type ChatCreateReq struct{}

func (*ChatCreateReq) ReqType(_ ChatRoomId, _ actor.OkReply) string { return "ChatCreateReq" }

type ChatJoinReq struct {
	MemberId int
	Name     string
}

func (*ChatJoinReq) ReqType(_ ChatRoomId, _ actor.OkReply) string { return "ChatJoinReq" }

type ChatLeaveReq struct{ MemberId int }

func (*ChatLeaveReq) ReqType(_ ChatRoomId, _ actor.OkReply) string { return "ChatLeaveReq" }

type ChatSendMsgReq struct {
	MemberId int
	Content  string
}
type ChatSendMsgReply struct{ MsgCount int }

func (*ChatSendMsgReq) ReqType(_ ChatRoomId, _ *ChatSendMsgReply) string { return "ChatSendMsgReq" }

type ChatGetInfoReq struct{}
type ChatGetInfoReply struct {
	MemberCount int
	MsgCount    int
}

func (*ChatGetInfoReq) ReqType(_ ChatRoomId, _ *ChatGetInfoReply) string { return "ChatGetInfoReq" }

// setupChatManagerV2 注册聊天室 Actor 的所有 handler。
func setupChatManagerV2(mgr *actor.Manager) {
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[ChatRoomId, ChatRoomState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[ChatRoomId, ChatRoomState], req *ChatCreateReq, spawning bool) (actor.OkReply, error) {
			a.SetState(ChatRoomState{Members: make(map[int]string)})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[ChatRoomId, ChatRoomState], req *ChatJoinReq, _ bool) (actor.OkReply, error) {
			a.State().Members[req.MemberId] = req.Name
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[ChatRoomId, ChatRoomState], req *ChatLeaveReq, _ bool) (actor.OkReply, error) {
			delete(a.State().Members, req.MemberId)
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[ChatRoomId, ChatRoomState], req *ChatSendMsgReq, _ bool) (*ChatSendMsgReply, error) {
			s := a.State()
			s.MsgCount++
			return &ChatSendMsgReply{MsgCount: s.MsgCount}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[ChatRoomId, ChatRoomState], req *ChatGetInfoReq, _ bool) (*ChatGetInfoReply, error) {
			s := a.State()
			return &ChatGetInfoReply{MemberCount: len(s.Members), MsgCount: s.MsgCount}, nil
		})
	})
}

// BenchmarkV2ChatRoomScenario 模拟聊天室场景：
// 单个聊天室 Actor，成员加入/离开/发消息/查询的混合负载。
func BenchmarkV2ChatRoomScenario(b *testing.B) {
	mgr := actor.NewManager()
	setupChatManagerV2(mgr)
	ctx := context.Background()

	roomId := ChatRoomId{RoomId: 1}
	_ = actor.Post(mgr, roomId, &ChatCreateReq{})
	actor.Call(ctx, mgr, roomId, &ChatGetInfoReq{})

	for i := 0; i < 100; i++ {
		actor.Call(ctx, mgr, roomId, &ChatJoinReq{MemberId: i, Name: fmt.Sprintf("user_%d", i)})
	}

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		var idx int
		for pb.Next() {
			idx++
			memberId := idx % 200
			opType := idx % 100

			switch {
			case opType < 50: // 50% 发消息
				actor.Call(ctx, mgr, roomId, &ChatSendMsgReq{MemberId: memberId, Content: "hello world"})
			case opType < 70: // 20% 加入
				actor.Call(ctx, mgr, roomId, &ChatJoinReq{MemberId: memberId, Name: fmt.Sprintf("user_%d", memberId)})
			case opType < 80: // 10% 离开
				actor.Call(ctx, mgr, roomId, &ChatLeaveReq{MemberId: memberId})
			default: // 20% 查询
				actor.Call(ctx, mgr, roomId, &ChatGetInfoReq{})
			}
		}
	})
}

// ---- 电商订单场景 ----

type EOrderId struct {
	OrderId int
}

func (id EOrderId) ActorType() actor.ActorType { return "EOrderV2" }
func (id EOrderId) String() string             { return fmt.Sprintf("EOrderV2(%d)", id.OrderId) }

type OrderItem struct {
	Name  string
	Price int
	Qty   int
}

type EOrderState struct {
	Items  []OrderItem
	Status string
	Total  int
}

type EOrderCreateReq struct{ InitBalance int }

func (*EOrderCreateReq) ReqType(_ EOrderId, _ actor.OkReply) string { return "EOrderCreateReq" }

type EOrderAddItemReq struct {
	Name  string
	Price int
	Qty   int
}
type EOrderAddItemReply struct {
	Total     int
	ItemCount int
}

func (*EOrderAddItemReq) ReqType(_ EOrderId, _ *EOrderAddItemReply) string { return "EOrderAddItemReq" }

type EOrderCheckoutReq struct{}
type EOrderCheckoutReply struct {
	Total  int
	Status string
}

func (*EOrderCheckoutReq) ReqType(_ EOrderId, _ *EOrderCheckoutReply) string {
	return "EOrderCheckoutReq"
}

type EOrderGetTotalReq struct{}
type EOrderGetTotalReply struct {
	Total     int
	ItemCount int
}

func (*EOrderGetTotalReq) ReqType(_ EOrderId, _ *EOrderGetTotalReply) string {
	return "EOrderGetTotalReq"
}

// BenchmarkV2ECommerceScenario 模拟电商订单场景。
func BenchmarkV2ECommerceScenario(b *testing.B) {
	const numOrders = 32
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[EOrderId, EOrderState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[EOrderId, EOrderState], req *EOrderCreateReq, spawning bool) (actor.OkReply, error) {
			a.SetState(EOrderState{Status: "open"})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[EOrderId, EOrderState], req *EOrderAddItemReq, _ bool) (*EOrderAddItemReply, error) {
			s := a.State()
			if s.Status != "open" {
				return &EOrderAddItemReply{Total: s.Total, ItemCount: len(s.Items)}, nil
			}
			s.Items = append(s.Items, OrderItem{Name: req.Name, Price: req.Price, Qty: req.Qty})
			s.Total += req.Price * req.Qty
			return &EOrderAddItemReply{Total: s.Total, ItemCount: len(s.Items)}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[EOrderId, EOrderState], req *EOrderCheckoutReq, _ bool) (*EOrderCheckoutReply, error) {
			s := a.State()
			s.Status = "checked_out"
			return &EOrderCheckoutReply{Total: s.Total, Status: s.Status}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[EOrderId, EOrderState], req *EOrderGetTotalReq, _ bool) (*EOrderGetTotalReply, error) {
			s := a.State()
			return &EOrderGetTotalReply{Total: s.Total, ItemCount: len(s.Items)}, nil
		})
	})

	ctx := context.Background()
	ids := make([]EOrderId, numOrders)
	for i := 0; i < numOrders; i++ {
		ids[i] = EOrderId{OrderId: i + 1}
		_ = actor.Post(mgr, ids[i], &EOrderCreateReq{InitBalance: 0})
		actor.Call(ctx, mgr, ids[i], &EOrderGetTotalReq{})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		items := []OrderItem{
			{Name: "laptop", Price: 9999, Qty: 1},
			{Name: "mouse", Price: 199, Qty: 2},
			{Name: "keyboard", Price: 599, Qty: 1},
			{Name: "monitor", Price: 2999, Qty: 1},
		}
		for pb.Next() {
			i := int(idx.Add(1))
			orderIdx := i % numOrders
			opType := i % 100

			switch {
			case opType < 70: // 70% 添加商品
				item := items[i%len(items)]
				actor.Call(ctx, mgr, ids[orderIdx], &EOrderAddItemReq{Name: item.Name, Price: item.Price, Qty: item.Qty})
			case opType < 85: // 15% 查询
				actor.Call(ctx, mgr, ids[orderIdx], &EOrderGetTotalReq{})
			default: // 15% 结账
				actor.Call(ctx, mgr, ids[orderIdx], &EOrderCheckoutReq{})
			}
		}
	})
}

// ---- Actor 生命周期吞吐 ----

// BenchmarkV2ActorLifecycle 测试 Actor 完整生命周期吞吐：spawn → work → close。
func BenchmarkV2ActorLifecycle(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			a.SetState(WorkloadState{Value: req.Init, Data: make(map[string]int)})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MapOpsReq, _ bool) (*MapOpsReply, error) {
			s := a.State()
			s.Data[req.Key] = req.Value
			s.Value += req.Value
			return &MapOpsReply{Value: s.Value, Size: len(s.Data)}, nil
		})
	})
	ctx := context.Background()

	const opsPerLifecycle = 5

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		id := WorkloadId{Id: i}
		_ = actor.Post(mgr, id, &WorkloadSpawn{Init: 0})
		actor.Call(ctx, mgr, id, &MapOpsReq{Op: "set", Key: "spawn_check", Value: 0})
		for j := 0; j < opsPerLifecycle; j++ {
			key := fmt.Sprintf("key_%d", j)
			actor.Call(ctx, mgr, id, &MapOpsReq{Op: "set", Key: key, Value: j})
		}
	}
}

// ---- 混合负载 ----

// BenchmarkV2MixedWorkload 混合 Post + Call + Broadcast 并发负载。
func BenchmarkV2MixedWorkload(b *testing.B) {
	const numActors = 50
	mgr := actor.NewManager()

	var recv atomic.Int64
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			a.SetState(WorkloadState{Value: req.Init})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *FibReq, _ bool) (*FibReply, error) {
			return &FibReply{Result: fib(req.N)}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MapOpsReq, _ bool) (*MapOpsReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			return &MapOpsReply{Value: s.Value}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MixBroadcastReq, _ bool) (actor.OkReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			return actor.OK, nil
		})
	})

	ctx := context.Background()
	ids := make([]WorkloadId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = WorkloadId{Id: i + 1}
		_ = actor.Post(mgr, ids[i], &WorkloadSpawn{Init: 0})
		actor.Call(ctx, mgr, ids[i], &FibReq{N: 1})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1))
			actorIdx := i % numActors
			opType := i % 100

			switch {
			case opType < 40: // 40% Call (CPU workload)
				actor.Call(ctx, mgr, ids[actorIdx], &FibReq{N: 5})
			case opType < 70: // 30% Post
				actor.Call(ctx, mgr, ids[actorIdx], &MapOpsReq{Key: "ping", Value: 1})
			case opType < 85: // 15% Broadcast
				actor.Broadcast(mgr, &MixBroadcastReq{Key: "bcast", Value: 1})
			default: // 15% Spawn new actor
				newId := WorkloadId{Id: i + numActors + 1}
				_ = actor.Post(mgr, newId, &WorkloadSpawn{Init: 0})
			}
		}
	})
}

// BenchmarkV2MixedWorkloadNoSpawn 与 MixedWorkload 相同但没有 Spawn 操作，
// 用于隔离 Spawn 写锁对并行吞吐的影响。
func BenchmarkV2MixedWorkloadNoSpawn(b *testing.B) {
	const numActors = 50
	mgr := actor.NewManager()

	var recv atomic.Int64
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			a.SetState(WorkloadState{Value: req.Init})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *FibReq, _ bool) (*FibReply, error) {
			return &FibReply{Result: fib(req.N)}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MapOpsReq, _ bool) (*MapOpsReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			return &MapOpsReply{Value: s.Value}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MixBroadcastReq, _ bool) (actor.OkReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			return actor.OK, nil
		})
	})

	ctx := context.Background()
	ids := make([]WorkloadId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = WorkloadId{Id: i + 1}
		_ = actor.Post(mgr, ids[i], &WorkloadSpawn{Init: 0})
		actor.Call(ctx, mgr, ids[i], &FibReq{N: 1})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1))
			actorIdx := i % numActors
			opType := i % 85 // 去掉 Spawn，按比例重新分布到 85%

			switch {
			case opType < 40: // 40/85 ≈ 47% Call (CPU workload)
				actor.Call(ctx, mgr, ids[actorIdx], &FibReq{N: 5})
			case opType < 70: // 30/85 ≈ 35% Post
				actor.Call(ctx, mgr, ids[actorIdx], &MapOpsReq{Key: "ping", Value: 1})
			default: // 15/85 ≈ 18% Broadcast
				actor.Broadcast(mgr, &MixBroadcastReq{Key: "bcast", Value: 1})
			}
		}
	})
}

// BenchmarkV2MixedWorkloadChurn 混合负载含 actor churn：每个 actor 处理 N 个请求后
// 通过 Quit() 优雅退出，由 Spawn 操作重新创建，总 actor 数稳定在 numActors。
// hold/unhold 原子操作保证退出期间 send 安全，不存在 v1 的并发调用 panic 风险。
func BenchmarkV2MixedWorkloadChurn(b *testing.B) {
	const numActors = 100
	const requestsBeforeQuit = 50
	mgr := actor.NewManager()

	var recv atomic.Int64
	actor.Serve(mgr, options, func(b *actor.RegistryBuilder[WorkloadId, WorkloadState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *WorkloadSpawn, spawning bool) (actor.OkReply, error) {
			a.SetState(WorkloadState{Value: req.Init})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *FibReq, _ bool) (*FibReply, error) {
			s := a.State()
			s.ReqCount++
			if s.ReqCount >= requestsBeforeQuit {
				a.Quit()
			}
			return &FibReply{Result: fib(req.N)}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MapOpsReq, _ bool) (*MapOpsReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			s.ReqCount++
			if s.ReqCount >= requestsBeforeQuit {
				a.Quit()
			}
			return &MapOpsReply{Value: s.Value}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[WorkloadId, WorkloadState], req *MixBroadcastReq, _ bool) (actor.OkReply, error) {
			s := a.State()
			s.Value += req.Value
			recv.Add(1)
			return actor.OK, nil
		})
	})

	ctx := context.Background()
	ids := make([]WorkloadId, numActors)
	for i := 0; i < numActors; i++ {
		ids[i] = WorkloadId{Id: i + 1}
		_ = actor.Post(mgr, ids[i], &WorkloadSpawn{Init: 0})
		actor.Call(ctx, mgr, ids[i], &FibReq{N: 1})
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1))
			actorIdx := i % numActors
			opType := i % 100

			id := ids[actorIdx]

			switch {
			case opType < 40: // 40% Call (CPU workload)
				actor.Call(ctx, mgr, id, &FibReq{N: 5})
			case opType < 70: // 30% Post
				actor.Call(ctx, mgr, id, &MapOpsReq{Key: "ping", Value: 1})
			case opType < 85: // 15% Broadcast
				actor.Broadcast(mgr, &MixBroadcastReq{Key: "bcast", Value: 1})
			default: // 15% Spawn（重建已退出的 actor，未退出时是空操作）
				_ = actor.Post(mgr, id, &WorkloadSpawn{Init: 0})
			}
		}
	})
}

// ---- 状态累积负载 ----

// MixBroadcastReq 用于 Broadcast 的 OkReply 消息类型。
type MixBroadcastReq struct {
	Key   string
	Value int
}

func (*MixBroadcastReq) ReqType(_ WorkloadId, _ actor.OkReply) string { return "MixBroadcastReq" }

type GrowStateId struct {
	Id int
}

func (id GrowStateId) ActorType() actor.ActorType { return "GrowStateV2" }
func (id GrowStateId) String() string             { return fmt.Sprintf("GrowStateV2(%d)", id.Id) }

type GrowState struct {
	Slice   []int
	MapData map[string]int
	Cursor  int
}

type GrowSpawnReq struct{}

func (*GrowSpawnReq) ReqType(_ GrowStateId, _ actor.OkReply) string { return "GrowSpawnReq" }

type GrowAppendReq struct{ Value int }
type GrowAppendReply struct{ Len int }

func (*GrowAppendReq) ReqType(_ GrowStateId, _ *GrowAppendReply) string { return "GrowAppendReq" }

type GrowQueryReq struct{ Index int }
type GrowQueryReply struct {
	Value int
	Len   int
}

func (*GrowQueryReq) ReqType(_ GrowStateId, _ *GrowQueryReply) string { return "GrowQueryReq" }

// BenchmarkV2StatefulGrowingWorkload 测试 Actor 状态持续增长时的性能表现。
func BenchmarkV2StatefulGrowingWorkload(b *testing.B) {
	mgr := actor.NewManager()
	actor.Serve(mgr, options, func(bb *actor.RegistryBuilder[GrowStateId, GrowState]) {
		actor.RegisterSpawn(bb, func(a *actor.ActorContext[GrowStateId, GrowState], req *GrowSpawnReq, spawning bool) (actor.OkReply, error) {
			a.SetState(GrowState{
				Slice:   make([]int, 0, 1024),
				MapData: make(map[string]int),
			})
			return actor.OK, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[GrowStateId, GrowState], req *GrowAppendReq, _ bool) (*GrowAppendReply, error) {
			s := a.State()
			s.Slice = append(s.Slice, req.Value)
			s.MapData[fmt.Sprintf("key_%d", s.Cursor)] = req.Value
			s.Cursor++
			return &GrowAppendReply{Len: len(s.Slice)}, nil
		})
		actor.RegisterQuery(bb, func(a *actor.ActorContext[GrowStateId, GrowState], req *GrowQueryReq, _ bool) (*GrowQueryReply, error) {
			s := a.State()
			if req.Index >= 0 && req.Index < len(s.Slice) {
				return &GrowQueryReply{Value: s.Slice[req.Index], Len: len(s.Slice)}, nil
			}
			return &GrowQueryReply{Len: len(s.Slice)}, nil
		})
	})

	const numActors = 16
	ids := make([]GrowStateId, numActors)
	ctx := context.Background()

	for i := 0; i < numActors; i++ {
		ids[i] = GrowStateId{Id: i}
		_ = actor.Post(mgr, ids[i], &GrowSpawnReq{})
		actor.Call(ctx, mgr, ids[i], &GrowAppendReq{Value: 0})
		prefill := i * 100
		for j := 0; j < prefill; j++ {
			actor.Call(ctx, mgr, ids[i], &GrowAppendReq{Value: j})
		}
	}

	var idx atomic.Int64

	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := int(idx.Add(1))
			actorIdx := i % numActors
			if i%3 == 0 {
				actor.Call(ctx, mgr, ids[actorIdx], &GrowAppendReq{Value: i})
			} else {
				actor.Call(ctx, mgr, ids[actorIdx], &GrowQueryReq{Index: i % 500})
			}
		}
	})
}

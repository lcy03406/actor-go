package actor_test

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// 测试类型定义
// ============================================================

type TestActorId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id TestActorId) ActorType() actor.ActorType { return "TestActorId" }
func (id TestActorId) String() string {
	return fmt.Sprintf("TestActorId(%d,%s)", id.ServerId, id.OpenId)
}

type TestActorData struct {
	Int int
}

type TestAddReply struct {
	Result int
}

// 请求类型 reqType 字符串常量（仅 RegisterPost 需要）
const (
	ReqTypeTestLogin  = "TestLogin"
	ReqTypeTestLogout = "TestLogout"
	ReqTypeTestClose  = "TestClose"
	ReqTypeTestAdd    = "TestAdd"
)

// 请求类型：实现 actor.Request[TestActorId, R] 接口
// ReqType 的参数类型 (A, *R) 确保编译器能检查 Q 与 A、R 的匹配关系

type TestLogin struct {
	Data TestActorData
}

func (*TestLogin) ReqType(_ TestActorId, _ actor.OkReply) string { return ReqTypeTestLogin }

type TestLogout struct{}

func (*TestLogout) ReqType(_ TestActorId, _ actor.OkReply) string { return ReqTypeTestLogout }

type TestClose struct{}

func (*TestClose) ReqType(_ TestActorId, _ actor.OkReply) string { return ReqTypeTestClose }

type TestAdd struct {
	Add int
}

func (*TestAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return ReqTypeTestAdd }

// TestLoginWithReply 用于 RegisterQuerySpawn 测试，返回 TestAddReply 而非 OkReply。
type TestLoginWithReply struct {
	Data TestActorData
}

func (*TestLoginWithReply) ReqType(_ TestActorId, _ *TestAddReply) string {
	return "TestLoginWithReply"
}

func setupManager(mgr *actor.Manager) {
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			a.State().Int += req.Add
			return &TestAddReply{Result: a.State().Int}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogout, _ bool) (actor.OkReply, error) {
			a.Quit() // Logout 直接关闭 Actor
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestClose, _ bool) (actor.OkReply, error) {
			a.Quit()
			return actor.OK, nil
		})
	})
}

// TestActorBasic 测试 Actor 的基本功能：spawn、call、post、close。
func TestActorBasic(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "4242"}

	// Post: spawn
	err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 1}})
	if err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Call: Go 从返回值推导 R=TestAddReply，ReqType(TestActorId, *TestAddReply) 确保兼容
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 10})
	if err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if reply.Result != 11 {
		t.Errorf("expected result 11, got %d", reply.Result)
	}

	// Post: logout (fire-and-forget, handler 内部会 post Close)
	err = actor.Post(mgr, testId, &TestLogout{})
	if err != nil {
		t.Fatalf("Post Logout failed: %v", err)
	}

	// 轮询等待 Actor 关闭
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if count, _ := actor.Count[TestActorId](mgr); count == 0 {
			break
		}
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors, got %d", count)
	}
}

// TestActorFinalize 测试 Finalize 广播关闭。
func TestActorFinalize(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	// 创建多个 Actor
	for i := 0; i < 3; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("player_%d", i)}
		err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: i}})
		if err != nil {
			t.Fatalf("Post Login failed: %v", err)
		}
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Errorf("expected 3 actors, got %d", count)
	}

	actor.Finalize(mgr, &TestClose{})

	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after finalize, got %d", count)
	}
}

// TestActorNotFound 测试 Actor 不存在时的错误处理。
func TestActorNotFound(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 99, OpenId: "nonexistent"}

	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error for nonexistent actor")
	}
}

// TestActorBroadcast 测试广播功能。
func TestActorBroadcast(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	for i := 0; i < 5; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("player_%d", i)}
		actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}})
	}
	time.Sleep(100 * time.Millisecond)

	count, _ := actor.Broadcast(mgr, &TestClose{})
	if count != 5 {
		t.Errorf("expected broadcast to 5 actors, got %d", count)
	}
	// 所有 Actor 收到 Close 后应关闭
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if c, _ := actor.Count[TestActorId](mgr); c == 0 {
			break
		}
	}
	if c, _ := actor.Count[TestActorId](mgr); c != 0 {
		t.Errorf("expected 0 actors after broadcast close, got %d", c)
	}
}

// TestActorCallWithinTimeout 测试在超时时间内完成调用的正常路径。
func TestActorCallWithinTimeout(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "timeout_test"}

	// 创建 Actor
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err != nil {
		t.Fatalf("Call with timeout failed: %v", err)
	}
	if reply.Result != 1 {
		t.Errorf("expected result 1, got %d", reply.Result)
	}
}

// TestActorTimer 测试定时器功能。
func TestActorTimer(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			// 设置定时器：50ms 后修改状态
			a.Timer(50*time.Millisecond, func() {
				a.State().Int += 100
			})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	testId := TestActorId{ServerId: 1, OpenId: "timer_test"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 10}})
	time.Sleep(100 * time.Millisecond) // 等待定时器触发

	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Result != 110 {
		t.Errorf("expected timer-triggered result 110, got %d", reply.Result)
	}
}

// TestActorTimerCancel 测试取消定时器。
func TestActorTimerCancel(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			// 设置定时器并立即取消
			timer := a.Timer(50*time.Millisecond, func() {
				a.State().Int += 100
			})
			a.StopTimer(timer)
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	testId := TestActorId{ServerId: 1, OpenId: "timer_cancel"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 10}})
	time.Sleep(100 * time.Millisecond)

	ctx := context.Background()
	reply2, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply2.Result != 10 {
		t.Errorf("expected unchanged result 10 (timer cancelled), got %d", reply2.Result)
	}
}

// TestActorMulticast 测试多播功能。
func TestActorMulticast(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestClose, _ bool) (actor.OkReply, error) {
			a.Quit()
			return actor.OK, nil
		})
	})

	ids := make([]TestActorId, 3)
	for i := 0; i < 3; i++ {
		ids[i] = TestActorId{ServerId: 1, OpenId: fmt.Sprintf("multi_%d", i)}
		actor.Post(mgr, ids[i], &TestLogin{Data: TestActorData{Int: i}})
	}
	time.Sleep(100 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}

	// Multicast close 到指定 Actor
	hit, _ := actor.Multicast(mgr, ids[:2], &TestClose{})
	if hit != 2 {
		t.Errorf("expected multicast to hit 2 actors, got %d", hit)
	}

	// 第 3 个 Actor 未被关闭
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if c, _ := actor.Count[TestActorId](mgr); c == 1 {
			break
		}
	}
	if c, _ := actor.Count[TestActorId](mgr); c != 1 {
		t.Errorf("expected 1 actor remaining, got %d", c)
	}
}

// TestActorRequestSpawn 测试 RegisterServe（首次请求创建 Actor 并返回回复）。
func TestActorRequestSpawn(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterServe(b,
			func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLoginWithReply, spawning bool) (*TestAddReply, error) {
				a.Open() // spawn 后保持活跃（框架不再自动激活）
				a.SetState(TestActorData{Int: req.Data.Int})
				return &TestAddReply{Result: a.State().Int}, nil
			})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			a.State().Int += req.Add
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	testId := TestActorId{ServerId: 1, OpenId: "reqspawn"}

	// 首次 Call 触发 spawn 并返回回复
	ctx := context.Background()
	reply, err := actor.Call(ctx, mgr, testId, &TestLoginWithReply{Data: TestActorData{Int: 42}})
	if err != nil {
		t.Fatalf("Call RequestSpawn failed: %v", err)
	}
	if reply.Result != 42 {
		t.Errorf("expected result 42, got %d", reply.Result)
	}

	// 确认 Actor 已创建，后续 Call 正常
	reply2, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 8})
	if err != nil {
		t.Fatalf("Call Add failed: %v", err)
	}
	if reply2.Result != 50 {
		t.Errorf("expected result 50, got %d", reply2.Result)
	}
}

// TestActorSpawningFlag 测试 spawning 标志的正确性：RegisterServe 首次调用 spawning=true，
// 对已存在 Actor 再次调用同一 handler 时 spawning=false。
func TestActorSpawningFlag(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		// 使用 RegisterServe（allow_spawn=true, allow_query=true）才能让 handler
		// 在 Actor 已存在时也被调用，从而触发 spawning=false 分支。
		actor.RegisterServe(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			if spawning {
				a.Open()                          // spawn 后保持活跃（框架不再自动激活）
				a.SetState(TestActorData{Int: 1}) // spawning=true 时初始化
			} else {
				a.State().Int += 100 // spawning=false 时追加
			}
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	testId := TestActorId{ServerId: 1, OpenId: "spawning"}

	ctx := context.Background()

	// 首次 Call: spawning=true，初始化 state=1
	_, err := actor.Call(ctx, mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	if err != nil {
		t.Fatalf("first Call (spawn) failed: %v", err)
	}
	reply, _ := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if reply.Result != 1 {
		t.Errorf("expected result 1 (spawning=true initialized), got %d", reply.Result)
	}

	// 第二次 Call: spawning=false，追加 100
	_, err = actor.Call(ctx, mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	if err != nil {
		t.Fatalf("second Call (query) failed: %v", err)
	}
	reply2, _ := actor.Call(ctx, mgr, testId, &TestAdd{Add: 0})
	if reply2.Result != 101 {
		t.Errorf("expected result 101 (spawning=false added 100), got %d", reply2.Result)
	}
}

// TestActorCallContextCancel 测试 context 取消时 Call 返回错误。
func TestActorCallContextCancel(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "ctx_cancel"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	_, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error when context is cancelled")
	}
}

// TestActorCallTimeoutExceeded 测试超时发生时 Call 返回错误。
func TestActorCallTimeoutExceeded(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		// 注册一个会延迟的 handler
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			time.Sleep(200 * time.Millisecond) // 慢处理
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	testId := TestActorId{ServerId: 1, OpenId: "timeout_exceed"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	time.Sleep(50 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected timeout error")
	}
}

// TestActorSequentialCalls 测试对同一 Actor 的连续多次 Call。
func TestActorSequentialCalls(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "sequential"}
	actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	for i := 1; i <= 5; i++ {
		reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: i})
		if err != nil {
			t.Fatalf("Call %d failed: %v", i, err)
		}
		expected := i * (i + 1) / 2 // 1+2+...+i
		if reply.Result != expected {
			t.Errorf("Call %d: expected result %d, got %d", i, expected, reply.Result)
		}
	}
}

// ============================================================
// 多 Group 测试类型定义
// ============================================================

type TestActorId2 struct {
	Id int
}

func (id TestActorId2) ActorType() actor.ActorType { return "TestActorId2" }
func (id TestActorId2) String() string             { return fmt.Sprintf("TestActorId2(%d)", id.Id) }

type TestActorData2 struct {
	Value string
}

type TestPingReply struct {
	Echo string
}

type TestPing struct {
	Msg string
}

func (*TestPing) ReqType(_ TestActorId2, _ *TestPingReply) string { return "TestPing" }

type TestReset struct{}

func (*TestReset) ReqType(_ TestActorId2, _ actor.OkReply) string { return "TestReset" }

type TestSpawn2 struct {
	Val string
}

func (*TestSpawn2) ReqType(_ TestActorId2, _ actor.OkReply) string { return "TestSpawn2" }

// TestMultiGroup 测试同一 Manager 管理多个不同 Group。
func TestMultiGroup(t *testing.T) {
	mgr := actor.NewManager()

	// 注册 Group1：TestActorId + TestActorData
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			a.State().Int += req.Add
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	// 注册 Group2：TestActorId2 + TestActorData2
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId2, TestActorData2]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId2, TestActorData2], req *TestSpawn2, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData2{Value: req.Val})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId2, TestActorData2], req *TestPing, _ bool) (*TestPingReply, error) {
			return &TestPingReply{Echo: a.State().Value + ":" + req.Msg}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId2, TestActorData2], req *TestReset, _ bool) (actor.OkReply, error) {
			a.State().Value = ""
			return actor.OK, nil
		})
	})

	// 操作 Group1
	id1 := TestActorId{ServerId: 1, OpenId: "g1"}
	actor.Post(mgr, id1, &TestLogin{Data: TestActorData{Int: 10}})
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	reply1, err := actor.Call(ctx, mgr, id1, &TestAdd{Add: 5})
	if err != nil {
		t.Fatalf("Group1 Call failed: %v", err)
	}
	if reply1.Result != 15 {
		t.Errorf("Group1: expected 15, got %d", reply1.Result)
	}

	// 操作 Group2
	id2 := TestActorId2{Id: 42}
	actor.Post(mgr, id2, &TestSpawn2{Val: "hello"})
	time.Sleep(50 * time.Millisecond)

	reply2, err := actor.Call(ctx, mgr, id2, &TestPing{Msg: "world"})
	if err != nil {
		t.Fatalf("Group2 Call failed: %v", err)
	}
	if reply2.Echo != "hello:world" {
		t.Errorf("Group2: expected 'hello:world', got '%s'", reply2.Echo)
	}

	// 验证两个 Group 独立计数
	if c1, _ := actor.Count[TestActorId](mgr); c1 != 1 {
		t.Errorf("Group1 count: expected 1, got %d", c1)
	}
	if c2, _ := actor.Count[TestActorId2](mgr); c2 != 1 {
		t.Errorf("Group2 count: expected 1, got %d", c2)
	}

	// 关闭 Group2 的 Actor，不影响 Group1
	actor.Call(ctx, mgr, id2, &TestReset{})
	// Group2 的 Actor 仍在（Reset 没关闭它），但 Group1 不受影响
	if c1, _ := actor.Count[TestActorId](mgr); c1 != 1 {
		t.Errorf("Group1 count after Group2 reset: expected 1, got %d", c1)
	}
}

// TestMultiGroupTypeSafety 验证多 Group 下编译期类型安全：错误的类型组合会被编译器拒绝。
// 此测试不包含会编译失败的代码，而是验证正确类型组合能正常工作。
func TestMultiGroupTypeSafety(t *testing.T) {
	mgr := actor.NewManager()

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
	})

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId2, TestActorData2]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId2, TestActorData2], req *TestSpawn2, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData2{Value: req.Val})
			return actor.OK, nil
		})
	})

	// 用 TestActorId 发送 TestLogin 到 Group1 — 正确
	id1 := TestActorId{ServerId: 1, OpenId: "ts"}
	if err := actor.Post(mgr, id1, &TestLogin{Data: TestActorData{Int: 1}}); err != nil {
		t.Fatalf("Post to Group1 failed: %v", err)
	}

	// 用 TestActorId2 发送 TestSpawn2 到 Group2 — 正确
	id2 := TestActorId2{Id: 1}
	if err := actor.Post(mgr, id2, &TestSpawn2{Val: "x"}); err != nil {
		t.Fatalf("Post to Group2 failed: %v", err)
	}

	time.Sleep(50 * time.Millisecond)

	if c1, _ := actor.Count[TestActorId](mgr); c1 != 1 {
		t.Errorf("Group1 count: expected 1, got %d", c1)
	}
	if c2, _ := actor.Count[TestActorId2](mgr); c2 != 1 {
		t.Errorf("Group2 count: expected 1, got %d", c2)
	}
}

// ============================================================
// 退出相关测试
// ============================================================

// TestCloseActorBasic 测试 CloseActor/JoinActor 的返回值：存在返回 true，不存在返回 false。
func TestCloseActorBasic(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "close_basic"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if !actor.CloseActor(mgr, id) {
		t.Error("expected CloseActor to return true for existing actor")
	}
	if !actor.JoinActor(mgr, id) {
		t.Error("expected JoinActor to return true after CloseActor")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after close+join, got %d", count)
	}

	// 不存在的 actor
	missing := TestActorId{ServerId: 99, OpenId: "missing"}
	if actor.CloseActor(mgr, missing) {
		t.Error("expected CloseActor to return false for non-existent actor")
	}
	if actor.JoinActor(mgr, missing) {
		t.Error("expected JoinActor to return false for non-existent actor")
	}
}

// TestKillActorBasic 测试 KillActor 的返回值：存在返回 true，不存在返回 false。
func TestKillActorBasic(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "kill_basic"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if !actor.KillActor(mgr, id) {
		t.Error("expected KillActor to return true for existing actor")
	}
	if !actor.JoinActor(mgr, id) {
		t.Error("expected JoinActor to return true after KillActor")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after kill+join, got %d", count)
	}

	missing := TestActorId{ServerId: 99, OpenId: "missing"}
	if actor.KillActor(mgr, missing) {
		t.Error("expected KillActor to return false for non-existent actor")
	}
}

// TestCloseActorGraceful 测试温和关闭：in-flight handler 不被打断，正常完成。
func TestCloseActorGraceful(t *testing.T) {
	mgr := actor.NewManager()
	handlerStart := make(chan struct{})
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			close(handlerStart)
			time.Sleep(150 * time.Millisecond) // 模拟慢处理
			return &TestAddReply{Result: a.State().Int + req.Add}, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "graceful"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 10}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	type callRes struct {
		reply *TestAddReply
		err   error
	}
	resCh := make(chan callRes, 1)
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 5})
		resCh <- callRes{reply, err}
	}()

	<-handlerStart // 等 handler 开始
	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false for existing actor")
	}

	// 温和关闭不应打断 in-flight handler
	select {
	case r := <-resCh:
		if r.err != nil {
			t.Errorf("in-flight handler should complete, got err: %v", r.err)
		} else if r.reply.Result != 15 {
			t.Errorf("expected result 15, got %d", r.reply.Result)
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not complete in time")
	}

	actor.JoinActor(mgr, id)
}

// TestKillActorInterrupts 测试强制关闭：cancel ctx 中断 in-flight handler 中监听 ctx.Done 的操作。
func TestKillActorInterrupts(t *testing.T) {
	mgr := actor.NewManager()
	handlerStart := make(chan struct{})
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			close(handlerStart)
			select {
			case <-a.Context().Done():
				return nil, a.Context().Err()
			case <-time.After(2 * time.Second):
				return &TestAddReply{Result: a.State().Int}, nil
			}
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "kill_interrupt"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	type callRes struct {
		err error
	}
	resCh := make(chan callRes, 1)
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 1})
		resCh <- callRes{err}
	}()

	<-handlerStart
	start := time.Now()
	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false for existing actor")
	}

	select {
	case r := <-resCh:
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("KillActor should interrupt handler quickly, took %v", elapsed)
		}
		if r.err == nil {
			t.Error("expected handler to return error after kill (ctx canceled)")
		}
	case <-time.After(time.Second):
		t.Error("Call did not return after KillActor")
	}

	actor.JoinActor(mgr, id)
}

// TestCloseActorDrainsMailbox 测试温和关闭后排队消息以 ActorClosedError 失败，
// 而 in-flight handler 仍正常完成。
func TestCloseActorDrainsMailbox(t *testing.T) {
	mgr := actor.NewManager()
	handlerStart := make(chan struct{})
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			close(handlerStart)
			time.Sleep(150 * time.Millisecond) // 第一个 Call 慢，让后续 Call 排队
			a.State().Int += req.Add
			return &TestAddReply{Result: a.State().Int}, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "drain"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	type callRes struct {
		reply *TestAddReply
		err   error
	}
	r1 := make(chan callRes, 1)
	r2 := make(chan callRes, 1)
	r3 := make(chan callRes, 1)

	// #1 进入 in-flight
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 1})
		r1 <- callRes{reply, err}
	}()
	<-handlerStart

	// #2 #3 进入 mailbox 排队
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 10})
		r2 <- callRes{reply, err}
	}()
	go func() {
		reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 100})
		r3 <- callRes{reply, err}
	}()
	time.Sleep(20 * time.Millisecond)

	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false for existing actor")
	}

	// #1 应该正常完成
	select {
	case r := <-r1:
		if r.err != nil {
			t.Errorf("in-flight Call should succeed, got err: %v", r.err)
		} else if r.reply.Result != 1 {
			t.Errorf("expected result 1, got %d", r.reply.Result)
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not complete")
	}

	// #2 #3 应该收到 ActorClosedError（drainMailbox 排空残余消息）
	for i, ch := range []chan callRes{r2, r3} {
		select {
		case r := <-ch:
			if r.err == nil {
				t.Errorf("queued Call #%d should fail, got nil err", i+2)
				continue
			}
			var ace *actor.ActorClosedError
			if !errors.As(r.err, &ace) {
				t.Errorf("queued Call #%d expected ActorClosedError, got %T: %v", i+2, r.err, r.err)
			}
		case <-time.After(time.Second):
			t.Errorf("queued Call #%d did not return", i+2)
		}
	}

	actor.JoinActor(mgr, id)
}

// TestRespawnAfterClose 测试 CloseActor + JoinActor 后可重新 spawn 同一 ID 的 Actor。
func TestRespawnAfterClose(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "respawn"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 100}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false")
	}
	if !actor.JoinActor(mgr, id) {
		t.Fatal("JoinActor returned false")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Fatalf("expected 0 actors after close+join, got %d", count)
	}

	// 重新 spawn 同一 ID，状态应被重置
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 50}}); err != nil {
		t.Fatalf("Post Login for respawn failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("expected 1 actor after respawn, got %d", count)
	}

	reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call after respawn failed: %v", err)
	}
	if reply.Result != 50 {
		t.Errorf("expected state reset to 50, got %d", reply.Result)
	}
}

// TestCloseJoinManager 测试 Manager 级别的关闭与等待：
// IsClosed、CloseManager 幂等、JoinManager 等待 in-flight handler 完成后才返回、退出后新消息失败。
func TestCloseJoinManager(t *testing.T) {
	mgr := actor.NewManager()
	handlerDone := make(chan struct{})

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestClose, _ bool) (actor.OkReply, error) {
			<-handlerDone // 阻塞，模拟 in-flight handler
			a.Quit()
			return actor.OK, nil
		})
	})

	for i := 0; i < 3; i++ {
		id := TestActorId{ServerId: 1, OpenId: fmt.Sprintf("mgr_%d", i)}
		if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: i}}); err != nil {
			t.Fatalf("Post Login failed: %v", err)
		}
	}
	time.Sleep(50 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}
	if mgr.IsClosed() {
		t.Fatal("manager should not be closed initially")
	}

	// 对第一个 actor 发送 Close 请求，handler 会阻塞在 handlerDone 上
	id0 := TestActorId{ServerId: 1, OpenId: "mgr_0"}
	go actor.Call(context.Background(), mgr, id0, &TestClose{})
	time.Sleep(50 * time.Millisecond) // 确保 handler 已进入阻塞

	// 启动 JoinManager goroutine，它应阻塞等待 in-flight handler 完成
	joinDone := make(chan struct{})
	go func() {
		mgr.JoinManager()
		close(joinDone)
	}()

	// JoinManager 不应在 handler 仍阻塞时返回
	time.Sleep(100 * time.Millisecond)
	select {
	case <-joinDone:
		t.Fatal("JoinManager should not return while handler is still in-flight")
	default:
		// 预期行为：仍阻塞
	}

	// 释放 handler，JoinManager 应随后完成
	close(handlerDone)
	select {
	case <-joinDone:
	case <-time.After(2 * time.Second):
		t.Fatal("JoinManager did not return after handler unblocked")
	}

	if !mgr.IsClosed() {
		t.Error("manager should be closed after JoinManager")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after JoinManager, got %d", count)
	}

	// JoinManager 后新消息失败（stopping=true 阻止新请求）
	id := TestActorId{ServerId: 1, OpenId: "after_join"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err == nil {
		t.Error("expected Post to fail after JoinManager")
	}
	ctx := context.Background()
	if _, err := actor.Call(ctx, mgr, id, &TestAdd{Add: 1}); err == nil {
		t.Error("expected Call to fail after JoinManager")
	}

	// 二次 CloseManager/JoinManager 应幂等
	mgr.CloseManager()
	mgr.JoinManager()
}

// TestQuitExitsAtEndOfMessage 测试 Quit（自身发起）：当前消息正常完成，在当前消息结束时退出，
// 退出后后续消息以 ActorClosedError 失败。
func TestQuitExitsAtEndOfMessage(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "quit_self"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 42}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// Call TestLogout：handler 内部调用 a.Quit()，当前消息应正常完成
	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, id, &TestLogout{})
	if err != nil {
		t.Fatalf("Quit handler should complete normally, got: %v", err)
	}

	// Quit 在当前消息结束后退出，后续消息应失败
	_, err = actor.Call(ctx, mgr, id, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error for message after Quit")
	}

	// Quit 是自身发起的退出，在第一次 Call 返回后 run loop 已开始清理流程。
	// 先检查 Count：无论 run loop 清理到哪一步，closed=true 的 actor 已不被计数。
	// 再 JoinActor：无论返回 true（还在 map 中，等待 doneCh）还是 false（已从 map 移除），
	// 只要不卡死就说明 run goroutine 已退出。
	for i := 0; i < 20; i++ {
		time.Sleep(50 * time.Millisecond)
		if count, _ := actor.Count[TestActorId](mgr); count == 0 {
			break
		}
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after Quit, got %d", count)
	}
	actor.JoinActor(mgr, id) // 不应卡死，返回值无所谓
}

// TestKillDrainsMailbox 测试 Kill（外部强制关闭）：中断 in-flight handler，
// 并排空 mailbox 中排队消息以 ActorClosedError 失败。
func TestKillDrainsMailbox(t *testing.T) {
	mgr := actor.NewManager()
	handlerStart := make(chan struct{})
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			close(handlerStart)
			select {
			case <-a.Context().Done():
				return nil, a.Context().Err()
			case <-time.After(2 * time.Second):
				return &TestAddReply{Result: a.State().Int}, nil
			}
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "kill_drain"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	type callRes struct {
		err error
	}
	r1 := make(chan callRes, 1)
	r2 := make(chan callRes, 1)
	r3 := make(chan callRes, 1)

	// #1 进入 in-flight
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 1})
		r1 <- callRes{err}
	}()
	<-handlerStart

	// #2 #3 进入 mailbox 排队
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 10})
		r2 <- callRes{err}
	}()
	go func() {
		_, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 100})
		r3 <- callRes{err}
	}()
	time.Sleep(20 * time.Millisecond)

	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false for existing actor")
	}

	// #1 应被 Kill 中断（ctx.Done 触发）
	select {
	case r := <-r1:
		if r.err == nil {
			t.Error("in-flight Call should be interrupted by Kill")
		}
	case <-time.After(time.Second):
		t.Error("in-flight Call did not return after Kill")
	}

	// #2 #3 应收到 ActorClosedError（drainMailbox 排空）
	for i, ch := range []chan callRes{r2, r3} {
		select {
		case r := <-ch:
			if r.err == nil {
				t.Errorf("queued Call #%d should fail after Kill, got nil err", i+2)
				continue
			}
			var ace *actor.ActorClosedError
			if !errors.As(r.err, &ace) {
				t.Errorf("queued Call #%d expected ActorClosedError, got %T: %v", i+2, r.err, r.err)
			}
		case <-time.After(time.Second):
			t.Errorf("queued Call #%d did not return", i+2)
		}
	}

	actor.JoinActor(mgr, id)
}

// TestTimerCancelledOnQuit 测试自身退出（Quit）时定时器被取消，回调不触发。
func TestTimerCancelledOnQuit(t *testing.T) {
	var timerFired atomic.Bool
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		// handler 设置定时器后调用 Quit，定时器应被取消
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogout, _ bool) (actor.OkReply, error) {
			a.Timer(100*time.Millisecond, func() {
				timerFired.Store(true)
			})
			a.Quit()
			return actor.OK, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_quit"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 调用 Quit handler
	_, err := actor.Call(context.Background(), mgr, id, &TestLogout{})
	if err != nil {
		t.Fatalf("Quit handler should succeed: %v", err)
	}

	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间后，验证定时器未触发
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor quits, but it fired")
	}
}

// TestTimerCancelledOnClose 测试外部温和关闭（CloseActor）时定时器被取消，回调不触发。
func TestTimerCancelledOnClose(t *testing.T) {
	var timerFired atomic.Bool
	spawnDone := make(chan struct{})
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			a.Timer(100*time.Millisecond, func() {
				timerFired.Store(true)
			})
			close(spawnDone)
			return actor.OK, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_close"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	<-spawnDone // 确认 handler 已执行并设置定时器

	// 外部温和关闭
	if !actor.CloseActor(mgr, id) {
		t.Fatal("CloseActor returned false")
	}
	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor is closed, but it fired")
	}
}

// TestTimerCancelledOnKill 测试外部强制关闭（KillActor）时定时器被取消，回调不触发。
func TestTimerCancelledOnKill(t *testing.T) {
	var timerFired atomic.Bool
	spawnDone := make(chan struct{})
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			a.Timer(100*time.Millisecond, func() {
				timerFired.Store(true)
			})
			close(spawnDone)
			return actor.OK, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "timer_kill"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	<-spawnDone // 确认 handler 已执行并设置定时器

	// 强制关闭：cancel ctx 应立即取消定时器
	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false")
	}
	actor.JoinActor(mgr, id)

	// 等待定时器应触发的时间
	time.Sleep(200 * time.Millisecond)
	if timerFired.Load() {
		t.Error("timer should be cancelled when actor is killed, but it fired")
	}
}

// ============================================================
// 补充测试：Handler Panic、并发、未注册 Group、Kill 后 Respawn、空 Multicast、Finalize In-Flight
// ============================================================

// TestActorHandlerPanic 测试 handler 中发生 panic 时，Call 能收到错误而非让 actor 崩溃。
func TestActorHandlerPanic(t *testing.T) {
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd, _ bool) (*TestAddReply, error) {
			panic("test panic in handler")
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "panic_test"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	_, err := actor.Call(ctx, mgr, id, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error when handler panics")
		return
	}
	var hce *actor.HandlerCallError
	if !errors.As(err, &hce) {
		t.Errorf("expected HandlerCallError, got %T: %v", err, err)
	}

	// Actor 应仍存活（panic 仅影响单次调用）
	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("actor should survive handler panic, expected 1 actor, got %d", count)
	}
}

// TestActorConcurrentCalls 测试并发 Call 的串行化保证：
// 100 个 goroutine 同时 Call 同一 Actor，结果应为 1..100 不重不丢。
func TestActorConcurrentCalls(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	testId := TestActorId{ServerId: 42, OpenId: "concurrent"}
	if err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	ctx := context.Background()
	const N = 100
	var wg sync.WaitGroup
	results := make([]int, N)

	for i := 0; i < N; i++ {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			reply, err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
			if err != nil {
				t.Errorf("concurrent Call %d failed: %v", idx, err)
				return
			}
			results[idx] = reply.Result
		}(i)
	}
	wg.Wait()

	// 验证串行化：100 次 Call 各 +1，结果集应为 {1,2,...,100}
	seen := make(map[int]bool, N)
	for _, r := range results {
		if r < 1 || r > N {
			t.Errorf("result %d out of range [1, %d]", r, N)
		}
		if seen[r] {
			t.Errorf("duplicate result %d — actor serialization broken", r)
		}
		seen[r] = true
	}
	if len(seen) != N {
		t.Errorf("expected %d unique results, got %d", N, len(seen))
	}
}

// TestActorGroupNotFound 测试向未注册 Group 发送消息时的错误处理。
func TestActorGroupNotFound(t *testing.T) {
	mgr := actor.NewManager()
	// 不注册任何 Group，所有操作应返回 GroupNotFoundError

	testId := TestActorId{ServerId: 1, OpenId: "no_group"}

	// Post 应失败
	err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 0}})
	if err == nil {
		t.Error("expected error when posting to unregistered group")
	}
	var gnf *actor.GroupNotFoundError
	if !errors.As(err, &gnf) {
		t.Errorf("expected GroupNotFoundError, got %T: %v", err, err)
	}

	// Call 也应失败
	ctx := context.Background()
	_, err = actor.Call(ctx, mgr, testId, &TestAdd{Add: 1})
	if err == nil {
		t.Error("expected error when calling unregistered group")
	}

	// Count 应返回错误
	_, err = actor.Count[TestActorId](mgr)
	if err == nil {
		t.Error("expected error when counting unregistered group")
	}

	// Broadcast 应返回 0
	n, _ := actor.Broadcast(mgr, &TestClose{})
	if n != 0 {
		t.Errorf("expected 0 broadcast hits for unregistered group, got %d", n)
	}

	// Multicast 应返回 0
	n, _ = actor.Multicast(mgr, []TestActorId{testId}, &TestClose{})
	if n != 0 {
		t.Errorf("expected 0 multicast hits for unregistered group, got %d", n)
	}
}

// TestActorRespawnAfterKill 测试 KillActor + JoinActor 后可重新 spawn 同一 ID 的 Actor。
func TestActorRespawnAfterKill(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	id := TestActorId{ServerId: 1, OpenId: "respawn_kill"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 100}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if !actor.KillActor(mgr, id) {
		t.Fatal("KillActor returned false")
	}
	if !actor.JoinActor(mgr, id) {
		t.Fatal("JoinActor returned false")
	}
	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Fatalf("expected 0 actors after kill+join, got %d", count)
	}

	// 重新 spawn 同一 ID
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 50}}); err != nil {
		t.Fatalf("Post Login for respawn failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	if count, _ := actor.Count[TestActorId](mgr); count != 1 {
		t.Errorf("expected 1 actor after respawn, got %d", count)
	}

	reply, err := actor.Call(context.Background(), mgr, id, &TestAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call after respawn failed: %v", err)
	}
	if reply.Result != 50 {
		t.Errorf("expected state reset to 50, got %d", reply.Result)
	}
}

// TestActorEmptyMulticast 测试空 ID 列表的 Multicast 返回 (0, nil)。
func TestActorEmptyMulticast(t *testing.T) {
	mgr := actor.NewManager()
	setupManager(mgr)

	hit, err := actor.Multicast(mgr, []TestActorId{}, &TestClose{})
	if err != nil {
		t.Errorf("expected no error for empty multicast, got: %v", err)
	}
	if hit != 0 {
		t.Errorf("expected 0 hits for empty multicast, got %d", hit)
	}
}

// TestActorFinalizeWithInFlight 测试 Finalize 在 Actor 有 in-flight handler 时正确等待。
func TestActorFinalizeWithInFlight(t *testing.T) {
	mgr := actor.NewManager()
	handlerStart := make(chan struct{})
	handlerDone := make(chan struct{})

	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestActorData{Int: req.Data.Int})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestActorId, TestActorData], req *TestClose, _ bool) (actor.OkReply, error) {
			close(handlerStart)
			<-handlerDone // 阻塞，模拟慢 handler
			a.Quit()
			return actor.OK, nil
		})
	})

	id := TestActorId{ServerId: 1, OpenId: "fin_inflight"}
	if err := actor.Post(mgr, id, &TestLogin{Data: TestActorData{Int: 0}}); err != nil {
		t.Fatalf("Post Login failed: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	// 发送会阻塞的请求
	go actor.Call(context.Background(), mgr, id, &TestClose{})
	<-handlerStart // 确认 handler 进入阻塞

	// 在另一个 goroutine 中调用 Finalize，应阻塞等待 handler 完成
	finalizeDone := make(chan struct{})
	go func() {
		actor.Finalize(mgr, &TestClose{})
		close(finalizeDone)
	}()

	// Finalize 不应在 handler 仍阻塞时返回
	time.Sleep(100 * time.Millisecond)
	select {
	case <-finalizeDone:
		t.Fatal("Finalize should not return while handler is still in-flight")
	default:
		// 预期行为
	}

	// 释放 handler，Finalize 应完成
	close(handlerDone)
	select {
	case <-finalizeDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Finalize did not return after handler unblocked")
	}

	if count, _ := actor.Count[TestActorId](mgr); count != 0 {
		t.Errorf("expected 0 actors after finalize, got %d", count)
	}
}

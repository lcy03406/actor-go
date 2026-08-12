package actor_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/internal/testutil"
)

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

func (req *TestSpawn2) Handle(a *actor.ActorContext[TestActorId2, TestActorData2], _ bool) (actor.OkReply, error) {
	a.Open()
	a.SetState(TestActorData2{Value: req.Val})
	return actor.OK, nil
}

func (req *TestPing) Handle(a *actor.ActorContext[TestActorId2, TestActorData2], _ bool) (*TestPingReply, error) {
	return &TestPingReply{Echo: a.State().Value + ":" + req.Msg}, nil
}

func (req *TestReset) Handle(a *actor.ActorContext[TestActorId2, TestActorData2], _ bool) (actor.OkReply, error) {
	a.State().Value = ""
	return actor.OK, nil
}

// TestMultiGroup 测试同一 Manager 管理多个不同 Group。
func TestMultiGroup(t *testing.T) {
	mgr := actor.NewManager()

	// 注册 Group1：TestActorId + TestActorData
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
		actor.RegisterQueryHandler[TestActorId, TestActorData, *TestAdd](b)
	})

	// 注册 Group2：TestActorId2 + TestActorData2
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId2, TestActorData2]) {
		actor.RegisterSpawnHandler[TestActorId2, TestActorData2, *TestSpawn2](b)
		actor.RegisterQueryHandler[TestActorId2, TestActorData2, *TestPing](b)
		actor.RegisterQueryHandler[TestActorId2, TestActorData2, *TestReset](b)
	})

	// 操作 Group1
	id1 := TestActorId{ServerId: 1, OpenId: "g1"}
	actor.Post(mgr, id1, &TestLogin{Data: TestActorData{Int: 10}})
	testutil.Settle()

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
	testutil.Settle()

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

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterSpawnHandler[TestActorId, TestActorData, *TestLogin](b)
	})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId2, TestActorData2]) {
		actor.RegisterSpawnHandler[TestActorId2, TestActorData2, *TestSpawn2](b)
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

	testutil.Settle()

	if c1, _ := actor.Count[TestActorId](mgr); c1 != 1 {
		t.Errorf("Group1 count: expected 1, got %d", c1)
	}
	if c2, _ := actor.Count[TestActorId2](mgr); c2 != 1 {
		t.Errorf("Group2 count: expected 1, got %d", c2)
	}
}

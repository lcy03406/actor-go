package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// 类型定义
// ============================================================

type TestActorId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id TestActorId) ActorType() string { return "TestActorId" }
func (id TestActorId) String() string {
	return fmt.Sprintf("TestActorId(%d,%s)", id.ServerId, id.OpenId)
}

type TestActorData struct {
	Int int `json:"int"`
}

type TestAddReply struct {
	Result int `json:"result"`
}

// 请求类型 reqType 字符串常量（仅 RegisterPost 需要）
const ReqTypeTestLogin = "TestLogin"

// 请求类型：实现 actor.Request[TestActorId, R] 接口
// ReqType 的参数类型 (A, *R) 确保编译器能检查 Q 与 A、R 的匹配关系

type TestLogin struct {
	Data TestActorData `json:"data"`
}

func (*TestLogin) ReqType(_ TestActorId, _ *actor.OkReply) string { return ReqTypeTestLogin }

type TestLogout struct{}

func (*TestLogout) ReqType(_ TestActorId, _ *actor.OkReply) string { return "TestLogout" }

type TestClose struct{}

func (*TestClose) ReqType(_ TestActorId, _ *actor.OkReply) string { return "TestClose" }

type TestAdd struct {
	Add int `json:"add"`
}

func (*TestAdd) ReqType(_ TestActorId, _ *TestAddReply) string { return "TestAdd" }

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug})))

	fmt.Println("=== Actor Model Demo (Typed) ===")
	fmt.Println()

	// 创建 Manager（可容纳多个 Group）
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		// spawn: 首次消息创建 Actor，ReqType 由 Q 自动推导
		actor.RegisterSpawn(b,
			func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogin, spawning bool) (actor.OkReply, error) {
				a.SetState(&TestActorData{Int: req.Data.Int})
				a.Logger().Info("Login success", "data", *a.State())
				return actor.OkReply{}, nil
			})

		// request: handler 拿到带类型的 State，无需类型断言
		actor.RegisterRequest(b,
			func(a *actor.ActorContext[TestActorId, TestActorData], req *TestAdd) (TestAddReply, error) {
				a.State().Int += req.Add
				a.Logger().Info("Add", "add", req.Add, "result", a.State().Int)
				return TestAddReply{Result: a.State().Int}, nil
			})

		actor.RegisterRequest(b,
			func(a *actor.ActorContext[TestActorId, TestActorData], req *TestLogout) (actor.OkReply, error) {
				a.Logger().Info("Logout")
				a.Post(&TestClose{})
				return actor.OkReply{}, nil
			})

		actor.RegisterRequest(b,
			func(a *actor.ActorContext[TestActorId, TestActorData], req *TestClose) (actor.OkReply, error) {
				a.Close()
				return actor.OkReply{}, nil
			})
	})

	testId := TestActorId{ServerId: 42, OpenId: "player_4242"}
	ctx := context.Background()

	// Post: spawn（无需传 reqType 字符串，由 ReqType 方法自动推导）
	fmt.Println("--- Post Login ---")
	if err := actor.Post(mgr, testId, &TestLogin{Data: TestActorData{Int: 1}}); err != nil {
		fmt.Printf("Post Login error: %v\n", err)
	}
	time.Sleep(100 * time.Millisecond)

	// Call: Go 从 &reply 推导 R=TestAddReply
	// ReqType(TestActorId, *TestAddReply) 方法签名确保编译期类型匹配
	fmt.Println("--- Call Add ---")
	var reply TestAddReply
	if err := actor.Call(ctx, mgr, testId, &TestAdd{Add: 10}, &reply); err != nil {
		fmt.Printf("Call Add error: %v\n", err)
	} else {
		fmt.Printf("Add result: %d (type: %T)\n", reply.Result, reply)
	}

	// Post: logout（fire-and-forget）
	fmt.Println("--- Post Logout ---")
	if err := actor.Post(mgr, testId, &TestLogout{}); err != nil {
		fmt.Printf("Post Logout error: %v\n", err)
	}
	time.Sleep(200 * time.Millisecond)

	fmt.Printf("Actor count: %d (expected: 0)\n", actor.Count[TestActorId](mgr))
	fmt.Println()
	fmt.Println("=== Demo Complete ===")
}

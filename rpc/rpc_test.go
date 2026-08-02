package rpc_test

import (
	"context"
	"encoding/json"
	"fmt"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
	"github.com/lcy03406/actor-go/rpc"
)

// ============================================================
// 便捷类型别名
// ============================================================

type testServer = rpc.Server[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
type testClient = rpc.Client[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport]
type testRegBuilder = rpc.RegistryBuilder[json.RawMessage, rpc.JsonCodec]

// ============================================================
// 测试类型定义
// ============================================================

type TestRpcId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id TestRpcId) ActorType() actor.ActorType { return "TestRpc" }
func (id TestRpcId) String() string             { return fmt.Sprintf("TestRpc(%d,%s)", id.ServerId, id.OpenId) }

type TestRpcState struct {
	Value int
}

type TestRpcLogin struct {
	Init int `json:"init"`
}

func (*TestRpcLogin) ReqType(_ TestRpcId, _ actor.OkReply) string { return "TestRpcLogin" }

type TestRpcLoginWithReply struct {
	Init int `json:"init"`
}

func (*TestRpcLoginWithReply) ReqType(_ TestRpcId, _ *TestRpcAddReply) string {
	return "TestRpcLoginWithReply"
}

type TestRpcAdd struct {
	Add int `json:"add"`
}

type TestRpcAddReply struct {
	Result int `json:"result"`
}

func (*TestRpcAdd) ReqType(_ TestRpcId, _ *TestRpcAddReply) string { return "TestRpcAdd" }

type TestRpcClose struct{}

func (*TestRpcClose) ReqType(_ TestRpcId, _ actor.OkReply) string { return "TestRpcClose" }

// ============================================================
// 测试辅助函数
// ============================================================

// setupRPCServer 启动 RPC 服务端，返回服务端、Manager 和实际端口。
func setupRPCServer(t *testing.T) (*testServer, *actor.Manager, int) {
	t.Helper()

	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestRpcId, TestRpcState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestRpcState{Value: req.Init})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcAdd, spawning bool) (*TestRpcAddReply, error) {
			state := a.State()
			state.Value += req.Add
			return &TestRpcAddReply{Result: state.Value}, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcClose, spawning bool) (actor.OkReply, error) {
			a.Quit()
			return actor.OK, nil
		})
	})

	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		"localhost:0",
		mgr,
		func(b *testRegBuilder) {
			rpc.RegisterRequest(b, &TestRpcLogin{})
			rpc.RegisterRequest(b, &TestRpcAdd{})
			rpc.RegisterRequest(b, &TestRpcClose{})
		},
	)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}

	// 等待服务端就绪
	time.Sleep(300 * time.Millisecond)

	_, port, _ := net.SplitHostPort(server.Addr())
	p, _ := strconv.Atoi(port)
	return server, mgr, p
}

// setupRPCClient 创建并连接 RPC 客户端。
func setupRPCClient(t *testing.T, port int) *testClient {
	t.Helper()

	client := rpc.NewClient[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		fmt.Sprintf("localhost:%d", port),
	)
	if err := client.Connect(); err != nil {
		t.Fatalf("failed to connect client: %v", err)
	}
	return client
}

// ============================================================
// 测试用例
// ============================================================

// TestRPCPost 测试远程 Post（fire-and-forget）。
func TestRPCPost(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	t.Log("client connected, about to Post")

	id := TestRpcId{ServerId: 1, OpenId: "post_test"}
	if err := rpc.Post(client, id, &TestRpcLogin{Init: 42}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}

	t.Log("Post done, waiting 300ms")

	time.Sleep(300 * time.Millisecond)

	t.Log("about to Call")

	// 通过 Call 验证 Actor 已创建
	ctx := context.Background()
	reply, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Result != 42 {
		t.Errorf("expected 42, got %d", reply.Result)
	}
}

// TestRPCCall 测试远程 Call（请求-回复）。
func TestRPCCall(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	id := TestRpcId{ServerId: 1, OpenId: "call_test"}

	// Post 创建 Actor
	if err := rpc.Post(client, id, &TestRpcLogin{Init: 10}); err != nil {
		t.Fatalf("Post failed: %v", err)
	}
	time.Sleep(300 * time.Millisecond)

	// Call 发起请求并等待回复
	ctx := context.Background()
	reply, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 5})
	if err != nil {
		t.Fatalf("Call failed: %v", err)
	}
	if reply.Result != 15 {
		t.Errorf("expected 15, got %d", reply.Result)
	}

	// 再次 Call
	reply2, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 3})
	if err != nil {
		t.Fatalf("Call 2 failed: %v", err)
	}
	if reply2.Result != 18 {
		t.Errorf("expected 18, got %d", reply2.Result)
	}
}

// TestRPCCallSpawn 测试远程 Call 触发 spawn（RequestSpawn 模式）。
func TestRPCCallSpawn(t *testing.T) {
	// 使用带 RequestSpawn 的服务端
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestRpcId, TestRpcState]) {
		actor.RegisterServe(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcLoginWithReply, spawning bool) (*TestRpcAddReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestRpcState{Value: req.Init})
			return &TestRpcAddReply{Result: a.State().Value}, nil
		})
	})

	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		"localhost:0",
		mgr,
		func(b *testRegBuilder) {
			rpc.RegisterRequest(b, &TestRpcLoginWithReply{})
		},
	)
	server.Start()
	defer server.Shutdown(context.Background())
	time.Sleep(300 * time.Millisecond)

	_, port, _ := net.SplitHostPort(server.Addr())
	p, _ := strconv.Atoi(port)
	client := setupRPCClient(t, p)
	defer client.Close()

	// 首次 Call 触发 spawn + 返回回复
	id := TestRpcId{ServerId: 1, OpenId: "spawn_call"}
	ctx := context.Background()
	reply, err := rpc.Call(ctx, client, id, &TestRpcLoginWithReply{Init: 99})
	if err != nil {
		t.Fatalf("Call spawn failed: %v", err)
	}
	if reply.Result != 99 {
		t.Errorf("expected 99, got %d", reply.Result)
	}
}

// TestRPCCallTimeout 测试远程 Call 在正常超时时间内完成。
func TestRPCCallTimeout(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	id := TestRpcId{ServerId: 1, OpenId: "timeout_test"}
	rpc.Post(client, id, &TestRpcLogin{Init: 0})
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 1})
	if err != nil {
		t.Fatalf("Call with timeout failed: %v", err)
	}
}

// TestRPCCallTimeoutExceeded 测试超时发生时 Call 返回错误。
func TestRPCCallTimeoutExceeded(t *testing.T) {
	// 使用带延迟 handler 的服务端
	mgr := actor.NewManager()
	actor.Serve(mgr, 100, func(b *actor.RegistryBuilder[TestRpcId, TestRpcState]) {
		actor.RegisterSpawn(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcLogin, spawning bool) (actor.OkReply, error) {
			a.Open() // spawn 后保持活跃（框架不再自动激活）
			a.SetState(TestRpcState{Value: req.Init})
			return actor.OK, nil
		})
		actor.RegisterQuery(b, func(a *actor.ActorContext[TestRpcId, TestRpcState], req *TestRpcAdd, spawning bool) (*TestRpcAddReply, error) {
			time.Sleep(200 * time.Millisecond)
			return &TestRpcAddReply{Result: a.State().Value}, nil
		})
	})

	server := rpc.NewServer[json.RawMessage, rpc.JsonCodec, rpc.JsonTransport](
		"localhost:0",
		mgr,
		func(b *testRegBuilder) {
			rpc.RegisterRequest(b, &TestRpcLogin{})
			rpc.RegisterRequest(b, &TestRpcAdd{})
		},
	)
	server.Start()
	defer server.Shutdown(context.Background())
	time.Sleep(300 * time.Millisecond)

	_, port, _ := net.SplitHostPort(server.Addr())
	p, _ := strconv.Atoi(port)
	client := setupRPCClient(t, p)
	defer client.Close()

	id := TestRpcId{ServerId: 1, OpenId: "timeout_exceed"}
	rpc.Post(client, id, &TestRpcLogin{Init: 0})
	time.Sleep(300 * time.Millisecond)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	_, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 1})
	if err == nil {
		t.Error("expected timeout error")
	}
}

// TestRPCBroadcast 测试远程广播。
func TestRPCBroadcast(t *testing.T) {
	server, mgr, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	// 创建多个 Actor
	for i := 0; i < 3; i++ {
		id := TestRpcId{ServerId: 1, OpenId: fmt.Sprintf("bc_%d", i)}
		if err := rpc.Post(client, id, &TestRpcLogin{Init: i}); err != nil {
			t.Fatalf("Post %d failed: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	count, err := actor.Count[TestRpcId](mgr)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 3 {
		t.Fatalf("expected 3 actors, got %d", count)
	}

	// 广播关闭
	if err := rpc.Broadcast(client, &TestRpcClose{}); err != nil {
		t.Fatalf("Broadcast failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// 验证所有 Actor 已关闭
	count, err = actor.Count[TestRpcId](mgr)
	if err != nil {
		t.Fatalf("Count failed: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 actors after broadcast, got %d", count)
	}
}

// TestRPCMulticast 测试远程多播。
func TestRPCMulticast(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	ids := make([]TestRpcId, 3)
	for i := 0; i < 3; i++ {
		ids[i] = TestRpcId{ServerId: 1, OpenId: fmt.Sprintf("mc_%d", i)}
		if err := rpc.Post(client, ids[i], &TestRpcLogin{Init: i}); err != nil {
			t.Fatalf("Post %d failed: %v", i, err)
		}
	}
	time.Sleep(300 * time.Millisecond)

	// 多播关闭到前 2 个
	if err := rpc.Multicast(client, ids[:2], &TestRpcClose{}); err != nil {
		t.Fatalf("Multicast failed: %v", err)
	}
	time.Sleep(200 * time.Millisecond)

	// 验证第 3 个未被关闭
	ctx := context.Background()
	reply, err := rpc.Call(ctx, client, ids[2], &TestRpcAdd{Add: 0})
	if err != nil {
		t.Fatalf("Call to surviving actor failed: %v", err)
	}
	if reply.Result != 2 {
		t.Errorf("expected 2, got %d", reply.Result)
	}
}

// TestRPCNotFound 测试远程 Actor 不存在时的错误处理。
func TestRPCNotFound(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	// 不存在的 Actor
	id := TestRpcId{ServerId: 99, OpenId: "nonexistent"}
	ctx := context.Background()
	_, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 1})
	if err == nil {
		t.Error("expected error for nonexistent actor")
	}
}

// TestRPCClientClose 测试客户端关闭后通知 pending call。
func TestRPCClientClose(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	id := TestRpcId{ServerId: 1, OpenId: "close_test"}
	rpc.Post(client, id, &TestRpcLogin{Init: 0})
	time.Sleep(300 * time.Millisecond)

	// 关闭客户端，pending call 应收到错误
	client.Close()

	ctx := context.Background()
	_, err := rpc.Call(ctx, client, id, &TestRpcAdd{Add: 1})
	if err == nil {
		t.Error("expected error after client close")
	}
}

// TestRPCCallTimeoutHelper 测试 CallTimeout 便捷函数。
func TestRPCCallTimeoutHelper(t *testing.T) {
	server, _, port := setupRPCServer(t)
	defer server.Shutdown(context.Background())

	client := setupRPCClient(t, port)
	defer client.Close()

	id := TestRpcId{ServerId: 1, OpenId: "ct_helper"}
	rpc.Post(client, id, &TestRpcLogin{Init: 7})
	time.Sleep(300 * time.Millisecond)

	reply, err := rpc.CallTimeout(context.Background(), client, id, &TestRpcAdd{Add: 3}, 5*time.Second)
	if err != nil {
		t.Fatalf("CallTimeout failed: %v", err)
	}
	if reply.Result != 10 {
		t.Errorf("expected 10, got %d", reply.Result)
	}
}

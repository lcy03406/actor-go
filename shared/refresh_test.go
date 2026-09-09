package shared

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ---------- 写方(board) ----------

type boardId struct{ ID string }

func (id boardId) ActorType() actor.ActorType { return "shared_test_board" }
func (id boardId) String() string             { return "board-" + id.ID }

type boardState struct {
	Data Versioned[readerId, string] // 数据 + 版本 + 读者注册表,零值可用
}

// ---------- 读者(reader) ----------

type readerId struct{ ID string }

func (id readerId) ActorType() actor.ActorType { return "shared_test_reader" }
func (id readerId) String() string             { return "reader-" + id.ID }

type readerState struct {
	Board Cache[readerId, boardId, readerState, string] // 首次 Get 按需订阅,零值可用
}

// read 读者读取:首次 Get 自动向写方注册并取当前快照,此后命中本地缓存。
type read struct{}

type readReply struct {
	Data    string
	Version int64
}

func (*read) ReqType(_ readerId, _ *readReply) string { return "Read" }
func (req *read) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*readReply, error) {
	if spawning {
		ctx.Open()
	}
	data, err := ctx.State().Board.Get(ctx, boardId{ID: "b1"})
	if err != nil {
		return nil, err
	}
	return &readReply{Data: data, Version: ctx.State().Board.Version()}, nil
}

// peek 暴露缓存内部状态,供断言。
type peek struct{}

type peekReply struct {
	Loaded  bool
	Version int64
	Data    string
}

func (*peek) ReqType(_ readerId, _ *peekReply) string { return "Peek" }
func (req *peek) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*peekReply, error) {
	return &peekReply{
		Loaded:  !ctx.State().Board.Stale(),
		Version: ctx.State().Board.Version(),
		Data:    ctx.State().Board.Data(),
	}, nil
}

// ---------- 脚手架 ----------

func newSharedTestManager(t *testing.T, cooldown time.Duration) *actor.Manager {
	t.Helper()
	mgr := actor.NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)))
	opts := actor.Options{BufMails: 100}

	actor.Serve(mgr, opts, func(b *actor.RegistryBuilder[boardId, boardState]) {
		ServeWriter(b, func(s *boardState) *Versioned[readerId, string] {
			return &s.Data
		}, WithCooldown(cooldown))
	})

	actor.Serve(mgr, opts, func(b *actor.RegistryBuilder[readerId, readerState]) {
		RegisterRefresh(b, func(s *readerState) *Cache[readerId, boardId, readerState, string] {
			return &s.Board
		})
		actor.RegisterServeHandler2(b, (*read)(nil))
		actor.RegisterQueryHandler2(b, (*peek)(nil))
	})

	t.Cleanup(func() {
		mgr.CloseManager()
		mgr.JoinManager()
	})
	return mgr
}

// mustWrite 通过标准 Write 消息写入:更新数据 + 版本推进 + 冷却推送一步完成。
// 使用 Call 保证写 handler(含刷新推送入队)完成后才返回,后续断言严格有序。
func mustWrite(t *testing.T, mgr *actor.Manager, board boardId, data string) {
	t.Helper()
	if _, err := actor.Call[boardId](context.Background(), mgr, board, &Write[boardId, string]{Data: data}); err != nil {
		t.Fatalf("Write(%q): %v", data, err)
	}
}

func mustRead(t *testing.T, mgr *actor.Manager, reader readerId) readReply {
	t.Helper()
	rep, err := actor.Call[readerId](context.Background(), mgr, reader, &read{})
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	return *rep
}

func mustPeek(t *testing.T, mgr *actor.Manager, reader readerId) peekReply {
	t.Helper()
	rep, err := actor.Call[readerId](context.Background(), mgr, reader, &peek{})
	if err != nil {
		t.Fatalf("Peek: %v", err)
	}
	return *rep
}

// waitPeekVersion 轮询等待读者缓存推进到目标版本(冷却定时推送后收敛)。
func waitPeekVersion(t *testing.T, mgr *actor.Manager, reader readerId, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p := mustPeek(t, mgr, reader); p.Version == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("waitPeekVersion: reader %s not reached version %d", reader, want)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// ---------- 用例 ----------

// TestShared_SubscribeOnDemandAndPush 验证:首次读取按需注册并取当前快照,
// 后续写入由框架自动推送(带数据),读取不再触网。
func TestShared_SubscribeOnDemandAndPush(t *testing.T) {
	mgr := newSharedTestManager(t, 0) // 无冷却:每次写入立即推送
	board := boardId{ID: "b1"}
	reader := readerId{ID: "r1"}

	mustWrite(t, mgr, board, "v1")

	// 首次读取:按需注册(Subscribe)→ 取当前快照
	r := mustRead(t, mgr, reader)
	if r.Data != "v1" || r.Version != 1 {
		t.Fatalf("first read: want data=v1 version=1, got %+v", r)
	}

	// 写 v2:框架自动向已注册读者推送 Refresh(携带数据)
	mustWrite(t, mgr, board, "v2")

	// FIFO:Peek 在 Refresh 之后处理 → 缓存已被推送更新,无需再拉取
	p := mustPeek(t, mgr, reader)
	if !p.Loaded || p.Version != 2 || p.Data != "v2" {
		t.Fatalf("after write v2: want pushed v2, got %+v", p)
	}

	// 读取:直接命中本地缓存
	r = mustRead(t, mgr, reader)
	if r.Data != "v2" || r.Version != 2 {
		t.Fatalf("cached read: want data=v2 version=2, got %+v", r)
	}
}

// TestShared_CooldownCoalescesWrites 验证冷却期内多次写入合并为一次推送。
func TestShared_CooldownCoalescesWrites(t *testing.T) {
	mgr := newSharedTestManager(t, 200*time.Millisecond)
	board := boardId{ID: "b1"}
	reader := readerId{ID: "r1"}

	mustWrite(t, mgr, board, "v1")
	mustRead(t, mgr, reader) // 订阅,缓存 v1

	// 冷却期外写入:立即推送
	mustWrite(t, mgr, board, "v2")
	if p := mustPeek(t, mgr, reader); p.Version != 2 {
		t.Fatalf("after v2: want pushed immediately, got %+v", p)
	}

	// 冷却期内连续写入 v3、v4:合并,不逐条推送
	mustWrite(t, mgr, board, "v3")
	mustWrite(t, mgr, board, "v4")
	if p := mustPeek(t, mgr, reader); p.Version != 2 {
		t.Fatalf("within cooldown: want still v2, got %+v", p)
	}

	// 冷却结束后:合并推送最新 v4
	waitPeekVersion(t, mgr, reader, 4)
}

// TestShared_OnDemandNotBroadcast 验证按需注册:未注册的读者不会收到推送,
// 其首次读取按需注册并直接取当前快照。
func TestShared_OnDemandNotBroadcast(t *testing.T) {
	mgr := newSharedTestManager(t, 0)
	board := boardId{ID: "b1"}
	r1 := readerId{ID: "r1"} // 注册并持续接收推送
	r2 := readerId{ID: "r2"} // 从未读取,未注册

	mustWrite(t, mgr, board, "v1")
	mustRead(t, mgr, r1) // 仅 r1 注册

	mustWrite(t, mgr, board, "v2")
	if p := mustPeek(t, mgr, r1); p.Version != 2 {
		t.Fatalf("r1 should receive push, got %+v", p)
	}

	// r2 按需注册:首次读取直接取当前快照 v2
	r := mustRead(t, mgr, r2)
	if r.Data != "v2" || r.Version != 2 {
		t.Fatalf("r2 on-demand subscribe: want v2, got %+v", r)
	}
}

// TestShared_RebornReaderResubscribes 验证读者被驱逐后重建:
// 写方在推送时清理已消失读者,重建后的读者首次读取重新注册并取最新快照。
func TestShared_RebornReaderResubscribes(t *testing.T) {
	mgr := newSharedTestManager(t, 0)
	board := boardId{ID: "b1"}
	reader := readerId{ID: "r1"}

	mustWrite(t, mgr, board, "v1")
	mustRead(t, mgr, reader) // 订阅,缓存 v1

	// 关闭读者(模拟驱逐):仍在写方注册表,但推送会失败并被清理
	if !actor.CloseActor[readerId](mgr, reader) {
		t.Fatal("CloseActor returned false")
	}
	actor.JoinActor[readerId](mgr, reader)

	mustWrite(t, mgr, board, "v2") // 推送失败 → 写方自动清理该读者注册

	// 读者重建:首次读取重新注册,取最新 v2
	r := mustRead(t, mgr, reader)
	if r.Data != "v2" || r.Version != 2 {
		t.Fatalf("reborn reader: want data=v2 version=2, got %+v", r)
	}
}

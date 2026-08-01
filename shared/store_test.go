package shared

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ---------- key 定义 ----------

type techState struct {
	Level int
	Nodes []string
}

var (
	kSeed = Define[int64]("test/world/%s/seed", Immutable())
	kNote = Define[string]("test/world/%s/note")
	kTech = Define[techState]("test/family/%s/tech", WithTTL(30*time.Millisecond), WithCooldown(60*time.Millisecond))
	kConf = Define[int64]("test/ttl/%s/conf", WithTTL(30*time.Millisecond))
	kList = Define[[]int]("test/clone/%s", WithClone(func(v []int) []int {
		c := make([]int, len(v))
		copy(c, v)
		return c
	}))
)

// ---------- owner（发布方） ----------

type ownerId struct{ ID string }

func (ownerId) ActorType() actor.ActorType { return "t_owner" }
func (id ownerId) String() string          { return "owner-" + id.ID }

type ownerState struct{}

type pubInt struct {
	K Key[int64]
	V int64
}

func (*pubInt) ReqType(_ ownerId, _ *actor.Ok) string { return "PubInt" }
func (r *pubInt) Handle(ctx *actor.ActorContext[ownerId, ownerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	if err := PublishWait(ctx, r.K, r.V); err != nil {
		return nil, err
	}
	return actor.OK, nil
}

type pubStr struct {
	K Key[string]
	V string
}

func (*pubStr) ReqType(_ ownerId, _ *actor.Ok) string { return "PubStr" }
func (r *pubStr) Handle(ctx *actor.ActorContext[ownerId, ownerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	if err := PublishWait(ctx, r.K, r.V); err != nil {
		return nil, err
	}
	return actor.OK, nil
}

type pubTech struct {
	K Key[techState]
	V techState
}

func (*pubTech) ReqType(_ ownerId, _ *actor.Ok) string { return "PubTech" }
func (r *pubTech) Handle(ctx *actor.ActorContext[ownerId, ownerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	if err := PublishWait(ctx, r.K, r.V); err != nil {
		return nil, err
	}
	return actor.OK, nil
}

// pubList 发布 slice，并在发布后就地修改调用方的底层数组，用于验证 WithClone。
type pubList struct {
	K       Key[[]int]
	V       []int
	Mutated *int
}

func (*pubList) ReqType(_ ownerId, _ *actor.Ok) string { return "PubList" }
func (r *pubList) Handle(ctx *actor.ActorContext[ownerId, ownerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	if err := PublishWait(ctx, r.K, r.V); err != nil {
		return nil, err
	}
	if len(r.V) > 0 {
		r.V[0] = 999
		if r.Mutated != nil {
			*r.Mutated = 999
		}
	}
	return actor.OK, nil
}

type unpub struct{ Name string }

func (*unpub) ReqType(_ ownerId, _ *actor.Ok) string { return "Unpub" }
func (r *unpub) Handle(ctx *actor.ActorContext[ownerId, ownerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	if err := actor.Post[storeId](ctx.Manager(), shardOf(r.Name), &unpublishReq{Name: r.Name}); err != nil {
		return nil, err
	}
	return actor.OK, nil
}

// ---------- reader（订阅方） ----------

type readerId struct{ ID string }

func (readerId) ActorType() actor.ActorType { return "t_reader" }
func (id readerId) String() string          { return "reader-" + id.ID }

type readerState struct {
	Vars Reader
}

type readInt struct{ K Key[int64] }

type readIntReply struct {
	Value   int64
	Version int64
}

func (*readInt) ReqType(_ readerId, _ *readIntReply) string { return "ReadInt" }
func (r *readInt) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*readIntReply, error) {
	if spawning {
		ctx.Open()
	}
	v, err := Watch(ctx, &ctx.State().Vars, r.K)
	if err != nil {
		return nil, err
	}
	ver, _ := VersionOf(&ctx.State().Vars, r.K)
	return &readIntReply{Value: v, Version: ver}, nil
}

type readStr struct{ K Key[string] }

type readStrReply struct{ Value string }

func (*readStr) ReqType(_ readerId, _ *readStrReply) string { return "ReadStr" }
func (r *readStr) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*readStrReply, error) {
	if spawning {
		ctx.Open()
	}
	v, err := Watch(ctx, &ctx.State().Vars, r.K)
	if err != nil {
		return nil, err
	}
	return &readStrReply{Value: v}, nil
}

type readTech struct{ K Key[techState] }

type readTechReply struct{ Value techState }

func (*readTech) ReqType(_ readerId, _ *readTechReply) string { return "ReadTech" }
func (r *readTech) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*readTechReply, error) {
	if spawning {
		ctx.Open()
	}
	v, err := Watch(ctx, &ctx.State().Vars, r.K)
	if err != nil {
		return nil, err
	}
	return &readTechReply{Value: v}, nil
}

type readList struct{ K Key[[]int] }

type readListReply struct{ Value []int }

func (*readList) ReqType(_ readerId, _ *readListReply) string { return "ReadList" }
func (r *readList) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*readListReply, error) {
	if spawning {
		ctx.Open()
	}
	v, err := Watch(ctx, &ctx.State().Vars, r.K)
	if err != nil {
		return nil, err
	}
	return &readListReply{Value: v}, nil
}

// fetchInt 强一致读取，不订阅。
type fetchInt struct{ K Key[int64] }

type fetchIntReply struct {
	Value   int64
	Version int64
}

func (*fetchInt) ReqType(_ readerId, _ *fetchIntReply) string { return "FetchInt" }
func (r *fetchInt) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*fetchIntReply, error) {
	if spawning {
		ctx.Open()
	}
	v, err := Fetch(ctx, r.K)
	if err != nil {
		return nil, err
	}
	return &fetchIntReply{Value: v}, nil
}

// peek 暴露读者本地缓存的内部状态，供断言。
type peek struct{ Name string }

type peekReply struct {
	Loaded  bool
	Version int64
	Value   any
}

func (*peek) ReqType(_ readerId, _ *peekReply) string { return "Peek" }
func (r *peek) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*peekReply, error) {
	if spawning {
		ctx.Open()
	}
	e := ctx.State().Vars.entries[r.Name]
	if e == nil {
		return &peekReply{}, nil
	}
	return &peekReply{Loaded: e.loaded, Version: e.version, Value: e.value}, nil
}

// poison 直接改写读者本地缓存（同包内可访问），用于构造"缓存落后于容器"的场景。
type poison struct {
	Name    string
	Value   any
	Version int64
	Age     time.Duration
}

func (*poison) ReqType(_ readerId, _ *actor.Ok) string { return "Poison" }
func (r *poison) Handle(ctx *actor.ActorContext[readerId, readerState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	e := ctx.State().Vars.entry(r.Name)
	e.value = r.Value
	e.version = r.Version
	e.loaded = true
	e.at = time.Now().Add(-r.Age)
	return actor.OK, nil
}

// ---------- 未注册 ServeReader 的读者 Group ----------

type strayId struct{ ID string }

func (strayId) ActorType() actor.ActorType { return "t_stray" }
func (id strayId) String() string          { return "stray-" + id.ID }

type strayState struct{ Vars Reader }

type strayRead struct{ K Key[int64] }

func (*strayRead) ReqType(_ strayId, _ *actor.Ok) string { return "StrayRead" }
func (r *strayRead) Handle(ctx *actor.ActorContext[strayId, strayState], spawning bool) (*actor.Ok, error) {
	if spawning {
		ctx.Open()
	}
	_, err := Watch(ctx, &ctx.State().Vars, r.K)
	return nil, err
}

// ---------- 脚手架 ----------

func newTestManager(t *testing.T, opts Options) *actor.Manager {
	t.Helper()
	mgr := actor.NewManager(slog.New(slog.NewTextHandler(io.Discard, nil)))

	opts.Shards = 4 // 分片数进程内必须一致
	Serve(mgr, opts)

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[ownerId, ownerState]) {
		actor.RegisterServeHandler2(b, (*pubInt)(nil))
		actor.RegisterServeHandler2(b, (*pubStr)(nil))
		actor.RegisterServeHandler2(b, (*pubTech)(nil))
		actor.RegisterServeHandler2(b, (*pubList)(nil))
		actor.RegisterServeHandler2(b, (*unpub)(nil))
	})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[readerId, readerState]) {
		ServeReader(b, func(s *readerState) *Reader { return &s.Vars })
		actor.RegisterServeHandler2(b, (*readInt)(nil))
		actor.RegisterServeHandler2(b, (*readStr)(nil))
		actor.RegisterServeHandler2(b, (*readTech)(nil))
		actor.RegisterServeHandler2(b, (*readList)(nil))
		actor.RegisterServeHandler2(b, (*fetchInt)(nil))
		actor.RegisterServeHandler2(b, (*peek)(nil))
		actor.RegisterServeHandler2(b, (*poison)(nil))
	})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[strayId, strayState]) {
		actor.RegisterServeHandler2(b, (*strayRead)(nil))
	})

	t.Cleanup(func() {
		mgr.CloseManager()
		mgr.JoinManager()
	})
	return mgr
}

var testOwner = ownerId{ID: "o1"}

func mustPubInt(t *testing.T, mgr *actor.Manager, k Key[int64], v int64) {
	t.Helper()
	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner, &pubInt{K: k, V: v}); err != nil {
		t.Fatalf("pub %s=%d: %v", k.Name(), v, err)
	}
}

func mustReadInt(t *testing.T, mgr *actor.Manager, r readerId, k Key[int64]) readIntReply {
	t.Helper()
	rep, err := actor.Call[readerId](context.Background(), mgr, r, &readInt{K: k})
	if err != nil {
		t.Fatalf("read %s: %v", k.Name(), err)
	}
	return *rep
}

func mustPeek(t *testing.T, mgr *actor.Manager, r readerId, name string) peekReply {
	t.Helper()
	rep, err := actor.Call[readerId](context.Background(), mgr, r, &peek{Name: name})
	if err != nil {
		t.Fatalf("peek %s: %v", name, err)
	}
	return *rep
}

func mustPoison(t *testing.T, mgr *actor.Manager, r readerId, name string, value any, version int64, age time.Duration) {
	t.Helper()
	if _, err := actor.Call[readerId](context.Background(), mgr, r,
		&poison{Name: name, Value: value, Version: version, Age: age}); err != nil {
		t.Fatalf("poison %s: %v", name, err)
	}
}

func waitPeekVersion(t *testing.T, mgr *actor.Manager, r readerId, name string, want int64) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		if p := mustPeek(t, mgr, r, name); p.Version == want {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("reader %s: %s not reached version %d", r, name, want)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// ---------- 用例 ----------

// TestStore_WatchAndPush 验证首次订阅取快照、后续发布由推送更新、读取命中本地缓存。
func TestStore_WatchAndPush(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, seed, 42)

	got := mustReadInt(t, mgr, reader, seed)
	if got.Value != 42 || got.Version != 1 {
		t.Fatalf("first watch: want 42/v1, got %+v", got)
	}

	mustPubInt(t, mgr, seed, 43)
	p := mustPeek(t, mgr, reader, seed.Name()) // FIFO：推送先于 peek 处理
	if !p.Loaded || p.Version != 2 || p.Value != int64(43) {
		t.Fatalf("after publish: want pushed 43/v2, got %+v", p)
	}

	got = mustReadInt(t, mgr, reader, seed)
	if got.Value != 43 || got.Version != 2 {
		t.Fatalf("cached read: want 43/v2, got %+v", got)
	}
}

// TestStore_MultipleKeys 验证同一读者可同时订阅多个不同类型的 key（旧实现只能一份）。
func TestStore_MultipleKeys(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	note := kNote.Of("w1")
	tech := kTech.Of("f1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, seed, 7)
	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner, &pubStr{K: note, V: "n1"}); err != nil {
		t.Fatalf("pub note: %v", err)
	}
	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner, &pubTech{K: tech, V: techState{Level: 1}}); err != nil {
		t.Fatalf("pub tech: %v", err)
	}

	rep, err := actor.Call[readerId](context.Background(), mgr, reader, &readStr{K: note})
	if err != nil {
		t.Fatalf("read note: %v", err)
	}
	if rep.Value != "n1" {
		t.Fatalf("note: want n1, got %q", rep.Value)
	}
	if got := mustReadInt(t, mgr, reader, seed); got.Value != 7 {
		t.Fatalf("seed: want 7, got %d", got.Value)
	}

	// 三个 key 各自独立更新，互不覆盖
	mustPubInt(t, mgr, seed, 8)
	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner, &pubStr{K: note, V: "n2"}); err != nil {
		t.Fatalf("pub note 2: %v", err)
	}

	if p := mustPeek(t, mgr, reader, seed.Name()); p.Version != 2 || p.Value != int64(8) {
		t.Fatalf("seed after publish: want 8/v2, got %+v", p)
	}
	if p := mustPeek(t, mgr, reader, note.Name()); p.Version != 2 || p.Value != "n2" {
		t.Fatalf("note after publish: want n2/v2, got %+v", p)
	}

	techRep, err := actor.Call[readerId](context.Background(), mgr, reader, &readTech{K: tech})
	if err != nil {
		t.Fatalf("read tech: %v", err)
	}
	if techRep.Value.Level != 1 {
		t.Fatalf("tech: want level 1, got %d", techRep.Value.Level)
	}
}

// TestStore_CooldownCoalesces 验证冷却期内的多次发布合并为一次推送，读者最终拿到最新值。
func TestStore_CooldownCoalesces(t *testing.T) {
	mgr := newTestManager(t, Options{})
	tech := kTech.Of("f1")
	reader := readerId{ID: "r1"}

	publish := func(level int) {
		t.Helper()
		if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner,
			&pubTech{K: tech, V: techState{Level: level}}); err != nil {
			t.Fatalf("pub tech %d: %v", level, err)
		}
	}

	publish(1)
	if _, err := actor.Call[readerId](context.Background(), mgr, reader, &readTech{K: tech}); err != nil {
		t.Fatalf("read tech: %v", err)
	}

	publish(2) // 冷却期外：立即推送
	if p := mustPeek(t, mgr, reader, tech.Name()); p.Version != 2 {
		t.Fatalf("after level2: want v2 pushed, got %+v", p)
	}

	publish(3) // 冷却期内：合并
	publish(4)
	if p := mustPeek(t, mgr, reader, tech.Name()); p.Version != 2 {
		t.Fatalf("within cooldown: want still v2, got %+v", p)
	}

	waitPeekVersion(t, mgr, reader, tech.Name(), 4)
	techRep, err := actor.Call[readerId](context.Background(), mgr, reader, &readTech{K: tech})
	if err != nil {
		t.Fatalf("read tech after cooldown: %v", err)
	}
	if techRep.Value.Level != 4 {
		t.Fatalf("want merged level 4, got %d", techRep.Value.Level)
	}
}

// TestStore_TTLReconciles 验证 TTL 到期后 Watch 重新向容器拉取（推送丢失时的自愈）。
func TestStore_TTLReconciles(t *testing.T) {
	mgr := newTestManager(t, Options{})
	conf := kConf.Of("w1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, conf, 1)
	if got := mustReadInt(t, mgr, reader, conf); got.Value != 1 {
		t.Fatalf("first watch: want 1, got %d", got.Value)
	}

	mustPubInt(t, mgr, conf, 2)
	if got := mustReadInt(t, mgr, reader, conf); got.Value != 2 {
		t.Fatalf("after publish: want 2, got %d", got.Value)
	}

	// 模拟推送丢失：本地退回 v1 的旧值，但 TTL 未到期 → 仍读缓存
	mustPoison(t, mgr, reader, conf.Name(), int64(1), 1, 0)
	if got := mustReadInt(t, mgr, reader, conf); got.Value != 1 || got.Version != 1 {
		t.Fatalf("before TTL: want stale 1/v1, got %+v", got)
	}

	// TTL 到期 → Watch 重新向容器拉取，自愈回最新值
	mustPoison(t, mgr, reader, conf.Name(), int64(1), 1, time.Second)
	got := mustReadInt(t, mgr, reader, conf)
	if got.Value != 2 || got.Version != 2 {
		t.Fatalf("after TTL: want reconciled 2/v2, got %+v", got)
	}
}

// TestStore_NotPublished 验证未发布的 key 返回 ErrNotPublished。
func TestStore_NotPublished(t *testing.T) {
	mgr := newTestManager(t, Options{})
	missing := kSeed.Of("nope")
	reader := readerId{ID: "r1"}

	_, err := actor.Call[readerId](context.Background(), mgr, reader, &readInt{K: missing})
	if !errors.Is(err, ErrNotPublished) {
		t.Fatalf("watch missing: want ErrNotPublished, got %v", err)
	}
	_, err = actor.Call[readerId](context.Background(), mgr, reader, &fetchInt{K: missing})
	if !errors.Is(err, ErrNotPublished) {
		t.Fatalf("fetch missing: want ErrNotPublished, got %v", err)
	}
}

// TestStore_FetchDoesNotSubscribe 验证 Fetch 是强一致读取且不写本地缓存。
func TestStore_FetchDoesNotSubscribe(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, seed, 1)
	rep, err := actor.Call[readerId](context.Background(), mgr, reader, &fetchInt{K: seed})
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if rep.Value != 1 {
		t.Fatalf("fetch: want 1, got %d", rep.Value)
	}
	if p := mustPeek(t, mgr, reader, seed.Name()); p.Loaded {
		t.Fatalf("fetch should not populate local cache, got %+v", p)
	}

	mustPubInt(t, mgr, seed, 2)
	rep, err = actor.Call[readerId](context.Background(), mgr, reader, &fetchInt{K: seed})
	if err != nil {
		t.Fatalf("fetch 2: %v", err)
	}
	if rep.Value != 2 {
		t.Fatalf("fetch after publish: want 2, got %d", rep.Value)
	}
}

// TestStore_ShardsDistributeKeys 验证不同 key 分散到多个分片且功能正常。
func TestStore_ShardsDistributeKeys(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seen := make(map[int]bool)
	for i := 0; i < 50; i++ {
		seen[shardOf(kSeed.Of(string(rune('a'+i))).Name()).Shard] = true
	}
	if len(seen) < 2 {
		t.Fatalf("keys should spread over shards, got %v", seen)
	}

	// 跨分片的多个 key 都能正常发布订阅
	for i := 0; i < 5; i++ {
		k := kSeed.Of(string(rune('a' + i)))
		mustPubInt(t, mgr, k, int64(i))
		if got := mustReadInt(t, mgr, readerId{ID: "r1"}, k); got.Value != int64(i) {
			t.Fatalf("key %s: want %d, got %d", k.Name(), i, got.Value)
		}
	}
}

// TestStore_ReaderReborn 验证读者消失后容器清理注册，重建后重新订阅取最新值。
func TestStore_ReaderReborn(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, seed, 1)
	mustReadInt(t, mgr, reader, seed)

	if !actor.CloseActor[readerId](mgr, reader) {
		t.Fatal("CloseActor returned false")
	}
	actor.JoinActor[readerId](mgr, reader)

	mustPubInt(t, mgr, seed, 2) // 推送失败 → 容器清理该读者

	got := mustReadInt(t, mgr, reader, seed) // 重建后重新订阅
	if got.Value != 2 || got.Version != 2 {
		t.Fatalf("reborn reader: want 2/v2, got %+v", got)
	}
}

// TestStore_Unpublish 验证 Unpublish 后重新订阅会报未发布。
func TestStore_Unpublish(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	reader := readerId{ID: "r1"}

	mustPubInt(t, mgr, seed, 1)
	mustReadInt(t, mgr, reader, seed)

	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner, &unpub{Name: seed.Name()}); err != nil {
		t.Fatalf("unpub: %v", err)
	}

	deadline := time.Now().Add(2 * time.Second)
	for {
		_, err := actor.Call[readerId](context.Background(), mgr, reader, &fetchInt{K: seed})
		if errors.Is(err, ErrNotPublished) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("want ErrNotPublished after unpublish, got %v", err)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestStore_ReaderNotRegistered 验证未调用 ServeReader 的 Group 读取时报错。
func TestStore_ReaderNotRegistered(t *testing.T) {
	mgr := newTestManager(t, Options{})
	seed := kSeed.Of("w1")
	mustPubInt(t, mgr, seed, 1)

	_, err := actor.Call[strayId](context.Background(), mgr, strayId{ID: "s1"}, &strayRead{K: seed})
	if !errors.Is(err, ErrReaderNotRegistered) {
		t.Fatalf("want ErrReaderNotRegistered, got %v", err)
	}
}

// TestStore_WithClone 验证 clone：owner 发布后就地修改不影响读者看到的值。
func TestStore_WithClone(t *testing.T) {
	mgr := newTestManager(t, Options{})
	list := kList.Of("w1")
	mutated := 0

	if _, err := actor.Call[ownerId](context.Background(), mgr, testOwner,
		&pubList{K: list, V: []int{1, 2, 3}, Mutated: &mutated}); err != nil {
		t.Fatalf("pub list: %v", err)
	}
	if mutated != 999 {
		t.Fatalf("owner should have mutated its own slice, got %d", mutated)
	}

	rep, err := actor.Call[readerId](context.Background(), mgr, readerId{ID: "r1"}, &readList{K: list})
	if err != nil {
		t.Fatalf("read list: %v", err)
	}
	if len(rep.Value) != 3 || rep.Value[0] != 1 {
		t.Fatalf("want cloned [1 2 3], got %v", rep.Value)
	}
}

// TestDefine_DuplicatePatternPanics 验证重复定义同一 pattern 立即 panic。
func TestDefine_DuplicatePatternPanics(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Fatal("duplicate Define should panic")
		}
	}()
	Define[int]("test/dup")
	Define[string]("test/dup")
}

// Package shared 提供 actor 之间的共享数据容器：owner 按 key 发布，读者按 key 订阅，
// 容器负责版本推进、冷却合并推送与读者注册表维护。
//
// 模型：
//   - 容器（Store）是框架内置的 Group，由分片 actor 组成，项目侧不能注册 handler；
//     它只回复请求、只用 Post 推送，永不主动 Call 任何 actor，因此不可能参与调用环（无死锁）；
//   - 权威数据仍在业务 actor（world / family 等），容器只持有用于分发的副本，进程内纯内存，
//     重启后由 owner 重新 Publish；
//   - 读者在 State 中嵌入 Reader，首次 Watch 向容器订阅并取得快照，此后由推送更新本地缓存；
//   - 读者消失时，容器在推送失败（ActorNotFound）时清理其注册。
//
// 一致性语义（最终一致）：
//   - 推送是尽力投递：读者邮箱满时消息会丢失，靠下一次推送、TTL 对账或重新订阅自愈；
//   - 需要强一致的读取用 Fetch（每次触网），不要用 Watch。
//
// 线程模型：Reader 只允许在所属 actor 的 run goroutine 内访问（handler / Timer 回调）。
package shared

import (
	"errors"
	"fmt"
	"hash/fnv"
	"sync"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// storeActorType 是容器 Group 的类型名。
const storeActorType actor.ActorType = "__shared_store__"

// storeId 是容器分片 actor 的 ID。项目侧不直接使用。
type storeId struct{ Shard int }

func (storeId) ActorType() actor.ActorType { return storeActorType }
func (id storeId) String() string          { return fmt.Sprintf("store#%d", id.Shard) }

// Options 是容器配置。Shards 在进程内必须一致（首次 Serve 决定，之后不一致会 panic）。
type Options struct {
	Shards       int           // 容器分片数，<=0 取 1
	BufMails     int           // 容器 actor 邮箱容量，<=0 取 1024
	Cooldown     time.Duration // 默认推送冷却，key 可用 WithCooldown 覆盖
	Sweep        time.Duration // 空闲 key 清理周期，0 表示不清理
	IdleTTL      time.Duration // 配合 Sweep：无读者且超过该时间未发布的 key 被清理
	FetchTimeout time.Duration // 读者订阅 / 拉取的超时，<=0 取 2s
}

func (o Options) withDefaults() Options {
	if o.Shards <= 0 {
		o.Shards = 1
	}
	if o.BufMails <= 0 {
		o.BufMails = 1024
	}
	if o.FetchTimeout <= 0 {
		o.FetchTimeout = 2 * time.Second
	}
	return o
}

// 容器配置是进程级的：key → 分片的映射必须全局一致，否则发布与订阅会落在不同分片。
var storeMu sync.Mutex
var storeShards int
var storeOptions Options

// Serve 注册容器 Group。应在业务 Group 注册前后调用一次即可。
func Serve(mgr *actor.Manager, opts Options) {
	opts = opts.withDefaults()
	storeMu.Lock()
	if storeShards == 0 {
		storeShards = opts.Shards
		storeOptions = opts
	} else if storeShards != opts.Shards {
		storeMu.Unlock()
		panic(fmt.Sprintf("shared: shard count mismatch: already %d, got %d", storeShards, opts.Shards))
	}
	shards := storeShards
	storeMu.Unlock()
	_ = shards

	actor.Serve(mgr, actor.Options{BufMails: opts.BufMails}, func(b *actor.RegistryBuilder[storeId, storeState]) {
		b.SetOnSpawn(func(ctx *actor.ActorContext[storeId, storeState]) error {
			if ctx.State().entries == nil {
				ctx.State().entries = make(map[string]*entry)
			}
			return nil
		})
		registerStoreHandlers(b, opts)
	})
}

// shardOf 计算 key 所属分片。
func shardOf(name string) storeId {
	storeMu.Lock()
	n := storeShards
	storeMu.Unlock()
	if n <= 0 {
		n = 1
	}
	h := fnv.New32a()
	_, _ = h.Write([]byte(name))
	return storeId{Shard: int(h.Sum32() % uint32(n))}
}

func fetchTimeout() time.Duration {
	storeMu.Lock()
	defer storeMu.Unlock()
	return storeOptions.FetchTimeout
}

// ---------- 容器状态 ----------

type storeState struct {
	entries map[string]*entry
}

type entry struct {
	name     string
	value    any
	version  int64
	hasValue bool
	info     *defInfo // 首次发布时记录（冷却 / TTL / clone）

	lastPub  time.Time
	lastSent time.Time
	pending  bool

	subs map[*binding]any // binding -> map[R]struct{}（真实类型由 binding 掌握）
}

func (s *storeState) entry(name string) *entry {
	if s.entries == nil {
		s.entries = make(map[string]*entry)
	}
	e := s.entries[name]
	if e == nil {
		e = &entry{name: name, subs: make(map[*binding]any)}
		s.entries[name] = e
	}
	return e
}

// ---------- 容器消息 ----------

type publishReq struct {
	Name  string
	Value any
	Info  *defInfo
}

func (*publishReq) ReqType(_ storeId, _ *actor.Ok) string { return "__shared_publish__" }

type subscribeReq struct {
	Name   string
	Reader actor.ActorIdBase // 读者 ID（真实类型是读者 Group 的 R）
	Bind   *binding          // 读者 Group 的投递绑定
}

func (*subscribeReq) ReqType(_ storeId, _ *subscribeRep) string { return "__shared_subscribe__" }

type subscribeRep struct {
	Version int64
	Value   any
}

type fetchReq struct{ Name string }

func (*fetchReq) ReqType(_ storeId, _ *fetchRep) string { return "__shared_fetch__" }

type fetchRep struct {
	Version int64
	Value   any
}

type unpublishReq struct{ Name string }

func (*unpublishReq) ReqType(_ storeId, _ *actor.Ok) string { return "__shared_unpublish__" }

// ---------- 容器 handler ----------

func registerStoreHandlers(b *actor.RegistryBuilder[storeId, storeState], opts Options) {
	actor.RegisterServe[storeId, storeState, *publishReq, *actor.Ok, publishReq, actor.Ok](b,
		func(ctx *actor.ActorContext[storeId, storeState], req *publishReq, spawning bool) (*actor.Ok, error) {
			if spawning {
				ctx.Open() // 容器常驻：分发数据不应因空闲被回收
				if opts.Sweep > 0 {
					scheduleSweep(ctx, opts)
				}
			}
			s := ctx.State()
			e := s.entry(req.Name)
			e.info = req.Info
			e.value = req.Value
			e.version++
			e.hasValue = true
			e.lastPub = time.Now()
			scheduleRefresh(ctx, e, opts.Cooldown)
			return actor.OK, nil
		})

	actor.RegisterServe[storeId, storeState, *subscribeReq, *subscribeRep, subscribeReq, subscribeRep](b,
		func(ctx *actor.ActorContext[storeId, storeState], req *subscribeReq, spawning bool) (*subscribeRep, error) {
			if spawning {
				ctx.Open()
			}
			s := ctx.State()
			e := s.entry(req.Name)
			if req.Bind != nil {
				e.subs[req.Bind] = req.Bind.add(e.subs[req.Bind], req.Reader)
			}
			if !e.hasValue {
				return nil, &NotPublishedError{Name: req.Name}
			}
			return &subscribeRep{Version: e.version, Value: e.value}, nil
		})

	// Fetch 不唤醒容器：向不存在的 key 拉取应立刻报错而不是建一个空容器 actor。
	actor.RegisterQuery[storeId, storeState, *fetchReq, *fetchRep, fetchReq, fetchRep](b,
		func(ctx *actor.ActorContext[storeId, storeState], req *fetchReq, _ bool) (*fetchRep, error) {
			e := ctx.State().entries[req.Name]
			if e == nil || !e.hasValue {
				return nil, &NotPublishedError{Name: req.Name}
			}
			return &fetchRep{Version: e.version, Value: e.value}, nil
		})

	actor.RegisterServe[storeId, storeState, *unpublishReq, *actor.Ok, unpublishReq, actor.Ok](b,
		func(ctx *actor.ActorContext[storeId, storeState], req *unpublishReq, _ bool) (*actor.Ok, error) {
			delete(ctx.State().entries, req.Name)
			return actor.OK, nil
		})
}

// scheduleRefresh 按冷却策略推送：冷却期外立即推送，冷却期内合并为一个定时推送。
func scheduleRefresh(ctx *actor.ActorContext[storeId, storeState], e *entry, defaultCooldown time.Duration) {
	if e.pending {
		return
	}
	cd := defaultCooldown
	if e.info != nil && e.info.cooldown > 0 {
		cd = e.info.cooldown
	}
	if cd <= 0 || time.Since(e.lastSent) >= cd {
		sendRefresh(ctx.Manager(), e)
		return
	}
	e.pending = true
	ctx.Timer("shared_refresh:"+e.name, time.Until(e.lastSent.Add(cd)), func() {
		e.pending = false
		sendRefresh(ctx.Manager(), e)
	})
}

// sendRefresh 向全部已注册读者推送最新值，并清理已消失的读者。
func sendRefresh(mgr *actor.Manager, e *entry) {
	if len(e.subs) == 0 {
		return
	}
	e.lastSent = time.Now()
	var ttl time.Duration
	if e.info != nil {
		ttl = e.info.ttl
	}
	for bind, subs := range e.subs {
		for _, id := range bind.deliver(mgr, subs, e.name, e.version, e.value, ttl) {
			bind.remove(subs, id)
		}
	}
}

// scheduleSweep 周期性清理长时间无人订阅且未再发布的 key，避免 per-family 类 key 无限累积。
func scheduleSweep(ctx *actor.ActorContext[storeId, storeState], opts Options) {
	ctx.Timer("shared_sweep", opts.Sweep, func() {
		s := ctx.State()
		if opts.IdleTTL > 0 {
			now := time.Now()
			for name, e := range s.entries {
				if len(e.subs) == 0 && now.Sub(e.lastPub) > opts.IdleTTL {
					delete(s.entries, name)
				}
			}
		}
		scheduleSweep(ctx, opts)
	})
}

// ---------- 错误 ----------

// ErrStoreNotServed 表示容器 Group 没有注册（漏了 shared.Serve），属于装配错误。
var ErrStoreNotServed = errors.New("shared: store group not served; call shared.Serve before use")

// ErrNotPublished 表示 key 尚未发布。
var ErrNotPublished = errors.New("shared: key not published")

// NotPublishedError 携带未发布的 key 名。
type NotPublishedError struct{ Name string }

func (e *NotPublishedError) Error() string { return "shared: key not published: " + e.Name }
func (e *NotPublishedError) Unwrap() error { return ErrNotPublished }

// ErrTypeMismatch 表示容器里的值类型与 key 定义的类型不一致（同名 key 用不同类型发布）。
var ErrTypeMismatch = errors.New("shared: value type mismatch")

// TypeMismatchError 携带出错的 key 名。
type TypeMismatchError struct{ Name string }

func (e *TypeMismatchError) Error() string { return "shared: value type mismatch: " + e.Name }
func (e *TypeMismatchError) Unwrap() error { return ErrTypeMismatch }

// ErrReaderNotRegistered 表示读者 Group 没有调用 ServeReader。
var ErrReaderNotRegistered = errors.New("shared: reader group not registered")

// ReaderNotRegisteredError 携带读者 Group 名。
type ReaderNotRegisteredError struct{ Type actor.ActorType }

func (e *ReaderNotRegisteredError) Error() string {
	return "shared: reader group not registered: " + string(e.Type)
}
func (e *ReaderNotRegisteredError) Unwrap() error { return ErrReaderNotRegistered }

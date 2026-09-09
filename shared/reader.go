package shared

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// refreshReq 是容器向读者推送的刷新消息。携带 key 名，读者按 key 路由到本地缓存，
// 因此一个读者 Group 可以订阅任意多个 key（不同 key、不同类型互不干扰）。
type refreshReq[R actor.ActorId] struct {
	Name    string
	Version int64
	Value   any
	TTL     time.Duration
}

type refreshRep struct{}

func (*refreshReq[R]) ReqType(_ R, _ *refreshRep) string { return "__shared_refresh__" }

// binding 是读者 Group 的投递绑定：容器内只存读者 ID 与绑定指针，
// 真实类型 R 由绑定闭包掌握，推送时再还原为强类型做 Multicast。
type binding struct {
	add     func(subs any, id actor.ActorIdBase) any
	remove  func(subs any, id actor.ActorIdBase)
	deliver func(mgr *actor.Manager, subs any, name string, version int64, value any, ttl time.Duration) []actor.ActorIdBase
}

var (
	bindMu   sync.RWMutex
	bindings = make(map[actor.ActorType]*binding)
)

func lookupBinding(t actor.ActorType) *binding {
	bindMu.RLock()
	defer bindMu.RUnlock()
	return bindings[t]
}

func newBinding[R actor.ActorId]() *binding {
	return &binding{
		add: func(subs any, id actor.ActorIdBase) any {
			m, _ := subs.(map[R]struct{})
			if m == nil {
				m = make(map[R]struct{})
			}
			if r, ok := any(id).(R); ok {
				m[r] = struct{}{}
			}
			return m
		},
		remove: func(subs any, id actor.ActorIdBase) {
			m, _ := subs.(map[R]struct{})
			if m == nil {
				return
			}
			if r, ok := any(id).(R); ok {
				delete(m, r)
			}
		},
		deliver: func(mgr *actor.Manager, subs any, name string, version int64, value any, ttl time.Duration) []actor.ActorIdBase {
			m, _ := subs.(map[R]struct{})
			if len(m) == 0 {
				return nil
			}
			ids := make([]R, 0, len(m))
			for id := range m {
				ids = append(ids, id)
			}
			req := &refreshReq[R]{Name: name, Version: version, Value: value, TTL: ttl}
			errs, _ := actor.Multicast[R](mgr, ids, req)
			var gone []actor.ActorIdBase
			for _, e := range errs {
				if e.Err == nil {
					continue
				}
				var nf *actor.ActorNotFoundError
				if errors.As(e.Err, &nf) {
					gone = append(gone, e.Id)
				}
			}
			return gone
		},
	}
}

// ServeReader 在读者 Group 注册刷新处理器，并绑定 State 中的 Reader。
// 每个读者 Group 只需注册一次，之后可 Watch 任意 key。
//
//	actor.Serve(mgr, opts, func(b *actor.RegistryBuilder[RegionId, RegionState]) {
//	    shared.ServeReader(b, func(s *RegionState) *shared.Reader { return &s.Vars })
//	})
func ServeReader[R actor.ActorId, S any](b *actor.RegistryBuilder[R, S], accessor func(*S) *Reader) {
	var zero R
	rt := zero.ActorType()

	bindMu.Lock()
	bindings[rt] = newBinding[R]()
	bindMu.Unlock()

	actor.RegisterQuery[R, S, *refreshReq[R], *refreshRep, refreshReq[R], refreshRep](b,
		func(ctx *actor.ActorContext[R, S], req *refreshReq[R], _ bool) (*refreshRep, error) {
			accessor(ctx.State()).set(req.Name, req.Version, req.Value, req.TTL)
			return &refreshRep{}, nil
		})
}

// Reader 是读者 Actor 持有的本地缓存表。零值可用，嵌入 State 即可，
// 只允许在该 actor 的 run goroutine 内访问。
type Reader struct {
	entries map[string]*cacheEntry
}

type cacheEntry struct {
	value   any
	version int64
	loaded  bool
	at      time.Time
	ttl     time.Duration
}

func (r *Reader) entry(name string) *cacheEntry {
	if r.entries == nil {
		r.entries = make(map[string]*cacheEntry)
	}
	e := r.entries[name]
	if e == nil {
		e = &cacheEntry{}
		r.entries[name] = e
	}
	return e
}

// set 用容器推送的快照替换本地缓存。
func (r *Reader) set(name string, version int64, value any, ttl time.Duration) {
	e := r.entry(name)
	e.value = value
	e.version = version
	e.loaded = true
	e.at = time.Now()
	e.ttl = ttl
}

// Watch 读取共享数据：首次调用向容器订阅并取得快照，之后命中本地缓存。
// key 未发布时返回 ErrNotPublished；读者 Group 未调用 ServeReader 时返回 ErrReaderNotRegistered。
func Watch[R actor.ActorId, S any, V any](ctx *actor.ActorContext[R, S], r *Reader, k Key[V]) (V, error) {
	return WatchFrom(ctx.Context(), ctx.Manager(), ctx.Id(), r, k)
}

// WatchFrom 是 Watch 的变体，供只有 ActorControl / Manager 的场合使用
// （如 grain 的 OnSpawn 钩子、CIS 风格 handler）。语义完全一致。
func WatchFrom[V any](ctx context.Context, mgr *actor.Manager, id actor.ActorIdBase, r *Reader, k Key[V]) (V, error) {
	var zero V
	bind := lookupBinding(id.ActorType())
	if bind == nil {
		return zero, &ReaderNotRegisteredError{Type: id.ActorType()}
	}
	if v, ok := cacheGet(r, k); ok {
		return v, nil
	}

	callCtx, cancel := context.WithTimeout(ctx, fetchTimeout())
	defer cancel()
	rep, err := actor.Call[storeId](callCtx, mgr, shardOf(k.name),
		&subscribeReq{Name: k.name, Reader: id, Bind: bind})
	if err != nil {
		return zero, translateStoreError(err, k.name)
	}
	v, ok := rep.Value.(V)
	if !ok {
		return zero, &TypeMismatchError{Name: k.name}
	}
	r.set(k.name, rep.Version, v, k.info.ttl)
	return v, nil
}

// Fetch 强一致读取：每次都向容器拉取，不订阅、不写本地缓存。
// 适合"必须看到最新值"的读取（如发号、校验），热路径请用 Watch。
func Fetch[R actor.ActorId, S any, V any](ctx *actor.ActorContext[R, S], k Key[V]) (V, error) {
	return FetchFrom(ctx.Context(), ctx.Manager(), k)
}

// FetchFrom 是 Fetch 的变体，供只有 Manager 的场合使用。
func FetchFrom[V any](ctx context.Context, mgr *actor.Manager, k Key[V]) (V, error) {
	var zero V
	callCtx, cancel := context.WithTimeout(ctx, fetchTimeout())
	defer cancel()
	rep, err := actor.Call[storeId](callCtx, mgr, shardOf(k.name), &fetchReq{Name: k.name})
	if err != nil {
		return zero, translateStoreError(err, k.name)
	}
	v, ok := rep.Value.(V)
	if !ok {
		return zero, &TypeMismatchError{Name: k.name}
	}
	return v, nil
}

// Invalidate 丢弃本地缓存，下一次 Watch 会重新向容器订阅。
// 用于业务明确知道缓存已失效（但容器尚未推送新版本）的场景。
func Invalidate[V any](r *Reader, k Key[V]) {
	if e := r.entries[k.name]; e != nil {
		e.loaded = false
	}
}

// VersionOf 返回本地缓存的版本号与是否已加载，供诊断与测试断言。
func VersionOf[V any](r *Reader, k Key[V]) (int64, bool) {
	e := r.entries[k.name]
	if e == nil || !e.loaded {
		return 0, false
	}
	return e.version, true
}

func cacheGet[V any](r *Reader, k Key[V]) (V, bool) {
	var zero V
	e := r.entries[k.name]
	if e == nil || !e.loaded {
		return zero, false
	}
	if e.ttl > 0 && time.Since(e.at) > e.ttl {
		return zero, false // TTL 到期：重新订阅，对账自愈
	}
	v, ok := e.value.(V)
	if !ok {
		return zero, false
	}
	return v, true
}

// translateStoreError 把框架错误翻译成 shared 的语义错误，便于调用方判断。
func translateStoreError(err error, name string) error {
	// 容器 Group 没注册（漏了 shared.Serve）是装配错误，不能和"未发布"混为一谈。
	var ge *actor.GroupNotFoundError
	if errors.As(err, &ge) {
		return ErrStoreNotServed
	}
	var nf *actor.ActorNotFoundError
	if errors.As(err, &nf) {
		return &NotPublishedError{Name: name}
	}
	return err
}

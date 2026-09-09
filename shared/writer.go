package shared

import (
	"errors"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// Option 配置写方行为。
type Option func(*writerOptions)

type writerOptions struct {
	cooldown time.Duration
}

// WithCooldown 设置刷新推送的冷却时间:冷却期内多次写入会被合并为一次推送,
// 读者最终收到的是最新数据,而非每一次写入。默认 0(每次写入立即推送)。
func WithCooldown(d time.Duration) Option {
	return func(o *writerOptions) { o.cooldown = d }
}

// Versioned 是写方持有的共享数据:数据、版本与读者注册表绑定。
// 零值可用;Set / Update 推进版本;框架在标准 Write 时按冷却策略自动推送 Refresh。
// 推荐通过标准 Write 消息写入;直接调用 Set / Update 只推进版本,不会推送。
type Versioned[R actor.ActorId, T any] struct {
	version  int64
	data     T
	readers  map[R]struct{}
	lastSent time.Time
	pending  bool
}

// Data 返回当前数据。
func (v *Versioned[R, T]) Data() T {
	return v.data
}

// Version 返回当前数据版本。
func (v *Versioned[R, T]) Version() int64 {
	return v.version
}

// Set 整体替换数据并推进版本。仅推进版本,不推送;推荐改用标准 Write 消息。
func (v *Versioned[R, T]) Set(data T) {
	v.data = data
	v.version++
}

// Update 就地修改数据并推进版本。语义同 Set。
func (v *Versioned[R, T]) Update(fn func(*T)) {
	fn(&v.data)
	v.version++
}

func (v *Versioned[R, T]) addReader(id R) {
	if v.readers == nil {
		v.readers = make(map[R]struct{})
	}
	v.readers[id] = struct{}{}
}

func (v *Versioned[R, T]) removeReader(id R) {
	delete(v.readers, id)
}

func (v *Versioned[R, T]) readerIDs() []R {
	ids := make([]R, 0, len(v.readers))
	for id := range v.readers {
		ids = append(ids, id)
	}
	return ids
}

// Write 是标准写请求:更新数据 + 推进版本,框架按冷却策略自动向已注册读者推送 Refresh。
// 返回 OkReply,可 Post(发后不理)或 Call(等待确认)。
type Write[A actor.ActorId, T any] struct {
	Data T
}

func (*Write[A, T]) ReqType(_ A, _ *actor.Ok) string { return "__shared_write__" }

// Subscribe 是读者按需注册请求:写方登记该读者并返回当前快照。
// 由 Cache.Get 首次调用时自动发送,无需业务手动注册。
type Subscribe[A actor.ActorId, R actor.ActorId, T any] struct {
	Reader R
}

// SubscribeReply 是 Subscribe 的回复:写方当前权威版本与数据。
type SubscribeReply[T any] struct {
	Version int64
	Data    T
}

func (*Subscribe[A, R, T]) ReqType(_ A, _ *SubscribeReply[T]) string {
	return "__shared_subscribe__"
}

// ServeWriter 在写方 Group 注册标准协议:
//   - Write:更新数据 + 版本推进,带冷却的自动刷新推送;
//   - Subscribe:读者按需注册,返回当前快照。
//
// 所有类型参数均可从参数推导;opts 可配 WithCooldown。
//
//	actor.Serve(mgr, opts, func(b *actor.RegistryBuilder[BoardId, BoardState]) {
//	    shared.ServeWriter(b, func(s *BoardState) *shared.Versioned[PlayerId, BoardData] {
//	        return &s.Data
//	    }, shared.WithCooldown(50*time.Millisecond))
//	})
func ServeWriter[A actor.ActorId, R actor.ActorId, S any, T any](
	b *actor.RegistryBuilder[A, S],
	accessor func(*S) *Versioned[R, T],
	opts ...Option,
) {
	o := writerOptions{}
	for _, opt := range opts {
		opt(&o)
	}
	cooldown := o.cooldown

	actor.RegisterServe[A, S, *Write[A, T], *actor.Ok, Write[A, T], actor.Ok](
		b,
		func(ctx *actor.ActorContext[A, S], req *Write[A, T], spawning bool) (*actor.Ok, error) {
			if spawning {
				ctx.Open() // 数据写方保持存活,避免空闲驱逐丢失数据
			}
			v := accessor(ctx.State())
			v.Set(req.Data)
			scheduleRefresh(ctx, v, cooldown)
			return actor.OK, nil
		},
	)

	actor.RegisterServe[A, S, *Subscribe[A, R, T], *SubscribeReply[T], Subscribe[A, R, T], SubscribeReply[T]](
		b,
		func(ctx *actor.ActorContext[A, S], req *Subscribe[A, R, T], spawning bool) (*SubscribeReply[T], error) {
			if spawning {
				ctx.Open()
			}
			v := accessor(ctx.State())
			v.addReader(req.Reader)
			return &SubscribeReply[T]{Version: v.Version(), Data: v.Data()}, nil
		},
	)
}

// scheduleRefresh 按冷却策略推送刷新:冷却期外立即推送,冷却期内合并为单个定时推送。
func scheduleRefresh[A actor.ActorId, R actor.ActorId, S any, T any](
	ctx *actor.ActorContext[A, S],
	v *Versioned[R, T],
	cooldown time.Duration,
) {
	if v.pending {
		return
	}
	mgr := ctx.Manager()
	if cooldown <= 0 || time.Since(v.lastSent) >= cooldown {
		sendRefresh(mgr, v)
		return
	}
	v.pending = true
	ctx.Timer("shared_refresh", time.Until(v.lastSent.Add(cooldown)), func() {
		v.pending = false
		sendRefresh(mgr, v)
	})
}

// sendRefresh 把当前快照(复制后的数据指针)推送给所有已注册读者,并清理已消失的读者。
func sendRefresh[R actor.ActorId, T any](mgr *actor.Manager, v *Versioned[R, T]) {
	ids := v.readerIDs()
	if len(ids) == 0 {
		return
	}
	v.lastSent = time.Now()
	// 复制一份数据再取指针,避免读者 goroutine 读取与写方后续写入产生数据竞争。
	data := v.Data()
	errs, _ := actor.Multicast[R](mgr, ids, &Refresh[R, T]{Version: v.Version(), Data: &data})
	for _, e := range errs {
		var nf *actor.ActorNotFoundError
		if errors.As(e.Err, &nf) {
			v.removeReader(e.Id)
		}
	}
}

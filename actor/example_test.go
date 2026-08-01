package actor_test

// 本文件是 README 示例的唯一事实来源：被 // region: <name> ... // endregion: <name>
// 圈出的片段由 cmd/readmegen 注入 README.md / README.zh.md。
// 因为本文件是编译单元（go test 会编译它），示例随 API 变更而过时会被直接发现。

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ─── 示例业务类型 ───

// region: types
// Actor ID：ActorType() 决定所属 Group，String() 用于日志与错误信息。
type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
	return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

// 状态：处理器直接读写，无需加锁。
type PlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
}

// 回复：Call 的返回类型。
type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

// 请求：实现 Request[A, R]，ReqType 把 Id 与 Reply 绑定，跨 Group 发送时编译期报错。
type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

type Attack struct {
	Damage int `json:"damage"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

type Heal struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ PlayerId, _ *HealReply) string { return "Heal" }

type Close struct{}

func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }

// endregion: types

// ─── RequestHandler：把处理逻辑绑定在请求类型上 ───

// region: request_handler
type Logout struct{}

func (*Logout) ReqType(_ PlayerId, _ actor.OkReply) string { return "Logout" }

// Handle 包含处理逻辑 —— 无需在 Serve 中另写闭包。
func (req *Logout) Handle(ctx *actor.ActorContext[PlayerId, PlayerState], _ bool) (actor.OkReply, error) {
	ctx.Quit()
	return actor.OK, nil
}

// endregion: request_handler

// ─── ActorRef：同 Group 内的直连引用 ───

// ErrPlayerNotFound 是示例业务错误：目标 Player 尚未创建。
var ErrPlayerNotFound = errors.New("player not found")

// region: ref_types
type Follow struct {
	Target PlayerId
}

func (*Follow) ReqType(_ PlayerId, _ actor.OkReply) string { return "Follow" }

// Ping 发给被引用的 Player（发后不理）。
type Ping struct {
	From PlayerId
}

func (*Ping) ReqType(_ PlayerId, _ actor.OkReply) string { return "Ping" }

// QueryHP 向被引用的 Player 查询血量（请求-回复）。
type QueryHP struct{}

type HPReply struct {
	HP int `json:"hp"`
}

func (*QueryHP) ReqType(_ PlayerId, _ *HPReply) string { return "QueryHP" }

// endregion: ref_types

// ─── SafeReply：需要释放资源的回复 ───

// region: safe_reply
// ResourceReply 持有外部资源，实现 SafeReply[ResourceReply]（~*R0 + Close()）。
type ResourceReply struct {
	Data   []byte
	handle *os.File
}

func (r *ResourceReply) Close() {
	if r.handle != nil {
		_ = r.handle.Close()
	}
}

type LoadData struct{}

func (*LoadData) ReqType(_ PlayerId, _ *ResourceReply) string { return "LoadData" }

// endregion: safe_reply

// ─── 强类型 Manager context 值（CtxValue）───

// region: ctx_value
// 定义 per-Manager 的强类型 context 值：key 由句柄自带，写入与读取都在编译期检查类型，
// 不需要手写私有 key struct 与类型断言。
var SeqAllocValue = actor.DefineValue[*SeqAlloc]()

// SeqAlloc 是挂在 Manager 上的基础设施句柄（如 ID 分配器）。
type SeqAlloc struct{ n int }

// Next 只在所属 Manager 的 actor 内调用，因此无需加锁。
func (a *SeqAlloc) Next() int {
	a.n++
	return a.n
}

// endregion: ctx_value

// ─── 各主题示例 ───

func Example_serve() {
	// region: serve
	mgr := actor.NewManager(slog.Default())

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		// RegisterSpawn：首条消息创建 Actor（发后不理）
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
			ctx.Open() // spawn 时框架不会自动激活，需显式 Open 才能常驻
			ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel})
			return actor.OK, nil
		})

		// RegisterQuery：只处理已存在的 Actor（返回回复）
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().HP -= req.Damage
			alive := ctx.State().HP > 0
			return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
		})

		// RegisterServe：首条消息创建 Actor，并且返回回复
		actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, spawning bool) (*HealReply, error) {
			if spawning {
				ctx.Open()
			}
			ctx.State().HP += req.Amount
			return &HealReply{NewHP: ctx.State().HP}, nil
		})
	})
	// endregion: serve

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_request_handler() {
	mgr := actor.NewManager(slog.Default())

	// region: register_handler
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		// 请求类型自带 Handle：注册时只需给出类型，不必再传闭包
		actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
	})
	// endregion: register_handler

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_onspawn() {
	mgr := actor.NewManager(slog.Default())

	// region: onspawn
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		b.SetOnSpawn(func(ctx *actor.ActorContext[PlayerId, PlayerState]) error {
			// 首次创建时、spawn 处理器之前运行一次
			ctx.SetState(PlayerState{HP: 100})
			return nil // 返回 error 会中止创建，spawn 消息的调用方收到该错误
		})
		actor.RegisterSpawnHandler[PlayerId, PlayerState, *Logout](b)
	})
	// endregion: onspawn

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_send() {
	mgr := actor.NewManager(slog.Default())

	// region: send
	playerId := PlayerId{ServerId: 1, OpenId: "alice"}
	ctx := context.Background()

	// Post：发后不理（必要时按 spawn 语义创建 Actor）
	if err := actor.Post(mgr, playerId, &Login{InitHP: 100, InitLevel: 1}); err != nil {
		slog.Error("post login failed", "err", err)
	}

	// Call：同步等待回复
	reply, err := actor.Call(ctx, mgr, playerId, &Attack{Damage: 30})
	if err != nil {
		slog.Error("call attack failed", "err", err)
	}
	fmt.Println(reply.RemainingHP) // 70

	// 带超时的 Call
	timeoutCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	reply, err = actor.Call(timeoutCtx, mgr, playerId, &Attack{Damage: 10})
	if err != nil {
		slog.Error("call attack with timeout failed", "err", err)
	}
	fmt.Println(reply.RemainingHP) // 60

	// Broadcast：发给 Group 中的所有 Actor；Multicast：发给指定的一组 Actor
	_, _ = actor.Broadcast(mgr, &Close{})
	_, _ = actor.Multicast(mgr, []PlayerId{playerId}, &Close{})

	// Count / Finalize：统计活跃数量 / 关闭 Group 中所有 Actor 并等待
	n, _ := actor.Count[PlayerId](mgr)
	fmt.Println(n)
	actor.Finalize(mgr, &Close{})
	// endregion: send

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_context() {
	mgr := actor.NewManager(slog.Default())

	// region: context
	teammate := PlayerId{ServerId: 1, OpenId: "bob"}
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
			state := ctx.State()  // *PlayerState —— 无需类型断言
			id := ctx.Id()        // 当前 ActorId
			log := ctx.Logger()   // *slog.Logger
			base := ctx.Context() // 随 Actor 退出而取消的 context

			log.Info("attacked", "player", id, "hp", state.HP)

			// 调度延迟回调（返回定时器 ID），以及取消它
			timer := ctx.Timer("regen", time.Second, func() { state.HP++ })
			ctx.StopTimer(timer)

			// 下游调用用 ctx.Context()：Actor 退出时下游请求一并取消
			if _, err := actor.Call(base, ctx.Manager(), teammate, &Heal{Amount: 1}); err != nil {
				log.Warn("heal teammate failed", "err", err)
			}

			state.HP -= req.Damage
			return &AttackReply{RemainingHP: state.HP, Alive: state.HP > 0}, nil
		})
	})
	// endregion: context

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_ref() {
	mgr := actor.NewManager(slog.Default())

	// region: ref
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Follow, _ bool) (actor.OkReply, error) {
			// ctx.Ref 只在同一 Group 中查找已存在的 Actor —— 不会创建，找不到返回 nil
			ref := ctx.Ref(req.Target)
			if ref == nil {
				return nil, ErrPlayerNotFound
			}
			defer ref.Release() // 释放持有，允许目标之后空闲退出

			// RefPost：发后不理，绕过 Group 查找
			if err := actor.RefPost(ref, &Ping{From: ctx.Id()}); err != nil {
				return nil, err
			}

			// RefCall：请求-回复，绕过 Group 查找
			hp, err := actor.RefCall(ctx.Context(), ref, &QueryHP{})
			if err != nil {
				return nil, err
			}
			ctx.Logger().Info("target hp", "target", ref.Id(), "hp", hp.HP)
			return actor.OK, nil
		})
	})
	// endregion: ref

	mgr.CloseManager()
	mgr.JoinManager()
}

// region: safe_call
// SafeCall：成功时调用方负责 Close，超时/取消时由框架兜底 Close。
func loadData(ctx context.Context, mgr *actor.Manager, id PlayerId) error {
	resource, err := actor.SafeCall(ctx, mgr, id, &LoadData{})
	if err != nil {
		// 超时或取消时框架已经调用过 resource 的 Close()
		return err
	}
	defer resource.Close() // 成功时调用方负责清理

	fmt.Println(len(resource.Data))
	return nil
}

// endregion: safe_call

func Example_safe_call() {
	_ = loadData(context.Background(), actor.NewManager(slog.Default()), PlayerId{ServerId: 1, OpenId: "alice"})
}

func Example_ctx_value() {
	mgr := actor.NewManager(slog.Default())

	// region: ctx_value_use
	// 必须在任何 Serve 之前挂载：Group 创建时捕获当时的 Manager context，
	// 之后挂载的值对已注册的 Group 不可见（框架直接 panic，避免静默失效）。
	actor.WithValue(mgr, SeqAllocValue, &SeqAlloc{})

	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
			alloc, ok := SeqAllocValue.Get(ctx.Context()) // 编译期已知类型，无需断言
			if !ok {
				return actor.OK, errors.New("seq alloc not mounted")
			}
			ctx.Open()
			ctx.SetState(PlayerState{HP: req.InitHP, Level: alloc.Next()})
			return actor.OK, nil
		})
	})
	// endregion: ctx_value_use

	mgr.CloseManager()
	mgr.JoinManager()
}

func Example_manager() {
	mgr := actor.NewManager(slog.Default())
	playerId := PlayerId{ServerId: 1, OpenId: "alice"}

	// region: manager
	// 优雅关闭：停止接收新消息，等待所有 Actor 退出
	mgr.CloseManager()
	mgr.JoinManager()

	// 检查 Manager 是否已关闭
	if mgr.IsClosed() {
		slog.Info("manager closed")
	}

	// 单 Actor 生命周期
	actor.CloseActor[PlayerId](mgr, playerId) // 温和关闭：排空邮箱，完成在途请求
	actor.KillActor[PlayerId](mgr, playerId)  // 强制关闭：取消 ctx，丢弃待处理
	actor.JoinActor[PlayerId](mgr, playerId)  // 等待 Actor 的 goroutine 退出
	// endregion: manager
}

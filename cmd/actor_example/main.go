// 本地 Actor 示例：演示注册（Spawn / Query / Serve）、RequestHandler、
// OnSpawn 钩子、Timer、CtxValue、ActorRef 与优雅关闭。
//
// 运行：go run ./cmd/actor_example/
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ─── 类型 ───

type PlayerId struct {
	ServerId int    `json:"serverId"`
	OpenId   string `json:"openId"`
}

func (id PlayerId) ActorType() actor.ActorType { return "Player" }
func (id PlayerId) String() string {
	return fmt.Sprintf("Player(%d,%s)", id.ServerId, id.OpenId)
}

type PlayerState struct {
	HP    int `json:"hp"`
	Level int `json:"level"`
	Seq   int `json:"seq"` // 由 Manager 级序号分配器发放
}

// ─── 请求 ───

// Login 只在 Actor 不存在时处理（RegisterSpawn）。
type Login struct {
	InitHP    int `json:"initHP"`
	InitLevel int `json:"initLevel"`
}

func (*Login) ReqType(_ PlayerId, _ actor.OkReply) string { return "Login" }

// Attack 只处理已存在的 Actor，返回自定义回复（RegisterQuery）。
type Attack struct {
	Damage int `json:"damage"`
}

type AttackReply struct {
	RemainingHP int  `json:"remainingHP"`
	Alive       bool `json:"alive"`
}

func (*Attack) ReqType(_ PlayerId, _ *AttackReply) string { return "Attack" }

// Heal 首条消息也能创建 Actor（RegisterServe）。
type Heal struct {
	Amount int `json:"amount"`
}

type HealReply struct {
	NewHP int `json:"newHP"`
}

func (*Heal) ReqType(_ PlayerId, _ *HealReply) string { return "Heal" }

// Follow 取到同组另一个 Actor 的 ActorRef，直接投递消息（绕过 Group 查找）。
type Follow struct {
	Target PlayerId
}

func (*Follow) ReqType(_ PlayerId, _ actor.OkReply) string { return "Follow" }

type Ping struct {
	From PlayerId
}

func (*Ping) ReqType(_ PlayerId, _ actor.OkReply) string { return "Ping" }

type QueryHP struct{}

type HPReply struct {
	HP int `json:"hp"`
}

func (*QueryHP) ReqType(_ PlayerId, _ *HPReply) string { return "QueryHP" }

// Logout 把处理逻辑写在请求类型上（RequestHandler），注册时不必再传闭包。
type Logout struct{}

func (*Logout) ReqType(_ PlayerId, _ actor.OkReply) string { return "Logout" }

func (req *Logout) Handle(ctx *actor.ActorContext[PlayerId, PlayerState], _ bool) (actor.OkReply, error) {
	ctx.Logger().Info("player logout", "hp", ctx.State().HP)
	ctx.Quit()
	return actor.OK, nil
}

type Close struct{}

func (*Close) ReqType(_ PlayerId, _ actor.OkReply) string { return "Close" }

// ─── Manager 级强类型 context 值（CtxValue）───

// PlayerSeqValue 由 Manager 持有：同一进程里的两个 Manager 各有一份，互不影响。
var PlayerSeqValue = actor.DefineValue[*PlayerSeq]()

type PlayerSeq struct{ n int }

func (s *PlayerSeq) Next() int {
	s.n++
	return s.n
}

// ─── 注册 ───

var options100 = actor.Options{BufMails: 100}

func setupPlayer(mgr *actor.Manager) {
	actor.Serve(mgr, options100, func(b *actor.RegistryBuilder[PlayerId, PlayerState]) {
		// OnSpawn：首次创建时、处理器之前运行一次，适合初始化状态与资源。
		b.SetOnSpawn(func(ctx *actor.ActorContext[PlayerId, PlayerState]) error {
			ctx.Logger().Info("player on spawn")
			return nil
		})

		actor.RegisterSpawn(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Login, _ bool) (actor.OkReply, error) {
			alloc, ok := PlayerSeqValue.Get(ctx.Context())
			if !ok {
				return actor.OK, fmt.Errorf("PlayerSeqValue not mounted")
			}

			ctx.Open() // 框架不会自动激活：不 Open 的 Actor 处理完本条消息即被回收
			ctx.SetState(PlayerState{HP: req.InitHP, Level: req.InitLevel, Seq: alloc.Next()})

			// 定时器回调在该 Actor 的 goroutine 内执行，改状态无需加锁。
			ctx.Timer("regen", 300*time.Millisecond, func() {
				ctx.State().HP++
			})
			ctx.Logger().Info("player login", "hp", req.InitHP, "level", req.InitLevel, "seq", ctx.State().Seq)
			return actor.OK, nil
		})

		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Attack, _ bool) (*AttackReply, error) {
			ctx.State().HP -= req.Damage
			alive := ctx.State().HP > 0
			ctx.Logger().Info("player attacked", "damage", req.Damage, "remainingHP", ctx.State().HP)
			return &AttackReply{RemainingHP: ctx.State().HP, Alive: alive}, nil
		})

		actor.RegisterServe(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Heal, spawning bool) (*HealReply, error) {
			if spawning {
				ctx.Open()
			}
			ctx.State().HP += req.Amount
			return &HealReply{NewHP: ctx.State().HP}, nil
		})

		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Follow, _ bool) (actor.OkReply, error) {
			// ctx.Ref 只在同一 Group 内查找已存在的 Actor：不创建，找不到返回 nil。
			ref := ctx.Ref(req.Target)
			if ref == nil {
				return nil, fmt.Errorf("player not found: %s", req.Target)
			}
			defer ref.Release() // 释放引用，允许目标之后空闲退出

			if err := actor.RefPost(ref, &Ping{From: ctx.Id()}); err != nil {
				return nil, err
			}
			hp, err := actor.RefCall(ctx.Context(), ref, &QueryHP{})
			if err != nil {
				return nil, err
			}
			ctx.Logger().Info("followed", "target", ref.Id(), "targetHP", hp.HP)
			return actor.OK, nil
		})

		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *QueryHP, _ bool) (*HPReply, error) {
			return &HPReply{HP: ctx.State().HP}, nil
		})

		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Ping, _ bool) (actor.OkReply, error) {
			ctx.Logger().Info("ping received", "from", req.From)
			return actor.OK, nil
		})

		// 请求类型自带 Handle：注册时只需给出类型参数。
		// 用 RegisterServeHandler（而非 RegisterSpawnHandler）：下线时 Actor 已存在，
		// spawn 变体只在 Actor 不存在时才会被调用。
		actor.RegisterServeHandler[PlayerId, PlayerState, *Logout](b)

		actor.RegisterQuery(b, func(ctx *actor.ActorContext[PlayerId, PlayerState], req *Close, _ bool) (actor.OkReply, error) {
			ctx.Quit()
			return actor.OK, nil
		})
	})
}

// ─── main ───

func main() {
	slog.SetDefault(slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo})))

	mgr := actor.NewManager(slog.Default())

	// CtxValue 必须在任何 Serve 之前挂载，否则对已注册的 Group 不可见（框架直接 panic）。
	actor.WithValue(mgr, PlayerSeqValue, &PlayerSeq{})
	setupPlayer(mgr)

	ctx := context.Background()
	player1 := PlayerId{ServerId: 1, OpenId: "alice"}
	player2 := PlayerId{ServerId: 1, OpenId: "bob"}

	// 1. Post：发后不理，首条消息创建 Actor
	fmt.Println("\n=== 1. Post: 创建玩家 ===")
	_ = actor.Post(mgr, player1, &Login{InitHP: 100, InitLevel: 1})
	_ = actor.Post(mgr, player2, &Login{InitHP: 80, InitLevel: 2})
	time.Sleep(100 * time.Millisecond)

	// 2. Call：同步等回复
	fmt.Println("\n=== 2. Call: 攻击 ===")
	reply, err := actor.Call(ctx, mgr, player1, &Attack{Damage: 30})
	if err != nil {
		fmt.Printf("攻击失败: %v\n", err)
	} else {
		fmt.Printf("alice 受到 30 伤害：remainingHP=%d alive=%v\n", reply.RemainingHP, reply.Alive)
	}

	// 3. Call：治疗（RegisterServe，已存在时不会重复创建）
	fmt.Println("\n=== 3. Call: 治疗 ===")
	healReply, err := actor.Call(ctx, mgr, player2, &Heal{Amount: 20})
	if err != nil {
		fmt.Printf("治疗失败: %v\n", err)
	} else {
		fmt.Printf("bob 治疗 20：newHP=%d\n", healReply.NewHP)
	}

	// 4. ActorRef：同组 Actor 之间直连（RefPost + RefCall）
	fmt.Println("\n=== 4. Call: 关注（ActorRef） ===")
	if _, err := actor.Call(ctx, mgr, player1, &Follow{Target: player2}); err != nil {
		fmt.Printf("关注失败: %v\n", err)
	}

	// 5. Timer：每 300ms 回血一次，等两轮后观察
	fmt.Println("\n=== 5. Timer: 等待回血 ===")
	time.Sleep(700 * time.Millisecond)
	if reply, err := actor.Call(ctx, mgr, player1, &Attack{Damage: 0}); err == nil {
		fmt.Printf("alice 回血后 HP=%d\n", reply.RemainingHP)
	}

	// 6. RequestHandler：Logout 自带 Handle，处理逻辑里 Quit
	fmt.Println("\n=== 6. Post: bob 下线 ===")
	_ = actor.Post(mgr, player2, &Logout{})
	time.Sleep(100 * time.Millisecond)

	count, _ := actor.Count[PlayerId](mgr)
	fmt.Printf("在线玩家：%d\n", count)

	// 7. Broadcast：关闭 Group 中所有 Actor
	fmt.Println("\n=== 7. Broadcast: 全部下线 ===")
	_, _ = actor.Broadcast(mgr, &Close{})
	time.Sleep(100 * time.Millisecond)

	count, _ = actor.Count[PlayerId](mgr)
	fmt.Printf("在线玩家：%d\n", count)

	// 8. Manager 关闭：停止接收新消息并等待所有 Actor 退出
	mgr.CloseManager()
	mgr.JoinManager()
	fmt.Println("\n=== Done ===")
}

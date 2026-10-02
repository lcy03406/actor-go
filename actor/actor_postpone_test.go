package actor_test

import (
	"fmt"
	"log/slog"
	"sync/atomic"
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// ============================================================
// 延后队列（postpone）的 flush 语义
//
// 队列挂在 ActorControl 上，即**本次生命周期**：目标信箱满 ⇒ 消息延后，重试原本
// 只由发送方下一次发消息被动驱动。本组用例锁住两条兜底：①队列非空时的周期性
// flush（发送方此后不发消息也能补投）；②生命周期结束（Quit）时的最后一次 flush
// ——发不出去的部分就此终结，不静默，也不带入新生命周期。
// ============================================================

// PostponeTargetId 目标 actor 的 id：所属 Group 信箱容量为 1，用于稳定制造 busy。
type PostponeTargetId struct{ N int }

func (id PostponeTargetId) ActorType() actor.ActorType { return "PostponeTargetId" }
func (id PostponeTargetId) String() string             { return fmt.Sprintf("PostponeTarget(%d)", id.N) }

// ppBlock 占住目标 actor 的处理线程（信箱容量 1 被随后的第一条 ping 占满）。
type ppBlock struct {
	Running chan struct{}
	Release chan struct{}
}

func (*ppBlock) ReqType(_ PostponeTargetId, _ actor.OkReply) string { return "PPBlock" }
func (req *ppBlock) Handle(a *actor.ActorContext[PostponeTargetId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	if req.Running != nil {
		close(req.Running)
	}
	<-req.Release
	return actor.OK, nil
}

// ppPing 到达即计一次数。
type ppPing struct {
	Counter *int32
}

func (*ppPing) ReqType(_ PostponeTargetId, _ actor.OkReply) string { return "PPPing" }
func (req *ppPing) Handle(_ *actor.ActorContext[PostponeTargetId, TestActorData], _ bool) (actor.OkReply, error) {
	atomic.AddInt32(req.Counter, 1)
	return actor.OK, nil
}

// ppPost 向目标连发 N 条 ping：第一条进信箱，第二条起撞上满信箱 ⇒ 延后。
// Quit=true 时发完立即退出（模拟"最后一发"的发送方，如 Post 完即 Quit 的掉落路径）。
// Done（可选）在 handler 收尾时关闭，供测试确认"已发完"。
type ppPost struct {
	Target  PostponeTargetId
	Counter *int32
	N       int
	Quit    bool
	Done    chan struct{}
}

func (*ppPost) ReqType(_ TestActorId, _ actor.OkReply) string { return "PPPost" }
func (req *ppPost) Handle(a *actor.ActorContext[TestActorId, TestActorData], _ bool) (actor.OkReply, error) {
	a.Open()
	for i := 0; i < req.N; i++ {
		actor.APost(a.Control(), req.Target, &ppPing{Counter: req.Counter})
	}
	if req.Quit {
		a.Quit()
	}
	if req.Done != nil {
		close(req.Done)
	}
	return actor.OK, nil
}

func servePostponeGroups(mgr *actor.Manager) {
	actor.Serve(mgr, actor.Options{BufMails: 1}, func(b *actor.RegistryBuilder[PostponeTargetId, TestActorData]) {
		actor.RegisterServeHandler[PostponeTargetId, TestActorData, *ppBlock](b)
		actor.RegisterServeHandler[PostponeTargetId, TestActorData, *ppPing](b)
	})
	actor.Serve(mgr, actor.Options{BufMails: 100}, func(b *actor.RegistryBuilder[TestActorId, TestActorData]) {
		actor.RegisterServeHandler[TestActorId, TestActorData, *ppPost](b)
	})
}

// blockTarget 起一个被占住的目标 actor，返回"放开它"的 channel。
func blockTarget(t *testing.T, mgr *actor.Manager, id PostponeTargetId) chan struct{} {
	t.Helper()
	running := make(chan struct{})
	release := make(chan struct{})
	if err := actor.Post(mgr, id, &ppBlock{Running: running, Release: release}); err != nil {
		t.Fatalf("post ppBlock: %v", err)
	}
	<-running // 目标已占住处理线程，此后发往它的第二条消息必定撞上满信箱
	return release
}

// TestPostponeFlushedPeriodically 锁住周期性 flush：发送方此后**不再发任何消息**，
// 延后的那条仍应被补投出去。
func TestPostponeFlushedPeriodically(t *testing.T) {
	var counter int32
	mgr := actor.NewManager(slog.Default())
	servePostponeGroups(mgr)

	targetID := PostponeTargetId{N: 1}
	release := blockTarget(t, mgr, targetID)

	sender := TestActorId{ServerId: 1, OpenId: "pp-periodic"}
	if err := actor.Post(mgr, sender, &ppPost{Target: targetID, Counter: &counter, N: 2}); err != nil {
		t.Fatalf("post ppPost: %v", err)
	}
	time.Sleep(150 * time.Millisecond) // 长于一个 flush 周期，但目标被占住 ⇒ 补投必然失败
	if got := atomic.LoadInt32(&counter); got != 0 {
		t.Fatalf("目标仍被占住，不应收到 ping：got %d, want 0", got)
	}

	close(release) // 放开目标：延后的那条应由周期性 flush 补投
	deadline := time.Now().Add(2 * time.Second)
	for atomic.LoadInt32(&counter) < 2 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if got := atomic.LoadInt32(&counter); got != 2 {
		t.Fatalf("延后消息未被周期性 flush 补投：got %d, want 2", got)
	}
}

// TestPostponeTerminatesOnQuit 锁住生命周期终局：退出前的最后一次 flush 仍撞上满
// 信箱 ⇒ 该消息就此终结（不再投递），且不带入新生命周期。
func TestPostponeTerminatesOnQuit(t *testing.T) {
	var counter int32
	mgr := actor.NewManager(slog.Default())
	servePostponeGroups(mgr)

	targetID := PostponeTargetId{N: 2}
	release := blockTarget(t, mgr, targetID)

	sender := TestActorId{ServerId: 1, OpenId: "pp-quit"}
	done := make(chan struct{})
	if err := actor.Post(mgr, sender, &ppPost{Target: targetID, Counter: &counter, N: 2, Quit: true, Done: done}); err != nil {
		t.Fatalf("post ppPost: %v", err)
	}
	<-done
	// 等退出收尾（clear → 退出前 flush）跑完，再放开目标：此时延后的那条已经终结，
	// 目标腾出信箱也不会再有载体把它补投出去。
	time.Sleep(200 * time.Millisecond)

	close(release)
	time.Sleep(500 * time.Millisecond)
	if got := atomic.LoadInt32(&counter); got != 1 {
		t.Fatalf("退出后的延后消息不应再被投递：got %d, want 1", got)
	}
}

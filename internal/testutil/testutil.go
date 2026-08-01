// Package testutil 提供各包单元测试共享的脚手架辅助函数，
// 用于消除测试中重复的等待轮询、异步 Call 收结果等样板代码。
package testutil

import (
	"testing"
	"time"

	"github.com/lcy03406/actor-go/actor"
)

// DefaultSettle 是等待 Actor 异步处理完成的默认时长。
// 多数测试在 Post 一个 spawn 请求后等待该时长即可观察到 Actor 就绪。
const DefaultSettle = 50 * time.Millisecond

// Settle 短暂休眠，等待 Actor 异步处理（spawn / 关闭）落定。
// 传入 ms 可覆盖默认等待时长，例如 testutil.Settle(100) 等待 100ms。
func Settle(ms ...time.Duration) {
	d := DefaultSettle
	if len(ms) > 0 {
		d = ms[0]
	}
	time.Sleep(d)
}

// WaitCount 轮询 actor.Count[A]，直到其等于 want 或超时。
// 失败时在 t 上记录错误并返回 false。
func WaitCount[A actor.ActorId](t *testing.T, mgr *actor.Manager, want int, timeout time.Duration) bool {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		n, _ := actor.Count[A](mgr)
		if n == want {
			return true
		}
		if time.Now().After(deadline) {
			t.Errorf("WaitCount: expected %d actors, got %d after %v", want, n, timeout)
			return false
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// WaitStop 等待指定 Group 的 Actor 全部关闭（Count==0）。
func WaitStop[A actor.ActorId](t *testing.T, mgr *actor.Manager, timeout time.Duration) bool {
	return WaitCount[A](t, mgr, 0, timeout)
}

// CallResult 是 GoCall 返回的异步调用结果。
type CallResult[R any] struct {
	Reply R
	Err   error
}

// GoCall 在独立 goroutine 中执行一次调用，并通过 channel 返回其结果，
// 用于替代测试中重复的 `make(chan callRes, 1)` + `go func(){...}()` 样板。
func GoCall[R any](fn func() (R, error)) <-chan CallResult[R] {
	ch := make(chan CallResult[R], 1)
	go func() {
		reply, err := fn()
		ch <- CallResult[R]{Reply: reply, Err: err}
	}()
	return ch
}

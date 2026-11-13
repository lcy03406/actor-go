package actor_test

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"gopkg.in/natefinch/lumberjack.v2"
)

// init 配置默认 slog 日志，由 BENCH_LOGGER 环境变量切换：
//   - none：handler 级别设为 127（高于所有级别），Enabled 永远 false。
//     框架每次日志调用在「级别判断」处立即返回——不构建记录、不格式化、不落盘。
//     衡量「框架日志调用 + 级别判断」的纯逻辑开销（对照基线）。
//   - discard：仍按 Debug 级构建每条 slog 记录（属性解析 + 文本格式化），写入 io.Discard。
//     用于对比 lumberjack（JSON 格式化 + 轮转落盘）带来的额外性能开销。
//   - async：lumberjack JSON 轮转落盘，但包一层 channel + 后台 goroutine 异步写出，
//     把 JSON 序列化 + 文件 I/O 移出关键路径，用于衡量「异步化能回收多少 lumberjack 开销」。
//   - 默认（或 lumberjack）：与实际项目 server/app/server.go 的 initLogger 同款——
//     JSON 格式 + lumberjack 轮转文件落盘，控制台仅保留 error 级（quiet 模式）。
func init() {
	switch os.Getenv("BENCH_LOGGER") {
	case "none":
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.Level(127)})))
		return
	case "discard":
		slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelDebug})))
		return
	}

	logDir := "bench_logs"
	_ = os.MkdirAll(logDir, 0o755)
	filename := filepath.Join(logDir, "bench-"+time.Now().Format("20060102-150405.000")+".log")

	fileWriter := &lumberjack.Logger{
		Filename:   filename,
		MaxSize:    100, // MB，与实际项目一致
		MaxBackups: 5,   // benchmark 每条消息都打 INFO，量极大，封顶保留最近 5 份轮转文件
		Compress:   true,
	}

	fileHandler := slog.NewJSONHandler(fileWriter, &slog.HandlerOptions{Level: slog.LevelDebug})
	consoleHandler := slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelError})

	if os.Getenv("BENCH_LOGGER") == "async" {
		// 文件 JSON handler 走异步：关键路径只做记录克隆 + 入队，序列化与落盘交给后台 goroutine。
		asyncFile := newAsyncHandler(fileHandler, 1<<16)
		slog.SetDefault(slog.New(slog.NewMultiHandler(asyncFile, consoleHandler)))
		return
	}

	handler := slog.NewMultiHandler(fileHandler, consoleHandler)
	slog.SetDefault(slog.New(handler))
}

// asyncItem 携带「待写出的记录」与「负责写出的底层 handler」。
// 之所以把 inner 一起入队，是因为 slog 的 WithAttrs/WithGroup 会派生出携带不同 inner 的
// 子 handler；后台 goroutine 必须按入队时的 inner 写出，否则 With 附加的属性会丢失。
type asyncItem struct {
	rec   slog.Record
	inner slog.Handler
}

// asyncHandler 把 slog 记录克隆后投入带缓冲 channel，由后台 goroutine 异步写出，
// 从而把 JSON 序列化 + 文件 I/O 移出关键路径。
type asyncHandler struct {
	ch    chan asyncItem
	inner slog.Handler
}

func newAsyncHandler(inner slog.Handler, buf int) *asyncHandler {
	h := &asyncHandler{
		ch:    make(chan asyncItem, buf),
		inner: inner,
	}
	go func() {
		for it := range h.ch {
			_ = it.inner.Handle(context.Background(), it.rec)
		}
	}()
	return h
}

func (h *asyncHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *asyncHandler) Handle(_ context.Context, rec slog.Record) error {
	h.ch <- asyncItem{rec: rec.Clone(), inner: h.inner}
	return nil
}

func (h *asyncHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &asyncHandler{ch: h.ch, inner: h.inner.WithAttrs(attrs)}
}

func (h *asyncHandler) WithGroup(name string) slog.Handler {
	return &asyncHandler{ch: h.ch, inner: h.inner.WithGroup(name)}
}

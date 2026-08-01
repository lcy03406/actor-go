package logger_test

import (
	"log/slog"
	"os"
	"testing"

	"github.com/lcy03406/actor-go/logger"
)

// 使用示例
func TestLogger(t *testing.T) {
	handler := logger.NewTracingHandler(os.Stdout, slog.LevelInfo)
	logger := slog.New(handler)

	// 设置标签（通过 With）
	logger = logger.With("app", "demo", "env", "prod")

	// 输出日志：时间 级别 标签 消息 参数
	logger.Info("user login", "user_id", 12345, "ip", "192.168.1.1")
}

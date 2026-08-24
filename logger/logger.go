package logger

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
)

const TagAttr = "logtag"

// TracingHandler 实现 slog.Handler 接口
type TracingHandler struct {
	tag string
	lvl slog.Leveler
	out io.Writer // 输出目标
}

// NewTracingHandler 创建新的 Handler
func NewTracingHandler(out io.Writer, leveler slog.Leveler) *TracingHandler {
	return &TracingHandler{
		tag: "",
		lvl: leveler,
		out: out,
	}
}

// Enabled 控制日志级别
func (h *TracingHandler) Enabled(_ context.Context, level slog.Level) bool {
	return h.lvl.Level() <= level
}

// Handle 处理单条日志记录
func (h *TracingHandler) Handle(_ context.Context, r slog.Record) error {
	parts := make([]string, 0, 4+r.NumAttrs())
	// 1. 时间：本地时间，格式 "Jan 2 15:04:05.000"（省略年份）
	timeStr := r.Time.Local().Format("0102.150405.000")
	parts = append(parts, timeStr)

	// 2. 级别：三字母缩写
	levelStr := levelAbbr(r.Level)
	parts = append(parts, levelStr)

	// 4. 标签
	parts = append(parts, h.tag)

	// 4. 消息
	parts = append(parts, r.Message)

	// 5. 参数：Record 中的属性
	r.Attrs(func(a slog.Attr) bool {
		parts = append(parts, fmt.Sprintf("%s=%s", a.Key, a.Value.String()))
		return true
	})

	// 组装最终输出（各部分用空格分隔）
	line := strings.Join(parts, " ") + "\n"
	_, err := h.out.Write([]byte(line))
	return err
}

// WithAttrs 实现 With 添加属性（标签）
func (h *TracingHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	parts := make([]string, 0, 1+len(attrs))
	if len(h.tag) > 0 {
		parts = append(parts, h.tag)
	}
	// 返回新 Handler，合并原有标签与新传入的属性
	for _, attr := range attrs {
		parts = append(parts, attr.Value.String())
	}
	tag := strings.Join(parts, ".")
	return &TracingHandler{
		tag: tag,
		lvl: h.lvl,
		out: h.out,
	}
}

// WithGroup 暂不实现（可按需扩展）
func (h *TracingHandler) WithGroup(name string) slog.Handler {
	// 简单忽略，或 panic
	return h
}

// levelAbbr 将 slog.Level 转为三字母大写缩写
func levelAbbr(l slog.Level) string {
	switch l {
	case slog.LevelDebug:
		return "DEB"
	case slog.LevelInfo:
		return "INF"
	case slog.LevelWarn:
		return "WRN"
	case slog.LevelError:
		return "ERR"
	default:
		// 其他级别（如更高）简单转为大写并截取前4字符？这里返回原字符串
		return l.String()
	}
}

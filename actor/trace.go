package actor

import (
	"iter"
	"log/slog"
	"strings"

	"github.com/domonda/go-pretty"
)

var brief = pretty.Printer{
	MaxStringLength: 200, // 字符串最长200字符
	MaxSliceLength:  20,  // 切片最多显示20个元素
}

var verbose = pretty.Printer{}

// Seq 包装 iter.Seq，使其实现 pretty.Printable。
type Seq[A any] iter.Seq[A]

// Print 实现 pretty.Printable 接口。
func (s Seq[A]) Print(pr *pretty.Printer) string {
	if s == nil {
		return "nil"
	}

	limit := pr.MaxSliceLength
	if limit <= 0 {
		// 如果未设置限制，使用一个合理默认值（例如 100）
		limit = 100
	}

	var elems []string
	count := 0
	for v := range Seq[A](s) {
		if count >= limit {
			break // 达到限制，停止消费
		}
		// 使用 printer 格式化每个元素，保持一致的风格
		elems = append(elems, pr.Sprint(v))
		count++
	}

	if count == 0 {
		return "[]"
	}

	// 构建结果字符串
	if count < limit {
		// 未截断，完整输出
		return "[" + strings.Join(elems, ", ") + "]"
	}
	// 截断，显示省略号
	return "[" + strings.Join(elems, ", ") + ", ...]"
}

func traceLogSend(option TraceOption, logger *slog.Logger, title string, to any, reqType string, req any) {
	switch option {
	case TraceNone:
		return
	case TraceHead:
		logger.Info(title, "to", to, "req", reqType)
	case TraceBrief:
		logger.Info(title, "to", to, "req", reqType, "msg", brief.Sprint(req))
	case TraceVerbose:
		logger.Info(title, "to", to, "req", reqType, "msg", verbose.Sprint(req))
	}
}

func traceLogRecv(option TraceOption, logger *slog.Logger, title string, from any, reqType string, req any) {
	switch option {
	case TraceNone:
		return
	case TraceHead:
		logger.Info(title, "from", from, "req", reqType)
	case TraceBrief:
		logger.Info(title, "from", from, "req", reqType, "msg", brief.Sprint(req))
	case TraceVerbose:
		logger.Info(title, "from", from, "req", reqType, "msg", verbose.Sprint(req))
	}
}

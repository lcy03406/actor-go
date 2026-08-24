package actor

import (
	"log/slog"

	"github.com/domonda/go-pretty"
)

var brief = pretty.Printer{
	MaxStringLength: 200, // 字符串最长200字符
	MaxSliceLength:  20,  // 切片最多显示20个元素
}

var verbose = pretty.Printer{}

func traceLogSend(option TraceOption, logger *slog.Logger, title string, to any, reqType string, req any) {
	switch option {
	case TraceNone:
		return
	case TraceHead:
		logger.Info(title, "to", brief.Sprint(to), "req", reqType)
	case TraceBrief:
		logger.Info(title, "to", brief.Sprint(to), "req", reqType, "msg", brief.Sprint(req))
	case TraceVerbose:
		logger.Info(title, "to", verbose.Sprint(to), "req", reqType, "msg", verbose.Sprint(req))
	}
}

func traceLogRecv(option TraceOption, logger *slog.Logger, title string, reqType string, req any) {
	switch option {
	case TraceNone:
		return
	case TraceHead:
		logger.Info(title, "req", reqType)
	case TraceBrief:
		logger.Info(title, "req", reqType, "msg", brief.Sprint(req))
	case TraceVerbose:
		logger.Info(title, "req", reqType, "msg", verbose.Sprint(req))
	}
}

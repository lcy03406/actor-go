package actor

import (
	"log/slog"
)

func traceLogSend(option TraceOption, logger *slog.Logger, title string, next From, to any, reqType string, req any) {
	switch option {
	case TraceNone:
		return
	case TraceHead:
		logger.Info(title, "next", next.String(), "to", to, "req", reqType)
	case TraceBrief:
		logger.Info(title, "next", next.String(), "to", to, "req", reqType, "msg", req)
	case TraceVerbose:
		logger.Info(title, "next", next.String(), "to", to, "req", reqType, "msg", req)
	}
}

package actor

import (
	"log/slog"
)

type From struct {
	Origin string `json:"origin,omitempty"`  //初始来源
	ReqSeq string `json:"req_seq,omitempty"` //请求序号
}

func (f From) String() string {
	return f.Origin + ":" + f.ReqSeq
}

func OriginFrom(origin string) From {
	return From{Origin: origin, ReqSeq: origin}
}

func MakeFrom[A ActorId](id A, seq string) From {
	reqSeq := actorNameOf(id) + "." + seq
	return From{Origin: reqSeq, ReqSeq: reqSeq}
}

func traceLogSend(logger *slog.Logger, title string, next From, to any, reqType string, req any) {
	logger.Info(title, "next", next.String(), "to", to, "req", reqType, "msg", req)
}

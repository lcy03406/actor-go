package rpc

import (
	"github.com/lcy03406/actor-go/actor"
)

// Message 是编解码用的消息包。例如json.RawMessage、proto.Message。
type Message interface{}

type Codec[M Message] interface {
	Decode(data M, v any) error
	Encode(v any) (M, error)
}

type Transport[M Message] interface {
	DecodeReq(data []byte) (seq uint64, method string, actorType actor.ActorType, reqType string, idM M, idsM []M, reqM M, err error)
	DecodeRep(data []byte) (seq uint64, repM M, rerr string, err error)
	EncodeReq(seq uint64, method string, actorType actor.ActorType, reqType string, idM M, idsM []M, reqM M) (data []byte, err error)
	EncodeRep(seq uint64, repM M, rerr string) (data []byte, err error)
}

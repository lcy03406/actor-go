package rpc

import (
	"github.com/lcy03406/actor-go/actor"
)

// Message 是编解码用的消息包。例如json.RawMessage、proto.Message。
type Message interface{}

// Codec 是消息编解码器接口，负责在字节/消息体与具体 Go 值之间转换。
// M 为底层消息载体类型（如 json.RawMessage、proto.Message）。
// Decode 将消息体反序列化为目标值 v；Encode 将 Go 值序列化为消息体。
type Codec[M Message] interface {
	Decode(data M, v any) error
	Encode(v any) (M, error)
}

// Transport 是 RPC 传输层编解码接口，负责在字节流与框架请求/响应协议之间转换。
// M 为消息载体类型。它解析并组装包含序列号、方法名、Actor 类型、请求类型、
// Actor ID（单个/批量）及请求体的信封，使 rpc 层与具体网络/序列化格式解耦。
type Transport[M Message] interface {
	DecodeReq(data []byte) (seq uint64, method string, actorType actor.ActorType, reqType string, idM M, idsM []M, reqM M, err error)
	DecodeRep(data []byte) (seq uint64, repM M, rerr string, err error)
	EncodeReq(seq uint64, method string, actorType actor.ActorType, reqType string, idM M, idsM []M, reqM M) (data []byte, err error)
	EncodeRep(seq uint64, repM M, rerr string) (data []byte, err error)
}

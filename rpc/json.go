package rpc

import (
	"encoding/json"

	"github.com/lcy03406/actor-go/actor"
)

// JsonServer 是基于 JSON 编码的 rpc.Server 便捷别名，直接可用而无需显式指定泛型参数。
type JsonServer = Server[json.RawMessage, JsonCodec, JsonTransport]
// JsonClient 是基于 JSON 编码的 rpc.Client 便捷别名，直接可用而无需显式指定泛型参数。
type JsonClient = Client[json.RawMessage, JsonCodec, JsonTransport]
// JsonRegBuilder 是基于 JSON 编码的 rpc.RegistryBuilder 便捷别名，用于 JSON 消息的注册。
type JsonRegBuilder = RegistryBuilder[json.RawMessage, JsonCodec]

type JsonCodec struct{}

func (j JsonCodec) Decode(data json.RawMessage, v any) error {
	return json.Unmarshal(data, v)
}
func (j JsonCodec) Encode(v any) (json.RawMessage, error) {
	return json.Marshal(v)
}

type jsonMessage struct {
	Seq       uint64            `json:"seq,omitempty"`
	Method    string            `json:"method"`
	ActorType actor.ActorType   `json:"actorType,omitempty"`
	ReqType   string            `json:"reqType"`
	ActorId   json.RawMessage   `json:"actorId,omitempty"`
	Targets   []json.RawMessage `json:"targets,omitempty"`
	Req       json.RawMessage   `json:"req"`
}

type jsonResponse struct {
	Seq   uint64          `json:"seq"`
	Reply json.RawMessage `json:"reply,omitempty"`
	Error string          `json:"error,omitempty"`
}

type JsonTransport struct{}

func (t JsonTransport) DecodeReq(data []byte) (seq uint64, method string, actorType actor.ActorType, reqType string, idM json.RawMessage, idsM []json.RawMessage, reqM json.RawMessage, err error) {
	var msg jsonMessage
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return
	}
	return msg.Seq, msg.Method, msg.ActorType, msg.ReqType, msg.ActorId, msg.Targets, msg.Req, err
}

func (t JsonTransport) DecodeRep(data []byte) (seq uint64, repM json.RawMessage, rerr string, err error) {
	var msg jsonResponse
	err = json.Unmarshal(data, &msg)
	if err != nil {
		return
	}
	return msg.Seq, msg.Reply, msg.Error, err
}

func (t JsonTransport) EncodeReq(seq uint64, method string, actorType actor.ActorType, reqType string, idM json.RawMessage, idsM []json.RawMessage, reqM json.RawMessage) (data []byte, err error) {
	msg := jsonMessage{seq, method, actorType, reqType, idM, idsM, reqM}
	return json.Marshal(msg)
}

func (t JsonTransport) EncodeRep(seq uint64, repM json.RawMessage, rerr string) (data []byte, err error) {
	msg := jsonResponse{seq, repM, rerr}
	return json.Marshal(msg)
}

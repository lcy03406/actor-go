package cluster

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"
)

// ErrRemoteCallFailed 表示远程调用失败。
var ErrRemoteCallFailed = errors.New("remote call failed")

// Transport 是集群内节点间通信的传输层接口。
//
// 负责将消息路由到目标节点。不同实现可以使用不同协议：
//   - HTTPTransport：用于测试和简单部署
//   - gRPC transport：用于生产环境（低延迟、流式传输）
//
// 典型用法：
//   - cluster.Resolve 返回 RouteForward 时，调用 transport.ForwardCall
//   - cluster.Resolve 返回 RouteLocal 时，直接调用本地 manager
type Transport interface {
	// ForwardCall 向目标节点转发 Call 请求并等待响应。
	ForwardCall(ctx context.Context, target Node, msg *RoutedMessage) (*RoutedReply, error)

	// ForwardPost 向目标节点转发 fire-and-forget 消息。
	ForwardPost(ctx context.Context, target Node, msg *RoutedMessage) error

	// ForwardBroadcast 向所有节点广播消息。
	ForwardBroadcast(ctx context.Context, targets NodeSet, msg *RoutedMessage) (int, error)
}

// RoutedMessage 是集群路由消息的通用格式。
// 编解码由具体的 Transport 实现负责。
type RoutedMessage struct {
	ActorType string          `json:"actor_type"`
	ActorId   json.RawMessage `json:"actor_id"`
	ReqType   string          `json:"req_type"`
	Req       json.RawMessage `json:"req"`
	Method    string          `json:"method"` // "call" / "post" / "broadcast"
}

// RoutedReply 是集群路由回复的通用格式。
type RoutedReply struct {
	Reply json.RawMessage `json:"reply"`
	Error string          `json:"error,omitempty"`
}

// HTTPTransport 是基于 HTTP 的集群传输实现，纯 forwarder。
//
// 适用于测试和简单部署。生产环境建议使用 gRPC 实现。
// 服务端路由处理由调用方在外部 HTTP server 中注册。
type HTTPTransport struct {
	client *http.Client
}

// NewHTTPTransport 创建一个 HTTP 传输。
func NewHTTPTransport() *HTTPTransport {
	return &HTTPTransport{
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

// ForwardCall 通过 HTTP POST 转发 Call 请求。
func (t *HTTPTransport) ForwardCall(ctx context.Context, target Node, msg *RoutedMessage) (*RoutedReply, error) {
	body, err := json.Marshal(msg)
	if err != nil {
		return nil, err
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://%s%s", target.Addr, "/cluster/"+msg.ActorType),
		io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("forward call to %s: %w", target.ID, err)
	}
	defer func() { _ = resp.Body.Close() }()

	var reply RoutedReply
	if err := json.NewDecoder(resp.Body).Decode(&reply); err != nil {
		return nil, err
	}
	if reply.Error != "" {
		return &reply, fmt.Errorf("%w: %s", ErrRemoteCallFailed, reply.Error)
	}
	return &reply, nil
}

// ForwardPost 通过 HTTP POST 转发 fire-and-forget 消息。
func (t *HTTPTransport) ForwardPost(ctx context.Context, target Node, msg *RoutedMessage) error {
	body, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, "POST",
		fmt.Sprintf("http://%s%s", target.Addr, "/cluster/"+msg.ActorType),
		io.NopCloser(bytes.NewReader(body)))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := t.client.Do(req)
	if err != nil {
		return fmt.Errorf("forward post to %s: %w", target.ID, err)
	}
	if err := resp.Body.Close(); err != nil {
		return fmt.Errorf("forward post to %s: close body: %w", target.ID, err)
	}
	return nil
}

// ForwardBroadcast 向所有节点广播消息。
func (t *HTTPTransport) ForwardBroadcast(ctx context.Context, targets NodeSet, msg *RoutedMessage) (int, error) {
	count := 0
	for _, target := range targets {
		if err := t.ForwardPost(ctx, target, msg); err != nil {
			// 广播模式忽略单个节点失败
			continue
		}
		count++
	}
	return count, nil
}

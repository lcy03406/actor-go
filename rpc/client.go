package rpc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lcy03406/actor-go/actor"

	"github.com/gorilla/websocket"
)

// ErrClientClosed 表示 RPC 客户端已关闭。
var ErrClientClosed = errors.New("rpc client closed")

// ErrRpcCallFailed 表示 RPC 调用失败。
var ErrRpcCallFailed = errors.New("rpc call failed")

type result[M Message] struct {
	repM  M
	Error string
}

// Client 是 RPC 客户端，连接远程 RPC Server 并代理 Actor 消息。
// A 和 S 是类型参数，与 Manager[A,S] 对应。
type Client[M Message, C Codec[M], T Transport[M]] struct {
	serverAddr   string
	conn         *websocket.Conn
	connMu       sync.Mutex
	closed       atomic.Bool
	pendingCalls map[uint64]chan result[M]
	pendingMu    sync.Mutex
	reqIDSeq     atomic.Uint64
	done         chan struct{}
	logger       *slog.Logger
}

// NewClient 创建一个新的 RPC Client。
func NewClient[M Message, C Codec[M], T Transport[M]](serverAddr string) *Client[M, C, T] {
	return &Client[M, C, T]{
		serverAddr:   serverAddr,
		pendingCalls: make(map[uint64]chan result[M]),
		done:         make(chan struct{}),
		logger:       slog.With("component", "RpcClient", "server", serverAddr),
	}
}

// Connect 连接到远程 RPC Server。
func (c *Client[M, C, T]) Connect() error {
	u := url.URL{Scheme: "ws", Host: c.serverAddr, Path: "/rpc"}
	c.logger.Info("connecting to RPC server", "url", u.String())

	conn, resp, err := websocket.DefaultDialer.DialContext(context.Background(), u.String(), nil)
	if err != nil {
		return fmt.Errorf("rpc client connect failed: %w", err)
	}
	if resp != nil && resp.Body != nil {
		_ = resp.Body.Close()
	}
	c.connMu.Lock()
	c.conn = conn
	c.connMu.Unlock()

	go c.readResponses()

	c.logger.Info("connected to RPC server")
	return nil
}

// Close 关闭连接，并通知所有 pending call 失败。
func (c *Client[M, C, T]) Close() {
	if !c.closed.CompareAndSwap(false, true) {
		return
	}
	c.logger.Info("closing RPC client")
	close(c.done)
	errReply := result[M]{Error: "connection closed"}
	c.pendingMu.Lock()
	for id, ch := range c.pendingCalls {
		ch <- errReply
		delete(c.pendingCalls, id)
	}
	c.pendingMu.Unlock()
	if c.conn != nil {
		_ = c.conn.Close()
	}
}

func (c *Client[M, C, T]) readResponses() {
	var t T
	for !c.closed.Load() {
		_, msgBytes, err := c.conn.ReadMessage()
		if err != nil {
			if !c.closed.Load() {
				c.logger.Warn("read response failed", "error", err)
				c.Close()
			}
			return
		}
		seq, repM, rerr, err := t.DecodeRep(msgBytes)
		if err != nil {
			c.logger.Error("unmarshal response failed", "error", err)
			continue
		}
		c.pendingMu.Lock()
		ch, ok := c.pendingCalls[seq]
		if ok {
			delete(c.pendingCalls, seq)
		}
		c.pendingMu.Unlock()
		if ok {
			res := result[M]{repM, rerr}
			ch <- res
		}
	}
}

func (c *Client[M, C, T]) newSeq() uint64 {
	return c.reqIDSeq.Add(1)
}

// Post 向远程 Actor 发送 fire-and-forget 消息。
func Post[M Message, C Codec[M], T Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](c *Client[M, C, T], id A, req Q) error {
	var t T
	var d C
	idM, err := d.Encode(id)
	if err != nil {
		return err
	}
	reqM, err := d.Encode(req)
	if err != nil {
		return err
	}
	data, err := t.EncodeReq(0, "post", id.ActorType(), req.ReqType(id, nil), idM, nil, reqM)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Call 向远程 Actor 发送请求，等待结果。
func Call[M Message, C Codec[M], T Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](ctx context.Context, c *Client[M, C, T], id A, req Q) (R, error) {
	var t T
	var d C
	idM, err := d.Encode(id)
	if err != nil {
		return nil, err
	}
	reqM, err := d.Encode(req)
	if err != nil {
		return nil, err
	}
	seq := c.newSeq()
	data, err := t.EncodeReq(seq, "call", id.ActorType(), req.ReqType(id, nil), idM, nil, reqM)
	if err != nil {
		return nil, err
	}

	ch := make(chan result[M], 1)
	c.pendingMu.Lock()
	c.pendingCalls[seq] = ch
	c.pendingMu.Unlock()

	defer func() {
		c.pendingMu.Lock()
		delete(c.pendingCalls, seq)
		c.pendingMu.Unlock()
	}()

	c.connMu.Lock()
	err = c.conn.WriteMessage(websocket.TextMessage, data)
	c.connMu.Unlock()
	if err != nil {
		return nil, err
	}

	select {
	case resp := <-ch:
		if resp.Error != "" {
			return nil, fmt.Errorf("%w: %s", ErrRpcCallFailed, resp.Error)
		}
		rep := new(R0)
		err = d.Decode(resp.repM, rep)
		if err != nil {
			return nil, err
		}
		return rep, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, ErrClientClosed
	}
}

// CallTimeout 向远程 Actor 发送请求并等待回复，带超时。
func CallTimeout[M Message, C Codec[M], T Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](ctx context.Context, c *Client[M, C, T], id A, req Q, timeout time.Duration) (R, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return Call(ctx, c, id, req)
}

// Broadcast 向远程所有 Actor 广播 fire-and-forget 消息。
func Broadcast[M Message, C Codec[M], T Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](c *Client[M, C, T], req Q) error {
	var t T
	var d C
	var id0 A
	reqM, err := d.Encode(req)
	if err != nil {
		return err
	}
	var idM M
	data, err := t.EncodeReq(0, "broadcast", id0.ActorType(), req.ReqType(id0, nil), idM, nil, reqM)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

// Multicast 向远程一组 Actor 发送 fire-and-forget 消息。
func Multicast[M Message, C Codec[M], T Transport[M], A actor.ActorId, Q actor.Request[A, R, Q0, R0], R actor.PtrReply[R0], Q0 any, R0 any](c *Client[M, C, T], targets []A, req Q) error {
	if len(targets) == 0 {
		return nil
	}
	var t T
	var d C
	idsM := make([]M, 0, len(targets))
	for _, id := range targets {
		idM, err := d.Encode(id)
		if err != nil {
			return err
		}
		idsM = append(idsM, idM)
	}
	reqM, err := d.Encode(req)
	if err != nil {
		return err
	}
	var idM M
	id0 := targets[0]
	data, err := t.EncodeReq(0, "multicast", id0.ActorType(), req.ReqType(id0, nil), idM, idsM, reqM)
	if err != nil {
		return err
	}
	c.connMu.Lock()
	defer c.connMu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, data)
}

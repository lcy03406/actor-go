package rpc

import (
	"context"
	"errors"
	"log/slog"
	"net/http"

	"github.com/lcy03406/actor-go/actor"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// Server 是 RPC 服务端。
type Server[M Message, C Codec[M], T Transport[M]] struct {
	mgr      *actor.Manager
	addr     string
	entryMap map[registryKey]entry[M, C]
	server   *http.Server
	logger   *slog.Logger
}

// NewServer 创建一个新的 RPC Server。
func NewServer[M Message, C Codec[M], T Transport[M]](addr string, mgr *actor.Manager, reg func(*RegistryBuilder[M, C])) *Server[M, C, T] {
	b := &RegistryBuilder[M, C]{entryMap: make(map[registryKey]entry[M, C])}
	reg(b)
	return &Server[M, C, T]{
		mgr:      mgr,
		addr:     addr,
		entryMap: b.entryMap,
		logger:   slog.With("component", "RpcServer"),
	}
}

// Start 启动 RPC 服务（非阻塞）。
func (s *Server[M, C, T]) Start() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", s.handleRPC)
	s.server = &http.Server{Addr: s.addr, Handler: mux}
	s.logger.Info("RPC server starting", "addr", s.addr)
	go func() {
		if err := s.server.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			s.logger.Error("RPC server error", "error", err)
		}
	}()
	return nil
}

// Run 启动 RPC 服务（阻塞）。
func (s *Server[M, C, T]) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/rpc", s.handleRPC)
	s.server = &http.Server{Addr: s.addr, Handler: mux}
	s.logger.Info("RPC server running", "addr", s.addr)
	return s.server.ListenAndServe()
}

// Shutdown 优雅关闭 RPC 服务。
func (s *Server[M, C, T]) Shutdown(ctx context.Context) error {
	if s.server != nil {
		s.logger.Info("RPC server shutting down")
		return s.server.Shutdown(ctx)
	}
	return nil
}

func (s *Server[M, C, T]) handleRPC(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		s.logger.Error("websocket upgrade failed", "error", err)
		return
	}
	defer func() { _ = conn.Close() }()

	s.logger.Info("RPC client connected", "remote", r.RemoteAddr)

	for {
		_, msgBytes, err := conn.ReadMessage()
		if err != nil {
			s.logger.Info("RPC client disconnected", "error", err)
			break
		}
		var t T
		seq, method, actorType, reqType, idM, idsM, reqM, err := t.DecodeReq(msgBytes)
		if err != nil {
			s.logger.Error("failed to unmarshal RPC message", "error", err)
			continue
		}
		key := registryKey{actorType, reqType}
		entry, ok := s.entryMap[key]
		if !ok {
			s.sendError(conn, seq, "unknown reqType")
			continue
		}
		var repM M
		switch method {
		case "post":
			err = entry.post(s.mgr, idM, reqM)
		case "call":
			repM, err = entry.call(context.Background(), s.mgr, idM, reqM)
		case "broadcast":
			_, err = entry.broadcast(s.mgr, reqM)
		case "multicast":
			_, err = entry.multicast(s.mgr, idsM, reqM)
		default:
			s.sendError(conn, seq, "unknown method")
			continue
		}
		if seq != 0 {
			var errMsg string
			if err != nil {
				errMsg = err.Error()
			}
			respBytes, encErr := t.EncodeRep(seq, repM, errMsg)
			if encErr != nil {
				s.sendError(conn, seq, encErr.Error())
				continue
			}
			_ = conn.WriteMessage(websocket.TextMessage, respBytes)
		}
	}
}

func (s *Server[M, C, T]) sendError(conn *websocket.Conn, seq uint64, errMsg string) {
	if seq == 0 {
		return
	}
	var t T
	var repM M
	respBytes, _ := t.EncodeRep(seq, repM, errMsg)
	_ = conn.WriteMessage(websocket.TextMessage, respBytes)
}

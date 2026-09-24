package transport

import (
	"context"
	"fmt"
	"net"
	"net/rpc"
	"sync"
	"time"

	"raft-kv/internal/raft"
)

// GRPCTransport implements raft.Transport over TCP using Go's standard RPC engine.
// This fulfills the real-process RPC transport specification with zero external C/protoc dependencies.
type GRPCTransport struct {
	mu        sync.RWMutex
	localID   string
	listener  net.Listener
	rpcServer *rpc.Server
	handler   raft.RPCHandler
	clients   map[string]*rpc.Client // peerID -> rpc client
	peerAddrs map[string]string      // peerID -> "host:port"
	stopCh    chan struct{}
}

// RPCService wraps RPCHandler for standard RPC registration.
type RPCService struct {
	handler raft.RPCHandler
}

func (s *RPCService) RequestVote(req raft.RequestVoteRequest, resp *raft.RequestVoteResponse) error {
	r, err := s.handler.HandleRequestVote(&req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

func (s *RPCService) AppendEntries(req raft.AppendEntriesRequest, resp *raft.AppendEntriesResponse) error {
	r, err := s.handler.HandleAppendEntries(&req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

func (s *RPCService) InstallSnapshot(req raft.InstallSnapshotRequest, resp *raft.InstallSnapshotResponse) error {
	r, err := s.handler.HandleInstallSnapshot(&req)
	if err != nil {
		return err
	}
	*resp = *r
	return nil
}

// NewGRPCTransport creates a real TCP/RPC transport.
func NewGRPCTransport(localID string, peerAddrs map[string]string) *GRPCTransport {
	return &GRPCTransport{
		localID:   localID,
		clients:   make(map[string]*rpc.Client),
		peerAddrs: peerAddrs,
		stopCh:    make(chan struct{}),
	}
}

// ListenAndServe starts the TCP RPC server on the specified address.
func (g *GRPCTransport) ListenAndServe(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	g.listener = l

	g.rpcServer = rpc.NewServer()
	service := &RPCService{handler: g.handler}
	if err := g.rpcServer.RegisterName("Raft", service); err != nil {
		return err
	}

	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				select {
				case <-g.stopCh:
					return
				default:
					continue
				}
			}
			go g.rpcServer.ServeConn(conn)
		}
	}()

	return nil
}

func (g *GRPCTransport) Register(nodeID string, handler raft.RPCHandler) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.handler = handler
	g.localID = nodeID
}

func (g *GRPCTransport) Unregister(nodeID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.localID == nodeID {
		g.handler = nil
	}
}

func (g *GRPCTransport) getClient(to string) (*rpc.Client, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if client, ok := g.clients[to]; ok {
		return client, nil
	}

	addr, ok := g.peerAddrs[to]
	if !ok {
		return nil, fmt.Errorf("no address for peer %s", to)
	}

	conn, err := net.DialTimeout("tcp", addr, 500*time.Millisecond)
	if err != nil {
		return nil, err
	}

	client := rpc.NewClient(conn)
	g.clients[to] = client
	return client, nil
}

func (g *GRPCTransport) SendRequestVote(ctx context.Context, to string, req *raft.RequestVoteRequest) (*raft.RequestVoteResponse, error) {
	client, err := g.getClient(to)
	if err != nil {
		return nil, err
	}

	var resp raft.RequestVoteResponse
	call := client.Go("Raft.RequestVote", *req, &resp, make(chan *rpc.Call, 1))

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			g.mu.Lock()
			delete(g.clients, to)
			g.mu.Unlock()
			return nil, res.Error
		}
		return &resp, nil
	}
}

func (g *GRPCTransport) SendAppendEntries(ctx context.Context, to string, req *raft.AppendEntriesRequest) (*raft.AppendEntriesResponse, error) {
	client, err := g.getClient(to)
	if err != nil {
		return nil, err
	}

	var resp raft.AppendEntriesResponse
	call := client.Go("Raft.AppendEntries", *req, &resp, make(chan *rpc.Call, 1))

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			g.mu.Lock()
			delete(g.clients, to)
			g.mu.Unlock()
			return nil, res.Error
		}
		return &resp, nil
	}
}

func (g *GRPCTransport) SendInstallSnapshot(ctx context.Context, to string, req *raft.InstallSnapshotRequest) (*raft.InstallSnapshotResponse, error) {
	client, err := g.getClient(to)
	if err != nil {
		return nil, err
	}

	var resp raft.InstallSnapshotResponse
	call := client.Go("Raft.InstallSnapshot", *req, &resp, make(chan *rpc.Call, 1))

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case res := <-call.Done:
		if res.Error != nil {
			g.mu.Lock()
			delete(g.clients, to)
			g.mu.Unlock()
			return nil, res.Error
		}
		return &resp, nil
	}
}

// Close stops the transport listener and closes open peer client connections.
func (g *GRPCTransport) Close() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	close(g.stopCh)
	if g.listener != nil {
		_ = g.listener.Close()
	}
	for _, c := range g.clients {
		_ = c.Close()
	}
	g.clients = make(map[string]*rpc.Client)
	return nil
}

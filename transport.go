package raftgrpc

import (
	"context"
	"net"
	pb "raftgrpc/proto"
	"sync"
	"time"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type peer struct {
	id     uint64
	addr   string
	port   string
	conn   *grpc.ClientConn
	client pb.RaftClient
}

type grpcTransport struct {
	sync.RWMutex
	// pb.UnimplementedRaftServer

	stopCh chan struct{}
	errCh  chan error
	peers  map[uint64]*peer

	rc     *raftNode
	logger *zap.Logger
}

// Start transport
func (gt *grpcTransport) Start(p *peer, logger *zap.Logger) error {
	lis, err := net.Listen("tcp", net.JoinHostPort(p.addr, p.port))
	if err != nil {
		return err
	}
	s := grpc.NewServer(
		grpc.StreamInterceptor(grpcMiddleware.ChainStreamServer(
			grpcZap.StreamServerInterceptor(logger),
			grpcRecovery.StreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(
			grpcZap.UnaryServerInterceptor(logger),
			grpcRecovery.UnaryServerInterceptor(),
		)),
	)
	pb.RegisterRaftServer(s, gt)
	go func() {
		if err := s.Serve(lis); err != nil {
			gt.errCh <- err
		}
	}()
	select {
	case <-gt.stopCh:
		logger.Info("stopping node grpc")
		s.Stop()
	case err := <-gt.errCh:
		return err
	}
	return nil
}

// Send raft msg
func (gt *grpcTransport) Send(ctx context.Context, msg *pb.SendReq) (*pb.SendResp, error) {
	err := gt.rc.Process(ctx, *msg.Msg)
	if err != nil {
		return nil, err
	}
	resp := pb.SendResp{
		Success: true,
	}
	return &resp, nil
}

// Stop transport
func (gt *grpcTransport) Stop() {
	gt.stopCh <- struct{}{}
}

// SendMsgs message
func (gt *grpcTransport) SendMsgs(messages []raftpb.Message) error {
	peers := gt.getPeers()
	for _, m := range messages {
		// If node is an active raft member send the message
		if p, ok := peers[m.To]; ok {
			req := pb.SendReq{
				Msg: &m,
			}
			_, err := p.client.Send(context.Background(), &req)
			if err != nil {
				gt.logger.Error("send msg", zap.Uint64("id", m.To), zap.String("url", net.JoinHostPort(p.addr, p.port)), zap.Error(err))
				gt.rc.ReportUnreachable(p.id)
			}
		}
	}
	return nil
}

// AddPeer a peer
func (gt *grpcTransport) AddPeer(node *pb.NodeInfo) error {
	conn, cli, err := gt.newClient(node)
	if err != nil {
		return err
	}
	p := peer{
		id:     node.ID,
		addr:   node.Addr,
		port:   node.Port,
		conn:   conn,
		client: cli,
	}
	p.conn = conn
	p.client = cli
	gt.Lock()
	defer gt.Unlock()
	gt.peers[p.id] = &p
	return nil
}

// RemovePeer a peer
func (gt *grpcTransport) RemovePeer(id uint64) {
	gt.Lock()
	defer gt.Unlock()
	if p, ok := gt.peers[id]; ok {
		delete(gt.peers, id)
		if p != nil && p.conn != nil {
			_ = p.conn.Close()
		}
	}
}

func (gt *grpcTransport) getPeer(id uint64) *peer {
	gt.RLock()
	defer gt.RUnlock()
	return gt.peers[id]
}

func (gt *grpcTransport) getPeers() map[uint64]*peer {
	gt.RLock()
	defer gt.RUnlock()
	ps := make(map[uint64]*peer, len(gt.peers))
	for k, v := range gt.peers {
		ps[k] = v
	}
	return ps
}

// new grpc client
func (gt *grpcTransport) newClient(node *pb.NodeInfo) (*grpc.ClientConn, pb.RaftClient, error) {
	opts := []grpcRetry.CallOption{
		grpcRetry.WithBackoff(grpcRetry.BackoffLinear(100 * time.Millisecond)),
		grpcRetry.WithMax(3),
	}
	conn, err := grpc.Dial(net.JoinHostPort(node.Addr, node.Port),
		grpc.WithStreamInterceptor(grpcRetry.StreamClientInterceptor(opts...)),
		grpc.WithUnaryInterceptor(grpcRetry.UnaryClientInterceptor(opts...)),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, err
	}
	cli := pb.NewRaftClient(conn)
	return conn, cli, nil
}

package raftgrpc

import (
	"net"
	pb "raftgrpc/proto"
	"sync"
	"time"

	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type peer struct {
	id     uint64
	addr   string
	port   string
	conn   *grpc.ClientConn
	client *pb.RaftClient
}

type grpcTransport struct {
	sync.RWMutex

	stopCh chan struct{}
	errCh  chan error
	peers  map[uint64]*peer
}

func (gt *grpcTransport) start(peer *peer, logger *zap.Logger) error {
	lis, err := net.Listen("tcp", net.JoinHostPort(peer.addr, peer.port))
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
	pb.RegisterRaftServer(s, nil)
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

func (gt *grpcTransport) stop() {
	gt.stopCh <- struct{}{}
}

func (gt *grpcTransport) addPeer(peer *peer) error {
	if peer.conn == nil {
		conn, cli, err := gt.newClient(peer)
		if err != nil {
			return err
		}
		peer.conn = conn
		peer.client = cli
	}
	gt.Lock()
	defer gt.Unlock()
	gt.peers[peer.id] = peer
	return nil
}

func (gt *grpcTransport) removePeer(id uint64) {
	gt.RLock()
	defer gt.Unlock()
	if p, ok := gt.peers[id]; ok {
		delete(gt.peers, id)
		if p != nil && p.conn != nil {
			_ = p.conn.Close()
		}
	}
}

func (gt *grpcTransport) send() {

}

// new grpc client
func (gt *grpcTransport) newClient(peer *peer) (*grpc.ClientConn, *pb.RaftClient, error) {
	opts := []grpcRetry.CallOption{
		grpcRetry.WithBackoff(grpcRetry.BackoffLinear(100 * time.Millisecond)),
		grpcRetry.WithMax(3),
	}
	conn, err := grpc.Dial(net.JoinHostPort(peer.addr, peer.port),
		grpc.WithStreamInterceptor(grpcRetry.StreamClientInterceptor(opts...)),
		grpc.WithUnaryInterceptor(grpcRetry.UnaryClientInterceptor(opts...)),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, nil, err
	}
	cli := pb.NewRaftClient(conn)
	return conn, &cli, nil
}

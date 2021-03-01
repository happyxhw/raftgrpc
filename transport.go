package raftgrpc

import (
	"net"
	pb "raftgrpc/proto"
	"sync"
	"time"

	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
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

	peers map[uint64]*peer
}

func (gt *grpcTransport) start() {

}

func (gt *grpcTransport) stop() {

}

func (gt *grpcTransport) addPeer(peer *peer) {

}

func (gt *grpcTransport) removePeer() {

}

func (gt *grpcTransport) send() {

}q

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

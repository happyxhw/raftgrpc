package raftgrpc

import (
	"context"
	"net"
	pb "raftgrpc/proto"
	"sync"
	"time"

	"github.com/golang/protobuf/proto"
	grpcMiddleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpcZap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"

	// grpcRecovery "github.com/grpc-ecosystem/go-grpc-middleware/recovery"
	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/happyxhw/gopkg/logger"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type peer struct {
	id     uint64
	addr   string
	conn   *grpc.ClientConn
	client pb.RaftClient
}

// GrpcTransport grpc transport layer
type GrpcTransport struct {
	sync.RWMutex

	stopCh        chan struct{}
	waitingStopCh chan struct{}
	peers         map[uint64]*peer

	raftNode *RaftNode
	logger   *zap.Logger
}

// NewGrpcTransport return grpc transport
func NewGrpcTransport(node *RaftNode, logger *zap.Logger) *GrpcTransport {
	return &GrpcTransport{
		stopCh:        make(chan struct{}),
		waitingStopCh: make(chan struct{}),
		peers:         make(map[uint64]*peer),
		raftNode:      node,
		logger:        logger,
	}
}

// KvAction kv store action
func (gt *GrpcTransport) KvAction(ctx context.Context, pair *pb.Pair) (*pb.KvResp, error) {
	var resp pb.KvResp
	switch pair.Action {
	case pb.Pair_INSERT, pb.Pair_UPDATE, pb.Pair_DELETE:
		return gt.putOrDel(ctx, pair)
	case pb.Pair_QUERY:
		return gt.get(ctx, pair)
	}
	return &resp, nil
}

// Start transport
func (gt *GrpcTransport) Start(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	s := grpc.NewServer(
		grpc.StreamInterceptor(grpcMiddleware.ChainStreamServer(
			grpcZap.StreamServerInterceptor(gt.logger),
			// grpcRecovery.StreamServerInterceptor(),
		)),
		grpc.UnaryInterceptor(grpcMiddleware.ChainUnaryServer(
			grpcZap.UnaryServerInterceptor(gt.logger),
			// grpcRecovery.UnaryServerInterceptor(),
		)),
	)
	pb.RegisterRaftServer(s, gt)
	errCh := make(chan error)
	go func() {
		if err := s.Serve(lis); err != nil {
			errCh <- err
		}
	}()
	select {
	case <-gt.stopCh:
		logger.Info("stopping transport")
		s.Stop()
		gt.waitingStopCh <- struct{}{}
	case err := <-errCh:
		return err
	}
	return nil
}

// Send raft msg
func (gt *GrpcTransport) Send(ctx context.Context, msg *pb.SendReq) (*pb.SendResp, error) {
	err := gt.raftNode.Process(ctx, *msg.Msg)
	if err != nil {
		return nil, err
	}
	resp := pb.SendResp{
		Success: true,
	}
	return &resp, nil
}

// Join a cluster
func (gt *GrpcTransport) Join(ctx context.Context, info *pb.NodeInfo) (*pb.JoinResp, error) {
	id := genId(info.Addr)
	byt, _ := proto.Marshal(info)
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeAddNode,
		NodeID:  id,
		Context: byt,
	}
	err := gt.raftNode.ProposeConfChange(context.Background(), cc)
	if err != nil {
		return nil, err
	}
	resp := &pb.JoinResp{
		Success: true,
	}
	return resp, nil
}

// Leave a cluster
func (gt *GrpcTransport) Leave(ctx context.Context, info *pb.NodeInfo) (*pb.LeaveResp, error) {
	id := genId(info.Addr)
	byt, _ := proto.Marshal(info)
	cc := raftpb.ConfChange{
		Type:    raftpb.ConfChangeRemoveNode,
		NodeID:  id,
		Context: byt,
	}
	err := gt.raftNode.ProposeConfChange(context.Background(), cc)
	if err != nil {
		return nil, err
	}
	resp := &pb.LeaveResp{
		Success: true,
	}
	return resp, nil
}

func (gt *GrpcTransport) putOrDel(ctx context.Context, pair *pb.Pair) (*pb.KvResp, error) {
	err := gt.raftNode.Propose(pair)
	if err != nil {
		return nil, err
	}
	return &pb.KvResp{Success: true}, nil
}

func (gt *GrpcTransport) get(ctx context.Context, pair *pb.Pair) (*pb.KvResp, error) {
	p, ok := gt.raftNode.LookUp(pair.Key)
	resp := pb.KvResp{}
	if ok {
		resp.Success = true
		resp.Pair = &pb.Pair{Key: p.Key, Value: p.Value}
	}
	// return &pb.KvResp{Success: true, Pair: p}, nil
	return &resp, nil
}

// Stop transport
func (gt *GrpcTransport) Stop() {
	gt.stopCh <- struct{}{}
	<-gt.waitingStopCh
}

// SendMsgList message
func (gt *GrpcTransport) SendMsgList(messages []raftpb.Message) error {
	peers := gt.getPeers()
	for _, m := range messages {
		if p, ok := peers[m.To]; ok {
			req := pb.SendReq{
				Msg: &m,
			}
			_, err := p.client.Send(context.Background(), &req)
			if err != nil {
				gt.logger.Error("send msg", zap.Uint64("id", m.To), zap.String("addr", p.addr), zap.Error(err))
				gt.raftNode.ReportUnreachable(p.id)
			}
		}
	}
	return nil
}

// AddPeer a peer
func (gt *GrpcTransport) AddPeer(node *pb.NodeInfo) error {
	conn, cli, err := gt.newClient(node)
	if err != nil {
		return err
	}
	p := peer{
		id:     node.Id,
		addr:   node.Addr,
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
func (gt *GrpcTransport) RemovePeer(id uint64) {
	gt.Lock()
	defer gt.Unlock()
	if p, ok := gt.peers[id]; ok {
		delete(gt.peers, id)
		if p != nil && p.conn != nil {
			_ = p.conn.Close()
		}
	}
}

func (gt *GrpcTransport) getPeer(id uint64) *peer {
	gt.RLock()
	defer gt.RUnlock()
	return gt.peers[id]
}

func (gt *GrpcTransport) getPeers() map[uint64]*peer {
	gt.RLock()
	defer gt.RUnlock()
	ps := make(map[uint64]*peer, len(gt.peers))
	for k, v := range gt.peers {
		ps[k] = v
	}
	return ps
}

// new grpc client
func (gt *GrpcTransport) newClient(node *pb.NodeInfo) (*grpc.ClientConn, pb.RaftClient, error) {
	opts := []grpcRetry.CallOption{
		grpcRetry.WithBackoff(grpcRetry.BackoffLinear(100 * time.Millisecond)),
		grpcRetry.WithMax(3),
	}
	conn, err := grpc.Dial(node.Addr,
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

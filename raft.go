package raftgrpc

import (
	"context"
	pb "raftgrpc/proto"
	"time"

	"github.com/happyxhw/gopkg/logger"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

// KvStore interface
type KvStore interface {
	Get(string) (string, bool)       // get a key
	Put(string, string) error        // put a pair key value
	Del(string) (string, error)      // delete a key
	Recover(*snap.Snapshotter) error // recover store when start up
	Snapshot() ([]byte, error)       // get store snapshot
}

// Transport interface
type Transport interface {
}

// Snapshot interface
type Snapshot interface {
	Load() (*raftpb.Snapshot, error)
}

// RaftNode raft node
type RaftNode struct {
	id    uint64 // node id
	join  bool   // node is joining an existing cluster
	addr  string
	peers []string

	// raft backing for the commit/error channel
	node             raft.Node
	snapshotterReady chan struct{} // signals when snapshotter is ready
	transport        *GrpcTransport
	snapHandler      *EtcdSnapshot
	stopCh           chan struct{}

	kvStore KvStore // signals proposal channel closed
}

var defaultSnapshotCount uint64 = 10000

// NewRaftNode initiates a raft instance
func NewRaftNode(addr string, join bool, peers []string, kv KvStore) *RaftNode {
	id := genId(addr)
	rn := &RaftNode{
		id:               id,
		addr:             addr,
		join:             join,
		peers:            peers,
		stopCh:           make(chan struct{}),
		snapshotterReady: make(chan struct{}, 1),

		kvStore: kv,
		// rest of structure populated after WAL replay
	}
	rn.snapHandler = NewEtcdSnapshot(rn.id, rn.kvStore.Snapshot)
	return rn
}

// Start start node
func (rn *RaftNode) Start() {
	go func() {
		<-rn.snapshotterReady
		if err := rn.kvStore.Recover(rn.snapHandler.snapshotter); err != nil {
			logger.Fatal("recover store snapshot", zap.Error(err))
		}
	}()
	rn.startRaft()
}

// processEntries writes committed log
func (rn *RaftNode) processEntries(entries []raftpb.Entry) ([][]byte, bool) {
	if len(entries) == 0 {
		return nil, true
	}
	data := make([][]byte, 0, len(entries))
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				// ignore empty messages
				break
			}
			data = append(data, entries[i].Data)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			_ = cc.Unmarshal(entries[i].Data)
			confState := *rn.node.ApplyConfChange(cc)

			// TODO
			rn.snapHandler.UpdateConfState(confState)

			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					var node pb.NodeInfo
					_ = node.Unmarshal(cc.Context)
					_ = rn.transport.AddPeer(&node)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rn.id {
					logger.Info("shutting down")
					return nil, false
				}
				rn.transport.RemovePeer(cc.NodeID)
			}
		}
	}
	// after commit, update appliedIndex
	// TODO
	rn.snapHandler.UpdateAppliedIndex(entries[len(entries)-1].Index)
	return data, true
}

// stop node
func (rn *RaftNode) stop() {
	rn.transport.Stop()
	rn.node.Stop()
}

// start node
func (rn *RaftNode) startRaft() {
	// signal replay has finished
	// TODOs
	oldWal := rn.snapHandler.ExistWal()
	if err := rn.snapHandler.initSnapshot(); err != nil {
		logger.Fatal("init snapshot", zap.Error(err))
	}
	rn.snapshotterReady <- struct{}{}

	rPeers := make([]raft.Peer, len(rn.peers))
	for i, p := range rn.peers {
		rPeers[i] = raft.Peer{ID: genId(p)}
	}
	// rPeers := raftNode.transport.getPeers()
	c := &raft.Config{
		ID:                        rn.id,
		ElectionTick:              2,
		HeartbeatTick:             1,
		Storage:                   rn.snapHandler.RaftStorage(),
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}
	if oldWal || rn.join {
		rn.node = raft.RestartNode(c)
	} else {
		rn.node = raft.StartNode(c, rPeers)
	}
	rn.transport = NewGrpcTransport(rn, logger.GetLogger())

	for _, p := range rn.peers {
		node := pb.NodeInfo{
			Id:   genId(p),
			Addr: p,
		}
		_ = rn.transport.AddPeer(&node)
	}
	go rn.ready()
	rn.transport.Start(rn.addr)
}

var snapshotCatchUpEntriesN uint64 = 10000

// Propose data
func (rn *RaftNode) Propose(p *pb.Pair) error {
	if p == nil {
		return nil
	}
	byt, err := p.Marshal()
	if err != nil {
		logger.Error("marshal", zap.Error(err))
		return err
	}
	return rn.node.Propose(context.TODO(), byt)
}

// LookUp a key
func (rn *RaftNode) LookUp(k string) (*pb.Pair, bool) {
	v, ok := rn.kvStore.Get(k)
	if !ok {
		return nil, false
	}
	return &pb.Pair{Key: k, Value: v}, true
}

// process for new propose && conf state change
func (rn *RaftNode) ready() {
	defer rn.snapHandler.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rn.node.Tick()
		// store raft entries to wal, then publish over commit channel
		case rd := <-rn.node.Ready():
			_ = rn.snapHandler.Save(rd)
			_ = rn.transport.SendMsgList(rd.Messages)
			data, ok := rn.processEntries(rn.snapHandler.EntriesToApply(rd.CommittedEntries))
			if !ok {
				rn.stop()
				return
			}
			// write data to store
			if len(data) > 0 {
				for _, item := range data {
					var kv pb.Pair
					if err := kv.Unmarshal(item); err != nil {
						logger.Error("unmarshal kv", zap.Error(err))
						continue
					}
					switch kv.Action {
					case pb.Pair_UPDATE, pb.Pair_INSERT:
						_ = rn.kvStore.Put(kv.Key, kv.Value)
					case pb.Pair_DELETE:
						_, _ = rn.kvStore.Del(kv.Key)
					}
				}
			}
			rn.snapHandler.MaybeTriggerSnapshot()
			rn.node.Advance()

		case <-rn.stopCh:
			rn.stop()
			return
		}
	}
}

func (rn *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rn.node.Step(ctx, m)
}
func (rn *RaftNode) ProposeConfChange(ctx context.Context, m raftpb.ConfChange) error {
	return rn.node.ProposeConfChange(ctx, m)
}
func (rn *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (rn *RaftNode) ReportUnreachable(id uint64) {
	rn.node.ReportUnreachable(id)
}
func (rn *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rn.node.ReportSnapshot(id, status)
}

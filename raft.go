package raftgrpc

import (
	"bytes"
	"context"
	"encoding/gob"
	pb "raftgrpc/proto"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.uber.org/zap"
)

type commit struct {
	data        []string
	applyDoneCh chan<- struct{}
}

// RaftNode raft node
type RaftNode struct {
	commitCh chan *commit // entries committed to log (k,v)
	errorCh  chan error   // errors from raft session

	id    uint64 // node id
	join  bool   // node is joining an existing cluster
	addr  string
	peers []string

	// raft backing for the commit/error channel
	node             raft.Node
	snapshotterReady chan struct{} // signals when snapshotter is ready
	transport        *GrpcTransport
	snapHandler      *Snapshot
	stopCh           chan struct{}

	kvStore *KvStore // signals proposal channel closed

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(addr string, join bool, peers []string, logger *zap.Logger) *RaftNode {

	commitCh := make(chan *commit)
	errorCh := make(chan error)
	id := genId(addr)
	rn := &RaftNode{
		commitCh:         commitCh,
		errorCh:          errorCh,
		id:               id,
		addr:             addr,
		join:             join,
		peers:            peers,
		stopCh:           make(chan struct{}),
		logger:           logger,
		snapshotterReady: make(chan struct{}, 1),
		// rest of structure populated after WAL replay
	}
	rn.snapHandler = NewSnapshot(id, rn.kvStore.GetSnapshot, commitCh, rn.stopCh, rn.logger)
	return rn
}

// Start start node
func (rn *RaftNode) Start() {
	go func() {
		<-rn.snapshotterReady
		rn.kvStore = NewKVStore(rn.snapHandler.Snapshotter(), rn.commitCh, rn.errorCh, rn.logger)
	}()
	rn.startRaft()
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rn *RaftNode) publishEntries(entries []raftpb.Entry) (<-chan struct{}, bool) {
	if len(entries) == 0 {
		return nil, true
	}

	data := make([]string, 0, len(entries))
	for i := range entries {
		switch entries[i].Type {
		case raftpb.EntryNormal:
			if len(entries[i].Data) == 0 {
				// ignore empty messages
				break
			}
			s := string(entries[i].Data)
			data = append(data, s)
		case raftpb.EntryConfChange:
			var cc raftpb.ConfChange
			_ = cc.Unmarshal(entries[i].Data)
			confState := *rn.node.ApplyConfChange(cc)
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
					rn.logger.Info("shutting down")
					return nil, false
				}
				rn.transport.RemovePeer(cc.NodeID)
			}
		}
	}

	var applyDoneCh chan struct{}

	if len(data) > 0 {
		applyDoneCh = make(chan struct{}, 1)
		select {
		case rn.commitCh <- &commit{data, applyDoneCh}:
		case <-rn.stopCh:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rn.snapHandler.UpdateAppliedIndex(entries[len(entries)-1].Index)
	return applyDoneCh, true
}

func (rn *RaftNode) writeError(err error) {
	rn.transport.Stop()
	close(rn.commitCh)
	rn.errorCh <- err
	close(rn.errorCh)
	rn.node.Stop()
}

// stop closes http, closes all channels, and stops raft.
func (rn *RaftNode) stop() {
	rn.transport.Stop()
	close(rn.commitCh)
	close(rn.errorCh)
	rn.node.Stop()
}

func (rn *RaftNode) startRaft() {
	// signal replay has finished
	oldWal := rn.snapHandler.ExistWal()
	rn.snapHandler.InitSnapshot()
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
	if oldWal || rn.join || len(rPeers) == 0 {
		rn.node = raft.RestartNode(c)
	} else {
		rn.node = raft.StartNode(c, rPeers)
	}
	rn.transport = NewGrpcTransport(rn, rn.logger)

	for _, p := range rn.peers {
		node := pb.NodeInfo{
			Id:   genId(p),
			Addr: p,
		}
		_ = rn.transport.AddPeer(&node)
	}
	go rn.serveChannels()
	rn.transport.Start(rn.addr)
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rn *RaftNode) Propose(p *pb.Pair) error {
	// err := rn.node.Propose(context.TODO(), data)
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(p); err != nil {
		rn.logger.Error("decode", zap.Error(err))
		return err
	}
	return rn.node.Propose(context.TODO(), buf.Bytes())
}

func (rn *RaftNode) LookUp(k string) (*pb.Pair, bool) {
	v, ok := rn.kvStore.LookUp(k)
	if !ok {
		return nil, false
	}
	return &pb.Pair{Key: k, Value: v}, true
}

// process for new propose && conf state change
func (rn *RaftNode) serveChannels() {
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
			rn.snapHandler.SaveToSnapshot(rd)
			_ = rn.transport.SendMsgList(rd.Messages)
			applyDoneCh, ok := rn.publishEntries(rn.snapHandler.EntriesToApply(rd.CommittedEntries))
			if !ok {
				rn.stop()
				return
			}
			rn.snapHandler.MaybeTriggerSnapshot(applyDoneCh)
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

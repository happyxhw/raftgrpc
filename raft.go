package raftgrpc

import (
	"context"
	"hash/fnv"
	"net"
	pb "raftgrpc/proto"
	"time"

	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

type commit struct {
	data        []string
	applyDoneCh chan<- struct{}
}

// RaftNode raft node
type RaftNode struct {
	proposeCh    <-chan string            // proposed messages (k,v)
	confChangeCh <-chan raftpb.ConfChange // proposed cluster config changes
	commitCh     chan<- *commit           // entries committed to log (k,v)
	errorCh      chan<- error             // errors from raft session

	id   uint64 // node id
	join bool   // node is joining an existing cluster

	peers []string

	// raft backing for the commit/error channel
	node             raft.Node
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready
	transport        *GrpcTransport
	snapHandler      *Snapshot
	stopCh           chan struct{} // signals proposal channel closed

	logger *zap.Logger
}

var defaultSnapshotCount uint64 = 10000

// NewRaftNode initiates a raft instance and returns a committed log entry
// channel and error channel. Proposals for log updates are sent over the
// provided the proposal channel. All log entries are replayed over the
// commit channel, followed by a nil message (to indicate the channel is
// current), then new log entries. To shutdown, close proposeC and read errorC.
func NewRaftNode(url string, join bool, peers []string, getSnapshotFn func() ([]byte, error), proposeCh <-chan string,
	confChangeCh <-chan raftpb.ConfChange, logger *zap.Logger) (<-chan *commit, <-chan error, <-chan *snap.Snapshotter) {

	commitCh := make(chan *commit)
	errorCh := make(chan error)
	id := genId(url)
	rc := &RaftNode{
		proposeCh:        proposeCh,
		confChangeCh:     confChangeCh,
		commitCh:         commitCh,
		errorCh:          errorCh,
		id:               id,
		join:             join,
		peers:            peers,
		stopCh:           make(chan struct{}),
		logger:           logger,
		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	rc.snapHandler = NewSnapshot(id, getSnapshotFn, commitCh, rc.stopCh, rc.logger)
	go rc.startRaft()
	return commitCh, errorCh, rc.snapshotterReady
}

// publishEntries writes committed log entries to commit channel and returns
// whether all entries could be published.
func (rc *RaftNode) publishEntries(entries []raftpb.Entry) (<-chan struct{}, bool) {
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
			confState := *rc.node.ApplyConfChange(cc)
			rc.snapHandler.UpdateConfState(confState)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					var node pb.NodeInfo
					_ = node.Unmarshal(cc.Context)
					_ = rc.transport.AddPeer(&node)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.id {
					rc.logger.Info("shutting down")
					return nil, false
				}
				rc.transport.RemovePeer(cc.NodeID)
			}
		}
	}

	var applyDoneCh chan struct{}

	if len(data) > 0 {
		applyDoneCh = make(chan struct{}, 1)
		select {
		case rc.commitCh <- &commit{data, applyDoneCh}:
		case <-rc.stopCh:
			return nil, false
		}
	}

	// after commit, update appliedIndex
	rc.snapHandler.UpdateAppliedIndex(entries[len(entries)-1].Index)
	return applyDoneCh, true
}

func (rc *RaftNode) writeError(err error) {
	rc.transport.Stop()
	close(rc.commitCh)
	rc.errorCh <- err
	close(rc.errorCh)
	rc.node.Stop()
}

// stop closes http, closes all channels, and stops raft.
func (rc *RaftNode) stop() {
	rc.transport.Stop()
	close(rc.commitCh)
	close(rc.errorCh)
	rc.node.Stop()
}

func (rc *RaftNode) startRaft() {
	// signal replay has finished
	oldWal := rc.snapHandler.ExistWal()
	rc.snapHandler.InitSnapshot()
	rc.snapshotterReady <- rc.snapHandler.Snapshotter()

	rPeers := make([]raft.Peer, len(rc.peers))
	for i, p := range rc.peers {
		rPeers[i] = raft.Peer{ID: genId(p)}
	}
	// rPeers := raftNode.transport.getPeers()
	c := &raft.Config{
		ID:                        rc.id,
		ElectionTick:              2,
		HeartbeatTick:             1,
		Storage:                   rc.snapHandler.RaftStorage(),
		MaxSizePerMsg:             1024 * 1024,
		MaxInflightMsgs:           256,
		MaxUncommittedEntriesSize: 1 << 30,
	}

	if oldWal || rc.join {
		rc.node = raft.RestartNode(c)
	} else {
		rc.node = raft.StartNode(c, rPeers)
	}
	rc.transport = NewGrpcTransport(rc, rc.logger)

	for _, p := range rc.peers {
		host, port, _ := net.SplitHostPort(p)
		node := pb.NodeInfo{
			ID:   genId(p),
			Addr: host,
			Port: port,
		}
		_ = rc.transport.AddPeer(&node)
	}
	go rc.transport.Start(rc.transport.getPeer(rc.id))
	go rc.serveChannels()
}

var snapshotCatchUpEntriesN uint64 = 10000

// process for new propose && conf state change
func (rc *RaftNode) serveChannels() {
	defer rc.snapHandler.Close()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// send proposals over raft
	go func() {
		confChangeCount := uint64(0)

		for rc.proposeCh != nil && rc.confChangeCh != nil {
			select {
			case prop, ok := <-rc.proposeCh:
				if !ok {
					rc.proposeCh = nil
				} else {
					// blocks until accepted by raft state machine
					_ = rc.node.Propose(context.TODO(), []byte(prop))
				}

			case cc, ok := <-rc.confChangeCh:
				if !ok {
					rc.confChangeCh = nil
				} else {
					confChangeCount++
					cc.ID = confChangeCount
					_ = rc.node.ProposeConfChange(context.TODO(), cc)
				}
			}
		}
		// client closed channel; shutdown raft if not already
		close(rc.stopCh)
	}()
	// event loop on raft state machine updates
	for {
		select {
		case <-ticker.C:
			rc.node.Tick()
		// store raft entries to wal, then publish over commit channel
		case rd := <-rc.node.Ready():
			rc.snapHandler.SaveToSnapshot(rd)
			_ = rc.transport.SendMsgList(rd.Messages)
			applyDoneCh, ok := rc.publishEntries(rc.snapHandler.EntriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			rc.snapHandler.MaybeTriggerSnapshot(applyDoneCh)
			rc.node.Advance()

		case <-rc.stopCh:
			rc.stop()
			return
		}
	}
}

func (rc *RaftNode) Process(ctx context.Context, m raftpb.Message) error {
	return rc.node.Step(ctx, m)
}
func (rc *RaftNode) IsIDRemoved(id uint64) bool {
	return false
}
func (rc *RaftNode) ReportUnreachable(id uint64) {
	rc.node.ReportUnreachable(id)
}
func (rc *RaftNode) ReportSnapshot(id uint64, status raft.SnapshotStatus) {
	rc.node.ReportSnapshot(id, status)
}

// 生成节点随机id，64 uint
func genId(url string) uint64 {
	// y := nuid.Next()
	h := fnv.New64()
	_, _ = h.Write([]byte(url))
	return h.Sum64()
}

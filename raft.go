package raftgrpc

import (
	"context"
	"fmt"
	"hash/fnv"
	"log"
	"net"
	"os"
	pb "raftgrpc/proto"
	"time"

	"go.etcd.io/etcd/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
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

	walDir        string // path to WAL directory
	snapDir       string // path to snapshot directory
	getSnapshotFn func() ([]byte, error)

	confState     raftpb.ConfState
	snapshotIndex uint64
	appliedIndex  uint64

	// raft backing for the commit/error channel
	node        raft.Node
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL

	snapshotter      *snap.Snapshotter
	snapshotterReady chan *snap.Snapshotter // signals when snapshotter is ready

	snapCount uint64
	transport *GrpcTransport

	stopCh chan struct{} // signals proposal channel closed

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
		proposeCh:     proposeCh,
		confChangeCh:  confChangeCh,
		commitCh:      commitCh,
		errorCh:       errorCh,
		id:            genId(url),
		join:          join,
		peers:         peers,
		walDir:        fmt.Sprintf("raftgrpc-%d", id),
		snapDir:       fmt.Sprintf("raftgrpc-%d-snap", id),
		getSnapshotFn: getSnapshotFn,
		snapCount:     defaultSnapshotCount,
		stopCh:        make(chan struct{}),

		logger: logger,

		snapshotterReady: make(chan *snap.Snapshotter, 1),
		// rest of structure populated after WAL replay
	}
	go rc.startRaft()
	return commitCh, errorCh, rc.snapshotterReady
}

func (rc *RaftNode) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := rc.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := rc.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return rc.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (rc *RaftNode) entriesToApply(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return entries
	}
	firstIdx := entries[0].Index
	if firstIdx > rc.appliedIndex+1 {
		rc.logger.Fatal("first index of committed", zap.Uint64("first", firstIdx), zap.Uint64("appliedIndex", rc.appliedIndex))
	}
	if rc.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		entries = entries[rc.appliedIndex-firstIdx+1:]
	}
	return entries
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
			rc.confState = *rc.node.ApplyConfChange(cc)
			switch cc.Type {
			case raftpb.ConfChangeAddNode:
				if len(cc.Context) > 0 {
					var node pb.NodeInfo
					_ = node.Unmarshal(cc.Context)
					_ = rc.transport.AddPeer(&node)
				}
			case raftpb.ConfChangeRemoveNode:
				if cc.NodeID == rc.id {
					log.Println("I've been removed from the cluster! Shutting down.")
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
	rc.appliedIndex = entries[len(entries)-1].Index

	return applyDoneCh, true
}

func (rc *RaftNode) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(rc.walDir) {
		walSnaps, err := wal.ValidSnapshotEntries(rc.logger, rc.walDir)
		if err != nil {
			rc.logger.Fatal("valid snapshots", zap.Error(err))
		}
		snapshot, err := rc.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			rc.logger.Fatal("loading snapshot", zap.Error(err))
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (rc *RaftNode) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(rc.walDir) {
		if err := os.Mkdir(rc.walDir, 0750); err != nil {
			rc.logger.Fatal("create dir for wal", zap.Error(err))
		}
		w, err := wal.Create(zap.NewExample(), rc.walDir, nil)
		if err != nil {
			rc.logger.Fatal("create wal", zap.Error(err))
		}
		w.Close()
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	rc.logger.Info("loading wal", zap.Uint64("term", walSnap.Term), zap.Uint64("index", walSnap.Index))
	w, err := wal.Open(zap.NewExample(), rc.walDir, walSnap)
	if err != nil {
		rc.logger.Fatal("loading wal", zap.Error(err))
	}
	return w
}

// replayWAL replays WAL entries into the raft instance.
func (rc *RaftNode) replayWAL() *wal.WAL {
	rc.logger.Info("replaying WAL", zap.Uint64("member", rc.id))
	snapshot := rc.loadSnapshot()
	w := rc.openWAL(snapshot)
	_, st, entries, err := w.ReadAll()
	if err != nil {
		rc.logger.Fatal("read WAL", zap.Error(err))
	}
	rc.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		_ = rc.raftStorage.ApplySnapshot(*snapshot)
	}
	_ = rc.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	_ = rc.raftStorage.Append(entries)

	return w
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
	if !fileutil.Exist(rc.snapDir) {
		if err := os.Mkdir(rc.snapDir, 0750); err != nil {
			rc.logger.Fatal("create dir for snapshot", zap.Error(err))
		}
	}
	rc.snapshotter = snap.New(rc.logger, rc.snapDir)

	oldWal := wal.Exist(rc.walDir)
	rc.wal = rc.replayWAL()

	// signal replay has finished
	rc.snapshotterReady <- rc.snapshotter

	rPeers := make([]raft.Peer, len(rc.peers))
	for i, p := range rc.peers {
		rPeers[i] = raft.Peer{ID: genId(p)}
	}
	// rPeers := raftNode.transport.getPeers()
	c := &raft.Config{
		ID:                        rc.id,
		ElectionTick:              10,
		HeartbeatTick:             1,
		Storage:                   rc.raftStorage,
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

func (rc *RaftNode) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}
	rc.logger.Info("publishing snapshot", zap.Uint64("index", rc.snapshotIndex))
	defer rc.logger.Info("finished publishing snapshot", zap.Uint64("index", rc.snapshotIndex))

	if snapshotToSave.Metadata.Index <= rc.appliedIndex {
		rc.logger.Fatal("snapshot index", zap.Uint64("index", snapshotToSave.Metadata.Index), zap.Uint64("appliedIndex", rc.appliedIndex))
	}
	rc.commitCh <- nil // trigger Pair store to load snapshot

	rc.confState = snapshotToSave.Metadata.ConfState
	rc.snapshotIndex = snapshotToSave.Metadata.Index
	rc.appliedIndex = snapshotToSave.Metadata.Index
}

var snapshotCatchUpEntriesN uint64 = 10000

func (rc *RaftNode) maybeTriggerSnapshot(applyDoneCh <-chan struct{}) {
	if rc.appliedIndex-rc.snapshotIndex <= rc.snapCount {
		return
	}

	// wait until all committed entries are applied (or server is closed)
	if applyDoneCh != nil {
		select {
		case <-applyDoneCh:
		case <-rc.stopCh:
			return
		}
	}
	rc.logger.Info("start snapshot", zap.Uint64("appliedIndex", rc.appliedIndex), zap.Uint64("last", rc.snapshotIndex))
	data, err := rc.getSnapshotFn()
	if err != nil {
		rc.logger.Fatal("get snapshot", zap.Error(err))
	}
	s, err := rc.raftStorage.CreateSnapshot(rc.appliedIndex, &rc.confState, data)
	if err != nil {
		rc.logger.Fatal("create snapshot", zap.Error(err))
	}
	if err := rc.saveSnap(s); err != nil {
		rc.logger.Fatal("save snapshot", zap.Error(err))
	}

	compactIndex := uint64(1)
	if rc.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = rc.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := rc.raftStorage.Compact(compactIndex); err != nil {
		rc.logger.Fatal("compact", zap.Error(err))
	}
	rc.logger.Info("compacted log", zap.Uint64("index", compactIndex))
	rc.snapshotIndex = rc.appliedIndex
}

func (rc *RaftNode) serveChannels() {
	s, err := rc.raftStorage.Snapshot()
	if err != nil {
		rc.logger.Fatal("get snapshot", zap.Error(err))
	}
	rc.confState = s.Metadata.ConfState
	rc.snapshotIndex = s.Metadata.Index
	rc.appliedIndex = s.Metadata.Index

	defer rc.wal.Close()

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
			_ = rc.wal.Save(rd.HardState, rd.Entries)
			if !raft.IsEmptySnap(rd.Snapshot) {
				_ = rc.saveSnap(rd.Snapshot)
				_ = rc.raftStorage.ApplySnapshot(rd.Snapshot)
				rc.publishSnapshot(rd.Snapshot)
			}
			_ = rc.raftStorage.Append(rd.Entries)
			_ = rc.transport.SendMsgList(rd.Messages)
			applyDoneCh, ok := rc.publishEntries(rc.entriesToApply(rd.CommittedEntries))
			if !ok {
				rc.stop()
				return
			}
			rc.maybeTriggerSnapshot(applyDoneCh)
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
func (rc *RaftNode) IsIDRemoved(id uint64) bool  { return false }
func (rc *RaftNode) ReportUnreachable(id uint64) { rc.node.ReportUnreachable(id) }
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

package raftgrpc

import (
	"fmt"
	"os"

	"go.etcd.io/etcd/pkg/v3/fileutil"
	"go.etcd.io/etcd/raft/v3"
	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.etcd.io/etcd/server/v3/wal"
	"go.etcd.io/etcd/server/v3/wal/walpb"
	"go.uber.org/zap"
)

type Snapshot struct {
	id      uint64
	walDir  string
	snapDir string

	snapCount     uint64
	snapshotIndex uint64
	appliedIndex  uint64

	confState   raftpb.ConfState
	raftStorage *raft.MemoryStorage
	wal         *wal.WAL
	snapshotter *snap.Snapshotter

	commitCh chan<- *commit
	stopCh   chan struct{}

	getSnapshotFn func() ([]byte, error)

	logger *zap.Logger
}

func NewSnapshot(id uint64, getSnapshotFn func() ([]byte, error),
	commitCh chan<- *commit, stopCh chan struct{}, logger *zap.Logger) *Snapshot {
	s := Snapshot{
		id:            id,
		walDir:        fmt.Sprintf("raftgrpc-%d", id),
		snapDir:       fmt.Sprintf("raftgrpc-%d-snap", id),
		snapCount:     defaultSnapshotCount,
		raftStorage:   raft.NewMemoryStorage(),
		commitCh:      commitCh,
		stopCh:        stopCh,
		getSnapshotFn: getSnapshotFn,
		logger:        logger,
	}
	return &s
}

func (s *Snapshot) InitSnapshot() {
	if !fileutil.Exist(s.snapDir) {
		if err := os.Mkdir(s.snapDir, 0750); err != nil {
			s.logger.Fatal("create dir for snapshot", zap.Error(err))
		}
	}
	s.snapshotter = snap.New(s.logger, s.snapDir)
	s.wal = s.replayWal()
	sn, err := s.raftStorage.Snapshot()
	if err != nil {
		s.logger.Fatal("get snapshot", zap.Error(err))
	}
	s.confState = sn.Metadata.ConfState
	s.snapshotIndex = sn.Metadata.Index
	s.appliedIndex = sn.Metadata.Index
}

func (s *Snapshot) SaveToSnapshot(rd raft.Ready) {
	_ = s.wal.Save(rd.HardState, rd.Entries)
	if !raft.IsEmptySnap(rd.Snapshot) {
		_ = s.saveSnap(rd.Snapshot)
		_ = s.raftStorage.ApplySnapshot(rd.Snapshot)
		s.publishSnapshot(rd.Snapshot)
	}
	_ = s.raftStorage.Append(rd.Entries)
}

func (s *Snapshot) EntriesToApply(entries []raftpb.Entry) []raftpb.Entry {
	if len(entries) == 0 {
		return entries
	}
	firstIdx := entries[0].Index
	if firstIdx > s.appliedIndex+1 {
		s.logger.Fatal("first index of committed", zap.Uint64("first", firstIdx), zap.Uint64("appliedIndex", s.appliedIndex))
	}
	if s.appliedIndex-firstIdx+1 < uint64(len(entries)) {
		entries = entries[s.appliedIndex-firstIdx+1:]
	}
	return entries
}

// MaybeTriggerSnapshot try to trigger snapshot action
func (s *Snapshot) MaybeTriggerSnapshot(applyDoneCh <-chan struct{}) {
	// check log count
	if s.appliedIndex-s.snapshotIndex <= s.snapCount {
		return
	}
	// wait until all committed entries are applied (or server is closed)
	if applyDoneCh != nil {
		select {
		case <-applyDoneCh:
		case <-s.stopCh:
			return
		}
	}
	s.logger.Info("start snapshot", zap.Uint64("appliedIndex", s.appliedIndex), zap.Uint64("last", s.snapshotIndex))
	data, err := s.getSnapshotFn()
	if err != nil {
		s.logger.Fatal("get snapshot", zap.Error(err))
	}
	ss, err := s.raftStorage.CreateSnapshot(s.appliedIndex, &s.confState, data)
	if err != nil {
		s.logger.Fatal("create snapshot", zap.Error(err))
	}
	if err := s.saveSnap(ss); err != nil {
		s.logger.Fatal("save snapshot", zap.Error(err))
	}

	compactIndex := uint64(1)
	if s.appliedIndex > snapshotCatchUpEntriesN {
		compactIndex = s.appliedIndex - snapshotCatchUpEntriesN
	}
	if err := s.raftStorage.Compact(compactIndex); err != nil {
		s.logger.Fatal("compact", zap.Error(err))
	}
	s.logger.Info("compacted log", zap.Uint64("index", compactIndex))
	s.snapshotIndex = s.appliedIndex
}

func (s *Snapshot) ExistWal() bool {
	return wal.Exist(s.walDir)
}

func (s *Snapshot) UpdateConfState(confState raftpb.ConfState) {
	s.confState = confState
}

func (s *Snapshot) UpdateAppliedIndex(appliedIndex uint64) {
	s.appliedIndex = appliedIndex
}

func (s *Snapshot) Snapshotter() *snap.Snapshotter {
	return s.snapshotter
}

func (s *Snapshot) RaftStorage() raft.Storage {
	return s.raftStorage
}

func (s *Snapshot) Close() {
	s.wal.Close()
}

func (s *Snapshot) saveSnap(snap raftpb.Snapshot) error {
	walSnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}
	// save the snapshot file before writing the snapshot to the wal.
	// This makes it possible for the snapshot file to become orphaned, but prevents
	// a WAL snapshot entry from having no corresponding snapshot file.
	if err := s.snapshotter.SaveSnap(snap); err != nil {
		return err
	}
	if err := s.wal.SaveSnapshot(walSnap); err != nil {
		return err
	}
	return s.wal.ReleaseLockTo(snap.Metadata.Index)
}

func (s *Snapshot) publishSnapshot(snapshotToSave raftpb.Snapshot) {
	if raft.IsEmptySnap(snapshotToSave) {
		return
	}
	s.logger.Info("publishing snapshot", zap.Uint64("index", s.snapshotIndex))
	defer s.logger.Info("finished publishing snapshot", zap.Uint64("index", s.snapshotIndex))

	if snapshotToSave.Metadata.Index <= s.appliedIndex {
		s.logger.Fatal("snapshot index", zap.Uint64("index", snapshotToSave.Metadata.Index), zap.Uint64("appliedIndex", s.appliedIndex))
	}
	s.commitCh <- nil // trigger Pair store to load snapshot

	s.confState = snapshotToSave.Metadata.ConfState
	s.snapshotIndex = snapshotToSave.Metadata.Index
	s.appliedIndex = snapshotToSave.Metadata.Index
}

func (s *Snapshot) replayWal() *wal.WAL {
	s.logger.Info("replaying WAL", zap.Uint64("member", s.id))
	snapshot := s.loadSnapshot()
	w := s.openWAL(snapshot)
	_, st, entries, err := w.ReadAll()
	if err != nil {
		s.logger.Fatal("read WAL", zap.Error(err))
	}
	// s.raftStorage = raft.NewMemoryStorage()
	if snapshot != nil {
		_ = s.raftStorage.ApplySnapshot(*snapshot)
	}
	_ = s.raftStorage.SetHardState(st)

	// append to storage so raft starts at the right place in log
	_ = s.raftStorage.Append(entries)
	return w
}

func (s *Snapshot) loadSnapshot() *raftpb.Snapshot {
	if wal.Exist(s.walDir) {
		walSnaps, err := wal.ValidSnapshotEntries(s.logger, s.walDir)
		if err != nil {
			s.logger.Fatal("valid snapshots", zap.Error(err))
		}
		snapshot, err := s.snapshotter.LoadNewestAvailable(walSnaps)
		if err != nil && err != snap.ErrNoSnapshot {
			s.logger.Fatal("loading snapshot", zap.Error(err))
		}
		return snapshot
	}
	return &raftpb.Snapshot{}
}

// openWAL returns a WAL ready for reading.
func (s *Snapshot) openWAL(snapshot *raftpb.Snapshot) *wal.WAL {
	if !wal.Exist(s.walDir) {
		if err := os.Mkdir(s.walDir, 0750); err != nil {
			s.logger.Fatal("create dir for wal", zap.Error(err))
		}
		w, err := wal.Create(s.logger, s.walDir, nil)
		if err != nil {
			s.logger.Fatal("create wal", zap.Error(err))
		}
		w.Close()
	}

	walSnap := walpb.Snapshot{}
	if snapshot != nil {
		walSnap.Index, walSnap.Term = snapshot.Metadata.Index, snapshot.Metadata.Term
	}
	s.logger.Info("loading wal", zap.Uint64("term", walSnap.Term), zap.Uint64("index", walSnap.Index))
	w, err := wal.Open(zap.NewExample(), s.walDir, walSnap)
	if err != nil {
		s.logger.Fatal("loading wal", zap.Error(err))
	}
	return w
}

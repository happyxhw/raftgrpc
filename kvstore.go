package raftgrpc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	pb "raftgrpc/proto"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

// a key-value store backed by raft
type KvStore struct {
	sync.RWMutex

	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter

	logger *zap.Logger
}

func NewKVStore(snapshotter *snap.Snapshotter, commitCh <-chan *commit, errorCh <-chan error, logger *zap.Logger) *KvStore {
	kv := &KvStore{
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
		logger:      logger,
	}
	snapshot, err := kv.loadSnapshot()
	if err != nil {
		logger.Fatal("load snapshot", zap.Error(err))
	}
	if snapshot != nil {
		logger.Info("loading snapshot", zap.Uint64("term", snapshot.Metadata.Term), zap.Uint64("index", snapshot.Metadata.Index))
		if err := kv.recoverFromSnapshot(snapshot.Data); err != nil {
			logger.Fatal("recover snapshot", zap.Error(err))
		}
	}
	// read commits from raft into KvStore map until error
	go kv.readCommits(commitCh, errorCh)
	return kv
}

func (kv *KvStore) LookUp(key string) (string, bool) {
	kv.RLock()
	defer kv.RUnlock()
	v, ok := kv.kvStore[key]
	return v, ok
}

// func (kv *KvStore) Propose(k string, v string) {
// 	var buf bytes.Buffer
// 	if err := gob.NewEncoder(&buf).Encode(Pair{k, v}); err != nil {
// 		kv.logger.Fatal("decode", zap.Error(err))
// 	}
// 	kv.proposeCh <- buf.String()
// }

func (kv *KvStore) readCommits(commitCh <-chan *commit, errorCh <-chan error) {
	for commit := range commitCh {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := kv.loadSnapshot()
			if err != nil {
				kv.logger.Fatal("load snapshot", zap.Error(err))
			}
			if snapshot != nil {
				kv.logger.Info("loading snapshot", zap.Uint64("term", snapshot.Metadata.Term), zap.Uint64("index", snapshot.Metadata.Index))
				if err := kv.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv pb.Pair
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				kv.logger.Fatal("decode", zap.Error(err))
			}
			kv.Lock()
			kv.kvStore[dataKv.Key] = dataKv.Value
			kv.Unlock()
		}
		close(commit.applyDoneCh)
	}
	if err, ok := <-errorCh; ok {
		log.Fatal(err)
	}
}

func (kv *KvStore) GetSnapshot() ([]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	return json.Marshal(kv.kvStore)
}

func (kv *KvStore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := kv.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (kv *KvStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	kv.Lock()
	defer kv.Unlock()
	kv.kvStore = store
	return nil
}

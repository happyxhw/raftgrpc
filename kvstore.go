package raftgrpc

import (
	"encoding/json"
	"sync"

	"github.com/happyxhw/gopkg/logger"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
	"go.uber.org/zap"
)

// MapKvStore a key-value store backed by raft
type MapKvStore struct {
	sync.RWMutex

	kvStore map[string]string // current committed key-value pairs
}

// NewMapKVStore kv store based on map
func NewMapKVStore() *MapKvStore {
	kv := &MapKvStore{
		kvStore: make(map[string]string),
	}
	return kv
}

// Recover from snapshot
func (kv *MapKvStore) Recover(snapshotter *snap.Snapshotter) error {
	snapshot, err := snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil
	}
	if err != nil {
		logger.Error("load snapshot", zap.Error(err))
		return err
	}
	if snapshot != nil {
		logger.Info("load snapshot", zap.Uint64("term", snapshot.Metadata.Term), zap.Uint64("index", snapshot.Metadata.Index))
		var store map[string]string
		if err := json.Unmarshal(snapshot.Data, &store); err != nil {
			logger.Error("unmarshal snapshot", zap.Error(err))
			return err
		}
		kv.Lock()
		defer kv.Unlock()
		kv.kvStore = store
	}
	return nil
}

// Get key
func (kv *MapKvStore) Get(key string) (string, bool) {
	kv.RLock()
	defer kv.RUnlock()
	v, ok := kv.kvStore[key]
	return v, ok
}

// Put a pair kv
func (kv *MapKvStore) Put(key, val string) error {
	kv.Lock()
	defer kv.Unlock()
	kv.kvStore[key] = val
	return nil
}

// Del a key
func (kv *MapKvStore) Del(key string) (string, error) {
	kv.Lock()
	defer kv.Unlock()
	val := kv.kvStore[key]
	delete(kv.kvStore, key)
	return val, nil
}

// Snapshot return snapshot data
func (kv *MapKvStore) Snapshot() ([]byte, error) {
	kv.RLock()
	defer kv.RUnlock()
	return json.Marshal(kv.kvStore)
}

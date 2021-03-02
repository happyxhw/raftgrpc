package raftgrpc

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"log"
	"sync"

	"go.etcd.io/etcd/raft/v3/raftpb"
	"go.etcd.io/etcd/server/v3/etcdserver/api/snap"
)

// a key-value store backed by raft
type kvStore struct {
	sync.RWMutex

	proposeCh   chan<- string     // channel for proposing updates
	kvStore     map[string]string // current committed key-value pairs
	snapshotter *snap.Snapshotter
}

type kv struct {
	Key string
	Val string
}

func newKVStore(snapshotter *snap.Snapshotter, proposeCh chan<- string, commitCh <-chan *commit, errorCh <-chan error) *kvStore {
	s := &kvStore{
		proposeCh:   proposeCh,
		kvStore:     make(map[string]string),
		snapshotter: snapshotter,
	}
	snapshot, err := s.loadSnapshot()
	if err != nil {
		log.Panic(err)
	}
	if snapshot != nil {
		log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
		if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
			log.Panic(err)
		}
	}
	// read commits from raft into kvStore map until error
	go s.readCommits(commitCh, errorCh)
	return s
}

func (s *kvStore) LookUp(key string) (string, bool) {
	s.RLock()
	defer s.RUnlock()
	v, ok := s.kvStore[key]
	return v, ok
}

func (s *kvStore) Propose(k string, v string) {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(kv{k, v}); err != nil {
		log.Fatal(err)
	}
	s.proposeCh <- buf.String()
}

func (s *kvStore) readCommits(commitCh <-chan *commit, errorCh <-chan error) {
	for commit := range commitCh {
		if commit == nil {
			// signaled to load snapshot
			snapshot, err := s.loadSnapshot()
			if err != nil {
				log.Panic(err)
			}
			if snapshot != nil {
				log.Printf("loading snapshot at term %d and index %d", snapshot.Metadata.Term, snapshot.Metadata.Index)
				if err := s.recoverFromSnapshot(snapshot.Data); err != nil {
					log.Panic(err)
				}
			}
			continue
		}

		for _, data := range commit.data {
			var dataKv kv
			dec := gob.NewDecoder(bytes.NewBufferString(data))
			if err := dec.Decode(&dataKv); err != nil {
				log.Fatalf("raftexample: could not decode message (%v)", err)
			}
			s.Lock()
			s.kvStore[dataKv.Key] = dataKv.Val
			s.Unlock()
		}
		close(commit.applyDoneCh)
	}
	if err, ok := <-errorCh; ok {
		log.Fatal(err)
	}
}

func (s *kvStore) getSnapshot() ([]byte, error) {
	s.RLock()
	defer s.RUnlock()
	return json.Marshal(s.kvStore)
}

func (s *kvStore) loadSnapshot() (*raftpb.Snapshot, error) {
	snapshot, err := s.snapshotter.Load()
	if err == snap.ErrNoSnapshot {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return snapshot, nil
}

func (s *kvStore) recoverFromSnapshot(snapshot []byte) error {
	var store map[string]string
	if err := json.Unmarshal(snapshot, &store); err != nil {
		return err
	}
	s.Lock()
	defer s.Unlock()
	s.kvStore = store
	return nil
}

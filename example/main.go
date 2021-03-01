package main

import (
	"raftgrpc"
	"time"

	"github.com/happyxhw/gopkg/logger"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

func main() {
	go startNode(8081, 0)
	go startNode(8082, 1)
	go startNode(8083, 2)

	time.Sleep(time.Second * 1000)
}

func startNode(port, index int) {
	proposeCh := make(chan string)
	defer close(proposeCh)
	confChangeCh := make(chan raftpb.ConfChange)
	defer close(confChangeCh)

	var kv *raftgrpc.KvStore
	getSnapshotFn := func() ([]byte, error) { return kv.GetSnapshot() }

	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	commitCh, errorCh, snapshotterReady := raftgrpc.NewRaftNode(
		peers[index], false, peers, getSnapshotFn, proposeCh, confChangeCh, logger.GetLogger(),
	)
	kv = raftgrpc.NewKVStore(<-snapshotterReady, proposeCh, commitCh, errorCh, logger.GetLogger())

	raftgrpc.ServeHttpKVAPI(kv, port, confChangeCh, errorCh, logger.GetLogger())
}

package raftgrpc

import (
	"testing"
	"time"

	"github.com/happyxhw/gopkg/logger"
	"go.etcd.io/etcd/raft/v3/raftpb"
)

var fn = func() ([]byte, error) { return nil, nil }

func Test_newRaftNode_1(t *testing.T) {
	proposeCh := make(chan string, 1)
	confChangeCh := make(chan raftpb.ConfChange, 1)
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	_, _, _ = newRaftNode(
		1, false, peers, fn, proposeCh, confChangeCh, logger.GetLogger(),
	)
	time.Sleep(time.Second * 1000)
}

func Test_newRaftNode_2(t *testing.T) {
	proposeCh := make(chan string, 1)
	confChangeCh := make(chan raftpb.ConfChange, 1)
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	_, _, _ = newRaftNode(
		2, false, peers, fn, proposeCh, confChangeCh, logger.GetLogger(),
	)
	time.Sleep(time.Second * 1000)
}

func Test_newRaftNode_3(t *testing.T) {
	proposeCh := make(chan string, 1)
	confChangeCh := make(chan raftpb.ConfChange, 1)
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	_, _, _ = newRaftNode(
		3, false, peers, fn, proposeCh, confChangeCh, logger.GetLogger(),
	)
	time.Sleep(time.Second * 1000)
}

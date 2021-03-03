package raftgrpc

import (
	"fmt"
	"testing"

	"github.com/happyxhw/gopkg/logger"
)

var fn = func() ([]byte, error) { return nil, nil }

func Test_newRaftNode_1(t *testing.T) {
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	rn := NewRaftNode(
		peers[0], false, peers, logger.GetLogger(),
	)
	rn.Start()
}

func Test_newRaftNode_2(t *testing.T) {
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	rn := NewRaftNode(
		peers[0], false, peers, logger.GetLogger(),
	)
	rn.Start()
}

func Test_newRaftNode_3(t *testing.T) {
	peers := []string{
		"127.0.0.1:8001",
		"127.0.0.1:8002",
		"127.0.0.1:8003",
	}
	rn := NewRaftNode(
		peers[0], false, peers, logger.GetLogger(),
	)
	rn.Start()
}

func Test_genId(t *testing.T) {
	x := "127.0.0.1:8002"
	fmt.Println(genId(x))
}

package raftgrpc

import (
	"testing"
	"time"
)

func Test_grpcTransport_start(t *testing.T) {
	p := peer{
		id:   1,
		addr: "127.0.0.1:8001",
	}

	ts := GrpcTransport{
		stopCh: make(chan struct{}),
	}
	go func() {
		time.Sleep(time.Second * 50)
		ts.Stop()
	}()
	if err := ts.Start(&p); err != nil {
		t.Fatal(err)
	}
}

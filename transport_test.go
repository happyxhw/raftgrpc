package raftgrpc

import (
	"testing"
	"time"

	"github.com/happyxhw/gopkg/logger"
)

func Test_grpcTransport_start(t *testing.T) {
	p := peer{
		id:   1,
		addr: "127.0.0.1",
		port: "8001",
	}

	ts := grpcTransport{
		stopCh: make(chan struct{}),
		errCh: make(chan error),
	}
	go func() {
		time.Sleep(time.Second*50)
		ts.Stop()
	}()
	if err := ts.Start(&p, logger.GetLogger()); err != nil {
		t.Fatal(err)
	}
}

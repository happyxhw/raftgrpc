package raftgrpc

import (
	"testing"
)

func TestKvStore_LookUp(t *testing.T) {
	kv := KvStore{
		kvStore:     make(map[string]string),
	}
	kv.LookUp("mykey")
}

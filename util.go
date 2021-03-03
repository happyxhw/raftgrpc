package raftgrpc

import "hash/fnv"

// 生成节点随机id，64 uint
func genId(addr string) uint64 {
	h := fnv.New64()
	_, _ = h.Write([]byte(addr))
	return h.Sum64()
}

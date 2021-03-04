### A Raft Example Based On Etcd Raft Library And Official Raft Example

**Just an Example**

[Official Raft Example](https://github.com/etcd-io/etcd/tree/master/contrib/raftexample)

<img src="https://user-images.githubusercontent.com/44490504/109909995-20d1b700-7ce2-11eb-946e-f7a8615bb5ca.png" alt="basic" style="zoom:33%;" />

### build
```shell
git clone https://github.com/happyxhw/raftgrpc.git -b develop
cd raftgrpc/example

go build
```

### start a cluster
```shell
./raftcmd start -h

# node-1
./raftcmd start --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --addr 127.0.0.1:8001

# node-2
./raftcmd start --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --addr 127.0.0.1:8002

# node-3
./raftcmd start --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --addr 127.0.0.1:8003
```

### put a key-value
```shell
./raftcmd put -h

./raftcmd put --addr 127.0.0.1:8001 --key mykey --value hello
```

### get a key
```shell
./raftcmd get -h

./raftcmd get --addr 127.0.0.1:8001 --key mykey
```

### join a cluster (not work now)
```shell
./raftcmd join -h
```

### leave a cluster
```shell
./raftcmd rm -h
```

### TODO
- [x] grpc transport
- [ ] more reliable kv-store

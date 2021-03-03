module raftcmd

go 1.16

require (
	github.com/grpc-ecosystem/go-grpc-middleware v1.2.2
	github.com/happyxhw/gopkg v1.0.9
	github.com/spf13/cobra v1.1.3
	go.uber.org/zap v1.16.0
	google.golang.org/grpc v1.36.0
	raftgrpc v0.0.0-00010101000000-000000000000
)

replace raftgrpc => ../

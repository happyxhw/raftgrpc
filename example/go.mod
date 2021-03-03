module raftcmd

go 1.16

require (
	github.com/happyxhw/gopkg v1.0.9
	github.com/spf13/cobra v1.1.3
	go.uber.org/zap v1.16.0
	raftgrpc v0.0.0-00010101000000-000000000000
)

replace raftgrpc => ../

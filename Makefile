.PHONY: proto
proto:
	protoc -I=. \
        -I=./vendor \
        -I=${HOME}/work/protobuf \
        --gofast_out=plugins=grpc:. \
        proto/raft_grpc.proto

# go get github.com/gogo/protobuf/protoc-gen-gofast
package cmd

import (
	"context"
	"fmt"
	"os"
	"raftgrpc"
	pb "raftgrpc/proto"
	"time"

	grpcRetry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/happyxhw/gopkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "raftgrpc",
	Short: "raftgrpc client",
	Long:  "raftgrpc client",
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func init() {
	rootCmd.PersistentFlags().StringP("addr", "a", "127.0.0.1:8001", "grpc 服务地址")
	rootCmd.AddCommand(newStart())
	rootCmd.AddCommand(newJoin())
	rootCmd.AddCommand(newRemove())
	rootCmd.AddCommand(newPut())
	rootCmd.AddCommand(newGet())
	rootCmd.AddCommand(newDel())
}

func run() {
}

func newStart() *cobra.Command {
	var peers []string
	var startCmd = &cobra.Command{
		Use:   "start",
		Short: "start a raft cluster",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			startNode(addr, false, peers)
		},
		Example: `
        ./raftcmd start --addr 127.0.0.1:8001
        ./raftcmd start --addr 127.0.0.1:8001 --peers 127.0.0.1:8001
        ./raftcmd start --peers 127.0.0.1:8001,127.0.0.1:8002,127.0.0.1:8003 --addr 127.0.0.1:8001 
        `,
	}
	startCmd.Flags().StringSliceVar(&peers, "peers", nil, "peers 地址")
	return startCmd
}

func newJoin() *cobra.Command {
	var cluster string
	var joinCmd = &cobra.Command{
		Use:   "join",
		Short: "join a raft cluster",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			cli, err := newClient(cluster)
			if err != nil {
				logger.Error("new grpc client", zap.Error(err))
				return
			}
			req := pb.NodeInfo{
				Addr: addr,
			}
			_, err = cli.Join(context.TODO(), &req)
			if err != nil {
				logger.Error("join", zap.Error(err))
				return
			}
			startNode(addr, false, nil)
		},
		Example: `
        ./raftcmd join --addr 127.0.0.1:8002 --cluster 127.0.0.1:8001
        `,
	}
	joinCmd.Flags().StringVar(&cluster, "cluster", "127.0.0.1:8001", "集群节点地址")
	_ = joinCmd.MarkFlagRequired("cluster")
	return joinCmd
}

func newRemove() *cobra.Command {
	var cluster string
	var rmCmd = &cobra.Command{
		Use:   "rm",
		Short: "remove a raft node",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			cli, err := newClient(cluster)
			if err != nil {
				logger.Error("new grpc client", zap.Error(err))
				return
			}
			req := pb.NodeInfo{
				Addr: addr,
			}
			_, err = cli.Leave(context.TODO(), &req)
			if err != nil {
				logger.Error("join", zap.Error(err))
				return
			}
		},
		Example: `
        ./raftcmd rm --addr 127.0.0.1:8002 --cluster 127.0.0.1:8001
        `,
	}
	rmCmd.Flags().StringVar(&cluster, "cluster", "127.0.0.1:8001", "集群节点地址")
	_ = rmCmd.MarkFlagRequired("cluster")
	return rmCmd
}

func newPut() *cobra.Command {
	var k, v string
	var putCmd = &cobra.Command{
		Use:   "put",
		Short: "put a pair kv",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			cli, err := newClient(addr)
			if err != nil {
				logger.Error("new grpc client", zap.Error(err))
				return
			}
			_, err = cli.KvAction(context.TODO(), &pb.Pair{Key: k, Value: v, Action: pb.Pair_INSERT})
			if err != nil {
				logger.Error("put", zap.Error(err))
				return
			}
		},
		Example: `
        ./raftcmd put --addr 127.0.0.1:8002 --key mykey --value hello
        `,
	}
	putCmd.Flags().StringVar(&k, "key", "", "key")
	putCmd.Flags().StringVar(&v, "value", "", "value")
	_ = putCmd.MarkFlagRequired("key")
	_ = putCmd.MarkFlagRequired("value")
	return putCmd
}

func newGet() *cobra.Command {
	var k string
	var getCmd = &cobra.Command{
		Use:   "get",
		Short: "get a kv",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			cli, err := newClient(addr)
			if err != nil {
				logger.Error("new grpc client", zap.Error(err))
				return
			}
			resp, err := cli.KvAction(context.TODO(), &pb.Pair{Key: k, Action: pb.Pair_QUERY})
			if err != nil {
				logger.Error("get", zap.Error(err))
				return
			}
			if resp != nil && resp.Pair != nil {
				logger.Info("resp", zap.String("key", k), zap.String("value", resp.Pair.Value))
			}
		},
		Example: `
        ./raftcmd get --addr 127.0.0.1:8002 --key mykey
        `,
	}
	getCmd.Flags().StringVar(&k, "key", "", "key")
	_ = getCmd.MarkFlagRequired("key")
	return getCmd
}

func newDel() *cobra.Command {
	var k string
	var delCmd = &cobra.Command{
		Use:   "del",
		Short: "del a kv",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			cli, err := newClient(addr)
			if err != nil {
				logger.Error("new grpc client", zap.Error(err))
				return
			}
			_, err = cli.KvAction(context.TODO(), &pb.Pair{Key: k, Action: pb.Pair_DELETE})
			if err != nil {
				logger.Error("get", zap.Error(err))
				return
			}
		},
		Example: `
        ./raftcmd del --addr 127.0.0.1:8002 --key mykey
        `,
	}
	delCmd.Flags().StringVar(&k, "key", "", "key")
	_ = delCmd.MarkFlagRequired("key")
	return delCmd
}

func startNode(addr string, join bool, peers []string) {
	logger.Info("starting node", zap.String("add", addr), zap.Strings("peers", peers))
	kv := raftgrpc.NewMapKVStore()
	rn := raftgrpc.NewRaftNode(addr, join, peers, kv)
	rn.Start()
}

// new grpc client
func newClient(addr string) (pb.RaftClient, error) {
	opts := []grpcRetry.CallOption{
		grpcRetry.WithBackoff(grpcRetry.BackoffLinear(100 * time.Millisecond)),
		grpcRetry.WithMax(3),
	}
	conn, err := grpc.Dial(addr,
		grpc.WithStreamInterceptor(grpcRetry.StreamClientInterceptor(opts...)),
		grpc.WithUnaryInterceptor(grpcRetry.UnaryClientInterceptor(opts...)),
		grpc.WithInsecure(),
	)
	if err != nil {
		return nil, err
	}
	cli := pb.NewRaftClient(conn)
	return cli, nil
}

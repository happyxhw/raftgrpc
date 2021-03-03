package cmd

import (
	"fmt"
	"os"
	"raftgrpc"

	"github.com/happyxhw/gopkg/logger"
	"github.com/spf13/cobra"
	"go.uber.org/zap"
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
}

func run() {
}

func newStart() *cobra.Command {
	var peers []string
	var startCmd = &cobra.Command{
		Use: "start",
		Run: func(cmd *cobra.Command, args []string) {
			addr := rootCmd.Flag("addr").Value.String()
			startNode(addr, peers)
		},
	}
	startCmd.Flags().StringSliceVarP(&peers, "peers", "p", nil, "peers 地址")
	return startCmd
}

var getSnapshotFn = func() ([]byte, error) { return nil, nil }

func startNode(addr string, peers []string) {
	logger.Info("starting node", zap.String("add", addr), zap.Strings("peers", peers))
	_, _, _, rn := raftgrpc.NewRaftNode(addr, false, peers, getSnapshotFn, logger.GetLogger())
	rn.Start()
}

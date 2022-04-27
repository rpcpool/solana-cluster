// Copyright 2022 Blockdaemon Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package tracker provides the `tracker` command.
package tracker

import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync/atomic"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/spf13/cobra"
	"go.blockdaemon.com/solana/cluster-manager/internal/logger"
	"go.blockdaemon.com/solana/cluster-manager/internal/scraper"
	"go.uber.org/zap"
	"golang.org/x/sync/errgroup"
)

var Cmd = cobra.Command{
	Use:   "tracker",
	Short: "Snapshot tracker server",
	Long: "Connects to sidecars on nodes and scrapes the available snapshot versions.\n" +
		"Provides an API allowing fetch jobs to find the latest snapshots.",
	Run: func(_ *cobra.Command, _ []string) {
		run()
	},
}

var (
	configPath string
	listen     string
)

func init() {
	flags := Cmd.Flags()
	flags.StringVar(&configPath, "config", "", "Path to config file")
	flags.StringVar(&listen, "listen", ":8457", "Listen URL")
	flags.AddFlagSet(logger.Flags)
}

func run() {
	log := logger.GetLogger()

	// Install signal handlers.
	onReload := make(chan os.Signal, 1)
	signal.Notify(onReload, syscall.SIGHUP)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	// Install HTTP handlers.
	http.HandleFunc("/reload", func(wr http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			http.Error(wr, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		onReload <- syscall.SIGHUP
		http.Error(wr, "reloaded", http.StatusOK)
	})
	httpErrLog, err := zap.NewStdLogAt(log.Named("prometheus"), zap.ErrorLevel)
	if err != nil {
		panic(err.Error())
	}
	http.Handle("/metrics", promhttp.HandlerFor(
		prometheus.DefaultGatherer,
		promhttp.HandlerOpts{
			ErrorLog: httpErrLog,
		},
	))

	// Create result collector.
	collector := scraper.NewCollector()

	// Start services.
	group, _ := errgroup.WithContext(ctx)
	group.Go(func() error {
		return http.ListenAndServe(listen, nil)
	})

	// Create config reloader.
	var configObj atomic.Value
	group.Go(func() error {
		_ = configObj
		return nil // TODO not implemented
	})

	// Create scrape managers.
	manager := scraper.NewManager(collector.Probes())
	group.Go(func() error {
		_ = manager
		return nil // TODO not implemented
	})

	// Wait until crash or graceful exit.
	if err := group.Wait(); err != nil {
		log.Error("Crashed", zap.Error(err))
	} else {
		log.Info("Shutting down")
	}
}

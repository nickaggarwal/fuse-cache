// node-init discovers the best local disk on the node, prepares it as the
// fuse-cache peer cache directory, and publishes the result for the client
// daemon — making the DaemonSet deployable on any Kubernetes cluster on any
// cloud without per-cloud disk configuration.
//
// Modes:
//
//	-mode init    one-shot (init container): discover → benchmark → prepare
//	              the cache dir → write the config file → exit 0.
//	-mode daemon  sidecar: do the init work if the config is missing, then
//	              periodically refresh measured free space / cache budget in
//	              the config so the client heartbeats real headroom.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"fuse-client/internal/nodeinit"
)

func main() {
	var (
		mode          = flag.String("mode", "init", "\"init\" (one-shot init container) or \"daemon\" (keep refreshing capacity)")
		hostRoot      = flag.String("host-root", "/host", "Where the node's root filesystem is mounted inside this container; \"\" on bare metal")
		configPath    = flag.String("config", "/var/run/fuse-client/node-init.json", "Config file to write for the client daemon (host path when -host-root is set: the client reads it via its own mount)")
		cacheSubdir   = flag.String("cache-subdir", "fuse-cache", "Directory created inside the winning mount point")
		minFreeGB     = flag.Int("min-free-gb", 2, "Disqualify filesystems with less free space (GiB)")
		cacheFraction = flag.Float64("cache-fraction", 0.8, "Fraction of free space given to the cache budget")
		maxCacheGB    = flag.Int("max-cache-gb", 0, "Cap the cache budget (GiB); 0 = uncapped")
		benchmark     = flag.Bool("benchmark", true, "Micro-benchmark finalist disks (64MiB sequential write+read)")
		benchmarkMB   = flag.Int("benchmark-mb", 64, "Benchmark file size in MiB")
		refreshSec    = flag.Int("refresh-interval-sec", 60, "Daemon mode: capacity refresh interval")
		exclude       = flag.String("exclude-prefixes", "", "Comma-separated extra mount-point prefixes to skip")
		failOpen      = flag.Bool("fail-open", true, "On discovery failure, exit 0 without a config so the client falls back to its static -nvme path instead of crash-looping the pod")
	)
	flag.Parse()

	logger := log.New(os.Stdout, "[NODE-INIT] ", log.LstdFlags)

	opts := nodeinit.DefaultOptions()
	opts.HostRoot = *hostRoot
	opts.CacheSubdir = *cacheSubdir
	opts.MinFreeBytes = int64(*minFreeGB) << 30
	opts.CacheFraction = *cacheFraction
	opts.MaxCacheBytes = int64(*maxCacheGB) << 30
	opts.Benchmark = *benchmark
	opts.BenchmarkBytes = int64(*benchmarkMB) << 20
	opts.NodeName = os.Getenv("NODE_NAME")
	if *exclude != "" {
		opts.ExtraExcludePrefixes = splitNonEmpty(*exclude)
	}

	switch *mode {
	case "init":
		if err := runInit(logger, opts, *configPath); err != nil {
			if *failOpen {
				// Degrade instead of blocking the pod: no config is written, so
				// after -node-init-wait-sec the client falls back to its static
				// -nvme path (NI-4). Better a working node on the default disk
				// than a crash-loop on discovery edge cases.
				logger.Printf("init failed: %v — failing open, client will use its static -nvme path", err)
				return
			}
			logger.Fatalf("init failed: %v", err)
		}
	case "daemon":
		if err := runDaemon(logger, opts, *configPath, time.Duration(*refreshSec)*time.Second); err != nil {
			if *failOpen {
				// The client is using its static -nvme path; the refresh daemon
				// has nothing to track. Idle instead of crash-looping the sidecar.
				logger.Printf("daemon: %v — failing open, idling (client uses static -nvme path)", err)
				select {}
			}
			logger.Fatalf("daemon failed: %v", err)
		}
	default:
		logger.Fatalf("unknown -mode %q (want init or daemon)", *mode)
	}
}

func runInit(logger *log.Logger, opts nodeinit.Options, configPath string) error {
	logger.Printf("discovering local disks (host root %q)", opts.HostRoot)
	cfg, err := nodeinit.SelectAndPrepare(opts)
	if err != nil {
		return err
	}
	if err := nodeinit.WriteConfig(cfg, configPath); err != nil {
		return err
	}
	pretty, _ := json.Marshal(cfg)
	logger.Printf("selected %s (%s, %s) budget=%.1fGiB score=%.0f → %s",
		cfg.CacheDir, cfg.DiskClass, cfg.Device,
		float64(cfg.CacheBytes)/(1<<30), cfg.Score, configPath)
	logger.Printf("config: %s", pretty)
	return nil
}

func runDaemon(logger *log.Logger, opts nodeinit.Options, configPath string, refresh time.Duration) error {
	// Adopt an existing config (init container already ran) or produce one.
	cfg, err := nodeinit.ReadConfig(configPath)
	if err != nil {
		logger.Printf("no usable config at %s (%v); running discovery", configPath, err)
		if err := runInit(logger, opts, configPath); err != nil {
			return err
		}
		if cfg, err = nodeinit.ReadConfig(configPath); err != nil {
			return err
		}
	} else {
		// Config exists — make sure the directory it points at still does.
		hostDir := filepath.Join(opts.HostRoot, cfg.CacheDir)
		if _, statErr := os.Stat(hostDir); statErr != nil {
			logger.Printf("cache dir %s vanished (%v); re-running discovery", hostDir, statErr)
			if err := runInit(logger, opts, configPath); err != nil {
				return err
			}
			if cfg, err = nodeinit.ReadConfig(configPath); err != nil {
				return err
			}
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if refresh <= 0 {
		refresh = time.Minute
	}
	logger.Printf("daemon: refreshing %s every %v", configPath, refresh)
	ticker := time.NewTicker(refresh)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			logger.Printf("daemon: shutting down")
			return nil
		case <-ticker.C:
			if err := nodeinit.RefreshFreeSpace(cfg, opts.HostRoot, opts); err != nil {
				logger.Printf("refresh failed: %v", err)
				continue
			}
			if err := nodeinit.WriteConfig(cfg, configPath); err != nil {
				logger.Printf("config rewrite failed: %v", err)
			}
		}
	}
}

func splitNonEmpty(s string) []string {
	var out []string
	for _, part := range strings.Split(s, ",") {
		if p := strings.TrimSpace(part); p != "" {
			out = append(out, p)
		}
	}
	return out
}

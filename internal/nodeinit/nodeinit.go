// Package nodeinit discovers the best local disk on a node and prepares it as
// the peer cache directory, so the fuse-cache DaemonSet can be dropped onto
// any Kubernetes cluster on any cloud without per-cloud path/size tuning.
//
// It runs as an init container (one-shot: discover → prepare → write config)
// and optionally keeps running as a sidecar daemon that refreshes measured
// capacity so the client's heartbeats report real headroom.
//
// Discovery is cloud-agnostic on purpose: it reads /proc/mounts and sysfs
// (rotational flag, device model) plus statfs, and micro-benchmarks the
// finalists. Well-known ephemeral-disk mount points (AKS /mnt, GKE/EKS local
// SSD paths) are recognized only as tie-break hints, never required.
package nodeinit

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Config is the contract between node-init and the client daemon. node-init
// writes it; the client reads it at startup (-node-init-config) and uses the
// chosen directory and size instead of its static -nvme/-nvme-max-gb flags.
type Config struct {
	Version int `json:"version"`
	// CacheDir is the prepared cache directory (host path).
	CacheDir string `json:"cache_dir"`
	// Device is the block device backing CacheDir (e.g. /dev/nvme0n1p1).
	Device string `json:"device,omitempty"`
	// FilesystemType as reported by /proc/mounts (ext4, xfs, ...).
	FilesystemType string `json:"filesystem_type,omitempty"`
	// DiskClass is the classified media: "nvme", "ssd", "disk", or "unknown".
	DiskClass string `json:"disk_class"`
	// CacheBytes is the byte budget the client should give the cache tier.
	CacheBytes int64 `json:"cache_bytes"`
	// TotalBytes / FreeBytes are the filesystem's size and free space at
	// selection time.
	TotalBytes int64 `json:"total_bytes"`
	FreeBytes  int64 `json:"free_bytes"`
	// WriteMBps / ReadMBps are micro-benchmark results (0 when skipped).
	WriteMBps float64 `json:"write_mbps,omitempty"`
	ReadMBps  float64 `json:"read_mbps,omitempty"`
	// Score is the composite selection score, kept for debugging.
	Score float64 `json:"score"`
	// NodeName records where this config was generated (K8s downward API).
	NodeName string `json:"node_name,omitempty"`
	// GeneratedAt is when discovery ran; the daemon mode refreshes it.
	GeneratedAt time.Time `json:"generated_at"`
}

// Candidate is a mounted filesystem considered for the cache.
type Candidate struct {
	MountPoint     string
	Device         string
	FilesystemType string
	DiskClass      string
	TotalBytes     int64
	FreeBytes      int64
	WriteMBps      float64
	ReadMBps       float64
	Score          float64
}

// Options tunes discovery and preparation.
type Options struct {
	// HostRoot prefixes all inspected paths ("/host" when the node filesystem
	// is mounted there inside the pod; "" on bare metal).
	HostRoot string
	// CacheSubdir is created inside the winning mount point.
	CacheSubdir string
	// MinFreeBytes disqualifies filesystems with less free space.
	MinFreeBytes int64
	// CacheFraction of free space given to the cache budget (0 < f <= 0.95].
	CacheFraction float64
	// MaxCacheBytes caps the computed budget; 0 = uncapped.
	MaxCacheBytes int64
	// Benchmark enables the write/read micro-benchmark on the finalists.
	Benchmark bool
	// BenchmarkBytes is the benchmark file size (default 64 MiB).
	BenchmarkBytes int64
	// ExtraExcludePrefixes adds mount-point prefixes to skip.
	ExtraExcludePrefixes []string
	// NodeName is stamped into the config (from the downward API).
	NodeName string
}

// DefaultOptions returns production defaults.
func DefaultOptions() Options {
	return Options{
		CacheSubdir:    "fuse-cache",
		MinFreeBytes:   2 << 30, // 2 GiB
		CacheFraction:  0.8,
		Benchmark:      true,
		BenchmarkBytes: 64 << 20,
	}
}

// Disk-class scoring weights. Class dominates (an NVMe beats a bigger HDD);
// free space and measured throughput refine within a class.
const (
	classScoreNVMe    = 1000.0
	classScoreSSD     = 400.0
	classScoreDisk    = 50.0
	classScoreUnknown = 100.0 // cloud ephemeral often shows as unknown virtual media
)

// wellKnownEphemeralPrefixes are tie-break hints for cloud ephemeral disks:
// Azure (waagent resource disk), GKE/EKS local SSD conventions.
var wellKnownEphemeralPrefixes = []string{
	"/mnt/resource",       // Azure waagent default
	"/mnt",                // AKS ephemeral (cloud-init)
	"/mnt/disks",          // GKE local SSD
	"/mnt/stateful_partition", // COS
	"/media/ephemeral",    // older EKS AMIs
	"/local1",             // some EKS/AL2 local NVMe conventions
}

const wellKnownEphemeralBonus = 25.0

// excludedMountPrefixes never host a cache.
var excludedMountPrefixes = []string{
	"/proc", "/sys", "/dev", "/run", "/boot", "/snap",
	"/var/lib/kubelet/pods", // pod volumes churn with pod lifecycle
	"/var/lib/containerd", "/var/lib/docker",
}

// allowedFilesystems can host a cache. Everything else (overlay, tmpfs, nfs,
// fuse, ...) is skipped: the cache must be a real local disk.
var allowedFilesystems = map[string]bool{
	"ext4": true, "ext3": true, "ext2": true,
	"xfs": true, "btrfs": true, "f2fs": true,
}

// scoreCandidate computes the composite score:
// class weight + log2(free GiB) * 10 + throughput bonus + well-known hint.
func scoreCandidate(c *Candidate) float64 {
	score := 0.0
	switch c.DiskClass {
	case "nvme":
		score += classScoreNVMe
	case "ssd":
		score += classScoreSSD
	case "disk":
		score += classScoreDisk
	default:
		score += classScoreUnknown
	}

	freeGiB := float64(c.FreeBytes) / (1 << 30)
	// log2 keeps a 4 TiB disk from drowning out media class entirely.
	if freeGiB >= 1 {
		score += 10 * (math.Floor(math.Log2(freeGiB)) + 1)
	}

	// Measured throughput settles ties between same-class disks; scaled so
	// ~1 GB/s ≈ one free-space doubling.
	score += (c.WriteMBps + c.ReadMBps) / 200.0

	for _, hint := range wellKnownEphemeralPrefixes {
		if c.MountPoint == hint || strings.HasPrefix(c.MountPoint, hint+"/") {
			score += wellKnownEphemeralBonus
			break
		}
	}
	return score
}

// rankCandidates scores and sorts candidates best-first, deterministically.
func rankCandidates(cands []Candidate) []Candidate {
	out := make([]Candidate, len(cands))
	copy(out, cands)
	for i := range out {
		out[i].Score = scoreCandidate(&out[i])
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Score != out[j].Score {
			return out[i].Score > out[j].Score
		}
		if out[i].FreeBytes != out[j].FreeBytes {
			return out[i].FreeBytes > out[j].FreeBytes
		}
		return out[i].MountPoint < out[j].MountPoint
	})
	return out
}

// cacheBudget computes the byte budget from a winner and options.
func cacheBudget(freeBytes int64, opts Options) int64 {
	frac := opts.CacheFraction
	if frac <= 0 || frac > 0.95 {
		frac = 0.8
	}
	budget := int64(float64(freeBytes) * frac)
	if opts.MaxCacheBytes > 0 && budget > opts.MaxCacheBytes {
		budget = opts.MaxCacheBytes
	}
	return budget
}

// buildConfig assembles the Config for a winning candidate.
func buildConfig(winner Candidate, cacheDir string, opts Options) *Config {
	return &Config{
		Version:        1,
		CacheDir:       cacheDir,
		Device:         winner.Device,
		FilesystemType: winner.FilesystemType,
		DiskClass:      winner.DiskClass,
		CacheBytes:     cacheBudget(winner.FreeBytes, opts),
		TotalBytes:     winner.TotalBytes,
		FreeBytes:      winner.FreeBytes,
		WriteMBps:      winner.WriteMBps,
		ReadMBps:       winner.ReadMBps,
		Score:          winner.Score,
		NodeName:       opts.NodeName,
		GeneratedAt:    time.Now(),
	}
}

// WriteConfig atomically writes cfg as JSON to path.
func WriteConfig(cfg *Config, path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadConfig loads and validates a Config written by node-init.
func ReadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse node-init config %s: %w", path, err)
	}
	if cfg.CacheDir == "" {
		return nil, fmt.Errorf("node-init config %s: cache_dir is empty", path)
	}
	if cfg.CacheBytes <= 0 {
		return nil, fmt.Errorf("node-init config %s: cache_bytes = %d", path, cfg.CacheBytes)
	}
	return &cfg, nil
}

package nodeinit

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// statfsFunc returns (total, free) bytes for a path. Overridable in tests and
// implemented per-GOOS (statfs_linux.go / statfs_other.go).
var statfsFunc = statfsBytes

// mountEntry is one /proc/mounts line.
type mountEntry struct {
	Device     string
	MountPoint string
	FSType     string
}

// parseMounts parses /proc/mounts content. Octal escapes in mount points
// (\040 for space) are left as-is: such paths are excluded later anyway.
func parseMounts(content string) []mountEntry {
	var out []mountEntry
	scanner := bufio.NewScanner(strings.NewReader(content))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 {
			continue
		}
		out = append(out, mountEntry{Device: fields[0], MountPoint: fields[1], FSType: fields[2]})
	}
	return out
}

// mountEligible filters out pseudo filesystems, excluded prefixes, and
// non-local-disk filesystem types.
func mountEligible(m mountEntry, extraExcludes []string) bool {
	if !allowedFilesystems[m.FSType] {
		return false
	}
	if !strings.HasPrefix(m.Device, "/dev/") {
		return false
	}
	for _, prefix := range append(append([]string{}, excludedMountPrefixes...), extraExcludes...) {
		if m.MountPoint == prefix || strings.HasPrefix(m.MountPoint, prefix+"/") {
			return false
		}
	}
	return true
}

// classifyDevice maps a /dev device to a media class using sysfs under root:
//   - /sys/block/<disk>/queue/rotational: "0" = flash, "1" = spinning
//   - device name nvme* = NVMe
//
// Cloud paravirtual disks that hide their media type come back "unknown".
func classifyDevice(root, device string) string {
	base := filepath.Base(device)
	disk := parentDiskName(base)
	if strings.HasPrefix(disk, "nvme") {
		return "nvme"
	}
	rotPath := filepath.Join(root, "sys/block", disk, "queue/rotational")
	data, err := os.ReadFile(rotPath)
	if err != nil {
		return "unknown"
	}
	switch strings.TrimSpace(string(data)) {
	case "0":
		return "ssd"
	case "1":
		return "disk"
	default:
		return "unknown"
	}
}

// parentDiskName strips a partition suffix to find the sysfs block dir:
// sda1 → sda, nvme0n1p2 → nvme0n1, xvdb1 → xvdb, mmcblk0p1 → mmcblk0.
func parentDiskName(dev string) string {
	if i := strings.LastIndex(dev, "p"); i > 0 &&
		(strings.HasPrefix(dev, "nvme") || strings.HasPrefix(dev, "mmcblk") || strings.HasPrefix(dev, "loop")) {
		if allDigits(dev[i+1:]) && dev[i+1:] != "" {
			return dev[:i]
		}
	}
	// sdX / xvdX / vdX: strip trailing digits.
	trimmed := strings.TrimRight(dev, "0123456789")
	if trimmed != "" && trimmed != dev {
		if strings.HasPrefix(dev, "nvme") || strings.HasPrefix(dev, "mmcblk") {
			return dev // nvme0n1 must keep its digits
		}
		return trimmed
	}
	return dev
}

func allDigits(s string) bool {
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}
	return len(s) > 0
}

// Discover enumerates eligible mounted filesystems under opts.HostRoot and
// returns them (unranked). Filesystem sizes come from statfs on the
// host-root-prefixed mount point.
func Discover(opts Options) ([]Candidate, error) {
	mountsPath := filepath.Join(opts.HostRoot, "proc/mounts")
	content, err := os.ReadFile(mountsPath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", mountsPath, err)
	}

	seen := make(map[string]bool) // one candidate per device: keep shallowest mount point
	var candidates []Candidate
	for _, m := range parseMounts(string(content)) {
		if !mountEligible(m, opts.ExtraExcludePrefixes) {
			continue
		}
		if seen[m.Device] {
			continue
		}

		hostPath := filepath.Join(opts.HostRoot, m.MountPoint)
		total, free, err := statfsFunc(hostPath)
		if err != nil || total <= 0 {
			continue
		}
		if free < opts.MinFreeBytes {
			continue
		}
		seen[m.Device] = true
		candidates = append(candidates, Candidate{
			MountPoint:     m.MountPoint,
			Device:         m.Device,
			FilesystemType: m.FSType,
			DiskClass:      classifyDevice(opts.HostRoot, m.Device),
			TotalBytes:     total,
			FreeBytes:      free,
		})
	}
	return candidates, nil
}

// SelectAndPrepare runs the full init flow: discover, benchmark the top
// finalists, rank, create the cache dir on the winner, and return the config.
func SelectAndPrepare(opts Options) (*Config, error) {
	candidates, err := Discover(opts)
	if err != nil {
		return nil, err
	}
	if len(candidates) == 0 {
		return nil, fmt.Errorf("no eligible local filesystem found (host root %q, min free %d bytes)", opts.HostRoot, opts.MinFreeBytes)
	}

	// Pre-rank on static signals, then benchmark only the top finalists —
	// benchmarking a slow HDD that already lost on class wastes init time.
	ranked := rankCandidates(candidates)
	if opts.Benchmark {
		finalists := ranked
		if len(finalists) > 3 {
			finalists = finalists[:3]
		}
		for i := range finalists {
			hostMount := filepath.Join(opts.HostRoot, finalists[i].MountPoint)
			w, r, err := benchmarkDir(hostMount, opts.BenchmarkBytes)
			if err != nil {
				continue // benchmark failure just means no throughput bonus
			}
			finalists[i].WriteMBps = w
			finalists[i].ReadMBps = r
		}
		ranked = rankCandidates(ranked)
	}

	winner := ranked[0]
	cacheDirHost := filepath.Join(opts.HostRoot, winner.MountPoint, opts.CacheSubdir)
	if err := os.MkdirAll(cacheDirHost, 0o755); err != nil {
		return nil, fmt.Errorf("prepare cache dir %s: %w", cacheDirHost, err)
	}
	// Recompute free space after mkdir for an accurate budget.
	if total, free, err := statfsFunc(cacheDirHost); err == nil && total > 0 {
		winner.TotalBytes = total
		winner.FreeBytes = free
	}

	cfg := buildConfig(winner, filepath.Join(winner.MountPoint, opts.CacheSubdir), opts)
	return cfg, nil
}

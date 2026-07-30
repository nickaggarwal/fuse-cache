package nodeinit

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
)

const (
	minRawDiskBytes = 8 << 30     // ignore tiny devices (sr0, etc.)
	rawMountBase    = "/mnt"      // host dir under which raw disks are mounted
	fuseCacheLabel  = "fusecache" // marks a filesystem WE created (idempotency)
)

// runCmd runs a command and returns combined output; injectable for tests.
var runCmd = func(name string, args ...string) (string, error) {
	out, err := exec.Command(name, args...).CombinedOutput()
	return string(out), err
}

// blockDeviceLister lists <hostRoot>/sys/block; injectable for tests.
var blockDeviceLister = func(hostRoot string) ([]string, error) {
	entries, err := os.ReadDir(filepath.Join(hostRoot, "sys/block"))
	if err != nil {
		return nil, err
	}
	var names []string
	for _, e := range entries {
		names = append(names, e.Name())
	}
	return names, nil
}

func isPhysicalDisk(name string) bool {
	for _, p := range []string{"loop", "ram", "sr", "dm-", "md", "fd", "dm"} {
		if strings.HasPrefix(name, p) {
			return false
		}
	}
	return strings.HasPrefix(name, "nvme") || strings.HasPrefix(name, "sd") ||
		strings.HasPrefix(name, "xvd") || strings.HasPrefix(name, "vd")
}

func inUseDisks(mounts []mountEntry) map[string]bool {
	inUse := make(map[string]bool)
	for _, m := range mounts {
		if strings.HasPrefix(m.Device, "/dev/") {
			inUse[parentDiskName(filepath.Base(m.Device))] = true
		}
	}
	return inUse
}

// fsProbe reports whether a device carries a filesystem and, if so, its label.
func fsProbe(device string) (hasFS bool, label string) {
	for _, dev := range []string{device, device + "p1", device + "1"} {
		out, err := runCmd("blkid", "-o", "export", dev)
		if err != nil {
			continue
		}
		hasFS = true
		for _, line := range strings.Split(out, "\n") {
			if strings.HasPrefix(line, "LABEL=") {
				label = strings.TrimPrefix(line, "LABEL=")
			}
		}
		return hasFS, label
	}
	return false, ""
}

func discoverAndMountRawDisks(opts Options, mounts []mountEntry) []Candidate {
	names, err := blockDeviceLister(opts.HostRoot)
	if err != nil {
		return nil
	}
	inUse := inUseDisks(mounts)
	var out []Candidate
	for _, name := range names {
		if !isPhysicalDisk(name) || inUse[name] {
			continue
		}
		device := "/dev/" + name
		sizeBytes := readDiskSize(opts.HostRoot, name)
		if sizeBytes < minRawDiskBytes {
			continue
		}
		hasFS, label := fsProbe(device)
		if hasFS && label != fuseCacheLabel {
			continue // holds another volume's data — never touch it
		}
		mountPoint := filepath.Join(rawMountBase, "fuse-"+name)
		if err := prepareRawDisk(device, mountPoint, !hasFS); err != nil {
			continue
		}
		out = append(out, Candidate{
			MountPoint:     mountPoint,
			Device:         device,
			FilesystemType: "ext4",
			DiskClass:      classifyDevice(opts.HostRoot, device),
			TotalBytes:     sizeBytes,
			FreeBytes:      sizeBytes,
		})
	}
	return out
}

func readDiskSize(hostRoot, name string) int64 {
	data, err := os.ReadFile(filepath.Join(hostRoot, "sys/block", name, "size"))
	if err != nil {
		return 0
	}
	sectors, err := strconv.ParseInt(strings.TrimSpace(string(data)), 10, 64)
	if err != nil {
		return 0
	}
	return sectors * 512
}

// prepareRawDisk formats (only when format==true) and mounts device at
// mountPoint in the host mount namespace. Idempotent: skips if already mounted.
func prepareRawDisk(device, mountPoint string, format bool) error {
	if out, _ := runCmd("nsenter", "-t", "1", "-m", "--", "sh", "-c",
		"mountpoint -q "+shellQuote(mountPoint)+" && echo MOUNTED || true"); strings.Contains(out, "MOUNTED") {
		return nil
	}
	if format {
		if out, err := runCmd("mkfs.ext4", "-F", "-m", "0", "-q", "-L", fuseCacheLabel, device); err != nil {
			return fmt.Errorf("mkfs %s: %v (%s)", device, err, strings.TrimSpace(out))
		}
	}
	if out, err := runCmd("nsenter", "-t", "1", "-m", "--", "mkdir", "-p", mountPoint); err != nil {
		return fmt.Errorf("mkdir %s: %v (%s)", mountPoint, err, strings.TrimSpace(out))
	}
	if out, err := runCmd("nsenter", "-t", "1", "-m", "--", "mount", "-o", "noatime", device, mountPoint); err != nil {
		return fmt.Errorf("mount %s %s: %v (%s)", device, mountPoint, err, strings.TrimSpace(out))
	}
	return nil
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}

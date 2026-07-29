package nodeinit

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestParseMounts(t *testing.T) {
	content := `sysfs /sys sysfs rw,nosuid 0 0
/dev/nvme0n1p1 /mnt/nvme ext4 rw,relatime 0 0
/dev/sda1 / ext4 rw,relatime 0 0
tmpfs /run tmpfs rw 0 0
malformed-line
/dev/sdb1 /data xfs rw 0 0
`
	mounts := parseMounts(content)
	if len(mounts) != 5 {
		t.Fatalf("parsed %d mounts, want 5", len(mounts))
	}
	if mounts[1].Device != "/dev/nvme0n1p1" || mounts[1].MountPoint != "/mnt/nvme" || mounts[1].FSType != "ext4" {
		t.Fatalf("mount[1] = %+v", mounts[1])
	}
}

func TestMountEligible(t *testing.T) {
	cases := []struct {
		name string
		m    mountEntry
		want bool
	}{
		{"local ext4", mountEntry{"/dev/sda1", "/data", "ext4"}, true},
		{"local xfs", mountEntry{"/dev/nvme0n1", "/mnt", "xfs"}, true},
		{"overlay", mountEntry{"overlay", "/var/lib/docker/overlay2/x", "overlay"}, false},
		{"tmpfs", mountEntry{"tmpfs", "/run", "tmpfs"}, false},
		{"nfs", mountEntry{"10.0.0.1:/share", "/mnt/nfs", "nfs4"}, false},
		{"proc prefix", mountEntry{"/dev/sda1", "/proc/x", "ext4"}, false},
		{"kubelet pods", mountEntry{"/dev/sdb1", "/var/lib/kubelet/pods/x", "ext4"}, false},
		{"containerd", mountEntry{"/dev/sdb1", "/var/lib/containerd", "ext4"}, false},
		{"non-dev device", mountEntry{"rootfs", "/", "ext4"}, false},
	}
	for _, tc := range cases {
		if got := mountEligible(tc.m, nil); got != tc.want {
			t.Errorf("%s: eligible = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestMountEligible_ExtraExcludes(t *testing.T) {
	m := mountEntry{"/dev/sda1", "/scratch", "ext4"}
	if !mountEligible(m, nil) {
		t.Fatal("baseline should be eligible")
	}
	if mountEligible(m, []string{"/scratch"}) {
		t.Fatal("extra exclude should disqualify")
	}
}

func TestParentDiskName(t *testing.T) {
	cases := map[string]string{
		"sda1":      "sda",
		"sda":       "sda",
		"sdb12":     "sdb",
		"xvdb1":     "xvdb",
		"vda2":      "vda",
		"nvme0n1":   "nvme0n1",
		"nvme0n1p2": "nvme0n1",
		"mmcblk0p1": "mmcblk0",
	}
	for dev, want := range cases {
		if got := parentDiskName(dev); got != want {
			t.Errorf("parentDiskName(%s) = %s, want %s", dev, got, want)
		}
	}
}

func TestClassifyDevice(t *testing.T) {
	root := t.TempDir()
	writeSysfs := func(disk, rotational string) {
		dir := filepath.Join(root, "sys/block", disk, "queue")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "rotational"), []byte(rotational+"\n"), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	writeSysfs("sda", "0")
	writeSysfs("sdb", "1")

	if got := classifyDevice(root, "/dev/nvme0n1p1"); got != "nvme" {
		t.Errorf("nvme = %s", got)
	}
	if got := classifyDevice(root, "/dev/sda1"); got != "ssd" {
		t.Errorf("sda1 = %s", got)
	}
	if got := classifyDevice(root, "/dev/sdb2"); got != "disk" {
		t.Errorf("sdb2 = %s", got)
	}
	if got := classifyDevice(root, "/dev/vdc1"); got != "unknown" {
		t.Errorf("no sysfs = %s", got)
	}
}

func TestScoringPrefersNVMeOverBiggerHDD(t *testing.T) {
	nvme := Candidate{MountPoint: "/mnt/nvme", DiskClass: "nvme", FreeBytes: 100 << 30}
	hdd := Candidate{MountPoint: "/data", DiskClass: "disk", FreeBytes: 4 << 40} // 4 TiB

	ranked := rankCandidates([]Candidate{hdd, nvme})
	if ranked[0].MountPoint != "/mnt/nvme" {
		t.Fatalf("winner = %s, want /mnt/nvme (class must dominate size)", ranked[0].MountPoint)
	}
}

func TestScoringSameClassPrefersMoreFreeSpace(t *testing.T) {
	small := Candidate{MountPoint: "/a", DiskClass: "ssd", FreeBytes: 10 << 30}
	big := Candidate{MountPoint: "/b", DiskClass: "ssd", FreeBytes: 500 << 30}

	ranked := rankCandidates([]Candidate{small, big})
	if ranked[0].MountPoint != "/b" {
		t.Fatalf("winner = %s, want /b", ranked[0].MountPoint)
	}
}

func TestScoringWellKnownEphemeralBreaksTies(t *testing.T) {
	generic := Candidate{MountPoint: "/data", DiskClass: "unknown", FreeBytes: 64 << 30}
	azure := Candidate{MountPoint: "/mnt", DiskClass: "unknown", FreeBytes: 64 << 30}

	ranked := rankCandidates([]Candidate{generic, azure})
	if ranked[0].MountPoint != "/mnt" {
		t.Fatalf("winner = %s, want /mnt (well-known ephemeral hint)", ranked[0].MountPoint)
	}
}

func TestScoringThroughputBreaksTies(t *testing.T) {
	slow := Candidate{MountPoint: "/a", DiskClass: "ssd", FreeBytes: 100 << 30, WriteMBps: 100, ReadMBps: 100}
	fast := Candidate{MountPoint: "/b", DiskClass: "ssd", FreeBytes: 100 << 30, WriteMBps: 2000, ReadMBps: 3000}

	ranked := rankCandidates([]Candidate{slow, fast})
	if ranked[0].MountPoint != "/b" {
		t.Fatalf("winner = %s, want /b (faster disk)", ranked[0].MountPoint)
	}
}

func TestRankDeterministicTieBreak(t *testing.T) {
	a := Candidate{MountPoint: "/a", DiskClass: "ssd", FreeBytes: 100 << 30}
	b := Candidate{MountPoint: "/b", DiskClass: "ssd", FreeBytes: 100 << 30}
	r1 := rankCandidates([]Candidate{b, a})
	r2 := rankCandidates([]Candidate{a, b})
	if r1[0].MountPoint != r2[0].MountPoint {
		t.Fatalf("tie break not deterministic: %s vs %s", r1[0].MountPoint, r2[0].MountPoint)
	}
}

func TestCacheBudget(t *testing.T) {
	opts := DefaultOptions()
	if got := cacheBudget(100<<30, opts); got != int64(float64(100<<30)*0.8) {
		t.Fatalf("budget = %d", got)
	}
	opts.MaxCacheBytes = 10 << 30
	if got := cacheBudget(100<<30, opts); got != 10<<30 {
		t.Fatalf("capped budget = %d, want 10GiB", got)
	}
	opts.CacheFraction = 5.0 // invalid → default 0.8
	opts.MaxCacheBytes = 0
	if got := cacheBudget(10<<30, opts); got != int64(float64(10<<30)*0.8) {
		t.Fatalf("invalid-fraction budget = %d", got)
	}
}

func TestConfigRoundTripAndValidation(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "sub", "node-init.json")

	cfg := &Config{
		Version:     1,
		CacheDir:    "/mnt/fuse-cache",
		DiskClass:   "nvme",
		CacheBytes:  80 << 30,
		TotalBytes:  120 << 30,
		FreeBytes:   100 << 30,
		Score:       1234,
		NodeName:    "node-a",
		GeneratedAt: time.Now(),
	}
	if err := WriteConfig(cfg, path); err != nil {
		t.Fatalf("write: %v", err)
	}
	got, err := ReadConfig(path)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.CacheDir != cfg.CacheDir || got.CacheBytes != cfg.CacheBytes || got.DiskClass != cfg.DiskClass {
		t.Fatalf("round trip mismatch: %+v", got)
	}

	// Invalid configs are rejected.
	bad := filepath.Join(dir, "bad.json")
	os.WriteFile(bad, []byte(`{"cache_dir":"","cache_bytes":0}`), 0o644)
	if _, err := ReadConfig(bad); err == nil {
		t.Fatal("empty cache_dir must be rejected")
	}
	os.WriteFile(bad, []byte(`{"cache_dir":"/x","cache_bytes":0}`), 0o644)
	if _, err := ReadConfig(bad); err == nil {
		t.Fatal("zero cache_bytes must be rejected")
	}
	os.WriteFile(bad, []byte(`not json`), 0o644)
	if _, err := ReadConfig(bad); err == nil {
		t.Fatal("garbage must be rejected")
	}
	if _, err := ReadConfig(filepath.Join(dir, "missing.json")); err == nil {
		t.Fatal("missing file must be rejected")
	}
}

// TestSelectAndPrepare_EndToEnd builds a fake host root (proc/mounts + sysfs
// + real temp dirs) and verifies discovery picks the NVMe, prepares the cache
// dir, and produces a sane config.
func TestSelectAndPrepare_EndToEnd(t *testing.T) {
	root := t.TempDir()

	// Fake host filesystem layout.
	mkdir := func(p string) {
		if err := os.MkdirAll(filepath.Join(root, p), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	mkdir("proc")
	mkdir("mnt/nvme")
	mkdir("data")
	mkdir("sys/block/sdb/queue")
	os.WriteFile(filepath.Join(root, "sys/block/sdb/queue/rotational"), []byte("1\n"), 0o644)

	mounts := `/dev/nvme0n1p1 /mnt/nvme ext4 rw 0 0
/dev/sdb1 /data ext4 rw 0 0
tmpfs /run tmpfs rw 0 0
overlay /var/lib/docker/overlay2/abc overlay rw 0 0
`
	if err := os.WriteFile(filepath.Join(root, "proc/mounts"), []byte(mounts), 0o644); err != nil {
		t.Fatal(err)
	}

	// Fake statfs: NVMe has 200GiB free, HDD 500GiB.
	origStatfs := statfsFunc
	defer func() { statfsFunc = origStatfs }()
	statfsFunc = func(path string) (int64, int64, error) {
		if filepath.HasPrefix(path, filepath.Join(root, "mnt/nvme")) {
			return 256 << 30, 200 << 30, nil
		}
		return 1 << 40, 500 << 30, nil
	}

	opts := DefaultOptions()
	opts.HostRoot = root
	opts.Benchmark = false // no real IO in unit tests
	opts.NodeName = "test-node"

	cfg, err := SelectAndPrepare(opts)
	if err != nil {
		t.Fatalf("SelectAndPrepare: %v", err)
	}
	if cfg.CacheDir != "/mnt/nvme/fuse-cache" {
		t.Fatalf("cache dir = %s, want /mnt/nvme/fuse-cache", cfg.CacheDir)
	}
	if cfg.DiskClass != "nvme" {
		t.Fatalf("disk class = %s, want nvme", cfg.DiskClass)
	}
	if cfg.Device != "/dev/nvme0n1p1" {
		t.Fatalf("device = %s", cfg.Device)
	}
	if cfg.NodeName != "test-node" {
		t.Fatalf("node name = %s", cfg.NodeName)
	}
	wantBudget := int64(float64(200<<30) * 0.8)
	if cfg.CacheBytes != wantBudget {
		t.Fatalf("budget = %d, want %d", cfg.CacheBytes, wantBudget)
	}
	// The cache dir must actually exist on "the host".
	if fi, err := os.Stat(filepath.Join(root, "mnt/nvme/fuse-cache")); err != nil || !fi.IsDir() {
		t.Fatalf("cache dir not prepared: %v", err)
	}
}

func TestSelectAndPrepare_NoEligibleDisk(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "proc"), 0o755)
	os.WriteFile(filepath.Join(root, "proc/mounts"), []byte("tmpfs /run tmpfs rw 0 0\n"), 0o644)

	opts := DefaultOptions()
	opts.HostRoot = root
	opts.Benchmark = false
	if _, err := SelectAndPrepare(opts); err == nil {
		t.Fatal("expected error with no eligible filesystems")
	}
}

func TestSelectAndPrepare_MinFreeFilters(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "proc"), 0o755)
	os.MkdirAll(filepath.Join(root, "small"), 0o755)
	os.WriteFile(filepath.Join(root, "proc/mounts"), []byte("/dev/sda1 /small ext4 rw 0 0\n"), 0o644)

	origStatfs := statfsFunc
	defer func() { statfsFunc = origStatfs }()
	statfsFunc = func(path string) (int64, int64, error) {
		return 10 << 30, 1 << 30, nil // only 1GiB free
	}

	opts := DefaultOptions()
	opts.HostRoot = root
	opts.Benchmark = false
	opts.MinFreeBytes = 2 << 30
	if _, err := SelectAndPrepare(opts); err == nil {
		t.Fatal("candidate below min free space must be filtered")
	}
}

func TestBenchmarkDir(t *testing.T) {
	dir := t.TempDir()
	w, r, err := benchmarkDir(dir, 4<<20) // small: unit-test speed
	if err != nil {
		t.Fatalf("benchmark: %v", err)
	}
	if w <= 0 || r <= 0 {
		t.Fatalf("throughput = (%f, %f), want > 0", w, r)
	}
	// Temp file must be cleaned up.
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		t.Fatalf("benchmark left file behind: %s", e.Name())
	}
}

func TestRefreshFreeSpace(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "mnt/fuse-cache"), 0o755)

	origStatfs := statfsFunc
	defer func() { statfsFunc = origStatfs }()
	free := int64(100 << 30)
	statfsFunc = func(path string) (int64, int64, error) {
		return 200 << 30, free, nil
	}

	cfg := &Config{CacheDir: "/mnt/fuse-cache", CacheBytes: 1, GeneratedAt: time.Now().Add(-time.Hour)}
	opts := DefaultOptions()
	if err := RefreshFreeSpace(cfg, root, opts); err != nil {
		t.Fatalf("refresh: %v", err)
	}
	if cfg.FreeBytes != free || cfg.CacheBytes != int64(float64(free)*0.8) {
		t.Fatalf("refreshed cfg = %+v", cfg)
	}

	// Disk fills up: budget follows.
	free = 10 << 30
	if err := RefreshFreeSpace(cfg, root, opts); err != nil {
		t.Fatalf("refresh 2: %v", err)
	}
	if cfg.CacheBytes != int64(float64(free)*0.8) {
		t.Fatalf("budget after fill = %d", cfg.CacheBytes)
	}
}

func TestDiscover_OneCandidatePerDevice(t *testing.T) {
	root := t.TempDir()
	os.MkdirAll(filepath.Join(root, "proc"), 0o755)
	os.MkdirAll(filepath.Join(root, "mnt"), 0o755)
	os.MkdirAll(filepath.Join(root, "mnt/sub"), 0o755)
	// Same device bind-mounted twice: only the first mount is kept.
	mounts := `/dev/sda1 /mnt ext4 rw 0 0
/dev/sda1 /mnt/sub ext4 rw 0 0
`
	os.WriteFile(filepath.Join(root, "proc/mounts"), []byte(mounts), 0o644)

	origStatfs := statfsFunc
	defer func() { statfsFunc = origStatfs }()
	statfsFunc = func(path string) (int64, int64, error) { return 100 << 30, 50 << 30, nil }

	opts := DefaultOptions()
	opts.HostRoot = root
	cands, err := Discover(opts)
	if err != nil {
		t.Fatalf("discover: %v", err)
	}
	if len(cands) != 1 {
		t.Fatalf("candidates = %d, want 1 (dedup by device)", len(cands))
	}
	if cands[0].MountPoint != "/mnt" {
		t.Fatalf("kept mount = %s, want /mnt", cands[0].MountPoint)
	}
}

// TestDiscover_SkipsFileBindMounts reproduces NI-1: Kubernetes bind-mounts
// /etc/hosts as a FILE backed by the node root fs. statfs on it reports the
// root fs's large free space, so it used to win discovery and then crash on
// MkdirAll(<file>/fuse-cache). Discovery must skip non-directory mount points.
func TestDiscover_SkipsFileBindMounts(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "proc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(root, "data"), 0o755); err != nil {
		t.Fatal(err)
	}
	// /etc/hosts exists as a FILE on the fake host.
	if err := os.MkdirAll(filepath.Join(root, "etc"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(root, "etc/hosts"), []byte("127.0.0.1 x\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	mounts := `/dev/sda1 /etc/hosts ext4 rw 0 0
/dev/sdb1 /data ext4 rw 0 0
`
	if err := os.WriteFile(filepath.Join(root, "proc/mounts"), []byte(mounts), 0o644); err != nil {
		t.Fatal(err)
	}

	origStatfs := statfsFunc
	defer func() { statfsFunc = origStatfs }()
	// Make the /etc/hosts fs look *bigger* so it would win without the fix.
	statfsFunc = func(path string) (int64, int64, error) {
		if filepath.HasPrefix(path, filepath.Join(root, "etc")) {
			return 1 << 40, 900 << 30, nil
		}
		return 256 << 30, 100 << 30, nil
	}

	opts := DefaultOptions()
	opts.HostRoot = root
	cands, err := Discover(opts)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}
	for _, c := range cands {
		if c.MountPoint == "/etc/hosts" {
			t.Fatalf("discovery selected file bind-mount /etc/hosts: %+v", c)
		}
	}
	// The real directory mount must still be found.
	found := false
	for _, c := range cands {
		if c.MountPoint == "/data" {
			found = true
		}
	}
	if !found {
		t.Fatalf("directory mount /data not discovered; candidates=%+v", cands)
	}
}

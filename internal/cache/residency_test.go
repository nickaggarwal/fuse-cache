package cache

// Tests for node-local residency reporting (residency.go). The property that
// matters is that the report measures the disk rather than repeating the
// coordinator's claim: eviction never retracts location rows, so a report
// built from metadata alone would say 100% for a node holding nothing.

import (
	"context"
	"errors"
	"os"
	"testing"

	"fuse-client/internal/coordinator"
)

func residencyManager(t *testing.T, locs []*coordinator.FileLocation) (*DefaultCacheManager, *countingStorage, *flakyWarmCoordinator) {
	t.Helper()
	return warmReconcileManager(t, 1<<30, locs)
}

func TestPrefixResidency_MeasuresDiskNotMetadata(t *testing.T) {
	ctx := context.Background()
	locs := []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 400},
		{FilePath: "/models/b.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 600},
	}
	cm, cloud, _ := residencyManager(t, locs)
	cloud.Write(ctx, "/models/a.bin", make([]byte, 400))
	cloud.Write(ctx, "/models/b.bin", make([]byte, 600))

	// Nothing local yet: the coordinator still lists both files, so a
	// metadata-derived report would claim full residency here.
	rep, err := cm.PrefixResidency(ctx, "/models")
	if err != nil {
		t.Fatalf("PrefixResidency: %v", err)
	}
	if rep.Files != 2 || rep.Bytes != 1000 {
		t.Fatalf("denominator = %d files/%d bytes, want 2/1000", rep.Files, rep.Bytes)
	}
	if rep.ResidentFiles != 0 || rep.ResidentBytes != 0 || rep.Fraction != 0 {
		t.Fatalf("cold node reported resident: %+v", rep)
	}

	// Warm one of the two: residency must be partial and byte-weighted, not
	// file-counted (600/1000, not 1/2).
	if _, err := cm.WarmPrefixOpts(ctx, "/models", WarmupOptions{Mode: "full", Source: "cloud-only"}); err != nil {
		t.Fatalf("warm: %v", err)
	}
	cm.bgWg.Wait()
	local, ok := cm.LocalFilePath(ctx, "/models/a.bin")
	if !ok {
		t.Fatal("expected /models/a.bin local after warm")
	}
	if err := os.Remove(local); err != nil {
		t.Fatalf("evict a.bin: %v", err)
	}

	rep, err = cm.PrefixResidency(ctx, "/models")
	if err != nil {
		t.Fatalf("PrefixResidency after evict: %v", err)
	}
	if rep.ResidentFiles != 1 || rep.ResidentBytes != 600 {
		t.Fatalf("after evicting a.bin: resident=%d files/%d bytes, want 1/600 (%+v)",
			rep.ResidentFiles, rep.ResidentBytes, rep)
	}
	if rep.Fraction < 0.59 || rep.Fraction > 0.61 {
		t.Fatalf("fraction = %v, want ~0.6", rep.Fraction)
	}
}

func TestPrefixResidency_ReportsIntentVerificationTime(t *testing.T) {
	ctx := context.Background()
	cm, cloud, _ := residencyManager(t, []*coordinator.FileLocation{
		{FilePath: "/models/llama/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 100},
	})
	cloud.Write(ctx, "/models/llama/a.bin", make([]byte, 100))

	rep, err := cm.PrefixResidency(ctx, "/models/llama")
	if err != nil {
		t.Fatalf("PrefixResidency: %v", err)
	}
	if rep.Declared {
		t.Fatal("no intent registered, but report says declared")
	}

	cm.RegisterWarmIntent("vol-1", "/models", WarmupOptions{Mode: "full", Source: "cloud-only"})
	cm.bgWg.Wait()

	// An intent on an ancestor prefix covers this one — a CSI mount declares
	// the volume root, and a router asks about a subdirectory of it.
	rep, err = cm.PrefixResidency(ctx, "/models/llama")
	if err != nil {
		t.Fatalf("PrefixResidency after intent: %v", err)
	}
	if !rep.Declared {
		t.Fatal("ancestor intent did not mark the prefix declared")
	}
	if rep.LastVerified.IsZero() {
		t.Fatal("clean pass did not set LastVerified")
	}
	if rep.Fraction != 1 {
		t.Fatalf("fraction = %v after warm, want 1", rep.Fraction)
	}
}

func TestPrefixResidency_CoordinatorFailureIsAnError(t *testing.T) {
	ctx := context.Background()
	cm, _, coord := residencyManager(t, []*coordinator.FileLocation{
		{FilePath: "/models/a.bin", PeerID: "p1", StorageTier: "cloud", FileSize: 100},
	})
	coord.fail(errors.New("coordinator unreachable"))

	// An empty report is indistinguishable from "this node holds nothing", so
	// a failed enumeration must not be flattened into one.
	if _, err := cm.PrefixResidency(ctx, "/models"); err == nil {
		t.Fatal("expected error when enumeration fails, got a report")
	}

	cm.config.Coordinator = nil
	if _, err := cm.PrefixResidency(ctx, "/models"); err == nil {
		t.Fatal("expected error with no coordinator, got a report")
	}
}

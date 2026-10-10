package cache

import (
	"reflect"
	"testing"
)

// The empty prefix is the migration guarantee for every existing deployment:
// if this ever stops being the identity mapping, keys move under live data.
func TestKeyMapperEmptyPrefixIsIdentity(t *testing.T) {
	t.Parallel()

	m := NewKeyMapper("")
	for _, path := range []string{"/models/a.bin", "models/a.bin", "/x_chunk_3", ""} {
		if got := m.Key(path); got != path {
			t.Fatalf("Key(%q) = %q, want unchanged", path, got)
		}
		if got := m.Candidates(path); !reflect.DeepEqual(got, []string{path}) {
			t.Fatalf("Candidates(%q) = %v, want exactly one unchanged candidate", path, got)
		}
	}
}

func TestKeyMapperScopesWrites(t *testing.T) {
	t.Parallel()

	m := NewKeyMapper("cluster-a")
	if got, want := m.Key("/models/a.bin"), "cluster-a/models/a.bin"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	// Chunk objects must be scoped too, or a chunked file half-migrates.
	if got, want := m.Key("/models/a.bin_chunk_7"), "cluster-a/models/a.bin_chunk_7"; got != want {
		t.Fatalf("chunk Key = %q, want %q", got, want)
	}
}

// Operators should not have to know which slash spelling the code wants.
func TestKeyMapperNormalizesPrefixSpelling(t *testing.T) {
	t.Parallel()

	want := "cluster-a/models/a.bin"
	for _, prefix := range []string{"cluster-a", "/cluster-a", "cluster-a/", "/cluster-a/", "  cluster-a  "} {
		if got := NewKeyMapper(prefix).Key("/models/a.bin"); got != want {
			t.Fatalf("prefix %q: Key = %q, want %q", prefix, got, want)
		}
	}
	// Whitespace- and slash-only prefixes mean "unscoped", not a literal key.
	for _, prefix := range []string{"", "   ", "/", "///"} {
		if got := NewKeyMapper(prefix).Prefix(); got != "" {
			t.Fatalf("prefix %q normalized to %q, want empty", prefix, got)
		}
	}
}

// A cluster that adopts a prefix must keep reading what it wrote before it.
func TestKeyMapperCandidatesFallBackToLegacyKey(t *testing.T) {
	t.Parallel()

	got := NewKeyMapper("cluster-a").Candidates("/models/a.bin")
	want := []string{"cluster-a/models/a.bin", "/models/a.bin"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Candidates = %v, want %v (scoped first, legacy second)", got, want)
	}
}

// TestScopeCloudKeysEmptyPrefixLeavesBackendsUnscoped is the migration
// guarantee at the wiring level, not just in the mapper: a deployment that
// sets no prefix must get byte-identical keys on all three providers.
func TestScopeCloudKeysEmptyPrefixLeavesBackendsUnscoped(t *testing.T) {
	s3 := &CloudStorage{}
	az := &AzureStorage{}
	gs := &GCPStorage{}
	for _, prefix := range []string{"", "  ", "/", "//"} {
		ScopeCloudKeys(s3, prefix)
		ScopeCloudKeys(az, prefix)
		ScopeCloudKeys(gs, prefix)
	}
	if got := s3.objectKey("/models/a.bin"); got != "models/a.bin" {
		t.Errorf("s3 unscoped key = %q, want %q", got, "models/a.bin")
	}
	// Azure and GCS have always used the raw path, leading slash and all.
	// Trimming it now would orphan every object already written.
	if got := az.objectKey("/models/a.bin"); got != "/models/a.bin" {
		t.Errorf("azure unscoped key = %q, want %q", got, "/models/a.bin")
	}
	if got := gs.objectKey("/models/a.bin"); got != "/models/a.bin" {
		t.Errorf("gcs unscoped key = %q, want %q", got, "/models/a.bin")
	}
	for name, cands := range map[string][]string{
		"azure": az.readKeyCandidates("/models/a.bin"),
		"gcs":   gs.readKeyCandidates("/models/a.bin"),
	} {
		if len(cands) != 1 || cands[0] != "/models/a.bin" {
			t.Errorf("%s unscoped candidates = %v, want exactly the raw path", name, cands)
		}
	}
}

func TestScopeCloudKeysAppliesToEveryProvider(t *testing.T) {
	s3 := &CloudStorage{}
	az := &AzureStorage{}
	gs := &GCPStorage{}
	ScopeCloudKeys(s3, "cluster-a")
	ScopeCloudKeys(az, "cluster-a")
	ScopeCloudKeys(gs, "cluster-a")

	if got := s3.objectKey("/models/a.bin"); got != "cluster-a/models/a.bin" {
		t.Errorf("s3 scoped key = %q", got)
	}
	if got := az.objectKey("/models/a.bin"); got != "cluster-a/models/a.bin" {
		t.Errorf("azure scoped key = %q", got)
	}
	if got := gs.objectKey("/models/a.bin"); got != "cluster-a/models/a.bin" {
		t.Errorf("gcs scoped key = %q", got)
	}

	// Reads try the scoped spelling first, then the legacy one, so a cluster
	// adopting a prefix keeps reading everything it wrote before.
	want := []string{"cluster-a/models/a.bin", "/models/a.bin"}
	for name, cands := range map[string][]string{
		"azure": az.readKeyCandidates("/models/a.bin"),
		"gcs":   gs.readKeyCandidates("/models/a.bin"),
	} {
		if len(cands) != len(want) || cands[0] != want[0] || cands[1] != want[1] {
			t.Errorf("%s scoped candidates = %v, want %v", name, cands, want)
		}
	}
	if cands := s3.readKeyCandidates("/models/a.bin"); cands[0] != "cluster-a/models/a.bin" {
		t.Errorf("s3 scoped candidates = %v, want scoped first", cands)
	}
}

// TestScopeCloudKeysSkipsLocalDisk pins that NVMe is never prefixed. Its
// paths are filesystem paths under the cache root, not object keys -- a
// prefix there would relocate the whole local cache.
func TestScopeCloudKeysSkipsLocalDisk(t *testing.T) {
	ns := &NVMeStorage{}
	before := *ns
	ScopeCloudKeys(ns, "cluster-a")
	if *ns != before {
		t.Error("ScopeCloudKeys mutated NVMeStorage; local disk must never be cluster-scoped")
	}
}

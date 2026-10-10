package cache

import "strings"

// KeyMapper scopes object-storage keys to a cluster.
//
// Every tier has always addressed objects by the mount-relative path alone,
// so two clusters pointed at one bucket write the same key for the same path
// and silently clobber each other. The prefix is what makes a shared bucket
// safe: each cluster writes under its own namespace, and the global control
// plane can hand another cluster a key it can actually fetch.
//
// The zero value is the historical behavior exactly -- an empty prefix maps
// every path to itself -- so a deployment that never sets one is unaffected.
type KeyMapper struct {
	// prefix is stored already trimmed of surrounding slashes, so Key can
	// join with a single separator without re-checking the shape every call.
	prefix string
}

// NewKeyMapper builds a mapper for a cluster key prefix. The prefix is
// normalized here rather than at each use: "/a/", "a/" and "a" all name the
// same namespace, and an operator should not have to know which spelling the
// code wants.
func NewKeyMapper(prefix string) KeyMapper {
	return KeyMapper{prefix: strings.Trim(strings.TrimSpace(prefix), "/")}
}

// Prefix returns the normalized cluster prefix, empty when unscoped.
func (m KeyMapper) Prefix() string { return m.prefix }

// Key returns the key to WRITE path under. Writes always use the scoped form;
// only reads consider the legacy spelling (see Candidates).
func (m KeyMapper) Key(path string) string {
	if m.prefix == "" {
		return path
	}
	return m.prefix + "/" + strings.TrimPrefix(path, "/")
}

// Candidates returns the keys to try for a READ, in order.
//
// This generalizes the existing per-backend keyCandidates: objects written
// before a prefix was configured still live at the bare path, so a cluster
// that adopts a prefix must keep reading everything it wrote beforehand.
// Scoped first, because after migration that is the hit on the first try.
//
// Backends layer their own normalization (S3 trims the leading slash, Azure
// and GCS do not) over whatever comes back, so this deliberately does not
// trim: it preserves each caller's spelling and only adds the scope.
func (m KeyMapper) Candidates(path string) []string {
	if m.prefix == "" {
		return []string{path}
	}
	return []string{m.Key(path), path}
}

// ScopeCloudKeys points a freshly-constructed cloud backend at a
// cluster-scoped key namespace. An empty prefix is a no-op, so the call can
// be unconditional at every construction site.
//
// This is a post-construction mutation rather than a constructor argument on
// purpose: the three backends have three different constructor shapes, and
// widening all of them (plus every caller) to carry a field that is empty in
// every existing deployment buys nothing. It is only ever called on a store
// that has not been published yet, so there is no store to race with.
//
// A backend with no notion of cluster scoping -- NVMe, which is local disk
// and must never be prefixed -- is left alone.
func ScopeCloudKeys(store TierStorage, prefix string) {
	m := NewKeyMapper(prefix)
	if m.Prefix() == "" {
		return
	}
	switch s := store.(type) {
	case *CloudStorage:
		s.keys = m
	case *AzureStorage:
		s.keys = m
	case *GCPStorage:
		s.keys = m
	}
}

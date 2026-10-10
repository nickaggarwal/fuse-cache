// Package globalcp implements the global control plane: the service that
// makes a published subset of each cluster's cache discoverable to other
// clusters.
//
// It holds no file bytes and brokers no connections. A cluster publishes
// "path P is in bucket B under prefix K"; another cluster reads P from
// object storage itself. See proto/globalcp.proto for why the global row is
// an ObjectRef rather than a peer-scoped FileLocation.
package globalcp

import "strings"

// MatchesPrefix reports whether path falls under prefix.
//
// The subtlety this exists for: "/models" must not match "/models-v2/a.bin".
// Naive strings.HasPrefix says it does, and the result is a cluster
// publishing paths outside the allowlist an operator believed they had set.
// A prefix matches at a path boundary or exactly, never mid-segment.
//
// An empty prefix matches nothing. "Allow everything" must be spelled "/",
// because an allowlist that silently means "all paths" when a config key is
// missing or mistyped is the one failure mode this whole mechanism exists to
// prevent.
func MatchesPrefix(path, prefix string) bool {
	if prefix == "" || path == "" {
		return false
	}
	path = normalizePath(path)
	prefix = normalizePath(prefix)
	if prefix == "/" {
		return true
	}
	if path == prefix {
		return true
	}
	return strings.HasPrefix(path, strings.TrimSuffix(prefix, "/")+"/")
}

// MatchesAny reports whether path falls under any prefix in the list.
// An empty list matches nothing, so a cluster that configures no allowlist
// publishes nothing -- local-only is the default.
func MatchesAny(path string, prefixes []string) bool {
	for _, p := range prefixes {
		if MatchesPrefix(path, p) {
			return true
		}
	}
	return false
}

// normalizePath gives every path one leading slash and no trailing one, so
// "/models/", "models" and "/models" are the same allowlist entry. Without
// this the allowlist silently depends on how an operator happened to type a
// value into a Helm values file.
func normalizePath(p string) string {
	p = strings.TrimSpace(p)
	if p == "" {
		return ""
	}
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	if p == "/" {
		return p
	}
	return strings.TrimRight(p, "/")
}

// NormalizePrefixes canonicalizes an allowlist and drops empties, so the
// stored form is comparable and a trailing-slash difference cannot produce
// two entries that mean the same thing.
func NormalizePrefixes(in []string) []string {
	if len(in) == 0 {
		return nil
	}
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, p := range in {
		n := normalizePath(p)
		if n == "" {
			continue
		}
		if _, dup := seen[n]; dup {
			continue
		}
		seen[n] = struct{}{}
		out = append(out, n)
	}
	return out
}

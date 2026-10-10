package globalcp

import "testing"

// TestMatchesPrefixRespectsPathBoundaries is the near-miss case the
// allowlist exists for. "/models" matching "/models-v2/..." would publish
// paths an operator believed they had excluded, and strings.HasPrefix alone
// says it matches.
func TestMatchesPrefixRespectsPathBoundaries(t *testing.T) {
	tests := []struct {
		path, prefix string
		want         bool
	}{
		{"/models/a.bin", "/models", true},
		{"/models/a.bin", "/models/", true},
		{"/models/deep/a.bin", "/models", true},
		{"/models", "/models", true},

		{"/models-v2/a.bin", "/models", false},
		{"/models2/a.bin", "/models", false},
		{"/modelsa", "/models", false},
		{"/other/a.bin", "/models", false},

		// Spelling must not change meaning: an operator typing "models/"
		// into a values file means the same thing as "/models".
		{"models/a.bin", "/models", true},
		{"/models/a.bin", "models/", true},
		{"/models/a.bin", "  /models  ", true},

		// "/" is the explicit way to say everything.
		{"/anything/at/all", "/", true},

		// An empty prefix matches NOTHING. A missing or mistyped config key
		// must fail closed; "publishes everything" is the one outcome this
		// mechanism exists to prevent.
		{"/models/a.bin", "", false},
		{"", "/models", false},
	}
	for _, tc := range tests {
		if got := MatchesPrefix(tc.path, tc.prefix); got != tc.want {
			t.Errorf("MatchesPrefix(%q, %q) = %v, want %v", tc.path, tc.prefix, got, tc.want)
		}
	}
}

func TestMatchesAnyEmptyListMatchesNothing(t *testing.T) {
	if MatchesAny("/models/a.bin", nil) {
		t.Error("nil allowlist matched; a cluster that configures nothing must publish nothing")
	}
	if MatchesAny("/models/a.bin", []string{}) {
		t.Error("empty allowlist matched")
	}
	if !MatchesAny("/ckpt/x", []string{"/models", "/ckpt"}) {
		t.Error("second entry did not match")
	}
}

func TestNormalizePrefixesCanonicalizesAndDedupes(t *testing.T) {
	got := NormalizePrefixes([]string{"/models/", "models", " /models ", "", "  ", "/ckpt"})
	want := []string{"/models", "/ckpt"}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
	if NormalizePrefixes(nil) != nil {
		t.Error("nil input should stay nil")
	}
}

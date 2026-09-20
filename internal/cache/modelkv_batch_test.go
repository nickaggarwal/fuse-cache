package cache

import (
	"strings"
	"testing"
)

func TestBatchRoundTrip(t *testing.T) {
	keys := []string{"a@model@1@0@h1", "b@model@1@0@h2", "c@model@1@0@h3"}
	blobs := map[string][]byte{
		keys[0]: []byte("first"),
		keys[1]: []byte(strings.Repeat("second", 1000)),
		keys[2]: {}, // empty block must survive the round trip
	}
	payload, err := encodeBatch(keys, blobs)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	out, err := decodeBatch(payload)
	if err != nil {
		t.Fatalf("decodeBatch: %v", err)
	}
	if len(out) != len(keys) {
		t.Fatalf("got %d blocks, want %d", len(out), len(keys))
	}
	for _, k := range keys {
		if string(out[k]) != string(blobs[k]) {
			t.Fatalf("block %q mismatch", k)
		}
	}
}

// TestBatchDeterministic: the same sorted key set must encode identically, so
// a re-flush of the same blocks is byte-identical.
func TestBatchDeterministic(t *testing.T) {
	keys := []string{"k1", "k2"}
	blobs := map[string][]byte{"k1": []byte("aa"), "k2": []byte("bb")}
	a, err := encodeBatch(keys, blobs)
	if err != nil {
		t.Fatalf("encode a: %v", err)
	}
	b, err := encodeBatch(keys, blobs)
	if err != nil {
		t.Fatalf("encode b: %v", err)
	}
	if string(a) != string(b) {
		t.Fatalf("encoding is not deterministic")
	}
}

// TestBatchDetectsCorruption is the load-bearing one: silently serving a
// corrupt KV block yields wrong tokens with no error anywhere. A flipped bit
// must be an error, not a subtly wrong answer.
func TestBatchDetectsCorruption(t *testing.T) {
	keys := []string{"k1"}
	blobs := map[string][]byte{"k1": []byte(strings.Repeat("payload", 100))}
	payload, err := encodeBatch(keys, blobs)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	// Flip a bit in the data region (past the header and key).
	corrupt := make([]byte, len(payload))
	copy(corrupt, payload)
	corrupt[len(corrupt)-10] ^= 0x01

	if _, err := decodeBatch(corrupt); err == nil {
		t.Fatalf("decodeBatch accepted corrupt data")
	} else if !strings.Contains(err.Error(), "checksum") {
		t.Fatalf("wrong error for corruption: %v", err)
	}
}

func TestBatchRejectsBadHeader(t *testing.T) {
	cases := map[string][]byte{
		"empty":     {},
		"short":     []byte("FK"),
		"bad magic": []byte("XXXX\x01\x00\x00\x00\x00\x00\x00\x00"),
		"truncated": []byte("FKVB\x01\x00\x00\x00\x05\x00\x00\x00"),
	}
	for name, in := range cases {
		if _, err := decodeBatch(in); err == nil {
			t.Fatalf("%s: decodeBatch accepted malformed payload", name)
		}
	}
}

// TestBatchRejectsOversizedBlock guards the allocation path: a corrupt length
// field must not drive a huge allocation.
func TestBatchRejectsOversizedBlock(t *testing.T) {
	keys := []string{"k1"}
	blobs := map[string][]byte{"k1": []byte("small")}
	payload, err := encodeBatch(keys, blobs)
	if err != nil {
		t.Fatalf("encodeBatch: %v", err)
	}
	// Header is 4+4+4 = 12 bytes, then keyLen(4) + key(2) = offset 18 is dataLen.
	corrupt := make([]byte, len(payload))
	copy(corrupt, payload)
	for i := 18; i < 26 && i < len(corrupt); i++ {
		corrupt[i] = 0xFF
	}
	if _, err := decodeBatch(corrupt); err == nil {
		t.Fatalf("decodeBatch accepted an absurd block length")
	}
}

func TestBatchMissingKeyIsError(t *testing.T) {
	if _, err := encodeBatch([]string{"present", "absent"}, map[string][]byte{"present": []byte("x")}); err == nil {
		t.Fatalf("encodeBatch accepted a key with no payload")
	}
}

func TestSanitizeKVKey(t *testing.T) {
	// LMCache keys carry the model name, which contains '/'.
	got := sanitizeKVKey("vllm@Qwen/Qwen2.5-1.5B-Instruct@1@0@abc123")
	if strings.Contains(got, "/") {
		t.Fatalf("sanitized key still contains a path separator: %q", got)
	}
	if strings.Contains(sanitizeKVKey("a/../../etc/passwd"), "..") {
		t.Fatalf("sanitized key still contains a traversal sequence")
	}
}

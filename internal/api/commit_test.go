package api

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/cache"
)

func commitRig(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	cm, err := cache.NewCacheManager(&cache.CacheConfig{NVMePath: dir, MaxNVMeSize: 64 << 20, ChunkSize: 1 << 20, CloudTimeout: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cm.Shutdown)
	srv := httptest.NewServer(NewHandler(cm, nil, "p", "").SetupRoutes())
	t.Cleanup(srv.Close)
	return srv.URL, dir
}

// stage writes data where a front would: a .stage- sibling of path.
func stage(t *testing.T, dir, path string, data []byte) string {
	t.Helper()
	rel := strings.TrimPrefix(path, "/") + ".stage-front1"
	full := filepath.Join(dir, rel)
	if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return rel
}

func commit(t *testing.T, url string, req commitRequest) (int, string) {
	t.Helper()
	b, _ := json.Marshal(req)
	resp, err := http.Post(url+"/api/internal/commit", "application/json", bytes.NewReader(b))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, strings.TrimSpace(string(body))
}

func hexSHA(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }

func readBack(t *testing.T, url, path string) (int, []byte) {
	t.Helper()
	resp, err := http.Get(url + "/api/files" + path)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp.StatusCode, b
}

// A committed staged file is stored exactly as a PUT would store it:
// readable, chunked over the chunk size, and the staged file is gone.
func TestCommitAdoptsStagedFile(t *testing.T) {
	url, dir := commitRig(t)
	for _, n := range []int{0, 1000, 3<<20 + 7} {
		data := bytes.Repeat([]byte{byte(n % 251)}, n)
		path := "/models/m/f" + strings.Repeat("x", n%5) + ".bin"
		staged := stage(t, dir, path, data)
		if code, msg := commit(t, url, commitRequest{Path: path, Staged: staged, Size: int64(n), SHA256: hexSHA(data)}); code != 200 {
			t.Fatalf("size %d: commit %d %s", n, code, msg)
		}
		if code, got := readBack(t, url, path); code != 200 || !bytes.Equal(got, data) {
			t.Fatalf("size %d: read back %d, %d bytes", n, code, len(got))
		}
		if _, err := os.Stat(filepath.Join(dir, staged)); !os.IsNotExist(err) {
			t.Fatalf("size %d: staged file left behind", n)
		}
	}
}

// Every refusal leaves nothing stored and removes the staged file.
func TestCommitRefusals(t *testing.T) {
	url, dir := commitRig(t)
	blob := []byte("layer bytes")
	d := hexSHA(blob)
	other := hexSHA([]byte("other"))
	cases := []struct {
		name  string
		path  string
		req   func(staged string) commitRequest
		code  int
		stays bool // staged file is outside the commit's reach and must survive
	}{
		{"oci digest mismatch", "/oci/sha256/" + other, func(s string) commitRequest {
			return commitRequest{Path: "/oci/sha256/" + other, Staged: s, Size: int64(len(blob)), SHA256: d}
		}, 400, false},
		{"size mismatch", "/a.bin", func(s string) commitRequest {
			return commitRequest{Path: "/a.bin", Staged: s, Size: int64(len(blob)) + 1, SHA256: d}
		}, 400, false},
		{"staged not a sibling", "/dir1/a.bin", func(s string) commitRequest {
			return commitRequest{Path: "/dir2/a.bin", Staged: s, Size: int64(len(blob)), SHA256: d}
		}, 400, true},
		{"staged not a stage name", "/b.bin", func(s string) commitRequest {
			return commitRequest{Path: "/b.bin", Staged: "b.bin", Size: int64(len(blob)), SHA256: d}
		}, 400, true},
		{"staged escapes the cache dir", "/c.bin", func(s string) commitRequest {
			return commitRequest{Path: "/c.bin", Staged: "../c.bin.stage-x", Size: int64(len(blob)), SHA256: d}
		}, 400, true},
		{"too large", "/d.bin", func(s string) commitRequest {
			return commitRequest{Path: "/d.bin", Staged: s, Size: defaultMaxUploadSize + 1, SHA256: d}
		}, 413, false},
	}
	for _, c := range cases {
		staged := stage(t, dir, c.path, blob)
		code, msg := commit(t, url, c.req(staged))
		if code != c.code {
			t.Errorf("%s: got %d %q, want %d", c.name, code, msg, c.code)
		}
		if code, _ := readBack(t, url, c.req(staged).Path); code == 200 {
			t.Errorf("%s: refused commit stored the file", c.name)
		}
		_, err := os.Stat(filepath.Join(dir, staged))
		if c.stays && err != nil {
			t.Errorf("%s: a file the commit could not name was removed", c.name)
		}
		if !c.stays && !os.IsNotExist(err) {
			t.Errorf("%s: staged file left behind", c.name)
		}
	}
}

// A matching OCI commit stores the blob and serves it on the blob route.
func TestCommitOCIBlob(t *testing.T) {
	url, dir := commitRig(t)
	blob := bytes.Repeat([]byte("nydus"), 4000)
	path := "/oci/sha256/" + hexSHA(blob)
	staged := stage(t, dir, path, blob)
	if code, msg := commit(t, url, commitRequest{Path: path, Staged: staged, Size: int64(len(blob)), SHA256: strings.ToUpper(hexSHA(blob))}); code != 200 {
		t.Fatalf("commit %d %s", code, msg)
	}
	resp, err := http.Get(url + "/oci/blobs/sha256:" + hexSHA(blob))
	if err != nil {
		t.Fatal(err)
	}
	got, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if resp.StatusCode != 200 || !bytes.Equal(got, blob) {
		t.Fatalf("blob route: %d, %d bytes", resp.StatusCode, len(got))
	}
}

func TestCommitIsLoopbackOnly(t *testing.T) {
	for addr, want := range map[string]bool{"127.0.0.1:5000": true, "[::1]:5000": true, "10.244.2.41:5000": false, "bogus": false} {
		if got := isLoopback(addr); got != want {
			t.Errorf("isLoopback(%q) = %v, want %v", addr, got, want)
		}
	}
}

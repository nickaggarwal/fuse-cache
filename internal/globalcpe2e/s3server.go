package globalcpe2e

import (
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
)

// s3Server is a minimal S3-protocol object store: one shared bucket, served
// over HTTP, addressed path-style.
//
// It implements only what the S3 backend actually issues -- GET, PUT, HEAD,
// DELETE on an object key -- and nothing else. That narrowness is the point.
// The property under test is that the key cluster A WRITES is the key
// cluster B DERIVES, and the only way to check that honestly is to have one
// store that neither side can special-case: A's write and B's read have to
// meet at the same string or the read 404s.
//
// This is deliberately not a stub of the TierStorage interface. A fake at
// that level would be told the key by the same code under test on both
// sides, so a reader and a writer that agreed on a WRONG key would still
// pass. Here the bytes cross a real HTTP boundary, through the real AWS SDK,
// and the map key is whatever the SDK actually put on the wire.
type s3Server struct {
	*httptest.Server

	mu      sync.Mutex
	objects map[string][]byte
	// gets records every key requested, in order, including misses. The
	// cross-cluster assertions are about which keys were tried, not only
	// about which bytes came back: a read that succeeds for the wrong
	// reason (the local prefix happening to match) is a passing test today
	// and a silent outage the first time two clusters differ.
	gets []string
}

func newS3Server() *s3Server {
	s := &s3Server{objects: make(map[string][]byte)}
	s.Server = httptest.NewServer(http.HandlerFunc(s.handle))
	return s
}

// startS3Server is newS3Server with teardown registered. One store is shared
// by every cluster in a federation: that sharing is the point, since it is
// what forces A's write key and B's read key to be the same literal string.
func startS3Server(t *testing.T) *s3Server {
	t.Helper()
	s := newS3Server()
	t.Cleanup(s.Close)
	return s
}

// handle serves path-style requests: /<bucket>/<key...>. The bucket is
// parsed and ignored for storage but echoed in errors, because the backend
// is configured with a bucket name and a mismatch should look like a
// mismatch rather than like an empty store.
func (s *s3Server) handle(w http.ResponseWriter, r *http.Request) {
	trimmed := strings.TrimPrefix(r.URL.Path, "/")
	bucket, key, found := strings.Cut(trimmed, "/")
	if !found || key == "" {
		// Bucket-level operations (list, create) are not implemented
		// because the backend never issues them.
		writeS3Error(w, http.StatusNotImplemented, "NotImplemented", "bucket-level operations are not served")
		return
	}
	_ = bucket

	switch r.Method {
	case http.MethodPut:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			writeS3Error(w, http.StatusBadRequest, "IncompleteBody", err.Error())
			return
		}
		s.mu.Lock()
		s.objects[key] = body
		s.mu.Unlock()
		w.Header().Set("ETag", `"`+fmt.Sprintf("%x", len(body))+`"`)
		w.WriteHeader(http.StatusOK)

	case http.MethodGet, http.MethodHead:
		s.mu.Lock()
		if r.Method == http.MethodGet {
			s.gets = append(s.gets, key)
		}
		data, ok := s.objects[key]
		s.mu.Unlock()
		if !ok {
			// NoSuchKey with a 404 is what isCloudMiss() recognizes as a
			// miss rather than a failure. Returning any other shape here
			// would make a stale index row look like a broken tier, which
			// is the exact confusion tier_miss.go exists to prevent -- and
			// the test would still pass while the taxonomy silently broke.
			writeS3Error(w, http.StatusNotFound, "NoSuchKey", "The specified key does not exist.")
			return
		}
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(data)))
		w.Header().Set("Accept-Ranges", "bytes")
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)

	case http.MethodDelete:
		s.mu.Lock()
		delete(s.objects, key)
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)

	default:
		writeS3Error(w, http.StatusMethodNotAllowed, "MethodNotAllowed", r.Method)
	}
}

// keys returns every stored key. Used to assert the LAYOUT of the shared
// bucket, which is the thing two clusters must agree on and the thing a
// per-cluster fake cannot show.
func (s *s3Server) keys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]string, 0, len(s.objects))
	for k := range s.objects {
		out = append(out, k)
	}
	return out
}

func (s *s3Server) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	d, ok := s.objects[key]
	return d, ok
}

func (s *s3Server) remove(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.objects, key)
}

// requestedKeys returns every key a GET was issued for, misses included.
func (s *s3Server) requestedKeys() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.gets...)
}

func (s *s3Server) resetRequests() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.gets = nil
}

type s3ErrorBody struct {
	XMLName xml.Name `xml:"Error"`
	Code    string   `xml:"Code"`
	Message string   `xml:"Message"`
}

func writeS3Error(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	_ = xml.NewEncoder(w).Encode(s3ErrorBody{Code: code, Message: message})
}

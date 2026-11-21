package api

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

// tokenRegistry is a registry like Nebius's: blob reads accept only a Bearer
// token from its own /v2/token/ realm, which takes Basic credentials.
type tokenRegistry struct {
	srv        *httptest.Server
	body       []byte
	tokenCalls atomic.Int64
	basicOnBlb atomic.Int64
}

func newTokenRegistry(t *testing.T, repo, dg string, body []byte) *tokenRegistry {
	tr := &tokenRegistry{body: body}
	want := "Basic " + base64.StdEncoding.EncodeToString([]byte("iam:secret"))
	tr.srv = httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/v2/token/":
			tr.tokenCalls.Add(1)
			if r.Header.Get("Authorization") != want || r.URL.Query().Get("scope") != "repository:"+repo+":pull" {
				http.Error(w, "denied", http.StatusUnauthorized)
				return
			}
			fmt.Fprint(w, `{"token":"tok-123","expires_in":300}`)
		case r.URL.Path == "/v2/"+repo+"/blobs/sha256:"+dg:
			if r.Header.Get("Authorization") != "Bearer tok-123" {
				if strings.HasPrefix(r.Header.Get("Authorization"), "Basic ") {
					tr.basicOnBlb.Add(1)
				}
				w.Header().Set("Www-Authenticate", fmt.Sprintf(`Bearer realm="https://%s/v2/token/",service="%s"`, r.Host, r.Host))
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
			var a, b int64
			if _, err := fmt.Sscanf(r.Header.Get("Range"), "bytes=%d-%d", &a, &b); err == nil {
				if b >= int64(len(tr.body)) {
					b = int64(len(tr.body)) - 1
				}
				w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", a, b, len(tr.body)))
				w.WriteHeader(http.StatusPartialContent)
				_, _ = w.Write(tr.body[a : b+1])
				return
			}
			_, _ = w.Write(tr.body)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(tr.srv.Close)
	return tr
}

// nydusd with static credentials sends Basic; the fill exchanges it for a
// token at the registry's realm and fills with that.
func TestRegistryFillExchangesBasicForToken(t *testing.T) {
	SetFillUnitBytes(256 << 10)
	defer SetFillUnitBytes(defaultFillUnit)
	body, dg := patterned(2 << 20)
	tr := newTokenRegistry(t, "r", dg, body)
	fc := newFillCache()
	h := NewHandler(fc, nil, "p", "")
	h.EnableRegistryFill(RegistryFillConfig{Enabled: true, HTTPClient: tr.srv.Client()})
	srv := httptest.NewServer(h.SetupRoutes())
	t.Cleanup(srv.Close)
	addr := strings.TrimPrefix(srv.URL, "http://")
	regHost := strings.TrimPrefix(tr.srv.URL, "https://")

	basic := "Basic " + base64.StdEncoding.EncodeToString([]byte("iam:secret"))
	code, got := rangeGetAuth(t, addr, regHost, dg, 1000, 1999, basic)
	if code != http.StatusPartialContent || !bytes.Equal(got, body[1000:2000]) {
		t.Fatalf("status %d, match=%t", code, bytes.Equal(got, body[1000:2000]))
	}
	waitFor(t, func() bool { return fc.has(OCIBlobPrefix + dg) }, "fill to commit")
	if n := tr.tokenCalls.Load(); n != 1 {
		t.Fatalf("token realm calls = %d, want 1 (cached for every range)", n)
	}
	if s := h.registryFillStats(); s.TokenExchanges != 1 || s.Completed != 1 {
		t.Fatalf("stats = %+v", s)
	}
}

// Credentials are never sent to a token realm off the registry's domain.
func TestTokenRealmMustMatchRegistry(t *testing.T) {
	if sameRegistryDomain("evil.example.com", "cr.eu-west1.nebius.cloud") {
		t.Fatal("foreign realm accepted")
	}
	for _, c := range [][2]string{{"cr.eu-west1.nebius.cloud", "cr.eu-west1.nebius.cloud"}, {"auth.nebius.cloud", "cr.eu-west1.nebius.cloud"}, {"ghcr.io", "ghcr.io"}} {
		if !sameRegistryDomain(c[0], c[1]) {
			t.Fatalf("%s should be accepted for %s", c[0], c[1])
		}
	}
	r, s, sc, ok := parseBearerChallenge(`Bearer realm="https://h/v2/token/",service="h",scope="repository:a/b:pull,push"`)
	if !ok || r != "https://h/v2/token/" || s != "h" || sc != "repository:a/b:pull,push" {
		t.Fatalf("parsed %q %q %q %v", r, s, sc, ok)
	}
}

func rangeGetAuth(t *testing.T, addr, regHost, dg string, start, end int64, auth string) (int, []byte) {
	t.Helper()
	req, _ := http.NewRequest(http.MethodGet, "http://"+addr+"/v2/r/blobs/sha256:"+dg, nil)
	req.URL.Opaque = "http://" + regHost + "/v2/r/blobs/sha256:" + dg
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-%d", start, end))
	req.Header.Set("Authorization", auth)
	resp, err := http.DefaultTransport.RoundTrip(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var b bytes.Buffer
	_, _ = b.ReadFrom(resp.Body)
	return resp.StatusCode, b.Bytes()
}

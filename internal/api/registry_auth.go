package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

// Registry token exchange for fills.
//
// A fill reuses the Authorization nydusd sent with its read. Against ACR that
// is a Bearer token and works as is. nydusd configured with static
// credentials ("auth" in its backend config, as on Nebius) sends Basic
// instead, and a registry that only accepts tokens answers 401: on the Nebius
// H200 lab every fill failed its size probe ("size probe: http 401") and
// fuse-cache served nothing, every read falling back to the registry.
//
// When the registry answers 401 with a Bearer challenge, the fill now does
// what any registry client does: it asks the challenge's realm for a pull
// token for the blob's repository, presenting nydusd's credentials, and uses
// that token for the size probe and every range. Tokens are cached per
// (realm, scope, credentials) until shortly before they expire; nothing is
// written to disk or logged.

// registryTokenSlack renews a token this long before its stated expiry.
const registryTokenSlack = 30 * time.Second

type registryToken struct {
	token string
	until time.Time
}

type registryTokens struct {
	mu sync.Mutex
	m  map[string]registryToken
}

func (t *registryTokens) get(key string) (string, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	tok, ok := t.m[key]
	if !ok || time.Now().After(tok.until) {
		return "", false
	}
	return tok.token, true
}

func (t *registryTokens) put(key, token string, until time.Time) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.m == nil {
		t.m = map[string]registryToken{}
	}
	if len(t.m) > 1024 { // bound memory; it is a cache
		t.m = map[string]registryToken{}
	}
	t.m[key] = registryToken{token: token, until: until}
}

// parseBearerChallenge reads realm, service and scope from a
// WWW-Authenticate: Bearer header. ok is false for anything else.
func parseBearerChallenge(h string) (realm, service, scope string, ok bool) {
	rest, found := strings.CutPrefix(strings.TrimSpace(h), "Bearer ")
	if !found {
		return "", "", "", false
	}
	for _, part := range splitChallenge(rest) {
		k, v, found := strings.Cut(part, "=")
		if !found {
			continue
		}
		v = strings.Trim(strings.TrimSpace(v), `"`)
		switch strings.ToLower(strings.TrimSpace(k)) {
		case "realm":
			realm = v
		case "service":
			service = v
		case "scope":
			scope = v
		}
	}
	return realm, service, scope, realm != ""
}

// splitChallenge splits k="v",k="v" on commas outside quotes (scope values
// contain commas: "repository:x:pull,push").
func splitChallenge(s string) []string {
	var out []string
	inQ, start := false, 0
	for i, c := range s {
		switch c {
		case '"':
			inQ = !inQ
		case ',':
			if !inQ {
				out = append(out, s[start:i])
				start = i + 1
			}
		}
	}
	return append(out, s[start:])
}

// repoFromBlobPath returns <repo> from /v2/<repo>/blobs/<digest>.
func repoFromBlobPath(p string) string {
	rest, ok := strings.CutPrefix(p, "/v2/")
	if !ok {
		return ""
	}
	if i := strings.LastIndex(rest, "/blobs/"); i > 0 {
		return rest[:i]
	}
	return ""
}

// exchangeToken gets a pull token for src's repository from the registry's
// token realm, presenting auth (nydusd's Basic credentials, or nothing).
func (f *registryFiller) exchangeToken(ctx context.Context, src *url.URL, auth, challenge string) (string, error) {
	realm, service, scope, ok := parseBearerChallenge(challenge)
	if !ok {
		return "", fmt.Errorf("no bearer challenge")
	}
	if scope == "" {
		if repo := repoFromBlobPath(src.Path); repo != "" {
			scope = "repository:" + repo + ":pull"
		}
	}
	key := realm + "|" + service + "|" + scope + "|" + auth
	if tok, ok := f.tokens.get(key); ok {
		return tok, nil
	}
	ru, err := url.Parse(realm)
	if err != nil || (ru.Scheme != "https" && ru.Scheme != "http") {
		return "", fmt.Errorf("bad token realm %q", realm)
	}
	// The realm must be the registry itself (or a subdomain of its parent
	// domain, as ACR and GHCR use), so nydusd's credentials never go to a
	// host the request did not name.
	if !sameRegistryDomain(ru.Hostname(), src.Hostname()) {
		return "", fmt.Errorf("token realm %s is not on the registry's domain", ru.Hostname())
	}
	q := ru.Query()
	if service != "" {
		q.Set("service", service)
	}
	if scope != "" {
		q.Set("scope", scope)
	}
	ru.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, ru.String(), nil)
	if err != nil {
		return "", err
	}
	if strings.HasPrefix(auth, "Basic ") {
		req.Header.Set("Authorization", auth)
	}
	resp, err := f.cfg.HTTPClient.Do(req)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<10))
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("token realm: http %d", resp.StatusCode)
	}
	var tr struct {
		Token       string `json:"token"`
		AccessToken string `json:"access_token"`
		ExpiresIn   int    `json:"expires_in"`
	}
	if err := json.Unmarshal(body, &tr); err != nil {
		return "", fmt.Errorf("token realm: %w", err)
	}
	tok := tr.Token
	if tok == "" {
		tok = tr.AccessToken
	}
	if tok == "" {
		return "", fmt.Errorf("token realm returned no token")
	}
	life := time.Duration(tr.ExpiresIn) * time.Second
	if life <= 0 {
		life = 60 * time.Second // the spec's default
	}
	f.tokens.put(key, tok, time.Now().Add(life-registryTokenSlack))
	f.tokenExchanges.Add(1)
	return tok, nil
}

// sameRegistryDomain reports whether host a is b or shares b's registrable
// parent (last two labels): cr.eu-west1.nebius.cloud and its realm,
// myacr.azurecr.io and myacr.azurecr.io, ghcr.io and ghcr.io.
func sameRegistryDomain(a, b string) bool {
	a, b = strings.ToLower(a), strings.ToLower(b)
	if a == b {
		return true
	}
	parent := func(h string) string {
		parts := strings.Split(h, ".")
		if len(parts) < 2 {
			return h
		}
		return strings.Join(parts[len(parts)-2:], ".")
	}
	return parent(a) == parent(b)
}

// resolveFillAuth returns the Authorization to use for src: auth itself, or
// a Bearer token exchanged for it when the registry rejects auth with a
// Bearer challenge. It probes with a 1-byte range GET.
func (f *registryFiller) resolveFillAuth(ctx context.Context, src *url.URL, auth string) string {
	if strings.HasPrefix(auth, "Bearer ") {
		return auth
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, src.String(), nil)
	if err != nil {
		return auth
	}
	if auth != "" {
		req.Header.Set("Authorization", auth)
	}
	req.Header.Set("Range", "bytes=0-0")
	resp, err := f.cfg.HTTPClient.Do(req)
	if err != nil {
		return auth
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<10))
	resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		return auth
	}
	tok, err := f.exchangeToken(ctx, src, auth, resp.Header.Get("Www-Authenticate"))
	if err != nil {
		f.tokenFailures.Add(1)
		return auth
	}
	return "Bearer " + tok
}

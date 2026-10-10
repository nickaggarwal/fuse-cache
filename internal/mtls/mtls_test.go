package mtls

import (
	"crypto/x509"
	"encoding/pem"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// The failure this package exists to prevent is a partial configuration
// that quietly serves plaintext, so most of these tests are about what
// happens when a field is MISSING rather than when everything is present.

func TestEmptyConfigIsDisabled(t *testing.T) {
	var c Config
	if c.Enabled() {
		t.Fatal("a config with no fields set must not report Enabled")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("an entirely empty config is a legitimate opt-out, not an error: %v", err)
	}
}

func TestPartialConfigIsEnabledAndThenRejected(t *testing.T) {
	// Enabled() is deliberately "any", not "all". If a one-field config
	// reported disabled, a single mistyped flag name would select plaintext
	// and nothing would say so.
	cases := map[string]Config{
		"cert only":    {CertFile: "a.crt"},
		"key only":     {KeyFile: "a.key"},
		"ca only":      {CAFile: "ca.crt"},
		"missing ca":   {CertFile: "a.crt", KeyFile: "a.key"},
		"missing key":  {CertFile: "a.crt", CAFile: "ca.crt"},
		"missing cert": {KeyFile: "a.key", CAFile: "ca.crt"},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			if !c.Enabled() {
				t.Fatal("a partially configured TLS setup must report Enabled so Validate can refuse it")
			}
			err := c.Validate()
			if err == nil {
				t.Fatal("expected Validate to reject a partial configuration")
			}
			// The message has to name what is missing, or an operator is
			// left guessing which of three flags they fat-fingered.
			if c.CertFile == "" && !strings.Contains(err.Error(), "cert") {
				t.Errorf("error should name the missing certificate: %v", err)
			}
			if c.KeyFile == "" && !strings.Contains(err.Error(), "key") {
				t.Errorf("error should name the missing key: %v", err)
			}
			if c.CAFile == "" && !strings.Contains(err.Error(), "CA") {
				t.Errorf("error should name the missing CA: %v", err)
			}
		})
	}
}

// A ServerName alone is not a TLS configuration -- it is a hint about one.
// Treating it as Enabled would make a coordinator that sets only the
// server name fail at startup for no reason.
func TestServerNameAloneDoesNotEnableTLS(t *testing.T) {
	c := Config{ServerName: "globalcp.example"}
	if c.Enabled() {
		t.Fatal("ServerName alone must not enable TLS")
	}
	if err := c.Validate(); err != nil {
		t.Fatalf("ServerName alone must validate: %v", err)
	}
}

func TestCredentialsBuildFromIssuedMaterial(t *testing.T) {
	ca, err := NewTestCA()
	if err != nil {
		t.Fatalf("NewTestCA: %v", err)
	}
	dir := t.TempDir()
	cfg, err := ca.WriteTo(dir, "cluster-a")
	if err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	if !cfg.Enabled() {
		t.Fatal("material written by the test CA must produce an enabled config")
	}
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if _, err := cfg.ServerCredentials(); err != nil {
		t.Fatalf("ServerCredentials: %v", err)
	}
	cfg.ServerName = "cluster-a"
	if _, err := cfg.ClientCredentials(); err != nil {
		t.Fatalf("ClientCredentials: %v", err)
	}
	if _, err := cfg.DialOption(); err != nil {
		t.Fatalf("DialOption: %v", err)
	}
}

// The pool must contain only what the operator named. If it fell back to
// the system roots, any publicly-trusted certificate would authenticate --
// and since the cluster ID is read out of that certificate, that is
// identity forgery rather than a merely weaker channel.
func TestCAPoolRejectsACertificateFromAnotherCA(t *testing.T) {
	first, err := NewTestCA()
	if err != nil {
		t.Fatalf("NewTestCA: %v", err)
	}
	second, err := NewTestCA()
	if err != nil {
		t.Fatalf("NewTestCA: %v", err)
	}
	dir := t.TempDir()
	if _, err := first.WriteTo(dir, "trusted"); err != nil {
		t.Fatalf("WriteTo: %v", err)
	}
	pool, err := loadCAPool(filepath.Join(dir, "ca.crt"))
	if err != nil {
		t.Fatalf("loadCAPool: %v", err)
	}

	strangerPEM, _, err := second.Issue("cluster-a")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := leafOf(t, strangerPEM).Verify(x509.VerifyOptions{Roots: pool}); err == nil {
		t.Fatal("a certificate from an untrusted CA verified against the configured pool")
	}

	// Control: the same check must pass for the CA that was configured,
	// or the assertion above would hold even if loadCAPool returned an
	// empty pool that rejects everything.
	ownPEM, _, err := first.Issue("cluster-a")
	if err != nil {
		t.Fatalf("Issue: %v", err)
	}
	if _, err := leafOf(t, ownPEM).Verify(x509.VerifyOptions{
		Roots:     pool,
		KeyUsages: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}); err != nil {
		t.Fatalf("a certificate from the configured CA failed to verify: %v", err)
	}
}

func leafOf(t *testing.T, certPEM []byte) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(certPEM)
	if block == nil {
		t.Fatal("no PEM block in certificate")
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func TestMissingCAFileIsAnErrorNotAnEmptyPool(t *testing.T) {
	// An unreadable CA bundle that produced an empty pool would reject
	// every client, which looks like a client problem. It is a server
	// configuration problem and has to be reported as one.
	if _, err := loadCAPool(filepath.Join(t.TempDir(), "absent.crt")); err == nil {
		t.Fatal("expected an error for a missing CA file")
	}
}

func TestGarbageCAFileIsRejected(t *testing.T) {
	dir := t.TempDir()
	p := filepath.Join(dir, "ca.crt")
	if err := os.WriteFile(p, []byte("not a certificate"), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadCAPool(p); err == nil {
		t.Fatal("expected an error for a CA file containing no PEM certificate")
	}
}

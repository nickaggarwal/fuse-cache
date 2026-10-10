package mtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"time"
)

// TestCA issues certificates for tests and for scripts/ops.
//
// Exported from the non-test file on purpose: scripts/ops/e2e-global-mtls.sh
// needs the same material the unit tests use, and a shell script wiring
// together openssl invocations would be a second, divergent implementation
// of the trust model this package defines. cmd/globalcp-certs is the thin
// wrapper around it.
//
// NOT for production. Keys are generated in memory with no passphrase, no
// rotation, and no revocation. Real deployments get certificates from
// cert-manager or whatever the cluster already uses; this exists so the
// cross-cluster path is testable without one.
type TestCA struct {
	cert    *x509.Certificate
	key     *ecdsa.PrivateKey
	certPEM []byte
}

// NewTestCA mints a self-signed CA.
func NewTestCA() (*TestCA, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "fuse-globalcp-test-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
		BasicConstraintsValid: true,
		IsCA:                  true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		return nil, err
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, err
	}
	return &TestCA{
		cert:    cert,
		key:     key,
		certPEM: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
	}, nil
}

// CertPEM is the CA bundle both ends verify against.
func (ca *TestCA) CertPEM() []byte { return ca.certPEM }

// Issue mints a leaf certificate whose first DNS SAN is name.
//
// The SAN is what the control plane reads as the cluster ID, so issuing a
// certificate IS granting a cluster its identity -- which is the property
// the authz tests exercise by issuing one cluster a certificate naming
// another.
func (ca *TestCA) Issue(name string) (certPEM, keyPEM []byte, err error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, err
	}
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, nil, err
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		DNSNames:     []string{name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, ca.cert, &key.PublicKey, ca.key)
	if err != nil {
		return nil, nil, err
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		return nil, nil, err
	}
	return pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}),
		pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}),
		nil
}

// WriteTo issues a certificate for name and writes cert, key and the CA
// bundle into dir, returning a Config that points at them.
func (ca *TestCA) WriteTo(dir, name string) (Config, error) {
	certPEM, keyPEM, err := ca.Issue(name)
	if err != nil {
		return Config{}, err
	}
	cfg := Config{
		CertFile: filepath.Join(dir, name+".crt"),
		KeyFile:  filepath.Join(dir, name+".key"),
		CAFile:   filepath.Join(dir, "ca.crt"),
	}
	for path, data := range map[string][]byte{
		cfg.CertFile: certPEM,
		cfg.KeyFile:  keyPEM,
		cfg.CAFile:   ca.certPEM,
	} {
		// 0600 on the key: the others are public, but one mode for all
		// three is one fewer thing to get wrong when this is copied.
		if err := os.WriteFile(path, data, 0o600); err != nil {
			return Config{}, err
		}
	}
	return cfg, nil
}

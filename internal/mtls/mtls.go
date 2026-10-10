// Package mtls builds the TLS material for the one link in this system that
// crosses a trust boundary: a cluster's coordinator to the global control
// plane.
//
// Everything else here is intra-cluster and unauthenticated by design --
// peer gRPC, the coordinator's own API, etcd. This package deliberately does
// not touch any of them. The global link is different because it is the only
// one that leaves the cluster, and because the identity it carries is
// load-bearing: the control plane stamps the authenticated cluster ID onto
// every published row, so a forged identity is a cluster writing rows in
// another cluster's name.
//
// One rule runs through all of it: a partial configuration is a startup
// error, never a quiet downgrade to plaintext. The failure this prevents is
// the realistic one -- an operator sets a cert and a key, forgets the CA,
// and the service comes up serving cleartext on a public port while every
// dashboard reports healthy. A process that refuses to start gets noticed in
// seconds; one that silently serves plaintext may never be noticed at all.
package mtls

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// Config names the files that make up one end of the link.
//
// All three are required together on either end. A server needs its own
// certificate to be authenticated BY clusters and the CA to authenticate
// them; a client needs exactly the mirror image. There is no supported
// configuration where only one side proves anything, because one-way TLS
// here would encrypt the channel while leaving cluster identity to a header
// -- which is the insecure mode wearing a lock icon.
type Config struct {
	// CertFile and KeyFile are this end's own identity.
	CertFile string
	KeyFile  string
	// CAFile verifies the other end. For the control plane it is the CA
	// that signs cluster certificates; for a cluster it is the CA that
	// signs the control plane's.
	CAFile string
	// ServerName overrides the name a client verifies the control plane's
	// certificate against. Needed when the endpoint is an IP or a port-
	// forward, where the dialed address is not the name in the cert.
	// Client-side only.
	ServerName string
}

// Enabled reports whether any TLS file was named at all.
//
// Deliberately "any", not "all": a config with one field set is Enabled and
// then fails Validate. If this returned false for a partial config, a typo
// in one flag name would silently select plaintext, which is the exact
// outcome this package exists to prevent.
func (c Config) Enabled() bool {
	return strings.TrimSpace(c.CertFile) != "" ||
		strings.TrimSpace(c.KeyFile) != "" ||
		strings.TrimSpace(c.CAFile) != ""
}

// Validate reports why a partially-specified config cannot be used.
func (c Config) Validate() error {
	if !c.Enabled() {
		return nil
	}
	var missing []string
	if strings.TrimSpace(c.CertFile) == "" {
		missing = append(missing, "certificate")
	}
	if strings.TrimSpace(c.KeyFile) == "" {
		missing = append(missing, "private key")
	}
	if strings.TrimSpace(c.CAFile) == "" {
		missing = append(missing, "CA bundle")
	}
	if len(missing) > 0 {
		return fmt.Errorf("incomplete TLS configuration: missing %s "+
			"(all of cert, key and CA are required; refusing to fall back to plaintext)",
			strings.Join(missing, ", "))
	}
	return nil
}

// ServerCredentials builds the control plane's side.
//
// RequireAndVerifyClientCert is the whole point: with anything weaker,
// VerifiedChains is empty, the service cannot derive a cluster identity from
// the connection, and it would be back to trusting a header. Chosen here
// rather than left to the caller so there is no way to configure that
// combination by accident.
func (c Config) ServerCredentials() (credentials.TransportCredentials, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if !c.Enabled() {
		return nil, errors.New("mtls: no TLS configuration")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load server keypair: %w", err)
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    pool,
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// ClientCredentials builds a cluster coordinator's side.
func (c Config) ClientCredentials() (credentials.TransportCredentials, error) {
	if err := c.Validate(); err != nil {
		return nil, err
	}
	if !c.Enabled() {
		return nil, errors.New("mtls: no TLS configuration")
	}
	cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("load client keypair: %w", err)
	}
	pool, err := loadCAPool(c.CAFile)
	if err != nil {
		return nil, err
	}
	return credentials.NewTLS(&tls.Config{
		Certificates: []tls.Certificate{cert},
		RootCAs:      pool,
		ServerName:   strings.TrimSpace(c.ServerName),
		MinVersion:   tls.VersionTLS13,
	}), nil
}

// DialOption returns the transport credentials as a grpc.DialOption, or an
// error. There is no "insecure if unset" branch: a caller that wants
// plaintext must say so itself, in code a reader can see.
func (c Config) DialOption() (grpc.DialOption, error) {
	creds, err := c.ClientCredentials()
	if err != nil {
		return nil, err
	}
	return grpc.WithTransportCredentials(creds), nil
}

// loadCAPool reads a PEM bundle into a pool that contains ONLY it.
//
// x509.NewCertPool rather than SystemCertPool, deliberately. The system pool
// trusts every public CA, so a certificate bought from any of them for any
// name would be accepted -- and because cluster identity is read out of the
// certificate, that is an identity forgery, not merely a weaker channel.
// The set of clusters allowed to join is exactly the set this one CA signs.
func loadCAPool(path string) (*x509.CertPool, error) {
	pem, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read CA bundle %s: %w", path, err)
	}
	pool := x509.NewCertPool()
	if !pool.AppendCertsFromPEM(pem) {
		return nil, fmt.Errorf("CA bundle %s contains no usable certificate", path)
	}
	return pool, nil
}

package globalcp

import (
	"context"
	"crypto/x509"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// ClusterIDHeader carries the cluster identity when mTLS is off.
const ClusterIDHeader = "x-fuse-cluster-id"

// identify resolves the calling cluster's identity.
//
// The ordering is the security property, not a convenience: when a verified
// client certificate is present it is the ONLY source consulted, so a
// cluster cannot claim to be another by setting a header or a request field.
// The header and the request body are consulted only when the connection
// carries no verified certificate at all -- i.e. the embedded/dev mode,
// which binds to loopback and is documented as not deployable over a real
// network.
//
// Stage 4 (mTLS) tightens exactly one thing here: AllowInsecureIdentity
// flips to false and the fallback disappears. Keeping the seam in one
// function is what makes that a one-line change rather than an audit.
func (s *Service) identify(ctx context.Context, fallbackID string) (string, error) {
	if id := certClusterID(ctx); id != "" {
		return id, nil
	}
	if !s.AllowInsecureIdentity {
		return "", status.Error(codes.Unauthenticated,
			"no verified client certificate: cluster identity must come from mTLS")
	}
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if vals := md.Get(ClusterIDHeader); len(vals) > 0 {
			if id := strings.TrimSpace(vals[0]); id != "" {
				return id, nil
			}
		}
	}
	if id := strings.TrimSpace(fallbackID); id != "" {
		return id, nil
	}
	return "", status.Errorf(codes.Unauthenticated,
		"cluster identity required: present a client certificate or set the %s header", ClusterIDHeader)
}

// certClusterID extracts the cluster identity from a verified peer
// certificate: the first DNS SAN, else the CN. Returns "" when the
// connection has no verified chain, which is the case the caller must treat
// as unauthenticated rather than as an empty identity.
func certClusterID(ctx context.Context) string {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return ""
	}
	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ""
	}
	// VerifiedChains, not PeerCertificates: the latter is whatever the
	// client sent, verified or not.
	for _, chain := range tlsInfo.State.VerifiedChains {
		if len(chain) == 0 {
			continue
		}
		return certName(chain[0])
	}
	return ""
}

func certName(cert *x509.Certificate) string {
	for _, dns := range cert.DNSNames {
		if d := strings.TrimSpace(dns); d != "" {
			return d
		}
	}
	return strings.TrimSpace(cert.Subject.CommonName)
}

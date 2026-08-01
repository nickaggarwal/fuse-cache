package cache

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"fuse-client/internal/coordinator"
)

// TestRawRead_DeadAddressFailsFast: a blackholed address (RFC 5737 TEST-NET,
// SYNs go nowhere) must fail within the connect timeout, not the 30s request
// timeout — this is the stale-registry rollout stall.
func TestRawRead_DeadAddressFailsFast(t *testing.T) {
	if testing.Short() {
		t.Skip("dials a blackholed address")
	}
	ps := newRawPeerStorage()
	start := time.Now()
	_, err := ps.readFromPeerRaw(context.Background(), "192.0.2.1:8081", "/x")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected connect failure")
	}
	// peerConnectTimeout is 2s; allow slack for CI schedulers.
	if elapsed > peerConnectTimeout+2*time.Second {
		t.Fatalf("dead address took %v, want ~%v (connect timeout not applied?)", elapsed, peerConnectTimeout)
	}
	if !isPeerUnreachable(err) {
		t.Fatalf("connect failure not classified unreachable: %v", err)
	}
}

// TestIsPeerUnreachable_Classification: dial errors are unreachable; HTTP
// status errors, busy, and response timeouts are not.
func TestIsPeerUnreachable_Classification(t *testing.T) {
	dialErr := &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connect: no route to host")}
	if !isPeerUnreachable(dialErr) {
		t.Fatal("dial OpError should be unreachable")
	}
	readErr := &net.OpError{Op: "read", Net: "tcp", Err: errors.New("connection reset")}
	if isPeerUnreachable(readErr) {
		t.Fatal("read OpError is not a connect failure")
	}
	if isPeerUnreachable(&peerBusyError{addr: "x"}) {
		t.Fatal("busy is not unreachable")
	}
	if isPeerUnreachable(errors.New("peer raw read x: status 500")) {
		t.Fatal("HTTP status error is not unreachable")
	}
	if isPeerUnreachable(nil) {
		t.Fatal("nil is not unreachable")
	}
}

// TestReadPeerDataInner_SkipsGRPCWhenUnreachable: a dead raw address must not
// pay a second connect timeout on the gRPC port of the same dead pod.
func TestReadPeerDataInner_SkipsGRPCWhenUnreachable(t *testing.T) {
	if testing.Short() {
		t.Skip("dials a blackholed address")
	}
	ps := newRawPeerStorage()
	peer := &coordinator.PeerInfo{
		ID: "stale", Address: "192.0.2.1:8081", GRPCAddress: "192.0.2.1:9081", Status: "active",
	}
	start := time.Now()
	_, err := ps.readPeerDataInner(context.Background(), peer, "/x")
	elapsed := time.Since(start)
	if err == nil {
		t.Fatal("expected failure against dead peer")
	}
	// One connect timeout, not two: if the gRPC fallback also dialed, this
	// would take ~2x peerConnectTimeout.
	if elapsed > peerConnectTimeout+2*time.Second {
		t.Fatalf("dead peer took %v — gRPC fallback likely dialed the same dead address", elapsed)
	}
}

// TestRawRead_SlowResponseStillFallsThrough: a peer that accepts connections
// but responds with a non-busy error must NOT be classified unreachable (the
// gRPC fallback is still worth trying).
func TestRawRead_SlowResponseStillFallsThrough(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "boom", http.StatusInternalServerError)
	}))
	defer srv.Close()
	ps := newRawPeerStorage()
	addr := strings.TrimPrefix(srv.URL, "http://")
	_, err := ps.readFromPeerRaw(context.Background(), addr, "/x")
	if err == nil {
		t.Fatal("expected 500 error")
	}
	if isPeerUnreachable(err) {
		t.Fatal("alive-but-erroring peer classified unreachable; gRPC fallback would be skipped")
	}
}

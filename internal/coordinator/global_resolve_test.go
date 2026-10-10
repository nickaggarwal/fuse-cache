package coordinator

import (
	"context"
	"net"
	"testing"

	pb "fuse-client/internal/pb"

	"google.golang.org/grpc"
)

// stubMirror stands in for the globalsync mirror.
type stubMirror struct {
	locs  []*GlobalLocation
	ready bool
}

func (s *stubMirror) Resolve(string) ([]*GlobalLocation, bool) { return s.locs, s.ready }

// TestResolveGlobalWithNoMirrorIsEmptyAndNotReady is the default every
// existing deployment runs in.
//
// Not-ready is the load-bearing half. A coordinator with global sync off
// must not be able to say "no other cluster has this path" -- it has no
// grounds for that claim, and a caller that caches it as a negative stops
// looking forever.
func TestResolveGlobalWithNoMirrorIsEmptyAndNotReady(t *testing.T) {
	cs := NewCoordinatorService()
	res, err := cs.ResolveGlobal(context.Background(), "/models/a.bin")
	if err != nil {
		t.Fatalf("a coordinator with no global sync must degrade, not error: %v", err)
	}
	if len(res.Locations) != 0 {
		t.Fatalf("locations = %v, want none", res.Locations)
	}
	if res.Ready {
		t.Fatal("a coordinator with no mirror reported a ready global index")
	}
}

// TestResolveGlobalDistinguishesEmptyFromUnsynced: a synced mirror that
// holds nothing for a path is a real negative; an unsynced one is not. They
// are the same Locations slice, so Ready is the only thing carrying the
// difference and it must survive every layer.
func TestResolveGlobalDistinguishesEmptyFromUnsynced(t *testing.T) {
	for _, ready := range []bool{true, false} {
		cs := NewCoordinatorService()
		cs.SetGlobalMirror(&stubMirror{ready: ready})
		res, err := cs.ResolveGlobal(context.Background(), "/models/a.bin")
		if err != nil {
			t.Fatalf("ready=%v: %v", ready, err)
		}
		if res.Ready != ready {
			t.Fatalf("ready=%v collapsed to %v", ready, res.Ready)
		}
	}
}

// TestResolveGlobalOverGRPCPreservesTheObjectRef is the test that matters
// for the wire: the reading cluster builds its object key out of KeyPrefix
// and Provider, so a field dropped in either converter produces a read
// under the wrong key -- a miss that looks exactly like "nothing published".
func TestResolveGlobalOverGRPCPreservesTheObjectRef(t *testing.T) {
	service := NewCoordinatorService()
	service.SetGlobalMirror(&stubMirror{
		ready: true,
		locs: []*GlobalLocation{{
			ClusterID: "cluster-a", FilePath: "/models/a.bin",
			FileSize: 4096, IsChunked: true, ChunkSize: 8 << 20,
			Provider: "s3", Bucket: "shared", KeyPrefix: "cluster-a",
			Region: "us-east-1", Endpoint: "https://s3.example", PublishedAt: 1234,
		}},
	})

	srv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(srv, NewGRPCServer(service))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	client, err := NewGRPCCoordinatorClient(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	res, err := client.ResolveGlobal(context.Background(), "/models/a.bin")
	if err != nil {
		t.Fatalf("ResolveGlobal: %v", err)
	}
	if !res.Ready {
		t.Fatal("ready did not survive the round trip")
	}
	if len(res.Locations) != 1 {
		t.Fatalf("locations = %d, want 1", len(res.Locations))
	}
	got := res.Locations[0]
	want := &GlobalLocation{
		ClusterID: "cluster-a", FilePath: "/models/a.bin",
		FileSize: 4096, IsChunked: true, ChunkSize: 8 << 20,
		Provider: "s3", Bucket: "shared", KeyPrefix: "cluster-a",
		Region: "us-east-1", Endpoint: "https://s3.example", PublishedAt: 1234,
	}
	if *got != *want {
		t.Fatalf("round trip lost fields:\n got %+v\nwant %+v", *got, *want)
	}
}

// TestResolveGlobalOverGRPCWithNoMirror: the same degrade-not-error contract
// across the wire, which is where a caller actually sits.
func TestResolveGlobalOverGRPCWithNoMirror(t *testing.T) {
	srv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(srv, NewGRPCServer(NewCoordinatorService()))
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	client, err := NewGRPCCoordinatorClient(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	res, err := client.ResolveGlobal(context.Background(), "/models/a.bin")
	if err != nil {
		t.Fatalf("want a degraded answer, got error: %v", err)
	}
	if res.Ready || len(res.Locations) != 0 {
		t.Fatalf("res = %+v, want empty and not ready", res)
	}
}

// TestResolveGlobalAgainstACoordinatorThatDoesNotKnowTheRPC covers the
// rolling upgrade: during a coordinator roll, a client that already has
// -global-read will hit old replicas. Unimplemented means "this cluster has
// no global index", not a failure, and the read path must not log errors
// for the duration of every upgrade.
func TestResolveGlobalAgainstACoordinatorThatDoesNotKnowTheRPC(t *testing.T) {
	// A gRPC server with no CoordinatorService registered answers
	// Unimplemented for every method, exactly as an older binary does for a
	// method it was built without.
	srv := grpc.NewServer()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	go srv.Serve(lis)
	defer srv.Stop()

	client, err := NewGRPCCoordinatorClient(lis.Addr().String())
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer client.Close()

	res, err := client.ResolveGlobal(context.Background(), "/models/a.bin")
	if err != nil {
		t.Fatalf("Unimplemented must degrade, not surface: %v", err)
	}
	if res.Ready || len(res.Locations) != 0 {
		t.Fatalf("res = %+v, want empty and not ready", res)
	}
}

package globalcpe2e

import (
	"bytes"
	"context"
	"testing"
	"time"
)

// waitRecover is waitFor with room for the agents' jittered reconnect
// backoff, which can run several seconds across two attempts.
func waitRecover(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestGlobalIndexRecoversAfterControlPlaneRestart is I3's acceptance test on
// the production write path.
//
// A chunked file and an unchunked file are written in cluster A through the
// cache (the chunked one the way FUSE and streaming PUT write it), B learns
// both, then the control plane restarts with an empty in-memory
// store. Nothing is rewritten. Before republish, B's reconnect replaced its
// mirror with the empty snapshot and both paths stayed undiscoverable until
// someone wrote them again -- for models and checkpoints, effectively never.
//
// Afterwards: exactly one row per logical file comes back (no chunk rows),
// paths outside A's allowlist stay out, and B reads the bytes again from A's
// keys.
func TestGlobalIndexRecoversAfterControlPlaneRestart(t *testing.T) {
	store := startS3Server(t)
	cp := startControlPlane(t)
	a := newCluster(t, cp, store, clusterConfig{
		id: "cluster-a", prefix: "a", publish: []string{"/models/"}, chunkSize: chunkedStride,
	})
	b := newCluster(t, cp, store, clusterConfig{
		id: "cluster-b", prefix: "b", subscribe: []string{"/"}, globalRead: true, chunkSize: chunkedStride,
	})
	a.waitReady(t)
	waitFor(t, "cluster-b connected", func() bool { return b.agent.Status().Mirror.Ready })

	const chunked = "/models/ckpt/shard-0.safetensors"
	const small = "/models/config.json"
	const private = "/scratch/private.bin"
	bigData := randomBytes(t, 2*chunkedStride+17) // 3 chunks
	smallData := []byte(`{"arch":"llama"}`)

	a.writeThroughCache(t, chunked, bigData)
	a.waitDurable(t, chunked)
	a.writeAndPublish(t, small, smallData)
	// In A's location table as a cloud row, but outside A's allowlist.
	a.writeAndPublish(t, private, []byte("never crosses"))

	waitFor(t, "b to learn both published files", func() bool { return b.sees(chunked) && b.sees(small) })
	waitFor(t, "a's first full republish", func() bool {
		return !a.agent.Status().LastFullRepublishAt.IsZero()
	})
	firstFull := a.agent.Status().LastFullRepublishAt

	restartAt := time.Now()
	cp.restart()

	waitRecover(t, "a to re-register and finish a new full republish", func() bool {
		st := a.agent.Status()
		return st.Connected && st.LastFullRepublishAt.After(firstFull)
	})
	// Connected and synced since the restart: until B's new session applies
	// the post-restart snapshot, its mirror still serves the last-good rows,
	// and "sees" would pass without republish doing anything.
	waitRecover(t, "b to reconnect and re-learn both files", func() bool {
		st := b.agent.Status()
		return st.Connected && st.Mirror.LastSyncAt.After(restartAt) && b.sees(chunked) && b.sees(small)
	})

	// Index contents after recovery: only A's allowlisted logical files.
	rows, err := cp.service().Store().ListLocations(context.Background(), []string{"/"})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, r := range rows {
		if r.GetClusterId() != "cluster-a" {
			continue
		}
		if got[r.GetFilePath()] {
			t.Fatalf("duplicate row for %s", r.GetFilePath())
		}
		got[r.GetFilePath()] = true
		if r.GetFilePath() == chunked {
			if !r.GetIsChunked() || r.GetFileSize() != int64(len(bigData)) || r.GetChunkSize() != chunkedStride {
				t.Fatalf("republished chunked row lost its shape: %v", r)
			}
		}
	}
	if len(got) != 2 || !got[chunked] || !got[small] {
		t.Fatalf("recovered index for cluster-a = %v, want exactly {%s, %s}", got, chunked, small)
	}
	if b.sees(private) {
		t.Fatal("a path outside the publisher's allowlist appeared after republish")
	}

	// The point of discovery: B reads the bytes without A rewriting
	// anything. B never read these before the restart, so it holds no local
	// copy; the only way to the bytes is the recovered global row.
	for p, want := range map[string][]byte{chunked: bigData, small: smallData} {
		data, ok := b.read(p)
		if !ok || !bytes.Equal(data, want) {
			t.Fatalf("cluster-b could not read %s after the control-plane restart (ok=%v, %d bytes)", p, ok, len(data))
		}
	}
}

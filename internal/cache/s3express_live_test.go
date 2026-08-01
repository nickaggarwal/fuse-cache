package cache

// Live S3 Express One Zone probe, gated behind an env var so normal test
// runs never touch the network: S3X_LIVE_BUCKET=<bucket> S3X_LIVE_ENDPOINT=<url> go test -run TestS3ExpressLive

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestS3ExpressLive(t *testing.T) {
	bucket := os.Getenv("S3X_LIVE_BUCKET")
	endpoint := os.Getenv("S3X_LIVE_ENDPOINT")
	if bucket == "" {
		t.Skip("S3X_LIVE_BUCKET not set")
	}
	cs, err := NewCloudStorageWithTuning(bucket, "us-east-1", 30*time.Second,
		S3TransferTuning{
			DownloadConcurrency: 8, DownloadPartSize: 8 << 20,
			UploadConcurrency: 4, UploadPartSize: 8 << 20,
			Endpoint: endpoint,
		})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	ctx := context.Background()
	payload := make([]byte, 16<<20)
	for i := range payload {
		payload[i] = byte(i % 250)
	}
	key := fmt.Sprintf("probe/express-%d_chunk_0", time.Now().UnixNano())

	t0 := time.Now()
	if err := cs.Write(ctx, key, payload); err != nil {
		t.Fatalf("write: %v", err)
	}
	wDur := time.Since(t0)

	t0 = time.Now()
	got, err := cs.Read(ctx, key)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	rDur := time.Since(t0)
	if len(got) != len(payload) {
		t.Fatalf("size mismatch: %d", len(got))
	}
	for i := range got {
		if got[i] != payload[i] {
			t.Fatalf("byte mismatch at %d", i)
		}
	}
	small := []byte("tiny")
	skey := key + ".meta"
	t0 = time.Now()
	if err := cs.Write(ctx, skey, small); err != nil {
		t.Fatalf("small write: %v", err)
	}
	sw := time.Since(t0)
	t0 = time.Now()
	if _, err := cs.Read(ctx, skey); err != nil {
		t.Fatalf("small read: %v", err)
	}
	sr := time.Since(t0)
	t.Logf("express ok: 16MB write=%v read=%v | 4B write=%v read=%v", wDur, rDur, sw, sr)
	cs.Delete(ctx, key)
	cs.Delete(ctx, skey)
}

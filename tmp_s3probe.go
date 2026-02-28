package main

import (
    "context"
    "fmt"
    "time"

    "fuse-client/internal/cache"
)

func probe(name, endpoint string, forcePath bool) {
    cs, err := cache.NewCloudStorageWithTuning(
        "model-store--use1-az6--x-s3",
        "us-east-1",
        20*time.Second,
        cache.S3TransferTuning{
            DownloadConcurrency: 4,
            DownloadPartSize:    8 * 1024 * 1024,
            UploadConcurrency:   2,
            UploadPartSize:      8 * 1024 * 1024,
            Endpoint:            endpoint,
            ForcePathStyle:      forcePath,
        },
    )
    if err != nil {
        fmt.Printf("%s: create failed: %v\n", name, err)
        return
    }
    key := fmt.Sprintf("codex/probe-%d.txt", time.Now().UnixNano())
    data := []byte("probe-data")
    ctx := context.Background()
    started := time.Now()
    if err := cs.Write(ctx, key, data); err != nil {
        fmt.Printf("%s: write failed after %v: %v\n", name, time.Since(started), err)
        return
    }
    fmt.Printf("%s: write ok in %v\n", name, time.Since(started))
    started = time.Now()
    got, err := cs.Read(ctx, key)
    if err != nil {
        fmt.Printf("%s: read failed after %v: %v\n", name, time.Since(started), err)
        return
    }
    fmt.Printf("%s: read ok in %v bytes=%d\n", name, time.Since(started), len(got))
}

func main() {
    probe("empty-endpoint", "", false)
    probe("root-endpoint", "https://s3express-use1-az6.us-east-1.amazonaws.com", false)
    probe("root-endpoint-pathstyle", "https://s3express-use1-az6.us-east-1.amazonaws.com", true)
    probe("bucket-endpoint", "https://model-store--use1-az6--x-s3.s3express-use1-az6.us-east-1.amazonaws.com", false)
}

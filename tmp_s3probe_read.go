package main

import (
    "context"
    "fmt"
    "time"

    "fuse-client/internal/cache"
)

func main() {
    cs, err := cache.NewCloudStorageWithTuning(
        "model-store--use1-az6--x-s3",
        "us-east-1",
        20*time.Second,
        cache.S3TransferTuning{
            DownloadConcurrency: 4,
            DownloadPartSize:    8 * 1024 * 1024,
            UploadConcurrency:   2,
            UploadPartSize:      8 * 1024 * 1024,
            Endpoint:            "https://s3express-use1-az6.us-east-1.amazonaws.com",
            ForcePathStyle:      false,
        },
    )
    if err != nil {
        panic(err)
    }
    ctx := context.Background()
    fmt.Println("exists", cs.Exists(ctx, "codex/s3express-check.txt"))
    started := time.Now()
    b, err := cs.Read(ctx, "codex/s3express-check.txt")
    fmt.Println("readDur", time.Since(started), "err", err, "len", len(b), "data", string(b))
}

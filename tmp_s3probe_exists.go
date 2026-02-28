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
        60*time.Second,
        cache.S3TransferTuning{Endpoint: "https://s3express-use1-az6.us-east-1.amazonaws.com"},
    )
    if err != nil { panic(err) }
    ctx := context.Background()
    for i := 0; i < 3; i++ {
        key := fmt.Sprintf("codex/does-not-exist-%d", time.Now().UnixNano())
        s := time.Now()
        ok := cs.Exists(ctx, key)
        fmt.Println("exists", ok, "dur", time.Since(s), "key", key)
    }
}

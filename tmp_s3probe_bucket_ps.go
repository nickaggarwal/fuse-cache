package main

import (
    "context"
    "fmt"
    "time"

    "fuse-client/internal/cache"
)

func main() {
    ep := "https://model-store--use1-az6--x-s3.s3express-use1-az6.us-east-1.amazonaws.com"
    cs, err := cache.NewCloudStorageWithTuning(
        "model-store--use1-az6--x-s3",
        "us-east-1",
        20*time.Second,
        cache.S3TransferTuning{Endpoint: ep, ForcePathStyle: true},
    )
    if err != nil { panic(err) }
    key := fmt.Sprintf("codex/probe-bucketps-%d.txt", time.Now().UnixNano())
    ctx := context.Background()
    s := time.Now()
    err = cs.Write(ctx, key, []byte("probe-data"))
    fmt.Println("write", time.Since(s), err)
    s = time.Now()
    ok := cs.Exists(ctx, key)
    fmt.Println("exists", ok, time.Since(s))
    s = time.Now()
    b, err := cs.Read(ctx, key)
    fmt.Println("read", time.Since(s), err, len(b))
}

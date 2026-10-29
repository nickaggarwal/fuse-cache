package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"net"
	"net/http"
	"os"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// cmdAzureVector records exactly what the azblob SDK (the one AzureStorage
// uses) sends for a ranged chunk GET: URL path, x-ms-date, x-ms-version,
// Range and Authorization. The Rust signer is tested against this capture,
// so the vector comes from the real Go client rather than from a
// re-derivation of the signing spec.
func cmdAzureVector(args []string) error {
	fs := flag.NewFlagSet("azure-vector", flag.ExitOnError)
	out := fs.String("out", "", "write JSON here (default stdout)")
	account := fs.String("account", "devstoreaccount1", "account")
	key := fs.String("key", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "account key (Azurite well-known dev key)")
	container := fs.String("container", "fuse-cache", "container")
	blob := fs.String("blob", "/bench/m.bin_chunk_3", "blob name")
	fs.Parse(args)

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	captured := make(chan map[string]string, 1)
	go http.Serve(ln, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case captured <- map[string]string{
			"escaped_path":  r.URL.EscapedPath(),
			"x_ms_date":     r.Header.Get("x-ms-date"),
			"x_ms_version":  r.Header.Get("x-ms-version"),
			"x_ms_range":    r.Header.Get("x-ms-range"),
			"range":         r.Header.Get("Range"),
			"authorization": r.Header.Get("Authorization"),
		}:
		default:
		}
		http.Error(w, "capture only", http.StatusTeapot)
	}))
	cred, err := azblob.NewSharedKeyCredential(*account, *key)
	if err != nil {
		return err
	}
	endpoint := fmt.Sprintf("http://%s/%s/", ln.Addr(), *account)
	client, err := azblob.NewClientWithSharedKeyCredential(endpoint, cred, nil)
	if err != nil {
		return err
	}
	_, _ = client.DownloadStream(context.Background(), *container, *blob, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: 0, Count: 4 << 20},
	})
	v := <-captured
	v["account"], v["key"], v["container"], v["blob"], v["endpoint_path"] = *account, *key, *container, *blob, "/"+*account
	b, _ := json.MarshalIndent(v, "", "  ")
	if *out == "" {
		fmt.Println(string(b))
		return nil
	}
	return os.WriteFile(*out, append(b, '\n'), 0o644)
}

// cmdAzurePut uploads a pattern file as chunk objects (<path>_chunk_<n>,
// raw path as blob name) to any blob endpoint, e.g. Azurite, with the same
// SDK call the client persists with (UploadBuffer).
func cmdAzurePut(args []string) error {
	fs := flag.NewFlagSet("azure-put", flag.ExitOnError)
	endpoint := fs.String("endpoint", "http://127.0.0.1:10000/devstoreaccount1/", "blob service URL")
	account := fs.String("account", "devstoreaccount1", "account")
	key := fs.String("key", "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==", "account key")
	container := fs.String("container", "fuse-cache", "container")
	path := fs.String("path", "/bench/az.bin", "logical path")
	size := fs.Int64("size", 256<<20, "bytes")
	strideMB := fs.Int64("stride-mb", 16, "chunk size")
	fs.Parse(args)
	cred, err := azblob.NewSharedKeyCredential(*account, *key)
	if err != nil {
		return err
	}
	client, err := azblob.NewClientWithSharedKeyCredential(*endpoint, cred, nil)
	if err != nil {
		return err
	}
	ctx := context.Background()
	_, _ = client.CreateContainer(ctx, *container, nil)
	stride := *strideMB << 20
	for i, off := 0, int64(0); off < *size; i, off = i+1, off+stride {
		n := stride
		if *size-off < n {
			n = *size - off
		}
		buf := make([]byte, n)
		fillPattern(buf, off)
		if _, err := client.UploadBuffer(ctx, *container, fmt.Sprintf("%s_chunk_%d", *path, i), buf, nil); err != nil {
			return fmt.Errorf("chunk %d: %w", i, err)
		}
	}
	fmt.Printf("uploaded %s (%d bytes) as %d-MiB chunks to %s/%s\n", *path, *size, *strideMB, *endpoint, *container)
	return nil
}

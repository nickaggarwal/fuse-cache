package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/blob"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/sas"
)

const (
	// azureCopySASTTL is how long the signed source URL handed to the copy
	// service stays valid. The copy reads the source once, at the start.
	azureCopySASTTL = 1 * time.Hour
	// azureCopyPollInterval / azureCopyPollTimeout bound the wait for an
	// async blob copy. Intra-account copies of committed blobs complete
	// server-side almost immediately; the timeout is a backstop, not a budget.
	azureCopyPollInterval = 250 * time.Millisecond
	azureCopyPollTimeout  = 5 * time.Minute
)

// AzureStorage implements TierStorage for Azure Blob Storage
type AzureStorage struct {
	client                   *azblob.Client
	containerName            string
	timeout                  time.Duration
	downloadConcurrency      uint16
	downloadBlockSize        int64
	parallelDownloadMinBytes int64
	// keys scopes blob names to a cluster so several clusters can share one
	// container. Zero value is unscoped, i.e. the historical layout.
	keys KeyMapper
}

// objectKey is the blob name to WRITE path under.
//
// Like the GCS backend and unlike S3, this does not trim the leading slash:
// Azure blob names have always been the raw path here, and changing that now
// would orphan every blob already written.
func (as *AzureStorage) objectKey(path string) string { return as.keys.Key(path) }

// readKeyCandidates lists the blob names to try for a read, scoped first.
// With no prefix it is the single raw name, exactly as before scoping existed.
func (as *AzureStorage) readKeyCandidates(path string) []string {
	if as.keys.Prefix() == "" {
		return []string{path}
	}
	return []string{as.objectKey(path), path}
}

// NewAzureStorage creates a new Azure Blob Storage instance
func NewAzureStorage(
	accountName, accountKey, containerName string,
	timeout time.Duration,
	downloadConcurrency int,
	downloadBlockSize int64,
	parallelDownloadMinBytes int64,
) (*AzureStorage, error) {
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("Azure storage account name and key are required")
	}
	if containerName == "" {
		containerName = "fuse-cache"
	}
	if downloadConcurrency <= 0 {
		downloadConcurrency = 8
	}
	if downloadConcurrency > int(^uint16(0)) {
		downloadConcurrency = int(^uint16(0))
	}
	if downloadBlockSize <= 0 {
		downloadBlockSize = 4 * 1024 * 1024
	}
	if parallelDownloadMinBytes < 0 {
		parallelDownloadMinBytes = 0
	}

	cred, err := azblob.NewSharedKeyCredential(accountName, accountKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure credential: %v", err)
	}

	serviceURL := fmt.Sprintf("https://%s.blob.core.windows.net/", accountName)
	client, err := azblob.NewClientWithSharedKeyCredential(serviceURL, cred, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create Azure client: %v", err)
	}

	return &AzureStorage{
		client:                   client,
		containerName:            containerName,
		timeout:                  timeout,
		downloadConcurrency:      uint16(downloadConcurrency),
		downloadBlockSize:        downloadBlockSize,
		parallelDownloadMinBytes: parallelDownloadMinBytes,
	}, nil
}

func (as *AzureStorage) Read(ctx context.Context, path string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	// The helpers below take a resolved blob name, not a mount path: the
	// candidate loop is the only place that knows which spelling is in play,
	// and threading a path through three layers that would each have to
	// re-resolve it is how the two drift apart.
	var firstErr error
	for _, name := range as.readKeyCandidates(path) {
		data, err := as.readName(timeoutCtx, name)
		if err == nil {
			return data, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		// A timeout says nothing about whether the next spelling exists, and
		// retrying under an already-expired context just returns the same
		// error more slowly.
		if timeoutCtx.Err() != nil {
			return nil, firstErr
		}
		if !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return nil, err
		}
	}
	return nil, firstErr
}

// readName reads one fully-resolved blob name.
func (as *AzureStorage) readName(timeoutCtx context.Context, name string) ([]byte, error) {
	// Chunk-object reads dominate cold range-read traffic. Avoiding a separate
	// GetProperties HEAD request removes one round-trip per chunk, and the
	// first-range read discovers the size from Content-Range so the rest of
	// the blob downloads in parallel (a single sequential GET was the ~10MB/s
	// tail on every cloud-fallback chunk).
	if isChunkObjectPath(name) {
		data, err := as.readFirstRangeThenParallel(timeoutCtx, name)
		if err == nil {
			return data, nil
		}
		if timeoutCtx.Err() != nil {
			return nil, err
		}
		// Any non-timeout failure falls back to the plain stream read.
		return as.readStream(timeoutCtx, name)
	}

	props, err := as.getPropertiesName(timeoutCtx, name)
	if err != nil {
		return nil, err
	}
	if props.ContentLength == nil || *props.ContentLength <= 0 {
		return []byte{}, nil
	}

	size := *props.ContentLength
	if as.downloadConcurrency > 1 && size >= as.parallelDownloadMinBytes {
		if size <= int64(int(^uint(0)>>1)) {
			buf := make([]byte, int(size))
			_, err = as.client.DownloadBuffer(timeoutCtx, as.containerName, name, buf, &azblob.DownloadBufferOptions{
				Range:       azblob.HTTPRange{Offset: 0, Count: size},
				BlockSize:   as.downloadBlockSize,
				Concurrency: as.downloadConcurrency,
			})
			if err == nil {
				return buf, nil
			}
			// Best-effort fallback to stream path for transient parallel-read failures.
			if errors.Is(timeoutCtx.Err(), context.DeadlineExceeded) || errors.Is(timeoutCtx.Err(), context.Canceled) {
				return nil, err
			}
		}
	}

	return as.readStream(timeoutCtx, name)
}

// readFirstRangeThenParallel reads one blob without a prior HEAD: it GETs the
// first block with an explicit Range, learns the total size from the
// response, and pulls the remainder with the SDK's parallel ranged
// downloader. Small blobs (<= one block) complete in exactly one round-trip;
// larger ones get concurrency without the GetProperties latency tax.
// It takes a resolved blob name, not a mount path.
func (as *AzureStorage) readFirstRangeThenParallel(ctx context.Context, name string) ([]byte, error) {
	// Constructor guarantees downloadBlockSize > 0.
	blockSize := as.downloadBlockSize

	resp, err := as.client.DownloadStream(ctx, as.containerName, name, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: 0, Count: blockSize},
	})
	if err != nil {
		return nil, err
	}

	// ContentLength is the range length; the blob's total size arrives in
	// ContentRange ("bytes 0-N/total").
	total := int64(-1)
	if resp.ContentRange != nil {
		if idx := strings.LastIndex(*resp.ContentRange, "/"); idx >= 0 {
			if v, perr := strconv.ParseInt((*resp.ContentRange)[idx+1:], 10, 64); perr == nil {
				total = v
			}
		}
	}
	if total < 0 {
		// No Content-Range (zero-byte blob or server quirk): fall back to the
		// plain stream read rather than re-implementing it here.
		resp.Body.Close()
		return as.readStream(ctx, name)
	}

	buf := make([]byte, total)
	first := total
	if first > blockSize {
		first = blockSize
	}
	_, err = io.ReadFull(resp.Body, buf[:first])
	resp.Body.Close()
	if err != nil {
		return nil, err
	}
	if total <= blockSize {
		return buf, nil
	}

	// Remainder in parallel ranged blocks.
	_, err = as.client.DownloadBuffer(ctx, as.containerName, name, buf[first:], &azblob.DownloadBufferOptions{
		Range:       azblob.HTTPRange{Offset: first, Count: total - first},
		BlockSize:   blockSize,
		Concurrency: as.downloadConcurrency,
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// readStream takes a resolved blob name, not a mount path.
func (as *AzureStorage) readStream(ctx context.Context, name string) ([]byte, error) {
	resp, err := as.client.DownloadStream(ctx, as.containerName, name, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var buf bytes.Buffer
	if resp.ContentLength != nil && *resp.ContentLength > 0 && *resp.ContentLength <= int64(int(^uint(0)>>1)) {
		buf.Grow(int(*resp.ContentLength))
	}
	if _, err := io.Copy(&buf, resp.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (as *AzureStorage) Write(ctx context.Context, path string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	_, err := as.client.UploadBuffer(timeoutCtx, as.containerName, as.objectKey(path), data, &azblob.UploadBufferOptions{
		BlockSize:   as.downloadBlockSize,
		Concurrency: as.downloadConcurrency,
	})
	return err
}

// CopyObject duplicates a blob inside the container without the bytes passing
// through this process. Rename uses it so moving a 50GB chunked object costs a
// handful of control-plane calls instead of a download plus an upload.
//
// The copy is started asynchronously and then polled. Azure's synchronous
// CopyFromURL caps at 256MB, which is smaller than the objects this matters
// most for, so the async form is the only one worth having.
func (as *AzureStorage) CopyObject(ctx context.Context, srcPath, dstPath string) error {
	if srcPath == dstPath {
		return nil
	}
	container := as.client.ServiceClient().NewContainerClient(as.containerName)
	// The source may still sit at its unscoped name (written before a prefix
	// was configured); the destination always gets the scoped one, so a
	// rename quietly migrates the blob forward.
	srcName := as.existingName(ctx, srcPath)
	src := container.NewBlobClient(srcName)
	dst := container.NewBlobClient(as.objectKey(dstPath))

	// The service fetches the source itself, so it needs a URL it can read.
	// GetSASURL signs with the shared key this client was built from.
	sourceURL, err := src.GetSASURL(sas.BlobPermissions{Read: true}, time.Now().Add(azureCopySASTTL), nil)
	if err != nil {
		return fmt.Errorf("sign copy source %s: %w", srcName, err)
	}

	startCtx, cancel := context.WithTimeout(ctx, as.timeout)
	resp, err := dst.StartCopyFromURL(startCtx, sourceURL, nil)
	cancel()
	if err != nil {
		return err
	}
	if resp.CopyStatus != nil && *resp.CopyStatus == blob.CopyStatusTypeSuccess {
		return nil
	}

	// Intra-account copies of already-committed blobs are normally immediate,
	// but the service is allowed to take its time, and a rename that returned
	// before the destination existed would let the source deletion race it.
	deadline := time.Now().Add(azureCopyPollTimeout)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(azureCopyPollInterval):
		}
		propsCtx, propsCancel := context.WithTimeout(ctx, as.timeout)
		props, err := dst.GetProperties(propsCtx, nil)
		propsCancel()
		if err != nil {
			return err
		}
		if props.CopyStatus == nil {
			// No copy in progress and the blob is readable: it landed.
			return nil
		}
		switch *props.CopyStatus {
		case blob.CopyStatusTypeSuccess:
			return nil
		case blob.CopyStatusTypeFailed, blob.CopyStatusTypeAborted:
			desc := ""
			if props.CopyStatusDescription != nil {
				desc = *props.CopyStatusDescription
			}
			return fmt.Errorf("copy %s -> %s ended as %s: %s", srcPath, dstPath, *props.CopyStatus, desc)
		}
		if time.Now().After(deadline) {
			if resp.CopyID != nil {
				abortCtx, abortCancel := context.WithTimeout(context.WithoutCancel(ctx), as.timeout)
				_, _ = dst.AbortCopyFromURL(abortCtx, *resp.CopyID, nil)
				abortCancel()
			}
			return fmt.Errorf("copy %s -> %s still pending after %v", srcPath, dstPath, azureCopyPollTimeout)
		}
	}
}

func (as *AzureStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	var firstErr error
	for _, name := range as.readKeyCandidates(path) {
		_, err := as.client.DeleteBlob(timeoutCtx, as.containerName, name, nil)
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if timeoutCtx.Err() != nil || !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return err
		}
	}
	return firstErr
}

func (as *AzureStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	_, err := as.getProperties(timeoutCtx, path)
	return err == nil
}

func (as *AzureStorage) Size(ctx context.Context, path string) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	props, err := as.getProperties(timeoutCtx, path)
	if err != nil {
		return 0, err
	}
	if props.ContentLength == nil {
		return 0, nil
	}
	return *props.ContentLength, nil
}

// getProperties heads the first candidate spelling that exists.
func (as *AzureStorage) getProperties(ctx context.Context, path string) (blob.GetPropertiesResponse, error) {
	var firstErr error
	for _, name := range as.readKeyCandidates(path) {
		props, err := as.getPropertiesName(ctx, name)
		if err == nil {
			return props, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if ctx.Err() != nil || !bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
			return props, err
		}
	}
	return blob.GetPropertiesResponse{}, firstErr
}

func (as *AzureStorage) getPropertiesName(ctx context.Context, name string) (blob.GetPropertiesResponse, error) {
	return as.client.ServiceClient().NewContainerClient(as.containerName).NewBlobClient(name).GetProperties(ctx, nil)
}

// existingName picks the candidate spelling a blob is actually stored under,
// falling back to the scoped name when neither answers.
func (as *AzureStorage) existingName(ctx context.Context, path string) string {
	cands := as.readKeyCandidates(path)
	if len(cands) == 1 {
		return cands[0]
	}
	for _, name := range cands {
		headCtx, cancel := context.WithTimeout(ctx, as.timeout)
		_, err := as.getPropertiesName(headCtx, name)
		cancel()
		if err == nil {
			return name
		}
	}
	return cands[0]
}

func isChunkObjectPath(path string) bool {
	idx := strings.LastIndex(path, "_chunk_")
	if idx < 0 {
		return false
	}
	suffix := path[idx+len("_chunk_"):]
	if suffix == "" {
		return false
	}
	for i := 0; i < len(suffix); i++ {
		if suffix[i] < '0' || suffix[i] > '9' {
			return false
		}
	}
	return true
}

// ListUnder returns blob names under prefix. It backs ModelKV's cross-node
// recovery, which must enumerate the catalog on a node that holds no local
// state and therefore has nothing else to enumerate from.
//
// Paging is followed to completion rather than capped: a truncated listing
// would silently under-recover, which presents as "those blocks were never
// durable" — the exact wrong answer for a durability store.
func (as *AzureStorage) ListUnder(ctx context.Context, prefix string) ([]string, error) {
	ctx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	// Callers feed these names straight back into Read, which re-applies the
	// cluster scope, so the listing must enumerate scoped but report
	// unscoped. Returning the stored names would double-prefix every
	// subsequent read.
	trimmed := strings.TrimPrefix(prefix, "/")
	scope := as.keys.Prefix()
	p := trimmed
	if scope != "" {
		p = scope + "/" + trimmed
	}
	pager := as.client.NewListBlobsFlatPager(as.containerName, &azblob.ListBlobsFlatOptions{
		Prefix: &p,
	})
	var out []string
	for pager.More() {
		page, err := pager.NextPage(ctx)
		if err != nil {
			return nil, fmt.Errorf("azure: list blobs under %s: %w", p, err)
		}
		for _, item := range page.Segment.BlobItems {
			if item == nil || item.Name == nil {
				continue
			}
			name := *item.Name
			if scope != "" {
				name = strings.TrimPrefix(name, scope+"/")
			}
			out = append(out, name)
		}
	}
	return out, nil
}

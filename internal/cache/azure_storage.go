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
)

// AzureStorage implements TierStorage for Azure Blob Storage
type AzureStorage struct {
	client                   *azblob.Client
	containerName            string
	timeout                  time.Duration
	downloadConcurrency      uint16
	downloadBlockSize        int64
	parallelDownloadMinBytes int64
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

	// Chunk-object reads dominate cold range-read traffic. Avoiding a separate
	// GetProperties HEAD request removes one round-trip per chunk, and the
	// first-range read discovers the size from Content-Range so the rest of
	// the blob downloads in parallel (a single sequential GET was the ~10MB/s
	// tail on every cloud-fallback chunk).
	if isChunkObjectPath(path) {
		data, err := as.readFirstRangeThenParallel(timeoutCtx, path)
		if err == nil {
			return data, nil
		}
		if timeoutCtx.Err() != nil {
			return nil, err
		}
		// Any non-timeout failure falls back to the plain stream read.
		return as.readStream(timeoutCtx, path)
	}

	props, err := as.getProperties(timeoutCtx, path)
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
			_, err = as.client.DownloadBuffer(timeoutCtx, as.containerName, path, buf, &azblob.DownloadBufferOptions{
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

	return as.readStream(timeoutCtx, path)
}

// readFirstRangeThenParallel reads one blob without a prior HEAD: it GETs the
// first block with an explicit Range, learns the total size from the
// response, and pulls the remainder with the SDK's parallel ranged
// downloader. Small blobs (<= one block) complete in exactly one round-trip;
// larger ones get concurrency without the GetProperties latency tax.
func (as *AzureStorage) readFirstRangeThenParallel(ctx context.Context, path string) ([]byte, error) {
	// Constructor guarantees downloadBlockSize > 0.
	blockSize := as.downloadBlockSize

	resp, err := as.client.DownloadStream(ctx, as.containerName, path, &azblob.DownloadStreamOptions{
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
		return as.readStream(ctx, path)
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
	_, err = as.client.DownloadBuffer(ctx, as.containerName, path, buf[first:], &azblob.DownloadBufferOptions{
		Range:       azblob.HTTPRange{Offset: first, Count: total - first},
		BlockSize:   blockSize,
		Concurrency: as.downloadConcurrency,
	})
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (as *AzureStorage) readStream(ctx context.Context, path string) ([]byte, error) {
	resp, err := as.client.DownloadStream(ctx, as.containerName, path, nil)
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

	_, err := as.client.UploadBuffer(timeoutCtx, as.containerName, path, data, &azblob.UploadBufferOptions{
		BlockSize:   as.downloadBlockSize,
		Concurrency: as.downloadConcurrency,
	})
	return err
}

func (as *AzureStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	_, err := as.client.DeleteBlob(timeoutCtx, as.containerName, path, nil)
	return err
}

func (as *AzureStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	_, err := as.getProperties(timeoutCtx, path)
	if err == nil {
		return true
	}
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return false
	}
	return false
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

func (as *AzureStorage) getProperties(ctx context.Context, path string) (blob.GetPropertiesResponse, error) {
	return as.client.ServiceClient().NewContainerClient(as.containerName).NewBlobClient(path).GetProperties(ctx, nil)
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

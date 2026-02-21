package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
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

	_, err := as.client.UploadBuffer(timeoutCtx, as.containerName, path, data, &azblob.UploadBufferOptions{})
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

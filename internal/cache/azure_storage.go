package cache

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob"
)

// AzureStorage implements TierStorage for Azure Blob Storage
type AzureStorage struct {
	client        *azblob.Client
	containerName string
	timeout       time.Duration
}

// NewAzureStorage creates a new Azure Blob Storage instance
func NewAzureStorage(accountName, accountKey, containerName string, timeout time.Duration) (*AzureStorage, error) {
	if accountName == "" || accountKey == "" {
		return nil, fmt.Errorf("Azure storage account name and key are required")
	}
	if containerName == "" {
		containerName = "fuse-cache"
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
		client:        client,
		containerName: containerName,
		timeout:       timeout,
	}, nil
}

func (as *AzureStorage) Read(ctx context.Context, path string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	resp, err := as.client.DownloadStream(timeoutCtx, as.containerName, path, nil)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	return io.ReadAll(resp.Body)
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

	// Try to get blob properties — if it fails, blob doesn't exist
	_, err := as.client.DownloadStream(timeoutCtx, as.containerName, path, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: 0, Count: 1},
	})
	return err == nil
}

func (as *AzureStorage) Size(ctx context.Context, path string) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, as.timeout)
	defer cancel()

	resp, err := as.client.DownloadStream(timeoutCtx, as.containerName, path, &azblob.DownloadStreamOptions{
		Range: azblob.HTTPRange{Offset: 0, Count: 1},
	})
	if err != nil {
		return 0, err
	}
	defer resp.Body.Close()

	// ContentRange header contains total size
	if resp.ContentLength != nil {
		return *resp.ContentLength, nil
	}

	// Fallback: download the full blob to get size
	fullResp, err := as.client.DownloadStream(timeoutCtx, as.containerName, path, nil)
	if err != nil {
		return 0, err
	}
	defer fullResp.Body.Close()
	data, err := io.ReadAll(fullResp.Body)
	if err != nil {
		return 0, err
	}
	_ = bytes.NewReader(data) // ensure bytes import is used
	return int64(len(data)), nil
}

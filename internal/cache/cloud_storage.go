package cache

import (
	"bytes"
	"context"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// CloudStorage implements TierStorage for cloud storage (AWS S3)
type CloudStorage struct {
	bucket     string
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	timeout    time.Duration
}

// NewCloudStorage creates a new cloud storage instance
func NewCloudStorage(timeout time.Duration) (*CloudStorage, error) {
	// Create AWS session
	sess, err := session.NewSession(&aws.Config{
		Region: aws.String("us-east-1"), // Default region
	})
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	return &CloudStorage{
		bucket:     "fuse-client-cache", // Default bucket name
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		timeout:    timeout,
	}, nil
}

// Read reads a file from cloud storage
func (cs *CloudStorage) Read(ctx context.Context, path string) ([]byte, error) {
	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	// Download the file
	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := cs.downloader.DownloadWithContext(timeoutCtx, buf, &s3.GetObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// Write writes a file to cloud storage
func (cs *CloudStorage) Write(ctx context.Context, path string, data []byte) error {
	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	// Upload the file
	_, err := cs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	})
	return err
}

// Delete removes a file from cloud storage
func (cs *CloudStorage) Delete(ctx context.Context, path string) error {
	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	return err
}

// Exists checks if a file exists in cloud storage
func (cs *CloudStorage) Exists(ctx context.Context, path string) bool {
	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	return err == nil
}

// Size returns the size of a file in cloud storage
func (cs *CloudStorage) Size(ctx context.Context, path string) (int64, error) {
	// Create a timeout context
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	resp, err := cs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

package cache

import (
	"bytes"
	"context"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

// GCPStorage implements TierStorage for Google Cloud Storage using
// the S3 interoperability endpoint (storage.googleapis.com).
type GCPStorage struct {
	bucket     string
	s3Client   *s3.S3
	uploader   *s3manager.Uploader
	downloader *s3manager.Downloader
	timeout    time.Duration
}

// NewGCPStorage creates a GCS-backed storage instance.
// It uses the AWS S3 client against GCS' S3-compatible endpoint.
func NewGCPStorage(bucket string, timeout time.Duration) (*GCPStorage, error) {
	if bucket == "" {
		bucket = "fuse-client-cache"
	}

	cfg := &aws.Config{
		Region:           aws.String("auto"),
		Endpoint:         aws.String("https://storage.googleapis.com"),
		S3ForcePathStyle: aws.Bool(true),
	}

	if keyID := os.Getenv("GCP_ACCESS_KEY_ID"); keyID != "" {
		cfg.Credentials = credentials.NewStaticCredentials(
			keyID,
			os.Getenv("GCP_SECRET_ACCESS_KEY"),
			"",
		)
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	return &GCPStorage{
		bucket:     bucket,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		timeout:    timeout,
	}, nil
}

func (gs *GCPStorage) Read(ctx context.Context, path string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	buf := aws.NewWriteAtBuffer([]byte{})
	_, err := gs.downloader.DownloadWithContext(timeoutCtx, buf, &s3.GetObjectInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (gs *GCPStorage) Write(ctx context.Context, path string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	_, err := gs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (gs *GCPStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	_, err := gs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(path),
	})
	return err
}

func (gs *GCPStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	_, err := gs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(path),
	})
	return err == nil
}

func (gs *GCPStorage) Size(ctx context.Context, path string) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	resp, err := gs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(path),
	})
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

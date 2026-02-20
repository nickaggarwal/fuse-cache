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
func NewCloudStorage(bucket, region string, timeout time.Duration) (*CloudStorage, error) {
	if bucket == "" {
		bucket = "fuse-client-cache"
	}
	if region == "" {
		region = "us-east-1"
	}

	sess, err := session.NewSession(&aws.Config{
		Region: aws.String(region),
	})
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)

	return &CloudStorage{
		bucket:     bucket,
		s3Client:   s3Client,
		uploader:   s3manager.NewUploader(sess),
		downloader: s3manager.NewDownloader(sess),
		timeout:    timeout,
	}, nil
}

func (cs *CloudStorage) Read(ctx context.Context, path string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

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

func (cs *CloudStorage) Write(ctx context.Context, path string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (cs *CloudStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	return err
}

func (cs *CloudStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(path),
	})
	return err == nil
}

func (cs *CloudStorage) Size(ctx context.Context, path string) (int64, error) {
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

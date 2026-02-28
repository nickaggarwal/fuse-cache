package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	defaultS3DownloadConcurrency = 32
	defaultS3DownloadPartSize    = 8 * 1024 * 1024
	defaultS3UploadConcurrency   = 16
	defaultS3UploadPartSize      = 8 * 1024 * 1024
	// Keep chunk/object writes <=32MiB on direct PutObject to avoid multipart
	// overhead and improve compatibility with S3 Express directory buckets.
	s3DirectPutMaxSize = 32 * 1024 * 1024
)

// S3TransferTuning controls S3 transfer concurrency and part sizing.
type S3TransferTuning struct {
	DownloadConcurrency int
	DownloadPartSize    int64
	UploadConcurrency   int
	UploadPartSize      int64
	Endpoint            string
	ForcePathStyle      bool
}

// CloudStorage implements TierStorage for cloud storage (AWS S3)
type CloudStorage struct {
	bucket          string
	s3Client        *s3.S3
	uploader        *s3manager.Uploader
	downloader      *s3manager.Downloader
	timeout         time.Duration
	preferDirectPut bool
}

// NewCloudStorage creates a new cloud storage instance
func NewCloudStorage(bucket, region string, timeout time.Duration) (*CloudStorage, error) {
	return NewCloudStorageWithTuning(bucket, region, timeout, S3TransferTuning{})
}

// NewCloudStorageWithTuning creates a new cloud storage instance with explicit
// transfer-manager tuning for higher throughput on large objects.
func NewCloudStorageWithTuning(bucket, region string, timeout time.Duration, tuning S3TransferTuning) (*CloudStorage, error) {
	if bucket == "" {
		bucket = "fuse-client-cache"
	}
	if region == "" {
		region = "us-east-1"
	}
	if tuning.DownloadConcurrency <= 0 {
		tuning.DownloadConcurrency = defaultS3DownloadConcurrency
	}
	if tuning.DownloadPartSize <= 0 {
		tuning.DownloadPartSize = defaultS3DownloadPartSize
	}
	if tuning.UploadConcurrency <= 0 {
		tuning.UploadConcurrency = defaultS3UploadConcurrency
	}
	if tuning.UploadPartSize <= 0 {
		tuning.UploadPartSize = defaultS3UploadPartSize
	}

	cfg := &aws.Config{
		Region: aws.String(region),
	}
	if strings.TrimSpace(tuning.Endpoint) != "" {
		cfg.Endpoint = aws.String(strings.TrimSpace(tuning.Endpoint))
	}
	if tuning.ForcePathStyle {
		cfg.S3ForcePathStyle = aws.Bool(true)
	}
	sess, err := session.NewSession(cfg)
	if err != nil {
		return nil, err
	}

	s3Client := s3.New(sess)
	uploader := s3manager.NewUploader(sess, func(u *s3manager.Uploader) {
		u.Concurrency = tuning.UploadConcurrency
		u.PartSize = tuning.UploadPartSize
	})
	downloader := s3manager.NewDownloader(sess, func(d *s3manager.Downloader) {
		d.Concurrency = tuning.DownloadConcurrency
		d.PartSize = tuning.DownloadPartSize
	})

	return &CloudStorage{
		bucket:          bucket,
		s3Client:        s3Client,
		uploader:        uploader,
		downloader:      downloader,
		timeout:         timeout,
		preferDirectPut: shouldPreferDirectPut(bucket, tuning.Endpoint),
	}, nil
}

func (cs *CloudStorage) Read(ctx context.Context, path string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	buf := aws.NewWriteAtBuffer([]byte{})
	var firstErr error
	for _, key := range keyCandidates(path) {
		buf = aws.NewWriteAtBuffer([]byte{})
		_, err := cs.downloader.DownloadWithContext(timeoutCtx, buf, &s3.GetObjectInput{
			Bucket: aws.String(cs.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return buf.Bytes(), nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if timeoutCtx.Err() != nil {
			return nil, err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("s3 read failed")
	}
	return nil, firstErr
}

func (cs *CloudStorage) Write(ctx context.Context, path string, data []byte) error {
	key := normalizeS3Key(path)

	// Small chunk-sized writes are faster and more reliable via PutObject than
	// multipart upload manager, especially on S3 Express directory buckets.
	if cs.preferDirectPut || int64(len(data)) <= s3DirectPutMaxSize {
		if err := cs.putObject(ctx, key, data); err == nil {
			return nil
		}
		// Fall through to uploader for non-express endpoints as a fallback path.
	}

	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	if err == nil {
		return nil
	}

	// S3 Express directory buckets reject Content-MD5 on multipart uploads.
	// Fall back to direct PutObject, which works for our chunk-sized writes.
	if supportsPutObjectFallback(err) {
		putErr := cs.putObject(ctx, key, data)
		if putErr == nil {
			return nil
		}
		return fmt.Errorf("multipart upload failed: %w; putobject fallback failed: %v", err, putErr)
	}
	return err
}

func (cs *CloudStorage) putObject(ctx context.Context, key string, data []byte) error {
	putCtx, putCancel := context.WithTimeout(ctx, cs.timeout)
	defer putCancel()
	_, err := cs.s3Client.PutObjectWithContext(putCtx, &s3.PutObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}

func (cs *CloudStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	_, err := cs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(normalizeS3Key(path)),
	})
	return err
}

func (cs *CloudStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	for _, key := range keyCandidates(path) {
		_, err := cs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
			Bucket: aws.String(cs.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return true
		}
		if timeoutCtx.Err() != nil {
			return false
		}
	}
	return false
}

func (cs *CloudStorage) Size(ctx context.Context, path string) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	var firstErr error
	for _, key := range keyCandidates(path) {
		resp, err := cs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
			Bucket: aws.String(cs.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			if resp.ContentLength == nil {
				return 0, nil
			}
			return *resp.ContentLength, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if timeoutCtx.Err() != nil {
			return 0, err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("s3 size lookup failed")
	}
	return 0, firstErr
}

func normalizeS3Key(path string) string {
	return strings.TrimPrefix(path, "/")
}

func keyCandidates(path string) []string {
	norm := normalizeS3Key(path)
	if norm == path {
		return []string{norm}
	}
	return []string{norm, path}
}

func supportsPutObjectFallback(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "content md5") ||
		strings.Contains(msg, "content-md5") ||
		strings.Contains(msg, "does not support content md5 header")
}

func shouldPreferDirectPut(bucket, endpoint string) bool {
	b := strings.ToLower(strings.TrimSpace(bucket))
	e := strings.ToLower(strings.TrimSpace(endpoint))
	return strings.HasSuffix(b, "--x-s3") || strings.Contains(e, "s3express")
}

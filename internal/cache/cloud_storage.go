package cache

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
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
	if shouldPreferDirectPut(bucket, tuning.Endpoint) {
		// S3 Express directory buckets reject the Content-MD5 header the SDK
		// attaches to uploads by default (501 NotImplemented on PutObject).
		cfg.S3DisableContentMD5Validation = aws.Bool(true)
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

	var firstErr error
	for _, key := range keyCandidates(path) {
		if shouldUseDirectS3Read(path) {
			data, err := cs.readDirect(timeoutCtx, key)
			if err == nil {
				return data, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			if timeoutCtx.Err() != nil {
				return nil, err
			}
		}
		// GrowthCoeff 2 = amortized doubling; the default exact-fit growth
		// re-copies the whole buffer on every concurrent part write.
		buf := aws.NewWriteAtBuffer([]byte{})
		buf.GrowthCoeff = 2
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

func (cs *CloudStorage) readDirect(ctx context.Context, key string) ([]byte, error) {
	resp, err := cs.s3Client.GetObjectWithContext(ctx, &s3.GetObjectInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	return readAllSized(resp.Body, aws.Int64Value(resp.ContentLength))
}

// readAllSized reads the body into a buffer presized from Content-Length,
// avoiding io.ReadAll's repeated grow-and-copy on multi-MB objects.
func readAllSized(r io.Reader, contentLength int64) ([]byte, error) {
	if contentLength <= 0 {
		return io.ReadAll(r)
	}
	buf := bytes.NewBuffer(make([]byte, 0, contentLength))
	if _, err := buf.ReadFrom(r); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
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

	// S3 multipart caps at 10,000 parts: at the configured part size, a
	// whole-object upload silently fails past partSize*10000 (~78GiB at the
	// 8MiB default; mountpoint-s3 documents the same ceiling). Raise the
	// part size for this call so big objects still upload.
	uploadOpts := func(u *s3manager.Uploader) {
		u.PartSize = multipartPartSizeFor(int64(len(data)), u.PartSize)
	}
	_, err := cs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(cs.bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	}, uploadOpts)
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

// CopyObject duplicates an object inside the bucket server-side, so a rename
// costs one request per object instead of a download plus an upload.
func (cs *CloudStorage) CopyObject(ctx context.Context, srcPath, dstPath string) error {
	return s3CopyObject(ctx, cs.s3Client, cs.bucket, srcPath, dstPath, cs.timeout)
}

// Delete removes the object under every spelling of path that Read, Exists,
// Size, and CopyObject are willing to find it under. Deleting only the
// normalized key while Exists tries both would report success on an object
// that still answers Exists — for a delete, "gone under one spelling" is not
// gone. S3 delete is idempotent, so the extra call on the common path is a
// no-op rather than an error.
func (cs *CloudStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, cs.timeout)
	defer cancel()

	var firstErr error
	for _, key := range keyCandidates(path) {
		_, err := cs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
			Bucket: aws.String(cs.bucket),
			Key:    aws.String(key),
		})
		if err != nil && firstErr == nil {
			firstErr = err
		}
		if timeoutCtx.Err() != nil {
			break
		}
	}
	return firstErr
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

// s3CopyObject issues a server-side copy, shared by the S3 and GCS-interop
// backends. The source is tried under both key spellings for the same reason
// reads are (keyCandidates): objects written before key normalization still
// carry the leading slash.
func s3CopyObject(ctx context.Context, client *s3.S3, bucket, srcPath, dstPath string, timeout time.Duration) error {
	if srcPath == dstPath {
		return nil
	}
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	var firstErr error
	for _, srcKey := range keyCandidates(srcPath) {
		_, err := client.CopyObjectWithContext(timeoutCtx, &s3.CopyObjectInput{
			Bucket: aws.String(bucket),
			Key:    aws.String(normalizeS3Key(dstPath)),
			// CopySource is "bucket/key" and must be URL-escaped: object keys
			// routinely contain characters (spaces, "+") that would otherwise
			// be reinterpreted by the service.
			CopySource: aws.String(url.PathEscape(bucket + "/" + srcKey)),
		})
		if err == nil {
			return nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if timeoutCtx.Err() != nil {
			return err
		}
	}
	if firstErr == nil {
		firstErr = errors.New("s3 copy failed")
	}
	return firstErr
}

func keyCandidates(path string) []string {
	norm := normalizeS3Key(path)
	if norm == path {
		return []string{norm}
	}
	return []string{norm, path}
}

func shouldUseDirectS3Read(path string) bool {
	return strings.Contains(path, "_chunk_")
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

// multipartPartSizeFor returns the part size for one upload: the configured
// size unless the object would exceed S3's 10,000-part cap, in which case the
// minimum part size that fits (ceil division). S3's own bounds (5MiB..5GiB
// per part) still apply upstream; objects past 48.8TiB are unuploadable.
func multipartPartSizeFor(objectSize, configuredPartSize int64) int64 {
	const maxParts = 10000
	if minPart := (objectSize + maxParts - 1) / maxParts; minPart > configuredPartSize {
		return minPart
	}
	return configuredPartSize
}

func shouldPreferDirectPut(bucket, endpoint string) bool {
	b := strings.ToLower(strings.TrimSpace(bucket))
	e := strings.ToLower(strings.TrimSpace(endpoint))
	return strings.HasSuffix(b, "--x-s3") || strings.Contains(e, "s3express")
}

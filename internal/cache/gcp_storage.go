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
	// keys scopes object keys to a cluster so several clusters can share one
	// bucket. Zero value is unscoped, i.e. the historical key layout.
	keys KeyMapper
}

// objectKey is the key to WRITE path under.
//
// Unlike the S3 backend this does not normalize the leading slash: GCS keys
// have always been the raw path here, and trimming now would orphan every
// object already written.
func (gs *GCPStorage) objectKey(path string) string { return gs.keys.Key(path) }

// readKeyCandidates lists the keys to try for a read, scoped first. With no
// prefix it is the single raw key, exactly as before cluster scoping existed.
func (gs *GCPStorage) readKeyCandidates(path string) []string {
	if gs.keys.Prefix() == "" {
		return []string{path}
	}
	return []string{gs.objectKey(path), path}
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

	var firstErr error
	for _, key := range gs.readKeyCandidates(path) {
		buf := aws.NewWriteAtBuffer([]byte{})
		buf.GrowthCoeff = 2 // amortized doubling instead of exact-fit re-copies
		_, err := gs.downloader.DownloadWithContext(timeoutCtx, buf, &s3.GetObjectInput{
			Bucket: aws.String(gs.bucket),
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
	return nil, firstErr
}

func (gs *GCPStorage) Write(ctx context.Context, path string, data []byte) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	_, err := gs.uploader.UploadWithContext(timeoutCtx, &s3manager.UploadInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(gs.objectKey(path)),
		Body:   bytes.NewReader(data),
	})
	return err
}

// CopyObject duplicates an object inside the bucket server-side. GCS honours
// the S3 CopyObject verb through the interoperability endpoint.
func (gs *GCPStorage) CopyObject(ctx context.Context, srcPath, dstPath string) error {
	return s3CopyObject(ctx, gs.s3Client, gs.bucket, srcPath, dstPath, gs.timeout, gs.keys)
}

func (gs *GCPStorage) Delete(ctx context.Context, path string) error {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	var firstErr error
	for _, key := range gs.readKeyCandidates(path) {
		_, err := gs.s3Client.DeleteObjectWithContext(timeoutCtx, &s3.DeleteObjectInput{
			Bucket: aws.String(gs.bucket),
			Key:    aws.String(key),
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
	return firstErr
}

func (gs *GCPStorage) Exists(ctx context.Context, path string) bool {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	for _, key := range gs.readKeyCandidates(path) {
		if _, err := gs.s3Client.HeadObjectWithContext(timeoutCtx, &s3.HeadObjectInput{
			Bucket: aws.String(gs.bucket),
			Key:    aws.String(key),
		}); err == nil {
			return true
		}
		if timeoutCtx.Err() != nil {
			return false
		}
	}
	return false
}

// headFirstMatch HEADs each read candidate and returns the first that exists,
// so Size sees a scoped object and a legacy one alike.
func (gs *GCPStorage) headFirstMatch(ctx context.Context, path string) (*s3.HeadObjectOutput, error) {
	var firstErr error
	for _, key := range gs.readKeyCandidates(path) {
		resp, err := gs.s3Client.HeadObjectWithContext(ctx, &s3.HeadObjectInput{
			Bucket: aws.String(gs.bucket),
			Key:    aws.String(key),
		})
		if err == nil {
			return resp, nil
		}
		if firstErr == nil {
			firstErr = err
		}
		if ctx.Err() != nil {
			return nil, err
		}
	}
	return nil, firstErr
}

func (gs *GCPStorage) Size(ctx context.Context, path string) (int64, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()

	resp, err := gs.headFirstMatch(timeoutCtx, path)
	if err != nil {
		return 0, err
	}

	return *resp.ContentLength, nil
}

// ReadKey implements rawKeyReader: fetch exactly key, unscoped. See the
// S3 implementation for why the cross-cluster path needs this.
func (gs *GCPStorage) ReadKey(ctx context.Context, key string) ([]byte, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, gs.timeout)
	defer cancel()
	buf := aws.NewWriteAtBuffer([]byte{})
	buf.GrowthCoeff = 2
	if _, err := gs.downloader.DownloadWithContext(timeoutCtx, buf, &s3.GetObjectInput{
		Bucket: aws.String(gs.bucket),
		Key:    aws.String(key),
	}); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// BucketName implements rawKeyReader.
func (gs *GCPStorage) BucketName() string { return gs.bucket }

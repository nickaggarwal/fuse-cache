package cache

import (
	"errors"
	"net/http"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/bloberror"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
)

// isCloudMiss reports whether err means the object simply is not in the cloud
// bucket/container, as opposed to the request having failed. Covers all three
// providers: Azure via bloberror codes, S3 and GCP (S3-interop) via awserr.
func isCloudMiss(err error) bool {
	if err == nil {
		return false
	}
	if bloberror.HasCode(err, bloberror.BlobNotFound, bloberror.ContainerNotFound) {
		return true
	}
	var reqErr awserr.RequestFailure
	if errors.As(err, &reqErr) && reqErr.StatusCode() == http.StatusNotFound {
		return true
	}
	var awsErr awserr.Error
	if errors.As(err, &awsErr) {
		switch awsErr.Code() {
		case s3.ErrCodeNoSuchKey, s3.ErrCodeNoSuchBucket, "NotFound":
			return true
		}
	}
	return false
}

// isTierMiss reports whether a remote-tier read failed because the tier does
// not hold the object, rather than because the transfer failed.
//
// This is the tier-level counterpart to isPeerMiss, and it exists for the same
// reason. The adaptive tier tracker scores a tier as success/latency, and the
// ordered fallback tries the primary tier first for every chunk. On a cold
// read the primary legitimately does not have the object yet — that is the
// definition of a cache miss, not evidence the tier is broken. Folding those
// into the tier EWMA drove fuse_tier_peer_success_ratio to 0.0 on stargz-test,
// which inverted order() (cloud ahead of peer: ~121ms preferred over ~13ms)
// and pinned shouldHedge() to always-hedge, since ewmaSuccess stayed below
// tierPerfReliableSuccess forever. Both effects are silent.
//
// Busy is excluded by the caller separately: it is admission control, and a
// shedding peer provably has the data.
func isTierMiss(err error) bool {
	return isPeerMiss(err) || isCloudMiss(err)
}

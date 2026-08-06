package cache

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// azureNotFound builds the error shape the azblob SDK actually returns for a
// missing blob: a *azcore.ResponseError whose ErrorCode bloberror.HasCode
// matches. Hand-rolling a sentinel would test nothing.
func azureNotFound() error {
	return &azcore.ResponseError{
		ErrorCode:   "BlobNotFound",
		StatusCode:  http.StatusNotFound,
		RawResponse: &http.Response{StatusCode: http.StatusNotFound},
	}
}

func TestIsTierMiss_Classification(t *testing.T) {
	cases := []struct {
		name string
		err  error
		want bool
	}{
		{"nil", nil, false},
		{"azure blob not found", azureNotFound(), true},
		{"azure wrapped", fmt.Errorf("read chunk: %w", azureNotFound()), true},
		{"s3 no such key", awserr.New(s3.ErrCodeNoSuchKey, "key missing", nil), true},
		{"s3 head 404", awserr.NewRequestFailure(
			awserr.New("NotFound", "Not Found", nil), http.StatusNotFound, "req-1"), true},
		{"peer miss typed", &peerMissError{addr: "10.0.0.1:9081", path: "/a.bin"}, true},
		{"peer miss grpc", status.Error(codes.NotFound, "file not found"), true},
		{"peer miss wrapped", fmt.Errorf("tier read: %w",
			&peerMissError{addr: "any", path: "/a.bin"}), true},

		// Real failures must stay failures — this is the half that keeps the
		// EWMA useful. A classifier that returns true too eagerly is worse
		// than the bug it replaces: the tracker would learn nothing at all.
		{"generic", errors.New("connection reset by peer"), false},
		{"s3 500", awserr.NewRequestFailure(
			awserr.New("InternalError", "boom", nil), 500, "req-2"), false},
		{"s3 slowdown", awserr.New("SlowDown", "throttled", nil), false},
		{"grpc unavailable", status.Error(codes.Unavailable, "no route"), false},
		{"grpc deadline", status.Error(codes.DeadlineExceeded, "timed out"), false},
		{"peer busy", status.Error(codes.ResourceExhausted, "serve gate full"), false},
		{"azure 503", &azcore.ResponseError{
			ErrorCode: "ServerBusy", StatusCode: http.StatusServiceUnavailable,
			RawResponse: &http.Response{StatusCode: http.StatusServiceUnavailable},
		}, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isTierMiss(tc.err); got != tc.want {
				t.Fatalf("isTierMiss(%v) = %t, want %t", tc.err, got, tc.want)
			}
		})
	}
}

// TestRecordTierOutcome_OnlyRealFailuresLandInEWMA pins the three-class split
// at the point where it matters — the tracker's sample count.
func TestRecordTierOutcome_OnlyRealFailuresLandInEWMA(t *testing.T) {
	cases := []struct {
		name        string
		err         error
		wantSamples int64
	}{
		{"success", nil, 1},
		{"miss not recorded", &peerMissError{addr: "any", path: "/x"}, 0},
		{"cloud miss not recorded", azureNotFound(), 0},
		{"busy not recorded", status.Error(codes.ResourceExhausted, "gate"), 0},
		{"real failure recorded", errors.New("connection reset"), 1},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cm := evictTestManager(t, 1<<30)
			cm.recordTierOutcome(TierPeer, time.Now(), tc.err)
			_, _, samples, _, _, _ := cm.tierPerf.snapshot()
			if samples != tc.wantSamples {
				t.Fatalf("peer samples = %d, want %d", samples, tc.wantSamples)
			}
		})
	}
}

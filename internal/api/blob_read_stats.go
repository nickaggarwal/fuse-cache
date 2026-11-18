package api

import (
	"fmt"
	"io"
	"net/http"
	"sync/atomic"
	"time"
)

// Per-read profile of the blob proxy: how long each nydusd read took, by the
// path that answered it. A start that is slow on one node but not another
// was guesswork without it (the Nebius H200 second node: disk, FUSE or
// peer?); with it, the path whose reads are slow is visible directly.

// blobReadPath is the path that answered a blob read.
type blobReadPath int

const (
	readLocal  blobReadPath = iota // whole local copy or local chunks
	readFill                       // a registry or peer fill in progress
	readRemote                     // per-chunk store/peer/cloud reads
	readMiss                       // not answered (nydusd falls back)
	numBlobReadPaths
)

var blobReadPathNames = [numBlobReadPaths]string{"local", "fill", "remote", "miss"}

// blobReadBuckets are histogram upper bounds in seconds.
var blobReadBuckets = []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5}

type blobReadHist struct {
	buckets [11]atomic.Int64 // len(blobReadBuckets)+1, last is +Inf
	count   atomic.Int64
	nanos   atomic.Int64
	bytes   atomic.Int64
}

type blobReadStats struct {
	paths [numBlobReadPaths]blobReadHist
}

func (s *blobReadStats) record(p blobReadPath, d time.Duration, n int64) {
	h := &s.paths[p]
	sec := d.Seconds()
	i := 0
	for i < len(blobReadBuckets) && sec > blobReadBuckets[i] {
		i++
	}
	h.buckets[i].Add(1)
	h.count.Add(1)
	h.nanos.Add(int64(d))
	h.bytes.Add(n)
}

func (s *blobReadStats) write(w io.Writer) {
	for p := blobReadPath(0); p < numBlobReadPaths; p++ {
		h := &s.paths[p]
		name := blobReadPathNames[p]
		var cum int64
		for i, le := range blobReadBuckets {
			cum += h.buckets[i].Load()
			fmt.Fprintf(w, "fuse_blob_read_seconds_bucket{path=%q,le=\"%g\"} %d\n", name, le, cum)
		}
		cum += h.buckets[len(blobReadBuckets)].Load()
		fmt.Fprintf(w, "fuse_blob_read_seconds_bucket{path=%q,le=\"+Inf\"} %d\n", name, cum)
		fmt.Fprintf(w, "fuse_blob_read_seconds_sum{path=%q} %.6f\n", name, time.Duration(h.nanos.Load()).Seconds())
		fmt.Fprintf(w, "fuse_blob_read_seconds_count{path=%q} %d\n", name, h.count.Load())
		fmt.Fprintf(w, "fuse_blob_read_bytes_total{path=%q} %d\n", name, h.bytes.Load())
	}
}

// statusRecorder notes the status and bytes a handler wrote.
type statusRecorder struct {
	http.ResponseWriter
	status int
	n      int64
}

func (r *statusRecorder) WriteHeader(code int) {
	r.status = code
	r.ResponseWriter.WriteHeader(code)
}

func (r *statusRecorder) Write(b []byte) (int, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	n, err := r.ResponseWriter.Write(b)
	r.n += int64(n)
	return n, err
}

// ReadFrom keeps the sendfile path for local pieces.
func (r *statusRecorder) ReadFrom(src io.Reader) (int64, error) {
	if r.status == 0 {
		r.status = http.StatusOK
	}
	var n int64
	var err error
	if rf, ok := r.ResponseWriter.(io.ReaderFrom); ok {
		n, err = rf.ReadFrom(src)
	} else {
		n, err = io.Copy(r.ResponseWriter, src)
	}
	r.n += n
	return n, err
}

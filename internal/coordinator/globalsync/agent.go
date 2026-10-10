package globalsync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	"fuse-client/internal/globalcp"
	"fuse-client/internal/pb"
)

const (
	// dialTimeout bounds one connection attempt. The agent is entirely off
	// the data path, so a slow CP costs discovery freshness and nothing
	// else -- there is no caller waiting on this.
	dialTimeout = 10 * time.Second

	// registerTimeout bounds the unary registration that precedes each
	// stream.
	registerTimeout = 15 * time.Second

	// Reconnect backoff. Jittered for the usual reason: N coordinators that
	// lost one CP must not resynchronize their retries into a thundering
	// herd against it as it comes back.
	reconnectBaseDelay = 1 * time.Second
	reconnectMaxDelay  = 60 * time.Second

	// agentHeartbeatInterval is how often the agent announces liveness when
	// it has nothing to publish. Half the CP's own 30s expectation, so one
	// lost message does not read as a dead cluster.
	agentHeartbeatInterval = 15 * time.Second
)

// Config configures the sync agent.
type Config struct {
	// ClusterID names this cluster in the global namespace. Required.
	//
	// Advisory on the wire once mTLS is on: the CP takes identity from the
	// verified client certificate and stamps it over whatever is sent. It
	// is still sent because the embedded/dev deployment has no certificate
	// and needs a name.
	ClusterID string

	// Endpoint is the global CP's gRPC address.
	Endpoint string

	// Object describes where this cluster's published objects live. The
	// KeyPrefix must match the client/coordinator -cloud-key-prefix, or
	// every remote read of a published path will look under the wrong key.
	Object *pb.ObjectRef

	// PublishPrefixes is what this cluster offers to the global namespace;
	// SubscribePrefixes is what it wants to hear about. Both default to
	// nothing, and an empty publish list means this cluster is a pure
	// consumer -- which is a legitimate configuration, not a misconfig.
	PublishPrefixes   []string
	SubscribePrefixes []string

	// DialOptions are applied to the CP connection. Stage 4 passes mTLS
	// credentials here; until then the caller passes nothing and the agent
	// dials insecure, which is why it logs a warning when it does.
	DialOptions []grpc.DialOption

	Logger *log.Logger
}

// Agent maintains the connection to the global control plane.
//
// Everything about it is off the data path. It owns one goroutine, it never
// blocks a caller, and when it cannot reach the CP the only consequence is
// that the mirror stops getting fresher. That is the local-autonomy
// requirement, and it is a property of the structure rather than of a
// fallback branch somewhere.
type Agent struct {
	cfg    Config
	logger *log.Logger
	mirror *Mirror
	queue  *publishQueue

	// publishAllow is the ACCEPTED allowlist, which is the CP's answer to
	// registration and may be narrower than what was configured. Filtering
	// against the configured list instead would queue rows the CP will
	// reject, and a rejected publish kills the stream.
	mu           sync.RWMutex
	publishAllow []string
	connected    bool
	lastErr      string
	lastConnect  time.Time

	// flushNow lets a publish wake the sender instead of waiting out the
	// flush tick. Buffered depth 1: a pending wake is as good as two.
	flushNow chan struct{}

	published  atomic.Uint64
	withdrawn  atomic.Uint64
	reconnects atomic.Uint64

	stop   context.CancelFunc
	doneCh chan struct{}
}

// New builds an agent. It does not connect; call Start.
func New(cfg Config) (*Agent, error) {
	if cfg.ClusterID == "" {
		return nil, errors.New("globalsync: ClusterID is required")
	}
	if cfg.Endpoint == "" {
		return nil, errors.New("globalsync: Endpoint is required")
	}
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "[GLOBALSYNC] ", log.LstdFlags)
	}
	cfg.PublishPrefixes = globalcp.NormalizePrefixes(cfg.PublishPrefixes)
	cfg.SubscribePrefixes = globalcp.NormalizePrefixes(cfg.SubscribePrefixes)

	return &Agent{
		cfg:      cfg,
		logger:   logger,
		mirror:   NewMirror(),
		queue:    newPublishQueue(),
		flushNow: make(chan struct{}, 1),
		doneCh:   make(chan struct{}),
	}, nil
}

// Mirror exposes the read-only global index. Safe before Start and after
// Stop: an unconnected agent has an empty, not-ready mirror, which answers
// every lookup with "nothing" rather than with an error.
func (a *Agent) Mirror() *Mirror { return a.mirror }

// Start begins the connect/retry loop in the background.
func (a *Agent) Start(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	a.stop = cancel
	go a.run(ctx)
}

// Stop shuts the agent down and waits for its goroutine.
func (a *Agent) Stop() {
	if a.stop != nil {
		a.stop()
	}
	select {
	case <-a.doneCh:
	case <-time.After(5 * time.Second):
		a.logger.Printf("shutdown timed out waiting for sync loop")
	}
}

func (a *Agent) run(ctx context.Context) {
	defer close(a.doneCh)

	attempt := 0
	for ctx.Err() == nil {
		err := a.session(ctx)
		if ctx.Err() != nil {
			return
		}
		a.setDisconnected(err)
		attempt++
		a.reconnects.Add(1)

		delay := backoff(attempt)
		if err != nil {
			a.logger.Printf("sync session ended: %v (reconnecting in %v)", err, delay.Truncate(time.Millisecond))
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(delay):
		}
	}
}

// backoff is exponential with full jitter, capped.
func backoff(attempt int) time.Duration {
	d := reconnectBaseDelay << uint(min(attempt, 16))
	if d > reconnectMaxDelay || d <= 0 {
		d = reconnectMaxDelay
	}
	// Full jitter rather than a fixed fraction: with N clusters retrying a
	// CP that just restarted, a deterministic delay reconverges them on the
	// same instant every round.
	return time.Duration(rand.Int63n(int64(d)) + int64(reconnectBaseDelay))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// session runs one connection from dial to disconnect. Every return is a
// reason to reconnect.
func (a *Agent) session(ctx context.Context) error {
	opts := a.cfg.DialOptions
	if len(opts) == 0 {
		// No credentials configured. The caller logs this once at startup
		// with the full explanation; repeating it per connection is what
		// keeps a long-running plaintext deployment from looking normal in
		// a log tail.
		a.logger.Printf("WARNING: connecting to %s without TLS", a.cfg.Endpoint)
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}

	dialCtx, cancel := context.WithTimeout(ctx, dialTimeout)
	conn, err := grpc.DialContext(dialCtx, a.cfg.Endpoint, append(opts, grpc.WithBlock())...)
	cancel()
	if err != nil {
		return fmt.Errorf("dial %s: %w", a.cfg.Endpoint, err)
	}
	defer conn.Close()

	client := pb.NewGlobalControlPlaneClient(conn)

	regCtx, regCancel := context.WithTimeout(a.outgoing(ctx), registerTimeout)
	reg, err := client.RegisterCluster(regCtx, &pb.RegisterClusterRequest{
		ClusterId:         a.cfg.ClusterID,
		Object:            a.cfg.Object,
		PublishPrefixes:   a.cfg.PublishPrefixes,
		SubscribePrefixes: a.cfg.SubscribePrefixes,
	})
	regCancel()
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	// The CP's answer is the authority, not the local config.
	a.mu.Lock()
	a.publishAllow = append([]string(nil), reg.GetPublishPrefixes()...)
	a.mu.Unlock()

	if len(reg.GetPublishPrefixes()) != len(a.cfg.PublishPrefixes) {
		a.logger.Printf("control plane narrowed the publish allowlist: asked %v, accepted %v",
			a.cfg.PublishPrefixes, reg.GetPublishPrefixes())
	}

	streamCtx, streamCancel := context.WithCancel(a.outgoing(ctx))
	defer streamCancel()

	stream, err := client.Sync(streamCtx)
	if err != nil {
		return fmt.Errorf("open sync stream: %w", err)
	}

	a.setConnected()
	a.logger.Printf("connected to global control plane %s as cluster %q (publish=%v subscribe=%v)",
		a.cfg.Endpoint, a.cfg.ClusterID, reg.GetPublishPrefixes(), a.cfg.SubscribePrefixes)

	// Two directions, two goroutines. The receive loop owns the mirror; the
	// send loop owns the queue. gRPC streams allow one concurrent Send and
	// one concurrent Recv, but not two of either, so neither side may be
	// split further.
	recvErr := make(chan error, 1)
	go func() { recvErr <- a.recvLoop(stream) }()

	sendErr := make(chan error, 1)
	go func() { sendErr <- a.sendLoop(streamCtx, stream) }()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err := <-recvErr:
		return err
	case err := <-sendErr:
		return err
	}
}

// outgoing attaches the cluster-ID header.
//
// This is NOT how identity is established once mTLS is on: the control
// plane reads the verified client certificate and ignores this header
// entirely, so sending it cannot let a cluster claim to be another. It
// exists for the embedded/dev mode, which has no certificates and would
// otherwise have no name to register under.
func (a *Agent) outgoing(ctx context.Context) context.Context {
	return metadata.AppendToOutgoingContext(ctx, globalcp.ClusterIDHeader, a.cfg.ClusterID)
}

// recvLoop applies the snapshot and subsequent events to the mirror.
func (a *Agent) recvLoop(stream pb.GlobalControlPlane_SyncClient) error {
	// Snapshot batches accumulate until the final flag, then replace the
	// mirror in one swap. Applying them incrementally would leave the
	// mirror briefly holding a partial index that looks complete to a
	// lookup, which is how "no remote copy" gets answered wrongly.
	var snapshot []*pb.GlobalLocation
	// Events that arrive mid-snapshot are held, not applied: the swap below
	// would overwrite them. The CP registers a subscriber before it builds
	// the snapshot -- deliberately, so nothing is missed -- which makes this
	// race the normal case on a busy namespace rather than an exotic one.
	var deferred []*pb.GlobalEvent
	snapshotting := true

	for {
		resp, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				return errors.New("control plane closed the stream")
			}
			return fmt.Errorf("recv: %w", err)
		}

		switch body := resp.GetBody().(type) {
		case *pb.SyncResponse_Snapshot:
			snapshot = append(snapshot, body.Snapshot.GetLocations()...)
			if body.Snapshot.GetFinal() {
				a.mirror.ApplySnapshot(snapshot)
				snapshot = nil
				snapshotting = false
				// Replay in arrival order, after the swap. Correct for both
				// kinds: an upsert re-adds a row the snapshot predates, and
				// a withdraw removes one the snapshot still carried.
				for _, ev := range deferred {
					a.mirror.ApplyEvent(ev)
				}
				deferred = nil
				st := a.mirror.Status()
				a.logger.Printf("mirror synced: %d paths, %d rows", st.Paths, st.Rows)
			}

		case *pb.SyncResponse_Event:
			if snapshotting {
				deferred = append(deferred, body.Event)
				continue
			}
			a.mirror.ApplyEvent(body.Event)

		case *pb.SyncResponse_Heartbeat:
			// Liveness only. Recv returning at all is the signal; there is
			// nothing to apply.
		}
	}
}

// sendLoop drains the publish queue onto the stream and keeps the
// connection warm with heartbeats.
func (a *Agent) sendLoop(ctx context.Context, stream pb.GlobalControlPlane_SyncClient) error {
	flush := time.NewTicker(publishFlushInterval)
	defer flush.Stop()
	hb := time.NewTicker(agentHeartbeatInterval)
	defer hb.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-hb.C:
			if err := stream.Send(&pb.SyncRequest{
				Body: &pb.SyncRequest_Heartbeat{Heartbeat: &pb.Heartbeat{SentUnix: time.Now().Unix()}},
			}); err != nil {
				return fmt.Errorf("heartbeat: %w", err)
			}

		case <-flush.C:
			if err := a.flush(stream); err != nil {
				return err
			}

		case <-a.flushNow:
			if err := a.flush(stream); err != nil {
				return err
			}
		}
	}
}

func (a *Agent) flush(stream pb.GlobalControlPlane_SyncClient) error {
	upserts, withdrawals := a.queue.Drain()
	if len(upserts) == 0 && len(withdrawals) == 0 {
		return nil
	}

	if len(upserts) > 0 {
		if err := stream.Send(&pb.SyncRequest{
			Body: &pb.SyncRequest_Publish{Publish: &pb.PublishBatch{Locations: upserts}},
		}); err != nil {
			// The batch never reached the CP, so put it back: dropping it
			// would leave this cluster's rows missing from the global index
			// until each path happens to be written again.
			a.queue.Requeue(upserts, withdrawals)
			return fmt.Errorf("publish: %w", err)
		}
		a.published.Add(uint64(len(upserts)))
	}

	if len(withdrawals) > 0 {
		if err := stream.Send(&pb.SyncRequest{
			Body: &pb.SyncRequest_Withdraw{Withdraw: &pb.WithdrawBatch{FilePaths: withdrawals}},
		}); err != nil {
			a.queue.Requeue(nil, withdrawals)
			return fmt.Errorf("withdraw: %w", err)
		}
		a.withdrawn.Add(uint64(len(withdrawals)))
	}
	return nil
}

// OnCloudPersist is the publish trigger: the coordinator calls it when a
// location row says a path's bytes are in object storage.
//
// It is called from the coordinator's metadata write path, so it must be
// cheap and must never block or fail. It takes a lock, maybe appends to a
// map, and returns.
func (a *Agent) OnCloudPersist(filePath, storageTier string, fileSize int64, isChunked bool, chunkSize int64) {
	a.mu.RLock()
	allow := a.publishAllow
	a.mu.RUnlock()

	if !publishable(filePath, storageTier, allow) {
		return
	}

	queued := a.queue.Publish(&pb.GlobalLocation{
		ClusterId: a.cfg.ClusterID,
		FilePath:  filePath,
		FileSize:  fileSize,
		IsChunked: isChunked,
		ChunkSize: chunkSize,
		Object:    a.cfg.Object,
	}, time.Now())

	if queued {
		a.wake()
	}
}

// OnDelete is the withdraw trigger.
//
// Unlike publish this does not check the tier -- a deleted path has no
// tier -- but it does still check the allowlist, because a path outside it
// was never published and queueing a withdraw for it is pure noise.
func (a *Agent) OnDelete(filePath string) {
	a.mu.RLock()
	allow := a.publishAllow
	a.mu.RUnlock()

	if !globalcp.MatchesAny(filePath, allow) {
		return
	}
	a.queue.Withdraw(filePath, time.Now())
	a.wake()
}

func (a *Agent) wake() {
	select {
	case a.flushNow <- struct{}{}:
	default:
	}
}

func (a *Agent) setConnected() {
	a.mu.Lock()
	a.connected = true
	a.lastErr = ""
	a.lastConnect = time.Now()
	a.mu.Unlock()
}

func (a *Agent) setDisconnected(err error) {
	a.mu.Lock()
	a.connected = false
	if err != nil {
		a.lastErr = err.Error()
	}
	a.mu.Unlock()
	// Deliberately keeps the mirror's contents. See Mirror.MarkDisconnected.
	a.mirror.MarkDisconnected()
}

// Status reports the agent's state, for /api/global/status.
type Status struct {
	ClusterID         string       `json:"cluster_id"`
	Endpoint          string       `json:"endpoint"`
	Connected         bool         `json:"connected"`
	LastConnectAt     time.Time    `json:"last_connect_at,omitempty"`
	LastError         string       `json:"last_error,omitempty"`
	Reconnects        uint64       `json:"reconnects"`
	PublishPrefixes   []string     `json:"publish_prefixes"`
	SubscribePrefixes []string     `json:"subscribe_prefixes"`
	AcceptedPrefixes  []string     `json:"accepted_publish_prefixes"`
	QueueDepth        int          `json:"queue_depth"`
	QueueDropped      uint64       `json:"queue_dropped"`
	Published         uint64       `json:"published_total"`
	Withdrawn         uint64       `json:"withdrawn_total"`
	Mirror            MirrorStatus `json:"mirror"`
}

func (a *Agent) Status() Status {
	a.mu.RLock()
	connected, lastErr, lastConn := a.connected, a.lastErr, a.lastConnect
	accepted := append([]string(nil), a.publishAllow...)
	a.mu.RUnlock()

	return Status{
		ClusterID:         a.cfg.ClusterID,
		Endpoint:          a.cfg.Endpoint,
		Connected:         connected,
		LastConnectAt:     lastConn,
		LastError:         lastErr,
		Reconnects:        a.reconnects.Load(),
		PublishPrefixes:   a.cfg.PublishPrefixes,
		SubscribePrefixes: a.cfg.SubscribePrefixes,
		AcceptedPrefixes:  accepted,
		QueueDepth:        a.queue.Len(),
		QueueDropped:      a.queue.Dropped(),
		Published:         a.published.Load(),
		Withdrawn:         a.withdrawn.Load(),
		Mirror:            a.mirror.Status(),
	}
}

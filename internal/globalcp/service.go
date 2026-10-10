package globalcp

import (
	"context"
	"log"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"fuse-client/internal/pb"
)

const (
	// subscriberQueueDepth bounds what one slow subscriber can cost the
	// publishers. See subscriber.send for why overflow drops the connection
	// rather than the event.
	subscriberQueueDepth = 1024
	// snapshotBatchSize keeps one snapshot message from growing unbounded on
	// a large index.
	snapshotBatchSize = 256
	// heartbeatInterval is how often the server pushes liveness on an
	// otherwise-idle stream, so an agent can tell "nothing published lately"
	// from "the connection is a black hole".
	heartbeatInterval = 30 * time.Second
)

// Config configures the service.
type Config struct {
	Store  Store
	Logger *log.Logger
	// MaxPublishPrefixes caps how many prefixes one cluster may register.
	// 0 uses the default.
	MaxPublishPrefixes int
	// AllowInsecureIdentity permits header-declared cluster identity on
	// connections with no verified client certificate. Dev/embedded only --
	// it lets any caller claim any cluster ID.
	AllowInsecureIdentity bool
}

// Service implements pb.GlobalControlPlaneServer.
type Service struct {
	pb.UnimplementedGlobalControlPlaneServer

	store  Store
	logger *log.Logger
	maxPfx int

	// AllowInsecureIdentity lets a connection without a verified client
	// certificate name itself via a header. It exists for the embedded/dev
	// mode only; see identify. Stage 4 deployments leave it false.
	AllowInsecureIdentity bool

	mu   sync.RWMutex
	subs map[int64]*subscriber

	nextSubID atomic.Int64
}

// subscriber is one connected cluster's server->client push channel.
type subscriber struct {
	id        int64
	clusterID string
	prefixes  []string
	ch        chan *pb.SyncResponse
	// closed once, by the owning Sync handler, after it returns.
	closeOnce sync.Once
	done      chan struct{}
}

// send delivers an event, or reports that the subscriber is too far behind.
//
// Overflow drops the SUBSCRIBER, not the event. A mirror that silently
// missed one upsert is wrong forever with no way to notice -- it would
// answer "no such path" for a file that exists. Dropping the stream is
// recoverable by construction: the agent reconnects and gets a fresh
// snapshot. An unbounded queue is not an option either, since one wedged
// subscriber would then grow until the CP dies.
func (s *subscriber) send(msg *pb.SyncResponse) bool {
	select {
	case s.ch <- msg:
		return true
	case <-s.done:
		return false
	default:
		return false
	}
}

func (s *subscriber) close() {
	s.closeOnce.Do(func() { close(s.done) })
}

func NewService(cfg Config) *Service {
	logger := cfg.Logger
	if logger == nil {
		logger = log.New(os.Stdout, "[GLOBALCP] ", log.LstdFlags)
	}
	store := cfg.Store
	if store == nil {
		store = NewMemStore()
	}
	maxPfx := cfg.MaxPublishPrefixes
	if maxPfx <= 0 {
		maxPfx = 64
	}
	return &Service{
		store:                 store,
		logger:                logger,
		maxPfx:                maxPfx,
		AllowInsecureIdentity: cfg.AllowInsecureIdentity,
		subs:                  make(map[int64]*subscriber),
	}
}

// RegisterCluster records a cluster and returns the allowlists it was
// actually granted.
func (s *Service) RegisterCluster(ctx context.Context, req *pb.RegisterClusterRequest) (*pb.RegisterClusterResponse, error) {
	clusterID, err := s.identify(ctx, req.GetClusterId())
	if err != nil {
		return nil, err
	}

	pub := NormalizePrefixes(req.GetPublishPrefixes())
	sub := NormalizePrefixes(req.GetSubscribePrefixes())
	if len(pub) > s.maxPfx || len(sub) > s.maxPfx {
		return nil, status.Errorf(codes.InvalidArgument, "at most %d prefixes per list", s.maxPfx)
	}

	now := time.Now()
	existing, err := s.store.GetCluster(ctx, clusterID)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read cluster: %v", err)
	}
	c := &Cluster{
		ID:                clusterID,
		Object:            req.GetObject(),
		PublishPrefixes:   pub,
		SubscribePrefixes: sub,
		RegisteredAt:      now,
		LastSeen:          now,
	}
	if existing != nil {
		c.RegisteredAt = existing.RegisteredAt
	}
	if err := s.store.PutCluster(ctx, c); err != nil {
		return nil, status.Errorf(codes.Internal, "store cluster: %v", err)
	}
	s.logger.Printf("registered cluster=%s publish=%v subscribe=%v", clusterID, pub, sub)

	return &pb.RegisterClusterResponse{
		ClusterId:         clusterID,
		PublishPrefixes:   pub,
		SubscribePrefixes: sub,
	}, nil
}

// Resolve answers a point lookup. Deliberately off every hot path: a
// reader only gets here when its mirror does not know the path.
func (s *Service) Resolve(ctx context.Context, req *pb.ResolveRequest) (*pb.ResolveResponse, error) {
	path := req.GetFilePath()
	if path == "" {
		return nil, status.Error(codes.InvalidArgument, "file_path is required")
	}
	locs, err := s.store.GetLocations(ctx, path)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "resolve: %v", err)
	}
	return &pb.ResolveResponse{Locations: locs}, nil
}

// Sync runs one cluster's long-lived stream: a snapshot, then events, with
// the client's publishes and withdraws flowing the other way.
func (s *Service) Sync(stream pb.GlobalControlPlane_SyncServer) error {
	ctx := stream.Context()
	clusterID, err := s.identify(ctx, "")
	if err != nil {
		return err
	}
	c, err := s.store.GetCluster(ctx, clusterID)
	if err != nil {
		return status.Errorf(codes.Internal, "read cluster: %v", err)
	}
	if c == nil {
		// Registration carries the allowlist, and without it there is no
		// basis on which to accept a publish or scope a subscription.
		return status.Errorf(codes.FailedPrecondition, "cluster %s must call RegisterCluster before Sync", clusterID)
	}

	sub := &subscriber{
		id:        s.nextSubID.Add(1),
		clusterID: clusterID,
		prefixes:  c.SubscribePrefixes,
		ch:        make(chan *pb.SyncResponse, subscriberQueueDepth),
		done:      make(chan struct{}),
	}
	s.addSubscriber(sub)
	defer func() {
		s.removeSubscriber(sub)
		sub.close()
	}()

	// The snapshot is sent before the subscriber can receive any event, so
	// a reconnecting agent converges without replaying history. Events that
	// arrive during the send queue up behind it in order.
	if err := s.sendSnapshot(ctx, stream, sub); err != nil {
		return err
	}

	// One writer goroutine owns the send side. gRPC streams are not safe for
	// concurrent SendMsg, and events arrive from other clusters' handlers.
	sendErr := make(chan error, 1)
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				sendErr <- ctx.Err()
				return
			case <-sub.done:
				sendErr <- nil
				return
			case msg := <-sub.ch:
				if err := stream.Send(msg); err != nil {
					sendErr <- err
					return
				}
			case <-ticker.C:
				hb := &pb.SyncResponse{Body: &pb.SyncResponse_Heartbeat{
					Heartbeat: &pb.Heartbeat{SentUnix: time.Now().Unix()},
				}}
				if err := stream.Send(hb); err != nil {
					sendErr <- err
					return
				}
			}
		}
	}()

	recvErr := make(chan error, 1)
	go func() { recvErr <- s.recvLoop(ctx, stream, c) }()

	select {
	case err := <-recvErr:
		return err
	case err := <-sendErr:
		return err
	}
}

func (s *Service) recvLoop(ctx context.Context, stream pb.GlobalControlPlane_SyncServer, c *Cluster) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}
		_ = s.store.TouchCluster(ctx, c.ID, time.Now())

		switch body := req.GetBody().(type) {
		case *pb.SyncRequest_Publish:
			if err := s.applyPublish(ctx, c, body.Publish.GetLocations()); err != nil {
				return err
			}
		case *pb.SyncRequest_Withdraw:
			if err := s.applyWithdraw(ctx, c, body.Withdraw.GetFilePaths()); err != nil {
				return err
			}
		case *pb.SyncRequest_Heartbeat:
			// Liveness only; TouchCluster above is the whole effect.
		}
	}
}

// applyPublish validates and stores a batch, then fans it out.
//
// Two rules, both enforced here rather than trusted from the wire:
// a row is stamped with the AUTHENTICATED cluster ID, so a cluster cannot
// publish on another's behalf; and every path must fall inside the
// registered allowlist, so a misconfigured agent cannot widen its own scope.
func (s *Service) applyPublish(ctx context.Context, c *Cluster, locs []*pb.GlobalLocation) error {
	if len(locs) == 0 {
		return nil
	}
	accepted := make([]*pb.GlobalLocation, 0, len(locs))
	for _, l := range locs {
		if l == nil || l.GetFilePath() == "" {
			continue
		}
		if !MatchesAny(l.GetFilePath(), c.PublishPrefixes) {
			return status.Errorf(codes.PermissionDenied,
				"cluster %s may not publish %s: outside its registered prefixes", c.ID, l.GetFilePath())
		}
		row := cloneLocation(l)
		row.ClusterId = c.ID
		if row.Object == nil {
			// Fall back to the cluster's registered object store rather than
			// storing a row that points nowhere.
			row.Object = c.Object
		}
		if row.PublishedUnix == 0 {
			row.PublishedUnix = time.Now().Unix()
		}
		accepted = append(accepted, row)
	}
	if len(accepted) == 0 {
		return nil
	}
	if err := s.store.PutLocations(ctx, accepted); err != nil {
		return status.Errorf(codes.Internal, "store locations: %v", err)
	}
	for _, row := range accepted {
		s.broadcast(&pb.GlobalEvent{Kind: pb.GlobalEvent_UPSERT, Location: row}, c.ID)
	}
	return nil
}

func (s *Service) applyWithdraw(ctx context.Context, c *Cluster, paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	// A withdraw outside the allowlist is not an authz violation worth
	// killing the stream over -- it can only ever delete the cluster's own
	// rows, and there are none outside its allowlist. Let it through
	// idempotently rather than failing a reconnecting agent that is
	// retracting rows from an older, wider config.
	if err := s.store.DeleteLocations(ctx, c.ID, paths); err != nil {
		return status.Errorf(codes.Internal, "delete locations: %v", err)
	}
	for _, p := range paths {
		s.broadcast(&pb.GlobalEvent{
			Kind:      pb.GlobalEvent_WITHDRAW,
			ClusterId: c.ID,
			FilePath:  p,
		}, c.ID)
	}
	return nil
}

func (s *Service) sendSnapshot(ctx context.Context, stream pb.GlobalControlPlane_SyncServer, sub *subscriber) error {
	locs, err := s.store.ListLocations(ctx, sub.prefixes)
	if err != nil {
		return status.Errorf(codes.Internal, "snapshot: %v", err)
	}
	// A cluster's own rows are not news to it, and echoing them back would
	// have a mirror answer "another cluster has this" for its own files.
	filtered := locs[:0]
	for _, l := range locs {
		if l.GetClusterId() != sub.clusterID {
			filtered = append(filtered, l)
		}
	}
	locs = filtered

	for i := 0; i < len(locs); i += snapshotBatchSize {
		end := i + snapshotBatchSize
		if end > len(locs) {
			end = len(locs)
		}
		if err := stream.Send(&pb.SyncResponse{Body: &pb.SyncResponse_Snapshot{
			Snapshot: &pb.SnapshotBatch{Locations: locs[i:end], Final: end == len(locs)},
		}}); err != nil {
			return err
		}
	}
	if len(locs) == 0 {
		// An empty index still needs its terminator, or an agent waits
		// forever for a mirror that is already complete.
		return stream.Send(&pb.SyncResponse{Body: &pb.SyncResponse_Snapshot{
			Snapshot: &pb.SnapshotBatch{Final: true},
		}})
	}
	return nil
}

// broadcast pushes an event to every subscriber whose allowlist covers the
// path, except the cluster that published it.
func (s *Service) broadcast(ev *pb.GlobalEvent, fromCluster string) {
	path := ev.GetFilePath()
	if path == "" {
		path = ev.GetLocation().GetFilePath()
	}

	s.mu.RLock()
	targets := make([]*subscriber, 0, len(s.subs))
	for _, sub := range s.subs {
		if sub.clusterID == fromCluster {
			continue
		}
		if !MatchesAny(path, sub.prefixes) {
			continue
		}
		targets = append(targets, sub)
	}
	s.mu.RUnlock()

	msg := &pb.SyncResponse{Body: &pb.SyncResponse_Event{Event: ev}}
	for _, sub := range targets {
		if !sub.send(msg) {
			s.logger.Printf("subscriber cluster=%s behind by >%d events; dropping stream to force resync",
				sub.clusterID, subscriberQueueDepth)
			sub.close()
		}
	}
}

func (s *Service) addSubscriber(sub *subscriber) {
	s.mu.Lock()
	s.subs[sub.id] = sub
	s.mu.Unlock()
}

func (s *Service) removeSubscriber(sub *subscriber) {
	s.mu.Lock()
	delete(s.subs, sub.id)
	s.mu.Unlock()
}

// SubscriberCount reports connected streams. For /api/global observability
// and for tests that need to wait on a connection rather than sleep.
func (s *Service) SubscriberCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.subs)
}

// Store exposes the backing store for the HTTP observability surface.
func (s *Service) Store() Store { return s.store }

package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"fuse-client/internal/cache"
	"fuse-client/internal/coordinator"
	"fuse-client/internal/coordinator/globalsync"
	"fuse-client/internal/globalcp"
	"fuse-client/internal/mtls"
	pb "fuse-client/internal/pb"

	clientv3 "go.etcd.io/etcd/client/v3"
	"google.golang.org/grpc"
)

func main() {
	var (
		port            = flag.Int("port", 8080, "Port for the coordinator HTTP server")
		grpcPort        = flag.Int("grpc-port", 9080, "Port for the coordinator gRPC server")
		etcdEndpoints   = flag.String("etcd-endpoints", "", "Comma-separated etcd endpoints (e.g. http://etcd-0:2379,http://etcd-1:2379). If empty, uses in-memory state.")
		etcdPrefix      = flag.String("etcd-prefix", "/fuse", "etcd key prefix for coordinator state")
		etcdLeaseTTL    = flag.Int("etcd-peer-lease-ttl", 90, "Peer liveness lease TTL in seconds (etcd-backed only); keep this at ~3x the client heartbeat interval (30s) so one slow heartbeat doesn't expire the peer")
		etcdDialTimeout = flag.Duration("etcd-dial-timeout", 5*time.Second, "etcd dial timeout")

		// Global control plane. Every one of these defaults to off: an
		// existing single-cluster deployment that upgrades this binary must
		// behave exactly as before, with no new goroutine, no new socket,
		// and no new dependency on anything being reachable.
		globalEnabled      = flag.Bool("global-enabled", false, "Join a global control plane so a published subset of this cluster's cache is discoverable by other clusters")
		globalClusterID    = flag.String("global-cluster-id", "", "This cluster's identity in the global namespace (required with -global-enabled)")
		globalEndpoint     = flag.String("global-endpoint", "", "Global control plane gRPC address (required with -global-enabled unless -global-embedded)")
		globalPublish      = flag.String("global-publish-prefixes", "", "Comma-separated path prefixes this cluster publishes globally, e.g. /models,/checkpoints. Empty publishes nothing.")
		globalSubscribe    = flag.String("global-subscribe-prefixes", "", "Comma-separated path prefixes this cluster wants to discover from other clusters. Empty subscribes to nothing.")
		globalEmbedded     = flag.Bool("global-embedded", false, "DEV ONLY: run the global control plane in-process on loopback with no TLS, and point the sync agent at it")
		globalEmbeddedPort = flag.Int("global-embedded-port", 9091, "Loopback port for the embedded global control plane")
		globalTLSCert      = flag.String("global-tls-cert", "", "PEM client certificate this cluster presents to the global control plane; its first DNS SAN is the cluster identity the CP will use")
		globalTLSKey       = flag.String("global-tls-key", "", "PEM private key for -global-tls-cert")
		globalTLSCA        = flag.String("global-tls-ca", "", "PEM CA bundle that signs the global control plane's server certificate")
		globalTLSName      = flag.String("global-tls-server-name", "", "Expected server name in the control plane's certificate; defaults to the host part of -global-endpoint")

		help = flag.Bool("help", false, "Show help")
	)
	flag.Parse()

	if *help {
		flag.Usage()
		return
	}

	logger := log.New(os.Stdout, "[COORDINATOR] ", log.LstdFlags)
	logger.Printf("Starting coordinator on HTTP port %d, gRPC port %d", *port, *grpcPort)

	// Create context for graceful shutdown
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Build the coordinator service, optionally backed by etcd.
	store, err := buildStore(ctx, *etcdEndpoints, *etcdPrefix, int64(*etcdLeaseTTL), *etcdDialTimeout, logger)
	if err != nil {
		logger.Fatalf("Failed to build coordinator store: %v", err)
	}
	coordinatorService := coordinator.NewCoordinatorServiceWithStore(store)
	coordinatorService.Start(ctx)
	defer func() {
		if err := store.Close(); err != nil {
			logger.Printf("Store close: %v", err)
		}
	}()

	// Start gRPC server
	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		logger.Fatalf("Failed to listen on gRPC port %d: %v", *grpcPort, err)
	}
	grpcSrv := grpc.NewServer()
	pb.RegisterCoordinatorServiceServer(grpcSrv, coordinator.NewGRPCServer(coordinatorService))

	go func() {
		logger.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			logger.Printf("gRPC server error: %v", err)
		}
	}()

	// Global control plane, if enabled. Everything below this point is
	// nil-safe when it is not.
	gcfg := globalConfigFromEnv(globalSettings{
		Enabled:      *globalEnabled,
		ClusterID:    *globalClusterID,
		Endpoint:     *globalEndpoint,
		Publish:      *globalPublish,
		Subscribe:    *globalSubscribe,
		Embedded:     *globalEmbedded,
		EmbeddedPort: *globalEmbeddedPort,
		TLSCert:      *globalTLSCert,
		TLSKey:       *globalTLSKey,
		TLSCA:        *globalTLSCA,
		TLSName:      *globalTLSName,
	})

	var (
		syncAgent   *globalsync.Agent
		embeddedSrv *grpc.Server
	)
	if gcfg.Enabled {
		// Fail at startup rather than at first publish. A coordinator that
		// came up "enabled" but silently never joined is the worst of the
		// three states: an operator sees a healthy pod and an empty global
		// index, with nothing connecting the two.
		if gcfg.ClusterID == "" {
			logger.Fatalf("-global-enabled requires -global-cluster-id (or FUSE_GLOBAL_CLUSTER_ID / CLUSTER_NAME)")
		}
		if !gcfg.Embedded && gcfg.Endpoint == "" {
			logger.Fatalf("-global-enabled requires -global-endpoint (or FUSE_GLOBAL_ENDPOINT), unless -global-embedded")
		}
		if len(gcfg.Publish) == 0 && len(gcfg.Subscribe) == 0 {
			// Not fatal: a cluster may legitimately join to be visible in
			// the registry before any prefix is configured. But it does
			// nothing, and silence here reads as a working setup.
			logger.Printf("WARNING: global sync enabled with no publish or subscribe prefixes; nothing will cross the cluster boundary")
		}
		if err := gcfg.TLS.Validate(); err != nil {
			logger.Fatalf("Global TLS configuration: %v", err)
		}
		// Embedded mode is loopback and certificate-free by construction, so
		// TLS flags there are a contradiction rather than a refinement -- most
		// likely a production config that also picked up -global-embedded.
		if gcfg.Embedded && gcfg.TLS.Enabled() {
			logger.Fatalf("-global-embedded cannot be combined with -global-tls-*: the embedded control plane is loopback-only and serves no certificate")
		}
		if !gcfg.Embedded && !gcfg.TLS.Enabled() {
			// Loud, not fatal: stages 0-3 are exercisable against a plaintext
			// CP on a trusted network, and failing here would break that. But
			// without a certificate the CP cannot tell this cluster from any
			// other, so the ID below is a claim, not an identity.
			logger.Printf("WARNING: connecting to global control plane %s without mTLS; cluster identity %q is self-declared and unverifiable. Do not run this across an untrusted network.",
				gcfg.Endpoint, gcfg.ClusterID)
		}

		var dialOpts []grpc.DialOption
		if gcfg.TLS.Enabled() {
			// Default the expected server name to the endpoint host, so an
			// operator who set no -global-tls-server-name still gets name
			// verification rather than silently skipping it.
			if gcfg.TLS.ServerName == "" {
				gcfg.TLS.ServerName = hostOnly(gcfg.Endpoint)
			}
			opt, derr := gcfg.TLS.DialOption()
			if derr != nil {
				logger.Fatalf("Failed to build global client TLS credentials: %v", derr)
			}
			dialOpts = append(dialOpts, opt)
		}

		if gcfg.Embedded {
			embeddedSrv, err = startEmbeddedGlobalCP(gcfg.EmbeddedPort, logger)
			if err != nil {
				logger.Fatalf("Failed to start embedded global control plane: %v", err)
			}
			gcfg.Endpoint = fmt.Sprintf("127.0.0.1:%d", gcfg.EmbeddedPort)
		}
		syncAgent, err = globalsync.New(globalsync.Config{
			ClusterID:         gcfg.ClusterID,
			Endpoint:          gcfg.Endpoint,
			Object:            globalObjectRefFromEnv(gcfg.ClusterID),
			PublishPrefixes:   gcfg.Publish,
			SubscribePrefixes: gcfg.Subscribe,
			DialOptions:       dialOpts,
			Logger:            log.New(os.Stdout, "[GLOBALSYNC] ", log.LstdFlags),
		})
		if err != nil {
			logger.Fatalf("Failed to build global sync agent: %v", err)
		}
		// Attached before the service starts serving, so no request can
		// observe a half-wired coordinator.
		coordinatorService.SetGlobalPublisher(syncAgent)
		// The read side of the same link. Installed together with the
		// publisher so a coordinator is never publishing into an index it
		// cannot itself resolve against.
		coordinatorService.SetGlobalMirror(syncAgent.Mirror())
		syncAgent.Start(ctx)
		logger.Printf("Global sync enabled: cluster=%q endpoint=%s mtls=%v publish=%v subscribe=%v",
			gcfg.ClusterID, gcfg.Endpoint, gcfg.TLS.Enabled(), gcfg.Publish, gcfg.Subscribe)
	}

	// Create HTTP server
	mux := http.NewServeMux()
	setupRoutes(mux, coordinatorService, newSnapshotCloudStoreFromEnv(logger))
	// Mounted unconditionally: with sync off these report disabled, which
	// is a usable answer. A 404 would be indistinguishable from an older
	// binary that has no global support at all.
	globalsync.RegisterRoutes(mux, syncAgent)

	server := &http.Server{
		Addr:    fmt.Sprintf(":%d", *port),
		Handler: mux,
	}

	// Start server in goroutine
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Printf("HTTP server error: %v", err)
		}
	}()

	logger.Printf("Coordinator started on HTTP port %d, gRPC port %d", *port, *grpcPort)

	// Wait for interrupt signal to gracefully shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down coordinator...")

	// Graceful shutdown
	cancel()
	if syncAgent != nil {
		syncAgent.Stop()
	}
	if embeddedSrv != nil {
		embeddedSrv.GracefulStop()
	}
	grpcSrv.GracefulStop()

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}

	// Save state before exiting
	if err := coordinatorService.SaveState(); err != nil {
		logger.Printf("Failed to save state: %v", err)
	}

	logger.Println("Coordinator stopped")
}

// globalSettings is the raw flag surface, before env defaults.
type globalSettings struct {
	Enabled      bool
	ClusterID    string
	Endpoint     string
	Publish      string
	Subscribe    string
	Embedded     bool
	EmbeddedPort int
	TLSCert      string
	TLSKey       string
	TLSCA        string
	TLSName      string
}

// globalConfig is the resolved configuration.
type globalConfig struct {
	Enabled      bool
	ClusterID    string
	Endpoint     string
	Publish      []string
	Subscribe    []string
	Embedded     bool
	EmbeddedPort int
	TLS          mtls.Config
}

// globalConfigFromEnv applies env defaults to the flag values.
//
// Precedence: flag wins, env is the default. The older settings in this
// repo vary on this (COORDINATOR_GRPC_ADDR unconditionally overrides its
// flag), but every setting added for the global path uses the one rule.
func globalConfigFromEnv(s globalSettings) globalConfig {
	str := func(flagVal, env string) string {
		if v := strings.TrimSpace(flagVal); v != "" {
			return v
		}
		return strings.TrimSpace(os.Getenv(env))
	}
	cfg := globalConfig{
		Enabled:      s.Enabled || envBool("FUSE_GLOBAL_ENABLED", false),
		ClusterID:    str(s.ClusterID, "FUSE_GLOBAL_CLUSTER_ID"),
		Endpoint:     str(s.Endpoint, "FUSE_GLOBAL_ENDPOINT"),
		Publish:      splitPrefixes(str(s.Publish, "FUSE_GLOBAL_PUBLISH_PREFIXES")),
		Subscribe:    splitPrefixes(str(s.Subscribe, "FUSE_GLOBAL_SUBSCRIBE_PREFIXES")),
		Embedded:     s.Embedded || envBool("FUSE_GLOBAL_EMBEDDED", false),
		EmbeddedPort: s.EmbeddedPort,
		TLS: mtls.Config{
			CertFile:   str(s.TLSCert, "FUSE_GLOBAL_TLS_CERT"),
			KeyFile:    str(s.TLSKey, "FUSE_GLOBAL_TLS_KEY"),
			CAFile:     str(s.TLSCA, "FUSE_GLOBAL_TLS_CA"),
			ServerName: str(s.TLSName, "FUSE_GLOBAL_TLS_SERVER_NAME"),
		},
	}
	if !cfg.Enabled {
		return cfg
	}
	// A cluster ID is not defaultable. Two clusters that both fall back to
	// the same generated name would silently overwrite each other's rows in
	// the global index, which is precisely the collision this whole design
	// exists to prevent -- so fall back to the Kubernetes cluster-ish names
	// only if an operator set one, and otherwise let startup fail loudly.
	if cfg.ClusterID == "" {
		cfg.ClusterID = strings.TrimSpace(os.Getenv("CLUSTER_NAME"))
	}
	return cfg
}

// hostOnly strips the port from a dial target, for certificate name
// verification. A bare host with no port is returned unchanged, so an
// endpoint written without one still verifies against itself rather than
// against the empty string, which gRPC treats as "skip the name check".
func hostOnly(endpoint string) string {
	h, _, err := net.SplitHostPort(strings.TrimSpace(endpoint))
	if err != nil {
		return strings.TrimSpace(endpoint)
	}
	return h
}

func splitPrefixes(v string) []string {
	if strings.TrimSpace(v) == "" {
		return nil
	}
	var out []string
	for _, p := range strings.Split(v, ",") {
		if t := strings.TrimSpace(p); t != "" {
			out = append(out, t)
		}
	}
	return out
}

// globalObjectRefFromEnv describes where this cluster's published objects
// live, from the same env the cache backends already read.
//
// KeyPrefix comes from FUSE_CLOUD_KEY_PREFIX, which must be the same value
// the clients use: a skew here does not fail, it makes every remote read of
// a published path look under a key that was never written.
func globalObjectRefFromEnv(clusterID string) *pb.ObjectRef {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("CLOUD_PROVIDER")))
	ref := &pb.ObjectRef{
		Provider:  provider,
		KeyPrefix: strings.TrimSpace(os.Getenv("FUSE_CLOUD_KEY_PREFIX")),
	}
	switch provider {
	case "azure":
		ref.Bucket = strings.TrimSpace(os.Getenv("AZURE_CONTAINER_NAME"))
		ref.Endpoint = strings.TrimSpace(os.Getenv("AZURE_STORAGE_ACCOUNT"))
	case "gcp":
		ref.Bucket = strings.TrimSpace(os.Getenv("GCP_BUCKET"))
		ref.Endpoint = strings.TrimSpace(os.Getenv("GCP_ENDPOINT"))
	default:
		ref.Bucket = strings.TrimSpace(os.Getenv("S3_BUCKET"))
		ref.Region = strings.TrimSpace(os.Getenv("AWS_REGION"))
		ref.Endpoint = strings.TrimSpace(os.Getenv("S3_ENDPOINT"))
	}
	return ref
}

// startEmbeddedGlobalCP runs the control plane in this process, on
// loopback, with no TLS.
//
// DEV ONLY, and the loopback bind is what enforces that rather than a
// comment: an embedded CP cannot be reached from another machine even if an
// operator sets the flag in production by mistake. Stage 4 is the gate for
// a real cross-network deployment; until then this is how the cross-cluster
// path is exercised at all, by running two coordinators against one of
// these.
func startEmbeddedGlobalCP(port int, logger *log.Logger) (*grpc.Server, error) {
	lis, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
	if err != nil {
		return nil, fmt.Errorf("listen on loopback %d: %w", port, err)
	}
	svc := globalcp.NewService(globalcp.Config{
		Logger: log.New(os.Stdout, "[GLOBALCP] ", log.LstdFlags),
		// No certificates in embedded mode, so identity comes from the
		// header. Safe only because the listener is loopback-bound.
		AllowInsecureIdentity: true,
	})
	srv := grpc.NewServer()
	pb.RegisterGlobalControlPlaneServer(srv, svc)
	go func() {
		if err := srv.Serve(lis); err != nil {
			logger.Printf("Embedded global control plane stopped: %v", err)
		}
	}()
	logger.Printf("DEV: embedded global control plane on 127.0.0.1:%d (no TLS, loopback only)", port)
	return srv, nil
}

type snapshotCloudStore interface {
	Read(ctx context.Context, path string) ([]byte, error)
	Write(ctx context.Context, path string, data []byte) error
}

// buildStore returns an etcd-backed store if endpoints are configured, otherwise
// the legacy in-memory store. Endpoints may be comma-separated.
func buildStore(ctx context.Context, endpoints, prefix string, leaseTTL int64, dialTimeout time.Duration, logger *log.Logger) (coordinator.Store, error) {
	endpoints = strings.TrimSpace(endpoints)
	if endpoints == "" {
		logger.Printf("Coordinator store: in-memory (set -etcd-endpoints for HA)")
		return coordinator.NewInMemoryStore(), nil
	}
	eps := make([]string, 0)
	for _, ep := range strings.Split(endpoints, ",") {
		if e := strings.TrimSpace(ep); e != "" {
			eps = append(eps, e)
		}
	}
	if len(eps) == 0 {
		return nil, fmt.Errorf("etcd endpoints flag set but no valid endpoints parsed: %q", endpoints)
	}
	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   eps,
		DialTimeout: dialTimeout,
		Context:     ctx,
	})
	if err != nil {
		return nil, fmt.Errorf("etcd client: %w", err)
	}
	logger.Printf("Coordinator store: etcd endpoints=%v prefix=%s peer-lease-ttl=%ds", eps, prefix, leaseTTL)
	return coordinator.NewEtcdStore(cli, coordinator.EtcdStoreConfig{
		Prefix:          prefix,
		PeerLeaseTTLSec: leaseTTL,
	}), nil
}

func envInt(name string, fallback int) int {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(v)
	if err != nil || parsed <= 0 {
		return fallback
	}
	return parsed
}

func envBool(name string, fallback bool) bool {
	v := strings.TrimSpace(os.Getenv(name))
	if v == "" {
		return fallback
	}
	parsed, err := strconv.ParseBool(v)
	if err != nil {
		return fallback
	}
	return parsed
}

func newSnapshotCloudStoreFromEnv(logger *log.Logger) snapshotCloudStore {
	provider := strings.ToLower(strings.TrimSpace(os.Getenv("CLOUD_PROVIDER")))
	if provider == "" {
		logger.Printf("Coordinator snapshot cloud store disabled: CLOUD_PROVIDER is empty")
		return nil
	}
	timeout := 30 * time.Second

	var (
		store cache.TierStorage
		err   error
	)

	switch provider {
	case "azure":
		account := strings.TrimSpace(os.Getenv("AZURE_STORAGE_ACCOUNT"))
		key := strings.TrimSpace(os.Getenv("AZURE_STORAGE_KEY"))
		container := strings.TrimSpace(os.Getenv("AZURE_CONTAINER_NAME"))
		if account == "" || key == "" || container == "" {
			logger.Printf("Coordinator snapshot cloud store disabled: missing Azure credentials or container")
			return nil
		}
		store, err = cache.NewAzureStorage(
			account,
			key,
			container,
			timeout,
			envInt("FUSE_AZURE_DOWNLOAD_CONCURRENCY", 8),
			int64(envInt("FUSE_AZURE_DOWNLOAD_BLOCK_SIZE_MB", 4))*1024*1024,
			int64(envInt("FUSE_AZURE_PARALLEL_DOWNLOAD_MIN_SIZE_MB", 8))*1024*1024,
		)
	case "gcp":
		bucket := strings.TrimSpace(os.Getenv("GCP_BUCKET"))
		if bucket == "" {
			logger.Printf("Coordinator snapshot cloud store disabled: GCP_BUCKET is empty")
			return nil
		}
		store, err = cache.NewGCPStorage(bucket, timeout)
	case "s3":
		store, err = cache.NewCloudStorageWithTuning(
			strings.TrimSpace(os.Getenv("S3_BUCKET")),
			strings.TrimSpace(os.Getenv("S3_REGION")),
			timeout,
			cache.S3TransferTuning{
				DownloadConcurrency: envInt("FUSE_S3_DOWNLOAD_CONCURRENCY", 32),
				DownloadPartSize:    int64(envInt("FUSE_S3_DOWNLOAD_PART_SIZE_MB", 8)) * 1024 * 1024,
				UploadConcurrency:   envInt("FUSE_S3_UPLOAD_CONCURRENCY", 16),
				UploadPartSize:      int64(envInt("FUSE_S3_UPLOAD_PART_SIZE_MB", 8)) * 1024 * 1024,
				Endpoint:            strings.TrimSpace(os.Getenv("S3_ENDPOINT")),
				ForcePathStyle:      envBool("FUSE_S3_FORCE_PATH_STYLE", false),
			},
		)
	default:
		logger.Printf("Coordinator snapshot cloud store disabled: unsupported provider %q", provider)
		return nil
	}
	if err != nil {
		logger.Printf("Coordinator snapshot cloud store initialization failed: %v", err)
		return nil
	}
	// Must match the clients' scope: the coordinator reads and writes the same
	// objects they do, so a prefix set on one side only makes every snapshot
	// read miss.
	keyPrefix := strings.TrimSpace(os.Getenv("FUSE_CLOUD_KEY_PREFIX"))
	cache.ScopeCloudKeys(store, keyPrefix)
	if keyPrefix != "" {
		logger.Printf("Coordinator snapshot cloud store key prefix=%s", keyPrefix)
	}
	logger.Printf("Coordinator snapshot cloud store enabled for provider=%s", provider)
	return store
}

func setupRoutes(mux *http.ServeMux, coordinatorService *coordinator.CoordinatorService, snapshotStore snapshotCloudStore) {
	// Register peer endpoint
	mux.HandleFunc("/api/peers/register", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var peer coordinator.PeerInfo
		if err := parseJSONRequest(r, &peer); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		if err := coordinatorService.RegisterPeer(r.Context(), &peer); err != nil {
			http.Error(w, "Failed to register peer", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	})

	// Get peers endpoint
	mux.HandleFunc("/api/peers", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		requesterID := r.URL.Query().Get("requester_id")
		peers, err := coordinatorService.GetPeers(r.Context(), requesterID)
		if err != nil {
			http.Error(w, "Failed to get peers", http.StatusInternalServerError)
			return
		}

		writeJSONResponse(w, peers)
	})

	// Update peer status endpoint
	mux.HandleFunc("/api/peers/status", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPut {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		var request struct {
			PeerID             string    `json:"peer_id"`
			Status             string    `json:"status"`
			AvailableSpace     int64     `json:"available_space"`
			UsedSpace          int64     `json:"used_space"`
			NetworkSpeedMBps   float64   `json:"network_speed_mbps,omitempty"`
			NetworkLatencyMs   float64   `json:"network_latency_ms,omitempty"`
			NetworkProbeBytes  int64     `json:"network_probe_bytes,omitempty"`
			NetworkProbeTarget string    `json:"network_probe_target,omitempty"`
			NetworkProbedAt    time.Time `json:"network_probed_at,omitempty"`
		}

		if err := parseJSONRequest(r, &request); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var metrics *coordinator.PeerNetworkMetrics
		if request.NetworkSpeedMBps > 0 || request.NetworkLatencyMs > 0 || request.NetworkProbeBytes > 0 || strings.TrimSpace(request.NetworkProbeTarget) != "" || !request.NetworkProbedAt.IsZero() {
			metrics = &coordinator.PeerNetworkMetrics{
				SpeedMBps:   request.NetworkSpeedMBps,
				LatencyMs:   request.NetworkLatencyMs,
				ProbeBytes:  request.NetworkProbeBytes,
				ProbeTarget: strings.TrimSpace(request.NetworkProbeTarget),
				ProbedAt:    request.NetworkProbedAt,
			}
		}
		err := coordinatorService.UpdatePeerStatusWithNetwork(r.Context(), request.PeerID, request.Status, request.AvailableSpace, request.UsedSpace, metrics)
		if err != nil {
			http.Error(w, "Failed to update peer status", http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"success": true}`))
	})

	// In-flight fetch leases: cross-node single-flight for origin pulls
	// (thundering-herd Phase 2). POST acquires/renews, DELETE releases.
	mux.HandleFunc("/api/fetch-lease", func(w http.ResponseWriter, r *http.Request) {
		var request struct {
			Key        string `json:"key"`
			PeerID     string `json:"peer_id"`
			TTLSeconds int64  `json:"ttl_seconds,omitempty"`
		}
		if err := parseJSONRequest(r, &request); err != nil || request.Key == "" || request.PeerID == "" {
			http.Error(w, "key and peer_id are required", http.StatusBadRequest)
			return
		}

		switch r.Method {
		case http.MethodPost:
			ttl := time.Duration(request.TTLSeconds) * time.Second
			holder, granted, err := coordinatorService.AcquireFetchLease(r.Context(), request.Key, request.PeerID, ttl)
			if err != nil {
				http.Error(w, "Failed to acquire fetch lease", http.StatusInternalServerError)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			json.NewEncoder(w).Encode(map[string]interface{}{
				"holder":  holder,
				"granted": granted,
			})
		case http.MethodDelete:
			if err := coordinatorService.ReleaseFetchLease(r.Context(), request.Key, request.PeerID); err != nil {
				http.Error(w, "Failed to release fetch lease", http.StatusInternalServerError)
				return
			}
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"success": true}`))
		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// File location endpoint (GET and PUT)
	mux.HandleFunc("/api/files/location", func(w http.ResponseWriter, r *http.Request) {
		switch r.Method {
		case http.MethodGet:
			filePath := r.URL.Query().Get("path")
			if filePath == "" {
				http.Error(w, "File path is required", http.StatusBadRequest)
				return
			}

			locations, err := coordinatorService.GetFileLocation(r.Context(), filePath)
			if err != nil {
				http.Error(w, "Failed to get file location", http.StatusInternalServerError)
				return
			}

			writeJSONResponse(w, locations)

		case http.MethodPut:
			var location coordinator.FileLocation
			if err := parseJSONRequest(r, &location); err != nil {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}

			if err := coordinatorService.UpdateFileLocation(r.Context(), &location); err != nil {
				http.Error(w, "Failed to update file location", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"success": true}`))

		case http.MethodDelete:
			filePath := r.URL.Query().Get("path")
			if filePath == "" {
				http.Error(w, "File path is required", http.StatusBadRequest)
				return
			}
			wholePath := r.URL.Query().Get("whole_path") == "true"
			peerID := r.URL.Query().Get("peer_id")
			if !wholePath && peerID == "" {
				http.Error(w, "peer_id is required unless whole_path is set", http.StatusBadRequest)
				return
			}

			if err := coordinatorService.DeleteFileLocation(r.Context(), filePath, peerID, wholePath); err != nil {
				http.Error(w, "Failed to delete file location", http.StatusInternalServerError)
				return
			}

			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"success": true}`))

		default:
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		}
	})

	// File metadata listing endpoint (GET)
	mux.HandleFunc("/api/files/locations", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		prefix := r.URL.Query().Get("prefix")
		locations, err := coordinatorService.ListFileLocations(r.Context(), prefix)
		if err != nil {
			http.Error(w, "Failed to list file locations", http.StatusInternalServerError)
			return
		}

		writeJSONResponse(w, locations)
	})

	// Full world view endpoint
	mux.HandleFunc("/api/worldview", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		view, err := coordinatorService.GetWorldView(r.Context(), prefix)
		if err != nil {
			http.Error(w, "Failed to build world view", http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, view)
	})

	// Seed a file path to a percentage of active peers.
	mux.HandleFunc("/api/cache/seed", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req coordinator.SeedCacheRequest
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		result, err := coordinatorService.SeedPathToPeers(r.Context(), req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to seed path: %v", err), http.StatusBadRequest)
			return
		}
		writeJSONResponse(w, result)
	})

	// Snapshot: write a manifest of the file paths the coordinator currently
	// tracks (path + size + chunking info) to local disk and/or cloud. This is
	// a portable cache-restore plan, not a full state dump.
	mux.HandleFunc("/api/snapshot", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Path         string `json:"path"`
			PersistCloud *bool  `json:"persist_cloud,omitempty"`
			CloudPath    string `json:"cloud_path,omitempty"`
		}
		if r.ContentLength > 0 {
			if err := parseJSONRequest(r, &req); err != nil {
				http.Error(w, "Invalid request", http.StatusBadRequest)
				return
			}
		}
		path := req.Path
		if path == "" {
			path = "coordinator_manifest.json"
		}

		manifest, err := coordinatorService.BuildManifest(r.Context())
		if err != nil {
			http.Error(w, "Failed to build manifest", http.StatusInternalServerError)
			return
		}
		data, err := json.MarshalIndent(manifest, "", "  ")
		if err != nil {
			http.Error(w, "Failed to serialize manifest", http.StatusInternalServerError)
			return
		}

		tmp := path + ".tmp"
		if err := os.WriteFile(tmp, data, 0644); err != nil {
			http.Error(w, "Failed to write manifest", http.StatusInternalServerError)
			return
		}
		if err := os.Rename(tmp, path); err != nil {
			http.Error(w, "Failed to publish manifest", http.StatusInternalServerError)
			return
		}

		persistCloud := (req.PersistCloud != nil && *req.PersistCloud) || strings.TrimSpace(req.CloudPath) != ""
		cloudPath := strings.TrimSpace(req.CloudPath)
		if persistCloud {
			if snapshotStore == nil {
				http.Error(w, "Cloud store is not configured", http.StatusNotImplemented)
				return
			}
			if cloudPath == "" {
				cloudPath = fmt.Sprintf("manifests/coordinator/%d.json", time.Now().Unix())
			}
			if err := snapshotStore.Write(r.Context(), cloudPath, data); err != nil {
				http.Error(w, "Failed to persist manifest to cloud", http.StatusInternalServerError)
				return
			}
		}

		resp := map[string]interface{}{
			"success":     true,
			"path":        path,
			"total_files": len(manifest.Files),
		}
		if persistCloud {
			resp["persisted_to_cloud"] = true
			resp["cloud_path"] = cloudPath
		}
		writeJSONResponse(w, resp)
	})

	// Restore: read a manifest from disk or cloud, then for each file fetch the
	// bytes from cloud (durable tier) and seed them onto a percentage of active
	// peers, matching SeedPathToPeers semantics. Chunked files are fetched and
	// seeded chunk-by-chunk using the <path>_chunk_<i> key convention.
	mux.HandleFunc("/api/restore", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req struct {
			Path           string `json:"path"`
			CloudPath      string `json:"cloud_path,omitempty"`
			SeedPercentage int    `json:"seed_percentage,omitempty"`
			TimeoutSeconds int    `json:"timeout_seconds,omitempty"`
			Concurrency    int    `json:"concurrency,omitempty"`
		}
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}

		var manifestBytes []byte
		cloudPath := strings.TrimSpace(req.CloudPath)
		switch {
		case cloudPath != "":
			if snapshotStore == nil {
				http.Error(w, "Cloud store is not configured", http.StatusNotImplemented)
				return
			}
			b, err := snapshotStore.Read(r.Context(), cloudPath)
			if err != nil {
				http.Error(w, "Failed to read cloud manifest", http.StatusInternalServerError)
				return
			}
			manifestBytes = b
		case strings.TrimSpace(req.Path) != "":
			b, err := os.ReadFile(req.Path)
			if err != nil {
				http.Error(w, "Failed to read local manifest", http.StatusInternalServerError)
				return
			}
			manifestBytes = b
		default:
			http.Error(w, "path or cloud_path is required", http.StatusBadRequest)
			return
		}

		var manifest coordinator.FileManifest
		if err := json.Unmarshal(manifestBytes, &manifest); err != nil {
			http.Error(w, "Invalid manifest JSON", http.StatusBadRequest)
			return
		}

		if snapshotStore == nil {
			http.Error(w, "Cloud store is not configured (required to fetch file bytes)", http.StatusNotImplemented)
			return
		}
		result, err := coordinatorService.RestoreFromManifest(r.Context(), &manifest, snapshotStore, coordinator.RestoreOptions{
			SeedPercentage: req.SeedPercentage,
			TimeoutSeconds: req.TimeoutSeconds,
			Concurrency:    req.Concurrency,
		})
		if err != nil {
			http.Error(w, fmt.Sprintf("Restore failed: %v", err), http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, result)
	})

	// Cluster-wide declarative warmup: select peers by node IDs / labels /
	// percentage and trigger an async prefix warm on each (the peer-side
	// engine is the same one CSI warmup=full sessions use).
	mux.HandleFunc("/api/warm", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var req coordinator.WarmRequest
		if err := parseJSONRequest(r, &req); err != nil {
			http.Error(w, "Invalid request", http.StatusBadRequest)
			return
		}
		result, err := coordinatorService.WarmPeers(r.Context(), req)
		if err != nil {
			http.Error(w, fmt.Sprintf("Warm fan-out failed: %v", err), http.StatusBadRequest)
			return
		}
		writeJSONResponse(w, result)
	})

	// Cluster-wide residency: which nodes currently hold a prefix, measured by
	// asking each node's own disk rather than reading location metadata (which
	// over-claims, because eviction never retracts it). This is the routing
	// signal an external scheduler scores on.
	mux.HandleFunc("/api/residency", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}
		prefix := r.URL.Query().Get("prefix")
		if strings.TrimSpace(prefix) == "" {
			http.Error(w, "prefix is required", http.StatusBadRequest)
			return
		}
		// Peers may require X-API-Key. Forward the caller's own key rather than
		// holding one here: it is the same cluster key, and a caller that could
		// not authenticate to a peer directly should not get to do so via the
		// coordinator.
		apiKey := r.Header.Get("X-API-Key")
		if apiKey == "" {
			apiKey = r.URL.Query().Get("api_key")
		}
		res, err := coordinatorService.PrefixResidency(r.Context(), prefix, apiKey)
		if err != nil {
			http.Error(w, fmt.Sprintf("Residency query failed: %v", err), http.StatusInternalServerError)
			return
		}
		writeJSONResponse(w, res)
	})

	// Get statistics endpoint
	mux.HandleFunc("/api/stats", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		stats := coordinatorService.GetPeerStats()
		writeJSONResponse(w, stats)
	})

	// Health check endpoint
	mux.HandleFunc("/api/health", func(w http.ResponseWriter, r *http.Request) {
		health := map[string]interface{}{
			"status":    "healthy",
			"timestamp": time.Now().Unix(),
			"service":   "coordinator",
		}
		writeJSONResponse(w, health)
	})
}

func parseJSONRequest(r *http.Request, v interface{}) error {
	return json.NewDecoder(r.Body).Decode(v)
}

func writeJSONResponse(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(v); err != nil {
		log.Printf("Failed to write response: %v", err)
	}
}

// Command globalcp runs the global control plane: the service that makes a
// published subset of each cluster's cache discoverable to other clusters.
//
// It is deployable in its own Kubernetes cluster, reachable by every
// participating cluster's coordinator. It never holds file bytes and never
// connects to a cluster -- coordinators dial out to it and hold the stream,
// so a private cluster needs no inbound rule.
//
// SECURITY: this service authenticates clusters by mTLS. Pass -tls-cert,
// -tls-key and -tls-ca and it requires a verified client certificate on
// every connection, taking each cluster's identity from that certificate's
// first DNS SAN.
//
// With no TLS flags it refuses to start unless -allow-insecure-identity is
// also given, which lets any caller name itself via a header. That
// combination is for loopback dev only. Refusing by default rather than
// warning is deliberate: a warning in a startup log is invisible in
// practice, and the failure it guards against -- a public port where any
// caller can publish rows as any cluster -- is not recoverable after the
// fact.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"

	"fuse-client/internal/globalcp"
	"fuse-client/internal/mtls"
	pb "fuse-client/internal/pb"
)

// str applies the one precedence rule every setting on the global path
// uses: flag wins, env is the default.
func str(flagVal, env string) string {
	if v := strings.TrimSpace(flagVal); v != "" {
		return v
	}
	return strings.TrimSpace(os.Getenv(env))
}

func main() {
	var (
		port          = flag.Int("port", 8090, "Port for the read-only HTTP observability server")
		grpcPort      = flag.Int("grpc-port", 9090, "Port for the GlobalControlPlane gRPC server")
		allowInsecure = flag.Bool("allow-insecure-identity", false, "Accept header-declared cluster identity on connections with no verified client certificate (DEV ONLY: any caller can then claim any cluster ID)")
		maxPrefixes   = flag.Int("max-prefixes", 64, "Maximum publish/subscribe prefixes one cluster may register")
		tlsCert       = flag.String("tls-cert", "", "PEM certificate this server presents to clusters")
		tlsKey        = flag.String("tls-key", "", "PEM private key for -tls-cert")
		tlsCA         = flag.String("tls-ca", "", "PEM CA bundle that signs cluster client certificates; clusters not signed by it cannot connect")
		help          = flag.Bool("help", false, "Show help")
	)
	flag.Parse()
	if *help {
		flag.Usage()
		return
	}

	logger := log.New(os.Stdout, "[GLOBALCP] ", log.LstdFlags)

	// Flag wins, env is the default -- one rule for every new setting.
	insecureIdentity := *allowInsecure
	if !insecureIdentity && strings.EqualFold(strings.TrimSpace(os.Getenv("FUSE_GLOBALCP_ALLOW_INSECURE_IDENTITY")), "true") {
		insecureIdentity = true
	}

	tlsCfg := mtls.Config{
		CertFile: str(*tlsCert, "FUSE_GLOBALCP_TLS_CERT"),
		KeyFile:  str(*tlsKey, "FUSE_GLOBALCP_TLS_KEY"),
		CAFile:   str(*tlsCA, "FUSE_GLOBALCP_TLS_CA"),
	}
	if err := tlsCfg.Validate(); err != nil {
		logger.Fatalf("TLS configuration: %v", err)
	}

	// The gate. Without mTLS there is no way to know who is calling, so the
	// only safe default is not to run -- an operator who wants the insecure
	// mode has to name it.
	if !tlsCfg.Enabled() && !insecureIdentity {
		logger.Fatalf("refusing to start without mTLS: pass -tls-cert/-tls-key/-tls-ca, " +
			"or -allow-insecure-identity for a loopback-only dev deployment")
	}
	if tlsCfg.Enabled() && insecureIdentity {
		// Not fatal, but it is almost certainly a mistake: a connection
		// that presents no certificate cannot get past
		// RequireAndVerifyClientCert, so the flag buys nothing and reads
		// as if identity were optional.
		logger.Printf("NOTE: -allow-insecure-identity has no effect with mTLS on; " +
			"client certificates are required by the transport.")
	}

	svc := globalcp.NewService(globalcp.Config{
		Store:                 globalcp.NewMemStore(),
		Logger:                logger,
		MaxPublishPrefixes:    *maxPrefixes,
		AllowInsecureIdentity: insecureIdentity,
	})

	grpcListener, err := net.Listen("tcp", fmt.Sprintf(":%d", *grpcPort))
	if err != nil {
		logger.Fatalf("Failed to listen on gRPC port %d: %v", *grpcPort, err)
	}
	var serverOpts []grpc.ServerOption
	if tlsCfg.Enabled() {
		creds, cerr := tlsCfg.ServerCredentials()
		if cerr != nil {
			logger.Fatalf("Failed to build server TLS credentials: %v", cerr)
		}
		serverOpts = append(serverOpts, grpc.Creds(creds))
		logger.Printf("mTLS enabled: cluster identity comes from the verified client certificate")
	} else {
		logger.Printf("WARNING: insecure identity mode — any caller may claim any cluster ID. Bind to loopback only.")
	}
	grpcSrv := grpc.NewServer(serverOpts...)
	pb.RegisterGlobalControlPlaneServer(grpcSrv, svc)
	go func() {
		logger.Printf("gRPC server listening on :%d", *grpcPort)
		if err := grpcSrv.Serve(grpcListener); err != nil {
			logger.Printf("gRPC server error: %v", err)
		}
	}()

	mux := http.NewServeMux()
	globalcp.RegisterRoutes(mux, svc)
	server := &http.Server{Addr: fmt.Sprintf(":%d", *port), Handler: mux}
	go func() {
		if err := server.ListenAndServe(); err != http.ErrServerClosed {
			logger.Printf("HTTP server error: %v", err)
		}
	}()

	logger.Printf("Global control plane started on HTTP :%d, gRPC :%d", *port, *grpcPort)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan

	logger.Println("Shutting down global control plane...")
	grpcSrv.GracefulStop()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Printf("Server shutdown error: %v", err)
	}
	if err := svc.Store().Close(); err != nil {
		logger.Printf("Store close: %v", err)
	}
	logger.Println("Global control plane stopped")
}

package main

import (
	"flag"
	"log"
	"net/http"
	"time"

	"fuse-client/internal/api"
	"fuse-client/internal/coordinator"
)

// cmdServeAPI runs the Go client's HTTP API (api.Handler over a real
// DefaultCacheManager) on a cache dir, without FUSE, gRPC or registration:
// the part of the client store-front sits in front of. Used by the
// store-front interop and differential tests (rust/store), which need the
// real handlers (PUT digest checks, blob routes, peer reads, health,
// metrics) and cannot run the FUSE-mounting client binary on every host.
//
// With -coord, metadata (published chunk strides) comes from that
// coordinator over HTTP, as the client's would.
func cmdServeAPI(args []string) error {
	fs := flag.NewFlagSet("serve-api", flag.ExitOnError)
	dir := fs.String("dir", "", "cache dir (-nvme)")
	listen := fs.String("listen", "127.0.0.1:8091", "listen address (-port)")
	chunkMB := fs.Int("chunk-mb", 16, "-chunk-size")
	apiKey := fs.String("api-key", "", "-api-key")
	coordAddr := fs.String("coord", "", "coordinator HTTP address")
	peerID := fs.String("peer-id", "store-0", "-peer-id")
	fs.Parse(args)
	var coord coordinator.Coordinator
	if *coordAddr != "" {
		coord = coordinator.NewCoordinatorClient(*coordAddr, 10*time.Second)
	}
	cm, err := newCacheManager(cmOpts{dir: *dir, chunkMB: *chunkMB, coord: coord, localPeerID: *peerID, noBlobFill: true})
	if err != nil {
		return err
	}
	defer cm.Shutdown()
	h := api.NewHandler(cm, coord, *peerID, *apiKey)
	srv := &http.Server{Addr: *listen, Handler: h.SetupRoutes()}
	log.Printf("go api on %s dir=%s", *listen, *dir)
	return srv.ListenAndServe()
}

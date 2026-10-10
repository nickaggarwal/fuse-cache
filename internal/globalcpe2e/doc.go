// Package globalcpe2e is the cross-cluster data-path end-to-end test.
//
// It is the last item in the plan's verification list, and it covers what
// neither of the two shell harnesses can:
//
//   - scripts/ops/e2e-global-mtls.sh drives the real binaries, but only the
//     CONTROL path: who may connect, who may claim which identity, and what
//     survives the control plane dying. It writes no bytes.
//   - scripts/ops/e2e-global-chart.sh proves the charts emit flags and
//     environment the binaries actually read. It also writes no bytes.
//
// What is left is the claim the whole design rests on: a file written in
// cluster A, published only because it reached object storage, becomes
// readable in cluster B -- with no network path between the two data planes.
//
// Why a Go test and not scripts/ops/e2e-global.sh, which the plan named and
// the mTLS harness's header points at. Two hard constraints:
//
//  1. cmd/client is Linux-only (bazil.org/fuse), so a script driving two
//     real clients cannot run on a developer laptop, which is where this
//     has to be runnable to be run at all.
//  2. The plan assumed MinIO for object storage. MinIO's image is not
//     pullable from Docker Hub, quay.io, ghcr.io or ECR Public without
//     credentials, so a script depending on it is a script that skips.
//
// A test that skips proves nothing, so the object store is an in-process S3
// server (s3server.go) instead. Everything ABOVE it is the real thing: the
// real globalcp service, two real globalsync agents over real gRPC, two real
// CoordinatorService instances, and two real CloudStorage backends speaking
// the AWS SDK over HTTP. The seam moved down to the object store, which is
// the one component this feature does not implement.
//
// This lives in its own package rather than internal/cache because the
// federation needs globalsync, and a test inside internal/cache that
// imported it would make the data-plane package depend on the control-plane
// agent for a test's sake.
package globalcpe2e

# Custom llm-d EndpointPicker with the fuse residency plugins compiled in.
#
# llm-d has no dynamic plugin loading — the registry is a package-level map
# populated before the runner starts — so any out-of-tree plugin means shipping
# your own EPP image. It does NOT mean forking llm-d-router: this builds a small
# main() that registers two plugins and then calls the upstream runner, with
# llm-d-router consumed as an ordinary pinned Go module. Every in-tree plugin
# still works, and a config that omits the fuse plugins behaves like stock EPP.
#
# Build from this directory:
#   docker build -t <registry>/fuse-residency-epp:latest .
#   az acr build --registry stargzrepo --image fuse-residency-epp:latest .
#
# Ports, user, and base image mirror upstream Dockerfile.epp so this drops into
# an existing EPP Deployment with only the image changed.
ARG BASE_IMAGE=gcr.io/distroless/static:nonroot

FROM --platform=${BUILDPLATFORM} golang:1.26.5 AS go-builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /workspace

# go.mod/go.sum first so the dependency layer survives source edits. This one
# is worth caring about: the module graph pulls in all of llm-d-router's
# dependencies, which is a slow download.
COPY go.mod go.sum ./
RUN go mod download

COPY cmd/ cmd/
COPY pkg/ pkg/

ARG LDFLAGS="-s -w"
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build \
    -ldflags="${LDFLAGS}" -o bin/epp ./cmd/epp

FROM ${BASE_IMAGE}

WORKDIR /

COPY --from=go-builder /workspace/bin/epp /app/epp

USER 65532:65532

# gRPC ext-proc, health, metrics — same as upstream EPP.
EXPOSE 9002
EXPOSE 9003
EXPOSE 9090

ENTRYPOINT ["/app/epp"]

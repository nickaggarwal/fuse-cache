# syntax=docker/dockerfile:1.7

# Build stage
FROM --platform=$BUILDPLATFORM golang:1.20 AS builder

WORKDIR /app

ARG TARGETOS=linux
ARG TARGETARCH=amd64

COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN --mount=type=cache,target=/go/pkg/mod \
    --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /bin/coordinator cmd/coordinator/main.go && \
    CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH go build -trimpath -ldflags="-s -w" -o /bin/client cmd/client/main.go

# Runtime stage
FROM ubuntu:22.04

RUN apt-get update && \
    apt-get install -y --no-install-recommends fuse3 ca-certificates && \
    rm -rf /var/lib/apt/lists/*

COPY --from=builder /bin/coordinator /usr/local/bin/coordinator
COPY --from=builder /bin/client /usr/local/bin/client

# Allow non-root FUSE mounts (used inside container)
RUN echo "user_allow_other" >> /etc/fuse.conf

ENTRYPOINT []
CMD ["coordinator"]

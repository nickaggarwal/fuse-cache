# Build stage
FROM golang:1.20 AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/coordinator cmd/coordinator/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /bin/client cmd/client/main.go

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

# FUSE Client Makefile

# Variables
BINARY_DIR := bin
COORDINATOR_BINARY := $(BINARY_DIR)/coordinator
CLIENT_BINARY := $(BINARY_DIR)/client
PROTO_DIR := internal/proto
GO_FILES := $(shell find . -name "*.go" -not -path "./vendor/*")

# Default target
.PHONY: all
all: build

# Create binary directory
$(BINARY_DIR):
	mkdir -p $(BINARY_DIR)

# Build targets
.PHONY: build
build: $(COORDINATOR_BINARY) $(CLIENT_BINARY)

$(COORDINATOR_BINARY): $(BINARY_DIR) $(GO_FILES)
	go build -o $(COORDINATOR_BINARY) cmd/coordinator/main.go

$(CLIENT_BINARY): $(BINARY_DIR) $(GO_FILES)
	go build -o $(CLIENT_BINARY) cmd/client/main.go

# Install dependencies
.PHONY: deps
deps:
	go mod tidy
	go mod download

# Clean build artifacts
.PHONY: clean
clean:
	rm -rf $(BINARY_DIR)
	go clean

# Run targets
.PHONY: run-coordinator
run-coordinator: $(COORDINATOR_BINARY)
	./$(COORDINATOR_BINARY) -port 8080

.PHONY: run-client
run-client: $(CLIENT_BINARY)
	./$(CLIENT_BINARY) -mount /tmp/fuse-client -nvme /tmp/nvme-cache -port 8081

.PHONY: run-client-1
run-client-1: $(CLIENT_BINARY)
	./$(CLIENT_BINARY) -mount /tmp/fuse-client1 -nvme /tmp/nvme-cache1 -port 8081 -peer-id client-1

.PHONY: run-client-2
run-client-2: $(CLIENT_BINARY)
	./$(CLIENT_BINARY) -mount /tmp/fuse-client2 -nvme /tmp/nvme-cache2 -port 8082 -peer-id client-2

.PHONY: run-client-3
run-client-3: $(CLIENT_BINARY)
	./$(CLIENT_BINARY) -mount /tmp/fuse-client3 -nvme /tmp/nvme-cache3 -port 8083 -peer-id client-3

# Development targets
.PHONY: fmt
fmt:
	go fmt ./...

.PHONY: vet
vet:
	go vet ./...

.PHONY: test
test:
	go test ./...

.PHONY: test-verbose
test-verbose:
	go test -v ./...

.PHONY: test-race
test-race:
	go test -race ./...

# Docker targets (for future use)
.PHONY: docker-build
docker-build:
	docker build -t fuse-client:latest .

.PHONY: docker-run-coordinator
docker-run-coordinator:
	docker run -d -p 8080:8080 --name fuse-coordinator fuse-client:latest coordinator

.PHONY: docker-run-client
docker-run-client:
	docker run -d -p 8081:8081 --name fuse-client-1 fuse-client:latest client

# Setup development environment
.PHONY: dev-setup
dev-setup:
	mkdir -p /tmp/fuse-client1
	mkdir -p /tmp/fuse-client2
	mkdir -p /tmp/fuse-client3
	mkdir -p /tmp/nvme-cache1
	mkdir -p /tmp/nvme-cache2
	mkdir -p /tmp/nvme-cache3

# Cleanup development environment
.PHONY: dev-cleanup
dev-cleanup:
	rm -rf /tmp/fuse-client*
	rm -rf /tmp/nvme-cache*

# Start full development environment
.PHONY: dev-start
dev-start: dev-setup build
	@echo "Starting coordinator..."
	./$(COORDINATOR_BINARY) -port 8080 -grpc-port 9080 &
	@echo "Waiting for coordinator to start..."
	sleep 2
	@echo "Starting client 1..."
	./$(CLIENT_BINARY) -mount /tmp/fuse-client1 -nvme /tmp/nvme-cache1 -port 8081 -grpc-port 9081 -coordinator-grpc localhost:9080 -peer-id client-1 &
	@echo "Starting client 2..."
	./$(CLIENT_BINARY) -mount /tmp/fuse-client2 -nvme /tmp/nvme-cache2 -port 8082 -grpc-port 9082 -coordinator-grpc localhost:9080 -peer-id client-2 &
	@echo "Starting client 3..."
	./$(CLIENT_BINARY) -mount /tmp/fuse-client3 -nvme /tmp/nvme-cache3 -port 8083 -grpc-port 9083 -coordinator-grpc localhost:9080 -peer-id client-3 &
	@echo "Development environment started!"
	@echo "Coordinator: http://localhost:8080"
	@echo "Client 1: http://localhost:8081"
	@echo "Client 2: http://localhost:8082"
	@echo "Client 3: http://localhost:8083"

# Stop development environment
.PHONY: dev-stop
dev-stop:
	@echo "Stopping all processes..."
	-pkill -f "$(COORDINATOR_BINARY)"
	-pkill -f "$(CLIENT_BINARY)"
	@echo "Development environment stopped!"

# Generate protobuf files
.PHONY: proto
proto:
	mkdir -p internal/pb
	protoc --proto_path=proto \
	  --go_out=internal/pb --go_opt=paths=source_relative \
	  --go-grpc_out=internal/pb --go-grpc_opt=paths=source_relative \
	  proto/coordinator.proto proto/peer.proto

# Check code quality
.PHONY: lint
lint:
	golangci-lint run

# Show help
.PHONY: help
help:
	@echo "Available targets:"
	@echo "  build         - Build both coordinator and client binaries"
	@echo "  clean         - Clean build artifacts"
	@echo "  deps          - Install dependencies"
	@echo "  fmt           - Format code"
	@echo "  vet           - Run go vet"
	@echo "  test          - Run tests"
	@echo "  test-verbose  - Run tests with verbose output"
	@echo "  test-race     - Run tests with race detector"
	@echo "  lint          - Run linter"
	@echo ""
	@echo "  run-coordinator - Run coordinator"
	@echo "  run-client      - Run single client"
	@echo "  run-client-1    - Run client 1"
	@echo "  run-client-2    - Run client 2"
	@echo "  run-client-3    - Run client 3"
	@echo ""
	@echo "  dev-setup     - Setup development environment"
	@echo "  dev-cleanup   - Cleanup development environment"
	@echo "  dev-start     - Start full development environment"
	@echo "  dev-stop      - Stop development environment"
	@echo ""
	@echo "  docker-build  - Build Docker image"
	@echo ""
	@echo "  k8s-devbox-install-tools - Install helm/kind/kubectl tools locally"
	@echo "  k8s-devbox-create - Create local kind cluster for Kubernetes testing"
	@echo "  k8s-devbox-deploy - Deploy Helm chart into the local dev cluster"
	@echo "  k8s-devbox-status - Show local dev cluster status"
	@echo "  k8s-devbox-delete - Delete local dev cluster"
	@echo "  help          - Show this help message"


.PHONY: k8s-devbox-install-tools
k8s-devbox-install-tools:
	./scripts/devbox/install-tools.sh all

.PHONY: k8s-devbox-create
k8s-devbox-create:
	./scripts/devbox/devbox.sh create

.PHONY: k8s-devbox-deploy
k8s-devbox-deploy:
	./scripts/devbox/devbox.sh deploy

.PHONY: k8s-devbox-status
k8s-devbox-status:
	./scripts/devbox/devbox.sh status

.PHONY: k8s-devbox-delete
k8s-devbox-delete:
	./scripts/devbox/devbox.sh delete

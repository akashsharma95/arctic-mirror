# Arctic Mirror Makefile
# Provides commands to build, test, and run the application

.PHONY: help build test test-race clean run docker-build docker-run docker-stop docker-clean lint fmt deps install-tools status dev rebuild compactor integration-test

# Default target
help:
	@echo "Arctic Mirror - Available Commands:"
	@echo ""
	@echo "Building:"
	@echo "  build          - Build the main application binary"
	@echo "  compactor      - Build the compactor binary"
	@echo "  clean          - Clean build artifacts"
	@echo ""
	@echo "Testing:"
	@echo "  test           - Run unit tests"
	@echo "  test-race      - Run tests with race detection"
	@echo "  integration-test - Run end-to-end integration tests"
	@echo ""
	@echo "Running:"
	@echo "  run            - Run the main application"
	@echo "  dev            - Run in development mode with auto-reload"
	@echo ""
	@echo "Docker:"
	@echo "  docker-build   - Build Docker image"
	@echo "  docker-run     - Run with Docker Compose"
	@echo "  docker-stop    - Stop Docker services"
	@echo "  docker-clean   - Clean Docker resources"
	@echo ""
	@echo "Code Quality:"
	@echo "  lint           - Run linter"
	@echo "  fmt            - Format code"
	@echo ""
	@echo "Development:"
	@echo "  deps           - Download dependencies"
	@echo "  install-tools  - Install development tools"
	@echo "  status         - Show project status"
	@echo "  rebuild        - Clean and rebuild everything"

# Build the main application
build:
	@echo "Building Arctic Mirror..."
	go build -o arctic-mirror ./main.go
	@echo "Build complete: arctic-mirror"

# Build the compactor binary
compactor:
	@echo "Building Arctic Mirror Compactor..."
	go build -o arctic-compactor ./cmd/compactor/main.go
	@echo "Build complete: arctic-compactor"

# Run tests
test:
	@echo "Running unit tests..."
	go test -v ./...
	@echo "Tests complete"

# Run tests with race detection
test-race:
	@echo "Running tests with race detection..."
	go test -race -v ./...
	@echo "Race detection tests complete"

# Run integration tests
integration-test:
	@echo "Running integration tests..."
	@echo "Note: This requires a running PostgreSQL instance"
	@echo "Set TEST_POSTGRES_HOST and TEST_POSTGRES_PORT environment variables if needed"
	go test -v -tags=integration ./integration/...
	@echo "Integration tests complete"

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -f arctic-mirror arctic-compactor
	go clean
	@echo "Clean complete"

# Run the application
run: build
	@echo "Starting Arctic Mirror..."
	./arctic-mirror -config config.yaml

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t arctic-mirror .
	@echo "Docker build complete"

# Run with Docker Compose
docker-run:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d
	@echo "Services started. Check status with: make status"

# Stop Docker services
docker-stop:
	@echo "Stopping Docker services..."
	docker-compose down
	@echo "Services stopped"

# Clean Docker resources
docker-clean:
	@echo "Cleaning Docker resources..."
	docker-compose down -v --remove-orphans
	docker system prune -f
	@echo "Docker cleanup complete"

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run
	@echo "Linting complete"

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	@echo "Code formatting complete"

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	@echo "Dependencies downloaded"

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	@echo "Tools installed"

# Show project status
status:
	@echo "Project Status:"
	@echo "==============="
	@echo "Go version: $(shell go version)"
	@echo "Go modules: $(shell go list -m all | wc -l) modules"
	@echo "Source files: $(shell find . -name "*.go" | wc -l) Go files"
	@echo "Test files: $(shell find . -name "*_test.go" | wc -l) test files"
	@echo ""
	@echo "Docker Status:"
	@docker-compose ps 2>/dev/null || echo "Docker Compose not running"
	@echo ""
	@echo "Build Status:"
	@if [ -f arctic-mirror ]; then echo "✓ Main binary: arctic-mirror"; else echo "✗ Main binary: not built"; fi
	@if [ -f arctic-compactor ]; then echo "✓ Compactor binary: arctic-compactor"; else echo "✗ Compactor binary: not built"; fi

# Development mode with auto-reload
dev:
	@echo "Starting development mode..."
	@echo "Install air for auto-reload: go install github.com/cosmtrek/air@latest"
	@if command -v air >/dev/null 2>&1; then \
		air; \
	else \
		echo "Air not found. Install with: go install github.com/cosmtrek/air@latest"; \
		echo "Falling back to manual run..."; \
		make run; \
	fi

# Rebuild everything
rebuild: clean deps build compactor
	@echo "Rebuild complete"
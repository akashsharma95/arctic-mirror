# Arctic Mirror Makefile
# Common development tasks

.PHONY: help build test clean run docker-build docker-run docker-stop docker-clean

# Default target
help:
	@echo "Arctic Mirror - Available targets:"
	@echo "  build        - Build the application"
	@echo "  test         - Run all tests"
	@echo "  test-race    - Run tests with race detection"
	@echo "  clean        - Clean build artifacts"
	@echo "  run          - Run the application locally"
	@echo "  docker-build - Build Docker image"
	@echo "  docker-run   - Run with Docker Compose"
	@echo "  docker-stop  - Stop Docker Compose services"
	@echo "  docker-clean - Clean Docker resources"
	@echo "  lint         - Run linter"
	@echo "  fmt          - Format code"
	@echo "  deps         - Download dependencies"

# Build the application
build:
	@echo "Building Arctic Mirror..."
	go build -o arctic-mirror .
	@echo "Build complete: arctic-mirror"

# Run all tests
test:
	@echo "Running tests..."
	go test ./... -v

# Run tests with race detection
test-race:
	@echo "Running tests with race detection..."
	go test -race ./... -v

# Clean build artifacts
clean:
	@echo "Cleaning build artifacts..."
	rm -f arctic-mirror
	@echo "Clean complete"

# Run the application locally
run: build
	@echo "Running Arctic Mirror locally..."
	./arctic-mirror --config config.yaml --health-port 8080

# Build Docker image
docker-build:
	@echo "Building Docker image..."
	docker build -t arctic-mirror:latest .

# Run with Docker Compose
docker-run:
	@echo "Starting services with Docker Compose..."
	docker-compose up -d
	@echo "Services started. Check logs with: docker-compose logs -f"

# Stop Docker Compose services
docker-stop:
	@echo "Stopping Docker Compose services..."
	docker-compose down

# Clean Docker resources
docker-clean:
	@echo "Cleaning Docker resources..."
	docker-compose down -v --remove-orphans
	docker system prune -f

# Run linter
lint:
	@echo "Running linter..."
	golangci-lint run

# Format code
fmt:
	@echo "Formatting code..."
	go fmt ./...
	goimports -w .

# Download dependencies
deps:
	@echo "Downloading dependencies..."
	go mod download
	go mod tidy

# Install development tools
install-tools:
	@echo "Installing development tools..."
	go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
	go install golang.org/x/tools/cmd/goimports@latest

# Show application status
status:
	@echo "=== Arctic Mirror Status ==="
	@echo "Application:"
	@if [ -f arctic-mirror ]; then echo "  Binary: ✓ Built"; else echo "  Binary: ✗ Not built"; fi
	@echo ""
	@echo "Docker:"
	@if docker ps | grep -q arctic-mirror; then echo "  Running: ✓"; else echo "  Running: ✗"; fi
	@echo ""
	@echo "Health Check:"
	@if curl -s http://localhost:8080/health > /dev/null 2>&1; then echo "  Health: ✓ Available"; else echo "  Health: ✗ Not available"; fi

# Development workflow
dev: deps test build
	@echo "Development workflow complete"

# Full clean and rebuild
rebuild: clean deps test build
	@echo "Full rebuild complete"
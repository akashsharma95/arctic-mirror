# Multi-stage build for Arctic Mirror
FROM golang:1.24-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git ca-certificates tzdata

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

# Build main application
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o arctic-mirror ./main.go

# Build compactor binary
RUN CGO_ENABLED=1 GOOS=linux go build -a -installsuffix cgo -o arctic-compactor ./cmd/compactor/main.go

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1001 -S appgroup && \
    adduser -u 1001 -S appuser -G appgroup

# Create necessary directories
RUN mkdir -p /data/warehouse /app/config && \
    chown -R appuser:appgroup /data /app

# Copy binaries from builder
COPY --from=builder /app/arctic-mirror /app/arctic-compactor /app/

# Copy configuration
COPY --from=builder /app/config.yaml /app/config/

# Set ownership
RUN chown appuser:appgroup /app/arctic-mirror /app/arctic-compactor /app/config.yaml

# Switch to non-root user
USER appuser

# Set working directory
WORKDIR /app

# Expose ports
EXPOSE 5433 8080

# Health check
HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Set entrypoint
ENTRYPOINT ["/app/arctic-mirror"]

# Default command
CMD ["-config", "/app/config/config.yaml"]
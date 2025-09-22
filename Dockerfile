# Builder + Runtime both Debian-based
FROM golang:1.25.0-bookworm AS builder

RUN apt-get update && apt-get install -y --no-install-recommends \
    git build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=1 GOOS=linux go build -o arctic-mirror ./cmd/arctic-mirror

# Final stage (Debian-slim instead of Alpine)
FROM debian:bookworm-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    ca-certificates wget \
    && rm -rf /var/lib/apt/lists/*

RUN addgroup --system appgroup && \
    adduser --system --ingroup appgroup --home /app appuser

RUN mkdir -p /app/warehouse /app/config && \
    chown -R appuser:appgroup /app

COPY --from=builder /app/arctic-mirror /app/
COPY --from=builder /app/config.yaml /app/config/
RUN chown appuser:appgroup /app/arctic-mirror /app/config/config.yaml

USER appuser
WORKDIR /app

EXPOSE 5433 8080

HEALTHCHECK --interval=30s --timeout=10s --start-period=5s --retries=3 \
    CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

ENTRYPOINT ["/app/arctic-mirror"]
CMD ["-config", "/app/config/config.yaml"]

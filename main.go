package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"arctic-mirror/compaction"
	"arctic-mirror/config"
	"arctic-mirror/health"
	"arctic-mirror/metrics"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"
	"encoding/json"
	"net/http"
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to config file")
	healthPort := flag.Int("health-port", 8080, "Port for health check server")
	flag.Parse()

	// Load and validate configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Arctic Mirror with config: %s", *configFile)
	log.Printf("PostgreSQL: %s:%d/%s", cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database)
	log.Printf("Iceberg path: %s", cfg.Iceberg.Path)
	log.Printf("Proxy port: %d", cfg.Proxy.Port)
	log.Printf("Health port: %d", *healthPort)

	// Initialize metrics registry
	metrics.Init()

	// Initialize health monitoring
	healthManager := health.NewManager(cfg)

	// Initialize compactor if enabled and register admin endpoint
	var compactor *compaction.Compactor
	if cfg.Compaction.Enabled {
		parallelism := cfg.Compaction.Parallelism
		if parallelism <= 0 {
			parallelism = 4
		}
		c, err := compaction.NewCompactor(cfg.Iceberg.Path, parallelism)
		if err != nil {
			log.Printf("Warning: Failed to initialize compactor: %v", err)
		} else {
			compactor = c
			// Admin endpoint to trigger compaction
			healthManager.AddHandler("/admin/compact", http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				ctx := r.Context()
				start := time.Now()
				stats, err := compactor.Compact(ctx)
				if err != nil {
					metrics.CompactionErrorsTotal.Inc()
					w.WriteHeader(http.StatusConflict)
					_ = json.NewEncoder(w).Encode(map[string]any{"error": err.Error()})
					return
				}
				// Metrics
				dur := time.Since(start)
				metrics.CompactionRunsTotal.Inc()
				metrics.CompactionDurationSeconds.Observe(dur.Seconds())
				metrics.CompactionFilesProcessedTotal.Add(float64(stats.FilesProcessed))
				metrics.CompactionFilesCompactedTotal.Add(float64(stats.FilesCompacted))
				metrics.CompactionBytesSavedTotal.Add(float64(stats.BytesSaved))
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]any{
					"files_processed": stats.FilesProcessed,
					"files_compacted": stats.FilesCompacted,
					"bytes_saved":     stats.BytesSaved,
					"duration_ms":     dur.Milliseconds(),
				})
			}))
		}
	}

	// Start health check server (registers admin endpoints above)
	if err := healthManager.StartHTTPServer(context.Background(), *healthPort); err != nil {
		log.Printf("Warning: Failed to start health server: %v", err)
	} else {
		log.Printf("Health check server started on port %d", *healthPort)
	}

	// Initialize components
	replicator, err := replication.NewReplicator(cfg)
	if err != nil {
		log.Fatalf("Failed to create replicator: %v", err)
	}

	proxy, err := proxy.NewDuckDBProxy(cfg)
	if err != nil {
		log.Fatalf("Failed to create proxy: %v", err)
	}

	// Register health checkers
	healthManager.RegisterChecker("system", health.NewSystemHealthChecker(cfg))
	healthManager.RegisterChecker("iceberg", health.NewIcebergHealthChecker(cfg.Iceberg.Path))
	
	// Note: PostgreSQL and DuckDB health checkers will be registered after connections are established

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start replication
	go func() {
		log.Println("Starting replication service...")
		if err := replicator.Start(ctx); err != nil {
			log.Printf("Replication error: %v", err)
			cancel()
		}
	}()

	// Start proxy server
	go func() {
		log.Println("Starting proxy server...")
		if err := proxy.Start(ctx); err != nil {
			log.Printf("Proxy error: %v", err)
			cancel()
		}
	}()

	// Start background compaction scheduler if enabled
	if compactor != nil {
		interval := time.Duration(cfg.Compaction.IntervalSeconds) * time.Second
		if interval <= 0 {
			interval = time.Hour
		}
		go func() {
			log.Printf("Starting compaction scheduler with interval %v", interval)
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			for {
				select {
				case <-ctx.Done():
					return
				case <-ticker.C:
					start := time.Now()
					stats, err := compactor.Compact(ctx)
					if err != nil {
						metrics.CompactionErrorsTotal.Inc()
						log.Printf("Compaction error: %v", err)
						continue
					}
					dur := time.Since(start)
					metrics.CompactionRunsTotal.Inc()
					metrics.CompactionDurationSeconds.Observe(dur.Seconds())
					metrics.CompactionFilesProcessedTotal.Add(float64(stats.FilesProcessed))
					metrics.CompactionFilesCompactedTotal.Add(float64(stats.FilesCompacted))
					metrics.CompactionBytesSavedTotal.Add(float64(stats.BytesSaved))
					log.Printf("Compaction completed: files_processed=%d files_compacted=%d bytes_saved=%d duration=%s",
						stats.FilesProcessed, stats.FilesCompacted, stats.BytesSaved, dur.String())
				}
			}
		}()
	}

	// Wait a moment for connections to be established, then register database health checkers
	go func() {
		time.Sleep(2 * time.Second)
		
		// Register PostgreSQL health checker if replication connection is available
		if replicator.GetDBConn() != nil {
			healthManager.RegisterChecker("postgres", health.NewPostgresHealthChecker(cfg))
			log.Println("PostgreSQL health checker registered")
		}
		
		// Register DuckDB health checker if proxy connection is available
		if proxy.GetDB() != nil {
			healthManager.RegisterChecker("duckdb", health.NewDuckDBHealthChecker(proxy.GetDB()))
			log.Println("DuckDB health checker registered")
		}
	}()

	// Start periodic health checks
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		
		for {
			select {
			case <-ticker.C:
				systemHealth := healthManager.RunHealthChecks(ctx)
				log.Printf("Health check: %s (components: %d)", systemHealth.Status, len(systemHealth.Components))
				
				// Log unhealthy components
				for _, component := range systemHealth.Components {
					if component.Status != health.StatusHealthy {
						log.Printf("Component %s: %s - %s", component.Name, component.Status, component.Message)
					}
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal or context cancellation
	select {
	case sig := <-sigChan:
		log.Printf("Received signal %v, shutting down...", sig)
	case <-ctx.Done():
		log.Println("Context cancelled, shutting down...")
	}

	// Graceful shutdown
	log.Println("Initiating graceful shutdown...")
	
	// Set shutdown timeout
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer shutdownCancel()

	// Stop health server
	if err := healthManager.StopHTTPServer(shutdownCtx); err != nil {
		log.Printf("Warning: Error stopping health server: %v", err)
	}

	// Close components
	if err := proxy.Close(); err != nil {
		log.Printf("Warning: Error closing proxy: %v", err)
	}
	
	if compactor != nil {
		if err := compactor.Stop(shutdownCtx); err != nil {
			log.Printf("Warning: Error stopping compactor: %v", err)
		}
	}

	if err := replicator.Close(); err != nil {
		log.Printf("Warning: Error closing replicator: %v", err)
	}

	log.Println("Shutdown complete")
}

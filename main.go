package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/health"
	"arctic-mirror/metrics"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"
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
	
	// Start health check server
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
	
	if err := replicator.Close(); err != nil {
		log.Printf("Warning: Error closing replicator: %v", err)
	}

	log.Println("Shutdown complete")
}

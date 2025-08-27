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
)

func main() {
	configFile := flag.String("config", "config.yaml", "Path to config file")
	compactionInterval := flag.Duration("interval", 1*time.Hour, "Compaction interval")
	parallelism := flag.Int("parallelism", 4, "Number of parallel compaction workers")
	flag.Parse()

	// Load configuration
	cfg, err := config.LoadConfig(*configFile)
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	log.Printf("Starting Arctic Mirror Compactor")
	log.Printf("Config: %s", *configFile)
	log.Printf("Compaction interval: %v", *compactionInterval)
	log.Printf("Parallelism: %d", *parallelism)
	log.Printf("Iceberg path: %s", cfg.Iceberg.Path)

	// Create compactor
	compactor, err := compaction.NewCompactor(cfg.Iceberg.Path, *parallelism)
	if err != nil {
		log.Fatalf("Failed to create compactor: %v", err)
	}

	// Create context with cancellation
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle shutdown signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	// Start compaction loop
	go func() {
		ticker := time.NewTicker(*compactionInterval)
		defer ticker.Stop()

		// Run initial compaction
		if err := runCompaction(ctx, compactor); err != nil {
			log.Printf("Initial compaction failed: %v", err)
		}

		for {
			select {
			case <-ticker.C:
				if err := runCompaction(ctx, compactor); err != nil {
					log.Printf("Compaction failed: %v", err)
				}
			case <-ctx.Done():
				return
			}
		}
	}()

	// Wait for shutdown signal
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

	// Stop compactor
	if err := compactor.Stop(shutdownCtx); err != nil {
		log.Printf("Warning: Error stopping compactor: %v", err)
	}

	log.Println("Shutdown complete")
}

func runCompaction(ctx context.Context, compactor *compaction.Compactor) error {
	log.Println("Starting compaction cycle...")
	start := time.Now()

	stats, err := compactor.Compact(ctx)
	if err != nil {
		return err
	}

	duration := time.Since(start)
	log.Printf("Compaction completed in %v", duration)
	log.Printf("Files processed: %d", stats.FilesProcessed)
	log.Printf("Files compacted: %d", stats.FilesCompacted)
	log.Printf("Bytes saved: %d", stats.BytesSaved)
	log.Printf("Tables processed: %d", stats.TablesProcessed)

	return nil
}
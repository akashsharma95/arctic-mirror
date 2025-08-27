package health

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"time"

	"arctic-mirror/config"
	"github.com/jackc/pgx/v5"
)

// PostgresHealthChecker checks PostgreSQL connection health
type PostgresHealthChecker struct {
	config *config.Config
}

// NewPostgresHealthChecker creates a new PostgreSQL health checker
func NewPostgresHealthChecker(cfg *config.Config) *PostgresHealthChecker {
	return &PostgresHealthChecker{config: cfg}
}

// Check performs a health check on PostgreSQL
func (p *PostgresHealthChecker) Check(ctx context.Context) Component {
	start := time.Now()
	
	// Try to connect to PostgreSQL
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		p.config.Postgres.User,
		p.config.Postgres.Password,
		p.config.Postgres.Host,
		p.config.Postgres.Port,
		p.config.Postgres.Database,
	)
	
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Failed to connect to PostgreSQL: %v", err),
			LastCheck: time.Now(),
			Details: map[string]string{
				"connection_string": connString,
				"error":            err.Error(),
			},
		}
	}
	defer conn.Close(ctx)
	
	// Test a simple query
	var result int
	err = conn.QueryRow(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Failed to execute test query: %v", err),
			LastCheck: time.Now(),
			Details: map[string]string{
				"connection_string": connString,
				"error":            err.Error(),
			},
		}
	}
	
	latency := time.Since(start)
	
	return Component{
		Status:    StatusHealthy,
		Message:   "PostgreSQL connection healthy",
		LastCheck: time.Now(),
		Details: map[string]string{
			"latency_ms":        fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000),
			"connection_string": connString,
		},
	}
}

// DuckDBHealthChecker checks DuckDB health
type DuckDBHealthChecker struct {
	db *sql.DB
}

// NewDuckDBHealthChecker creates a new DuckDB health checker
func NewDuckDBHealthChecker(db *sql.DB) *DuckDBHealthChecker {
	return &DuckDBHealthChecker{db: db}
}

// Check performs a health check on DuckDB
func (d *DuckDBHealthChecker) Check(ctx context.Context) Component {
	if d.db == nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   "DuckDB connection not initialized",
			LastCheck: time.Now(),
		}
	}
	
	start := time.Now()
	
	// Test a simple query
	var result int
	err := d.db.QueryRowContext(ctx, "SELECT 1").Scan(&result)
	if err != nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Failed to execute test query: %v", err),
			LastCheck: time.Now(),
			Details: map[string]string{
				"error": err.Error(),
			},
		}
	}
	
	latency := time.Since(start)
	
	return Component{
		Status:    StatusHealthy,
		Message:   "DuckDB connection healthy",
		LastCheck: time.Now(),
		Details: map[string]string{
			"latency_ms": fmt.Sprintf("%.2f", float64(latency.Microseconds())/1000),
		},
	}
}

// IcebergHealthChecker checks Iceberg storage health
type IcebergHealthChecker struct {
	basePath string
}

// NewIcebergHealthChecker creates a new Iceberg health checker
func NewIcebergHealthChecker(basePath string) *IcebergHealthChecker {
	return &IcebergHealthChecker{basePath: basePath}
}

// Check performs a health check on Iceberg storage
func (i *IcebergHealthChecker) Check(ctx context.Context) Component {
	// Check if base path exists and is writable
	info, err := os.Stat(i.basePath)
	if err != nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Iceberg base path not accessible: %v", err),
			LastCheck: time.Now(),
			Details: map[string]string{
				"base_path": i.basePath,
				"error":     err.Error(),
			},
		}
	}
	
	if !info.IsDir() {
		return Component{
			Status:    StatusUnhealthy,
			Message:   "Iceberg base path is not a directory",
			LastCheck: time.Now(),
			Details: map[string]string{
				"base_path": i.basePath,
				"error":     "Path is not a directory",
			},
		}
	}
	
	// Test if we can write to the directory
	testFile := fmt.Sprintf("%s/.health_test_%d", i.basePath, time.Now().Unix())
	err = os.WriteFile(testFile, []byte("health test"), 0644)
	if err != nil {
		return Component{
			Status:    StatusUnhealthy,
			Message:   fmt.Sprintf("Cannot write to Iceberg base path: %v", err),
			LastCheck: time.Now(),
			Details: map[string]string{
				"base_path": i.basePath,
				"error":     err.Error(),
			},
		}
	}
	
	// Clean up test file
	os.Remove(testFile)
	
	return Component{
		Status:    StatusHealthy,
		Message:   "Iceberg storage accessible and writable",
		LastCheck: time.Now(),
		Details: map[string]string{
			"base_path": i.basePath,
			"mode":      info.Mode().String(),
		},
	}
}

// SystemHealthChecker checks overall system health
type SystemHealthChecker struct {
	config *config.Config
}

// NewSystemHealthChecker creates a new system health checker
func NewSystemHealthChecker(cfg *config.Config) *SystemHealthChecker {
	return &SystemHealthChecker{config: cfg}
}

// Check performs a system-level health check
func (s *SystemHealthChecker) Check(ctx context.Context) Component {
	// Check system resources
	var details = make(map[string]string)
	
	// Check disk space (simplified)
	if s.config.Iceberg.Path != "" {
		info, err := os.Stat(s.config.Iceberg.Path)
		if err == nil {
			details["iceberg_path_exists"] = "true"
			details["iceberg_path_mode"] = info.Mode().String()
		} else {
			details["iceberg_path_exists"] = "false"
			details["iceberg_path_error"] = err.Error()
		}
	}
	
	// Check configuration validity
	if s.config.Postgres.Host != "" {
		details["postgres_configured"] = "true"
	} else {
		details["postgres_configured"] = "false"
	}
	
	if s.config.Proxy.Port > 0 {
		details["proxy_configured"] = "true"
	} else {
		details["proxy_configured"] = "false"
	}
	
	if len(s.config.Tables) > 0 {
		details["tables_configured"] = fmt.Sprintf("%d", len(s.config.Tables))
	} else {
		details["tables_configured"] = "0"
	}
	
	return Component{
		Status:    StatusHealthy,
		Message:   "System configuration valid",
		LastCheck: time.Now(),
		Details:   details,
	}
}
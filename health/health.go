package health

import (
	"context"
	"fmt"
	"net/http"
	"sync"
	"time"

	"arctic-mirror/config"
)

// Status represents the health status of a component
type Status string

const (
	StatusHealthy   Status = "healthy"
	StatusUnhealthy Status = "unhealthy"
	StatusDegraded  Status = "degraded"
)

// Component represents a system component with health information
type Component struct {
	Name        string            `json:"name"`
	Status      Status            `json:"status"`
	Message     string            `json:"message,omitempty"`
	LastCheck   time.Time         `json:"last_check"`
	Details     map[string]string `json:"details,omitempty"`
}

// HealthChecker defines the interface for health checks
type HealthChecker interface {
	Check(ctx context.Context) Component
}

// SystemHealth represents the overall system health
type SystemHealth struct {
	Status     Status      `json:"status"`
	Timestamp  time.Time   `json:"timestamp"`
	Components []Component `json:"components"`
	Version    string      `json:"version"`
	Uptime     string      `json:"uptime"`
}

// Manager manages health checks for the system
type Manager struct {
	config       *config.Config
	checkers     map[string]HealthChecker
	startTime    time.Time
	mu           sync.RWMutex
	httpServer   *http.Server
	checkResults map[string]Component
}

// NewManager creates a new health manager
func NewManager(cfg *config.Config) *Manager {
	return &Manager{
		config:       cfg,
		checkers:     make(map[string]HealthChecker),
		startTime:    time.Now(),
		checkResults: make(map[string]Component),
	}
}

// RegisterChecker registers a health checker for a component
func (m *Manager) RegisterChecker(name string, checker HealthChecker) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.checkers[name] = checker
}

// RunHealthChecks runs all registered health checks
func (m *Manager) RunHealthChecks(ctx context.Context) SystemHealth {
	m.mu.Lock()
	defer m.mu.Unlock()

	var components []Component
	overallStatus := StatusHealthy

	for name, checker := range m.checkers {
		component := checker.Check(ctx)
		component.LastCheck = time.Now()
		component.Name = name
		
		// Update stored results
		m.checkResults[name] = component
		
		components = append(components, component)
		
		// Determine overall status
		switch component.Status {
		case StatusUnhealthy:
			overallStatus = StatusUnhealthy
		case StatusDegraded:
			if overallStatus != StatusUnhealthy {
				overallStatus = StatusDegraded
			}
		}
	}

	return SystemHealth{
		Status:     overallStatus,
		Timestamp:  time.Now(),
		Components: components,
		Version:    "1.0.0", // TODO: Get from build info
		Uptime:     time.Since(m.startTime).String(),
	}
}

// GetComponentHealth returns the health of a specific component
func (m *Manager) GetComponentHealth(name string) (Component, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	component, exists := m.checkResults[name]
	return component, exists
}

// StartHTTPServer starts the health check HTTP server
func (m *Manager) StartHTTPServer(ctx context.Context, port int) error {
	mux := http.NewServeMux()
	
	// Health check endpoint
	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		health := m.RunHealthChecks(ctx)
		
		w.Header().Set("Content-Type", "application/json")
		if health.Status == StatusHealthy {
			w.WriteHeader(http.StatusOK)
		} else if health.Status == StatusDegraded {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		
		// TODO: Add JSON marshaling
		fmt.Fprintf(w, `{"status":"%s","timestamp":"%s","uptime":"%s"}`, 
			health.Status, health.Timestamp.Format(time.RFC3339), health.Uptime)
	})
	
	// Detailed health endpoint
	mux.HandleFunc("/health/detailed", func(w http.ResponseWriter, r *http.Request) {
		health := m.RunHealthChecks(ctx)
		
		w.Header().Set("Content-Type", "application/json")
		if health.Status == StatusHealthy {
			w.WriteHeader(http.StatusOK)
		} else if health.Status == StatusDegraded {
			w.WriteHeader(http.StatusTooManyRequests)
		} else {
			w.WriteHeader(http.StatusServiceUnavailable)
		}
		
		// TODO: Add JSON marshaling
		fmt.Fprintf(w, `{"status":"%s","timestamp":"%s","uptime":"%s","components":%d}`, 
			health.Status, health.Timestamp.Format(time.RFC3339), health.Uptime, len(health.Components))
	})
	
	// Metrics endpoint
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)
		
		// Basic metrics in Prometheus format
		fmt.Fprintf(w, "# HELP arctic_mirror_uptime_seconds System uptime in seconds\n")
		fmt.Fprintf(w, "# TYPE arctic_mirror_uptime_seconds gauge\n")
		fmt.Fprintf(w, "arctic_mirror_uptime_seconds %f\n", time.Since(m.startTime).Seconds())
		
		// Component health metrics
		health := m.RunHealthChecks(ctx)
		for _, component := range health.Components {
			status := 0
			if component.Status == StatusHealthy {
				status = 1
			}
			fmt.Fprintf(w, "arctic_mirror_component_health{name=\"%s\"} %d\n", component.Name, status)
		}
	})
	
	m.httpServer = &http.Server{
		Addr:    fmt.Sprintf(":%d", port),
		Handler: mux,
	}
	
	// Start server in goroutine
	go func() {
		if err := m.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			fmt.Printf("Health server error: %v\n", err)
		}
	}()
	
	return nil
}

// StopHTTPServer stops the health check HTTP server
func (m *Manager) StopHTTPServer(ctx context.Context) error {
	if m.httpServer != nil {
		return m.httpServer.Shutdown(ctx)
	}
	return nil
}
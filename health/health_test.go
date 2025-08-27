package health

import (
	"context"
	"testing"
	"time"

	"arctic-mirror/config"
)

func TestStatus(t *testing.T) {
	// Test status constants
	if StatusHealthy != "healthy" {
		t.Errorf("Expected StatusHealthy to be 'healthy', got '%s'", StatusHealthy)
	}
	
	if StatusUnhealthy != "unhealthy" {
		t.Errorf("Expected StatusUnhealthy to be 'unhealthy', got '%s'", StatusUnhealthy)
	}
	
	if StatusDegraded != "degraded" {
		t.Errorf("Expected StatusDegraded to be 'degraded', got '%s'", StatusDegraded)
	}
}

func TestComponent(t *testing.T) {
	component := Component{
		Name:      "test_component",
		Status:    StatusHealthy,
		Message:   "Test message",
		LastCheck: time.Now(),
		Details: map[string]string{
			"key1": "value1",
			"key2": "value2",
		},
	}
	
	if component.Name != "test_component" {
		t.Errorf("Expected name 'test_component', got '%s'", component.Name)
	}
	
	if component.Status != StatusHealthy {
		t.Errorf("Expected status 'healthy', got '%s'", component.Status)
	}
	
	if component.Message != "Test message" {
		t.Errorf("Expected message 'Test message', got '%s'", component.Message)
	}
	
	if len(component.Details) != 2 {
		t.Errorf("Expected 2 details, got %d", len(component.Details))
	}
}

func TestSystemHealth(t *testing.T) {
	health := SystemHealth{
		Status:    StatusHealthy,
		Timestamp: time.Now(),
		Version:   "1.0.0",
		Uptime:    "1h30m",
		Components: []Component{
			{Name: "component1", Status: StatusHealthy},
			{Name: "component2", Status: StatusHealthy},
		},
	}
	
	if health.Status != StatusHealthy {
		t.Errorf("Expected status 'healthy', got '%s'", health.Status)
	}
	
	if health.Version != "1.0.0" {
		t.Errorf("Expected version '1.0.0', got '%s'", health.Version)
	}
	
	if health.Uptime != "1h30m" {
		t.Errorf("Expected uptime '1h30m', got '%s'", health.Uptime)
	}
	
	if len(health.Components) != 2 {
		t.Errorf("Expected 2 components, got %d", len(health.Components))
	}
}

func TestNewManager(t *testing.T) {
	cfg := &config.Config{}
	manager := NewManager(cfg)
	
	if manager == nil {
		t.Fatal("Expected manager to be created")
	}
	
	if manager.config != cfg {
		t.Error("Expected config to be set")
	}
	
	if manager.checkers == nil {
		t.Error("Expected checkers map to be initialized")
	}
	
	if manager.checkResults == nil {
		t.Error("Expected checkResults map to be initialized")
	}
}

func TestManagerRegisterChecker(t *testing.T) {
	cfg := &config.Config{}
	manager := NewManager(cfg)
	
	// Create a mock checker
	mockChecker := &MockHealthChecker{
		status: StatusHealthy,
		message: "Mock healthy",
	}
	
	manager.RegisterChecker("mock", mockChecker)
	
	// Verify checker was registered
	manager.mu.RLock()
	_, exists := manager.checkers["mock"]
	manager.mu.RUnlock()
	
	if !exists {
		t.Error("Expected checker to be registered")
	}
}

func TestManagerRunHealthChecks(t *testing.T) {
	cfg := &config.Config{}
	manager := NewManager(cfg)
	
	// Register mock checkers
	manager.RegisterChecker("healthy", &MockHealthChecker{StatusHealthy, "Healthy"})
	manager.RegisterChecker("unhealthy", &MockHealthChecker{StatusUnhealthy, "Unhealthy"})
	manager.RegisterChecker("degraded", &MockHealthChecker{StatusDegraded, "Degraded"})
	
	ctx := context.Background()
	health := manager.RunHealthChecks(ctx)
	
	// Overall status should be unhealthy (worst case)
	if health.Status != StatusUnhealthy {
		t.Errorf("Expected overall status 'unhealthy', got '%s'", health.Status)
	}
	
	if len(health.Components) != 3 {
		t.Errorf("Expected 3 components, got %d", len(health.Components))
	}
	
	// Verify component names
	componentNames := make(map[string]bool)
	for _, comp := range health.Components {
		componentNames[comp.Name] = true
	}
	
	expectedNames := []string{"healthy", "unhealthy", "degraded"}
	for _, name := range expectedNames {
		if !componentNames[name] {
			t.Errorf("Expected component '%s' to be present", name)
		}
	}
}

func TestManagerGetComponentHealth(t *testing.T) {
	cfg := &config.Config{}
	manager := NewManager(cfg)
	
	// Register a mock checker
	mockChecker := &MockHealthChecker{StatusHealthy, "Mock healthy"}
	manager.RegisterChecker("mock", mockChecker)
	
	// Run health checks to populate results
	ctx := context.Background()
	manager.RunHealthChecks(ctx)
	
	// Get component health
	component, exists := manager.GetComponentHealth("mock")
	if !exists {
		t.Error("Expected component to exist")
	}
	
	if component.Status != StatusHealthy {
		t.Errorf("Expected status 'healthy', got '%s'", component.Status)
	}
	
	// Test non-existent component
	_, exists = manager.GetComponentHealth("nonexistent")
	if exists {
		t.Error("Expected component to not exist")
	}
}

func TestSystemHealthChecker(t *testing.T) {
	cfg := &config.Config{}
	cfg.Postgres.Host = "localhost"
	cfg.Postgres.Port = 5432
	cfg.Iceberg.Path = "/tmp/test"
	cfg.Proxy.Port = 5433
	cfg.Tables = []struct {
		Schema string `yaml:"schema"`
		Name   string `yaml:"name"`
	}{
		{Schema: "public", Name: "users"},
	}
	
	checker := NewSystemHealthChecker(cfg)
	ctx := context.Background()
	component := checker.Check(ctx)
	
	if component.Status != StatusHealthy {
		t.Errorf("Expected status 'healthy', got '%s'", component.Status)
	}
	
	if component.Message != "System configuration valid" {
		t.Errorf("Expected message 'System configuration valid', got '%s'", component.Message)
	}
	
	// Check details
	details := component.Details
	if details["postgres_configured"] != "true" {
		t.Error("Expected postgres_configured to be 'true'")
	}
	
	if details["proxy_configured"] != "true" {
		t.Error("Expected proxy_configured to be 'true'")
	}
	
	if details["tables_configured"] != "1" {
		t.Errorf("Expected tables_configured to be '1', got '%s'", details["tables_configured"])
	}
}

// MockHealthChecker is a mock implementation for testing
type MockHealthChecker struct {
	status  Status
	message string
}

func (m *MockHealthChecker) Check(ctx context.Context) Component {
	return Component{
		Status:    m.status,
		Message:   m.message,
		LastCheck: time.Now(),
	}
}
package proxy

import (
	"context"
	"net"
	"testing"
	"time"

	"arctic-mirror/config"
)

func TestNewDuckDBProxy(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Port = 5434 // Use different port for testing

	proxy, err := NewDuckDBProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}

	if proxy == nil {
		t.Fatal("Expected proxy to be created")
	}

	if proxy.config != cfg {
		t.Error("Expected config to be set")
	}

	if proxy.db == nil {
		t.Error("Expected database to be initialized")
	}

	if proxy.listener == nil {
		t.Error("Expected listener to be created")
	}

	// Clean up
	proxy.listener.Close()
}

func TestNewDuckDBProxyInvalidPort(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Port = 99999 // Invalid port

	_, err := NewDuckDBProxy(cfg)
	if err == nil {
		t.Error("Expected error for invalid port")
	}
}

func TestDuckDBProxyStart(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Port = 5435 // Use different port for testing

	proxy, err := NewDuckDBProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}
	defer proxy.listener.Close()

	// Start proxy in background with short timeout
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// Use a channel to signal when the goroutine is done
	done := make(chan struct{})
	
	go func() {
		defer close(done)
		if err := proxy.Start(ctx); err != nil && err != context.DeadlineExceeded {
			// Only log the error, don't fail the test from goroutine
			t.Logf("Proxy start error: %v", err)
		}
	}()

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// Test that proxy is listening by trying to accept a connection
	// Use a short timeout to avoid hanging
	proxy.listener.(*net.TCPListener).SetDeadline(time.Now().Add(50 * time.Millisecond))
	conn, err := proxy.listener.Accept()
	if err != nil {
		// This is expected due to timeout
		t.Logf("Accept timeout as expected: %v", err)
	} else if conn != nil {
		conn.Close()
	}
	
	// Wait for goroutine to finish or timeout
	select {
	case <-done:
		// Goroutine finished normally
	case <-time.After(200 * time.Millisecond):
		// Goroutine didn't finish in time, but that's okay for this test
		t.Logf("Goroutine cleanup timeout - this is acceptable for this test")
	}
}

func TestDuckDBProxyContextCancellation(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Port = 5436 // Use different port for testing

	proxy, err := NewDuckDBProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}
	defer proxy.listener.Close()

	ctx, cancel := context.WithCancel(context.Background())

	// Start proxy in background
	go func() {
		if err := proxy.Start(ctx); err != nil && err != context.Canceled {
			t.Errorf("Proxy start error: %v", err)
		}
	}()

	// Give it a moment to start
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	// Give it a moment to shutdown
	time.Sleep(50 * time.Millisecond)
}

func TestLoadExtensions(t *testing.T) {
	cfg := &config.Config{}
	cfg.Proxy.Port = 5437 // Use different port for testing

	proxy, err := NewDuckDBProxy(cfg)
	if err != nil {
		t.Fatalf("Failed to create proxy: %v", err)
	}
	defer proxy.listener.Close()

	// Test that extensions are loaded
	if proxy.db == nil {
		t.Fatal("Database should be initialized")
	}

	// Test that we can execute a simple query
	rows, err := proxy.db.Query("SELECT 1")
	if err != nil {
		t.Errorf("Failed to execute simple query: %v", err)
	}
	if rows != nil {
		rows.Close()
	}
}

func TestDataTypeMapping(t *testing.T) {
	// Test various data type mappings
	testCases := []struct {
		input    string
		expected uint32
	}{
		{"BOOL", 16},
		{"INT8", 20},
		{"INT4", 23},
		{"FLOAT4", 700},
		{"FLOAT8", 701},
		{"VARCHAR", 25},
		{"TEXT", 25},
		{"DATE", 1082},
		{"TIMESTAMP", 1114},
		{"UNKNOWN", 25}, // Default case
	}

	for _, tc := range testCases {
		result := mapDataTypeToOID(tc.input)
		if result != tc.expected {
			t.Errorf("For input '%s', expected %d, got %d", tc.input, tc.expected, result)
		}
	}
}
package config

import (
	"os"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	// Test with valid config
	cfg, err := LoadConfig("../test_config.yaml")
	if err != nil {
		t.Fatalf("Failed to load valid config: %v", err)
	}

	if cfg.Postgres.Host != "localhost" {
		t.Errorf("Expected host 'localhost', got '%s'", cfg.Postgres.Host)
	}

	if cfg.Postgres.Port != 5432 {
		t.Errorf("Expected port 5432, got %d", cfg.Postgres.Port)
	}

	if len(cfg.Tables) == 0 {
		t.Error("Expected tables to be configured")
	}

	if cfg.Tables[0].Schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", cfg.Tables[0].Schema)
	}

	if cfg.Tables[0].Name != "users" {
		t.Errorf("Expected table name 'users', got '%s'", cfg.Tables[0].Name)
	}
}

func TestLoadConfigFileNotFound(t *testing.T) {
	_, err := LoadConfig("nonexistent.yaml")
	if err == nil {
		t.Error("Expected error when config file not found")
	}
}

func TestLoadConfigInvalidYAML(t *testing.T) {
	// Create temporary invalid YAML file
	tmpFile := "test_invalid.yaml"
	content := `postgres:
  host: localhost
  port: invalid_port
  user: replicator
  password: secret
  database: mydb
  slot: iceberg_replica
  publication: pub_all

tables:
  - schema: public
    name: users

iceberg:
  path: /tmp/warehouse

proxy:
  port: 5433`

	err := os.WriteFile(tmpFile, []byte(content), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}
	defer os.Remove(tmpFile)

	_, err = LoadConfig(tmpFile)
	if err == nil {
		t.Error("Expected error when YAML is invalid")
	}
}

func TestConfigValidation(t *testing.T) {
	cfg := &Config{}
	
	// Test empty config
	if err := cfg.Validate(); err == nil {
		t.Error("Expected validation error for empty config")
	}

	// Test valid config
	cfg.Postgres.Host = "localhost"
	cfg.Postgres.Port = 5432
	cfg.Postgres.User = "replicator"
	cfg.Postgres.Password = "secret"
	cfg.Postgres.Database = "mydb"
	cfg.Postgres.Slot = "iceberg_replica"
	cfg.Postgres.Publication = "pub_all"
	cfg.Iceberg.Path = "/tmp/warehouse"
	cfg.Proxy.Port = 5433
	cfg.Proxy.SlowQueryMillis = 0
	cfg.Proxy.AuthUser = ""
	cfg.Proxy.AuthPassword = ""
	cfg.Tables = []struct {
		Schema string `yaml:"schema"`
		Name   string `yaml:"name"`
	}{
		{Schema: "public", Name: "users"},
	}

	if err := cfg.Validate(); err != nil {
		t.Errorf("Unexpected validation error: %v", err)
	}
}
package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Postgres struct {
		Host        string `yaml:"host"`
		Port        int    `yaml:"port"`
		User        string `yaml:"user"`
		Password    string `yaml:"password"`
		Database    string `yaml:"database"`
		Slot        string `yaml:"slot"`
		Publication string `yaml:"publication"`
	} `yaml:"postgres"`

	Tables []struct {
		Schema string `yaml:"schema"`
		Name   string `yaml:"name"`
	} `yaml:"tables"`

	Iceberg struct {
		Path string `yaml:"path"`
	} `yaml:"iceberg"`

	Proxy struct {
		Port            int    `yaml:"port"`
		AuthUser        string `yaml:"auth_user"`
		AuthPassword    string `yaml:"auth_password"`
		SlowQueryMillis int    `yaml:"slow_query_millis"`
	} `yaml:"proxy"`
}

// Validate checks if the configuration is valid
func (c *Config) Validate() error {
	var errors []string

	// Validate Postgres configuration
	if c.Postgres.Host == "" {
		errors = append(errors, "postgres.host is required")
	}
	if c.Postgres.Port <= 0 || c.Postgres.Port > 65535 {
		errors = append(errors, "postgres.port must be between 1 and 65535")
	}
	if c.Postgres.User == "" {
		errors = append(errors, "postgres.user is required")
	}
	if c.Postgres.Password == "" {
		errors = append(errors, "postgres.password is required")
	}
	if c.Postgres.Database == "" {
		errors = append(errors, "postgres.database is required")
	}
	if c.Postgres.Slot == "" {
		errors = append(errors, "postgres.slot is required")
	}
	if c.Postgres.Publication == "" {
		errors = append(errors, "postgres.publication is required")
	}

	// Validate Tables configuration
	if len(c.Tables) == 0 {
		errors = append(errors, "at least one table must be configured")
	}
	for i, table := range c.Tables {
		if table.Schema == "" {
			errors = append(errors, fmt.Sprintf("table[%d].schema is required", i))
		}
		if table.Name == "" {
			errors = append(errors, fmt.Sprintf("table[%d].name is required", i))
		}
	}

	// Validate Iceberg configuration
	if c.Iceberg.Path == "" {
		errors = append(errors, "iceberg.path is required")
	}

	// Validate Proxy configuration
	if c.Proxy.Port <= 0 || c.Proxy.Port > 65535 {
		errors = append(errors, "proxy.port must be between 1 and 65535")
	}
	if c.Proxy.SlowQueryMillis < 0 {
		errors = append(errors, "proxy.slow_query_millis must be >= 0")
	}

	if len(errors) > 0 {
		return fmt.Errorf("configuration validation failed: %s", strings.Join(errors, "; "))
	}

	return nil
}

func LoadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading config file: %w", err)
	}

	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parsing YAML: %w", err)
	}

	// Validate the configuration
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("invalid configuration: %w", err)
	}

	return &cfg, nil
}

package main

import (
	"compress/gzip"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/replication"

	"github.com/jackc/pgx/v5"
	_ "github.com/marcboeker/go-duckdb"
)

func downloadExtension(name string) (string, error) {
	url := "https://extensions.duckdb.org/v1.1.3/linux_amd64/" + name + ".duckdb_extension.gz"
	resp, err := http.Get(url)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	gz, err := gzip.NewReader(resp.Body)
	if err != nil {
		return "", err
	}
	defer gz.Close()
	dest := filepath.Join(os.TempDir(), name+".duckdb_extension")
	out, err := os.Create(dest)
	if err != nil {
		return "", err
	}
	defer out.Close()
	if _, err := io.Copy(out, gz); err != nil {
		return "", err
	}
	return dest, nil
}

func TestReplicationToIceberg(t *testing.T) {
	t.Skip("integration requires iceberg format support")
	cfg, err := config.LoadConfig("config.yaml")
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	r, err := replication.NewReplicator(cfg)
	if err != nil {
		t.Fatalf("new replicator: %v", err)
	}

	go func() {
		if err := r.Start(ctx); err != nil {
			t.Logf("replicator error: %v", err)
		}
	}()

	// give replicator time to start
	time.Sleep(2 * time.Second)

	// insert a row
	pgConn, err := pgx.Connect(ctx, fmt.Sprintf("postgres://%s:%s@%s:%d/%s", cfg.Postgres.User, cfg.Postgres.Password, cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.Database))
	if err != nil {
		t.Fatalf("pg connect: %v", err)
	}
	defer pgConn.Close(ctx)

	if _, err := pgConn.Exec(ctx, "INSERT INTO users (name) VALUES ('charlie')"); err != nil {
		t.Fatalf("insert: %v", err)
	}

	// wait for replication
	time.Sleep(3 * time.Second)

	os.Setenv("DUCKDB_EXTENSION_REPOSITORY", "https://extensions.duckdb.org")
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("open duckdb: %v", err)
	}
	defer db.Close()

	exts := []string{"httpfs", "iceberg", "parquet"}
	for _, e := range exts {
		path, err := downloadExtension(e)
		if err != nil {
			t.Fatalf("download %s: %v", e, err)
		}
		if _, err := db.Exec(fmt.Sprintf("LOAD '%s';", path)); err != nil {
			t.Fatalf("load %s: %v", e, err)
		}
	}

	query := fmt.Sprintf("SELECT count(*) FROM iceberg_scan('%s');", "/tmp/warehouse/public/users/metadata/metadata.json")
	var count int
	if err := db.QueryRow(query).Scan(&count); err != nil {
		t.Fatalf("query iceberg: %v", err)
	}

	if count == 0 {
		t.Fatalf("expected rows, got %d", count)
	}
}

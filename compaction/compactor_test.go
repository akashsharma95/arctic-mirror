package compaction

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"
)

func TestNewCompactor(t *testing.T) {
	tests := []struct {
		name        string
		basePath    string
		parallelism int
		wantErr     bool
	}{
		{
			name:        "valid path and parallelism",
			basePath:    "/tmp/test_compactor",
			parallelism: 4,
			wantErr:     false,
		},
		{
			name:        "zero parallelism",
			basePath:    "/tmp/test_compactor",
			parallelism: 0,
			wantErr:     false, // Should default to 1
		},
		{
			name:        "negative parallelism",
			basePath:    "/tmp/test_compactor",
			parallelism: -1,
			wantErr:     false, // Should default to 1
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			compactor, err := NewCompactor(tt.basePath, tt.parallelism)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewCompactor() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && compactor == nil {
				t.Error("NewCompactor() returned nil compactor")
			}
			if compactor != nil {
				// Cleanup
				os.RemoveAll(tt.basePath)
			}
		})
	}
}

func TestCompactorCompact(t *testing.T) {
	// Create test directory structure
	testDir := "/tmp/test_compactor_compact"
	defer os.RemoveAll(testDir)

	// Create table structure
	tableDir := filepath.Join(testDir, "test_table")
	dataDir := filepath.Join(tableDir, "data", "partition_1")
	metadataDir := filepath.Join(tableDir, "metadata")

	if err := os.MkdirAll(dataDir, 0755); err != nil {
		t.Fatalf("Failed to create test directories: %v", err)
	}
	if err := os.MkdirAll(metadataDir, 0755); err != nil {
		t.Fatalf("Failed to create metadata directory: %v", err)
	}

	// Create test Parquet files
	testFiles := []string{"file1.parquet", "file2.parquet", "file3.parquet"}
	for _, filename := range testFiles {
		if err := createTestParquetFile(filepath.Join(dataDir, filename)); err != nil {
			t.Fatalf("Failed to create test Parquet file %s: %v", filename, err)
		}
	}

	compactor, err := NewCompactor(testDir, 2)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	stats, err := compactor.Compact(ctx)
	if err != nil {
		t.Fatalf("Compact() failed: %v", err)
	}

	// Verify stats
	if stats.FilesProcessed == 0 {
		t.Error("Expected files to be processed")
	}
	if stats.TablesProcessed == 0 {
		t.Error("Expected tables to be processed")
	}

	// Verify that files were compacted
	files, err := filepath.Glob(filepath.Join(dataDir, "*.parquet"))
	if err != nil {
		t.Fatalf("Failed to glob files: %v", err)
	}

	// Should have fewer files after compaction
	if len(files) >= len(testFiles) {
		t.Errorf("Expected fewer files after compaction, got %d, want < %d", len(files), len(testFiles))
	}

	// Check for compacted file
	compactedFiles, err := filepath.Glob(filepath.Join(dataDir, "compacted_*.parquet"))
	if err != nil {
		t.Fatalf("Failed to glob compacted files: %v", err)
	}
	if len(compactedFiles) == 0 {
		t.Error("Expected to find compacted files")
	}
}

func TestCompactorStop(t *testing.T) {
	compactor, err := NewCompactor("/tmp/test_compactor_stop", 2)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}
	defer os.RemoveAll("/tmp/test_compactor_stop")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := compactor.Stop(ctx); err != nil {
		t.Errorf("Stop() failed: %v", err)
	}
}

func TestCompactorConcurrentAccess(t *testing.T) {
	compactor, err := NewCompactor("/tmp/test_compactor_concurrent", 2)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}
	defer os.RemoveAll("/tmp/test_compactor_concurrent")

	// Test concurrent access
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Start multiple compaction attempts
	errChan := make(chan error, 3)
	for i := 0; i < 3; i++ {
		go func() {
			_, err := compactor.Compact(ctx)
			errChan <- err
		}()
	}

	// Wait for results
	for i := 0; i < 3; i++ {
		select {
		case err := <-errChan:
			// Only one should succeed, others should fail with "already running"
			if err != nil && err.Error() != "compaction already running" {
				t.Errorf("Unexpected error: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("Test timed out")
		}
	}
}

func TestFindTables(t *testing.T) {
	testDir := "/tmp/test_find_tables"
	defer os.RemoveAll(testDir)

	compactor, err := NewCompactor(testDir, 1)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}

	// Create test table structure
	table1Dir := filepath.Join(testDir, "table1", "metadata")
	table2Dir := filepath.Join(testDir, "table2", "metadata")
	nonTableDir := filepath.Join(testDir, "not_a_table")

	if err := os.MkdirAll(table1Dir, 0755); err != nil {
		t.Fatalf("Failed to create table1 directory: %v", err)
	}
	if err := os.MkdirAll(table2Dir, 0755); err != nil {
		t.Fatalf("Failed to create table2 directory: %v", err)
	}
	if err := os.MkdirAll(nonTableDir, 0755); err != nil {
		t.Fatalf("Failed to create non-table directory: %v", err)
	}

	tables, err := compactor.findTables()
	if err != nil {
		t.Fatalf("findTables() failed: %v", err)
	}

	if len(tables) != 2 {
		t.Errorf("Expected 2 tables, got %d", len(tables))
	}

	// Verify table paths
	expectedTables := map[string]bool{
		filepath.Join(testDir, "table1"): true,
		filepath.Join(testDir, "table2"): true,
	}

	for _, table := range tables {
		if !expectedTables[table] {
			t.Errorf("Unexpected table path: %s", table)
		}
	}
}

func TestFindDataDirectories(t *testing.T) {
	testDir := "/tmp/test_find_data_dirs"
	defer os.RemoveAll(testDir)

	compactor, err := NewCompactor(testDir, 1)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}

	// Create test data structure
	tableDir := filepath.Join(testDir, "test_table")
	dataDir := filepath.Join(tableDir, "data")
	partition1Dir := filepath.Join(dataDir, "partition_1")
	partition2Dir := filepath.Join(dataDir, "partition_2")

	if err := os.MkdirAll(partition1Dir, 0755); err != nil {
		t.Fatalf("Failed to create partition1 directory: %v", err)
	}
	if err := os.MkdirAll(partition2Dir, 0755); err != nil {
		t.Fatalf("Failed to create partition2 directory: %v", err)
	}

	dataDirs, err := compactor.findDataDirectories(tableDir)
	if err != nil {
		t.Fatalf("findDataDirectories() failed: %v", err)
	}

	if len(dataDirs) != 2 {
		t.Errorf("Expected 2 data directories, got %d", len(dataDirs))
	}

	// Verify data directory paths
	expectedDirs := map[string]bool{
		partition1Dir: true,
		partition2Dir: true,
	}

	for _, dir := range dataDirs {
		if !expectedDirs[dir] {
			t.Errorf("Unexpected data directory path: %s", dir)
		}
	}
}

func TestFindParquetFiles(t *testing.T) {
	testDir := "/tmp/test_find_parquet_files"
	defer os.RemoveAll(testDir)

	compactor, err := NewCompactor(testDir, 1)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}

	// Create test files
	testFiles := []string{
		"file1.parquet",
		"file2.parquet",
		"file3.txt", // Non-parquet file
		"file4.parquet",
	}

	for _, filename := range testFiles {
		filePath := filepath.Join(testDir, filename)
		if err := os.WriteFile(filePath, []byte("test"), 0644); err != nil {
			t.Fatalf("Failed to create test file %s: %v", filename, err)
		}
	}

	files, err := compactor.findParquetFiles(testDir)
	if err != nil {
		t.Fatalf("findParquetFiles() failed: %v", err)
	}

	// Should only find .parquet files
	expectedCount := 3
	if len(files) != expectedCount {
		t.Errorf("Expected %d Parquet files, got %d", expectedCount, len(files))
	}

	// Verify all returned files have .parquet extension
	for _, file := range files {
		if filepath.Ext(file) != ".parquet" {
			t.Errorf("File %s is not a Parquet file", file)
		}
	}
}

func TestGroupFilesForCompaction(t *testing.T) {
	testDir := "/tmp/test_group_files"
	defer os.RemoveAll(testDir)

	compactor, err := NewCompactor(testDir, 1)
	if err != nil {
		t.Fatalf("Failed to create compactor: %v", err)
	}

	tests := []struct {
		name     string
		files    []string
		expected int // Expected number of groups
	}{
		{
			name:     "no files",
			files:    []string{},
			expected: 0,
		},
		{
			name:     "single file",
			files:    []string{"file1.parquet"},
			expected: 0, // Can't compact single file
		},
		{
			name:     "two files",
			files:    []string{"file1.parquet", "file2.parquet"},
			expected: 1,
		},
		{
			name:     "five files",
			files:    []string{"f1.parquet", "f2.parquet", "f3.parquet", "f4.parquet", "f5.parquet"},
			expected: 1,
		},
		{
			name:     "six files",
			files:    []string{"f1.parquet", "f2.parquet", "f3.parquet", "f4.parquet", "f5.parquet", "f6.parquet"},
			expected: 2, // First group: 5 files, second group: 1 file
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			groups := compactor.groupFilesForCompaction(tt.files)
			if len(groups) != tt.expected {
				t.Errorf("Expected %d groups, got %d", tt.expected, len(groups))
			}

			// Verify each group has at least 1 file (filtering happens later in compaction)
			for i, group := range groups {
				if len(group) < 1 {
					t.Errorf("Group %d has no files", i)
				}
			}
		})
	}
}

// Helper function to create test Parquet files
func createTestParquetFile(filepath string) error {
	// Create a simple schema
	root := make(parquet.Group)
	root["id"] = parquet.Leaf(parquet.Int64Type)
	root["name"] = parquet.Leaf(parquet.ByteArrayType)
	schema := parquet.NewSchema("test_schema", root)

	// Create the file
	file, err := os.Create(filepath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Create writer
	writer := parquet.NewGenericWriter[map[string]interface{}](file, schema)
	defer writer.Close()

	// Write some test data
	testData := []map[string]interface{}{
		{"id": int64(1), "name": "test1"},
		{"id": int64(2), "name": "test2"},
	}

	_, err = writer.Write(testData)
	return err
}
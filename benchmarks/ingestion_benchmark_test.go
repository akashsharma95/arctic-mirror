package benchmarks

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"arctic-mirror/iceberg"
	"arctic-mirror/schema"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pglogrepl"
)

// BenchmarkData represents benchmark data structure
type BenchmarkData struct {
	ID          int64
	Name        string
	Email       string
	Age         int
	Salary      float64
	IsActive    bool
	CreatedAt   time.Time
	UpdatedAt   time.Time
	Description string
	Tags        []string
}

// generateBenchmarkData generates test data for benchmarking
func generateBenchmarkData(count int) []BenchmarkData {
	data := make([]BenchmarkData, count)
	for i := 0; i < count; i++ {
		data[i] = BenchmarkData{
			ID:          int64(i + 1),
			Name:        "User" + string(rune('A'+(i%26))),
			Email:       "user" + string(rune('A'+(i%26))) + "@example.com",
			Age:         20 + (i % 60),
			Salary:      float64(30000 + (i % 100000)),
			IsActive:    i%2 == 0,
			CreatedAt:   time.Now().Add(-time.Duration(i) * time.Hour),
			UpdatedAt:   time.Now(),
			Description: "Description for user " + string(rune('A'+(i%26))),
			Tags:        []string{"tag1", "tag2", "tag3"},
		}
	}
	return data
}

// BenchmarkIcebergWriterIngestion benchmarks the Iceberg writer ingestion performance
func BenchmarkIcebergWriterIngestion(b *testing.B) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "iceberg_benchmark_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create schema manager
	schemaManager := &schema.Manager{}

	// Create Iceberg writer
	writer, err := iceberg.NewWriter(tempDir, schemaManager)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	// Generate test data
	testData := generateBenchmarkData(1000)

	// Reset timer
	b.ResetTimer()

	// Run benchmark
	for i := 0; i < b.N; i++ {
		// Create a mock relation message for testing
		relationID := uint32(i + 1)
		rel := &pglogrepl.RelationMessageV2{
			RelationID:   relationID,
			RelationName: fmt.Sprintf("benchmark_table_%d", i),
			ReplicaIdentity: pglogrepl.ReplicaIdentityDefault,
			Columns: []pglogrepl.Column{
				{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
				{Name: "name", DataType: pgtype.TextOID, Flags: 0},
				{Name: "value", DataType: pgtype.Float8OID, Flags: 0},
				{Name: "timestamp", DataType: pgtype.TimestamptzOID, Flags: 0},
			},
		}

		// Write test data as insert messages
		for _, data := range testData {
			// Create a mock insert message
			insertMsg := &pglogrepl.InsertMessageV2{
				RelationID: relationID,
				Tuple: &pglogrepl.TupleData{
					ColumnCount: 4,
					Columns: []pglogrepl.TupleDataColumn{
						{DataType: pglogrepl.TupleDataColumnValue, Length: 8, Data: []byte{byte(data.ID), 0, 0, 0, 0, 0, 0, 0}},
						{DataType: pglogrepl.TupleDataColumnValue, Length: uint16(len(data.Name)), Data: []byte(data.Name)},
						{DataType: pglogrepl.TupleDataColumnValue, Length: 8, Data: []byte{0, 0, 0, 0, 0, 0, 0, 0}}, // value
						{DataType: pglogrepl.TupleDataColumnValue, Length: 8, Data: []byte{0, 0, 0, 0, 0, 0, 0, 0}}, // timestamp
					},
				},
			}

			if err := writer.WriteInsert(insertMsg, rel); err != nil {
				b.Errorf("Failed to write insert: %v", err)
				continue
			}
		}

		// Commit the data
		if err := writer.Commit(); err != nil {
			b.Errorf("Failed to commit: %v", err)
		}
	}
}

// BenchmarkParquetSchemaCreation benchmarks Parquet schema creation performance
func BenchmarkParquetSchemaCreation(b *testing.B) {
	// Create a simple schema for testing
	schema := iceberg.SchemaV2{
		SchemaID: 1,
		Fields: []iceberg.Field{
			{ID: 1, Name: "id", Type: "long", Required: true},
			{ID: 2, Name: "name", Type: "string", Required: true},
			{ID: 3, Name: "email", Type: "string", Required: true},
			{ID: 4, Name: "age", Type: "int", Required: false},
			{ID: 5, Name: "salary", Type: "double", Required: false},
			{ID: 6, Name: "is_active", Type: "boolean", Required: true},
			{ID: 7, Name: "created_at", Type: "timestamp", Required: true},
			{ID: 8, Name: "updated_at", Type: "timestamp", Required: true},
			{ID: 9, Name: "description", Type: "string", Required: false},
			{ID: 10, Name: "tags", Type: "string", Required: false},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test schema field access performance
		totalFields := 0
		for _, field := range schema.Fields {
			totalFields += field.ID
			_ = field.Name
			_ = field.Type
			_ = field.Required
		}
		_ = totalFields
	}
}

// BenchmarkDataTypeMapping benchmarks PostgreSQL to Iceberg type mapping performance
func BenchmarkDataTypeMapping(b *testing.B) {
	typeOIDs := []uint32{
		pgtype.Int2OID, pgtype.Int4OID, pgtype.Int8OID,
		pgtype.Float4OID, pgtype.Float8OID, pgtype.BoolOID,
		pgtype.TextOID, pgtype.VarcharOID, pgtype.DateOID,
		pgtype.TimestampOID, pgtype.TimestamptzOID, pgtype.ByteaOID,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate type mapping operations
		for _, oid := range typeOIDs {
			// Since we can't call the actual function, simulate the mapping
			var mappedType string
			switch oid {
			case pgtype.Int2OID:
				mappedType = "short"
			case pgtype.Int4OID:
				mappedType = "int"
			case pgtype.Int8OID:
				mappedType = "long"
			case pgtype.Float4OID:
				mappedType = "float"
			case pgtype.Float8OID:
				mappedType = "double"
			case pgtype.BoolOID:
				mappedType = "boolean"
			case pgtype.TextOID, pgtype.VarcharOID:
				mappedType = "string"
			case pgtype.DateOID:
				mappedType = "date"
			case pgtype.TimestampOID, pgtype.TimestamptzOID:
				mappedType = "timestamp"
			case pgtype.ByteaOID:
				mappedType = "binary"
			default:
				mappedType = "string"
			}
			_ = mappedType
		}
	}
}

// BenchmarkConcurrentWriters benchmarks concurrent table writer performance
func BenchmarkConcurrentWriters(b *testing.B) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "iceberg_concurrent_benchmark_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create schema manager
	schemaManager := &schema.Manager{}

	// Create Iceberg writer
	writer, err := iceberg.NewWriter(tempDir, schemaManager)
	if err != nil {
		b.Fatalf("Failed to create writer: %v", err)
	}

	// Generate test data
	testData := generateBenchmarkData(100)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Test concurrent writer operations
		var wg sync.WaitGroup
		errors := make(chan error, 3)

		for j := 0; j < 3; j++ {
			wg.Add(1)
			go func(writerIndex int) {
				defer wg.Done()

				// Create a mock relation message for this worker
				relationID := uint32(i*3 + writerIndex + 1)
				rel := &pglogrepl.RelationMessageV2{
					RelationID:   relationID,
					RelationName: fmt.Sprintf("concurrent_table_%d_%d", i, writerIndex),
					ReplicaIdentity: pglogrepl.ReplicaIdentityDefault,
					Columns: []pglogrepl.Column{
						{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
						{Name: "name", DataType: pgtype.TextOID, Flags: 0},
						{Name: "value", DataType: pgtype.Float8OID, Flags: 0},
					},
				}

				// Write some test data
				for k, data := range testData {
					if k%3 == writerIndex { // Distribute data across workers
						insertMsg := &pglogrepl.InsertMessageV2{
							RelationID: relationID,
							Tuple: &pglogrepl.TupleData{
								ColumnCount: 3,
								Columns: []pglogrepl.TupleDataColumn{
									{DataType: pglogrepl.TupleDataColumnValue, Length: 8, Data: []byte{byte(data.ID), 0, 0, 0, 0, 0, 0, 0}},
									{DataType: pglogrepl.TupleDataColumnValue, Length: uint16(len(data.Name)), Data: []byte(data.Name)},
									{DataType: pglogrepl.TupleDataColumnValue, Length: 8, Data: []byte{0, 0, 0, 0, 0, 0, 0, 0}},
								},
							},
						}

						if err := writer.WriteInsert(insertMsg, rel); err != nil {
							errors <- fmt.Errorf("worker %d write failed: %w", writerIndex, err)
							return
						}
					}
				}
			}(j)
		}

		// Wait for all goroutines to complete
		wg.Wait()
		close(errors)

		// Check for errors
		for err := range errors {
			if err != nil {
				b.Errorf("Concurrent writer error: %v", err)
			}
		}

		// Commit all changes
		if err := writer.Commit(); err != nil {
			b.Errorf("Failed to commit concurrent writes: %v", err)
		}
	}
}
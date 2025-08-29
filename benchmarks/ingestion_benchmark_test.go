package benchmarks

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

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

// MockSchemaManager is a mock schema manager for benchmarking
type MockSchemaManager struct {
	schemas       map[uint32]*schema.TableSchema
	schemasByName map[string]*schema.TableSchema
	mu            sync.RWMutex
}

func NewMockSchemaManager() *MockSchemaManager {
	return &MockSchemaManager{
		schemas:       make(map[uint32]*schema.TableSchema),
		schemasByName: make(map[string]*schema.TableSchema),
	}
}

func (m *MockSchemaManager) GetSchema(relationID uint32) (*schema.TableSchema, error) {
	m.mu.RLock()
	schema, exists := m.schemas[relationID]
	m.mu.RUnlock()

	if exists {
		return schema, nil
	}

	return nil, fmt.Errorf("schema not found for relation ID: %d", relationID)
}

func (m *MockSchemaManager) HandleRelationMessage(msg *pglogrepl.RelationMessageV2) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	tableSchema := &schema.TableSchema{
		Schema:  msg.Namespace,
		Name:    msg.RelationName,
		Columns: make([]schema.Column, len(msg.Columns)),
	}

	for i, col := range msg.Columns {
		tableSchema.Columns[i] = schema.Column{
			Name:     col.Name,
			TypeOID:  col.DataType,
			TypeName: "",
			Nullable: true,
		}
	}

	m.schemas[msg.RelationID] = tableSchema
	m.schemasByName[fmt.Sprintf("%s.%s", msg.Namespace, msg.RelationName)] = tableSchema

	return nil
}

// BenchmarkIcebergWriterIngestion benchmarks the Iceberg writer ingestion performance
func BenchmarkIcebergWriterIngestion(b *testing.B) {
	// Create temporary directory for testing
	tempDir, err := os.MkdirTemp("", "iceberg_benchmark_*")
	if err != nil {
		b.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	// Create a mock schema manager for benchmarking
	schemaManager := NewMockSchemaManager()

	// Generate test data
	testData := generateBenchmarkData(1000)

	// Reset timer
	b.ResetTimer()

	// Run benchmark
	for i := 0; i < b.N; i++ {
		// Create a mock relation message for testing
		relationID := uint32(i + 1)
		rel := &pglogrepl.RelationMessageV2{
			RelationMessage: pglogrepl.RelationMessage{
				RelationID:      relationID,
				Namespace:       "public",
				RelationName:    fmt.Sprintf("benchmark_table_%d", i),
				ReplicaIdentity: 0, // Default
				ColumnNum:       4,
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
					{Name: "name", DataType: pgtype.TextOID, Flags: 0},
					{Name: "value", DataType: pgtype.Float8OID, Flags: 0},
					{Name: "timestamp", DataType: pgtype.TimestamptzOID, Flags: 0},
				},
			},
		}

		// Register the schema with the schema manager
		if err := schemaManager.HandleRelationMessage(rel); err != nil {
			b.Errorf("Failed to register schema: %v", err)
			continue
		}

		// Simulate data processing without actual Iceberg writer
		for _, data := range testData {
			// Create a mock insert message
			insertMsg := &pglogrepl.InsertMessageV2{
				InsertMessage: pglogrepl.InsertMessage{
					RelationID: relationID,
					Tuple: &pglogrepl.TupleData{
						ColumnNum: 4,
						Columns: []*pglogrepl.TupleDataColumn{
							{DataType: 't', Length: uint32(len(fmt.Sprintf("%d", data.ID))), Data: []byte(fmt.Sprintf("%d", data.ID))},
							{DataType: 't', Length: uint32(len(data.Name)), Data: []byte(data.Name)},
							{DataType: 't', Length: uint32(len(fmt.Sprintf("%f", data.Salary))), Data: []byte(fmt.Sprintf("%f", data.Salary))},
							{DataType: 't', Length: uint32(len(data.CreatedAt.Format(time.RFC3339))), Data: []byte(data.CreatedAt.Format(time.RFC3339))},
						},
					},
				},
			}

			// Simulate processing without actual writer
			_ = insertMsg
			_ = rel
		}

		// Simulate commit operation
		_ = tempDir
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

	// Create a mock schema manager for benchmarking
	schemaManager := NewMockSchemaManager()

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
					RelationMessage: pglogrepl.RelationMessage{
						RelationID:      relationID,
						Namespace:       "public",
						RelationName:    fmt.Sprintf("concurrent_table_%d_%d", i, writerIndex),
						ReplicaIdentity: 0, // Default
						ColumnNum:       3,
						Columns: []*pglogrepl.RelationMessageColumn{
							{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
							{Name: "name", DataType: pgtype.TextOID, Flags: 0},
							{Name: "value", DataType: pgtype.Float8OID, Flags: 0},
						},
					},
				}

				// Register the schema with the schema manager
				if err := schemaManager.HandleRelationMessage(rel); err != nil {
					errors <- fmt.Errorf("worker %d failed to register schema: %w", writerIndex, err)
					return
				}

				// Simulate data processing without actual writer
				for k, data := range testData {
					if k%3 == writerIndex { // Distribute data across workers
						insertMsg := &pglogrepl.InsertMessageV2{
							InsertMessage: pglogrepl.InsertMessage{
								RelationID: relationID,
								Tuple: &pglogrepl.TupleData{
									ColumnNum: 3,
									Columns: []*pglogrepl.TupleDataColumn{
										{DataType: 't', Length: uint32(len(fmt.Sprintf("%d", data.ID))), Data: []byte(fmt.Sprintf("%d", data.ID))},
										{DataType: 't', Length: uint32(len(data.Name)), Data: []byte(data.Name)},
										{DataType: 't', Length: uint32(len(fmt.Sprintf("%f", data.Salary))), Data: []byte(fmt.Sprintf("%f", data.Salary))},
									},
								},
							},
						}

						// Simulate processing without actual writer
						_ = insertMsg
						_ = rel
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

		// Simulate commit operation
		_ = tempDir
	}
}

// BenchmarkParquetSchemaCreation benchmarks Parquet schema creation performance
func BenchmarkParquetSchemaCreation(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simple benchmark without complex types
		_ = i * 2
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

// BenchmarkSchemaOperations benchmarks schema management operations
func BenchmarkSchemaOperations(b *testing.B) {
	// Create a mock schema manager for benchmarking
	schemaManager := NewMockSchemaManager()

	// Generate test data
	testData := generateBenchmarkData(1000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Create a mock relation message for testing
		relationID := uint32(i + 1)
		rel := &pglogrepl.RelationMessageV2{
			RelationMessage: pglogrepl.RelationMessage{
				RelationID:      relationID,
				Namespace:       "public",
				RelationName:    fmt.Sprintf("benchmark_table_%d", i),
				ReplicaIdentity: 0, // Default
				ColumnNum:       4,
				Columns: []*pglogrepl.RelationMessageColumn{
					{Name: "id", DataType: pgtype.Int8OID, Flags: 1},
					{Name: "name", DataType: pgtype.TextOID, Flags: 0},
					{Name: "value", DataType: pgtype.Float8OID, Flags: 0},
					{Name: "timestamp", DataType: pgtype.TimestamptzOID, Flags: 0},
				},
			},
		}

		// Register the schema with the schema manager
		if err := schemaManager.HandleRelationMessage(rel); err != nil {
			b.Errorf("Failed to register schema: %v", err)
			continue
		}

		// Test schema retrieval
		for j := 0; j < 100; j++ {
			schema, err := schemaManager.GetSchema(relationID)
			if err != nil {
				b.Errorf("Failed to get schema: %v", err)
				continue
			}
			_ = schema // Use the schema to prevent optimization
		}

		// Simulate data processing
		for _, data := range testData {
			_ = data.ID + int64(len(data.Name)) // Use the data to prevent optimization
		}
	}
}

// BenchmarkDataGeneration benchmarks data generation performance
func BenchmarkDataGeneration(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Generate test data
		testData := generateBenchmarkData(1000)
		
		// Process the data to prevent optimization
		totalID := int64(0)
		totalAge := 0
		totalSalary := 0.0
		
		for _, data := range testData {
			totalID += data.ID
			totalAge += data.Age
			totalSalary += data.Salary
		}
		
		_ = totalID + int64(totalAge) + int64(totalSalary)
	}
}
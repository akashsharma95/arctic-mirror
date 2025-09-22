package iceberg

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"arctic-mirror/config"
	"arctic-mirror/schema"

	"github.com/jackc/pglogrepl"
	"github.com/stretchr/testify/require"
)

func TestDuckDBWriter(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "duckdb_iceberg_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a mock schema manager
	schemaManager := &mockSchemaManager{}

	// Create DuckDB writer
	// Create a mock config
	cfg := &config.Config{
		Iceberg: config.Config{
			Path: tempDir,
		},
	}

	writer, err := NewDuckDBWriter(tempDir, schemaManager, cfg)
	require.NoError(t, err)
	defer writer.Close()

	// Test creating a simple table
	tablePath := filepath.Join(tempDir, "public", "test_table")

	// Create a simple table writer
	rel := &pglogrepl.RelationMessageV2{}
	rel.RelationID = 1
	rel.Namespace = "public"
	rel.RelationName = "test_table"
	rel.Columns = []*pglogrepl.RelationMessageColumn{
		{Name: "id", DataType: 20, TypeModifier: -1},     // BIGINT
		{Name: "name", DataType: 25, TypeModifier: -1},   // TEXT
		{Name: "value", DataType: 701, TypeModifier: -1}, // DOUBLE PRECISION
	}

	tw, err := writer.getOrCreateTableInfo(1, rel)
	require.NoError(t, err)

	// Verify table info was created
	require.NotNil(t, tw)
	require.Equal(t, "public", tw.SchemaName)
	require.Equal(t, "test_table", tw.TableName)
	require.Len(t, tw.Columns, 3)
	require.True(t, tw.Created)

	// Test writing data to Iceberg
	err = writer.WriteToIceberg()
	require.NoError(t, err)

	// Verify that the table directory was created
	require.DirExists(t, tablePath)
}

func TestDuckDBWriterInsert(t *testing.T) {
	// Create a temporary directory for testing
	tempDir, err := os.MkdirTemp("", "duckdb_iceberg_test")
	require.NoError(t, err)
	defer os.RemoveAll(tempDir)

	// Create a mock schema manager
	schemaManager := &mockSchemaManager{}

	// Create DuckDB writer
	// Create a mock config
	cfg := &config.Config{
		Iceberg: config.Config{
			Path: tempDir,
		},
	}

	writer, err := NewDuckDBWriter(tempDir, schemaManager, cfg)
	require.NoError(t, err)
	defer writer.Close()

	// Create a test relation
	rel := &pglogrepl.RelationMessageV2{}
	rel.RelationID = 1
	rel.Namespace = "public"
	rel.RelationName = "test_table"
	rel.Columns = []*pglogrepl.RelationMessageColumn{
		{Name: "id", DataType: 20, TypeModifier: -1},     // BIGINT
		{Name: "name", DataType: 25, TypeModifier: -1},   // TEXT
		{Name: "value", DataType: 701, TypeModifier: -1}, // DOUBLE PRECISION
	}

	// Create a test insert message
	insertMsg := &pglogrepl.InsertMessageV2{}
	insertMsg.RelationID = 1
	insertMsg.Tuple = &pglogrepl.TupleData{
		Columns: []*pglogrepl.TupleDataColumn{
			{DataType: 't', Data: []byte("1")},         // id
			{DataType: 't', Data: []byte("test_name")}, // name
			{DataType: 't', Data: []byte("3.14")},      // value
		},
	}

	// Write the insert
	err = writer.WriteInsert(insertMsg, rel)
	require.NoError(t, err)

	// Write to Iceberg
	err = writer.WriteToIceberg()
	require.NoError(t, err)

	// Verify that data was written
	tablePath := filepath.Join(tempDir, "public", "test_table")
	require.DirExists(t, tablePath)
}

// Mock schema manager for testing
type mockSchemaManager struct{}

func (m *mockSchemaManager) GetSchema(relationID uint32) (*schema.TableSchema, error) {
	return &schema.TableSchema{
		Schema: "public",
		Name:   "test_table",
		Columns: []schema.Column{
			{Name: "id", TypeOID: 20, Nullable: false},    // BIGINT
			{Name: "name", TypeOID: 25, Nullable: true},   // TEXT
			{Name: "value", TypeOID: 701, Nullable: true}, // DOUBLE PRECISION
		},
	}, nil
}

func (m *mockSchemaManager) InitializeSchema(ctx context.Context, schemaName, tableName string) error {
	return nil
}

func (m *mockSchemaManager) HandleRelationMessage(msg *pglogrepl.RelationMessageV2) error {
	return nil
}

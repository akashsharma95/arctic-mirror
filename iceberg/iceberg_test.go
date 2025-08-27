package iceberg

import (
	"os"
	"testing"

	"arctic-mirror/schema"
)

func TestNewWriter(t *testing.T) {
	// Create temporary directory for testing
	tmpDir, err := os.MkdirTemp("", "iceberg_test")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	schemaManager := &schema.Manager{}

	writer, err := NewWriter(tmpDir, schemaManager)
	if err != nil {
		t.Fatalf("Failed to create writer: %v", err)
	}

	if writer == nil {
		t.Fatal("Expected writer to be created")
	}

	if writer.basePath != tmpDir {
		t.Errorf("Expected basePath %s, got %s", tmpDir, writer.basePath)
	}

	if writer.schemaManager != schemaManager {
		t.Error("Expected schemaManager to be set")
	}

	if writer.writers == nil {
		t.Error("Expected writers map to be initialized")
	}
}

func TestNewWriterInvalidPath(t *testing.T) {
	// Test with invalid path
	_, err := NewWriter("/invalid/path/that/does/not/exist", &schema.Manager{})
	if err == nil {
		t.Error("Expected error with invalid path")
	}
}

func TestWriterStruct(t *testing.T) {
	writer := &Writer{
		basePath:      "/test/path",
		writers:       make(map[uint32]*tableWriter),
		schemaManager: &schema.Manager{},
	}

	if writer.basePath != "/test/path" {
		t.Errorf("Expected basePath '/test/path', got '%s'", writer.basePath)
	}

	if writer.writers == nil {
		t.Error("Expected writers map to be initialized")
	}

	if writer.schemaManager == nil {
		t.Error("Expected schemaManager to be set")
	}
}

func TestTableWriterStruct(t *testing.T) {
	tw := &tableWriter{
		schema:        &SchemaV2{},
		parquetSchema: nil,
		writer:        nil,
		path:          "/test/path",
		records:       0,
		metadata:      &TableMetadata{},
		manifests:     []ManifestEntry{},
	}

	if tw.schema == nil {
		t.Error("Expected schema to be set")
	}

	if tw.path != "/test/path" {
		t.Errorf("Expected path '/test/path', got '%s'", tw.path)
	}

	if tw.records != 0 {
		t.Errorf("Expected records 0, got %d", tw.records)
	}

	if tw.metadata == nil {
		t.Error("Expected metadata to be set")
	}

	if tw.manifests == nil {
		t.Error("Expected manifests to be initialized")
	}
}

func TestSchemaV2(t *testing.T) {
	schema := SchemaV2{
		SchemaID: 1,
		Fields: []Field{
			{
				ID:       1,
				Name:     "id",
				Type:     "int",
				Required: true,
			},
			{
				ID:       2,
				Name:     "name",
				Type:     "string",
				Required: false,
			},
		},
	}

	if schema.SchemaID != 1 {
		t.Errorf("Expected SchemaID 1, got %d", schema.SchemaID)
	}

	if len(schema.Fields) != 2 {
		t.Errorf("Expected 2 fields, got %d", len(schema.Fields))
	}

	if schema.Fields[0].Name != "id" {
		t.Errorf("Expected first field name 'id', got '%s'", schema.Fields[0].Name)
	}

	if schema.Fields[1].Name != "name" {
		t.Errorf("Expected second field name 'name', got '%s'", schema.Fields[1].Name)
	}
}

func TestField(t *testing.T) {
	field := Field{
		ID:       1,
		Name:     "test_field",
		Type:     "string",
		Required: true,
	}

	if field.ID != 1 {
		t.Errorf("Expected ID 1, got %d", field.ID)
	}

	if field.Name != "test_field" {
		t.Errorf("Expected Name 'test_field', got '%s'", field.Name)
	}

	if field.Type != "string" {
		t.Errorf("Expected Type 'string', got '%s'", field.Type)
	}

	if !field.Required {
		t.Error("Expected Required to be true")
	}
}

func TestTableMetadata(t *testing.T) {
	metadata := &TableMetadata{
		FormatVersion: 2,
		TableUUID:     "test-uuid",
		Location:      "/test/location",
		LastUpdated:   1234567890,
		LastColumnID:  5,
		SchemaID:      1,
		Schemas:       []SchemaV2{},
		CurrentSchema: SchemaV2{},
		PartitionSpec: []PartitionSpec{},
		Properties:    map[string]string{},
		CurrentSnapshot: &Snapshot{},
		Snapshots:     []*Snapshot{},
	}

	if metadata.FormatVersion != 2 {
		t.Errorf("Expected FormatVersion 2, got %d", metadata.FormatVersion)
	}

	if metadata.TableUUID != "test-uuid" {
		t.Errorf("Expected TableUUID 'test-uuid', got '%s'", metadata.TableUUID)
	}

	if metadata.Location != "/test/location" {
		t.Errorf("Expected Location '/test/location', got '%s'", metadata.Location)
	}

	if metadata.LastUpdated != 1234567890 {
		t.Errorf("Expected LastUpdated 1234567890, got %d", metadata.LastUpdated)
	}
}

func TestSnapshot(t *testing.T) {
	snapshot := &Snapshot{
		SnapshotID:       1234567890,
		ParentSnapshotID: 0,
		SequenceNumber:   1,
		TimestampMs:      1234567890,
		ManifestList:     "/test/manifest",
		Summary:          map[string]string{},
	}

	if snapshot.SnapshotID != 1234567890 {
		t.Errorf("Expected SnapshotID 1234567890, got %d", snapshot.SnapshotID)
	}

	if snapshot.ParentSnapshotID != 0 {
		t.Errorf("Expected ParentSnapshotID 0, got %d", snapshot.ParentSnapshotID)
	}

	if snapshot.SequenceNumber != 1 {
		t.Errorf("Expected SequenceNumber 1, got %d", snapshot.SequenceNumber)
	}

	if snapshot.TimestampMs != 1234567890 {
		t.Errorf("Expected TimestampMs 1234567890, got %d", snapshot.TimestampMs)
	}

	if snapshot.ManifestList != "/test/manifest" {
		t.Errorf("Expected ManifestList '/test/manifest', got '%s'", snapshot.ManifestList)
	}
}

func TestPostgresTypeToIceberg(t *testing.T) {
	testCases := []struct {
		pgTypeOID uint32
		expected  string
	}{
		{23, "int"},    // INT4
		{20, "int"},    // INT8
		{25, "string"}, // TEXT
		{701, "double"}, // FLOAT8
		{16, "boolean"}, // BOOL
		{1082, "date"}, // DATE
		{1114, "timestamp"}, // TIMESTAMP
		{1700, "double"}, // NUMERIC
		{17, "binary"}, // BYTEA
		{9999, "string"}, // Unknown type
	}

	for _, tc := range testCases {
		result := postgresTypeToIceberg(tc.pgTypeOID)
		if result != tc.expected {
			t.Errorf("For OID %d, expected '%s', got '%s'", tc.pgTypeOID, tc.expected, result)
		}
	}
}

func TestCreateParquetSchema(t *testing.T) {
	schema := SchemaV2{
		SchemaID: 1,
		Fields: []Field{
			{ID: 1, Name: "id", Type: "int", Required: true},
			{ID: 2, Name: "name", Type: "string", Required: false},
			{ID: 3, Name: "value", Type: "double", Required: true},
		},
	}

	parquetSchema, err := createParquetSchema(schema)
	if err != nil {
		t.Fatalf("Failed to create parquet schema: %v", err)
	}

	if parquetSchema == nil {
		t.Fatal("Expected parquet schema to be created")
	}
}

func TestCreateParquetSchemaUnsupportedType(t *testing.T) {
	schema := SchemaV2{
		SchemaID: 1,
		Fields: []Field{
			{ID: 1, Name: "test", Type: "unsupported_type", Required: true},
		},
	}

	_, err := createParquetSchema(schema)
	if err == nil {
		t.Error("Expected error for unsupported type")
	}
}
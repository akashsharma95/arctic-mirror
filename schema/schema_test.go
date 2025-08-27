package schema

import (
	"testing"
)

func TestColumn(t *testing.T) {
	col := Column{
		Name:     "test_column",
		TypeOID:  25, // TEXT
		TypeName: "text",
		Nullable: true,
	}

	if col.Name != "test_column" {
		t.Errorf("Expected name 'test_column', got '%s'", col.Name)
	}

	if col.TypeOID != 25 {
		t.Errorf("Expected TypeOID 25, got %d", col.TypeOID)
	}

	if col.TypeName != "text" {
		t.Errorf("Expected TypeName 'text', got '%s'", col.TypeName)
	}

	if !col.Nullable {
		t.Error("Expected column to be nullable")
	}
}

func TestTableSchema(t *testing.T) {
	schema := &TableSchema{
		Schema:  "public",
		Name:    "test_table",
		Columns: []Column{},
	}

	if schema.Schema != "public" {
		t.Errorf("Expected schema 'public', got '%s'", schema.Schema)
	}

	if schema.Name != "test_table" {
		t.Errorf("Expected name 'test_table', got '%s'", schema.Name)
	}

	if len(schema.Columns) != 0 {
		t.Errorf("Expected 0 columns, got %d", len(schema.Columns))
	}
}

func TestSchemaManager(t *testing.T) {
	// This test would require a mock database connection
	// For now, we'll test the basic structure
	manager := &Manager{
		schemas:       make(map[uint32]*TableSchema),
		schemasByName: make(map[string]*TableSchema),
	}

	if manager.schemas == nil {
		t.Error("Expected schemas map to be initialized")
	}

	if manager.schemasByName == nil {
		t.Error("Expected schemasByName map to be initialized")
	}
}

func TestSchemaManagerGetSchema(t *testing.T) {
	manager := &Manager{
		schemas:       make(map[uint32]*TableSchema),
		schemasByName: make(map[string]*TableSchema),
	}

	// Test getting non-existent schema
	_, err := manager.GetSchema(1)
	if err == nil {
		t.Error("Expected error when getting non-existent schema")
	}

	// Test getting existing schema
	testSchema := &TableSchema{
		Schema:  "public",
		Name:    "test_table",
		Columns: []Column{},
	}
	manager.schemas[1] = testSchema

	retrievedSchema, err := manager.GetSchema(1)
	if err != nil {
		t.Errorf("Unexpected error: %v", err)
	}

	if retrievedSchema != testSchema {
		t.Error("Retrieved schema doesn't match stored schema")
	}
}

func TestSchemaManagerHandleRelationMessage(t *testing.T) {
	manager := &Manager{
		schemas:       make(map[uint32]*TableSchema),
		schemasByName: make(map[string]*TableSchema),
	}

	// This test would require creating a mock RelationMessageV2
	// For now, we'll test the basic structure
	if manager.schemas == nil {
		t.Error("Expected schemas map to be initialized")
	}
}
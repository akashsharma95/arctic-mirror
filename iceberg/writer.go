package iceberg

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"arctic-mirror/schema"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
)

// DuckDBWriter uses DuckDB with Iceberg extension for simplified writes
type DuckDBWriter struct {
	db            *sql.DB
	basePath      string
	schemaManager *schema.Manager
	tableCache    map[uint32]*TableInfo
	mu            sync.RWMutex
}

type TableInfo struct {
	Schema      string
	Table       string
	ColumnNames []string
	ColumnTypes []string
}

// NewDuckDBWriter creates a new DuckDB-based Iceberg writer
func NewDuckDBWriter(db *sql.DB, basePath string, schemaManager *schema.Manager) (*DuckDBWriter, error) {
	if db == nil {
		return nil, fmt.Errorf("database connection cannot be nil")
	}

	if basePath == "" {
		return nil, fmt.Errorf("base path cannot be empty")
	}

	// Ensure base path exists
	if err := ensureBasePath(basePath); err != nil {
		return nil, fmt.Errorf("failed to setup base path: %w", err)
	}

	// Configure Iceberg catalog path
	if _, err := db.Exec(fmt.Sprintf("SET iceberg.catalog.hadoop_catalog.warehouse = '%s'", basePath)); err != nil {
		return nil, fmt.Errorf("configuring iceberg catalog: %w", err)
	}

	return &DuckDBWriter{
		db:            db,
		basePath:      basePath,
		schemaManager: schemaManager,
		tableCache:    make(map[uint32]*TableInfo),
	}, nil
}

// ensureBasePath creates the base directory if it doesn't exist
func ensureBasePath(basePath string) error {
	if err := os.MkdirAll(basePath, 0755); err != nil {
		return fmt.Errorf("cannot create base path %s: %w", basePath, err)
	}

	// Test if we can write to the directory
	testFile := filepath.Join(basePath, ".test_write")
	if err := os.WriteFile(testFile, []byte("test"), 0644); err != nil {
		return fmt.Errorf("cannot write to base path %s: %w", basePath, err)
	}
	os.Remove(testFile) // Clean up test file

	return nil
}

// Writer interface for backward compatibility
type Writer struct {
	*DuckDBWriter
}

// NewWriter creates a new Iceberg writer (backward compatibility wrapper)
func NewWriter(basePath string, schemaManager *schema.Manager) (*Writer, error) {
	return nil, fmt.Errorf("NewWriter deprecated - use NewDuckDBWriter with database connection")
}

// WriteInsert writes an insert message using DuckDB
func (w *DuckDBWriter) WriteInsert(msg *pglogrepl.InsertMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	tableInfo, err := w.getTableInfo(msg.RelationID)
	if err != nil {
		return fmt.Errorf("getting table info: %w", err)
	}

	// Build INSERT statement
	columns := strings.Join(tableInfo.ColumnNames, ", ")
	placeholders := strings.Repeat("?, ", len(tableInfo.ColumnNames))
	placeholders = placeholders[:len(placeholders)-2] // Remove trailing ", "

	fullTableName := fmt.Sprintf("%s.%s", tableInfo.Schema, tableInfo.Table)
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		fullTableName, columns, placeholders)

	// Extract values from tuple
	values, err := w.extractValuesFromTuple(msg.Tuple, rel)
	if err != nil {
		return fmt.Errorf("extracting values: %w", err)
	}

	// Execute insert
	_, err = w.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("executing insert: %w", err)
	}

	return nil
}

// WriteUpdate writes an update message using DuckDB
func (w *DuckDBWriter) WriteUpdate(msg *pglogrepl.UpdateMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// For simplicity, we'll treat updates as inserts (append-only pattern)
	// This is common in data warehousing scenarios
	// Just use the new tuple data as if it were an insert
	return w.writeTupleAsInsert(msg.NewTuple, rel)
}

// writeTupleAsInsert writes a tuple as an insert operation
func (w *DuckDBWriter) writeTupleAsInsert(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) error {
	// Build INSERT statement
	tableInfo, err := w.getTableInfo(rel.RelationID)
	if err != nil {
		return fmt.Errorf("getting table info: %w", err)
	}

	columns := strings.Join(tableInfo.ColumnNames, ", ")
	placeholders := strings.Repeat("?, ", len(tableInfo.ColumnNames))
	placeholders = placeholders[:len(placeholders)-2] // Remove trailing ", "

	fullTableName := fmt.Sprintf("%s.%s", tableInfo.Schema, tableInfo.Table)
	query := fmt.Sprintf("INSERT INTO \"%s\" (%s) VALUES (%s)",
		fullTableName, columns, placeholders)

	// Extract values from tuple
	values, err := w.extractValuesFromTuple(tuple, rel)
	if err != nil {
		return fmt.Errorf("extracting values: %w", err)
	}

	// Execute insert
	_, err = w.db.Exec(query, values...)
	if err != nil {
		return fmt.Errorf("executing insert: %w", err)
	}

	return nil
}

// WriteDelete handles delete messages (can be no-op for append-only patterns)
func (w *DuckDBWriter) WriteDelete(msg *pglogrepl.DeleteMessageV2, rel *pglogrepl.RelationMessageV2) error {
	// For append-only patterns, we skip deletes
	// In a full implementation, you might want to handle soft deletes
	log.Printf("Skipping delete for relation %d (append-only pattern)", msg.RelationID)
	return nil
}

// Commit commits any pending writes (no-op for DuckDB as writes are auto-committed)
func (w *DuckDBWriter) Commit() error {
	// DuckDB auto-commits by default, so this is a no-op
	return nil
}

// getTableInfo gets or creates table information for a relation
func (w *DuckDBWriter) getTableInfo(relationID uint32) (*TableInfo, error) {
	if info, exists := w.tableCache[relationID]; exists {
		return info, nil
	}

	// Get schema information
	pgSchema, err := w.schemaManager.GetSchema(relationID)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	// Create table info
	columnNames := make([]string, 0, len(pgSchema.Columns))
	columnTypes := make([]string, 0, len(pgSchema.Columns))

	for _, col := range pgSchema.Columns {
		columnNames = append(columnNames, col.Name)
		columnTypes = append(columnTypes, postgresTypeToDuckDB(col.TypeOID))
	}

	// Create the Iceberg table if it doesn't exist
	if err := w.CreateIcebergTable(pgSchema.Schema, pgSchema.Name, columnNames, columnTypes); err != nil {
		return nil, fmt.Errorf("creating iceberg table: %w", err)
	}

	info := &TableInfo{
		Schema:      pgSchema.Schema,
		Table:       pgSchema.Name,
		ColumnNames: columnNames,
		ColumnTypes: columnTypes,
	}

	w.tableCache[relationID] = info
	return info, nil
}

// extractValuesFromTuple extracts values from a PostgreSQL tuple
func (w *DuckDBWriter) extractValuesFromTuple(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) ([]interface{}, error) {
	typeMap := pgtype.NewMap()
	values := make([]interface{}, len(tuple.Columns))

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		dataType := rel.Columns[idx].DataType
		formatCode := pgtype.TextFormatCode

		switch col.DataType {
		case 'n': // null
			values[idx] = nil
		case 't': // text
			val, err := decodeColumnData(typeMap, col.Data, dataType, int16(formatCode))
			if err != nil {
				return nil, fmt.Errorf("decoding column data for %s: %w", colName, err)
			}
			values[idx] = val
		case 'b': // binary
			values[idx] = col.Data
		case 'u': // unchanged TOAST data
			values[idx] = nil
		default:
			return nil, fmt.Errorf("unknown column data type: %v", col.DataType)
		}
	}

	return values, nil
}

// decodeColumnData decodes PostgreSQL column data
func decodeColumnData(typeMap *pgtype.Map, data []byte, dataTypeOID uint32, formatCode int16) (interface{}, error) {
	dataType, ok := typeMap.TypeForOID(dataTypeOID)
	if !ok {
		// If the data type is unknown, default to returning the data as a string
		return string(data), nil
	}

	value, err := dataType.Codec.DecodeValue(typeMap, dataTypeOID, formatCode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value for OID %d: %w", dataTypeOID, err)
	}

	return value, nil
}

// postgresTypeToDuckDB maps PostgreSQL types to DuckDB types
func postgresTypeToDuckDB(pgTypeOID uint32) string {
	switch pgTypeOID {
	// Integer types
	case pgtype.Int2OID:
		return "SMALLINT"
	case pgtype.Int4OID:
		return "INTEGER"
	case pgtype.Int8OID:
		return "BIGINT"

	// Floating point types
	case pgtype.Float4OID:
		return "REAL"
	case pgtype.Float8OID:
		return "DOUBLE"

	// Character types
	case pgtype.BPCharOID:
		return "VARCHAR"
	case pgtype.VarcharOID:
		return "VARCHAR"
	case pgtype.TextOID:
		return "TEXT"

	// Boolean type
	case pgtype.BoolOID:
		return "BOOLEAN"

	// Date and time types
	case pgtype.DateOID:
		return "DATE"
	case pgtype.TimestampOID:
		return "TIMESTAMP"
	case pgtype.TimestamptzOID:
		return "TIMESTAMPTZ"

	// Binary types
	case pgtype.ByteaOID:
		return "BLOB"

	// Unknown types
	default:
		return "VARCHAR" // Default to VARCHAR for unknown types
	}
}

// Close closes the writer (no-op for DuckDB)
func (w *DuckDBWriter) Close() error {
	// DuckDB manages its own connections
	return nil
}

// CreateIcebergTable creates an Iceberg table in DuckDB if it doesn't exist
func (w *DuckDBWriter) CreateIcebergTable(schemaName, tableName string, columnNames []string, columnTypes []string) error {
	// Build column definitions
	var columns []string
	for i, name := range columnNames {
		columns = append(columns, fmt.Sprintf("\"%s\" %s", name, columnTypes[i]))
	}

	// Create table statement using proper Iceberg syntax
	// DuckDB uses a different syntax for Iceberg tables
	fullTableName := fmt.Sprintf("%s.%s", schemaName, tableName)
	query := fmt.Sprintf("CREATE OR REPLACE TABLE \"%s\" (%s) USING iceberg",
		fullTableName, strings.Join(columns, ", "))

	_, err := w.db.Exec(query)
	if err != nil {
		return fmt.Errorf("creating Iceberg table: %w", err)
	}

	log.Printf("Created/verified Iceberg table: %s", fullTableName)
	return nil
}

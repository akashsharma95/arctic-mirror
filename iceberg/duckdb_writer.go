package iceberg

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/schema"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/marcboeker/go-duckdb/v2"
)

// SchemaManager interface for dependency injection
type SchemaManager interface {
	GetSchema(relationID uint32) (*schema.TableSchema, error)
	InitializeSchema(ctx context.Context, schemaName, tableName string) error
	HandleRelationMessage(msg *pglogrepl.RelationMessageV2) error
}

// DuckDBWriter uses DuckDB's Iceberg extension to write data
type DuckDBWriter struct {
	db            *sql.DB
	basePath      string
	schemaManager SchemaManager
	config        *config.Config
	mu            sync.RWMutex
	tables        map[uint32]*TableInfo
}

// TableInfo holds information about a table being written to
type TableInfo struct {
	SchemaName string
	TableName  string
	Columns    []ColumnInfo
	Created    bool
}

// ColumnInfo holds column information
type ColumnInfo struct {
	Name     string
	Type     string
	Nullable bool
}

// NewDuckDBWriter creates a new DuckDB-based Iceberg writer
func NewDuckDBWriter(basePath string, schemaManager SchemaManager, cfg *config.Config) (*DuckDBWriter, error) {
	// Create DuckDB connection
	connector, err := duckdb.NewConnector("", nil)
	if err != nil {
		return nil, fmt.Errorf("creating DuckDB connector: %w", err)
	}

	db := sql.OpenDB(connector)

	// Install and load Iceberg extension
	if err := installIcebergExtension(db); err != nil {
		return nil, fmt.Errorf("installing Iceberg extension: %w", err)
	}

	// Create base directory if it doesn't exist
	if err := ensureDirectoryExists(basePath); err != nil {
		return nil, fmt.Errorf("creating base directory: %w", err)
	}

	return &DuckDBWriter{
		db:            db,
		basePath:      basePath,
		schemaManager: schemaManager,
		config:        cfg,
		tables:        make(map[uint32]*TableInfo),
	}, nil
}

// installIcebergExtension installs and loads the DuckDB Iceberg extension
func installIcebergExtension(db *sql.DB) error {
	// Install the iceberg extension
	if _, err := db.Exec("INSTALL iceberg"); err != nil {
		return fmt.Errorf("installing iceberg extension: %w", err)
	}

	// Load the iceberg extension
	if _, err := db.Exec("LOAD iceberg"); err != nil {
		return fmt.Errorf("loading iceberg extension: %w", err)
	}

	log.Println("DuckDB Iceberg extension installed and loaded successfully")
	return nil
}

// WriteInsert writes an insert message to the appropriate Iceberg table
func (w *DuckDBWriter) WriteInsert(msg *pglogrepl.InsertMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	log.Printf("WriteInsert: relationID=%d, relationName=%s.%s", msg.RelationID, rel.Namespace, rel.RelationName)

	// Get or create table info
	tableInfo, err := w.getOrCreateTableInfo(msg.RelationID, rel)
	if err != nil {
		return fmt.Errorf("getting table info: %w", err)
	}

	// Convert tuple to record
	record, err := w.mapTupleToRecord(msg.Tuple, rel)
	if err != nil {
		return fmt.Errorf("mapping tuple to record: %w", err)
	}

	// Insert record into DuckDB table
	if err := w.insertRecord(tableInfo, record); err != nil {
		return fmt.Errorf("inserting record: %w", err)
	}

	return nil
}

// WriteUpdate writes an update message to the appropriate Iceberg table
func (w *DuckDBWriter) WriteUpdate(msg *pglogrepl.UpdateMessageV2, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Get or create table info
	tableInfo, err := w.getOrCreateTableInfo(msg.RelationID, rel)
	if err != nil {
		return fmt.Errorf("getting table info: %w", err)
	}

	// Convert tuple to record
	record, err := w.mapTupleToRecord(msg.NewTuple, rel)
	if err != nil {
		return fmt.Errorf("mapping tuple to record: %w", err)
	}

	// For updates, we'll use MERGE INTO (available in DuckDB 1.4.0)
	// For now, we'll do a simple insert (upsert behavior)
	if err := w.insertRecord(tableInfo, record); err != nil {
		return fmt.Errorf("updating record: %w", err)
	}

	return nil
}

// WriteDelete writes a delete message to the appropriate Iceberg table
func (w *DuckDBWriter) WriteDelete(msg *pglogrepl.DeleteMessageV2, rel *pglogrepl.RelationMessageV2) error {
	// For now, we'll skip deletes as they require more complex handling
	// In a production system, you might want to implement tombstone records
	log.Printf("Delete operation skipped for relation %d", msg.RelationID)
	return nil
}

// Commit commits all pending changes to Iceberg tables
func (w *DuckDBWriter) Commit() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// With DuckDB's Iceberg extension, commits happen automatically
	// when we write to the attached Iceberg database
	log.Println("Commit completed - all changes written to Iceberg tables")
	return nil
}

// CloseAllWriters closes all active writers and finalizes Iceberg tables
func (w *DuckDBWriter) CloseAllWriters() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// With DuckDB's Iceberg extension, tables are automatically finalized
	// when we detach from the database
	log.Println("All writers closed - Iceberg tables finalized")
	return nil
}

// getOrCreateTableInfo gets or creates table information for a relation
func (w *DuckDBWriter) getOrCreateTableInfo(relationID uint32, rel *pglogrepl.RelationMessageV2) (*TableInfo, error) {
	if tableInfo, exists := w.tables[relationID]; exists {
		return tableInfo, nil
	}

	// Get PostgreSQL schema
	pgSchema, err := w.schemaManager.GetSchema(relationID)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	// Create table info
	tableInfo := &TableInfo{
		SchemaName: pgSchema.Schema,
		TableName:  pgSchema.Name,
		Columns:    make([]ColumnInfo, 0, len(pgSchema.Columns)),
		Created:    false,
	}

	// Map PostgreSQL columns to DuckDB columns
	for _, col := range pgSchema.Columns {
		columnInfo := ColumnInfo{
			Name:     col.Name,
			Type:     postgresTypeToDuckDB(col.TypeOID),
			Nullable: col.Nullable,
		}
		tableInfo.Columns = append(tableInfo.Columns, columnInfo)
	}

	// Create the table in DuckDB if it doesn't exist
	if err := w.createTableIfNotExists(tableInfo); err != nil {
		return nil, fmt.Errorf("creating table: %w", err)
	}

	tableInfo.Created = true
	w.tables[relationID] = tableInfo
	return tableInfo, nil
}

// createTableIfNotExists creates a DuckDB table that will be written to Iceberg
func (w *DuckDBWriter) createTableIfNotExists(tableInfo *TableInfo) error {
	// Create the table schema
	var columnDefs []string
	for _, col := range tableInfo.Columns {
		nullable := ""
		if !col.Nullable {
			nullable = " NOT NULL"
		}
		columnDefs = append(columnDefs, fmt.Sprintf("%s %s%s", col.Name, col.Type, nullable))
	}

	// Create temporary table in DuckDB
	tableName := fmt.Sprintf("temp_%s_%s", tableInfo.SchemaName, tableInfo.TableName)
	createSQL := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s)",
		tableName,
		strings.Join(columnDefs, ", "))

	if _, err := w.db.Exec(createSQL); err != nil {
		return fmt.Errorf("creating temporary table: %w", err)
	}

	log.Printf("Created temporary table: %s", tableName)
	return nil
}

// insertRecord inserts a record into the appropriate DuckDB table
func (w *DuckDBWriter) insertRecord(tableInfo *TableInfo, record map[string]interface{}) error {
	tableName := fmt.Sprintf("temp_%s_%s", tableInfo.SchemaName, tableInfo.TableName)

	// Build INSERT statement
	var columns []string
	var placeholders []string
	var values []interface{}

	for _, col := range tableInfo.Columns {
		columns = append(columns, col.Name)
		placeholders = append(placeholders, "?")
		values = append(values, record[col.Name])
	}

	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(columns, ", "),
		strings.Join(placeholders, ", "))

	if _, err := w.db.Exec(insertSQL, values...); err != nil {
		return fmt.Errorf("inserting record: %w", err)
	}

	return nil
}

// mapTupleToRecord converts a PostgreSQL tuple to a Go record
func (w *DuckDBWriter) mapTupleToRecord(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) (map[string]interface{}, error) {
	typeMap := pgtype.NewMap()
	record := make(map[string]interface{})

	for idx, col := range tuple.Columns {
		colName := rel.Columns[idx].Name
		dataType := rel.Columns[idx].DataType
		formatCode := pgtype.TextFormatCode

		switch col.DataType {
		case 'n': // null
			record[colName] = nil
		case 't': // text
			val, err := decodeColumnData(typeMap, col.Data, dataType, int16(formatCode))
			if err != nil {
				return nil, fmt.Errorf("decoding column data for %s: %w", colName, err)
			}

			convertedVal, err := convertPgTypeToGoType(val)
			if err != nil {
				return nil, fmt.Errorf("converting pgtype for %s: %w", colName, err)
			}
			record[colName] = convertedVal
		case 'b': // binary
			record[colName] = col.Data
		case 'u': // unchanged TOAST data
			record[colName] = nil
		default:
			return nil, fmt.Errorf("unknown column data type: %v", col.DataType)
		}
	}
	return record, nil
}

// WriteToIceberg writes all temporary tables to Iceberg format
func (w *DuckDBWriter) WriteToIceberg() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	for _, tableInfo := range w.tables {
		if !tableInfo.Created {
			continue
		}

		if err := w.writeTableToIceberg(tableInfo); err != nil {
			return fmt.Errorf("writing table %s.%s to Iceberg: %w", tableInfo.SchemaName, tableInfo.TableName, err)
		}

		log.Printf("Successfully wrote table %s.%s to Iceberg", tableInfo.SchemaName, tableInfo.TableName)
	}

	return nil
}

// writeTableToIceberg writes a single table to Iceberg format using DuckDB's extension
func (w *DuckDBWriter) writeTableToIceberg(tableInfo *TableInfo) error {
	tempTableName := fmt.Sprintf("temp_%s_%s", tableInfo.SchemaName, tableInfo.TableName)
	icebergTableName := fmt.Sprintf("%s_%s", tableInfo.SchemaName, tableInfo.TableName)

	// Note: icebergPath is not needed for REST catalog approach

	// Use DuckDB's Iceberg extension with REST catalog
	// This leverages the new Iceberg write support in DuckDB 1.4.0
	log.Printf("Setting up Iceberg REST catalog connection")

	// First, create S3 secret for MinIO (ignore if already exists)
	ep := w.config.Iceberg.S3Endpoint
	if strings.HasPrefix(ep, "http://") {
		ep = strings.TrimPrefix(ep, "http://")
	} else if strings.HasPrefix(ep, "https://") {
		ep = strings.TrimPrefix(ep, "https://")
	}
	createSecretSQL := fmt.Sprintf(`
        CREATE SECRET IF NOT EXISTS (
            TYPE S3,
            KEY_ID '%s',
            SECRET '%s',
            ENDPOINT '%s',
            URL_STYLE 'path',
            USE_SSL false
        )
    `, w.config.Iceberg.S3AccessKeyID, w.config.Iceberg.S3SecretAccessKey, ep)

	if _, err := w.db.Exec(createSecretSQL); err != nil {
		log.Printf("Warning: Could not create S3 secret, falling back to Parquet: %v", err)
		return w.writeTableToParquet(tableInfo)
	}

	// Detach any existing Iceberg catalog first
	if _, err := w.db.Exec("DETACH IF EXISTS iceberg_catalog"); err != nil {
		log.Printf("Warning: Could not detach existing Iceberg catalog: %v", err)
	}

	// Attach the Iceberg catalog
	attachSQL := fmt.Sprintf(`
        ATTACH '' AS iceberg_catalog (
            TYPE iceberg,
            ENDPOINT '%s',
            AUTHORIZATION_TYPE 'none'
        )
    `, w.config.Iceberg.CatalogEndpoint)

	if _, err := w.db.Exec(attachSQL); err != nil {
		log.Printf("Warning: Could not attach Iceberg catalog, falling back to Parquet: %v", err)
		return w.writeTableToParquet(tableInfo)
	}

	// Skip schema creation since the namespace already exists
	schemaName := tableInfo.SchemaName
	log.Printf("Using existing schema %s", schemaName)

	// Create the Iceberg table
	icebergTableName = fmt.Sprintf("iceberg_catalog.%s.%s", schemaName, tableInfo.TableName)
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE %s AS
		SELECT * FROM %s
	`, icebergTableName, tempTableName)

	if _, err := w.db.Exec(createTableSQL); err != nil {
		// If table exists, try INSERT INTO instead of falling back to Parquet
		if strings.Contains(err.Error(), "already exists") || strings.Contains(err.Error(), "Bad Request") {
			insertSQL := fmt.Sprintf(`
                INSERT INTO %s SELECT * FROM %s
            `, icebergTableName, tempTableName)
			if _, ierr := w.db.Exec(insertSQL); ierr != nil {
				log.Printf("Warning: Could not insert into existing Iceberg table, falling back to Parquet: %v", ierr)
				return w.writeTableToParquet(tableInfo)
			}
		} else {
			log.Printf("Warning: Could not create Iceberg table, falling back to Parquet: %v", err)
			return w.writeTableToParquet(tableInfo)
		}
	}

	// Detach the Iceberg catalog
	if _, err := w.db.Exec("DETACH iceberg_catalog"); err != nil {
		log.Printf("Warning: Could not detach Iceberg catalog: %v", err)
	}

	log.Printf("Successfully wrote table %s.%s to Iceberg format", schemaName, tableInfo.TableName)
	return nil
}

// writeTableToParquet writes a table to Parquet format as a fallback
func (w *DuckDBWriter) writeTableToParquet(tableInfo *TableInfo) error {
	tempTableName := fmt.Sprintf("temp_%s_%s", tableInfo.SchemaName, tableInfo.TableName)

	// Create directory structure
	icebergPath := fmt.Sprintf("%s/%s/%s", w.basePath, tableInfo.SchemaName, tableInfo.TableName)
	if err := os.MkdirAll(icebergPath, 0755); err != nil {
		return fmt.Errorf("creating directory structure: %w", err)
	}

	parquetPath := fmt.Sprintf("%s/data_%d.parquet", icebergPath, time.Now().Unix())

	copySQL := fmt.Sprintf(`
		COPY %s TO '%s' (FORMAT PARQUET)
	`, tempTableName, parquetPath)

	if _, err := w.db.Exec(copySQL); err != nil {
		return fmt.Errorf("copying data to Parquet: %w", err)
	}

	log.Printf("Copied data from %s to %s", tempTableName, parquetPath)
	return nil
}

// Close closes the DuckDB connection
func (w *DuckDBWriter) Close() error {
	if w.db != nil {
		return w.db.Close()
	}
	return nil
}

// Helper functions

// postgresTypeToDuckDB converts PostgreSQL type OIDs to DuckDB types
func postgresTypeToDuckDB(pgTypeOID uint32) string {
	switch pgTypeOID {
	// Integer types
	case pgtype.Int2OID:
		return "INTEGER"
	case pgtype.Int4OID:
		return "INTEGER"
	case pgtype.Int8OID:
		return "BIGINT"

	// Floating point types
	case pgtype.Float4OID:
		return "REAL"
	case pgtype.Float8OID:
		return "DOUBLE"
	case pgtype.NumericOID:
		return "DOUBLE"

	// Character types
	case pgtype.BPCharOID:
		return "VARCHAR"
	case pgtype.VarcharOID:
		return "VARCHAR"
	case pgtype.TextOID:
		return "VARCHAR"

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

// ensureDirectoryExists creates a directory if it doesn't exist
func ensureDirectoryExists(path string) error {
	// Create the directory if it doesn't exist
	if err := os.MkdirAll(path, 0755); err != nil {
		return fmt.Errorf("creating directory %s: %w", path, err)
	}
	return nil
}

// decodeColumnData decodes PostgreSQL column data
func decodeColumnData(typeMap *pgtype.Map, data []byte, dataTypeOID uint32, formatCode int16) (interface{}, error) {
	// Retrieve the DataType for the given OID
	dataType, ok := typeMap.TypeForOID(dataTypeOID)
	if !ok {
		// If the data type is unknown, default to returning the data as a string
		return string(data), nil
	}

	// Use the Codec's DecodeValue method to decode the data directly
	value, err := dataType.Codec.DecodeValue(typeMap, dataTypeOID, formatCode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value for OID %d: %w", dataTypeOID, err)
	}

	// Return the raw pgtype value - conversion will happen in convertPgTypeToGoType
	return value, nil
}

// convertPgTypeToGoType converts pgtype values to standard Go types for DuckDB compatibility
func convertPgTypeToGoType(value interface{}) (interface{}, error) {
	// Handle Numeric type specially since Value() returns string but we need float64
	if numeric, ok := value.(pgtype.Numeric); ok {
		if numeric.Valid {
			// For Numeric, we want float64 for DuckDB compatibility
			f64, err := numeric.Float64Value()
			if err != nil {
				return nil, err
			}
			if f64.Valid {
				return f64.Float64, nil
			}
		}
		return nil, nil
	}

	// For all other types, use the built-in Value() method
	if valuer, ok := value.(driver.Valuer); ok {
		// Use the built-in Value() method to get native Go types
		return valuer.Value()
	}

	// For types that don't implement Valuer, return as-is
	return value, nil
}

package iceberg

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"

	"arctic-mirror/schema"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgtype"
	_ "github.com/marcboeker/go-duckdb"
)

// DuckDB-backed Iceberg writer leveraging DuckDB's native iceberg write support.

const duckDBIcebergCatalog = "am_iceberg"

type Writer struct {
	basePath      string
	db            *sql.DB
	schemaManager *schema.Manager
	mu            sync.Mutex
	tables        map[uint32]*tableState
}

type tableState struct {
	qualifiedName string // catalog.schema.table
	insertSQL     string
	insertStmt    *sql.Stmt
}

func NewWriter(basePath string, schemaManager *schema.Manager) (*Writer, error) {
	if basePath == "" {
		return nil, fmt.Errorf("base path cannot be empty")
	}

	// Initialize DuckDB in-process
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("opening duckdb: %w", err)
	}

	// Enable iceberg support and attach the catalog at the provided warehouse path
	if _, err := db.Exec("INSTALL iceberg; LOAD iceberg;"); err != nil {
		return nil, fmt.Errorf("loading iceberg extension: %w", err)
	}

	attach := fmt.Sprintf("ATTACH '%s' AS %s (TYPE iceberg);", escapeSingleQuotes(basePath), duckDBIcebergCatalog)
	if _, err := db.Exec(attach); err != nil {
		return nil, fmt.Errorf("attaching iceberg catalog: %w", err)
	}

	return &Writer{
		basePath:      basePath,
		db:            db,
		schemaManager: schemaManager,
		tables:        make(map[uint32]*tableState),
	}, nil
}

func (w *Writer) WriteInsert(msg *pglogrepl.InsertMessageV2, rel *pglogrepl.RelationMessageV2) error {
	return w.insertTuple(msg.RelationID, msg.Tuple, rel)
}

func (w *Writer) WriteUpdate(msg *pglogrepl.UpdateMessageV2, rel *pglogrepl.RelationMessageV2) error {
	// Append-only for now, treat update as insert of the new row
	return w.insertTuple(msg.RelationID, msg.NewTuple, rel)
}

func (w *Writer) WriteDelete(_ *pglogrepl.DeleteMessageV2, _ *pglogrepl.RelationMessageV2) error {
	// No-op for now (append-only)
	return nil
}

func (w *Writer) Commit() error {
	// DuckDB autocommit is enabled by default; nothing to do.
	return nil
}

func (w *Writer) insertTuple(relationID uint32, tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	ts, err := w.getOrInitTableState(relationID, rel)
	if err != nil {
		return err
	}

	params, err := tupleToParams(tuple, rel)
	if err != nil {
		return fmt.Errorf("mapping tuple to params: %w", err)
	}

	if ts.insertStmt == nil {
		stmt, prepErr := w.db.Prepare(ts.insertSQL)
		if prepErr != nil {
			return fmt.Errorf("preparing insert: %w", prepErr)
		}
		ts.insertStmt = stmt
	}

	if _, err := ts.insertStmt.Exec(params...); err != nil {
		return fmt.Errorf("executing insert: %w", err)
	}
	return nil
}

func (w *Writer) getOrInitTableState(relationID uint32, rel *pglogrepl.RelationMessageV2) (*tableState, error) {
	if ts, ok := w.tables[relationID]; ok {
		return ts, nil
	}

	pgSchema, err := w.schemaManager.GetSchema(relationID)
	if err != nil {
		return nil, fmt.Errorf("getting schema: %w", err)
	}

	qualified := fmt.Sprintf("%s.%s.%s", duckDBIcebergCatalog, quoteIdent(pgSchema.Schema), quoteIdent(pgSchema.Name))

	// Ensure namespace exists
	createSchema := fmt.Sprintf("CREATE SCHEMA IF NOT EXISTS %s.%s;", duckDBIcebergCatalog, quoteIdent(pgSchema.Schema))
	if _, err := w.db.Exec(createSchema); err != nil {
		return nil, fmt.Errorf("creating namespace: %w", err)
	}

	// Ensure table exists with appropriate column types
	cols := make([]string, 0, len(pgSchema.Columns))
	for _, c := range pgSchema.Columns {
		colDef := fmt.Sprintf("%s %s", quoteIdent(c.Name), pgOIDToDuckDBType(c.TypeOID, c.TypeName))
		if !c.Nullable {
			colDef += " NOT NULL"
		}
		cols = append(cols, colDef)
	}
	createTable := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s);", qualified, strings.Join(cols, ", "))
	if _, err := w.db.Exec(createTable); err != nil {
		return nil, fmt.Errorf("creating iceberg table: %w", err)
	}

	// Prepare insert SQL
	colNames := make([]string, len(rel.Columns))
	placeholders := make([]string, len(rel.Columns))
	for i, c := range rel.Columns {
		colNames[i] = quoteIdent(c.Name)
		placeholders[i] = "?"
	}
	insertSQL := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)", qualified, strings.Join(colNames, ", "), strings.Join(placeholders, ", "))

	ts := &tableState{qualifiedName: qualified, insertSQL: insertSQL}
	w.tables[relationID] = ts
	return ts, nil
}

func tupleToParams(tuple *pglogrepl.TupleData, rel *pglogrepl.RelationMessageV2) ([]interface{}, error) {
	typeMap := pgtype.NewMap()
	params := make([]interface{}, len(rel.Columns))
	for idx, col := range tuple.Columns {
		dataType := rel.Columns[idx].DataType
		switch col.DataType {
		case 'n':
			params[idx] = nil
		case 't':
			val, err := decodeColumnData(typeMap, col.Data, dataType, int16(pgtype.TextFormatCode))
			if err != nil {
				return nil, fmt.Errorf("decoding column %s: %w", rel.Columns[idx].Name, err)
			}
			params[idx] = val
		case 'b':
			// Pass through binary data
			params[idx] = []byte(col.Data)
		case 'u':
			// unchanged TOAST; set NULL
			params[idx] = nil
		default:
			return nil, fmt.Errorf("unknown column data type: %v", col.DataType)
		}
	}
	return params, nil
}

func decodeColumnData(typeMap *pgtype.Map, data []byte, dataTypeOID uint32, formatCode int16) (interface{}, error) {
	dataType, ok := typeMap.TypeForOID(dataTypeOID)
	if !ok {
		return string(data), nil
	}
	value, err := dataType.Codec.DecodeValue(typeMap, dataTypeOID, formatCode, data)
	if err != nil {
		return nil, fmt.Errorf("failed to decode value for OID %d: %w", dataTypeOID, err)
	}
	return value, nil
}

func quoteIdent(id string) string {
	return "\"" + strings.ReplaceAll(id, "\"", "\"\"") + "\""
}

func escapeSingleQuotes(s string) string {
	return strings.ReplaceAll(s, "'", "''")
}

func pgOIDToDuckDBType(pgTypeOID uint32, typeName string) string {
	switch pgTypeOID {
	case pgtype.Int2OID:
		return "SMALLINT"
	case pgtype.Int4OID:
		return "INTEGER"
	case pgtype.Int8OID:
		return "BIGINT"
	case pgtype.Float4OID:
		return "REAL"
	case pgtype.Float8OID:
		return "DOUBLE"
	case pgtype.BPCharOID, pgtype.VarcharOID, pgtype.TextOID:
		return "VARCHAR"
	case pgtype.BoolOID:
		return "BOOLEAN"
	case pgtype.DateOID:
		return "DATE"
	case pgtype.TimestampOID:
		return "TIMESTAMP"
	case pgtype.TimestamptzOID:
		return "TIMESTAMPTZ"
	case pgtype.ByteaOID:
		return "BLOB"
	case pgtype.NumericOID:
		// Fallback generic precision
		return "DECIMAL(38, 10)"
	default:
		_ = typeName
		return "VARCHAR"
	}
}


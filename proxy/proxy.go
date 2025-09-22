package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/metrics"

	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/marcboeker/go-duckdb/v2"
)

type DuckDBProxy struct {
	config             *config.Config
	db                 *sql.DB
	listener           net.Listener
	prepared           map[string]*preparedStmt
	portals            map[string]*portal
	registeredTables   map[string]bool // Track registered tables to avoid duplicates
	autoRegisterTicker *time.Ticker
	autoRegisterCtx    context.Context
	autoRegisterCancel context.CancelFunc
	mu                 sync.RWMutex // Mutex for thread-safe operations
}

type preparedStmt struct {
	name      string
	query     string
	paramOIDs []uint32
}

type portal struct {
	name           string
	prepared       *preparedStmt
	rewrittenQuery string
	params         []interface{}
}

func NewDuckDBProxy(cfg *config.Config) (*DuckDBProxy, error) {
	// Initialize DuckDB
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, fmt.Errorf("opening duckdb: %w", err)
	}

	// Install and load extensions
	if err := loadExtensions(db); err != nil {
		return nil, fmt.Errorf("loading extensions: %w", err)
	}

	// Create listener
	listener, err := net.Listen("tcp", fmt.Sprintf(":%d", cfg.Proxy.Port))
	if err != nil {
		return nil, fmt.Errorf("creating listener: %w", err)
	}

	proxy := &DuckDBProxy{
		config:           cfg,
		db:               db,
		listener:         listener,
		prepared:         make(map[string]*preparedStmt),
		portals:          make(map[string]*portal),
		registeredTables: make(map[string]bool),
	}

	// Register initial Iceberg tables
	if err := proxy.registerIcebergTables(); err != nil {
		return nil, fmt.Errorf("registering iceberg tables: %w", err)
	}

	// Start auto-registration of new tables
	proxy.startAutoRegistration()

	return proxy, nil
}

func loadExtensions(db *sql.DB) error {
	extensions := []string{"iceberg", "parquet"}
	for _, ext := range extensions {
		if _, err := db.Exec(fmt.Sprintf("INSTALL %s; LOAD %s;", ext, ext)); err != nil {
			return fmt.Errorf("loading extension %s: %w", ext, err)
		}
	}
	return nil
}

func (p *DuckDBProxy) registerIcebergTables() error {
	// Ensure S3 secret is configured for DuckDB
	if err := p.ensureS3Secret(); err != nil {
		return fmt.Errorf("ensuring s3 secret: %w", err)
	}

	// Register each configured table as a view in DuckDB against the catalog
	for _, table := range p.config.Tables {
		tableKey := fmt.Sprintf("%s.%s", table.Schema, table.Name)

		// Resolve metadata location from REST catalog
		metaURL := fmt.Sprintf("%s/v1/namespaces/%s/tables/%s", p.config.Iceberg.CatalogEndpoint, table.Schema, table.Name)
		resp, err := http.Get(metaURL)
		if err != nil {
			log.Printf("Error resolving table %s via catalog: %v", tableKey, err)
			continue
		}
		body, _ := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			log.Printf("Catalog returned %d for %s: %s", resp.StatusCode, metaURL, string(body))
			continue
		}
		// very small inline parser to extract metadata-location without full struct
		// look for \"metadata-location\":\"...\"
		bodyStr := string(body)
		idx := strings.Index(bodyStr, "\"metadata-location\":\"")
		if idx < 0 {
			log.Printf("metadata-location not found in catalog response for %s", tableKey)
			continue
		}
		start := idx + len("\"metadata-location\":\"")
		end := strings.Index(bodyStr[start:], "\"")
		if end < 0 {
			log.Printf("failed to parse metadata-location for %s", tableKey)
			continue
		}
		metadataLocation := bodyStr[start : start+end]

		// Create a view in DuckDB that queries the Iceberg table using metadata file on S3
		viewName := fmt.Sprintf("%s_%s", table.Schema, table.Name)

		// Drop view if it exists (in case of restart)
		dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
		if _, err := p.db.Exec(dropSQL); err != nil {
			log.Printf("Warning: failed to drop existing view %s: %v", viewName, err)
		}

		createSQL := fmt.Sprintf(`
            CREATE VIEW %s AS
            SELECT * FROM iceberg_scan('%s')
        `, viewName, metadataLocation)

		if _, err := p.db.Exec(createSQL); err != nil {
			log.Printf("Error creating view for %s.%s: %v", table.Schema, table.Name, err)
			// Skip this table and continue with others
			continue
		}

		log.Printf("Registered Iceberg table %s.%s as view %s",
			table.Schema, table.Name, viewName)

		// Mark table as registered
		p.registeredTables[tableKey] = true

		// Also create a view without schema prefix for easier access
		simpleViewName := table.Name
		dropSimpleSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", simpleViewName)
		if _, err := p.db.Exec(dropSimpleSQL); err != nil {
			log.Printf("Warning: failed to drop existing view %s: %v", simpleViewName, err)
		}

		createSimpleSQL := fmt.Sprintf(`
            CREATE VIEW %s AS
            SELECT * FROM iceberg_scan('%s')
        `, simpleViewName, metadataLocation)

		if _, err := p.db.Exec(createSimpleSQL); err != nil {
			log.Printf("Warning: failed to create simple view %s: %v", simpleViewName, err)
		}
	}

	return nil
}

// startAutoRegistration starts the background process to automatically register new tables
func (p *DuckDBProxy) startAutoRegistration() {
	p.autoRegisterCtx, p.autoRegisterCancel = context.WithCancel(context.Background())
	p.autoRegisterTicker = time.NewTicker(30 * time.Second) // Check every 30 seconds

	go func() {
		defer p.autoRegisterTicker.Stop()

		// Run initial registration
		if err := p.registerIcebergTables(); err != nil {
			log.Printf("Initial table registration failed: %v", err)
		}

		for {
			select {
			case <-p.autoRegisterCtx.Done():
				return
			case <-p.autoRegisterTicker.C:
				log.Printf("Auto-registration tick: refreshing views")
				if err := p.registerIcebergTables(); err != nil {
					log.Printf("Auto-registration failed: %v", err)
				}
			}
		}
	}()
}

// scanAndRegisterNewTables now just refreshes views via the catalog
func (p *DuckDBProxy) scanAndRegisterNewTables() {
	if err := p.registerIcebergTables(); err != nil {
		log.Printf("Error refreshing Iceberg views: %v", err)
	}
}

// registerSingleTable registers a single table as a view in DuckDB
func (p *DuckDBProxy) registerSingleTable(schema, table, metadataPath string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	// Create view names
	viewName := fmt.Sprintf("%s_%s", schema, table)
	simpleViewName := table

	// Drop existing views if they exist
	dropSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", viewName)
	if _, err := p.db.Exec(dropSQL); err != nil {
		log.Printf("Warning: failed to drop existing view %s: %v", viewName, err)
	}

	// Create view with schema prefix
	createSQL := fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT * FROM iceberg_scan('%s')
	`, viewName, metadataPath)

	if _, err := p.db.Exec(createSQL); err != nil {
		return fmt.Errorf("creating view %s: %w", viewName, err)
	}

	// Drop simple view if it exists
	dropSimpleSQL := fmt.Sprintf("DROP VIEW IF EXISTS %s", simpleViewName)
	if _, err := p.db.Exec(dropSimpleSQL); err != nil {
		log.Printf("Warning: failed to drop existing view %s: %v", simpleViewName, err)
	}

	// Create simple view
	createSimpleSQL := fmt.Sprintf(`
		CREATE VIEW %s AS
		SELECT * FROM iceberg_scan('%s')
	`, simpleViewName, metadataPath)

	if _, err := p.db.Exec(createSimpleSQL); err != nil {
		// This is not critical, just log it
		log.Printf("Warning: failed to create simple view %s: %v", simpleViewName, err)
	}

	return nil
}

// ReRegisterIcebergTables re-scans and registers Iceberg tables
// This is useful when tables are created after the proxy starts
func (p *DuckDBProxy) ReRegisterIcebergTables() error {
	return p.registerIcebergTables()
}

// ensureS3Secret ensures S3 secret exists in DuckDB
func (p *DuckDBProxy) ensureS3Secret() error {
	// Create S3 secret for MinIO (ignore if exists)
	// DuckDB expects endpoint without scheme
	ep := p.config.Iceberg.S3Endpoint
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
    `, p.config.Iceberg.S3AccessKeyID, p.config.Iceberg.S3SecretAccessKey, ep)

	if _, err := p.db.Exec(createSecretSQL); err != nil {
		return fmt.Errorf("creating S3 secret: %w", err)
	}

	return nil
}

func (p *DuckDBProxy) Start(ctx context.Context) error {
	log.Printf("Starting DuckDB proxy server on port %d", p.config.Proxy.Port)

	// Use a channel to handle graceful shutdown
	done := make(chan struct{})
	go func() {
		<-ctx.Done()
		log.Println("Proxy server shutting down...")
		close(done)
	}()

	for {
		select {
		case <-done:
			return ctx.Err()
		default:
			// Set a timeout for Accept to allow checking context
			p.listener.(*net.TCPListener).SetDeadline(time.Now().Add(100 * time.Millisecond))
			conn, err := p.listener.Accept()
			if err != nil {
				select {
				case <-done:
					return ctx.Err()
				default:
					// Check if it's a timeout error (expected)
					if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
						continue
					}
					// Check if listener is closed
					if strings.Contains(err.Error(), "use of closed network connection") {
						return ctx.Err()
					}
					log.Printf("Accept error: %v", err)
					continue
				}
			}

			go p.handleConnection(ctx, conn)
		}
	}
}

// GetDB returns the database connection for health checks
func (p *DuckDBProxy) GetDB() *sql.DB {
	return p.db
}

// Close closes the proxy and cleans up resources
func (p *DuckDBProxy) Close() error {
	var err error

	// Stop auto-registration
	if p.autoRegisterCancel != nil {
		p.autoRegisterCancel()
	}

	// Close listener
	if p.listener != nil {
		err = p.listener.Close()
	}

	// Close database connection
	if p.db != nil {
		if dbErr := p.db.Close(); dbErr != nil && err == nil {
			err = dbErr
		}
	}

	return err
}

func (p *DuckDBProxy) handleConnection(ctx context.Context, conn net.Conn) {
	defer conn.Close()

	backend := pgproto3.NewBackend(conn, conn)

	// Handle startup
	sm, err := backend.ReceiveStartupMessage()
	if err != nil {
		return
	}

	// Handle SSLRequest by replying 'N' (no SSL) for now
	if _, ok := sm.(*pgproto3.SSLRequest); ok {
		// Respond 'N' and expect another StartupMessage
		if _, err := conn.Write([]byte{'N'}); err != nil {
			return
		}
		sm, err = backend.ReceiveStartupMessage()
		if err != nil {
			return
		}
	}

	// Optional cleartext auth based on config
	if p.config.Proxy.AuthUser != "" {
		switch msg := sm.(type) {
		case *pgproto3.StartupMessage:
			// Expect user from params
			user := msg.Parameters["user"]
			if user != p.config.Proxy.AuthUser {
				p.sendError(backend, fmt.Errorf("invalid user"))
				return
			}
		}
		// Request cleartext password
		backend.Send(&pgproto3.AuthenticationCleartextPassword{})
		if err := backend.Flush(); err != nil {
			return
		}
		// Receive password message
		msg, err := backend.Receive()
		if err != nil {
			return
		}
		pwdMsg, ok := msg.(*pgproto3.PasswordMessage)
		if !ok || string(pwdMsg.Password) != p.config.Proxy.AuthPassword {
			p.sendError(backend, fmt.Errorf("authentication failed"))
			return
		}
		backend.Send(&pgproto3.AuthenticationOk{})
	} else {
		// No auth configured
		backend.Send(&pgproto3.AuthenticationOk{})
	}

	// Send minimal ParameterStatus and BackendKeyData
	backend.Send(&pgproto3.ParameterStatus{Name: "server_version", Value: "14.0"})
	backend.Send(&pgproto3.ParameterStatus{Name: "client_encoding", Value: "UTF8"})
	backend.Send(&pgproto3.ParameterStatus{Name: "standard_conforming_strings", Value: "on"})
	backend.Send(&pgproto3.ParameterStatus{Name: "DateStyle", Value: "ISO, MDY"})
	backend.Send(&pgproto3.ParameterStatus{Name: "TimeZone", Value: "UTC"})
	backend.Send(&pgproto3.BackendKeyData{ProcessID: uint32(rand.Int31()), SecretKey: uint32(rand.Int31())})

	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	if err := backend.Flush(); err != nil {
		return
	}

	// Main message loop
	for {
		msg, err := backend.Receive()
		if err != nil {
			return
		}

		switch msg := msg.(type) {
		case *pgproto3.Query:
			if err := p.handleQuery(ctx, backend, msg.String); err != nil {
				p.sendError(backend, err)
				continue
			}

		case *pgproto3.Parse:
			p.prepared[msg.Name] = &preparedStmt{
				name:      msg.Name,
				query:     msg.Query,
				paramOIDs: msg.ParameterOIDs,
			}
			backend.Send(&pgproto3.ParseComplete{})
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Bind:
			pst, ok := p.prepared[msg.PreparedStatement]
			if !ok {
				p.sendError(backend, fmt.Errorf("unknown prepared statement: %s", msg.PreparedStatement))
				continue
			}
			params := make([]interface{}, len(msg.Parameters))
			for i, b := range msg.Parameters {
				// Treat all params as text for now
				if b == nil {
					params[i] = nil
				} else {
					params[i] = string(b)
				}
			}
			rewritten := rewritePostgresParamsToQuestion(pst.query)
			prt := &portal{
				name:           msg.DestinationPortal,
				prepared:       pst,
				rewrittenQuery: rewritten,
				params:         params,
			}
			p.portals[prt.name] = prt
			backend.Send(&pgproto3.BindComplete{})
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Describe:
			switch msg.ObjectType {
			case 'S': // prepared statement
				// Send parameter description, but no row description here
				backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: p.getParamOIDs(string(msg.Name))})
				backend.Send(&pgproto3.NoData{})
			case 'P': // portal
				backend.Send(&pgproto3.NoData{})
			}
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Execute:
			prt, ok := p.portals[msg.Portal]
			if !ok {
				p.sendError(backend, fmt.Errorf("unknown portal: %s", msg.Portal))
				continue
			}
			if err := p.executePortal(ctx, backend, prt); err != nil {
				p.sendError(backend, err)
				continue
			}

		case *pgproto3.Sync:
			backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Close:
			switch msg.ObjectType {
			case 'S':
				delete(p.prepared, msg.Name)
			case 'P':
				delete(p.portals, msg.Name)
			}
			backend.Send(&pgproto3.CloseComplete{})
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Flush:
			if err := backend.Flush(); err != nil {
				return
			}

		case *pgproto3.Terminate:
			return
		}
	}
}

func (p *DuckDBProxy) handleQuery(ctx context.Context, backend *pgproto3.Backend, query string) error {
	start := time.Now()

	// Execute query using DuckDB
	rows, err := p.db.QueryContext(ctx, query)
	if err != nil {
		// Check if it's a "table does not exist" error
		if strings.Contains(err.Error(), "does not exist") || strings.Contains(err.Error(), "Catalog Error") {
			log.Printf("Query failed due to missing table: %v", err)
			log.Printf("Attempting to auto-register tables...")

			// Try to auto-register tables and retry the query
			if autoErr := p.registerIcebergTables(); autoErr != nil {
				log.Printf("Failed to auto-register tables: %v", autoErr)
				return err
			}

			// Retry the query
			rows, err = p.db.QueryContext(ctx, query)
			if err != nil {
				return err
			}
		} else {
			return err
		}
	}
	defer rows.Close()

	// Metrics and slow query logging
	duration := time.Since(start)
	metrics.ProxyQueriesTotal.Inc()
	metrics.ProxyQueryDurationSeconds.Observe(duration.Seconds())
	if p.config.Proxy.SlowQueryMillis > 0 && duration.Milliseconds() >= int64(p.config.Proxy.SlowQueryMillis) {
		log.Printf("slow query: duration=%dms sql=%q", duration.Milliseconds(), query)
	}

	// Get column descriptions
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}

	// Send row description
	if err := p.sendRowDescription(backend, columnTypes); err != nil {
		return err
	}

	// Send data rows
	values := make([]interface{}, len(columnTypes))
	scanArgs := make([]interface{}, len(columnTypes))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	// Send data rows
	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}

		// Create data row message
		dataRow := &pgproto3.DataRow{
			Values: make([][]byte, len(columnTypes)),
		}

		// Convert values to bytes
		for i, val := range values {
			if val == nil {
				dataRow.Values[i] = nil
				continue
			}

			// Convert value to string representation
			dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
		}

		backend.Send(dataRow)
	}

	// Check for errors after iterating over rows
	if err := rows.Err(); err != nil {
		return err
	}

	// Send command complete
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})

	// Send ready for query
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})

	// Flush all sent messages
	if err := backend.Flush(); err != nil {
		return err
	}

	return nil
}

func (p *DuckDBProxy) executePortal(ctx context.Context, backend *pgproto3.Backend, prt *portal) error {
	start := time.Now()
	rows, err := p.db.QueryContext(ctx, prt.rewrittenQuery, prt.params...)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Metrics and slow query logging
	duration := time.Since(start)
	metrics.ProxyQueriesTotal.Inc()
	metrics.ProxyQueryDurationSeconds.Observe(duration.Seconds())
	if p.config.Proxy.SlowQueryMillis > 0 && duration.Milliseconds() >= int64(p.config.Proxy.SlowQueryMillis) {
		log.Printf("slow query: duration=%dms sql=%q", duration.Milliseconds(), prt.rewrittenQuery)
	}

	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return err
	}
	if err := p.sendRowDescription(backend, columnTypes); err != nil {
		return err
	}

	values := make([]interface{}, len(columnTypes))
	scanArgs := make([]interface{}, len(columnTypes))
	for i := range values {
		scanArgs[i] = &values[i]
	}

	for rows.Next() {
		if err := rows.Scan(scanArgs...); err != nil {
			return err
		}
		dataRow := &pgproto3.DataRow{Values: make([][]byte, len(columnTypes))}
		for i, val := range values {
			if val == nil {
				dataRow.Values[i] = nil
				continue
			}
			dataRow.Values[i] = []byte(fmt.Sprintf("%v", val))
		}
		backend.Send(dataRow)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	backend.Send(&pgproto3.CommandComplete{CommandTag: []byte("SELECT")})
	if err := backend.Flush(); err != nil {
		return err
	}
	return nil
}

// rewritePostgresParamsToQuestion rewrites $1, $2 ... placeholders to '?'
func rewritePostgresParamsToQuestion(query string) string {
	var b strings.Builder
	b.Grow(len(query))
	inSingle := false
	inDouble := false
	for i := 0; i < len(query); i++ {
		ch := query[i]
		if ch == '\'' && !inDouble {
			inSingle = !inSingle
			b.WriteByte(ch)
			continue
		}
		if ch == '"' && !inSingle {
			inDouble = !inDouble
			b.WriteByte(ch)
			continue
		}
		if !inSingle && !inDouble && ch == '$' {
			j := i + 1
			if j < len(query) && query[j] >= '1' && query[j] <= '9' {
				for j < len(query) && query[j] >= '0' && query[j] <= '9' {
					j++
				}
				b.WriteByte('?')
				i = j - 1
				continue
			}
		}
		b.WriteByte(ch)
	}
	return b.String()
}

func (p *DuckDBProxy) sendRowDescription(backend *pgproto3.Backend, columns []*sql.ColumnType) error {
	fields := make([]pgproto3.FieldDescription, len(columns))
	for i, col := range columns {
		dataTypeOID := uint32(25) // Default to TEXT OID
		if databaseTypeName := col.DatabaseTypeName(); databaseTypeName != "" {
			// Map database type name to OID if necessary
			dataTypeOID = mapDataTypeToOID(databaseTypeName)
		}

		fields[i] = pgproto3.FieldDescription{
			Name:                 []byte(col.Name()),
			TableOID:             0,
			TableAttributeNumber: 0,
			DataTypeOID:          dataTypeOID,
			DataTypeSize:         -1,
			TypeModifier:         -1,
			Format:               0,
		}
	}

	backend.Send(&pgproto3.RowDescription{Fields: fields})
	return backend.Flush()
}

func (p *DuckDBProxy) sendError(backend *pgproto3.Backend, err error) {
	backend.Send(&pgproto3.ErrorResponse{
		Severity: "ERROR",
		Code:     "XX000",
		Message:  err.Error(),
	})
	backend.Send(&pgproto3.ReadyForQuery{TxStatus: 'I'})
	_ = backend.Flush()
}

func mapDataTypeToOID(databaseTypeName string) uint32 {
	// Convert to uppercase for case-insensitive comparison
	upperType := strings.ToUpper(databaseTypeName)

	switch upperType {
	case "BOOL", "BOOLEAN":
		return 16 // BOOL OID
	case "INT8", "BIGINT", "LONG":
		return 20 // BIGINT OID
	case "INT4", "INT", "INTEGER", "INT32":
		return 23 // INTEGER OID
	case "INT2", "SMALLINT", "INT16", "SHORT":
		return 21 // SMALLINT OID
	case "FLOAT4", "REAL", "FLOAT":
		return 700 // REAL OID
	case "FLOAT8", "DOUBLE", "DOUBLE PRECISION":
		return 701 // DOUBLE PRECISION OID
	case "DECIMAL", "NUMERIC":
		return 1700 // NUMERIC OID
	case "VARCHAR", "TEXT", "STRING", "CHAR":
		return 25 // TEXT OID
	case "DATE":
		return 1082 // DATE OID
	case "TIME":
		return 1083 // TIME OID
	case "TIMESTAMP", "DATETIME":
		return 1114 // TIMESTAMP OID
	case "TIMESTAMPTZ", "TIMESTAMP WITH TIME ZONE":
		return 1184 // TIMESTAMPTZ OID
	case "BYTEA", "BLOB", "BINARY":
		return 17 // BYTEA OID
	case "UUID":
		return 2950 // UUID OID
	case "JSON":
		return 114 // JSON OID
	default:
		// Log unmapped type for debugging
		if databaseTypeName != "" {
			log.Printf("Warning: unmapped DuckDB type '%s', defaulting to TEXT", databaseTypeName)
		}
		return 25 // Default to TEXT OID
	}
}

func (p *DuckDBProxy) getParamOIDs(name string) []uint32 {
	pst, ok := p.prepared[name]
	if !ok || pst == nil {
		return nil
	}
	return pst.paramOIDs
}

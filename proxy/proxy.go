package proxy

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"net"
    "time"
    "math/rand"
    "strings"

	"arctic-mirror/config"
    "arctic-mirror/metrics"

	"github.com/jackc/pgx/v5/pgproto3"
	_ "github.com/marcboeker/go-duckdb"
)

type DuckDBProxy struct {
	config   *config.Config
	db       *sql.DB
	listener net.Listener
	prepared map[string]*preparedStmt
	portals  map[string]*portal
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

	return &DuckDBProxy{
		config:   cfg,
		db:       db,
		listener: listener,
		prepared: make(map[string]*preparedStmt),
		portals:  make(map[string]*portal),
	}, nil
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

func (p *DuckDBProxy) Start(ctx context.Context) error {
	log.Printf("Starting DuckDB proxy server on port %d", p.config.Proxy.Port)
	
	for {
		select {
		case <-ctx.Done():
			log.Println("Proxy server shutting down...")
			return ctx.Err()
		default:
			conn, err := p.listener.Accept()
			if err != nil {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
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
	if p.listener != nil {
		return p.listener.Close()
	}
	return nil
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

		case *pgproto3.Describe:
			switch msg.ObjectType {
			case 'S': // prepared statement
				// Send parameter description, but no row description here
				backend.Send(&pgproto3.ParameterDescription{ParameterOIDs: p.getParamOIDs(string(msg.Name))})
				backend.Send(&pgproto3.NoData{})
			case 'P': // portal
				backend.Send(&pgproto3.NoData{})
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

		case *pgproto3.Close:
			switch msg.ObjectType {
			case 'S':
				delete(p.prepared, msg.Name)
			case 'P':
				delete(p.portals, msg.Name)
			}
			backend.Send(&pgproto3.CloseComplete{})

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
		return err
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
	switch databaseTypeName {
	case "BOOL":
		return 16 // BOOL OID
	case "INT8":
		return 20 // BIGINT OID
	case "INT4":
		return 23 // INTEGER OID
	case "FLOAT4":
		return 700 // REAL OID
	case "FLOAT8":
		return 701 // DOUBLE PRECISION OID
	case "VARCHAR", "TEXT":
		return 25 // TEXT OID
	case "DATE":
		return 1082 // DATE OID
	case "TIMESTAMP":
		return 1114 // TIMESTAMP OID
	default:
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

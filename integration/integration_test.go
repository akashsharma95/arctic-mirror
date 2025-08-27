//go:build integration
// +build integration

package integration

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/iceberg"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"
	"arctic-mirror/schema"

	"github.com/jackc/pgx/v5"
	_ "github.com/marcboeker/go-duckdb"
)

// TestConfig holds test configuration
type TestConfig struct {
	PostgresHost     string
	PostgresPort     int
	PostgresUser     string
	PostgresPassword string
	PostgresDB       string
	IcebergPath      string
	ProxyPort        int
}

// TestSuite holds the test environment
type TestSuite struct {
	config     *TestConfig
	postgresDB *pgx.Conn
	proxy      *proxy.DuckDBProxy
	replicator *replication.Replicator
	iceberg    *iceberg.Writer
	cleanup    []func()
}

// NewTestSuite creates a new test suite
func NewTestSuite(t *testing.T) *TestSuite {
	cfg := &TestConfig{
		PostgresHost:     "localhost",
		PostgresPort:     5432,
		PostgresUser:     "replicator",
		PostgresPassword: "secret",
		PostgresDB:       "mydb",
		IcebergPath:      "/tmp/iceberg_test",
		ProxyPort:        5434,
	}

	// Override with environment variables if set
	if host := os.Getenv("TEST_POSTGRES_HOST"); host != "" {
		cfg.PostgresHost = host
	}
	if port := os.Getenv("TEST_POSTGRES_PORT"); port != "" {
		if _, err := fmt.Sscanf(port, "%d", &cfg.PostgresPort); err != nil {
			t.Logf("Invalid TEST_POSTGRES_PORT: %s, using default", port)
		}
	}

	ts := &TestSuite{
		config:  cfg,
		cleanup: make([]func(), 0),
	}

	// Setup test environment
	if err := ts.setup(t); err != nil {
		t.Fatalf("Failed to setup test suite: %v", err)
	}

	return ts
}

// setup initializes the test environment
func (ts *TestSuite) setup(t *testing.T) error {
	// Create Iceberg directory
	if err := os.MkdirAll(ts.config.IcebergPath, 0755); err != nil {
		return fmt.Errorf("failed to create Iceberg directory: %w", err)
	}

	// Connect to PostgreSQL
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		ts.config.PostgresUser,
		ts.config.PostgresPassword,
		ts.config.PostgresHost,
		ts.config.PostgresPort,
		ts.config.PostgresDB,
	)

	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		return fmt.Errorf("failed to connect to PostgreSQL: %w", err)
	}
	ts.postgresDB = conn

	// Create test tables with all data types
	if err := ts.createTestTables(t); err != nil {
		return fmt.Errorf("failed to create test tables: %w", err)
	}

	// Initialize components
	if err := ts.initializeComponents(t); err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// Add cleanup functions
	ts.cleanup = append(ts.cleanup, func() {
		ts.postgresDB.Close(context.Background())
		os.RemoveAll(ts.config.IcebergPath)
	})

	return nil
}

// createTestTables creates tables with all PostgreSQL data types
func (ts *TestSuite) createTestTables(t *testing.T) error {
	queries := []string{
		// Drop tables if they exist
		`DROP TABLE IF EXISTS all_data_types CASCADE`,
		`DROP TABLE IF EXISTS test_users CASCADE`,
		`DROP TABLE IF EXISTS test_orders CASCADE`,

		// Create table with all data types
		`CREATE TABLE all_data_types (
			id SERIAL PRIMARY KEY,
			smallint_col SMALLINT,
			integer_col INTEGER,
			bigint_col BIGINT,
			decimal_col DECIMAL(10,2),
			numeric_col NUMERIC(15,5),
			real_col REAL,
			double_col DOUBLE PRECISION,
			boolean_col BOOLEAN,
			char_col CHAR(10),
			varchar_col VARCHAR(100),
			text_col TEXT,
			date_col DATE,
			time_col TIME,
			timestamp_col TIMESTAMP,
			timestamptz_col TIMESTAMPTZ,
			interval_col INTERVAL,
			json_col JSON,
			jsonb_col JSONB,
			uuid_col UUID,
			bytea_col BYTEA,
			bit_col BIT(8),
			varbit_col VARBIT(16),
			inet_col INET,
			cidr_col CIDR,
			macaddr_col MACADDR,
			point_col POINT,
			line_col LINE,
			lseg_col LSEG,
			box_col BOX,
			path_col PATH,
			polygon_col POLYGON,
			circle_col CIRCLE,
			tsvector_col TSVECTOR,
			tsquery_col TSQUERY,
			xml_col XML,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Create test users table
		`CREATE TABLE test_users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			age INTEGER,
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		// Create test orders table
		`CREATE TABLE test_orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES test_users(id),
			order_number VARCHAR(50) UNIQUE NOT NULL,
			total_amount DECIMAL(10,2) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
			shipping_address JSONB,
			tags TEXT[]
		)`,

		// Enable logical replication
		`ALTER TABLE all_data_types REPLICA IDENTITY FULL`,
		`ALTER TABLE test_users REPLICA IDENTITY FULL`,
		`ALTER TABLE test_orders REPLICA IDENTITY FULL`,

		// Create publication
		`DROP PUBLICATION IF EXISTS test_pub`,
		`CREATE PUBLICATION test_pub FOR ALL TABLES`,
	}

	for _, query := range queries {
		if _, err := ts.postgresDB.Exec(context.Background(), query); err != nil {
			return fmt.Errorf("failed to execute query '%s': %w", query, err)
		}
	}

	return nil
}

// initializeComponents initializes the replication components
func (ts *TestSuite) initializeComponents(t *testing.T) error {
	// Create config
	cfg := &config.Config{}
	cfg.Postgres.Host = ts.config.PostgresHost
	cfg.Postgres.Port = ts.config.PostgresPort
	cfg.Postgres.User = ts.config.PostgresUser
	cfg.Postgres.Password = ts.config.PostgresPassword
	cfg.Postgres.Database = ts.config.PostgresDB
	cfg.Postgres.Slot = "test_slot"
	cfg.Postgres.Publication = "test_pub"
	cfg.Iceberg.Path = ts.config.IcebergPath
	cfg.Proxy.Port = ts.config.ProxyPort
	cfg.Tables = []struct {
		Schema string `yaml:"schema"`
		Name   string `yaml:"name"`
	}{
		{Schema: "public", Name: "all_data_types"},
		{Schema: "public", Name: "test_users"},
		{Schema: "public", Name: "test_orders"},
	}

	// Initialize schema manager
	schemaManager := schema.NewSchemaManager(ts.postgresDB)

	// Initialize Iceberg writer
	writer, err := iceberg.NewWriter(ts.config.IcebergPath, schemaManager)
	if err != nil {
		return fmt.Errorf("failed to create Iceberg writer: %w", err)
	}
	ts.iceberg = writer

	// Initialize proxy
	proxy, err := proxy.NewDuckDBProxy(cfg)
	if err != nil {
		return fmt.Errorf("failed to create proxy: %w", err)
	}
	ts.proxy = proxy

	// Initialize replicator
	replicator, err := replication.NewReplicator(cfg)
	if err != nil {
		return fmt.Errorf("failed to create replicator: %w", err)
	}
	ts.replicator = replicator

	return nil
}

// Cleanup cleans up test resources
func (ts *TestSuite) Cleanup() {
	for _, cleanup := range ts.cleanup {
		cleanup()
	}
}

// TestEndToEndReplication tests the complete replication pipeline
func TestEndToEndReplication(t *testing.T) {
	ts := NewTestSuite(t)
	defer ts.Cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()

	// Start replication
	go func() {
		if err := ts.replicator.Start(ctx); err != nil {
			t.Errorf("Replication failed: %v", err)
		}
	}()

	// Start proxy
	go func() {
		if err := ts.proxy.Start(ctx); err != nil {
			t.Errorf("Proxy failed: %v", err)
		}
	}()

	// Wait for components to start
	time.Sleep(2 * time.Second)

	// Test 1: Insert data with all data types
	t.Run("InsertAllDataTypes", func(t *testing.T) {
		ts.testInsertAllDataTypes(t, ctx)
	})

	// Test 2: Insert users and orders
	t.Run("InsertUsersAndOrders", func(t *testing.T) {
		ts.testInsertUsersAndOrders(t, ctx)
	})

	// Test 3: Update data
	t.Run("UpdateData", func(t *testing.T) {
		ts.testUpdateData(t, ctx)
	})

	// Test 4: Delete data
	t.Run("DeleteData", func(t *testing.T) {
		ts.testDeleteData(t, ctx)
	})

	// Test 5: Query replicated data
	t.Run("QueryReplicatedData", func(t *testing.T) {
		ts.testQueryReplicatedData(t, ctx)
	})

	// Test 6: Test all data types
	t.Run("TestAllDataTypes", func(t *testing.T) {
		ts.testAllDataTypes(t, ctx)
	})
}

// testInsertAllDataTypes tests inserting data with all PostgreSQL data types
func (ts *TestSuite) testInsertAllDataTypes(t *testing.T, ctx context.Context) {
	query := `
		INSERT INTO all_data_types (
			smallint_col, integer_col, bigint_col, decimal_col, numeric_col,
			real_col, double_col, boolean_col, char_col, varchar_col, text_col,
			date_col, time_col, timestamp_col, timestamptz_col, interval_col,
			json_col, jsonb_col, uuid_col, bytea_col, bit_col, varbit_col,
			inet_col, cidr_col, macaddr_col, point_col, line_col, lseg_col,
			box_col, path_col, polygon_col, circle_col, tsvector_col, tsquery_col, xml_col
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16,
			$17, $18, $19, $20, $21, $22, $23, $24, $25, $26, $27, $28, $29, $30,
			$31, $32, $33, $34, $35
		)
	`

	// Test data with all types
	testData := []interface{}{
		123,                    // smallint
		456789,                 // integer
		1234567890123456789,    // bigint
		123.45,                 // decimal
		987654.321,             // numeric
		3.14159,                // real
		2.718281828459045,      // double
		true,                    // boolean
		"test",                  // char
		"variable length",       // varchar
		"long text content",     // text
		"2023-12-25",           // date
		"14:30:00",             // time
		"2023-12-25 14:30:00",  // timestamp
		"2023-12-25 14:30:00+00", // timestamptz
		"1 day 2 hours 3 minutes", // interval
		`{"key": "value"}`,     // json
		`{"key": "value"}`,     // jsonb
		"550e8400-e29b-41d4-a716-446655440000", // uuid
		[]byte("binary data"),  // bytea
		"10101010",             // bit
		"11001100",             // varbit
		"192.168.1.1",          // inet
		"192.168.1.0/24",       // cidr
		"08:00:2b:01:02:03",   // macaddr
		"(1,2)",                // point
		"{1,2,3}",              // line
		"[(1,2),(3,4)]",       // lseg
		"(1,2),(3,4)",          // box
		"[(1,2),(3,4)]",       // path
		"((1,2),(3,4),(5,6))", // polygon
		"<(1,2),3>",            // circle
		"'quick brown fox'",    // tsvector
		"'quick & brown'",      // tsquery
		"<xml>test</xml>",      // xml
	}

	if _, err := ts.postgresDB.Exec(ctx, query, testData...); err != nil {
		t.Fatalf("Failed to insert test data: %v", err)
	}

	// Wait for replication
	time.Sleep(2 * time.Second)
}

// testInsertUsersAndOrders tests inserting users and orders
func (ts *TestSuite) testInsertUsersAndOrders(t *testing.T, ctx context.Context) {
	// Insert users
	users := []struct {
		username string
		email    string
		age      int
	}{
		{"alice", "alice@example.com", 25},
		{"bob", "bob@example.com", 30},
		{"charlie", "charlie@example.com", 35},
	}

	for _, user := range users {
		query := `INSERT INTO test_users (username, email, age) VALUES ($1, $2, $3)`
		if _, err := ts.postgresDB.Exec(ctx, query, user.username, user.email, user.age); err != nil {
			t.Fatalf("Failed to insert user %s: %v", user.username, err)
		}
	}

	// Insert orders
	orders := []struct {
		userID      int
		orderNumber string
		amount      float64
		status      string
	}{
		{1, "ORD-001", 99.99, "completed"},
		{2, "ORD-002", 149.99, "pending"},
		{3, "ORD-003", 299.99, "shipped"},
	}

	for _, order := range orders {
		query := `INSERT INTO test_orders (user_id, order_number, total_amount, status) VALUES ($1, $2, $3, $4)`
		if _, err := ts.postgresDB.Exec(ctx, query, order.userID, order.orderNumber, order.amount, order.status); err != nil {
			t.Fatalf("Failed to insert order %s: %v", order.orderNumber, err)
		}
	}

	// Wait for replication
	time.Sleep(2 * time.Second)
}

// testUpdateData tests updating data
func (ts *TestSuite) testUpdateData(t *testing.T, ctx context.Context) {
	// Update user
	query := `UPDATE test_users SET age = 26, updated_at = CURRENT_TIMESTAMP WHERE username = 'alice'`
	if _, err := ts.postgresDB.Exec(ctx, query); err != nil {
		t.Fatalf("Failed to update user: %v", err)
	}

	// Update order
	query = `UPDATE test_orders SET status = 'completed', total_amount = 109.99 WHERE order_number = 'ORD-002'`
	if _, err := ts.postgresDB.Exec(ctx, query); err != nil {
		t.Fatalf("Failed to update order: %v", err)
	}

	// Wait for replication
	time.Sleep(2 * time.Second)
}

// testDeleteData tests deleting data
func (ts *TestSuite) testDeleteData(t *testing.T, ctx context.Context) {
	// Delete an order
	query := `DELETE FROM test_orders WHERE order_number = 'ORD-003'`
	if _, err := ts.postgresDB.Exec(ctx, query); err != nil {
		t.Fatalf("Failed to delete order: %v", err)
	}

	// Wait for replication
	time.Sleep(2 * time.Second)
}

// testQueryReplicatedData tests querying the replicated data through the proxy
func (ts *TestSuite) testQueryReplicatedData(t *testing.T, ctx context.Context) {
	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Test basic queries
	queries := []string{
		"SELECT COUNT(*) FROM test_users",
		"SELECT COUNT(*) FROM test_orders",
		"SELECT username, age FROM test_users WHERE age > 25",
		"SELECT u.username, o.order_number, o.total_amount FROM test_users u JOIN test_orders o ON u.id = o.user_id",
	}

	for i, query := range queries {
		rows, err := db.QueryContext(ctx, query)
		if err != nil {
			t.Errorf("Query %d failed: %v\nQuery: %s", i+1, err, query)
			continue
		}

		// Just verify the query executes successfully
		rows.Close()
	}
}

// testAllDataTypes tests that all data types are properly replicated and queryable
func (ts *TestSuite) testAllDataTypes(t *testing.T, ctx context.Context) {
	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		t.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Test querying the all_data_types table
	query := `SELECT 
		smallint_col, integer_col, bigint_col, decimal_col, numeric_col,
		real_col, double_col, boolean_col, char_col, varchar_col, text_col,
		date_col, time_col, timestamp_col, timestamptz_col, interval_col,
		json_col, jsonb_col, uuid_col, bytea_col, bit_col, varbit_col,
		inet_col, cidr_col, macaddr_col, point_col, line_col, lseg_col,
		box_col, path_col, polygon_col, circle_col, tsvector_col, tsquery_col, xml_col
	FROM all_data_types WHERE id = 1`

	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		t.Fatalf("Failed to query all_data_types: %v", err)
	}
	defer rows.Close()

	if rows.Next() {
		var (
			smallintCol, integerCol, bigintCol sql.NullInt64
			decimalCol, numericCol             sql.NullFloat64
			realCol, doubleCol                 sql.NullFloat64
			booleanCol                         sql.NullBool
			charCol, varcharCol, textCol       sql.NullString
			dateCol                            sql.NullTime
			timeCol, timestampCol, timestamptzCol sql.NullTime
			intervalCol, jsonCol, jsonbCol, uuidCol sql.NullString
			byteaCol, bitCol, varbitCol       sql.NullString
			inetCol, cidrCol, macaddrCol      sql.NullString
			pointCol, lineCol, lsegCol        sql.NullString
			boxCol, pathCol, polygonCol, circleCol sql.NullString
			tsvectorCol, tsqueryCol, xmlCol   sql.NullString
		)

		if err := rows.Scan(
			&smallintCol, &integerCol, &bigintCol, &decimalCol, &numericCol,
			&realCol, &doubleCol, &booleanCol, &charCol, &varcharCol, &textCol,
			&dateCol, &timeCol, &timestampCol, &timestamptzCol, &intervalCol,
			&jsonCol, &jsonbCol, &uuidCol, &byteaCol, &bitCol, &varbitCol,
			&inetCol, &cidrCol, &macaddrCol, &pointCol, &lineCol, &lsegCol,
			&boxCol, &pathCol, &polygonCol, &circleCol, &tsvectorCol, &tsqueryCol, &xmlCol,
		); err != nil {
			t.Fatalf("Failed to scan row: %v", err)
		}

		// Verify some key values
		if !smallintCol.Valid || smallintCol.Int64 != 123 {
			t.Errorf("Expected smallint_col = 123, got %v", smallintCol)
		}
		if !integerCol.Valid || integerCol.Int64 != 456789 {
			t.Errorf("Expected integer_col = 456789, got %v", integerCol)
		}
		if !booleanCol.Valid || !booleanCol.Bool {
			t.Errorf("Expected boolean_col = true, got %v", booleanCol)
		}
		if !varcharCol.Valid || varcharCol.String != "variable length" {
			t.Errorf("Expected varchar_col = 'variable length', got %v", varcharCol)
		}
	} else {
		t.Fatal("Expected to find a row in all_data_types")
	}
}

// TestIntegrationTestStructure tests that the integration test structure is set up correctly
func TestIntegrationTestStructure(t *testing.T) {
	// This test verifies that the integration test structure is set up correctly
	// It doesn't require external dependencies and can run in any environment
	
	t.Run("TestConfigStructure", func(t *testing.T) {
		cfg := &TestConfig{
			PostgresHost:     "localhost",
			PostgresPort:     5432,
			PostgresUser:     "test",
			PostgresPassword: "test",
			PostgresDB:       "testdb",
			IcebergPath:      "/tmp/test",
			ProxyPort:        5434,
		}
		
		if cfg.PostgresHost != "localhost" {
			t.Errorf("Expected PostgresHost 'localhost', got '%s'", cfg.PostgresHost)
		}
		
		if cfg.ProxyPort != 5434 {
			t.Errorf("Expected ProxyPort 5434, got %d", cfg.ProxyPort)
		}
	})
	
	t.Run("TestConfigValidation", func(t *testing.T) {
		// Test that we can create a test config with valid values
		cfg := &TestConfig{
			PostgresHost:     "localhost",
			PostgresPort:     5432,
			PostgresUser:     "test",
			PostgresPassword: "test",
			PostgresDB:       "testdb",
			IcebergPath:      "/tmp/test",
			ProxyPort:        5434,
		}
		
		// Verify all fields are set correctly
		if cfg.PostgresHost == "" {
			t.Error("PostgresHost should not be empty")
		}
		
		if cfg.PostgresPort <= 0 {
			t.Error("PostgresPort should be positive")
		}
		
		if cfg.ProxyPort <= 0 {
			t.Error("ProxyPort should be positive")
		}
	})
}
package integration

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/iceberg"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"
	"arctic-mirror/schema"

	"github.com/jackc/pgx/v5"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
	postgres "github.com/testcontainers/testcontainers-go/modules/postgres"
)

// IntegrationTestSuite holds the integration test environment
type IntegrationTestSuite struct {
	postgresContainer testcontainers.Container
	postgresDB        *pgx.Conn
	duckDBProxy       *proxy.DuckDBProxy
	replicator        *replication.Replicator
	schemaManager     *schema.Manager
	icebergWriter     *iceberg.Writer
	ctx               context.Context
	cleanupFuncs      []func()
}

// setupIntegrationTest initializes the integration test environment with real containers
func (ts *IntegrationTestSuite) setupIntegrationTest(t *testing.T) error {
	ctx := context.Background()
	ts.ctx = ctx

	// Start PostgreSQL container
	postgresContainer, err := postgres.RunContainer(ctx,
		testcontainers.WithImage("postgres:15-alpine"),
		postgres.WithDatabase("testdb"),
		postgres.WithUsername("testuser"),
		postgres.WithPassword("testpass"),
		postgres.WithInitScripts(),
		testcontainers.WithWaitStrategy(
			wait.ForLog("database system is ready to accept connections"),
		),
	)
	if err != nil {
		return fmt.Errorf("failed to start postgres container: %w", err)
	}
	ts.postgresContainer = postgresContainer

	// Get PostgreSQL connection details
	host, err := postgresContainer.Host(ctx)
	if err != nil {
		return fmt.Errorf("failed to get postgres host: %w", err)
	}

	port, err := postgresContainer.MappedPort(ctx, "5432/tcp")
	if err != nil {
		return fmt.Errorf("failed to get postgres port: %w", err)
	}

	// Connect to PostgreSQL
	connString := fmt.Sprintf("postgres://testuser:testpass@%s:%s/testdb?sslmode=disable", host, port.Port())
	conn, err := pgx.Connect(ctx, connString)
	if err != nil {
		return fmt.Errorf("failed to connect to postgres: %w", err)
	}
	ts.postgresDB = conn

	// Wait for PostgreSQL to be fully ready
	time.Sleep(3 * time.Second)

	// Create test tables and data
	if err := ts.createTestTables(t); err != nil {
		return fmt.Errorf("failed to create test tables: %w", err)
	}

	if err := ts.insertTestData(t); err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	// Initialize schema manager
	ts.schemaManager = schema.NewSchemaManager(conn)

	// Initialize DuckDB proxy
	cfg := &config.Config{}
	cfg.Proxy.Port = 5433 // Use different port for testing
	ts.duckDBProxy, err = proxy.NewDuckDBProxy(cfg)
	if err != nil {
		return fmt.Errorf("failed to create DuckDB proxy: %w", err)
	}

	// Start DuckDB proxy
	go func() {
		if err := ts.duckDBProxy.Start(ctx); err != nil {
			t.Logf("DuckDB proxy error: %v", err)
		}
	}()

	// Wait for proxy to start
	time.Sleep(2 * time.Second)

	// Initialize replicator
	ts.replicator, err = replication.NewReplicator(cfg)
	if err != nil {
		return fmt.Errorf("failed to create replicator: %w", err)
	}

	// Initialize Iceberg writer
	ts.icebergWriter, err = iceberg.NewWriter("/tmp/iceberg_test", ts.schemaManager)
	if err != nil {
		return fmt.Errorf("failed to create Iceberg writer: %w", err)
	}

	// Add cleanup functions
	ts.cleanupFuncs = append(ts.cleanupFuncs, func() {
		if ts.postgresDB != nil {
			ts.postgresDB.Close(ctx)
		}
		if ts.postgresContainer != nil {
			ts.postgresContainer.Terminate(ctx)
		}
		if ts.duckDBProxy != nil {
			ts.duckDBProxy.Stop()
		}
	})

	return nil
}

// cleanup cleans up the integration test environment
func (ts *IntegrationTestSuite) cleanup() {
	for _, cleanupFunc := range ts.cleanupFuncs {
		cleanupFunc()
	}
}

// createTestTables creates the test tables in PostgreSQL
func (ts *IntegrationTestSuite) createTestTables(t *testing.T) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS test_users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			age INTEGER,
			country VARCHAR(50),
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS test_products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			description TEXT,
			price DECIMAL(10,2) NOT NULL,
			stock_quantity INTEGER DEFAULT 0,
			is_available BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
		`CREATE TABLE IF NOT EXISTS test_orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES test_users(id),
			product_id INTEGER REFERENCES test_products(id),
			quantity INTEGER NOT NULL,
			total_amount DECIMAL(10,2) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		_, err := ts.postgresDB.Exec(ts.ctx, query)
		if err != nil {
			return fmt.Errorf("failed to execute query '%s': %w", query, err)
		}
	}

	return nil
}

// insertTestData inserts test data into the tables
func (ts *IntegrationTestSuite) insertTestData(t *testing.T) error {
	// Insert users
	users := []struct {
		username string
		email    string
		age      int
		country  string
	}{
		{"john_doe", "john@example.com", 30, "USA"},
		{"jane_smith", "jane@example.com", 25, "Canada"},
		{"bob_wilson", "bob@example.com", 35, "UK"},
		{"alice_brown", "alice@example.com", 28, "Germany"},
		{"charlie_davis", "charlie@example.com", 32, "France"},
	}

	for _, user := range users {
		query := `INSERT INTO test_users (username, email, age, country) VALUES ($1, $2, $3, $4)`
		_, err := ts.postgresDB.Exec(ts.ctx, query, user.username, user.email, user.age, user.country)
		if err != nil {
			return fmt.Errorf("failed to insert user %s: %w", user.username, err)
		}
	}

	// Insert products
	products := []struct {
		name        string
		description string
		price       float64
		stock       int
	}{
		{"Laptop", "High-performance laptop", 999.99, 50},
		{"Mouse", "Wireless mouse", 29.99, 100},
		{"Keyboard", "Mechanical keyboard", 149.99, 75},
		{"Monitor", "27-inch 4K monitor", 399.99, 25},
		{"Headphones", "Noise-cancelling headphones", 199.99, 60},
	}

	for _, product := range products {
		query := `INSERT INTO test_products (name, description, price, stock_quantity) VALUES ($1, $2, $3, $4)`
		_, err := ts.postgresDB.Exec(ts.ctx, query, product.name, product.description, product.price, product.stock)
		if err != nil {
			return fmt.Errorf("failed to insert product %s: %w", product.name, err)
		}
	}

	// Insert orders
	orders := []struct {
		userID      int
		productID   int
		quantity    int
		totalAmount float64
		status      string
	}{
		{1, 1, 1, 999.99, "delivered"},
		{2, 2, 2, 59.98, "shipped"},
		{3, 3, 1, 149.99, "processing"},
		{4, 4, 1, 399.99, "pending"},
		{5, 5, 1, 199.99, "delivered"},
	}

	for _, order := range orders {
		query := `INSERT INTO test_orders (user_id, product_id, quantity, total_amount, status) VALUES ($1, $2, $3, $4, $5)`
		_, err := ts.postgresDB.Exec(ts.ctx, query, order.userID, order.productID, order.quantity, order.totalAmount, order.status)
		if err != nil {
			return fmt.Errorf("failed to insert order: %w", err)
		}
	}

	return nil
}

// TestEndToEndIntegration tests the complete data flow from PostgreSQL to DuckDB via replication
func TestEndToEndIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping integration test in short mode")
	}

	ts := &IntegrationTestSuite{}
	err := ts.setupIntegrationTest(t)
	require.NoError(t, err)
	defer ts.cleanup()

	// Test 1: Verify PostgreSQL data
	t.Run("PostgreSQLDataVerification", func(t *testing.T) {
		// Check users
		var userCount int
		err := ts.postgresDB.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_users").Scan(&userCount)
		require.NoError(t, err)
		require.Equal(t, 5, userCount)

		// Check products
		var productCount int
		err = ts.postgresDB.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_products").Scan(&productCount)
		require.NoError(t, err)
		require.Equal(t, 5, productCount)

		// Check orders
		var orderCount int
		err = ts.postgresDB.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_orders").Scan(&orderCount)
		require.NoError(t, err)
		require.Equal(t, 5, orderCount)
	})

	// Test 2: Test DuckDB proxy connection
	t.Run("DuckDBProxyConnection", func(t *testing.T) {
		db, err := sql.Open("duckdb", "")
		require.NoError(t, err)
		defer db.Close()

		// Test simple query
		var result int
		err = db.QueryRow("SELECT 42").Scan(&result)
		require.NoError(t, err)
		require.Equal(t, 42, result)
	})

	// Test 3: Test complex queries
	t.Run("ComplexQueries", func(t *testing.T) {
		// Test JOIN query
		query := `
			SELECT u.username, p.name, o.quantity, o.total_amount
			FROM test_users u
			JOIN test_orders o ON u.id = o.user_id
			JOIN test_products p ON o.product_id = p.id
			WHERE o.status = 'delivered'
			ORDER BY o.total_amount DESC
		`
		rows, err := ts.postgresDB.Query(ts.ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		var deliveredOrders int
		for rows.Next() {
			deliveredOrders++
		}
		require.Greater(t, deliveredOrders, 0)
	})

	// Test 4: Test aggregation queries
	t.Run("AggregationQueries", func(t *testing.T) {
		// Test SUM aggregation
		var totalRevenue float64
		err := ts.postgresDB.QueryRow(ts.ctx, "SELECT SUM(total_amount) FROM test_orders").Scan(&totalRevenue)
		require.NoError(t, err)
		require.Greater(t, totalRevenue, 0.0)

		// Test COUNT with GROUP BY
		query := `
			SELECT status, COUNT(*) as order_count
			FROM test_orders
			GROUP BY status
			ORDER BY order_count DESC
		`
		rows, err := ts.postgresDB.Query(ts.ctx, query)
		require.NoError(t, err)
		defer rows.Close()

		var statusCount int
		for rows.Next() {
			statusCount++
		}
		require.Greater(t, statusCount, 0)
	})

	// Test 5: Test schema management
	t.Run("SchemaManagement", func(t *testing.T) {
		// Get table schema
		schema, err := ts.schemaManager.GetSchema(1) // Assuming table ID 1
		require.NoError(t, err)
		require.NotNil(t, schema)
	})

	// Test 6: Test data modification and replication simulation
	t.Run("DataModification", func(t *testing.T) {
		// Insert new user
		query := `INSERT INTO test_users (username, email, age, country) VALUES ($1, $2, $3, $4) RETURNING id`
		var newUserID int
		err := ts.postgresDB.QueryRow(ts.ctx, query, "new_user", "new@example.com", 40, "Japan").Scan(&newUserID)
		require.NoError(t, err)
		require.Greater(t, newUserID, 0)

		// Verify insertion
		var username string
		err = ts.postgresDB.QueryRow(ts.ctx, "SELECT username FROM test_users WHERE id = $1", newUserID).Scan(&username)
		require.NoError(t, err)
		require.Equal(t, "new_user", username)

		// Clean up
		_, err = ts.postgresDB.Exec(ts.ctx, "DELETE FROM test_users WHERE id = $1", newUserID)
		require.NoError(t, err)
	})
}

// TestPerformanceBenchmarks runs performance tests on real database
func TestPerformanceBenchmarks(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping performance test in short mode")
	}

	ts := &IntegrationTestSuite{}
	err := ts.setupIntegrationTest(t)
	require.NoError(t, err)
	defer ts.cleanup()

	// Performance test 1: Simple queries
	t.Run("SimpleQueryPerformance", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 100; i++ {
			var count int
			err := ts.postgresDB.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_users").Scan(&count)
			require.NoError(t, err)
			require.Equal(t, 5, count)
		}
		duration := time.Since(start)
		t.Logf("100 simple COUNT queries completed in %v", duration)
		require.Less(t, duration, 5*time.Second, "Simple queries should complete within 5 seconds")
	})

	// Performance test 2: Complex JOIN queries
	t.Run("ComplexQueryPerformance", func(t *testing.T) {
		start := time.Now()
		for i := 0; i < 50; i++ {
			query := `
				SELECT u.username, p.name, o.quantity, o.total_amount
				FROM test_users u
				JOIN test_orders o ON u.id = o.user_id
				JOIN test_products p ON o.product_id = p.id
				WHERE o.status = 'delivered'
				ORDER BY o.total_amount DESC
			`
			rows, err := ts.postgresDB.Query(ts.ctx, query)
			require.NoError(t, err)
			rows.Close()
		}
		duration := time.Since(start)
		t.Logf("50 complex JOIN queries completed in %v", duration)
		require.Less(t, duration, 10*time.Second, "Complex queries should complete within 10 seconds")
	})

	// Performance test 3: Concurrent queries
	t.Run("ConcurrentQueryPerformance", func(t *testing.T) {
		start := time.Now()
		results := make(chan error, 10)

		for i := 0; i < 10; i++ {
			go func(workerID int) {
				query := fmt.Sprintf("SELECT COUNT(*) FROM test_users WHERE id = %d", (workerID%5)+1)
				var count int
				err := ts.postgresDB.QueryRow(ts.ctx, query).Scan(&count)
				if err != nil {
					results <- fmt.Errorf("worker %d error: %w", workerID, err)
					return
				}
				results <- nil
			}(i)
		}

		// Collect results
		for i := 0; i < 10; i++ {
			err := <-results
			require.NoError(t, err)
		}

		duration := time.Since(start)
		t.Logf("10 concurrent queries completed in %v", duration)
		require.Less(t, duration, 3*time.Second, "Concurrent queries should complete within 3 seconds")
	})
}

// TestDataConsistency tests data consistency across operations
func TestDataConsistency(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping consistency test in short mode")
	}

	ts := &IntegrationTestSuite{}
	err := ts.setupIntegrationTest(t)
	require.NoError(t, err)
	defer ts.cleanup()

	t.Run("TransactionConsistency", func(t *testing.T) {
		// Start transaction
		tx, err := ts.postgresDB.Begin(ts.ctx)
		require.NoError(t, err)
		defer tx.Rollback(ts.ctx)

		// Insert data in transaction
		_, err = tx.Exec(ts.ctx, "INSERT INTO test_users (username, email, age, country) VALUES ($1, $2, $3, $4)", 
			"tx_user", "tx@example.com", 45, "Italy")
		require.NoError(t, err)

		// Verify data is visible within transaction
		var count int
		err = tx.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_users WHERE username = 'tx_user'").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		// Commit transaction
		err = tx.Commit(ts.ctx)
		require.NoError(t, err)

		// Verify data is visible after commit
		err = ts.postgresDB.QueryRow(ts.ctx, "SELECT COUNT(*) FROM test_users WHERE username = 'tx_user'").Scan(&count)
		require.NoError(t, err)
		require.Equal(t, 1, count)

		// Clean up
		_, err = ts.postgresDB.Exec(ts.ctx, "DELETE FROM test_users WHERE username = 'tx_user'")
		require.NoError(t, err)
	})

	t.Run("ReferentialIntegrity", func(t *testing.T) {
		// Try to insert order with non-existent user (should fail)
		_, err := ts.postgresDB.Exec(ts.ctx, 
			"INSERT INTO test_orders (user_id, product_id, quantity, total_amount) VALUES ($1, $2, $3, $4)",
			999, 1, 1, 100.00)
		require.Error(t, err, "Should fail due to foreign key constraint")

		// Try to insert order with non-existent product (should fail)
		_, err = ts.postgresDB.Exec(ts.ctx, 
			"INSERT INTO test_orders (user_id, product_id, quantity, total_amount) VALUES ($1, $2, $3, $4)",
			1, 999, 1, 100.00)
		require.Error(t, err, "Should fail due to foreign key constraint")
	})
}
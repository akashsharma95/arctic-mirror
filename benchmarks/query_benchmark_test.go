package benchmarks

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"arctic-mirror/config"
	"arctic-mirror/proxy"
	"arctic-mirror/replication"

	"github.com/jackc/pgx/v5"
	_ "github.com/marcboeker/go-duckdb"
)

// TestSuite holds the test environment
type QueryBenchmarkSuite struct {
	config        *config.Config
	postgresDB    *pgx.Conn
	proxy         *proxy.DuckDBProxy
	replicator    *replication.Replicator
	ctx           context.Context
	cleanupFuncs  []func()
}

// setup initializes the benchmark environment
func (qbs *QueryBenchmarkSuite) setup(b *testing.B) error {
	// Load test configuration
	cfg, err := config.LoadConfig("../test_config.yaml")
	if err != nil {
		return fmt.Errorf("failed to load test config: %w", err)
	}
	qbs.config = cfg

	// Create Iceberg directory
	if err := os.MkdirAll(cfg.Iceberg.Path, 0755); err != nil {
		return fmt.Errorf("failed to create Iceberg directory: %w", err)
	}

	// Try to connect to PostgreSQL, but don't fail if unavailable
	connString := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		cfg.Postgres.User,
		cfg.Postgres.Password,
		cfg.Postgres.Host,
		cfg.Postgres.Port,
		cfg.Postgres.Database,
	)

	conn, err := pgx.Connect(context.Background(), connString)
	if err != nil {
		// If database connection fails, skip the benchmark
		b.Skipf("Database not available, skipping benchmark: %v", err)
		return nil
	}
	qbs.postgresDB = conn

	// Initialize components
	if err := qbs.initializeComponents(b); err != nil {
		return fmt.Errorf("failed to initialize components: %w", err)
	}

	// Create test tables and data
	if err := qbs.createTestData(b); err != nil {
		return fmt.Errorf("failed to create test data: %w", err)
	}

	// Add cleanup functions
	qbs.cleanupFuncs = append(qbs.cleanupFuncs, func() {
		qbs.postgresDB.Close(context.Background())
		os.RemoveAll(cfg.Iceberg.Path)
	})

	return nil
}

// initializeComponents initializes the replication and proxy components
func (qbs *QueryBenchmarkSuite) initializeComponents(b *testing.B) error {
	// Create replicator
	replicator, err := replication.NewReplicator(qbs.config)
	if err != nil {
		return fmt.Errorf("failed to create replicator: %w", err)
	}
	qbs.replicator = replicator

	// Create proxy
	proxy, err := proxy.NewDuckDBProxy(qbs.config)
	if err != nil {
		return fmt.Errorf("failed to create proxy: %w", err)
	}
	qbs.proxy = proxy

	// Start replication in background
	ctx, cancel := context.WithCancel(context.Background())
	qbs.ctx = ctx
	go func() {
		if err := replicator.Start(ctx); err != nil {
			b.Logf("Replication error: %v", err)
		}
	}()
	defer cancel() // Ensure context is cancelled when function returns

	// Start proxy in background
	go func() {
		if err := proxy.Start(ctx); err != nil {
			b.Logf("Proxy error: %v", err)
		}
	}()

	// Wait for components to start
	time.Sleep(2 * time.Second)

	return nil
}

// createTestData creates test tables and populates them with data
func (qbs *QueryBenchmarkSuite) createTestData(b *testing.B) error {
	// Create benchmark tables
	queries := []string{
		`DROP TABLE IF EXISTS benchmark_users CASCADE`,
		`DROP TABLE IF EXISTS benchmark_orders CASCADE`,
		`DROP TABLE IF EXISTS benchmark_products CASCADE`,
		`DROP TABLE IF EXISTS benchmark_categories CASCADE`,

		`CREATE TABLE benchmark_users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			age INTEGER,
			country VARCHAR(50),
			is_active BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE benchmark_categories (
			id SERIAL PRIMARY KEY,
			name VARCHAR(100) NOT NULL,
			description TEXT,
			parent_id INTEGER REFERENCES benchmark_categories(id)
		)`,

		`CREATE TABLE benchmark_products (
			id SERIAL PRIMARY KEY,
			name VARCHAR(200) NOT NULL,
			description TEXT,
			category_id INTEGER REFERENCES benchmark_categories(id),
			price DECIMAL(10,2) NOT NULL,
			stock_quantity INTEGER DEFAULT 0,
			is_available BOOLEAN DEFAULT true,
			created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,

		`CREATE TABLE benchmark_orders (
			id SERIAL PRIMARY KEY,
			user_id INTEGER REFERENCES benchmark_users(id),
			product_id INTEGER REFERENCES benchmark_products(id),
			quantity INTEGER NOT NULL,
			total_amount DECIMAL(10,2) NOT NULL,
			status VARCHAR(20) DEFAULT 'pending',
			order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
		)`,
	}

	for _, query := range queries {
		if _, err := qbs.postgresDB.Exec(qbs.ctx, query); err != nil {
			return fmt.Errorf("failed to execute query '%s': %w", query, err)
		}
	}

	// Insert test data
	if err := qbs.insertTestData(b); err != nil {
		return fmt.Errorf("failed to insert test data: %w", err)
	}

	// Wait for replication to process the data
	time.Sleep(3 * time.Second)

	return nil
}

// insertTestData populates the benchmark tables with realistic data
func (qbs *QueryBenchmarkSuite) insertTestData(b *testing.B) error {
	// Insert categories
	categories := []string{"Electronics", "Clothing", "Books", "Home & Garden", "Sports", "Toys", "Automotive", "Health"}
	for _, catName := range categories {
		query := `INSERT INTO benchmark_categories (name, description) VALUES ($1, $2)`
		if _, err := qbs.postgresDB.Exec(qbs.ctx, query, catName, "Category for "+catName); err != nil {
			return fmt.Errorf("failed to insert category %s: %w", catName, err)
		}
	}

	// Insert users (1000 users)
	for i := 1; i <= 1000; i++ {
		query := `INSERT INTO benchmark_users (username, email, age, country, is_active) VALUES ($1, $2, $3, $4, $5)`
		username := fmt.Sprintf("user%d", i)
		email := fmt.Sprintf("user%d@example.com", i)
		age := 18 + (i % 62) // Age 18-79
		country := []string{"USA", "Canada", "UK", "Germany", "France", "Japan", "Australia"}[i%7]
		isActive := i%10 != 0 // 90% active users

		if _, err := qbs.postgresDB.Exec(qbs.ctx, query, username, email, age, country, isActive); err != nil {
			return fmt.Errorf("failed to insert user %d: %w", i, err)
		}
	}

	// Insert products (5000 products)
	for i := 1; i <= 5000; i++ {
		query := `INSERT INTO benchmark_products (name, description, category_id, price, stock_quantity, is_available) VALUES ($1, $2, $3, $4, $5, $6)`
		name := fmt.Sprintf("Product %d", i)
		description := fmt.Sprintf("Description for product %d", i)
		categoryID := (i % 8) + 1
		price := float64(10+(i%990)) + float64(i%100)/100 // Price $10.00 - $999.99
		stockQuantity := i % 1000
		isAvailable := i%20 != 0 // 95% available

		if _, err := qbs.postgresDB.Exec(qbs.ctx, query, name, description, categoryID, price, stockQuantity, isAvailable); err != nil {
			return fmt.Errorf("failed to insert product %d: %w", i, err)
		}
	}

	// Insert orders (10000 orders)
	for i := 1; i <= 10000; i++ {
		query := `INSERT INTO benchmark_orders (user_id, product_id, quantity, total_amount, status, order_date) VALUES ($1, $2, $3, $4, $5, $6)`
		userID := (i % 1000) + 1
		productID := (i % 5000) + 1
		quantity := (i % 5) + 1
		totalAmount := float64(quantity) * (float64(10+(i%990)) + float64(i%100)/100)
		status := []string{"pending", "processing", "shipped", "delivered", "cancelled"}[i%5]
		orderDate := time.Now().Add(-time.Duration(i%365) * 24 * time.Hour)

		if _, err := qbs.postgresDB.Exec(qbs.ctx, query, userID, productID, quantity, totalAmount, status, orderDate); err != nil {
			return fmt.Errorf("failed to insert order %d: %w", i, err)
		}
	}

	return nil
}

// cleanup performs cleanup operations
func (qbs *QueryBenchmarkSuite) cleanup() {
	for _, cleanupFn := range qbs.cleanupFuncs {
		cleanupFn()
	}
}

// BenchmarkSimpleSelect benchmarks simple SELECT queries
func BenchmarkSimpleSelect(b *testing.B) {
	qbs := &QueryBenchmarkSuite{}
	if err := qbs.setup(b); err != nil {
		b.Fatalf("Failed to setup benchmark: %v", err)
	}
	defer qbs.cleanup()

	// Skip if database is not available
	if qbs.postgresDB == nil {
		b.Skip("Database not available, skipping benchmark")
		return
	}

	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Test queries
	queries := []string{
		"SELECT COUNT(*) FROM benchmark_users",
		"SELECT COUNT(*) FROM benchmark_products",
		"SELECT COUNT(*) FROM benchmark_orders",
		"SELECT * FROM benchmark_users LIMIT 10",
		"SELECT * FROM benchmark_products LIMIT 10",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		rows, err := db.QueryContext(qbs.ctx, query)
		if err != nil {
			b.Errorf("Query failed: %v\nQuery: %s", err, query)
			continue
		}
		rows.Close()
	}
}

// BenchmarkComplexQueries benchmarks complex queries with JOINs and WHERE clauses
func BenchmarkComplexQueries(b *testing.B) {
	qbs := &QueryBenchmarkSuite{}
	if err := qbs.setup(b); err != nil {
		b.Fatalf("Failed to setup benchmark: %v", err)
	}
	defer qbs.cleanup()

	// Skip if database is not available
	if qbs.postgresDB == nil {
		b.Skip("Database not available, skipping benchmark")
		return
	}

	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Complex test queries
	queries := []string{
		`SELECT u.username, u.age, COUNT(o.id) as order_count, SUM(o.total_amount) as total_spent
		 FROM benchmark_users u 
		 JOIN benchmark_orders o ON u.id = o.user_id 
		 WHERE u.is_active = true 
		 GROUP BY u.id, u.username, u.age 
		 HAVING COUNT(o.id) > 0 
		 ORDER BY total_spent DESC 
		 LIMIT 100`,

		`SELECT c.name as category_name, 
		        COUNT(p.id) as product_count, 
		        AVG(p.price) as avg_price,
		        SUM(p.stock_quantity) as total_stock
		 FROM benchmark_categories c 
		 LEFT JOIN benchmark_products p ON c.id = p.category_id 
		 WHERE p.is_available = true 
		 GROUP BY c.id, c.name 
		 ORDER BY product_count DESC`,

		`SELECT u.country, 
		        COUNT(DISTINCT u.id) as user_count,
		        COUNT(o.id) as order_count,
		        AVG(o.total_amount) as avg_order_value
		 FROM benchmark_users u 
		 LEFT JOIN benchmark_orders o ON u.id = o.user_id 
		 WHERE u.age BETWEEN 25 AND 65 
		 GROUP BY u.country 
		 HAVING COUNT(o.id) > 0 
		 ORDER BY avg_order_value DESC`,

		`SELECT p.name as product_name,
		        c.name as category_name,
		        COUNT(o.id) as times_ordered,
		        SUM(o.quantity) as total_quantity,
		        SUM(o.total_amount) as total_revenue
		 FROM benchmark_products p 
		 JOIN benchmark_categories c ON p.category_id = c.id 
		 JOIN benchmark_orders o ON p.id = o.product_id 
		 WHERE o.status IN ('shipped', 'delivered') 
		   AND o.order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 30 DAY)
		 GROUP BY p.id, p.name, c.name 
		 HAVING COUNT(o.id) >= 5 
		 ORDER BY total_revenue DESC 
		 LIMIT 50`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		rows, err := db.QueryContext(qbs.ctx, query)
		if err != nil {
			b.Errorf("Query failed: %v\nQuery: %s", err, query)
			continue
		}
		rows.Close()
	}
}

// BenchmarkAggregationQueries benchmarks aggregation and grouping queries
func BenchmarkAggregationQueries(b *testing.B) {
	qbs := &QueryBenchmarkSuite{}
	if err := qbs.setup(b); err != nil {
		b.Fatalf("Failed to setup benchmark: %v", err)
	}
	defer qbs.cleanup()

	// Skip if database is not available
	if qbs.postgresDB == nil {
		b.Skip("Database not available, skipping benchmark")
		return
	}

	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Aggregation test queries
	queries := []string{
		`SELECT 
			DATE_TRUNC('month', order_date) as month,
			COUNT(*) as order_count,
			SUM(total_amount) as total_revenue,
			AVG(total_amount) as avg_order_value,
			MIN(total_amount) as min_order,
			MAX(total_amount) as max_order
		 FROM benchmark_orders 
		 WHERE order_date >= DATE_SUB(CURRENT_DATE, INTERVAL 12 MONTH)
		 GROUP BY DATE_TRUNC('month', order_date)
		 ORDER BY month`,

		`SELECT 
			u.age_group,
			COUNT(DISTINCT u.id) as user_count,
			COUNT(o.id) as order_count,
			SUM(o.total_amount) as total_spent,
			AVG(o.total_amount) as avg_spent_per_user
		 FROM (
			SELECT id, 
			       CASE 
			           WHEN age < 25 THEN '18-24'
			           WHEN age < 35 THEN '25-34'
			           WHEN age < 45 THEN '35-44'
			           WHEN age < 55 THEN '45-54'
			           ELSE '55+'
			       END as age_group
			FROM benchmark_users
		 ) u 
		 LEFT JOIN benchmark_orders o ON u.id = o.user_id 
		 GROUP BY u.age_group 
		 ORDER BY u.age_group`,

		`SELECT 
			c.name as category_name,
			COUNT(p.id) as product_count,
			SUM(p.stock_quantity) as total_stock,
			AVG(p.price) as avg_price,
			MIN(p.price) as min_price,
			MAX(p.price) as max_price,
			SUM(p.stock_quantity * p.price) as inventory_value
		 FROM benchmark_categories c 
		 LEFT JOIN benchmark_products p ON c.id = p.category_id 
		 WHERE p.is_available = true 
		 GROUP BY c.id, c.name 
		 HAVING COUNT(p.id) > 0 
		 ORDER BY inventory_value DESC`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		rows, err := db.QueryContext(qbs.ctx, query)
		if err != nil {
			b.Errorf("Query failed: %v\nQuery: %s", err, query)
			continue
		}
		rows.Close()
	}
}

// BenchmarkConcurrentQueries benchmarks multiple concurrent queries
func BenchmarkConcurrentQueries(b *testing.B) {
	qbs := &QueryBenchmarkSuite{}
	if err := qbs.setup(b); err != nil {
		b.Fatalf("Failed to setup benchmark: %v", err)
	}
	defer qbs.cleanup()

	// Skip if database is not available
	if qbs.postgresDB == nil {
		b.Skip("Database not available, skipping benchmark")
		return
	}

	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Simple queries for concurrent execution
	queries := []string{
		"SELECT COUNT(*) FROM benchmark_users WHERE is_active = true",
		"SELECT COUNT(*) FROM benchmark_products WHERE is_available = true",
		"SELECT COUNT(*) FROM benchmark_orders WHERE status = 'delivered'",
		"SELECT AVG(price) FROM benchmark_products",
		"SELECT AVG(age) FROM benchmark_users",
		"SELECT COUNT(*) FROM benchmark_orders WHERE total_amount > 100",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Execute 4 queries concurrently
		results := make(chan error, 4)
		
		for j := 0; j < 4; j++ {
			go func(workerID int) {
				query := queries[(i+workerID)%len(queries)]
				rows, err := db.QueryContext(qbs.ctx, query)
				if err != nil {
					results <- fmt.Errorf("worker %d query failed: %v", workerID, err)
					return
				}
				rows.Close()
				results <- nil
			}(j)
		}

		// Collect results
		for j := 0; j < 4; j++ {
			if err := <-results; err != nil {
				b.Errorf("Concurrent query error: %v", err)
			}
		}
	}
}

// BenchmarkDataScanning benchmarks scanning large result sets
func BenchmarkDataScanning(b *testing.B) {
	qbs := &QueryBenchmarkSuite{}
	if err := qbs.setup(b); err != nil {
		b.Fatalf("Failed to setup benchmark: %v", err)
	}
	defer qbs.cleanup()

	// Skip if database is not available
	if qbs.postgresDB == nil {
		b.Skip("Database not available, skipping benchmark")
		return
	}

	// Connect to the proxy
	db, err := sql.Open("duckdb", "")
	if err != nil {
		b.Fatalf("Failed to open DuckDB connection: %v", err)
	}
	defer db.Close()

	// Queries that return larger result sets
	queries := []string{
		"SELECT * FROM benchmark_users ORDER BY id LIMIT 1000",
		"SELECT * FROM benchmark_products ORDER BY id LIMIT 1000",
		"SELECT * FROM benchmark_orders ORDER BY id LIMIT 1000",
		`SELECT u.username, u.email, u.age, u.country, 
		        p.name as product_name, p.price,
		        o.quantity, o.total_amount, o.status
		 FROM benchmark_users u 
		 JOIN benchmark_orders o ON u.id = o.user_id 
		 JOIN benchmark_products p ON o.product_id = p.id 
		 ORDER BY u.id, o.id 
		 LIMIT 1000`,
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		query := queries[i%len(queries)]
		rows, err := db.QueryContext(qbs.ctx, query)
		if err != nil {
			b.Errorf("Query failed: %v\nQuery: %s", err, query)
			continue
		}

		// Scan all rows to measure actual data processing
		rowCount := 0
		for rows.Next() {
			rowCount++
			// Simulate processing each row
			_ = rowCount
		}
		rows.Close()
	}
}

// BenchmarkMockQueryPerformance benchmarks mock query performance without database
func BenchmarkMockQueryPerformance(b *testing.B) {
	// Generate mock data
	users := make([]map[string]interface{}, 1000)
	orders := make([]map[string]interface{}, 10000)
	
	for i := 0; i < 1000; i++ {
		users[i] = map[string]interface{}{
			"id":        i + 1,
			"username":  fmt.Sprintf("user%d", i),
			"age":       18 + (i % 62),
			"country":   []string{"USA", "Canada", "UK", "Germany", "France", "Japan", "Australia"}[i%7],
			"is_active": i%10 != 0,
		}
	}
	
	for i := 0; i < 10000; i++ {
		orders[i] = map[string]interface{}{
			"id":           i + 1,
			"user_id":      (i % 1000) + 1,
			"total_amount": float64(10+(i%990)) + float64(i%100)/100,
			"status":       []string{"pending", "processing", "shipped", "delivered", "cancelled"}[i%5],
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Simulate different types of queries
		switch i % 4 {
		case 0:
			// Simulate COUNT query
			count := 0
			for _, user := range users {
				if user["is_active"].(bool) {
					count++
				}
			}
			_ = count
			
		case 1:
			// Simulate JOIN query
			totalRevenue := 0.0
			for _, order := range orders {
				if order["status"] == "delivered" {
					totalRevenue += order["total_amount"].(float64)
				}
			}
			_ = totalRevenue
			
		case 2:
			// Simulate GROUP BY query
			countryStats := make(map[string]int)
			for _, user := range users {
				country := user["country"].(string)
				countryStats[country]++
			}
			_ = countryStats
			
		case 3:
			// Simulate complex filtering
			highValueOrders := 0
			for _, order := range orders {
				if order["total_amount"].(float64) > 500.0 {
					highValueOrders++
				}
			}
			_ = highValueOrders
		}
	}
}
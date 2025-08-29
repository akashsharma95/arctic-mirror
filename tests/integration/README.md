# Integration Tests with Testcontainers

This directory contains comprehensive integration tests that use **testcontainers-go** to spin up real PostgreSQL and DuckDB containers for testing.

## 🚀 What These Tests Do

The integration tests provide **real database testing** by:

1. **Starting PostgreSQL containers** using testcontainers-go
2. **Creating real database tables** with realistic schemas
3. **Inserting test data** that mimics production scenarios
4. **Testing actual SQL queries** against real PostgreSQL
5. **Testing DuckDB proxy connections** 
6. **Testing schema management** and data consistency
7. **Performance benchmarking** on real databases

## 🐳 Prerequisites

### Docker Installation
```bash
# Ubuntu/Debian
sudo apt-get update
sudo apt-get install -y docker.io

# Start Docker service
sudo systemctl start docker
sudo systemctl enable docker

# Add user to docker group (optional, for non-sudo access)
sudo usermod -aG docker $USER
```

### Go Dependencies
```bash
go get github.com/testcontainers/testcontainers-go
go get github.com/testcontainers/testcontainers-go/modules/postgres
go get github.com/stretchr/testify
```

## 🧪 Running the Tests

### Run All Integration Tests
```bash
# From project root
go test ./tests/integration/... -v
```

### Run Specific Test Categories
```bash
# End-to-end integration tests
go test ./tests/integration/... -run TestEndToEndIntegration -v

# Performance benchmarks
go test ./tests/integration/... -run TestPerformanceBenchmarks -v

# Data consistency tests
go test ./tests/integration/... -run TestDataConsistency -v
```

### Skip Integration Tests (Short Mode)
```bash
# Run only unit tests, skip integration tests
go test ./tests/integration/... -short
```

## 📊 Test Coverage

### 1. End-to-End Integration (`TestEndToEndIntegration`)
- **PostgreSQL Data Verification**: Creates tables, inserts data, verifies counts
- **DuckDB Proxy Connection**: Tests DuckDB proxy connectivity
- **Complex Queries**: Tests JOINs, WHERE clauses, ORDER BY
- **Aggregation Queries**: Tests SUM, COUNT, GROUP BY operations
- **Schema Management**: Tests schema manager functionality
- **Data Modification**: Tests INSERT, UPDATE, DELETE operations

### 2. Performance Benchmarks (`TestPerformanceBenchmarks`)
- **Simple Query Performance**: 100 COUNT queries timing
- **Complex Query Performance**: 50 JOIN queries timing
- **Concurrent Query Performance**: 10 parallel queries timing

### 3. Data Consistency (`TestDataConsistency`)
- **Transaction Consistency**: Tests ACID properties
- **Referential Integrity**: Tests foreign key constraints

## 🏗️ Test Infrastructure

### Container Setup
```go
// PostgreSQL container with testcontainers
postgresContainer, err := postgres.RunContainer(ctx,
    testcontainers.WithImage("postgres:15-alpine"),
    postgres.WithDatabase("testdb"),
    postgres.WithUsername("testuser"),
    postgres.WithPassword("testpass"),
    testcontainers.WithWaitStrategy(
        wait.ForLog("database system is ready to accept connections"),
    ),
)
```

### Test Data Schema
```sql
-- Users table
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    username VARCHAR(50) UNIQUE NOT NULL,
    email VARCHAR(100) UNIQUE NOT NULL,
    age INTEGER,
    country VARCHAR(50),
    is_active BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Products table
CREATE TABLE test_products (
    id SERIAL PRIMARY KEY,
    name VARCHAR(200) NOT NULL,
    description TEXT,
    price DECIMAL(10,2) NOT NULL,
    stock_quantity INTEGER DEFAULT 0,
    is_available BOOLEAN DEFAULT true,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Orders table with foreign keys
CREATE TABLE test_orders (
    id SERIAL PRIMARY KEY,
    user_id INTEGER REFERENCES test_users(id),
    product_id INTEGER REFERENCES test_products(id),
    quantity INTEGER NOT NULL,
    total_amount DECIMAL(10,2) NOT NULL,
    status VARCHAR(20) DEFAULT 'pending',
    order_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

## 🔧 Configuration

### Test Ports
- **PostgreSQL**: Dynamic port mapping via testcontainers
- **DuckDB Proxy**: Port 5433 (configurable)
- **Iceberg**: `/tmp/iceberg_test` directory

### Test Data
- **5 users** with realistic profiles (age, country, active status)
- **5 products** with pricing and stock information
- **5 orders** with user-product relationships and status tracking

## 🚨 Troubleshooting

### Docker Issues
```bash
# Check Docker status
sudo systemctl status docker

# Restart Docker
sudo systemctl restart docker

# Check Docker logs
sudo journalctl -u docker.service -f
```

### Port Conflicts
If you get port conflicts, modify the proxy port in the test:
```go
cfg.Proxy.Port = 5434 // Change to available port
```

### Container Startup Issues
The tests include proper wait strategies and retry logic. If containers fail to start:
1. Check Docker has enough resources (memory, disk space)
2. Ensure no firewall blocking container networking
3. Verify Docker daemon is running

## 📈 Performance Expectations

### Query Performance Targets
- **Simple COUNT queries**: < 5 seconds for 100 queries
- **Complex JOIN queries**: < 10 seconds for 50 queries  
- **Concurrent queries**: < 3 seconds for 10 parallel queries

### Container Startup Times
- **PostgreSQL**: ~10-15 seconds (first run), ~5 seconds (subsequent)
- **DuckDB Proxy**: ~2-3 seconds
- **Total test setup**: ~20-30 seconds

## 🎯 Benefits of Real Database Testing

1. **Real Performance Data**: Actual query execution times, not mocked
2. **Schema Validation**: Real SQL syntax and constraint testing
3. **Connection Testing**: Real network and authentication testing
4. **Data Integrity**: Real foreign key and transaction testing
5. **Production Parity**: Tests run against same database versions as production

## 🔄 Continuous Integration

These tests are designed to run in CI/CD pipelines:
- **Automatic container cleanup** after each test
- **Isolated test environments** per test run
- **Configurable timeouts** for CI environments
- **Skip options** for different test environments

## 📝 Adding New Tests

To add new integration tests:

1. **Create test function** following the naming convention `Test*`
2. **Use the test suite** for setup/cleanup
3. **Add cleanup logic** to prevent resource leaks
4. **Use require assertions** for test failures
5. **Add appropriate logging** for debugging

Example:
```go
func TestNewFeature(t *testing.T) {
    ts := &IntegrationTestSuite{}
    err := ts.setupIntegrationTest(t)
    require.NoError(t, err)
    defer ts.cleanup()
    
    // Your test logic here
    t.Run("TestSpecificFeature", func(t *testing.T) {
        // Test implementation
    })
}
```
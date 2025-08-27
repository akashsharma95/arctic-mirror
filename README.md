# Arctic Mirror

Arctic Mirror is a high-performance data replication system that captures PostgreSQL changes in real-time and stores them in Apache Iceberg format. It provides a DuckDB proxy for querying the replicated data with PostgreSQL compatibility.

## Features

- **Real-time PostgreSQL Replication**: Captures changes using logical replication
- **Apache Iceberg Storage**: Stores data in open, efficient Iceberg format
- **DuckDB Proxy**: PostgreSQL-compatible query interface
- **Health Monitoring**: Built-in health checks and metrics
- **Docker Support**: Easy deployment with Docker and Docker Compose
- **Comprehensive Testing**: Full test coverage for all components

## Architecture

```
PostgreSQL → Logical Replication → Arctic Mirror → Iceberg Files
                                    ↓
                              DuckDB Proxy ← Clients
```

### Components

- **Replicator**: Handles PostgreSQL logical replication
- **Iceberg Writer**: Converts replication events to Iceberg format
- **DuckDB Proxy**: Provides PostgreSQL-compatible query interface
- **Health Monitor**: Monitors system health and provides metrics
- **Storage Layer**: Supports local filesystem and S3 storage

## Quick Start

### Prerequisites

- Go 1.24+
- Docker and Docker Compose (for containerized deployment)
- PostgreSQL 15+ with logical replication enabled

### Local Development

1. **Clone the repository**
   ```bash
   git clone <repository-url>
   cd arctic-mirror
   ```

2. **Install dependencies**
   ```bash
   make deps
   ```

3. **Run tests**
   ```bash
   make test
   ```

4. **Build the application**
   ```bash
   make build
   ```

5. **Run locally**
   ```bash
   make run
   ```

### Docker Deployment

1. **Start all services**
   ```bash
   make docker-run
   ```

2. **Check status**
   ```bash
   make status
   ```

3. **View logs**
   ```bash
   docker-compose logs -f
   ```

4. **Stop services**
   ```bash
   make docker-stop
   ```

## Configuration

### Configuration File

Create a `config.yaml` file:

```yaml
postgres:
  host: localhost
  port: 5432
  user: replicator
  password: secret
  database: mydb
  slot: iceberg_replica
  publication: pub_all

tables:
  - schema: public
    name: users
  - schema: public
    name: orders

iceberg:
  path: /data/warehouse

proxy:
  port: 5433
```

### Environment Variables

- `POSTGRES_HOST`: PostgreSQL host
- `POSTGRES_PORT`: PostgreSQL port
- `POSTGRES_USER`: PostgreSQL user
- `POSTGRES_PASSWORD`: PostgreSQL password
- `POSTGRES_DB`: PostgreSQL database

## Usage

### Starting the Application

```bash
# Basic usage
./arctic-mirror --config config.yaml

# With custom health port
./arctic-mirror --config config.yaml --health-port 8080

# Help
./arctic-mirror --help
```

### Health Monitoring

The application provides health check endpoints:

- **Health Check**: `GET /health`
- **Detailed Health**: `GET /health/detailed`
- **Metrics**: `GET /metrics`

### Querying Data

Connect to the DuckDB proxy using any PostgreSQL client:

```bash
# Using psql
psql -h localhost -p 5433 -U replicator -d mydb

# Using any PostgreSQL client library
```

Example queries:

```sql
-- Query replicated data
SELECT * FROM users;

-- Join with orders
SELECT u.username, o.total_amount, o.status
FROM users u
JOIN orders o ON u.id = o.user_id;

-- Aggregations
SELECT status, COUNT(*), AVG(total_amount)
FROM orders
GROUP BY status;
```

## Development

### Project Structure

```
arctic-mirror/
├── config/          # Configuration management
├── health/          # Health monitoring
├── iceberg/         # Iceberg format handling
├── proxy/           # DuckDB proxy server
├── replication/     # PostgreSQL replication
├── schema/          # Schema management
├── storage/         # Storage abstractions
├── main.go          # Main application
├── Dockerfile       # Docker configuration
├── docker-compose.yml # Docker Compose setup
├── Makefile         # Development tasks
└── README.md        # This file
```

### Running Tests

```bash
# All tests
make test

# Tests with race detection
make test-race

# Specific package
go test ./config -v
```

### Code Quality

```bash
# Format code
make fmt

# Run linter
make lint

# Install development tools
make install-tools
```

### Development Workflow

```bash
# Complete development workflow
make dev

# Full rebuild
make rebuild
```

## Monitoring and Observability

### Health Checks

The system provides comprehensive health monitoring:

- **PostgreSQL Connection**: Connection status and latency
- **DuckDB Health**: Database availability and performance
- **Iceberg Storage**: File system accessibility and permissions
- **System Configuration**: Configuration validation

### Metrics

Prometheus-compatible metrics are available at `/metrics`:

- System uptime
- Component health status
- Connection latencies
- Error counts

### Logging

Structured logging with different levels:

- Startup and configuration information
- Replication events and errors
- Health check results
- Performance metrics

## Troubleshooting

### Common Issues

1. **PostgreSQL Connection Failed**
   - Verify PostgreSQL is running
   - Check connection credentials
   - Ensure logical replication is enabled

2. **Replication Slot Creation Failed**
   - Verify user has replication privileges
   - Check if slot already exists
   - Ensure WAL level is set to logical

3. **Iceberg Write Permission Denied**
   - Check directory permissions
   - Verify disk space
   - Ensure user has write access

4. **Proxy Connection Refused**
   - Verify proxy port is not in use
   - Check firewall settings
   - Ensure DuckDB extensions are loaded

### Debug Mode

Enable verbose logging by setting log level:

```bash
export LOG_LEVEL=debug
./arctic-mirror --config config.yaml
```

### Health Check Failures

Check component health:

```bash
# Basic health
curl http://localhost:8080/health

# Detailed health
curl http://localhost:8080/health/detailed

# Metrics
curl http://localhost:8080/metrics
```

## Performance Tuning

### Replication Performance

- Adjust WAL buffer size
- Optimize table schemas
- Use appropriate replication slot settings

### Storage Performance

- Use SSD storage for Iceberg files
- Optimize Parquet compression
- Consider partitioning strategies

### Proxy Performance

- Tune DuckDB memory settings
- Optimize query patterns
- Monitor connection pooling

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Ensure all tests pass
6. Submit a pull request

### Development Guidelines

- Follow Go coding standards
- Write comprehensive tests
- Update documentation
- Use meaningful commit messages

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Support

For support and questions:

- Create an issue on GitHub
- Check the troubleshooting section
- Review the configuration examples
- Consult the health monitoring endpoints

## Roadmap

- [ ] S3 storage backend
- [ ] Additional database support
- [ ] Advanced partitioning strategies
- [ ] Real-time analytics
- [ ] Kubernetes deployment
- [ ] Performance benchmarks
- [ ] Additional monitoring integrations
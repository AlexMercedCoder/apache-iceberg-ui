# Iceberg UI - Backend

FastAPI-based backend for the Iceberg UI application, providing REST API for Iceberg catalog operations.

## Tech Stack

- **FastAPI** - Web framework
- **PyIceberg** - Iceberg Python library
- **DataFusion** - SQL query engine
- **Uvicorn** - ASGI server
- **PyArrow** - Data processing and file I/O
- **SQLParse** - SQL parsing utilities

## Development

### Install Dependencies

```bash
pip install -r requirements.txt
```

### Run Development Server

```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

Or with proper Python path:

```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)
uvicorn backend.main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at `http://localhost:8000`.

### API Documentation

FastAPI provides automatic API documentation:
- Swagger UI: `http://localhost:8000/docs`
- ReDoc: `http://localhost:8000/redoc`

## Project Structure

```
backend/
├── main.py              # FastAPI application and API endpoints
├── catalog_manager.py   # Multi-catalog management
├── query_engine.py      # SQL query execution with DataFusion
├── sql_utils.py         # SQL parsing utilities
├── requirements.txt     # Python dependencies
├── cache/              # Query result cache (gitignored)
├── test_*.py           # Test scripts
└── debug_*.py          # Debug utilities
```

## Core Modules

### main.py
FastAPI application with endpoints for:
- Catalog connections (`/connect`, `/disconnect`, `/catalogs`)
- Namespace operations (`/catalogs/{catalog}/namespaces`)
- Table operations (`/catalogs/{catalog}/tables/{namespace}`)
- Query execution (`/query`, `/query/export`)
- File uploads (`/catalogs/{catalog}/tables/{namespace}/{table}/upload`)
- Table maintenance (`/catalogs/{catalog}/tables/{namespace}/{table}/maintenance`)

### catalog_manager.py
Manages multiple Iceberg catalog connections:
- `connect(name, properties)` - Connect to a catalog
- `disconnect(name)` - Disconnect from a catalog
- `get_catalog(name)` - Get catalog instance
- `list_catalogs()` - List connected catalogs
- `get_table(catalog, namespace, table)` - Get table reference
- `list_namespaces(catalog)` - List namespaces
- `list_tables(catalog, namespace)` - List tables

### query_engine.py
SQL query execution engine:
- Automatic table registration from SQL
- Cross-catalog query support
- DML operations (INSERT, DELETE)
- Query result caching
- Predicate pushdown optimization
- Column projection
- Time travel queries
- Metadata table queries (`$files`, `$snapshots`, etc.)

### sql_utils.py
SQL parsing utilities:
- WHERE clause extraction
- SELECT column extraction
- SQL to Iceberg filter conversion

## API Endpoints

### Catalog Management

#### Connect to Catalog
```http
POST /connect
Content-Type: application/json

{
  "name": "my_catalog",
  "properties": {
    "uri": "https://catalog.example.com/api/iceberg",
    "warehouse": "s3://my-warehouse",
    "type": "rest"
  }
}
```

#### Disconnect from Catalog
```http
POST /disconnect/{catalog_name}
```

#### List Connected Catalogs
```http
GET /catalogs
```

### Namespace Operations

#### List Namespaces
```http
GET /catalogs/{catalog}/namespaces
```

#### Create Namespace
```http
POST /catalogs/{catalog}/namespaces
Content-Type: application/json

{
  "namespace": "my_namespace",
  "properties": {
    "owner": "user@example.com"
  }
}
```

### Table Operations

#### List Tables
```http
GET /catalogs/{catalog}/tables/{namespace}
```

#### Get Table Metadata
```http
GET /catalogs/{catalog}/tables/{namespace}/{table}/metadata
```

#### Upload File to Table
```http
POST /catalogs/{catalog}/tables/{namespace}/{table}/upload
Content-Type: multipart/form-data

file: <CSV/JSON/Parquet file>
```

### Query Execution

#### Execute Query
```http
POST /query
Content-Type: application/json

{
  "sql": "SELECT * FROM my_namespace.my_table LIMIT 10",
  "catalog": "my_catalog",
  "namespace": "my_namespace"
}
```

#### Export Query Results
```http
POST /query/export
Content-Type: application/json

{
  "sql": "SELECT * FROM my_namespace.my_table",
  "format": "csv",
  "catalog": "my_catalog"
}
```

Supported formats: `csv`, `json`, `parquet`

### Table Maintenance

#### Expire Snapshots
```http
POST /catalogs/{catalog}/tables/{namespace}/{table}/maintenance
Content-Type: application/json

{
  "older_than_ms": 604800000
}
```

## Features

### Multi-Catalog Support
The backend can manage multiple catalog connections simultaneously:
- Each catalog has a unique name/alias
- Catalogs are stored in a dictionary for fast lookup
- Supports different catalog types (REST, Hive, Glue, etc.)

### Cross-Catalog Queries
Execute SQL queries that span multiple catalogs:
```sql
SELECT u.name, o.amount 
FROM catalog1.db.users u 
JOIN catalog2.db.orders o ON u.id = o.user_id
```

The query engine:
1. Parses the SQL to find all table references
2. Registers each table with DataFusion using unique aliases
3. Rewrites the SQL to use aliases
4. Executes the query

### DML Operations

#### INSERT
```sql
-- INSERT with VALUES
INSERT INTO my_namespace.my_table VALUES (1, 'Alice'), (2, 'Bob');

-- INSERT with SELECT
INSERT INTO my_namespace.my_table 
SELECT * FROM another_namespace.source_table WHERE id > 100;
```

#### DELETE
```sql
DELETE FROM my_namespace.my_table WHERE created_at < '2024-01-01';
```

### File Uploads
Upload CSV, JSON, or Parquet files to append data to tables:
- Automatic format detection
- Schema validation
- Atomic append operations

### Query Caching
Query results are cached using MD5 hash of:
- Rewritten SQL
- Table snapshot IDs

Cache is invalidated when:
- Table data changes (new snapshot)
- SQL query changes

### Time Travel
Query historical table snapshots:
```sql
SELECT * FROM my_namespace.my_table 
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';
```

### Metadata Tables
Query Iceberg metadata:
```sql
-- List all files
SELECT * FROM my_namespace.my_table$files;

-- List snapshots
SELECT * FROM my_namespace.my_table$snapshots;

-- View partitions
SELECT * FROM my_namespace.my_table$partitions;
```

## Configuration

### Environment Variables
- `PYTHONPATH` - Should include the backend directory
- `AWS_ACCESS_KEY_ID` - AWS credentials for S3
- `AWS_SECRET_ACCESS_KEY` - AWS credentials for S3
- `AWS_REGION` - AWS region

### env.json (Optional)
Auto-connect to a catalog on startup:
```json
{
  "catalog": {
    "uri": "https://catalog.example.com/api/iceberg",
    "warehouse": "s3://my-warehouse",
    "type": "rest"
  }
}
```

**Note**: `env.json` is gitignored. Never commit credentials.

## Testing

### Run Tests
```bash
# Test multi-catalog and DML
python backend/test_phase4.py

# Test specific features
python backend/test_connection.py
python backend/test_caching.py
python backend/test_schema_evolution.py
```

### Debug Scripts
```bash
# Debug regex patterns
python backend/debug_regex.py

# Debug SQL replacement
python backend/debug_replace.py
```

## Performance Optimization

### Query Caching
- Results cached in `backend/cache/` as Feather files
- Cache key includes SQL and table snapshots
- Automatic invalidation on data changes

### Predicate Pushdown
- WHERE clauses converted to Iceberg filters
- Filters applied during table scan
- Reduces data read from storage

### Column Projection
- Only requested columns are read
- Reduces memory usage and I/O
- Disabled for joins (to avoid column mismatch errors)

## Error Handling

The API returns standard HTTP status codes:
- `200` - Success
- `400` - Bad request (invalid SQL, missing parameters)
- `404` - Resource not found (table, namespace, catalog)
- `500` - Internal server error (query execution failure)

Error responses include detailed messages:
```json
{
  "detail": "NoSuchTableException: Table does not exist: my_namespace.my_table"
}
```

## Security Considerations

1. **Credentials**: Never commit `env.json` or `.env` files
2. **CORS**: Configure `allow_origins` in production
3. **Authentication**: Add authentication middleware for production use
4. **Input Validation**: SQL injection protection via parameterized queries
5. **Rate Limiting**: Consider adding rate limiting for production

## Deployment

### Docker
The Dockerfile builds a production-ready image:
```bash
docker build -t iceberg-ui .
docker run -p 8000:8000 iceberg-ui
```

### Environment Variables in Docker
```bash
docker run -p 8000:8000 \
  -e AWS_ACCESS_KEY_ID=your_key \
  -e AWS_SECRET_ACCESS_KEY=your_secret \
  iceberg-ui
```

### Volume Mounts
Mount `env.json` for auto-connect:
```bash
docker run -p 8000:8000 \
  -v $(pwd)/env.json:/app/env.json \
  iceberg-ui
```

## Contributing

When adding new features:
1. Update API endpoints in `main.py`
2. Add business logic to appropriate modules
3. Update this README with new endpoints
4. Add tests for new functionality
5. Update OpenAPI documentation (automatic via FastAPI)

## Troubleshooting

### Import Errors
Ensure `PYTHONPATH` includes the backend directory:
```bash
export PYTHONPATH=$PYTHONPATH:$(pwd)/backend
```

### Connection Errors
Check catalog URI and credentials in `env.json` or connection request.

### Query Failures
- Check DataFusion logs for SQL parsing errors
- Verify table names are fully qualified
- Ensure tables exist in the specified catalog

### Cache Issues
Clear the cache directory:
```bash
rm -rf backend/cache/*
```

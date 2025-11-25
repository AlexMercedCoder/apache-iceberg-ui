# Iceberg UI

A modern web application for managing Apache Iceberg tables via a REST Catalog.

## 🎉 What's New in Phase 4

- **Multi-Catalog Support**: Connect to multiple catalogs simultaneously and switch between them
- **Cross-Catalog Joins**: Query and join tables from different catalogs in a single SQL statement
- **DML Operations**: Execute INSERT and DELETE statements directly on Iceberg tables
- **File Uploads**: Upload CSV, JSON, and Parquet files to append data to your tables
- **Enhanced UI**: Improved catalog management with dropdown selector and logout functionality

## Features

### Core Functionality
- **Multi-Catalog Support**: Connect to and manage multiple Iceberg catalogs simultaneously
- **Table Management**: Browse namespaces and tables across all connected catalogs
- **SQL Querying**: Run SQL queries with Apache DataFusion, including cross-catalog joins
- **DML Operations**: Execute INSERT and DELETE statements on Iceberg tables
- **File Uploads**: Upload CSV, JSON, and Parquet files to append data to tables
- **Metadata Viewer**: View table schema, snapshots, properties, and statistics
- **Table Maintenance**: Perform snapshot expiration and other maintenance tasks
- **Schema Evolution**: Add, rename, drop, and update table columns
- **Time Travel**: Query historical table snapshots
- **Modern UI**: Built with React and Material UI, featuring Light/Dark modes

### Advanced Features
- **Cross-Catalog Joins**: Query and join tables from different catalogs in a single SQL statement
- **Query Caching**: Automatic caching of query results for improved performance
- **Export Results**: Export query results to CSV, JSON, or Parquet formats
- **Query History**: Track and reuse previous queries
- **Saved Queries**: Save frequently used queries for quick access

## Architecture

- **Backend**: Python (FastAPI)
    - **PyIceberg**: Handles interactions with the Iceberg REST Catalog.
    - **DataFusion**: Provides a high-performance query engine.
- **Frontend**: React (Vite)
    - **Material UI**: For a polished, responsive user interface.

## Getting Started

### Prerequisites

- Python 3.9+
- Node.js 16+
- (Optional) An Iceberg catalog server - you can use the included Docker Compose setup with Nessie + MinIO

### Backend Setup

1. Navigate to the `backend` directory:
   ```bash
   cd backend
   ```
2. Create a virtual environment and install dependencies:
   ```bash
   python -m venv venv
   source venv/bin/activate
   pip install -r requirements.txt
   ```
3. Start the server:
   ```bash
   uvicorn main:app --reload
   ```

### Frontend Setup

1. Navigate to the `frontend` directory:
   ```bash
   cd frontend
   ```
2. Install dependencies:
   ```bash
   npm install
   ```
3. Start the development server:
   ```bash
   npm run dev
   ```

### Configuration

#### Option 1: Using env.json (Auto-connect on startup)

Copy `example.env.json` to `env.json` and update it with your catalog details. The application will automatically connect to this catalog on startup.

```json
{
  "catalog": {
    "uri": "https://catalog.example.com/api/iceberg",
    "oauth2-server-uri": "https://auth.example.com/oauth/token",
    "token": "your-token-here",
    "warehouse": "s3://your-warehouse",
    "type": "rest"
  }
}
```

**Note**: `env.json` is gitignored for security. Never commit credentials to version control.

#### Option 2: Connect via UI

You can also connect to catalogs directly through the UI without pre-configuring `env.json`. This allows you to:
- Connect to multiple catalogs in a single session
- Give each catalog a friendly name
- Switch between catalogs easily

#### Supported Catalog Types

- **REST**: Iceberg REST Catalog (Dremio, Polaris, Nessie, etc.)
- **Hive**: Hive Metastore
- **Glue**: AWS Glue Data Catalog
- **DynamoDB**: AWS DynamoDB Catalog
- **SQL**: PostgreSQL, MySQL, SQLite catalogs

## Usage

### Connecting to Catalogs

1. Open your browser to the frontend URL (usually `http://localhost:5173`).
2. Click "Connect" and enter your catalog connection details:
   - **Catalog Name**: A friendly name for this connection (e.g., "production", "staging")
   - **Catalog Type**: REST, Hive, Glue, etc.
   - **URI**: The catalog endpoint URL
   - **Warehouse**: The warehouse location (S3, HDFS, etc.)
   - **Credentials**: Authentication details if required

3. You can connect to multiple catalogs and switch between them using the catalog selector.

### Browsing Tables

1. Use the sidebar explorer to browse namespaces and tables.
2. Click on a table to view its metadata, schema, and snapshots.
3. Use the upload button (cloud icon) next to any table to upload data files.
4. Use the "Play" button (▶️) next to any table to instantly populate a `SELECT *` query in the editor.

### Running SQL Queries

Execute SQL queries in the Query Editor:

```sql
-- Simple query
SELECT * FROM my_namespace.my_table LIMIT 10;

-- Cross-catalog join
SELECT u.name, o.amount 
FROM catalog1.db.users u 
JOIN catalog2.db.orders o ON u.id = o.user_id;

-- INSERT data
INSERT INTO my_namespace.my_table VALUES (1, 'Alice'), (2, 'Bob');

-- DELETE data
DELETE FROM my_namespace.my_table WHERE id > 100;

-- Time travel
SELECT * FROM my_namespace.my_table 
FOR SYSTEM_TIME AS OF TIMESTAMP '2024-01-01 00:00:00';
```

### Uploading Files

### Uploading Files

1. **Append Data**: Navigate to an existing table and click the upload icon (cloud) next to the table name.
2. **Create Table**: Click the upload icon on a **Namespace** folder.
3. Select a CSV, JSON, or Parquet file.
4. If creating a new table, enter a name. The schema will be automatically inferred from the file.
5. The data will be uploaded and the table created/updated.

### Exporting Results

After running a query:
1. Click the "Export" button.
2. Choose your format (CSV, JSON, or Parquet).
3. The file will be downloaded to your browser.

### Managing Catalogs

- **Switch Catalogs**: Use the dropdown in the explorer to switch between connected catalogs.
- **Log Out**: Click "Log Out" in the header to disconnect from all catalogs.

## Testing

The project includes an End-to-End (E2E) testing suite using Playwright.

```bash
cd frontend
npx playwright test
```

## Docker Usage 🐳

You can run the application easily using the pre-built Docker image.

### Quick Start (Standalone)

Run the UI on port 8000:

```bash
docker run -p 8000:8000 alexmerced/iceberg-ui
```

Access the UI at `http://localhost:8000`.

### Full Environment (with Nessie & Minio)

To spin up a complete testing environment with a Nessie Catalog and Minio S3 storage, use the provided `docker-compose.yml` (or create one):

```yaml
version: '3.8'
services:
  iceberg-ui:
    image: alexmerced/iceberg-ui
    ports:
      - "8000:8000"
    environment:
      - AWS_ACCESS_KEY_ID=admin
      - AWS_SECRET_ACCESS_KEY=password
      - AWS_REGION=us-east-1
      - S3_ENDPOINT=http://minio:9000
      - CATALOG_URI=http://nessie:19120/api/v1
      - CATALOG_WAREHOUSE=s3://warehouse/
    depends_on:
      - nessie
      - minio

  nessie:
    image: projectnessie/nessie:latest
    ports:
      - "19120:19120"
    environment:
      - QUARKUS_HTTP_PORT=19120
      - NESSIE_VERSION_STORE_TYPE=INMEMORY
      - NESSIE_SERVER_DEFAULT_BRANCH=main
      # Credential Vending
      - NESSIE_CATALOG_DEFAULT_WAREHOUSE=s3://warehouse/
      - NESSIE_CATALOG_ICEBERG_CONFIG_DEFAULTS_S3_ENDPOINT=http://minio:9000
      - NESSIE_CATALOG_ICEBERG_CONFIG_DEFAULTS_S3_PATH_STYLE_ACCESS=true
      - NESSIE_CATALOG_ICEBERG_CONFIG_DEFAULTS_S3_ACCESS_KEY_ID=admin
      - NESSIE_CATALOG_ICEBERG_CONFIG_DEFAULTS_S3_SECRET_ACCESS_KEY=password
      - NESSIE_CATALOG_ICEBERG_CONFIG_DEFAULTS_CLIENT_REGION=us-east-1

  minio:
    image: minio/minio:latest
    ports:
      - "9000:9000"
      - "9001:9001"
    environment:
      - MINIO_ROOT_USER=admin
      - MINIO_ROOT_PASSWORD=password
    command: server /data --console-address ":9001"

  createbuckets:
    image: minio/mc
    depends_on:
      - minio
    entrypoint: >
      /bin/sh -c "
      /usr/bin/mc alias set myminio http://minio:9000 admin password;
      /usr/bin/mc mb myminio/warehouse;
      /usr/bin/mc anonymous set public myminio/warehouse;
      exit 0;
      "
```

Run with:
```bash
docker-compose up -d
```

- [Hands-on Tutorial with Apache Polaris based Dremio Catalog](https://www.dremio.com/blog/hands-on-introduction-to-dremio-cloud-next-gen-self-guided-workshop/)
- [Reference Blog on Catalog Settings when Using Nessie REST](https://www.dremio.com/blog/intro-to-pyiceberg/)
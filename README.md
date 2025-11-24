# Iceberg UI

A modern web application for managing Apache Iceberg tables via a REST Catalog.

## Features

- **Table Management**: Browse namespaces and tables, view metadata (schema, snapshots).
- **SQL Querying**: Run SQL queries against your Iceberg tables using Apache DataFusion.
- **Maintenance**: Perform table maintenance tasks like snapshot expiration.
- **Modern UI**: Built with React and Material UI, featuring Light/Dark modes.

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
- An Iceberg REST Catalog (e.g., `rest-catalog-open-api` compliant server).

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

Copy `example.env.json` to `env.json` and update it with your catalog details. Alternatively, you can provide these details via the UI.

```json
{
  "catalog": {
    "uri": "http://localhost:8181",
    "credential": "client:secret",
    "warehouse": "s3://bucket/warehouse"
  }
}
```

## Usage

1. Open your browser to the frontend URL (usually `http://localhost:5173`).
2. Enter your catalog connection details if not already configured via `env.json`.
3. Browse tables in the sidebar.
4. Use the "Query" tab to run SQL.
5. Use the "Metadata" tab to view table details.

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
---
description: Run Iceberg UI with Docker
---

# Run Iceberg UI with Docker

This workflow describes how to build and run the Iceberg UI using Docker and Docker Compose.

## Prerequisites
- Docker
- Docker Compose

## Steps

1. **Build the Docker Image**
   ```bash
   docker build -t iceberg-ui .
   ```

2. **Run with Docker Compose (Full Environment)**
   This starts the Iceberg UI along with Nessie (Catalog) and Minio (S3 Storage).
   ```bash
   docker-compose up -d
   ```

3. **Access the Application**
   - **UI**: http://localhost:8000
   - **Nessie API**: http://localhost:19120
   - **Minio Console**: http://localhost:9001 (User: admin, Pass: password)

4. **Connect to Catalog**
   In the UI Connection screen, use:
   - **URI**: http://nessie:19120/api/v1
   - **Warehouse**: s3://warehouse/
   - **Access Key**: admin
   - **Secret Key**: password
   - **S3 Endpoint**: http://minio:9000
   - **Region**: us-east-1

## Development vs Production

The Docker setup is optimized for production-like usage (serving built static files). 
For local development with hot-reloading, continue using `npm run dev` and `uvicorn main:app --reload`.

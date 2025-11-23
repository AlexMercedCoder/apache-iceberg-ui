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

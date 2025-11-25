# Architecture Documentation

## Backend (`backend/`)

### `main.py`
The entry point for the FastAPI application.
- **`POST /connect`**: Connects to an Iceberg catalog.
- **`POST /disconnect/{name}`**: Disconnects a catalog.
- **`GET /catalogs`**: Lists connected catalogs.
- `POST /catalogs/{catalog}/namespaces`: Create a new namespace.
- `POST /catalogs/{catalog}/namespaces/{namespace}/upload`: Create a new table from an uploaded file (CSV, JSON, Parquet).
- `GET /catalogs/{catalog}/tables/{namespace}`: Lists tables in a namespace.
- **`GET /catalogs/{catalog}/tables/{namespace}/{table}/metadata`**: Retrieves table metadata (schema, snapshots, properties).
- **`GET /catalogs/{catalog}/tables/{namespace}/{table}/stats`**: Retrieves table statistics (files, partitions).
- **`POST /query`**: Executes a SQL query using DuckDB.
- **`POST /query/export`**: Exports query results to CSV, JSON, or Parquet.
- **`POST .../upload`**: Uploads a file (CSV/JSON/Parquet) to a table.

### `catalog_manager.py`
Manages Iceberg catalog connections using `pyiceberg`.
- **`connect(name, properties)`**: Connects to a catalog.
- **`get_table_metadata(...)`**: Returns standardized metadata dict.
- **`get_table_stats(...)`**: Returns file and partition stats.
- **`_load_initial_config()`**: Loads catalogs from `env.json`.

### `query_engine.py`
Handles SQL execution using DuckDB.
- **`execute_query(sql)`**: Runs SQL against registered Iceberg tables.
- **`register_all_tables_in_namespace(...)`**: Registers tables as DuckDB views for querying.

## Frontend (`frontend/src/`)

### `App.jsx`
Main application component.
- Manages global state (`connected`, `catalogs`, `activeCatalog`, `theme`).
- Renders `AppBar` (Logo, Theme Toggle), `Drawer` (TableExplorer), and Main Content (Tabs).

### `components/TableExplorer.jsx`
Sidebar component for browsing catalogs.
- **Features**: Catalog selector, "Add Catalog" button, Namespace expansion, Table listing.
- **Actions**: Select table, Drag table (for query), Upload file.

### `components/QueryEditor.jsx`
SQL query interface.
- **Features**: Text editor, Run button, "Select *" template, "Create Table" template, History drawer, Export buttons.
- **State**: `sql` (current query), `results` (table data).

### `components/MetadataViewer.jsx`
Displays details for a selected table.
- **Tabs**:
    - **Overview**: Table properties.
    - **Schema**: Column definitions.
    - **Snapshots**: History of table snapshots.
    - **Visualization**: Charts for file sizes and partitions.

### `components/ConnectionForm.jsx`
Form for connecting to a new catalog.
- Supports OAuth2 (Client Creds, Token) and custom properties.

### `components/MetadataCharts.jsx`
Renders charts in the Visualization tab.
- Uses `recharts` to display file size distribution and partition counts.

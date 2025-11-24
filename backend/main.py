from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, StreamingResponse
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
import os
import io
import json
import traceback
import pyarrow as pa
import pyarrow.csv as csv
import pyarrow.parquet as parquet
from catalog_manager import CatalogManager
from query_engine import QueryEngine

app = FastAPI()

# Allow CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # In production, specify the frontend URL
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files if the directory exists (for production/docker)
static_dir = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_dir):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_dir, "assets")), name="assets")

catalog_manager = CatalogManager()
query_engine = QueryEngine(catalog_manager)

class ConnectRequest(BaseModel):
    properties: Dict[str, str]

class CreateNamespaceRequest(BaseModel):
    namespace: str
    properties: Optional[Dict[str, str]] = None

class QueryRequest(BaseModel):
    sql: str
    namespace: Optional[str] = None # Optional context to load tables from

class MaintenanceRequest(BaseModel):
    older_than_ms: Optional[int] = None

@app.post("/connect")
def connect_catalog(request: ConnectRequest):
    try:
        catalog_manager.connect(request.properties)
        return {"status": "connected", "config": request.properties}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/status")
def get_status():
    return {"connected": catalog_manager.catalog is not None}

@app.get("/namespaces")
def list_namespaces():
    try:
        namespaces = catalog_manager.list_namespaces()
        # namespaces are tuples, convert to list of strings or list of lists
        return {"namespaces": [ns for ns in namespaces]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/namespaces")
def create_namespace(request: CreateNamespaceRequest):
    try:
        catalog_manager.create_namespace(request.namespace, request.properties)
        return {"status": "created", "namespace": request.namespace}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/tables/{namespace}")
def list_tables(namespace: str):
    try:
        tables = catalog_manager.list_tables(namespace)
        return {"tables": [t[-1] for t in tables]} # Return just table names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{namespace}/{table}/metadata")
def get_table_metadata(namespace: str, table: str):
    try:
        metadata = catalog_manager.get_table_metadata(namespace, table)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.post("/query")
def run_query(query: QueryRequest):
    try:
        # If a namespace is provided, pre-load tables from it
        if query.namespace:
            query_engine.register_all_tables_in_namespace(query.namespace)
        
        result = query_engine.execute_query(query.sql)
        return {"data": result}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class ExportRequest(BaseModel):
    sql: str
    format: str = "csv" # csv, json, parquet

@app.post("/query/export")
def export_query(request: ExportRequest):
    try:
        # Get result as Arrow Table
        arrow_table = query_engine.execute_query(request.sql, return_arrow=True)
        
        # Handle metadata table results (list of dicts)
        if isinstance(arrow_table, list):
             # Convert list of dicts to Arrow Table for consistent export
             if not arrow_table:
                 # Empty result
                 arrow_table = pa.Table.from_pylist([])
             else:
                 arrow_table = pa.Table.from_pylist(arrow_table)

        if request.format.lower() == 'csv':
            # Write to memory buffer
            sink = io.BytesIO()
            csv.write_csv(arrow_table, sink)
            sink.seek(0)
            return StreamingResponse(
                sink, 
                media_type="text/csv", 
                headers={"Content-Disposition": "attachment; filename=export.csv"}
            )
        
        elif request.format.lower() == 'json':
            # For JSON, we can just use pylist and json dump, or arrow json
            # Using pylist is simpler for now as Arrow JSON is line-delimited usually
            data = arrow_table.to_pylist()
            sink = io.StringIO()
            json.dump(data, sink, default=str)
            sink.seek(0)
            return StreamingResponse(
                sink,
                media_type="application/json",
                headers={"Content-Disposition": "attachment; filename=export.json"}
            )
            
        elif request.format.lower() == 'parquet':
            sink = io.BytesIO()
            parquet.write_table(arrow_table, sink)
            sink.seek(0)
            return StreamingResponse(
                sink,
                media_type="application/vnd.apache.parquet",
                headers={"Content-Disposition": "attachment; filename=export.parquet"}
            )
            
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported format: {request.format}")

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/maintenance")
def expire_snapshots(namespace: str, table: str, request: MaintenanceRequest):
    try:
        catalog_manager.expire_snapshots(namespace, table, request.older_than_ms)
        return {"status": "maintenance_completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

class AddColumnRequest(BaseModel):
    name: str
    type: str
    required: bool = False

class RenameColumnRequest(BaseModel):
    name: str
    new_name: str

class UpdateColumnRequest(BaseModel):
    name: str
    new_type: str

class DropColumnRequest(BaseModel):
    name: str

@app.post("/tables/{namespace}/{table}/schema/add")
def add_column(namespace: str, table: str, request: AddColumnRequest):
    try:
        catalog_manager.add_column(namespace, table, request.name, request.type, request.required)
        return {"status": "column_added"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/schema/drop")
def drop_column(namespace: str, table: str, request: DropColumnRequest):
    try:
        catalog_manager.drop_column(namespace, table, request.name)
        return {"status": "column_dropped"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/schema/rename")
def rename_column(namespace: str, table: str, request: RenameColumnRequest):
    try:
        catalog_manager.rename_column(namespace, table, request.name, request.new_name)
        return {"status": "column_renamed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/schema/update")
def update_column(namespace: str, table: str, request: UpdateColumnRequest):
    try:
        catalog_manager.update_column_type(namespace, table, request.name, request.new_type)
        return {"status": "column_updated"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/load")
def load_table_for_query(namespace: str, table: str):
    try:
        query_engine._ensure_table_registered(namespace, table)
        return {"status": "loaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/tables/{namespace}/{table}/stats")
def get_table_stats(namespace: str, table: str):
    try:
        # Query metadata tables
        # We need to ensure the table is registered first
        query_engine._ensure_table_registered(namespace, table)
        
        # Get partitions
        partitions_sql = f"SELECT * FROM {namespace}.{table}$partitions"
        partitions_data = query_engine.execute_query(partitions_sql)
        
        # Get files (limit to avoid huge response if many files)
        files_sql = f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {namespace}.{table}$files LIMIT 1000"
        files_data = query_engine.execute_query(files_sql)
        
        return {
            "partitions": partitions_data,
            "files": files_data
        }
    except Exception as e:
        # If metadata tables fail (e.g. unpartitioned), return empty
        print(f"Stats query failed: {e}")
        return {"partitions": [], "files": []}

# Catch-all for SPA client-side routing
@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if os.path.exists(static_dir):
        index_path = os.path.join(static_dir, "index.html")
        if os.path.exists(index_path):
            return FileResponse(index_path)
    return {"message": "API is running. Static files not found (dev mode)."}

from fastapi import FastAPI, HTTPException, Body, UploadFile, File
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
# Allow CORS
origins = ["*"]
frontend_url = os.environ.get("FRONTEND_URL")
if frontend_url:
    origins = frontend_url.split(",")

app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

from pathlib import Path

# Serve static files if the directory exists (for production/docker)
BASE_DIR = Path(__file__).resolve().parent
static_dir = BASE_DIR / "static"

print(f"DEBUG: static_dir calculated as: {static_dir}")
print(f"DEBUG: static_dir exists: {static_dir.exists()}")

if static_dir.exists():
    assets_dir = static_dir / "assets"
    print(f"DEBUG: Mounting static assets from {assets_dir}")
    app.mount("/assets", StaticFiles(directory=str(assets_dir)), name="assets")
else:
    print("DEBUG: Static directory not found, skipping mount.")

catalog_manager = CatalogManager()
query_engine = QueryEngine(catalog_manager)

class ConnectRequest(BaseModel):
    name: str = "default"
    properties: Dict[str, str]

class CreateNamespaceRequest(BaseModel):
    namespace: str
    properties: Optional[Dict[str, str]] = None

class QueryRequest(BaseModel):
    sql: str
    catalog: str = "default" # Default context
    namespace: Optional[str] = None # Optional context

class MaintenanceRequest(BaseModel):
    older_than_ms: Optional[int] = None

@app.post("/connect")
def connect_catalog(request: ConnectRequest):
    try:
        catalog_manager.connect(request.name, request.properties)
        return {"status": "connected", "name": request.name}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/disconnect/{name}")
def disconnect_catalog(name: str):
    catalog_manager.disconnect(name)
    return {"status": "disconnected"}

@app.get("/status")
def get_status():
    return {"connected": catalog_manager.catalog is not None}

@app.get("/catalogs")
def list_catalogs():
    return {"catalogs": catalog_manager.list_catalogs()}

@app.get("/catalogs/{catalog}/namespaces")
def list_namespaces(catalog: str):
    try:
        namespaces = catalog_manager.list_namespaces(catalog)
        # namespaces are tuples, convert to list of strings or list of lists
        return {"namespaces": [ns for ns in namespaces]}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/catalogs/{catalog}/namespaces")
def create_namespace(catalog: str, request: CreateNamespaceRequest):
    try:
        catalog_manager.create_namespace(catalog, request.namespace, request.properties)
        return {"status": "created", "namespace": request.namespace}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/catalogs/{catalog}/tables/{namespace}")
def list_tables(catalog: str, namespace: str):
    try:
        tables = catalog_manager.list_tables(catalog, namespace)
        return {"tables": [t[-1] for t in tables]} # Return just table names
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/catalogs/{catalog}/tables/{namespace}/{table}/metadata")
def get_table_metadata(catalog: str, namespace: str, table: str):
    try:
        metadata = catalog_manager.get_table_metadata(catalog, namespace, table)
        return metadata
    except Exception as e:
        raise HTTPException(status_code=404, detail=str(e))

@app.get("/catalogs/{catalog}/tables/{namespace}/{table}/stats")
def get_table_stats(catalog: str, namespace: str, table: str):
    try:
        stats = catalog_manager.get_table_stats(catalog, namespace, table)
        return stats
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/query")
def run_query(query: QueryRequest):
    try:
        # If a namespace is provided, pre-load tables from it
        if query.namespace:
            query_engine.register_all_tables_in_namespace(query.catalog, query.namespace)
        
        result = query_engine.execute_query(query.sql, default_catalog=query.catalog)
        return {"data": result}
    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

class ExportRequest(BaseModel):
    sql: str
    format: str = "csv" # csv, json, parquet
    catalog: str = "default"

@app.post("/query/export")
def export_query(request: ExportRequest):
    try:
        # Get result as Arrow Table
        arrow_table = query_engine.execute_query(request.sql, return_arrow=True, default_catalog=request.catalog)
        
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

@app.post("/catalogs/{catalog}/tables/{namespace}/{table}/maintenance")
def expire_snapshots(catalog: str, namespace: str, table: str, request: MaintenanceRequest):
    try:
        catalog_manager.expire_snapshots(catalog, namespace, table, request.older_than_ms)
        return {"status": "maintenance_completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/catalogs/{catalog}/tables/{namespace}/{table}/load")
def load_table_for_query(catalog: str, namespace: str, table: str):
    try:
        query_engine._ensure_table_registered(catalog, namespace, table)
        return {"status": "loaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))



@app.post("/catalogs/{catalog}/tables/{namespace}/{table}/upload")
async def upload_file(catalog: str, namespace: str, table: str, file: UploadFile = File(...)):
    try:
        contents = await file.read()
        
        # Determine file type
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            # Convert CSV to Arrow Table
            arrow_table = csv.read_csv(io.BytesIO(contents))
        elif filename.endswith('.json'):
            # Convert JSON to Arrow Table
            data = json.loads(contents)
            if isinstance(data, dict):
                data = [data]
            arrow_table = pa.Table.from_pylist(data)
        elif filename.endswith('.parquet'):
            # Read Parquet to Arrow Table
            arrow_table = parquet.read_table(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, JSON, or Parquet.")

        # Append to Iceberg table
        # We need to get the table first
        iceberg_table = catalog_manager.get_table(catalog, namespace, table)
        
        # Ensure name mapping exists to avoid "Parquet file does not have field-ids" error
        # This is important if the file being written (or read back) lacks field IDs
        if "schema.name-mapping.default" not in iceberg_table.properties:
             try:
                 from pyiceberg.table.name_mapping import name_mapping_from_schema
                 name_mapping = name_mapping_from_schema(iceberg_table.schema())
                 # Use transaction to update properties
                 with iceberg_table.transaction() as txn:
                     txn.set_properties({"schema.name-mapping.default": name_mapping.model_dump_json()})
             except Exception as e:
                 print(f"Warning: Failed to set name mapping: {e}")

        iceberg_table.append(arrow_table)
        
        return {"status": "uploaded", "rows_appended": arrow_table.num_rows}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/catalogs/{catalog}/namespaces/{namespace}/upload")
async def create_table_from_file(catalog: str, namespace: str, table_name: str = Body(...), file: UploadFile = File(...)):
    try:
        contents = await file.read()
        
        # Determine file type
        filename = file.filename.lower()
        if filename.endswith('.csv'):
            # Convert CSV to Arrow Table
            arrow_table = csv.read_csv(io.BytesIO(contents))
        elif filename.endswith('.json'):
            # Convert JSON to Arrow Table
            data = json.loads(contents)
            if isinstance(data, dict):
                data = [data]
            arrow_table = pa.Table.from_pylist(data)
        elif filename.endswith('.parquet'):
            # Read Parquet to Arrow Table
            arrow_table = parquet.read_table(io.BytesIO(contents))
        else:
            raise HTTPException(status_code=400, detail="Unsupported file type. Use CSV, JSON, or Parquet.")

        # Create table with inferred schema
        # Create table with inferred schema
        # Manual conversion to avoid "Parquet file does not have field-ids" error from pyarrow_to_schema
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, StringType, IntegerType, LongType, FloatType, DoubleType, BooleanType, DateType, TimestampType
        
        def arrow_type_to_iceberg(arrow_type):
            if pa.types.is_string(arrow_type) or pa.types.is_large_string(arrow_type):
                return StringType()
            elif pa.types.is_int32(arrow_type):
                return IntegerType()
            elif pa.types.is_int64(arrow_type):
                return LongType()
            elif pa.types.is_float32(arrow_type):
                return FloatType()
            elif pa.types.is_float64(arrow_type):
                return DoubleType()
            elif pa.types.is_boolean(arrow_type):
                return BooleanType()
            elif pa.types.is_date32(arrow_type) or pa.types.is_date64(arrow_type):
                return DateType()
            elif pa.types.is_timestamp(arrow_type):
                return TimestampType()
            else:
                return StringType() # Default to string for unknown types

        fields = []
        for i, field in enumerate(arrow_table.schema):
            iceberg_type = arrow_type_to_iceberg(field.type)
            fields.append(NestedField(field_id=i+1, name=field.name, field_type=iceberg_type, required=False))
            
        iceberg_schema = Schema(*fields)
        
        catalog_obj = catalog_manager.get_catalog(catalog)
        
        # Check if table exists
        try:
            catalog_obj.load_table(f"{namespace}.{table_name}")
            raise HTTPException(status_code=400, detail=f"Table {namespace}.{table_name} already exists.")
        except:
            pass # Table does not exist, proceed
            
        # Prepare properties with name mapping
        properties = {}
        try:
            from pyiceberg.table.name_mapping import name_mapping_from_schema
            name_mapping = name_mapping_from_schema(iceberg_schema)
            properties["schema.name-mapping.default"] = name_mapping.model_dump_json()
        except Exception as e:
            print(f"Warning: Failed to generate name mapping: {e}")

        # Create table
        table = catalog_obj.create_table(f"{namespace}.{table_name}", schema=iceberg_schema, properties=properties)
        
        # Append data
        table.append(arrow_table)
        
        return {"status": "created", "table": table_name, "rows": arrow_table.num_rows}

    except Exception as e:
        traceback.print_exc()
        raise HTTPException(status_code=500, detail=str(e))


# Catch-all for SPA client-side routing
@app.get("/{full_path:path}")
async def serve_spa(full_path: str):
    if static_dir.exists():
        index_path = static_dir / "index.html"
        if index_path.exists():
            return FileResponse(index_path)
        else:
             print(f"DEBUG: index.html not found at {index_path}")
    else:
        print(f"DEBUG: static_dir not found at {static_dir}")
    return {"message": "API is running. Static files not found (dev mode)."}

if __name__ == "__main__":
    import uvicorn
    # Check BACKEND_PORT, then PORT, then default 8000
    port = int(os.environ.get("BACKEND_PORT", os.environ.get("PORT", 8000)))
    uvicorn.run(app, host="0.0.0.0", port=port)

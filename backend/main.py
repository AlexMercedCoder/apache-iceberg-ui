from fastapi import FastAPI, HTTPException, Body
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import Dict, Any, List, Optional
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
def run_query(request: QueryRequest):
    try:
        # If a namespace is provided, pre-load tables from it
        if request.namespace:
            query_engine.register_all_tables_in_namespace(request.namespace)
        
        results = query_engine.execute_query(request.sql)
        return {"results": results}
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.post("/tables/{namespace}/{table}/maintenance")
def expire_snapshots(namespace: str, table: str, request: MaintenanceRequest):
    try:
        catalog_manager.expire_snapshots(namespace, table, request.older_than_ms)
        return {"status": "maintenance_completed"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.post("/tables/{namespace}/{table}/load")
def load_table_for_query(namespace: str, table: str):
    try:
        query_engine._ensure_table_registered(namespace, table)
        return {"status": "loaded"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

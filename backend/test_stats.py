import os
import sys
import json

# Add current directory to sys.path
sys.path.append(os.path.dirname(__file__))

from catalog_manager import CatalogManager
from query_engine import QueryEngine

def test_stats():
    print("--- Testing Table Stats ---")
    
    # Load config (env vars or json)
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "env.json")
    
    config = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            data = json.load(f)
            config = data.get("catalog", {})
    else:
        # Fallback to env vars
        uri = os.environ.get("CATALOG_URI")
        if uri:
            config = {
                "uri": uri,
                "warehouse": os.environ.get("CATALOG_WAREHOUSE"),
                "s3.endpoint": os.environ.get("S3_ENDPOINT"),
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "s3.region": os.environ.get("AWS_REGION"),
                "type": "rest"
            }
            
    if not config:
        print("FAIL: No configuration found")
        return

    manager = CatalogManager()
    manager.connect(config)
    engine = QueryEngine(manager)
    
    namespace = "db"
    table_name = "schema_test" # Use the table created in previous test
    
    # Ensure table exists (it should from previous test)
    try:
        manager.get_table(namespace, table_name)
    except:
        print(f"Table {namespace}.{table_name} not found. Creating...")
        from pyiceberg.schema import Schema
        from pyiceberg.types import NestedField, IntegerType
        schema = Schema(NestedField(1, "id", IntegerType(), required=False))
        manager.catalog.create_table(f"{namespace}.{table_name}", schema=schema)

    print(f"\n1. Fetching stats for {namespace}.{table_name}...")
    
    try:
        # Simulate what the endpoint does
        engine._ensure_table_registered(namespace, table_name)
        
        partitions_sql = f"SELECT * FROM {namespace}.{table_name}$partitions"
        print(f"Executing: {partitions_sql}")
        partitions_data = engine.execute_query(partitions_sql)
        print(f"Partitions: {len(partitions_data)} rows")
        
        files_sql = f"SELECT file_path, file_format, record_count, file_size_in_bytes FROM {namespace}.{table_name}$files LIMIT 10"
        print(f"Executing: {files_sql}")
        files_data = engine.execute_query(files_sql)
        print(f"Files: {len(files_data)} rows")
        
        if isinstance(files_data, list):
            print("PASS: Files data returned as list")
        else:
            print("FAIL: Files data is not a list")
            
    except Exception as e:
        print(f"FAIL: Stats query failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_stats()

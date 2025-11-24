import os
import sys
import time

# Add current directory to sys.path
sys.path.append(os.path.dirname(__file__))

from catalog_manager import CatalogManager
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, StringType, IntegerType

def test_schema_evolution():
    print("--- Testing Schema Evolution and Maintenance ---")
    
    # Load config (env vars or json)
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "env.json")
    import json
    
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
    
    namespace = "db"
    table_name = "schema_test"
    full_name = f"{namespace}.{table_name}"
    
    # Cleanup if exists
    try:
        manager.catalog.drop_table(full_name)
        print(f"Dropped existing table {full_name}")
    except:
        pass
        
    # Create Table
    print(f"\n1. Creating table {full_name}...")
    schema = Schema(
        NestedField(1, "id", IntegerType(), required=False),
        NestedField(2, "data", StringType(), required=False)
    )
    manager.catalog.create_table(full_name, schema=schema)
    print("Table created.")
    
    # Add Column
    print("\n2. Adding column 'age' (int)...")
    manager.add_column(namespace, table_name, "age", "int")
    table = manager.get_table(namespace, table_name)
    print(f"Schema: {table.schema()}")
    if "age" in [f.name for f in table.schema().fields]:
        print("PASS: Column 'age' added")
    else:
        print("FAIL: Column 'age' not found")
        
    # Rename Column
    print("\n3. Renaming 'data' to 'info'...")
    manager.rename_column(namespace, table_name, "data", "info")
    table = manager.get_table(namespace, table_name)
    print(f"Schema: {table.schema()}")
    if "info" in [f.name for f in table.schema().fields]:
        print("PASS: Column renamed to 'info'")
    else:
        print("FAIL: Column 'info' not found")

    # Update Column Type
    print("\n4. Updating 'age' to 'long'...")
    manager.update_column_type(namespace, table_name, "age", "long")
    table = manager.get_table(namespace, table_name)
    print(f"Schema: {table.schema()}")
    age_field = table.schema().find_field("age")
    if str(age_field.field_type) == "long":
        print("PASS: Column 'age' updated to long")
    else:
        print(f"FAIL: Column 'age' is {age_field.field_type}")

    # Drop Column
    print("\n5. Dropping column 'info'...")
    manager.drop_column(namespace, table_name, "info")
    table = manager.get_table(namespace, table_name)
    print(f"Schema: {table.schema()}")
    if "info" not in [f.name for f in table.schema().fields]:
        print("PASS: Column 'info' dropped")
    else:
        print("FAIL: Column 'info' still exists")
        
    # Expire Snapshots
    print("\n6. Expiring snapshots...")
    import pyiceberg
    print(f"PyIceberg version: {pyiceberg.__version__}")
    print(f"Table type: {type(table)}")
    print(f"Snapshot methods: {[m for m in dir(table) if 'snap' in m]}")
    
    try:
        if hasattr(table, 'expire_snapshots'):
            manager.expire_snapshots(namespace, table_name)
            print("PASS: Expire snapshots ran successfully")
        else:
            print("WARN: expire_snapshots method not found on Table object")
    except Exception as e:
        print(f"FAIL: Expire snapshots failed: {e}")

if __name__ == "__main__":
    test_schema_evolution()

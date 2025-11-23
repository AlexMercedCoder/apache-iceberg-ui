import sys
import os
from catalog_manager import CatalogManager
from query_engine import QueryEngine

# Ensure we can import modules
sys.path.append(os.path.dirname(__file__))

def debug():
    print("--- Starting Debug ---")
    cm = CatalogManager()
    
    # 1. Test Connection
    print("\n1. Testing Connection...")
    if not cm.catalog:
        print("Catalog not connected. Check env.json.")
        return
    print(f"Connected to catalog: {cm.catalog}")

    # 2. List Namespaces
    print("\n2. Listing Namespaces...")
    try:
        namespaces = cm.list_namespaces()
        print(f"Namespaces: {namespaces}")
    except Exception as e:
        print(f"Failed to list namespaces: {e}")
        return

    if not namespaces:
        print("No namespaces found. Cannot proceed with table tests.")
        return

    # Use the first namespace found, or 'db' if available
    ns = namespaces[0][0]
    for n in namespaces:
        if n[0] == 'db':
            ns = 'db'
            break
    
    print(f"Using namespace: {ns}")

    # 3. List Tables
    print(f"\n3. Listing Tables in {ns}...")
    try:
        tables = cm.list_tables(ns)
        print(f"Tables: {tables}")
    except Exception as e:
        print(f"Failed to list tables: {e}")
        return

    if not tables:
        print("No tables found. Attempting to create one via PyIceberg API...")
        try:
            from pyiceberg.schema import Schema
            from pyiceberg.types import NestedField, IntegerType, StringType
            
            schema = Schema(
                NestedField(1, "id", IntegerType(), required=True),
                NestedField(2, "data", StringType(), required=False),
            )
            cm.catalog.create_table(f"{ns}.test_table", schema=schema)
            print("Created 'test_table'.")
            tables = [(ns, 'test_table')]
        except Exception as e:
            print(f"Failed to create table: {e}")
            return

    table_name = tables[0][-1]
    print(f"Using table: {table_name}")

    # 4. Get Metadata
    print(f"\n4. Getting Metadata for {ns}.{table_name}...")
    try:
        metadata = cm.get_table_metadata(ns, table_name)
        print("Metadata retrieved successfully.")
        print(f"Schema: {metadata.get('schema')}")
    except Exception as e:
        print(f"Failed to get metadata: {e}")
        import traceback
        traceback.print_exc()

    # 5. Test Query Engine (Read)
    print(f"\n5. Testing Query Engine (Read) for {ns}.{table_name}...")
    qe = QueryEngine(cm)
    try:
        print("Attempting table.scan().to_arrow()...")
        table = cm.get_table(ns, table_name)
        arrow_table = table.scan().to_arrow()
        print(f"Scan successful. Rows: {arrow_table.num_rows}")
        
        qe._ensure_table_registered(ns, table_name)
        print("Table registered with DataFusion.")
        
        results = qe.execute_query(f"SELECT * FROM {table_name} LIMIT 5")
        print(f"Query Results: {results}")
    except Exception as e:
        print(f"Query/Scan failed: {e}")
        import traceback
        traceback.print_exc()

    # 6. Test Query Engine (Create via SQL - Mocking user attempt)
    print(f"\n6. Testing SQL Create Table...")
    try:
        # This is expected to fail with current implementation
        qe.execute_query(f"CREATE TABLE {ns}.sql_created (id INT) USING iceberg")
    except Exception as e:
        print(f"SQL Create failed as expected (or not?): {e}")

if __name__ == "__main__":
    debug()

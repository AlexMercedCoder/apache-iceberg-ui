import os
import sys
import shutil
import time

# Add current directory to sys.path
sys.path.append(os.path.dirname(__file__))

from catalog_manager import CatalogManager
from query_engine import QueryEngine

def test_caching_and_export():
    print("--- Testing Caching and Export ---")
    
    # Setup
    cache_dir = os.path.join(os.path.dirname(__file__), "cache")
    if os.path.exists(cache_dir):
        shutil.rmtree(cache_dir)
    
    # Load config
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "env.json")
    import json
    
    config = {}
    if os.path.exists(env_path):
        with open(env_path, "r") as f:
            data = json.load(f)
            config = data.get("catalog", {})
    else:
        # Fallback to env vars
        print("env.json not found, checking environment variables...")
        uri = os.environ.get("CATALOG_URI")
        warehouse = os.environ.get("CATALOG_WAREHOUSE")
        if uri:
            config = {
                "uri": uri,
                "warehouse": warehouse,
                "s3.endpoint": os.environ.get("S3_ENDPOINT"),
                "s3.access-key-id": os.environ.get("AWS_ACCESS_KEY_ID"),
                "s3.secret-access-key": os.environ.get("AWS_SECRET_ACCESS_KEY"),
                "s3.region": os.environ.get("AWS_REGION"),
                "type": "rest"
            }
            
    if not config:
        print("FAIL: No configuration found (env.json or env vars)")
        return

    manager = CatalogManager()
    manager.connect(config)
    
    engine = QueryEngine(manager)
    
    # 1. Run Query (Cache Miss)
    print("\n1. Running Query (Expect Cache MISS)...")
    sql = "SELECT * FROM db.customers LIMIT 5"
    start = time.time()
    result1 = engine.execute_query(sql)
    print(f"Result count: {len(result1)}")
    print(f"Time: {time.time() - start:.4f}s")
    
    # Check cache file
    if not os.path.exists(cache_dir):
        print("FAIL: Cache directory not created")
        return
    
    cache_files = os.listdir(cache_dir)
    if not cache_files:
        print("FAIL: No cache file created")
        return
    print(f"Cache file created: {cache_files[0]}")
    
    # 2. Run Query Again (Cache Hit)
    print("\n2. Running Query Again (Expect Cache HIT)...")
    start = time.time()
    result2 = engine.execute_query(sql)
    print(f"Result count: {len(result2)}")
    print(f"Time: {time.time() - start:.4f}s")
    
    # 3. Test Export (Return Arrow)
    print("\n3. Testing Export (Return Arrow)...")
    arrow_table = engine.execute_query(sql, return_arrow=True)
    print(f"Returned type: {type(arrow_table)}")
    if hasattr(arrow_table, 'to_pylist'):
        print("PASS: Returned object has to_pylist (likely Arrow Table)")
    else:
        print("FAIL: Returned object is not Arrow Table")

if __name__ == "__main__":
    test_caching_and_export()

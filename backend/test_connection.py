import json
import os
import sys

# Add current directory to sys.path to import catalog_manager
sys.path.append(os.path.dirname(__file__))

from catalog_manager import CatalogManager

def test_connection():
    # Load configuration from env.json
    # Assuming env.json is in the root directory (one level up from backend)
    env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "env.json")
    
    if not os.path.exists(env_path):
        print(f"Error: {env_path} not found.")
        return

    try:
        with open(env_path, "r") as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error reading env.json: {e}")
        return

    print("Testing connection with config:", json.dumps(config, indent=2, default=str))

    manager = CatalogManager()
    try:
        manager.connect(config)
        print("Successfully connected to catalog!")
        
        # List namespaces to verify
        namespaces = manager.list_namespaces()
        print(f"Namespaces: {namespaces}")
        
        if namespaces:
            first_ns = namespaces[0][0] if isinstance(namespaces[0], tuple) else namespaces[0]
            print(f"Listing tables in namespace '{first_ns}'...")
            tables = manager.list_tables(first_ns)
            print("Tables found:", tables)
        
    except Exception as e:
        print(f"Connection failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    test_connection()

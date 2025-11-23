import os
import json
from typing import List, Dict, Any, Optional
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

class CatalogManager:
    def __init__(self):
        self.catalog: Optional[Catalog] = None
        self.config: Dict[str, Any] = {}
        self._load_initial_config()

    def _load_initial_config(self):
        """Loads config from env.json if it exists."""
        env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "env.json")
        if os.path.exists(env_path):
            try:
                with open(env_path, "r") as f:
                    data = json.load(f)
                    if "catalog" in data:
                        self.connect(data["catalog"])
            except Exception as e:
                print(f"Failed to load env.json: {e}")

    def connect(self, properties: Dict[str, str]):
        """Connects to the Iceberg catalog with the given properties."""
        self.config = properties
        # Ensure we use the REST catalog implementation if not specified, 
        # though usually 'type': 'rest' is passed in properties or we default it.
        if "type" not in properties:
            properties["type"] = "rest"
        
        self.catalog = load_catalog("default", **properties)

    def list_namespaces(self) -> List[tuple]:
        if not self.catalog:
            raise ValueError("Catalog not connected")
        return self.catalog.list_namespaces()

    def create_namespace(self, namespace: str, properties: Dict[str, str] = None):
        if not self.catalog:
            raise ValueError("Catalog not connected")
        self.catalog.create_namespace(namespace, properties or {})

    def list_tables(self, namespace: str) -> List[tuple]:
        if not self.catalog:
            raise ValueError("Catalog not connected")
        try:
            return self.catalog.list_tables(namespace)
        except NoSuchNamespaceError:
            return []

    def get_table(self, namespace: str, table_name: str) -> Table:
        if not self.catalog:
            raise ValueError("Catalog not connected")
        return self.catalog.load_table(f"{namespace}.{table_name}")

    def get_table_metadata(self, namespace: str, table_name: str) -> Dict[str, Any]:
        table = self.get_table(namespace, table_name)
        return {
            "identifier": f"{namespace}.{table_name}",
            "schema": table.schema().model_dump(mode="json"),
            "partition_spec": [spec.model_dump() for spec in table.specs().values()],
            "properties": table.properties,
            "snapshots": [s.model_dump() for s in table.snapshots()],
            "current_snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None
        }

    def expire_snapshots(self, namespace: str, table_name: str, older_than_ms: Optional[int] = None):
        """
        Expires snapshots. 
        older_than_ms: Timestamp in milliseconds. If None, retains only the current snapshot (logic may vary).
        """
        table = self.get_table(namespace, table_name)
        expire = table.expire_snapshots()
        
        if older_than_ms:
            expire = expire.expire_older_than(older_than_ms)
        
        # If no timestamp provided, we might want to just run it to clean up unreferenced files 
        # or implement a default retention policy. For now, let's assume the user passes a timestamp 
        # or we rely on table properties if set.
        
        expire.commit()

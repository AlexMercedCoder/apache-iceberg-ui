import os
import json
from typing import List, Dict, Any, Optional
from pyiceberg.catalog import load_catalog, Catalog
from pyiceberg.table import Table
from pyiceberg.exceptions import NoSuchTableError, NoSuchNamespaceError

class CatalogManager:
    def __init__(self):
        self.catalogs: Dict[str, Catalog] = {}
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
                        # Default catalog name is "default"
                        self.connect("default", data["catalog"])
            except Exception as e:
                print(f"Failed to load env.json: {e}")

    def connect(self, name: str, properties: Dict[str, str]):
        """Connects to an Iceberg catalog and stores it with the given name."""
        # Ensure we use the REST catalog implementation if not specified
        if "type" not in properties:
            properties["type"] = "rest"
        
        try:
            catalog = load_catalog(name, **properties)
            self.catalogs[name] = catalog
            print(f"Connected to catalog: {name}")
        except Exception as e:
            print(f"Failed to connect to catalog {name}: {e}")
            raise e

    def disconnect(self, name: str):
        if name in self.catalogs:
            del self.catalogs[name]

    def get_catalog(self, name: str) -> Catalog:
        if name not in self.catalogs:
            # Fallback for backward compatibility or single-catalog view
            if len(self.catalogs) == 1:
                return list(self.catalogs.values())[0]
            raise ValueError(f"Catalog '{name}' not found. Available: {list(self.catalogs.keys())}")
        return self.catalogs[name]

    def list_catalogs(self) -> List[str]:
        return list(self.catalogs.keys())

    def list_namespaces(self, catalog_name: str) -> List[tuple]:
        catalog = self.get_catalog(catalog_name)
        return catalog.list_namespaces()

    def create_namespace(self, catalog_name: str, namespace: str, properties: Dict[str, str] = None):
        catalog = self.get_catalog(catalog_name)
        catalog.create_namespace(namespace, properties or {})

    def list_tables(self, catalog_name: str, namespace: str) -> List[tuple]:
        catalog = self.get_catalog(catalog_name)
        try:
            return catalog.list_tables(namespace)
        except NoSuchNamespaceError:
            return []

    def get_table(self, catalog_name: str, namespace: str, table_name: str) -> Table:
        catalog = self.get_catalog(catalog_name)
        return catalog.load_table(f"{namespace}.{table_name}")

    def get_table_metadata(self, catalog_name: str, namespace: str, table_name: str) -> Dict[str, Any]:
        table = self.get_table(catalog_name, namespace, table_name)
        return {
            "identifier": f"{catalog_name}.{namespace}.{table_name}",
            "schema": table.schema().model_dump(mode="json"),
            "partition_spec": [spec.model_dump() for spec in table.specs().values()],
            "properties": table.properties,
            "snapshots": [s.model_dump() for s in table.snapshots()],
            "current_snapshot_id": table.current_snapshot().snapshot_id if table.current_snapshot() else None
        }

    def expire_snapshots(self, catalog_name: str, namespace: str, table_name: str, older_than_ms: Optional[int] = None):
        table = self.get_table(catalog_name, namespace, table_name)
        
        # PyIceberg 0.10.0+ uses manage_snapshots()
        if hasattr(table, "manage_snapshots"):
            expire = table.manage_snapshots().expire_snapshots()
        else:
            expire = table.expire_snapshots()
        
        if older_than_ms:
            expire = expire.expire_older_than(older_than_ms)
        
        expire.commit()

    def add_column(self, catalog_name: str, namespace: str, table_name: str, col_name: str, col_type: str, required: bool = False):
        table = self.get_table(catalog_name, namespace, table_name)
        from pyiceberg.types import IntegerType, StringType, BooleanType, FloatType, DoubleType, LongType, DateType, TimestampType
        
        type_map = {
            "int": IntegerType(),
            "string": StringType(),
            "boolean": BooleanType(),
            "float": FloatType(),
            "double": DoubleType(),
            "long": LongType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }
        
        iceberg_type = type_map.get(col_type.lower())
        if not iceberg_type:
            raise ValueError(f"Unsupported type: {col_type}")
            
        with table.update_schema() as update:
            update.add_column(col_name, iceberg_type, required=required)

    def drop_column(self, catalog_name: str, namespace: str, table_name: str, col_name: str):
        table = self.get_table(catalog_name, namespace, table_name)
        with table.update_schema() as update:
            update.delete_column(col_name)

    def rename_column(self, catalog_name: str, namespace: str, table_name: str, col_name: str, new_name: str):
        table = self.get_table(catalog_name, namespace, table_name)
        with table.update_schema() as update:
            update.rename_column(col_name, new_name)

    def update_column_type(self, catalog_name: str, namespace: str, table_name: str, col_name: str, new_type: str):
        table = self.get_table(catalog_name, namespace, table_name)
        from pyiceberg.types import IntegerType, StringType, BooleanType, FloatType, DoubleType, LongType, DateType, TimestampType
        
        type_map = {
            "int": IntegerType(),
            "string": StringType(),
            "boolean": BooleanType(),
            "float": FloatType(),
            "double": DoubleType(),
            "long": LongType(),
            "date": DateType(),
            "timestamp": TimestampType()
        }
        
        iceberg_type = type_map.get(new_type.lower())
        if not iceberg_type:
            raise ValueError(f"Unsupported type: {new_type}")
            
        with table.update_schema() as update:
            update.update_column(col_name, field_type=iceberg_type)

    def get_table_stats(self, catalog_name: str, namespace: str, table_name: str) -> Dict[str, Any]:
        table = self.get_table(catalog_name, namespace, table_name)
        
        # Get files
        files = []
        try:
            # Inspect the files table
            files_table = table.inspect.files()
            # Convert to list of dicts
            # PyIceberg 0.6.0+ returns a PyArrow table for inspect tables
            if hasattr(files_table, "to_pylist"):
                files = files_table.to_pylist()
            else:
                # Fallback if it returns something else (older versions)
                files = []
        except Exception as e:
            print(f"Error fetching files stats: {e}")

        # Get partitions
        partitions = []
        try:
            partitions_table = table.inspect.partitions()
            if hasattr(partitions_table, "to_pylist"):
                partitions = partitions_table.to_pylist()
        except Exception as e:
            print(f"Error fetching partitions stats: {e}")

        return {
            "files": files,
            "partitions": partitions
        }

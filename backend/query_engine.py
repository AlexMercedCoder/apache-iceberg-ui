from datafusion import SessionContext
import pyarrow as pa
from catalog_manager import CatalogManager
import re
from pyiceberg.schema import Schema
from pyiceberg.types import NestedField, IntegerType, StringType, BooleanType, FloatType, DoubleType, LongType, DateType, TimestampType

class QueryEngine:
    def __init__(self, catalog_manager: CatalogManager):
        self.ctx = SessionContext()
        self.catalog_manager = catalog_manager
        self.registered_tables = set()

    def _ensure_table_registered(self, namespace: str, table_name: str):
        """
        Registers the Iceberg table with DataFusion as an Arrow table using qualified name.
        """
        full_name = f"{namespace}.{table_name}"
        if full_name in self.registered_tables:
            return  # Already registered
            
        try:
            table = self.catalog_manager.get_table(namespace, table_name)
            arrow_table = table.scan().to_arrow()
            # Register with qualified name to avoid namespace conflicts
            self.ctx.from_arrow_table(arrow_table, name=full_name)
            self.registered_tables.add(full_name)
        except Exception as e:
            # Log error but don't crash if one table fails to load
            print(f"Failed to register table {full_name}: {e}")

    def execute_query(self, sql: str):
        """
        Executes a SQL query. Handles CREATE TABLE manually.
        Auto-detects and registers namespace-qualified table references.
        """
        # Check for CREATE TABLE
        create_match = re.match(r"CREATE\s+TABLE\s+([\w\.]+)\s*\((.*)\)\s*(?:USING\s+iceberg)?", sql, re.IGNORECASE | re.DOTALL)
        if create_match:
            return self._handle_create_table(create_match)

        # Extract namespace-qualified table references (e.g., "db.customers" or "level1.level2.table")
        # Pattern: captures everything up to the last dot as namespace, and after last dot as table
        # Matches: namespace(.namespace)*.table
        table_pattern = r'(?:FROM|JOIN)\s+((?:[\w]+\.)+)([\w]+)\b'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)
        
        # Register tables and rewrite SQL to use unique aliases
        # We'll use format: namespace_table (replacing dots with underscores)
        rewritten_sql = sql
        for namespace_with_dot, table_name in matches:
            # Remove trailing dot from namespace
            namespace = namespace_with_dot.rstrip('.')
            
            # Create alias: replace all dots with underscores
            alias = f"{namespace.replace('.', '_')}_{table_name}"
            qualified_name = f"{namespace}.{table_name}"
            
            # Register table with alias if not already registered
            if alias not in self.registered_tables:
                try:
                    # For multi-level namespaces, we need to pass it to catalog correctly
                    # Iceberg expects namespace as a string "level1.level2" or tuple ('level1', 'level2')
                    # Most REST catalogs accept the dotted string format
                    table = self.catalog_manager.get_table(namespace, table_name)
                    arrow_table = table.scan().to_arrow()
                    self.ctx.from_arrow_table(arrow_table, name=alias)
                    self.registered_tables.add(alias)
                except Exception as e:
                    print(f"Failed to register table {qualified_name}: {e}")
            
            # Replace qualified name with alias in SQL
            # Use word boundaries to avoid partial replacements
            rewritten_sql = re.sub(
                rf'\b{re.escape(qualified_name)}\b',
                alias,
                rewritten_sql,
                flags=re.IGNORECASE
            )

        try:
            df = self.ctx.sql(rewritten_sql)
            return df.to_arrow_table().to_pylist()
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")

    def _handle_create_table(self, match):
        full_table_name = match.group(1)
        columns_def = match.group(2)
        
        # Parse namespace and table
        parts = full_table_name.split('.')
        if len(parts) == 2:
            namespace, table_name = parts
        else:
            raise ValueError("Table name must be in format 'namespace.table'")

        # Parse columns (very basic parser)
        # Expected format: id INT, name STRING, ...
        fields = []
        field_id = 1
        for col_def in columns_def.split(','):
            col_def = col_def.strip()
            if not col_def: continue
            
            col_parts = col_def.split()
            if len(col_parts) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            
            col_name = col_parts[0]
            col_type_str = col_parts[1].upper()
            
            col_type = StringType() # Default
            if 'INT' in col_type_str: col_type = IntegerType()
            elif 'LONG' in col_type_str: col_type = LongType()
            elif 'FLOAT' in col_type_str: col_type = FloatType()
            elif 'DOUBLE' in col_type_str: col_type = DoubleType()
            elif 'BOOL' in col_type_str: col_type = BooleanType()
            elif 'DATE' in col_type_str: col_type = DateType()
            elif 'TIMESTAMP' in col_type_str: col_type = TimestampType()
            
            fields.append(NestedField(field_id, col_name, col_type, required=False))
            field_id += 1

        schema = Schema(*fields)
        self.catalog_manager.catalog.create_table(f"{namespace}.{table_name}", schema=schema)
        return [{"status": "Table created successfully"}]

    def register_all_tables_in_namespace(self, namespace: str):
        tables = self.catalog_manager.list_tables(namespace)
        for table_ident in tables:
            t_name = table_ident[-1] 
            self._ensure_table_registered(namespace, t_name)

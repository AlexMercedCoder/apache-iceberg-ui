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

    def _ensure_table_registered(self, catalog_name: str, namespace: str, table_name: str):
        """
        Registers the Iceberg table with DataFusion as an Arrow table using qualified name.
        """
        full_name = f"{catalog_name}.{namespace}.{table_name}"
        if full_name in self.registered_tables:
            return  # Already registered
            
        try:
            table = self.catalog_manager.get_table(catalog_name, namespace, table_name)
            arrow_table = table.scan().to_arrow()
            # Register with qualified name to avoid namespace conflicts
            # DataFusion might struggle with dots in table names if not quoted, 
            # but we'll use the alias approach in execute_query to handle this.
            self.ctx.from_arrow_table(arrow_table, name=full_name)
            self.registered_tables.add(full_name)
        except Exception as e:
            # Log error but don't crash if one table fails to load
            print(f"Failed to register table {full_name}: {e}")

    def execute_query(self, sql: str, return_arrow: bool = False, default_catalog: str = "default"):
        """
        Executes a SQL query. Handles CREATE TABLE manually.
        Auto-detects and registers qualified table references.
        Supports time travel: SELECT * FROM table FOR SYSTEM_TIME AS OF SNAPSHOT <id>
        """
        # Check for time travel syntax: FOR SYSTEM_TIME AS OF SNAPSHOT <id>
        # or FOR SYSTEM_TIME AS OF TIMESTAMP <ms>
        snapshot_id = None
        as_of_timestamp = None
        
        time_travel_snapshot = re.search(r'FOR\s+SYSTEM_TIME\s+AS\s+OF\s+SNAPSHOT\s+(\d+)', sql, re.IGNORECASE)
        if time_travel_snapshot:
            snapshot_id = int(time_travel_snapshot.group(1))
            # Remove time travel clause from SQL for DataFusion
            sql = re.sub(r'\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+SNAPSHOT\s+\d+', '', sql, flags=re.IGNORECASE)
        
        time_travel_timestamp = re.search(r'FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+(\d+)', sql, re.IGNORECASE)
        if time_travel_timestamp:
            as_of_timestamp = int(time_travel_timestamp.group(1))
            # Remove time travel clause from SQL for DataFusion
            sql = re.sub(r'\s+FOR\s+SYSTEM_TIME\s+AS\s+OF\s+TIMESTAMP\s+\d+', '', sql, flags=re.IGNORECASE)
        
        # Check for CREATE TABLE
        create_match = re.match(r"CREATE\s+TABLE\s+([\w\.]+)\s*\((.*)\)\s*(?:USING\s+iceberg)?", sql, re.IGNORECASE | re.DOTALL)
        if create_match:
            return self._handle_create_table(create_match, default_catalog)

        # Check for INSERT INTO
        insert_match = re.match(r"INSERT\s+INTO\s+([\w\.]+)\s+(.*)", sql, re.IGNORECASE | re.DOTALL)
        if insert_match:
            return self._handle_insert(insert_match, default_catalog)

        # Check for DELETE FROM
        delete_match = re.match(r"DELETE\s+FROM\s+([\w\.]+)\s+WHERE\s+(.*)", sql, re.IGNORECASE | re.DOTALL)
        if delete_match:
            return self._handle_delete(delete_match, default_catalog)

        # Extract table references
        # Pattern now needs to handle 3 parts: catalog.namespace.table OR namespace.table
        # We'll be permissive: capture dot-separated identifiers
        table_pattern = r'(?:FROM|JOIN)\s+([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)+)(?:\$([a-zA-Z0-9_]+))?\b'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)
        
        # Extract WHERE clause and SELECT columns for optimization
        from sql_utils import extract_where_clause, extract_select_columns, sql_where_to_iceberg_filter
        where_clause = extract_where_clause(sql)
        select_columns_raw = extract_select_columns(sql)
        
        # Only apply column selection if there's a single table (not a join)
        # For joins, we need to scan all columns and let DataFusion handle the selection
        select_columns = select_columns_raw if len(matches) == 1 else None
        
        # Convert WHERE to PyIceberg filter
        row_filter = sql_where_to_iceberg_filter(where_clause) if where_clause else None
        
        # Register tables and rewrite SQL to use unique aliases
        rewritten_sql = sql
        involved_tables_snapshots = [] # List of (catalog, namespace, table, snapshot_id) for cache key

        for full_identifier, metadata_suffix in matches:
            parts = full_identifier.split('.')
            
            # Determine catalog, namespace, table
            if len(parts) >= 3:
                # catalog.namespace.table
                catalog_name = parts[0]
                namespace = ".".join(parts[1:-1])
                table_name = parts[-1]
            else:
                # namespace.table (use default catalog)
                catalog_name = default_catalog
                namespace = ".".join(parts[:-1])
                table_name = parts[-1]
            
            # Create unique alias
            alias = f"{catalog_name}_{namespace.replace('.', '_')}_{table_name}"
            if metadata_suffix:
                alias += f"_{metadata_suffix}"
            
            # Skip if already registered
            if alias in self.registered_tables:
                continue
            
            qualified_name = f"{catalog_name}.{namespace}.{table_name}"
            
            try:
                if metadata_suffix:
                    # Handle metadata tables (files, snapshots, etc.)
                    # Note: Metadata tables are not cached via DataFusion in this simple engine
                    self._handle_metadata_table(catalog_name, namespace, table_name, metadata_suffix, alias)
                    involved_tables_snapshots.append(f"{qualified_name}${metadata_suffix}:metadata")
                else:
                    # Regular table
                    table = self.catalog_manager.get_table(catalog_name, namespace, table_name)
                    current_snapshot = table.current_snapshot()
                    snapshot_id = current_snapshot.snapshot_id if current_snapshot else "no_snapshot"
                    involved_tables_snapshots.append(f"{qualified_name}:{snapshot_id}")
                    
                    # Check for time travel
                    as_of_timestamp = None
                    as_of_snapshot = None
                    if 'FOR SYSTEM_TIME AS OF' in sql.upper():
                        # Extract timestamp or snapshot ID
                        # Simplified: look for TIMESTAMP 'value' or SNAPSHOT snapshot_id
                        timestamp_match = re.search(r"TIMESTAMP\s+'([^']+)'", sql, re.IGNORECASE)
                        snapshot_match = re.search(r"SNAPSHOT\s+(\d+)", sql, re.IGNORECASE)
                        if timestamp_match:
                            as_of_timestamp = timestamp_match.group(1)
                        elif snapshot_match:
                            as_of_snapshot = int(snapshot_match.group(1))
                    
                    # Scan table with optimizations
                    if as_of_snapshot:
                        print(f"Time travel to snapshot: {as_of_snapshot}")
                        scan = table.scan(snapshot_id=as_of_snapshot)
                        involved_tables_snapshots.append(f"{qualified_name}:tt_{as_of_snapshot}")
                    elif as_of_timestamp:
                        print(f"Time travel to timestamp: {as_of_timestamp}")
                        scan = table.scan(as_of_timestamp=as_of_timestamp)
                        involved_tables_snapshots.append(f"{qualified_name}:tt_{as_of_timestamp}")
                    else:
                        scan = table.scan()
                    
                    if row_filter:
                        print(f"Applying filter: {row_filter}")
                        scan = scan.filter(row_filter)
                    if select_columns:
                        print(f"Selecting columns: {select_columns}")
                        scan = scan.select(*select_columns)
                    
                    arrow_table = scan.to_arrow()
                    self.ctx.from_arrow_table(arrow_table, name=alias)
                    self.registered_tables.add(alias)
            except Exception as e:
                print(f"Failed to register/inspect table {qualified_name}: {e}")
                # If we can't find it in the specified catalog, maybe it's a local view or CTE?
                # Continue and let DataFusion fail if it's missing
                continue
            
            # Replace qualified name with alias in SQL
            # We need to be careful to replace the exact string matched
            # If the user wrote "nessie.db.table", we replace that.
            # If they wrote "db.table", we replace that.
            target_string = full_identifier
            if metadata_suffix and '$' not in full_identifier:
                 target_string += f"${metadata_suffix}"
            
            print(f"DEBUG: Replacing '{target_string}' with '{alias}'")
            print(f"DEBUG: SQL before: {rewritten_sql}")
                 
            rewritten_sql = re.sub(
                rf'\b{re.escape(target_string)}\b',
                alias,
                rewritten_sql,
                flags=re.IGNORECASE
            )
            print(f"DEBUG: SQL after: {rewritten_sql}")


        # Caching Logic
        import hashlib
        import os
        import pyarrow.feather as feather
        
        # Create cache key
        # Key = hash(rewritten_sql + sorted(involved_tables_snapshots))
        # This ensures that if SQL changes OR any table's snapshot changes, key changes.
        cache_key_str = rewritten_sql + "|" + ",".join(sorted(involved_tables_snapshots))
        cache_hash = hashlib.md5(cache_key_str.encode('utf-8')).hexdigest()
        cache_dir = os.path.join(os.path.dirname(__file__), "cache")
        cache_file = os.path.join(cache_dir, f"{cache_hash}.feather")
        
        # Check cache
        if os.path.exists(cache_file):
            print(f"Cache HIT: {cache_file}")
            try:
                cached_table = feather.read_table(cache_file)
                if return_arrow:
                    return cached_table
                return cached_table.to_pylist()
            except Exception as e:
                print(f"Cache read failed, re-executing: {e}")

        print(f"Cache MISS: Executing query...")
        try:
            df = self.ctx.sql(rewritten_sql)
            result_arrow = df.to_arrow_table()
            
            # Write to cache
            try:
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                feather.write_feather(result_arrow, cache_file)
                print(f"Cache saved: {cache_file}")
            except Exception as e:
                print(f"Failed to write cache: {e}")
            
            if return_arrow:
                return result_arrow
            return result_arrow.to_pylist()
        except Exception as e:
            raise RuntimeError(f"Query execution failed: {e}")

    def _handle_insert(self, match, default_catalog: str):
        full_table_name = match.group(1)
        rest_of_query = match.group(2).strip()
        
        # Parse target table
        parts = full_table_name.split('.')
        if len(parts) == 3:
            catalog_name, namespace, table_name = parts
        elif len(parts) == 2:
            catalog_name = default_catalog
            namespace, table_name = parts
        else:
            raise ValueError("Target table must be in format 'catalog.namespace.table' or 'namespace.table'")
            
        target_table = self.catalog_manager.get_table(catalog_name, namespace, table_name)
        
        # Check if it's VALUES or SELECT
        if rest_of_query.upper().startswith("VALUES"):
            # VALUES (...), (...)
            # This is hard to parse robustly without a real parser, but let's try simple cases
            # We'll use DataFusion to parse/execute "SELECT * FROM (VALUES ...)"
            # But DataFusion needs column names. 
            # Strategy: Create a temporary view or just run "SELECT * FROM (VALUES ...)" and cast to target schema?
            # Simpler: Let DataFusion handle the VALUES part as a query if we wrap it?
            # "INSERT INTO t VALUES (1, 'a')" -> "SELECT 1, 'a'"
            
            # Actually, DataFusion supports "VALUES (1, 'a'), (2, 'b')" as a valid query if columns are aliased?
            # Let's try to construct a SELECT query from it.
            
            # Hack: "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) as t(col1, col2)"
            # We need to know column names/types of target table to cast correctly.
            schema = target_table.schema()
            col_names = [f.name for f in schema.fields]
            
            # Construct a query that DataFusion can execute to get Arrow data
            # "SELECT * FROM (VALUES (1, 'a'), (2, 'b')) as t(id, data)"
            select_sql = f"SELECT * FROM ({rest_of_query}) as t({', '.join(col_names)})"
            
            print(f"Executing INSERT source: {select_sql}")
            arrow_table = self.execute_query(select_sql, return_arrow=True, default_catalog=default_catalog)
            
        elif rest_of_query.upper().startswith("SELECT"):
            # INSERT INTO t SELECT ...
            print(f"Executing INSERT source: {rest_of_query}")
            arrow_table = self.execute_query(rest_of_query, return_arrow=True, default_catalog=default_catalog)
            
        else:
            raise ValueError("INSERT must be followed by VALUES or SELECT")
            
        # Append to Iceberg table
        # Ensure schema matches (PyIceberg checks this, but we might need to cast if DataFusion inferred types differently)
        # For now, assume PyIceberg handles safe casting or errors
        target_table.append(arrow_table)
        
        return [{"status": f"Inserted {arrow_table.num_rows} rows"}]

    def _handle_delete(self, match, default_catalog: str):
        full_table_name = match.group(1)
        where_clause = match.group(2).strip()
        
        # Parse target table
        parts = full_table_name.split('.')
        if len(parts) == 3:
            catalog_name, namespace, table_name = parts
        elif len(parts) == 2:
            catalog_name = default_catalog
            namespace, table_name = parts
        else:
            raise ValueError("Target table must be in format 'catalog.namespace.table' or 'namespace.table'")
            
        target_table = self.catalog_manager.get_table(catalog_name, namespace, table_name)
        
        # Convert WHERE to PyIceberg filter
        from sql_utils import sql_where_to_iceberg_filter
        delete_filter = sql_where_to_iceberg_filter(where_clause)
        
        if not delete_filter:
            raise ValueError("Could not parse WHERE clause for DELETE")
            
        print(f"Executing DELETE on {full_table_name} with filter: {delete_filter}")
        target_table.delete(delete_filter)
        
        return [{"status": "Delete operation committed"}]

    def _handle_create_table(self, match, default_catalog: str):
        full_table_name = match.group(1)
        columns_def = match.group(2)
        
        # Parse catalog, namespace and table
        parts = full_table_name.split('.')
        if len(parts) == 3:
            catalog_name, namespace, table_name = parts
        elif len(parts) == 2:
            catalog_name = default_catalog
            namespace, table_name = parts
        else:
            raise ValueError("Table name must be in format 'catalog.namespace.table' or 'namespace.table'")

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
        catalog = self.catalog_manager.get_catalog(catalog_name)
        catalog.create_table(f"{namespace}.{table_name}", schema=schema)
        return [{"status": "Table created successfully"}]

    def _handle_metadata_table(self, catalog_name: str, namespace: str, table_name: str, metadata_type: str):
        """
        Handle metadata table queries (table$snapshots, table$files, etc.)
        """
        table = self.catalog_manager.get_table(catalog_name, namespace, table_name)
        
        if metadata_type == 'snapshots':
            # Return snapshot information
            snapshots = table.snapshots()
            return [{
                'snapshot_id': s.snapshot_id,
                'timestamp_ms': s.timestamp_ms,
                'operation': s.summary.operation if s.summary else None,
                'manifest_list': s.manifest_list
            } for s in snapshots]
        
        elif metadata_type == 'files':
            # Return data files
            files_info = []
            for snapshot in table.snapshots():
                scan = table.scan(snapshot_id=snapshot.snapshot_id)
                # Note: This is a simplified version
                # Full implementation would inspect manifests
                files_info.append({
                    'snapshot_id': snapshot.snapshot_id,
                    'file_count': 'N/A',  # Would need to count from manifests
                    'note': 'Use table.inspect.files() for full details'
                })
            return files_info
        
        elif metadata_type == 'partitions':
            # Return partition information
            return [{"note": "Partition metadata not yet implemented"}]
        
        elif metadata_type == 'stats':
            # Return table statistics
            metadata = table.metadata
            current_snapshot = table.current_snapshot()
            return [{
                'table': f"{catalog_name}.{namespace}.{table_name}",
                'total_snapshots': len(table.snapshots()),
                'current_snapshot_id': current_snapshot.snapshot_id if current_snapshot else None,
                'total_records': current_snapshot.summary.get('total-records') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_data_files': current_snapshot.summary.get('total-data-files') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_delete_files': current_snapshot.summary.get('total-delete-files') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_size_bytes': current_snapshot.summary.get('total-files-size') if current_snapshot and current_snapshot.summary else 'N/A'
            }]
        
        else:
            raise ValueError(f"Unknown metadata type: {metadata_type}")

    def register_all_tables_in_namespace(self, catalog_name: str, namespace: str):
        tables = self.catalog_manager.list_tables(catalog_name, namespace)
        for table_ident in tables:
            t_name = table_ident[-1] 
            self._ensure_table_registered(catalog_name, namespace, t_name)


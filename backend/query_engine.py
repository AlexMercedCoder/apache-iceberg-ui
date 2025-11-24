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

    def execute_query(self, sql: str, return_arrow: bool = False):
        """
        Executes a SQL query. Handles CREATE TABLE manually.
        Auto-detects and registers namespace-qualified table references.
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
            return self._handle_create_table(create_match)

        # Extract namespace-qualified table references (e.g., "db.customers" or "level1.level2.table" or "db.customers$snapshots")
        # Pattern: captures everything up to the last dot as namespace, and after last dot as table (with optional $metadata)
        # Matches: namespace(.namespace)*.table($metadata)?
        table_pattern = r'(?:FROM|JOIN)\s+((?:[\w]+\.)+)([\w$]+)\b'
        matches = re.findall(table_pattern, sql, re.IGNORECASE)
        
        # Extract WHERE clause and SELECT columns for optimization
        from sql_utils import extract_where_clause, extract_select_columns, sql_where_to_iceberg_filter
        where_clause = extract_where_clause(sql)
        select_columns = extract_select_columns(sql)
        
        # Convert WHERE to PyIceberg filter
        row_filter = sql_where_to_iceberg_filter(where_clause) if where_clause else None
        
        # Register tables and rewrite SQL to use unique aliases
        # We'll use format: namespace_table (replacing dots with underscores)
        rewritten_sql = sql
        involved_tables_snapshots = [] # List of (namespace, table, snapshot_id) for cache key

        for namespace_with_dot, table_name in matches:
            # Remove trailing dot from namespace
            namespace = namespace_with_dot.rstrip('.')
            
            # Create alias: replace all dots with underscores
            alias = f"{namespace.replace('.', '_')}_{table_name}"
            qualified_name = f"{namespace}.{table_name}"
            
            # Check for metadata tables (table$suffix)
            if '$' in table_name:
                # Handle metadata tables separately
                result = self._handle_metadata_table(namespace, table_name.replace('$', '.'))
                return result
            
            # Register table with alias if not already registered
            # We always need to check the table to get the snapshot ID for caching
            try:
                table = self.catalog_manager.get_table(namespace, table_name)
                current_snapshot = table.current_snapshot()
                snap_id = current_snapshot.snapshot_id if current_snapshot else 0
                involved_tables_snapshots.append(f"{qualified_name}:{snap_id}")

                if alias not in self.registered_tables:
                    # Apply predicate pushdown and column projection
                    # Start with base scan, optionally with time travel
                    if snapshot_id:
                        print(f"Time travel to snapshot: {snapshot_id}")
                        scan = table.scan(snapshot_id=snapshot_id)
                        # For time travel, use the requested snapshot ID for cache key
                        involved_tables_snapshots.append(f"{qualified_name}:tt_{snapshot_id}")
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
                import traceback
                traceback.print_exc()
            
            # Replace qualified name with alias in SQL
            # Use word boundaries to avoid partial replacements
            rewritten_sql = re.sub(
                rf'\b{re.escape(qualified_name)}\b',
                alias,
                rewritten_sql,
                flags=re.IGNORECASE
            )

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

    def _handle_metadata_table(self, namespace: str, table_spec: str):
        """
        Handle metadata table queries (table$snapshots, table$files, etc.)
        table_spec is like "customers.snapshots"
        """
        parts = table_spec.split('.')
        if len(parts) != 2:
            raise ValueError(f"Invalid metadata table spec: {table_spec}")
        
        table_name, metadata_type = parts
        table = self.catalog_manager.get_table(namespace, table_name)
        
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
                'table': f"{namespace}.{table_name}",
                'total_snapshots': len(table.snapshots()),
                'current_snapshot_id': current_snapshot.snapshot_id if current_snapshot else None,
                'total_records': current_snapshot.summary.get('total-records') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_data_files': current_snapshot.summary.get('total-data-files') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_delete_files': current_snapshot.summary.get('total-delete-files') if current_snapshot and current_snapshot.summary else 'N/A',
                'total_size_bytes': current_snapshot.summary.get('total-files-size') if current_snapshot and current_snapshot.summary else 'N/A'
            }]
        
        else:
            raise ValueError(f"Unknown metadata type: {metadata_type}")

    def register_all_tables_in_namespace(self, namespace: str):
        tables = self.catalog_manager.list_tables(namespace)
        for table_ident in tables:
            t_name = table_ident[-1] 
            self._ensure_table_registered(namespace, t_name)


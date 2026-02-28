"""
Flask-compatible database manager with connection pooling
"""
import sqlite3
import mariadb
from mariadb import Error as MariaDBError
from typing import Union, Dict, Any, List
import os
from contextlib import contextmanager
import pandas as pd
from flask import g
import json


class DatabaseManagerFlask:
    """Handles database connections and operations for Flask application"""
    _connections = {}
    _connection_types = {}
    _mariadb_pool = None
    _socketio = None

    @classmethod
    def set_socketio(cls, socketio):
        """Set the SocketIO instance for real-time updates."""
        cls._socketio = socketio
    
    @classmethod
    def get_connection(cls, db_path: Union[str, Dict[str, Any]], connection_type: str = 'sqlite'):
        """
        Get or create a database connection.
        
        Args:
            db_path: For SQLite: path to database file. 
                    For MySQL/MariaDB: dict with connection parameters
            connection_type: 'sqlite' or 'mysql'
        """
        if connection_type == 'sqlite':
            if not isinstance(db_path, str) or not db_path:
                raise ValueError("Invalid SQLite database path")
            
            db_path = os.path.abspath(db_path)
            
            if not os.path.exists(db_path):
                # Create the database file if it doesn't exist
                open(db_path, 'a').close()
            
            # Check if connection exists and is valid
            needs_new_connection = False
            if db_path in cls._connections:
                try:
                    # Test if connection is still valid
                    cursor = cls._connections[db_path].cursor()
                    cursor.execute("SELECT 1")
                except (sqlite3.ProgrammingError, sqlite3.OperationalError):
                    # Connection is closed or invalid, need new one
                    needs_new_connection = True
                    try:
                        cls._connections[db_path].close()
                    except:
                        pass
                    del cls._connections[db_path]
            else:
                needs_new_connection = True
            
            if needs_new_connection:
                try:
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute("PRAGMA foreign_keys = ON")
                    cursor.execute("PRAGMA journal_mode=WAL")
                    cls._connections[db_path] = conn
                    cls._connection_types[db_path] = 'sqlite'
                    cls.initialize_recycle_bin(conn, 'sqlite')
                except sqlite3.Error as e:
                    raise ConnectionError(f"Failed to connect to SQLite database: {e}")
            
            return cls._connections[db_path]
        
        elif connection_type == 'mysql':
            if not isinstance(db_path, dict):
                raise ValueError("For MySQL/MariaDB, db_path must be a dictionary")
            
            # Use g to store connection for the current request context
            try:
                if 'db_conn' in g and cls._connection_types.get(f"mysql_{id(g.db_conn)}") == 'mysql':
                    return g.db_conn
            except (RuntimeError, AttributeError):
                # Outside request context
                pass

            # Validate required parameters
            required = ['host', 'user', 'password', 'database']
            missing = [param for param in required if param not in db_path]
            if missing:
                raise ValueError(f"Missing required MySQL connection parameters: {', '.join(missing)}")
            
            # Create connection pool if it doesn't exist
            if not cls._mariadb_pool:
                try:
                    pool_name = "HaoXai_pool"
                    # Try to create pool, but be prepared if it already exists globally
                    try:
                        cls._mariadb_pool = mariadb.ConnectionPool(
                            pool_name=pool_name,
                            host=db_path['host'],
                            user=db_path['user'],
                            password=db_path['password'],
                            database=db_path['database'],
                            port=int(db_path.get('port', 3306)),
                            pool_size=32,  # Increased for stability
                            connect_timeout=10
                        )
                        print(f"[DEBUG] Created MariaDB connection pool: {pool_name}")
                    except mariadb.ProgrammingError as pe:
                        if "already exists" in str(pe):
                            # Try to get existing pool (some drivers allow this, or we just try to get a connection)
                            # MariaDB python driver doesn't have a direct get_pool, but we can try mariadb.connect
                            # with the same pool_name to "reattach" if needed, OR we trust the first creation.
                            # Since we can't easily reattach, we'll try to use the name globally
                            print(f"[DEBUG] MariaDB pool '{pool_name}' already exists globally.")
                            # Fallback: create a dummy pool object that uses mariadb.connect(pool_name=...)
                            # or just try to get one below.
                        else:
                            raise
                except MariaDBError as e:
                    raise ConnectionError(f"Failed to create MariaDB connection pool: {e}")
            
            try:
                # Use pool to get a connection
                if cls._mariadb_pool:
                    conn = cls._mariadb_pool.get_connection()
                else:
                    # Fallback if pool exists but our reference is lost (unlikely but possible during reloads)
                    conn = mariadb.connect(pool_name="HaoXai_pool")
                
                # Store connection type info
                conn_key = f"mysql_{id(conn)}"
                cls._connection_types[conn_key] = 'mysql'
                
                # Store in g for the current request
                try:
                    g.db_conn = conn
                except (RuntimeError, AttributeError):
                    pass
                
                # Check for RecycleBin on new connections if not already checked for this session
                cls.initialize_recycle_bin(conn, 'mysql')
                
                return conn
            except MariaDBError as e:
                print(f"[ERROR] MariaDB pool error: {e}")
                raise ConnectionError(f"No MariaDB connection available: {e}")
        
        else:
            raise ValueError(f"Unsupported database type: {connection_type}")
    
    @classmethod
    def initialize_recycle_bin(cls, connection, db_type: str):
        """Initialize the RecycleBin table if it doesn't exist."""
        cursor = connection.cursor()
        
        try:
            if db_type == 'sqlite':
                cursor.execute('''CREATE TABLE IF NOT EXISTS RecycleBin (
                    id INTEGER PRIMARY KEY,
                    original_table TEXT NOT NULL,
                    data TEXT NOT NULL,
                    table_schema TEXT,  -- Stores CREATE TABLE SQL
                    deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )''')
                
                # Migration: Add table_schema column if it doesn't exist
                cursor.execute("PRAGMA table_info(RecycleBin)")
                cols = [col[1] for col in cursor.fetchall()]
                if 'table_schema' not in cols:
                    print("[INFO] Migrating RecycleBin: Adding table_schema column (SQLite)")
                    cursor.execute("ALTER TABLE RecycleBin ADD COLUMN table_schema TEXT")
            else:  # MySQL/MariaDB
                cursor.execute('''CREATE TABLE IF NOT EXISTS RecycleBin (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    original_table VARCHAR(255) NOT NULL,
                    data TEXT NOT NULL,
                    table_schema TEXT,
                    deleted_at DATETIME DEFAULT CURRENT_TIMESTAMP
                )''')
                
                # Migration: Add table_schema column if it doesn't exist
                cursor.execute("SHOW COLUMNS FROM RecycleBin LIKE 'table_schema'")
                if not cursor.fetchone():
                    print("[INFO] Migrating RecycleBin: Adding table_schema column (MySQL)")
                    cursor.execute("ALTER TABLE RecycleBin ADD COLUMN table_schema TEXT")
            
            connection.commit()
        except Exception as e:
            print(f"[ERROR] Failed to initialize RecycleBin: {e}")
            connection.rollback()
    
    @classmethod
    def get_tables(cls, connection, db_type: str) -> List[str]:
        """Get list of tables in database"""
        cursor = connection.cursor()
        
        if db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        else:  # MySQL
            cursor.execute("SHOW TABLES")
            
        tables = []
        for row in cursor.fetchall():
            if not row:
                continue
            table_name = row[0]
            if isinstance(table_name, bytes):
                table_name = table_name.decode('utf-8')
            # Ensure table_name is a string before adding to list
            tables.append(str(table_name))
        return tables

    @classmethod
    def is_link_table(cls, table_name) -> bool:
        """Helper to determine if a table is a link table"""
        if table_name is None:
            return False
            
        t_lower = str(table_name).lower()
        # Same criteria as used in routes
        return t_lower.endswith('_link') or '_link_' in t_lower or t_lower.startswith('link_') or t_lower == 'links' or 'link' in t_lower
    
    @classmethod
    def get_schema(cls, connection, db_type: str) -> Dict[str, List[Dict]]:
        """Get database schema"""
        cursor = connection.cursor()
        schema = {}
        
        tables = cls.get_tables(connection, db_type)
        
        for table in tables:
            if db_type == 'sqlite':
                cursor.execute(f'PRAGMA table_info("{table}")')
                columns = cursor.fetchall()
                schema[table] = [
                    {
                        'name': col[1],
                        'type': col[2],
                        'notnull': bool(col[3]),
                        'default': col[4],
                        'pk': bool(col[5])
                    }
                    for col in columns
                ]
            else:  # MySQL
                cursor.execute(f'DESCRIBE `{table}`')
                columns = cursor.fetchall()
                schema[table] = [
                    {
                        'name': col[0],
                        'type': col[1],
                        'null': col[2] == 'YES',
                        'key': col[3],
                        'default': col[4]
                    }
                    for col in columns
                ]
        
        return schema
    
    @classmethod
    def import_dataframe(cls, connection, df: pd.DataFrame, table_name: str, db_type: str) -> Dict[str, int]:
        """
        Import pandas DataFrame to database
        
        Returns:
            Dictionary with import statistics
        """
        cursor = connection.cursor()
        stats = {
            'existing_records': 0,
            'updated_records': 0,
            'new_records': 0
        }
        
        try:
            # Check if table exists
            if db_type == 'sqlite':
                cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
                table_exists = cursor.fetchone() is not None
            else:
                cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
                table_exists = cursor.fetchone() is not None
            
            if table_exists:
                # Table exists - append data
                if db_type == 'mysql':
                    # For MySQL, use parameterized INSERT
                    for _, row in df.iterrows():
                        placeholders = ', '.join(['%s'] * len(row))
                        insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                        values = tuple(None if pd.isna(val) else val for val in row.values)
                        cursor.execute(insert_sql, values)
                        stats['new_records'] += 1
                else:
                    # For SQLite, use pandas to_sql
                    df.to_sql(table_name, connection, if_exists='append', index=False)
                    stats['new_records'] = len(df)
            else:
                # Create new table
                if db_type == 'mysql':
                    # Build CREATE TABLE statement
                    columns_def = []
                    for col in df.columns:
                        if df[col].dtype == 'object':
                            col_type = 'TEXT'
                        elif df[col].dtype in ['int64', 'int32']:
                            col_type = 'INT'
                        elif df[col].dtype in ['float64', 'float32']:
                            col_type = 'DOUBLE'
                        else:
                            col_type = 'TEXT'
                        columns_def.append(f"`{col}` {col_type}")
                    
                    create_sql = f"CREATE TABLE `{table_name}` ({', '.join(columns_def)})"
                    cursor.execute(create_sql)
                    
                    # Insert data
                    for _, row in df.iterrows():
                        placeholders = ', '.join(['%s'] * len(row))
                        insert_sql = f"INSERT INTO `{table_name}` VALUES ({placeholders})"
                        values = tuple(None if pd.isna(val) else val for val in row.values)
                        cursor.execute(insert_sql, values)
                        stats['new_records'] += 1
                else:
                    # For SQLite, use pandas to_sql with explicit connection handling
                    try:
                        print(f"DEBUG: Creating table '{table_name}' with {len(df)} rows")
                        print(f"DEBUG: DataFrame columns: {list(df.columns)}")
                        
                        # Ensure connection is committed and closed properly
                        df.to_sql(table_name, connection, if_exists='fail', index=False)
                        connection.commit()  # Explicit commit
                        stats['new_records'] = len(df)
                        
                        # Force table visibility by querying sqlite_master
                        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                        table_check = cursor.fetchone()
                        if table_check:
                            print(f"DEBUG: Table '{table_name}' created successfully and is visible")
                            
                            # Verify table has data
                            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
                            row_count = cursor.fetchone()[0]
                            print(f"DEBUG: Table '{table_name}' contains {row_count} rows")
                        else:
                            print(f"WARNING: Table '{table_name}' not found after creation")
                            
                    except Exception as sql_error:
                        print(f"SQLite table creation error: {sql_error}")
                        raise sql_error
            
            connection.commit()
            
            # Emit real-time updates for different operations
            if cls._socketio:
                if stats['new_records'] > 0:
                    cls._socketio.emit('data_inserted', {
                        'table': table_name,
                        'count': stats['new_records'],
                        'action': 'insert',
                        'timestamp': pd.Timestamp.now().isoformat()
                    })
                
                if stats['updated_records'] > 0:
                    cls._socketio.emit('data_updated', {
                        'table': table_name,
                        'count': stats['updated_records'],
                        'action': 'update',
                        'timestamp': pd.Timestamp.now().isoformat()
                    })
                
                # General database update event
                cls._socketio.emit('database_updated', {
                    'table': table_name,
                    'stats': stats,
                    'action': 'import',
                    'timestamp': pd.Timestamp.now().isoformat()
                })
                
                # Update statistics
                cls._socketio.emit('stats_updated', {
                    'tables_affected': [table_name],
                    'total_changes': stats['new_records'] + stats['updated_records'],
                    'timestamp': pd.Timestamp.now().isoformat()
                })
                
            return stats
            
        except Exception as e:
            connection.rollback()
            raise Exception(f"Import failed: {e}")
    
    @classmethod
    def emit_realtime_event(cls, event_type: str, table_name: str, data: Dict[str, Any] = None):
        """Emit real-time Socket.IO events for database operations"""
        if cls._socketio:
            event_data = {
                'table': table_name,
                'action': event_type,
                'timestamp': pd.Timestamp.now().isoformat()
            }
            
            if data:
                event_data.update(data)
            
            # Emit specific event type
            cls._socketio.emit(f'data_{event_type}', event_data)
            
            # Emit general database update event
            cls._socketio.emit('database_updated', event_data)
            
            # Emit stats update
            cls._socketio.emit('stats_updated', {
                'tables_affected': [table_name],
                'action': event_type,
                'timestamp': pd.Timestamp.now().isoformat()
            })
    
    @classmethod
    def delete_records(cls, connection, table_name: str, where_clause: str = None, db_type: str = 'sqlite') -> Dict[str, int]:
        """Delete records from table and move to RecycleBin, then emit real-time events"""
        cursor = connection.cursor()
        stats = {'deleted_records': 0}
        
        try:
            # Build select query to backup data
            if where_clause:
                select_query = f"SELECT * FROM {table_name if db_type == 'sqlite' else f'`{table_name}`'} WHERE {where_clause}"
                delete_query = f"DELETE FROM {table_name if db_type == 'sqlite' else f'`{table_name}`'} WHERE {where_clause}"
            else:
                select_query = f"SELECT * FROM {table_name if db_type == 'sqlite' else f'`{table_name}`'}"
                delete_query = f"DELETE FROM {table_name if db_type == 'sqlite' else f'`{table_name}`'}"
            
            # 1. Fetch data for RecycleBin
            cursor.execute(select_query)
            rows = cursor.fetchall()
            columns = [desc[0] for desc in cursor.description]
            
            # Helper for JSON serialization
            def json_default(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return str(obj)

            # 2. Insert into RecycleBin as a single aggregate entry
            all_deleted_data = []
            for row in rows:
                # Convert row to dict
                row_data = {}
                for i, col in enumerate(columns):
                    row_data[col] = row[i]
                all_deleted_data.append(row_data)
            
            if all_deleted_data:
                data_json = json.dumps(all_deleted_data, default=json_default)
                
                if db_type == 'sqlite':
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (?, ?, ?)",
                        (table_name, data_json, None)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (%s, %s, %s)",
                        (table_name, data_json, None)
                    )
            
            # Get count before deletion
            if db_type == 'sqlite':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            else:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            count_before = cursor.fetchone()[0]
            
            # 3. Execute delete
            cursor.execute(delete_query)
            stats['deleted_records'] = cursor.rowcount
            connection.commit()
            
            # Emit real-time event
            if stats['deleted_records'] > 0:
                cls.emit_realtime_event('deleted', table_name, {
                    'count': stats['deleted_records'],
                    'count_before': count_before,
                    'count_after': count_before - stats['deleted_records'],
                    'moved_to_recycle_bin': len(rows)
                })
            
            return stats
            
        except Exception as e:
            connection.rollback()
            raise Exception(f"Delete failed: {e}")
    
    @classmethod
    def update_records(cls, connection, table_name: str, set_clause: str, where_clause: str = None, db_type: str = 'sqlite') -> Dict[str, int]:
        """Update records in table and emit real-time events"""
        cursor = connection.cursor()
        stats = {'updated_records': 0}
        
        try:
            # Build update query
            q = '"' if db_type == 'sqlite' else '`'
            if where_clause:
                query = f"UPDATE {q}{table_name}{q} SET {set_clause} WHERE {where_clause}"
            else:
                query = f"UPDATE {q}{table_name}{q} SET {set_clause}"
            
            # Execute update
            cursor.execute(query)
            stats['updated_records'] = cursor.rowcount
            connection.commit()
            
            # Emit real-time event
            if stats['updated_records'] > 0:
                cls.emit_realtime_event('updated', table_name, {
                    'count': stats['updated_records'],
                    'set_clause': set_clause,
                    'where_clause': where_clause
                })
            
            return stats
            
        except Exception as e:
            connection.rollback()
            raise Exception(f"Update failed: {e}")

    @classmethod
    def drop_table(cls, connection, table_name: str, db_type: str = 'sqlite') -> bool:
        """Move table data to RecycleBin and then drop the table"""
        cursor = connection.cursor()
        try:
            # 1. Ensure RecycleBin exists
            cls.initialize_recycle_bin(connection, db_type)
            
            # 2. Get table schema (CREATE TABLE SQL)
            table_schema = None
            try:
                if db_type == 'sqlite':
                    cursor.execute("SELECT sql FROM sqlite_master WHERE type='table' AND name=?", (table_name,))
                else:
                    cursor.execute(f"SHOW CREATE TABLE `{table_name}`")
                
                result = cursor.fetchone()
                if result:
                    table_schema = result[0] if db_type == 'sqlite' else result[1]
            except Exception as schema_err:
                print(f"[WARNING] Failed to fetch schema for {table_name}: {schema_err}")

            # 3. Get row count
            if db_type == 'sqlite':
                cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            else:
                cursor.execute(f"SELECT COUNT(*) FROM `{table_name}`")
            row_count = cursor.fetchone()[0]
            
            # 4. Archive data if any
            if row_count > 0:
                if db_type == 'sqlite':
                    cursor.execute(f"SELECT * FROM {table_name}")
                else:
                    cursor.execute(f"SELECT * FROM `{table_name}`")
                
                rows = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                
                # Use json_default to handle datetime objects
                def json_default(obj):
                    if hasattr(obj, 'isoformat'):
                        return obj.isoformat()
                    return str(obj)

                all_rows_data = []
                for row in rows:
                    # Convert row to dict
                    row_data = {}
                    for i, col in enumerate(columns):
                        row_data[col] = row[i]
                    all_rows_data.append(row_data)
                
                data_json = json.dumps(all_rows_data, default=json_default)
                
                if db_type == 'sqlite':
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (?, ?, ?)", 
                        (table_name, data_json, table_schema)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (%s, %s, %s)", 
                        (table_name, data_json, table_schema)
                    )
            elif table_schema:
                # Even if empty, store the schema so the table can be recreated (storing empty list)
                if db_type == 'sqlite':
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (?, ?, ?)", 
                        (table_name, "[]", table_schema)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (%s, %s, %s)", 
                        (table_name, "[]", table_schema)
                    )

            # 5. Drop the table
            if db_type == 'sqlite':
                cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
            else:
                cursor.execute(f"DROP TABLE IF EXISTS `{table_name}`")
            
            connection.commit()
            
            # Emit real-time event
            cls.emit_realtime_event('table_deleted', table_name, {'rows_archived': row_count})
            
            return True
        except Exception as e:
            connection.rollback()
            print(f"[ERROR] Drop table failed: {e}")
            raise Exception(f"Drop table failed: {e}")

    @classmethod
    def close_all(cls):
        """Close all database connections"""
        for conn in cls._connections.values():
            try:
                conn.close()
            except:
                pass
        cls._connections.clear()
        cls._connection_types.clear()

    @classmethod
    def cleanup_recycle_bin(cls, connection, days_to_keep=30):
        """
        Permanently delete old entries from the RecycleBin table.
        
        Args:
            connection: Database connection object
            days_to_keep: Number of days to keep deleted items (default: 30)
            
        Returns:
            int: Number of rows deleted
        """
        cursor = connection.cursor()
        
        try:
            # Determine database type from connection
            is_sqlite = isinstance(connection, sqlite3.Connection)
            db_type = 'sqlite' if is_sqlite else 'mysql'
            
            # First, make sure the RecycleBin table exists
            cls.initialize_recycle_bin(connection, db_type)
            
            if is_sqlite:
                # For SQLite, use DATE function with 'now' modifier
                cursor.execute('''
                    DELETE FROM RecycleBin 
                    WHERE deleted_at < DATE('now', ?)
                ''', (f'-{days_to_keep} days',))
            else:  # MySQL/MariaDB
                cursor.execute('''
                    DELETE FROM RecycleBin 
                    WHERE deleted_at < DATE_SUB(NOW(), INTERVAL ? DAY)
                ''', (days_to_keep,))
                
            deleted_count = cursor.rowcount
            connection.commit()
            print(f"[INFO] Cleaned up {deleted_count} old entries from RecycleBin")
            return deleted_count
            
        except Exception as e:
            print(f"[ERROR] Failed to clean up RecycleBin: {e}")
            connection.rollback()
            return 0


def init_db(app):
    """Initialize database for Flask app"""
    @app.teardown_appcontext
    def shutdown_session(exception=None):
        """Clean up database connections when app context ends"""
        if exception:
            print(f"Exception during request: {exception}")
        
        # Close request-scoped connection to return it to the pool
        if 'db_conn' in g:
            try:
                # For SQLite, it might be better to keep it open or let internal pooling handle it
                # For MariaDB (SQLAlchemy/mariadb), closing the connection returns it to the pool
                conn = g.pop('db_conn', None)
                if conn:
                    conn.close()
            except Exception as e:
                print(f"[DEBUG] Error closing connection during teardown: {e}")
    
    # Ensure upload folder exists
    with app.app_context():
        os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)
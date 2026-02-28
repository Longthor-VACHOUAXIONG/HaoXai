# database/db_manager.py
"""Database management module for SQLite and MySQL operations."""
import sqlite3
import os
import json
from typing import Union, Dict, Any, Optional, Tuple, List
import mariadb
from mariadb import Error as MariaDBError
from datetime import datetime
import threading

class DatabaseManager:
    """Handles database connections and operations for both SQLite and MySQL."""
    _connections = {}
    _connection_types = {}
    _local = threading.local()  # Thread-local storage for SQLite connections

    @classmethod
    def get_connection(
        cls, 
        db_path: Union[str, Dict[str, Any]], 
        connection_type: str = 'sqlite'
    ):
        """
        Get or create a database connection.
        
        Args:
            db_path: For SQLite: path to database file. 
                    For MySQL/MariaDB: dict with host, user, password, database, port (optional)
                                  OR connection string in format "mysql://user:password@host:port/database"
            connection_type: 'sqlite' or 'mysql'
        """
        print(f"[DEBUG] get_connection called with type: {connection_type}, db_path: {db_path}")
        
        if connection_type == 'sqlite':
            if not isinstance(db_path, str) or not db_path:
                raise ValueError("Invalid SQLite database path")
            
            # Convert to absolute path if it's a relative path
            db_path = os.path.abspath(db_path)
            
            if not os.path.exists(db_path):
                # For SQLite, create the database file if it doesn't exist
                print(f"[DEBUG] SQLite database not found at {db_path}, creating new database")
                try:
                    open(db_path, 'a').close()
                except IOError as e:
                    raise FileNotFoundError(f"Failed to create SQLite database: {e}")
            
            # Use thread-local storage for SQLite connections
            if not hasattr(cls._local, 'connections'):
                cls._local.connections = {}
            
            if db_path not in cls._local.connections:
                try:
                    print(f"[DEBUG] Creating new SQLite connection to {db_path} for thread {threading.current_thread().ident}")
                    conn = sqlite3.connect(db_path, check_same_thread=False)
                    conn.execute("PRAGMA foreign_keys = ON")
                    conn.execute("PRAGMA journal_mode=WAL")
                    conn.row_factory = sqlite3.Row  # Enable row factory for dict-like access
                    cls._local.connections[db_path] = conn
                    cls._connection_types[db_path] = 'sqlite'
                    cls.initialize_recycle_bin(conn)
                except sqlite3.Error as e:
                    raise ConnectionError(f"Failed to connect to SQLite database: {e}")
                    
            return cls._local.connections[db_path]
            
        elif connection_type == 'mysql':
            # Handle both dict and connection string
            if isinstance(db_path, str) and db_path.startswith('mysql://'):
                # Parse connection string
                import re
                match = re.match(r'mysql://([^:]+):?([^@]*)@([^:/]+):?(\d*)/(.+)', db_path)
                if not match:
                    raise ValueError("Invalid MySQL connection string format. Use: mysql://user:password@host:port/database")
                    
                user, password, host, port, database = match.groups()
                db_path = {
                    'host': host,
                    'user': user,
                    'password': password or '',
                    'database': database,
                    'port': int(port) if port else 3306
                }
                
            elif not isinstance(db_path, dict):
                raise ValueError("For MySQL/MariaDB, db_path must be a dictionary or connection string")
                
            # Validate required parameters
            required = ['host', 'user', 'password', 'database']
            missing = [param for param in required if param not in db_path]
            if missing:
                raise ValueError(f"Missing required MySQL connection parameters: {', '.join(missing)}")
                
            # Create a unique key for this connection
            conn_key = f"mariadb://{db_path.get('user')}@{db_path.get('host')}/{db_path.get('database')}"
            
            # Initialize thread-local storage if needed
            if not hasattr(cls._local, 'connections'):
                cls._local.connections = {}
            
            if conn_key not in cls._local.connections:
                try:
                    print(f"[DEBUG] Creating new MariaDB connection to {db_path.get('host')}/{db_path.get('database')} for thread {threading.current_thread().ident}")
                    
                    # Extract connection parameters with defaults
                    conn_params = {
                        'host': db_path['host'],
                        'user': db_path['user'],
                        'password': db_path['password'],
                        'database': db_path['database'],
                        'port': int(db_path.get('port', 3306)),
                        'ssl': False,  # Explicitly disable SSL
                        'connect_timeout': 10  # Add connection timeout
                    }
                    
                    print(f"[DEBUG] Connection parameters: {conn_params}")
                    
                    # Create the connection
                    conn = mariadb.connect(**conn_params)
                    
                    # Store the connection in thread-local storage
                    cls._local.connections[conn_key] = conn
                    cls._connection_types[conn_key] = 'mysql'
                    
                    # Pass the connection object instead of the key
                    cls.initialize_recycle_bin(conn)
                    
                    print("[DEBUG] Successfully connected to MariaDB")
                    
                except MariaDBError as e:
                    error_msg = f"Failed to connect to MariaDB: {e}"
                    print(f"[ERROR] {error_msg}")
                    raise ConnectionError(error_msg)
                except Exception as e:
                    error_msg = f"Unexpected error connecting to MariaDB: {e}"
                    print(f"[ERROR] {error_msg}")
                    raise ConnectionError(error_msg)
                    
            return cls._local.connections[conn_key]
            
        else:
            error_msg = f"Unsupported database type: {connection_type}"
            print(f"[ERROR] {error_msg}")
            raise ValueError(error_msg)

    @classmethod
    def get_connection_type(cls, connection_key: str) -> str:
        """Get the type of database for a connection."""
        return cls._connection_types.get(connection_key, 'sqlite')
        
    @classmethod
    def initialize_recycle_bin(cls, connection):
        """Initialize the RecycleBin table if it doesn't exist.
        
        Args:
            connection: Either a connection object or a connection key string
        """
        # If connection is a string (connection key), get the actual connection
        # If connection is a string (connection key), get the actual connection
        if isinstance(connection, str):
            if hasattr(cls._local, 'connections'):
                conn = cls._local.connections.get(connection)
            else:
                conn = None
            
            if conn is None:
                # Fallback to shared connections
                conn = cls._connections.get(connection)
                
            if conn is None:
                print(f"[WARNING] No connection found for key: {connection}")
                return
        else:
            conn = connection
            
        cursor = conn.cursor()
        
        try:
            # Determine if this is SQLite or MySQL
            is_sqlite = isinstance(conn, sqlite3.Connection)
            
            if is_sqlite:
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
                
                # Add performance indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recyclebin_deleted_at ON RecycleBin(deleted_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recyclebin_table ON RecycleBin(original_table)")
                
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
                
                # Add performance indexes
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recyclebin_deleted_at ON RecycleBin(deleted_at DESC)")
                cursor.execute("CREATE INDEX IF NOT EXISTS idx_recyclebin_table ON RecycleBin(original_table)")
                
            conn.commit()
        except Exception as e:
            print(f"[ERROR] Failed to initialize RecycleBin: {e}")
            conn.rollback()
            raise
            
    @classmethod
    def cleanup_recycle_bin(cls, connection, days_to_keep=30):
        """
        Permanently delete old entries from the RecycleBin table.
        
        Args:
            connection: Either a connection object or a connection key string
            days_to_keep: Number of days to keep deleted items (default: 30)
            
        Returns:
            int: Number of rows deleted
        """
        # If connection is a string (connection key), get the actual connection
        if isinstance(connection, str):
            if hasattr(cls._local, 'connections'):
                conn = cls._local.connections.get(connection)
            else:
                conn = None
                
            if conn is None:
                # Fallback to shared connections if not in thread-local (legacy support)
                conn = cls._connections.get(connection)
                
            if conn is None:
                print(f"[WARNING] No connection found for key: {connection}")
                return 0
        else:
            conn = connection
            
        cursor = conn.cursor()
        
        try:
            # First, make sure the RecycleBin table exists
            cls.initialize_recycle_bin(conn)
            
            # Determine if this is SQLite or MySQL
            is_sqlite = isinstance(conn, sqlite3.Connection)
            
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
            conn.commit()
            print(f"[INFO] Cleaned up {deleted_count} old entries from RecycleBin")
            return deleted_count
            
        except Exception as e:
            print(f"[ERROR] Failed to clean up RecycleBin: {e}")
            conn.rollback()
            return 0

    @classmethod
    def delete_records(cls, connection, table_name: str, where_clause: str = None) -> Dict[str, int]:
        """Delete records from table and move to RecycleBin.
        
        Args:
            connection: Database connection object
            table_name: Name of the table
            where_clause: Optional WHERE clause for deletion
            
        Returns:
            dict: Statistics about the deletion
        """
        cursor = connection.cursor()
        stats = {'deleted_records': 0}
        
        # Determine if this is SQLite or MySQL
        is_sqlite = isinstance(connection, sqlite3.Connection)
        
        try:
            # First, make sure the RecycleBin table exists
            cls.initialize_recycle_bin(connection)
            
            # Build select query to backup data
            table_ref = table_name if is_sqlite else f"`{table_name}`"
            if where_clause:
                select_query = f"SELECT * FROM {table_ref} WHERE {where_clause}"
                delete_query = f"DELETE FROM {table_ref} WHERE {where_clause}"
            else:
                select_query = f"SELECT * FROM {table_ref}"
                delete_query = f"DELETE FROM {table_ref}"
            
            # 1. Fetch data for RecycleBin
            cursor.execute(select_query)
            rows = cursor.fetchall()
            
            # Handle different row formats (SQLite Row vs MySQL tuple/list)
            columns = [desc[0] for desc in cursor.description]
            
            # Helper for JSON serialization
            def json_default(obj):
                if hasattr(obj, 'isoformat'):
                    return obj.isoformat()
                return str(obj)

            # 2. Insert into RecycleBin
            for row in rows:
                row_data = {}
                for i, col in enumerate(columns):
                    row_data[col] = row[i]
                
                data_json = json.dumps(row_data, default=json_default)
                
                if is_sqlite:
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (?, ?, ?)",
                        (table_name, data_json, None)
                    )
                else:
                    cursor.execute(
                        "INSERT INTO RecycleBin (original_table, data, table_schema) VALUES (%s, %s, %s)",
                        (table_name, data_json, None)
                    )
            
            # 3. Execute delete
            cursor.execute(delete_query)
            stats['deleted_records'] = cursor.rowcount
            connection.commit()
            
            print(f"[INFO] Deleted {stats['deleted_records']} records from {table_name} and moved to RecycleBin")
            return stats
            
        except Exception as e:
            connection.rollback()
            print(f"[ERROR] Delete failed: {e}")
            raise
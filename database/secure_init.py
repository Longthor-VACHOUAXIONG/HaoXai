"""
Secure Database Initializer for HaoXai
Handles the creation of the secure database schema, audit triggers, and initial security setup.
"""

import os
import sqlite3
import mariadb
import logging
import shutil
from datetime import datetime
from typing import Optional, List, Dict, Any
from .security import DatabaseSecurity

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class SecureDatabaseInitializer:
    """Handles the initialization of a secure HaoXai database"""
    
    def __init__(self, db_path: Any, db_type: str = 'sqlite'):
        self.db_path = db_path
        self.db_type = db_type
        self.security = DatabaseSecurity(db_path, db_type=db_type)
        
    def _create_secure_schema(self):
        """Create the core application tables with security features"""
        core_tables = [
            """
            CREATE TABLE IF NOT EXISTS rodent_hosts (
                host_id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_code TEXT UNIQUE NOT NULL,
                species TEXT,
                gender TEXT,
                weight REAL,
                collection_date DATE,
                location_id INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS samples (
                sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
                host_id INTEGER,
                sample_type TEXT NOT NULL,
                sample_code TEXT UNIQUE NOT NULL,
                collection_date DATE,
                storage_location TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (host_id) REFERENCES rodent_hosts (host_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS screening_results (
                result_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_id INTEGER,
                virus_type TEXT,
                pcr_result TEXT,
                ct_value REAL,
                sequencing_status TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sample_id) REFERENCES samples (sample_id)
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS storage (
                storage_id INTEGER PRIMARY KEY AUTOINCREMENT,
                sample_id INTEGER NOT NULL,
                freezer_name TEXT,
                cabinet_no TEXT,
                cabinet_floor TEXT,
                floor_row TEXT,
                box_no TEXT,
                position_in_box TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (sample_id) REFERENCES samples (sample_id)
            )
            """
        ]
        
        processed_tables = []
        for sql in core_tables:
            if self.db_type == 'mysql':
                sql = sql.replace('INTEGER PRIMARY KEY AUTOINCREMENT', 'INT AUTO_INCREMENT PRIMARY KEY')
                sql = sql.replace('REAL', 'DOUBLE')
                sql = sql.replace('TEXT', 'VARCHAR(255)')
                if 'freezer_name' in sql:
                    sql = sql.replace('VARCHAR(255)', 'TEXT')
            processed_tables.append(sql)

        cursor = self.security.conn.cursor()
        for table_sql in processed_tables:
            cursor.execute(table_sql)
        self.security.conn.commit()

    def _create_audit_triggers(self):
        """Create audit triggers for tracking changes (SQLite only)"""
        if self.db_type != 'sqlite':
            return

        tables = ['rodent_hosts', 'samples', 'screening_results', 'storage']
        cursor = self.security.conn.cursor()
        
        for table in tables:
            cursor.execute(f"DROP TRIGGER IF EXISTS audit_{table}_insert")
            cursor.execute(f"""
                CREATE TRIGGER audit_{table}_insert AFTER INSERT ON {table}
                BEGIN
                    INSERT INTO security_audit_log (action, table_name, record_id, new_values)
                    VALUES ('INSERT', '{table}', NEW.rowid, 'New record created');
                END;
            """)
        self.security.conn.commit()

    def _create_indexes(self):
        """Create performance indexes"""
        indexes = [
            "CREATE INDEX IF NOT EXISTS idx_host_code ON rodent_hosts(host_code)",
            "CREATE INDEX IF NOT EXISTS idx_sample_code ON samples(sample_code)",
            "CREATE INDEX IF NOT EXISTS idx_sample_type ON samples(sample_type)"
        ]
        cursor = self.security.conn.cursor()
        for idx_sql in indexes:
            cursor.execute(idx_sql)
        self.security.conn.commit()

    def _setup_schema_protection(self):
        """Set up default schema protection rules"""
        protection_rules = [
            ('security_users', 'DROP', '["admin"]', True),
            ('security_roles', 'DROP', '["admin"]', True),
            ('rodent_hosts', 'DROP', '["admin"]', True),
            ('samples', 'DROP', '["admin"]', True)
        ]
        
        cursor = self.security.conn.cursor()
        for table, op, roles, req_app in protection_rules:
            if self.db_type == 'sqlite':
                cursor.execute("""
                    INSERT OR IGNORE INTO security_schema_protection 
                    (table_name, operation, allowed_roles, requires_approval) 
                    VALUES (?, ?, ?, ?)
                """, (table, op, roles, req_app))
            else:
                cursor.execute("SELECT COUNT(*) FROM security_schema_protection WHERE table_name = ? AND operation = ?", (table, op))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        INSERT INTO security_schema_protection 
                        (table_name, operation, allowed_roles, requires_approval) 
                        VALUES (?, ?, ?, ?)
                    """, (table, op, roles, req_app))
        self.security.conn.commit()

    def _setup_row_level_security(self):
        """Set up default row-level security policies"""
        policies = [
            ('Personal Data Access', 'security_users', 'user_id = CURRENT_USER_ID() OR ROLE = "admin"')
        ]
        
        cursor = self.security.conn.cursor()
        for name, table, filter in policies:
            if self.db_type == 'sqlite':
                cursor.execute("""
                    INSERT OR IGNORE INTO security_row_policies 
                    (policy_id, policy_name, table_name, role_filter) 
                    VALUES ((SELECT IFNULL(MAX(policy_id), 0) + 1 FROM security_row_policies), ?, ?, ?)
                """, (name, table, filter))
            else:
                cursor.execute("SELECT COUNT(*) FROM security_row_policies WHERE policy_name = ?", (name,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        INSERT INTO security_row_policies 
                        (policy_name, table_name, role_filter) 
                        VALUES (?, ?, ?)
                    """, (name, table, filter))
        self.security.conn.commit()

    def initialize(self):
        """Run all initialization steps"""
        try:
            self._create_secure_schema()
            self._create_audit_triggers()
            self._create_indexes()
            self._setup_schema_protection()
            self._setup_row_level_security()
            logger.info("Database initialization complete.")
            return True
        except Exception as e:
            logger.error(f"Error initializing database: {e}")
            return False

    def close(self):
        self.security.close()

def create_secure_database(db_path: Any, db_type: str = 'sqlite') -> bool:
    """Helper function to create and initialize a new secure database"""
    initializer = SecureDatabaseInitializer(db_path, db_type=db_type)
    success = initializer.initialize()
    initializer.close()
    return success

def migrate_existing_database(db_path: Any, db_type: str = 'sqlite') -> bool:
    """Migrate an existing database to the secure structure"""
    try:
        if db_type == 'sqlite' and os.path.exists(db_path):
            backup_path = f"{db_path}.{datetime.now().strftime('%Y%m%d_%H%M%S')}.bak"
            shutil.copy2(db_path, backup_path)
        
        initializer = SecureDatabaseInitializer(db_path, db_type=db_type)
        success = initializer.initialize()
        initializer.close()
        return success
    except Exception as e:
        logger.error(f"Migration error: {e}")
        return False

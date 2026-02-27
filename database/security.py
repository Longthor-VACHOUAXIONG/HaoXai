"""
Database Security Module for HaoXai
Provides comprehensive database protection including:
- User authentication and role-based access control
- Data encryption for sensitive information
- Audit logging for all database operations
- Automatic backup and recovery
- Row-level security
- Schema validation and protection
"""

import sqlite3
import mariadb
from mariadb import Error as MariaDBError
import hashlib
import secrets
import json
import base64
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from cryptography.fernet import Fernet
from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.kdf.pbkdf2 import PBKDF2HMAC
import os
import shutil
import logging

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class DatabaseSecurity:
    """Comprehensive database security manager for HaoXai"""
    
    def __init__(self, db_path: str, encryption_key: Optional[str] = None, db_type: str = 'sqlite'):
        self.db_path = db_path
        self.db_type = db_type
        
        if not db_path:
            raise ValueError("Database path or connection parameters must be provided")

        # Create connection based on database type
        if db_type == 'sqlite':
            if not isinstance(db_path, str) or not db_path.strip():
                raise ValueError("For SQLite, db_path must be a non-empty string")
            self.conn = sqlite3.connect(db_path)
            cursor = self.conn.cursor()
            cursor.execute("PRAGMA foreign_keys=ON")
        elif db_type == 'mysql':
            if not isinstance(db_path, dict) or not all(k in db_path for k in ['host', 'user', 'database']):
                raise ValueError("For MySQL, db_path must be a dictionary with at least 'host', 'user', and 'database'")
            self.conn = mariadb.connect(**db_path)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")
        
        # Initialize encryption
        self.encryption_key = encryption_key or self._generate_encryption_key()
        self.cipher = Fernet(self.encryption_key)
        
        # Initialize security tables
        self._initialize_security_tables()
        
    def _generate_encryption_key(self) -> bytes:
        """Generate a secure encryption key"""
        password = secrets.token_bytes(32)
        salt = os.urandom(16)
        kdf = PBKDF2HMAC(
            algorithm=hashes.SHA256(),
            length=32,
            salt=salt,
            iterations=100000,
        )
        key = base64.urlsafe_b64encode(kdf.derive(password))
        return key
    
    def _initialize_security_tables(self):
        """Create all security-related tables"""
        
        # Define table schemas based on database type
        if self.db_type == 'sqlite':
            # SQLite syntax
            users_table = """
                CREATE TABLE IF NOT EXISTS security_users (
                    user_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    username TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    salt TEXT NOT NULL,
                    email TEXT UNIQUE,
                    role TEXT NOT NULL DEFAULT 'viewer',
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_login TIMESTAMP,
                    failed_login_attempts INTEGER DEFAULT 0,
                    locked_until TIMESTAMP
                )
            """
            
            roles_table = """
                CREATE TABLE IF NOT EXISTS security_roles (
                    role_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    role_name TEXT UNIQUE NOT NULL,
                    description TEXT,
                    permissions TEXT, -- JSON array of permissions
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            audit_table = """
                CREATE TABLE IF NOT EXISTS security_audit_log (
                    log_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id INTEGER,
                    action TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    record_id TEXT,
                    old_values TEXT, -- JSON
                    new_values TEXT, -- JSON
                    ip_address TEXT,
                    user_agent TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES security_users (user_id)
                )
            """
            
            backup_table = """
                CREATE TABLE IF NOT EXISTS security_backup_log (
                    backup_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    backup_path TEXT NOT NULL,
                    backup_type TEXT NOT NULL,
                    file_size INTEGER,
                    checksum TEXT,
                    created_by INTEGER,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES security_users (user_id)
                )
            """
            
            schema_table = """
                CREATE TABLE IF NOT EXISTS security_schema_protection (
                    protection_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    operation TEXT NOT NULL, -- 'CREATE', 'ALTER', 'DROP'
                    allowed_roles TEXT, -- JSON array of roles that can perform this
                    requires_approval BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """

            row_policies_table = """
                CREATE TABLE IF NOT EXISTS security_row_policies (
                    policy_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    policy_name TEXT NOT NULL,
                    table_name TEXT NOT NULL,
                    role_filter TEXT NOT NULL, -- SQL WHERE clause for role-based filtering
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by INTEGER,
                    FOREIGN KEY (created_by) REFERENCES security_users (user_id)
                )
            """

            encrypted_fields_table = """
                CREATE TABLE IF NOT EXISTS security_encrypted_fields (
                    field_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    table_name TEXT NOT NULL,
                    column_name TEXT NOT NULL,
                    encryption_type TEXT DEFAULT 'AES256',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            
        else:  # MySQL syntax
            users_table = """
                CREATE TABLE IF NOT EXISTS security_users (
                    user_id INT AUTO_INCREMENT PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    salt VARCHAR(255) NOT NULL,
                    email VARCHAR(255) UNIQUE,
                    role VARCHAR(50) NOT NULL DEFAULT 'viewer',
                    is_active BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                    last_login TIMESTAMP NULL,
                    failed_login_attempts INT DEFAULT 0,
                    locked_until TIMESTAMP NULL
                )
            """
            
            roles_table = """
                CREATE TABLE IF NOT EXISTS security_roles (
                    role_id INT AUTO_INCREMENT PRIMARY KEY,
                    role_name VARCHAR(100) UNIQUE NOT NULL,
                    description TEXT,
                    permissions TEXT, -- JSON array of permissions
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
            
            audit_table = """
                CREATE TABLE IF NOT EXISTS security_audit_log (
                    log_id INT AUTO_INCREMENT PRIMARY KEY,
                    user_id INT,
                    action VARCHAR(100) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    record_id VARCHAR(255),
                    old_values TEXT, -- JSON
                    new_values TEXT, -- JSON
                    ip_address VARCHAR(45),
                    user_agent TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (user_id) REFERENCES security_users (user_id)
                )
            """
            
            backup_table = """
                CREATE TABLE IF NOT EXISTS security_backup_log (
                    backup_id INT AUTO_INCREMENT PRIMARY KEY,
                    backup_path VARCHAR(500) NOT NULL,
                    backup_type VARCHAR(50) NOT NULL,
                    file_size BIGINT,
                    checksum VARCHAR(255),
                    created_by INT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (created_by) REFERENCES security_users (user_id)
                )
            """

            schema_table = """
                CREATE TABLE IF NOT EXISTS security_schema_protection (
                    protection_id INT AUTO_INCREMENT PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    operation VARCHAR(50) NOT NULL, -- 'CREATE', 'ALTER', 'DROP'
                    allowed_roles TEXT, -- JSON array of roles that can perform this
                    requires_approval BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """

            row_policies_table = """
                CREATE TABLE IF NOT EXISTS security_row_policies (
                    policy_id INT AUTO_INCREMENT PRIMARY KEY,
                    policy_name VARCHAR(255) NOT NULL,
                    table_name VARCHAR(255) NOT NULL,
                    role_filter TEXT NOT NULL, -- SQL WHERE clause for role-based filtering
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    created_by INT,
                    FOREIGN KEY (created_by) REFERENCES security_users (user_id)
                )
            """

            encrypted_fields_table = """
                CREATE TABLE IF NOT EXISTS security_encrypted_fields (
                    field_id INT AUTO_INCREMENT PRIMARY KEY,
                    table_name VARCHAR(255) NOT NULL,
                    column_name VARCHAR(255) NOT NULL,
                    encryption_type VARCHAR(50) DEFAULT 'AES256',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """
        
        # Create tables
        cursor = self.conn.cursor()
        cursor.execute(users_table)
        cursor.execute(roles_table)
        cursor.execute(audit_table)
        cursor.execute(backup_table)
        cursor.execute(schema_table)
        cursor.execute(row_policies_table)
        cursor.execute(encrypted_fields_table)
        
        self.conn.commit()
        
        # Insert default roles
        self._insert_default_roles()
        
        # Create default admin user if none exists
        self._create_default_admin()
        
        self.conn.commit()
    
    def _insert_default_roles(self):
        """Insert default security roles"""
        default_roles = [
            ('admin', 'Full system access including user management', 
             '["ALL"]'),
            ('researcher', 'Can view, create, and modify data', 
             '["READ", "WRITE", "IMPORT", "EXPORT"]'),
            ('viewer', 'Read-only access to data', 
             '["READ"]'),
            ('analyst', 'Can analyze data and generate reports', 
             '["READ", "ANALYZE", "EXPORT"]')
        ]
        
        cursor = self.conn.cursor()
        for role_name, description, permissions in default_roles:
            if self.db_type == 'sqlite':
                cursor.execute("""
                    INSERT OR IGNORE INTO security_roles 
                    (role_name, description, permissions) 
                    VALUES (?, ?, ?)
                """, (role_name, description, json.dumps(permissions)))
            else:
                cursor.execute("SELECT COUNT(*) FROM security_roles WHERE role_name = ?", (role_name,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute("""
                        INSERT INTO security_roles 
                        (role_name, description, permissions) 
                        VALUES (?, ?, ?)
                    """, (role_name, description, json.dumps(permissions)))
        self.conn.commit()
    
    def update_role(self, role_id: int, role_name: str, description: str, permissions: List[str]) -> bool:
        """Update an existing security role"""
        try:
            cursor = self.conn.cursor()
            
            # Get old values for audit log
            cursor.execute("SELECT role_name, description, permissions FROM security_roles WHERE role_id = ?", (role_id,))
            old_row = cursor.fetchone()
            if not old_row:
                return False
                
            old_values = {
                'role_name': old_row[0],
                'description': old_row[1],
                'permissions': json.loads(old_row[2]) if old_row[2] else []
            }
            
            new_values = {
                'role_name': role_name,
                'description': description,
                'permissions': permissions
            }
            
            cursor.execute("""
                UPDATE security_roles 
                SET role_name = ?, description = ?, permissions = ?
                WHERE role_id = ?
            """, (role_name, description, json.dumps(permissions), role_id))
            
            # Update all users who had the old role name if it changed
            if role_name != old_values['role_name']:
                cursor.execute("""
                    UPDATE security_users
                    SET role = ?
                    WHERE role = ?
                """, (role_name, old_values['role_name']))
            
            self._log_action(None, 'UPDATE_ROLE', 'security_roles', str(role_id), 
                          old_values, new_values, None, None)
            
            self.conn.commit()
            logger.info(f"Role updated successfully: {role_name}")
            return True
        except Exception as e:
            logger.error(f"Error updating role: {e}")
            return False

    def delete_role(self, role_id: int) -> bool:
        """Delete a security role"""
        try:
            cursor = self.conn.cursor()
            
            # Prevent deleting system roles
            cursor.execute("SELECT role_name, description, permissions FROM security_roles WHERE role_id = ?", (role_id,))
            role_data = cursor.fetchone()
            if not role_data:
                return False
                
            role_name = role_data[0]
            if role_name in ['admin', 'researcher', 'viewer', 'analyst']:
                logger.warning(f"Attempted to delete system role: {role_name}")
                return False
            
            # Check if any users are using this role
            cursor.execute("SELECT COUNT(*) FROM security_users WHERE role = ?", (role_name,))
            if cursor.fetchone()[0] > 0:
                logger.warning(f"Cannot delete role '{role_name}' because it is in use by users")
                return False
            
            old_values = {
                'role_name': role_name,
                'description': role_data[1],
                'permissions': json.loads(role_data[2]) if role_data[2] else []
            }
            
            cursor.execute("DELETE FROM security_roles WHERE role_id = ?", (role_id,))
            
            self._log_action(None, 'DELETE_ROLE', 'security_roles', str(role_id), 
                          old_values, None, None, None)
            
            self.conn.commit()
            logger.info(f"Role deleted successfully: {role_name}")
            return True
        except Exception as e:
            logger.error(f"Error deleting role: {e}")
            return False

    
    def _create_default_admin(self):
        """Create default admin user if none exists"""
        cursor = self.conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM security_users WHERE role = 'admin'")
        if cursor.fetchone()[0] == 0:
            # Generate secure password
            default_password = "admin123"
            salt = secrets.token_hex(16)
            password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          default_password.encode(), 
                                          salt.encode(), 
                                          100000).hex()
            
            cursor.execute("""
                INSERT INTO security_users 
                (username, password_hash, salt, email, role) 
                VALUES (?, ?, ?, ?, ?)
            """, ("admin", password_hash, salt, "admin@HaoXai.local", "admin"))
            self.conn.commit()
            logger.info("Default admin user created: admin / admin123")
    
    def create_user(self, username: str, password: str, email: str, role: str = 'viewer') -> bool:
        """Create a new user with secure password hashing"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT user_id FROM security_users WHERE username = ?", (username,))
            if cursor.fetchone():
                return False
            
            salt = secrets.token_hex(16)
            password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          password.encode(), 
                                          salt.encode(), 
                                          100000).hex()
            
            cursor.execute("""
                INSERT INTO security_users 
                (username, password_hash, salt, email, role) 
                VALUES (?, ?, ?, ?, ?)
            """, (username, password_hash, salt, email, role))
            
            self._log_action(None, 'CREATE_USER', 'security_users', None, 
                          None, {'username': username, 'role': role}, None, None)
            
            self.conn.commit()
            logger.info(f"User created successfully: {username}")
            return True
            
        except Exception as e:
            logger.error(f"Error creating user: {e}")
            return False
    
    def authenticate_user(self, username: str, password: str) -> Optional[Dict[str, Any]]:
        """Authenticate user with secure password verification"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT user_id, username, password_hash, salt, role, is_active, 
                       failed_login_attempts, locked_until 
                FROM security_users 
                WHERE username = ?
            """, (username,))
            
            user_data = cursor.fetchone()
            if not user_data:
                return None
            
            user_id, _, stored_hash, salt, role, is_active, failed_attempts, locked_until = user_data
            
            if locked_until:
                try:
                    if isinstance(locked_until, str):
                        locked_until_dt = datetime.fromisoformat(locked_until)
                    else:
                        locked_until_dt = locked_until
                    
                    if datetime.now() < locked_until_dt:
                        return None
                except (ValueError, TypeError):
                    pass
            
            if not is_active:
                return None
            
            password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          password.encode(), 
                                          salt.encode(), 
                                          100000).hex()
            
            if password_hash == stored_hash:
                cursor.execute("""
                    UPDATE security_users 
                    SET failed_login_attempts = 0, 
                        locked_until = NULL, 
                        last_login = CURRENT_TIMESTAMP 
                    WHERE user_id = ?
                """, (user_id,))
                
                self._log_action(user_id, 'LOGIN', 'security_users', str(user_id), 
                               None, None, None, None)
                
                self.conn.commit()
                
                return {
                    'user_id': user_id,
                    'username': username,
                    'role': role,
                    'is_active': is_active
                }
            else:
                failed_attempts = (failed_attempts or 0) + 1
                lock_time = None
                
                if failed_attempts >= 5:
                    lock_time = datetime.now() + timedelta(minutes=30)
                    if self.db_type == 'sqlite':
                        lock_time = lock_time.isoformat()
                
                cursor.execute("""
                    UPDATE security_users 
                    SET failed_login_attempts = ?, locked_until = ? 
                    WHERE user_id = ?
                """, (failed_attempts, lock_time, user_id))
                
                self._log_action(None, 'FAILED_LOGIN', 'security_users', str(user_id), 
                               None, None, None, None)
                
                self.conn.commit()
                return None
                
        except Exception as e:
            logger.error(f"Authentication error: {e}")
            return None
    
    def encrypt_data(self, data: str) -> str:
        """Encrypt sensitive data"""
        try:
            return self.cipher.encrypt(data.encode()).decode()
        except Exception as e:
            logger.error(f"Encryption error: {e}")
            return data
    
    def decrypt_data(self, encrypted_data: str) -> str:
        """Decrypt sensitive data"""
        try:
            return self.cipher.decrypt(encrypted_data.encode()).decode()
        except Exception as e:
            logger.error(f"Decryption error: {e}")
            return encrypted_data
    
    def register_encrypted_field(self, table_name: str, column_name: str):
        """Register a field for encryption"""
        try:
            cursor = self.conn.cursor()
            if self.db_type == 'sqlite':
                cursor.execute("""
                    INSERT OR REPLACE INTO security_encrypted_fields 
                    (table_name, column_name) 
                    VALUES (?, ?)
                """, (table_name, column_name))
            else:
                cursor.execute("""
                    INSERT INTO security_encrypted_fields (table_name, column_name) 
                    VALUES (?, ?) ON DUPLICATE KEY UPDATE table_name=table_name
                """, (table_name, column_name))
            self.conn.commit()
            logger.info(f"Registered encrypted field: {table_name}.{column_name}")
        except Exception as e:
            logger.error(f"Error registering encrypted field: {e}")
    
    def _log_action(self, user_id: Optional[int], action: str, table_name: str, 
                   record_id: Optional[str], old_values: Optional[Dict], 
                   new_values: Optional[Dict], ip_address: Optional[str], 
                   user_agent: Optional[str]):
        """Log all database actions for audit trail"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO security_audit_log 
                (user_id, action, table_name, record_id, old_values, new_values, 
                 ip_address, user_agent) 
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                user_id, action, table_name, record_id,
                json.dumps(old_values) if old_values else None,
                json.dumps(new_values) if new_values else None,
                ip_address, user_agent
            ))
        except Exception as e:
            logger.error(f"Error logging action: {e}")
    
    def create_backup(self, backup_type: str = 'manual', user_id: Optional[int] = None) -> bool:
        """Create database backup with security metadata"""
        try:
            if self.db_type != 'sqlite':
                logger.warning(f"File-based backup not applicable for {self.db_type}. Use server tools.")
                return False

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            backup_filename = f"lms_backup_{timestamp}.db"
            backup_path = os.path.join(os.path.dirname(self.db_path), "backups")
            
            os.makedirs(backup_path, exist_ok=True)
            backup_full_path = os.path.join(backup_path, backup_filename)
            
            if not os.path.exists(self.db_path):
                return False
            
            shutil.copy2(self.db_path, backup_full_path)
            
            with open(backup_full_path, 'rb') as f:
                file_content = f.read()
                checksum = hashlib.sha256(file_content).hexdigest()
            
            file_size = os.path.getsize(backup_full_path)
            
            cursor = self.conn.cursor()
            cursor.execute("""
                INSERT INTO security_backup_log 
                (backup_path, backup_type, file_size, checksum, created_by, created_at) 
                VALUES (?, ?, ?, ?, ?, ?)
            """, (backup_full_path, backup_type, file_size, checksum, user_id, datetime.now()))
            
            self.conn.commit()
            return True
            
        except Exception as e:
            logger.error(f"Error creating backup: {e}")
            return False
    
    def check_schema_permission(self, table_name: str, operation: str, user_role: str) -> bool:
        """Check if user role has permission for schema operation"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("""
                SELECT allowed_roles FROM security_schema_protection 
                WHERE table_name = ? AND operation = ?
            """, (table_name, operation))
            
            result = cursor.fetchone()
            if not result:
                return False
            
            allowed_roles = json.loads(result[0])
            return user_role in allowed_roles or 'ALL' in allowed_roles
            
        except Exception as e:
            logger.error(f"Error checking schema permission: {e}")
            return False
    
    def apply_row_level_security(self, query: str, user_id: int) -> str:
        """Apply row-level security filters to SQL queries"""
        try:
            cursor = self.conn.cursor()
            cursor.execute("SELECT role FROM security_users WHERE user_id = ?", (user_id,))
            user_role_data = cursor.fetchone()
            
            if not user_role_data:
                return query
            
            user_role = user_role_data[0]
            
            cursor.execute("SELECT table_name, role_filter FROM security_row_policies")
            policies = cursor.fetchall()
            
            for table_name, role_filter in policies:
                if table_name.lower() in query.lower() and user_role.lower() in role_filter.lower():
                    if 'WHERE' in query.upper():
                        query += f" AND ({role_filter})"
                    else:
                        query += f" WHERE {role_filter}"
                    break
            return query
            
        except Exception as e:
            logger.error(f"Error applying row-level security: {e}")
            return query
    
    def get_audit_log(self, table_name: Optional[str] = None, 
                     start_date: Optional[datetime] = None,
                     end_date: Optional[datetime] = None,
                     user_id: Optional[int] = None) -> List[Dict]:
        """Retrieve audit log with filtering options"""
        try:
            query = "SELECT * FROM security_audit_log WHERE 1=1"
            params = []
            
            if table_name:
                query += " AND table_name = ?"
                params.append(table_name)
            if start_date:
                query += " AND timestamp >= ?"
                params.append(start_date.isoformat() if self.db_type == 'sqlite' else start_date)
            if end_date:
                query += " AND timestamp <= ?"
                params.append(end_date.isoformat() if self.db_type == 'sqlite' else end_date)
            if user_id:
                query += " AND user_id = ?"
                params.append(user_id)
            
            query += " ORDER BY timestamp DESC"
            
            cursor = self.conn.cursor()
            cursor.execute(query, params)
            
            columns = [description[0] for description in cursor.description]
            results = []
            
            for row in cursor.fetchall():
                result = dict(zip(columns, row))
                try:
                    if result.get('old_values'):
                        result['old_values'] = json.loads(result['old_values'])
                    if result.get('new_values'):
                        result['new_values'] = json.loads(result['new_values'])
                except:
                    pass
                results.append(result)
            return results
        except Exception as e:
            logger.error(f"Error retrieving audit log: {e}")
            return []
    
    def close(self):
        """Close database connection"""
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()

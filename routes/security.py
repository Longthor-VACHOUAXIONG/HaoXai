"""
Security API Routes for HaoXai
Provides REST API endpoints for security management
"""

from flask import Blueprint, jsonify, request, session
from database.security import DatabaseSecurity
from database.db_manager_flask import DatabaseManagerFlask
from database.secure_init import create_secure_database, migrate_existing_database
import json
from datetime import datetime
import hashlib
import os

security_bp = Blueprint("security", __name__)

@security_bp.route("/stats")
def get_security_stats():
    """Get security statistics"""
    print("=" * 50)
    print("🔍 SECURITY STATS API CALLED")
    print("=" * 50)
    
    try:
        if 'db_path' not in session:
            print("❌ No db_path in session")
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        # Debug: Print what database we're using
        db_path = session['db_path']
        db_type = session.get('db_type', 'sqlite')
        print(f"🗂️  Database path: {db_path}")
        print(f"🗂️  Database type: {db_type}")
        
        # Check if database file exists
        import os
        if not os.path.exists(db_path):
            print(f"❌ Database file does not exist: {db_path}")
            return jsonify({"success": False, "message": f"Database file not found: {db_path}"}), 400
        else:
            print(f"✅ Database file exists: {db_path}")
        
        conn = DatabaseManagerFlask.get_connection(db_path, db_type)
        security = DatabaseSecurity(db_path)
        
        cursor = conn.cursor()
        
        # Debug: Check if tables exist
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
        tables = cursor.fetchall()
        table_names = [t[0] for t in tables]
        print(f"📋 Security tables found: {table_names}")
        
        # Check each table specifically
        backup_count = 0
        audit_count = 0
        
        if 'security_backup_log' in table_names:
            cursor.execute("SELECT COUNT(*) FROM security_backup_log")
            backup_count = cursor.fetchone()[0]
            print(f"💾 Backup log count: {backup_count}")
        else:
            print("❌ security_backup_log table NOT found")
            
        if 'security_audit_log' in table_names:
            cursor.execute("SELECT COUNT(*) FROM security_audit_log")
            audit_count = cursor.fetchone()[0]
            print(f"📝 Audit log count: {audit_count}")
        else:
            print("❌ security_audit_log table NOT found")
        
        # Get user statistics
        cursor.execute("SELECT COUNT(*) FROM security_users")
        total_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM security_users WHERE is_active = 1")
        active_users = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM security_users WHERE failed_login_attempts > 0")
        failed_logins = cursor.fetchone()[0]
        
        result = {
            "success": True,
            "total_users": total_users,
            "active_users": active_users,
            "failed_logins": failed_logins,
            "backups_count": backup_count,
            "audit_count": audit_count
        }
        
        print(f"📤 Returning result: {result}")
        print("=" * 50)
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/users", methods=["GET"])
def get_users():
    """Get all users"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT user_id, username, email, role, is_active, created_at, last_login
            FROM security_users
            ORDER BY created_at DESC
        """)
        
        users = []
        for row in cursor.fetchall():
            users.append({
                'user_id': row[0],
                'username': row[1],
                'email': row[2],
                'role': row[3],
                'is_active': bool(row[4]),
                'created_at': row[5],
                'last_login': row[6]
            })
        
        return jsonify(users)
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/users", methods=["POST"])
def create_user():
    """Create a new user"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        username = request.form.get('username')
        email = request.form.get('email')
        password = request.form.get('password')
        role = request.form.get('role')
        
        if not all([username, email, password, role]):
            return jsonify({"success": False, "message": "All fields are required"}), 400
        
        # Get database type from session
        db_type = session.get('db_type', 'sqlite')
        security = DatabaseSecurity(session['db_path'], db_type=db_type)
        
        if security.create_user(username, password, email, role):
            return jsonify({"success": True, "message": "User created successfully"})
        else:
            return jsonify({"success": False, "message": "Username already exists"}), 400
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/users/<int:user_id>", methods=["PUT"])
def update_user(user_id):
    """Update existing user - requires admin password verification"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        # Verify admin password
        admin_password = request.form.get('admin_password')
        if not admin_password:
            return jsonify({"success": False, "message": "Admin password is required for authorization"}), 403
        
        # Get current user's username from session
        current_username = session.get('user', 'admin')
        
        # Verify the admin password
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        cursor = conn.cursor()
        
        cursor.execute("SELECT password_hash, salt FROM security_users WHERE username = ?", (current_username,))
        admin_user = cursor.fetchone()
        
        if not admin_user:
            return jsonify({"success": False, "message": "Admin user not found"}), 403
        
        stored_hash, salt = admin_user
        password_hash = hashlib.pbkdf2_hmac('sha256', 
                                      admin_password.encode(), 
                                      salt.encode(), 
                                      100000).hex()
        
        if password_hash != stored_hash:
            return jsonify({"success": False, "message": "Invalid admin password"}), 403
        
        # Continue with user update
        username = request.form.get('username')
        email = request.form.get('email')
        role = request.form.get('role')
        is_active = request.form.get('is_active') == 'true'
        password = request.form.get('password')
        
        if not all([username, email, role]):
            return jsonify({"success": False, "message": "All fields are required except password"}), 400
        
        # Update user
        update_fields = ["username = ?", "email = ?", "role = ?", "is_active = ?", "updated_at = ?"]
        update_values = [username, email, role, is_active, datetime.now()]
        
        if password and password.strip():
            # Update password
            import secrets
            salt = secrets.token_hex(16)
            password_hash = hashlib.pbkdf2_hmac('sha256', 
                                          password.encode(), 
                                          salt.encode(), 
                                          100000).hex()
            update_fields.append("password_hash = ?")
            update_fields.append("salt = ?")
            update_values.append(password_hash)
            update_values.append(salt)
        
        update_values.append(user_id)
        
        cursor.execute(f"""
            UPDATE security_users 
            SET {', '.join(update_fields)}
            WHERE user_id = ?
        """, update_values)
        
        conn.commit()
        
        return jsonify({"success": True, "message": "User updated successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/users/<int:user_id>", methods=["DELETE"])
def delete_user(user_id):
    """Delete user"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        cursor = conn.cursor()
        
        # Prevent deleting admin users
        cursor.execute("SELECT role FROM security_users WHERE user_id = ?", (user_id,))
        user = cursor.fetchone()
        
        if user and user[0] == 'admin':
            return jsonify({"success": False, "message": "Cannot delete admin users"}), 400
        
        # Use unified delete_records to ensure Recycle Bin integration
        DatabaseManagerFlask.delete_records(
            conn, 
            'security_users', 
            f"user_id = {user_id}", 
            db_type=session.get('db_type', 'sqlite')
        )
        
        return jsonify({"success": True, "message": "User deleted successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/users/<int:user_id>/reset-password", methods=["POST"])
def reset_password(user_id):
    """Reset user password - requires admin password verification"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        data = request.get_json()
        new_password = data.get('password')
        admin_password = data.get('admin_password')
        
        if not new_password:
            return jsonify({"success": False, "message": "New password is required"}), 400
        
        if not admin_password:
            return jsonify({"success": False, "message": "Admin password is required for authorization"}), 403
        
        # Get current user's username from session
        current_username = session.get('user', 'admin')
        
        # Verify the admin password
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        cursor = conn.cursor()
        
        cursor.execute("SELECT password_hash, salt FROM security_users WHERE username = ?", (current_username,))
        admin_user = cursor.fetchone()
        
        if not admin_user:
            return jsonify({"success": False, "message": "Admin user not found"}), 403
        
        stored_hash, salt = admin_user
        password_hash = hashlib.pbkdf2_hmac('sha256', 
                                      admin_password.encode(), 
                                      salt.encode(), 
                                      100000).hex()
        
        if password_hash != stored_hash:
            return jsonify({"success": False, "message": "Invalid admin password"}), 403
        
        # Continue with password reset
        import secrets
        salt = secrets.token_hex(16)
        password_hash = hashlib.pbkdf2_hmac('sha256', 
                                      new_password.encode(), 
                                      salt.encode(), 
                                      100000).hex()
        
        cursor.execute("""
            UPDATE security_users 
            SET password_hash = ?, salt = ?, updated_at = ?
            WHERE user_id = ?
        """, (password_hash, salt, datetime.now(), user_id))
        
        conn.commit()
        
        return jsonify({"success": True, "message": "Password reset successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/roles")
def get_roles():
    """Get all roles"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        
        cursor = conn.cursor()
        cursor.execute("""
            SELECT role_id, role_name, description, permissions, created_at
            FROM security_roles
            ORDER BY role_name
        """)
        
        roles = []
        for row in cursor.fetchall():
            permissions = json.loads(row[3]) if row[3] else []
            roles.append({
                'role_id': row[0],
                'role_name': row[1],
                'description': row[2],
                'permissions': permissions,
                'created_at': row[4]
            })
        
        return jsonify(roles)
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/roles", methods=["POST"])
def create_role():
    """Create a new role"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        role_name = request.form.get('role_name')
        description = request.form.get('description', '')
        permissions = request.form.get('permissions', '[]')
        
        if not role_name:
            return jsonify({"success": False, "message": "Role name is required"}), 400
        
        # Validate permissions JSON
        try:
            permissions_list = json.loads(permissions)
        except json.JSONDecodeError:
            return jsonify({"success": False, "message": "Invalid permissions format"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO security_roles (role_name, description, permissions)
            VALUES (?, ?, ?)
        """, (role_name, description, json.dumps(permissions_list)))
        
        conn.commit()
        
        return jsonify({"success": True, "message": "Role created successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/roles/<int:role_id>", methods=["PUT"])
def update_role(role_id):
    """Update existing role"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        role_name = request.form.get('role_name')
        description = request.form.get('description', '')
        permissions = request.form.get('permissions', '[]')
        
        if not role_name:
            return jsonify({"success": False, "message": "Role name is required"}), 400
        
        # Validate permissions JSON
        try:
            permissions_list = json.loads(permissions)
        except json.JSONDecodeError:
            return jsonify({"success": False, "message": "Invalid permissions format"}), 400
            
        security = DatabaseSecurity(session['db_path'], db_type=session.get('db_type', 'sqlite'))
        success = security.update_role(role_id, role_name, description, permissions_list)
        security.close()
        
        if success:
            return jsonify({"success": True, "message": "Role updated successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to update role or role not found"}), 404
            
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/roles/<int:role_id>", methods=["DELETE"])
def delete_role(role_id):
    """Delete role"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        security = DatabaseSecurity(session['db_path'], db_type=session.get('db_type', 'sqlite'))
        success = security.delete_role(role_id)
        security.close()
        
        if success:
            return jsonify({"success": True, "message": "Role deleted successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to delete role. It may be a system role or in use by users."}), 400
            
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500


@security_bp.route("/audit-log")
def get_audit_log():
    """Get audit log entries"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        security = DatabaseSecurity(session['db_path'])
        
        # Get recent audit log entries
        cursor = conn.cursor()
        cursor.execute("""
            SELECT al.action, al.table_name, al.record_id, al.timestamp, su.username
            FROM security_audit_log al
            LEFT JOIN security_users su ON al.user_id = su.user_id
            ORDER BY al.timestamp DESC
            LIMIT 50
        """)
        
        logs = []
        for row in cursor.fetchall():
            logs.append({
                'action': row[0],
                'table_name': row[1],
                'record_id': row[2],
                'timestamp': row[3],
                'username': row[4] or 'System'
            })
        
        return jsonify(logs)
        
    except Exception as e:
        import traceback
        print(f"[ERROR] Audit log error: {str(e)}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/audit-log/export")
def export_audit_log():
    """Export audit log as CSV"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        security = DatabaseSecurity(session['db_path'])
        
        # First check if security tables exist
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='security_audit_log'")
        audit_table_exists = cursor.fetchone()
        
        if not audit_table_exists:
            # Return empty list if audit table doesn't exist
            return jsonify([])
        
        # Get all audit log entries
        cursor.execute("""
            SELECT al.action, al.table_name, al.record_id, al.timestamp, su.username,
                   al.old_values, al.new_values, al.ip_address, al.user_agent
            FROM security_audit_log al
            LEFT JOIN security_users su ON al.user_id = su.user_id
            ORDER BY al.timestamp DESC
        """)
        
        # Create CSV content
        import csv
        import io
        
        output = io.StringIO()
        writer = csv.writer(output)
        
        # Write header
        writer.writerow(['Action', 'Table', 'Record ID', 'Timestamp', 'User', 'IP Address', 'User Agent'])
        
        # Write data
        for row in cursor.fetchall():
            writer.writerow([row[0], row[1], row[2], row[3], row[4] or 'System', row[7], row[8]])
        
        output.seek(0)
        
        from flask import Response
        return Response(
            output.getvalue(),
            mimetype='text/csv',
            headers={'Content-Disposition': f'attachment; filename=audit_log_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'}
        )
        
    except Exception as e:
        import traceback
        print(f"[ERROR] Audit log error: {str(e)}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/backups")
def get_backups():
    """Get backup history"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        
        # First check if security_backup_log table exists
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='security_backup_log'")
        backup_table_exists = cursor.fetchone()
        
        if not backup_table_exists:
            # Return empty list if backup table doesn't exist
            return jsonify([])
        
        # Check if security_users table exists for the JOIN
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='security_users'")
        users_table_exists = cursor.fetchone()
        
        if users_table_exists:
            # Use JOIN if users table exists
            cursor.execute("""
                SELECT sb.backup_path, sb.backup_type, sb.file_size, sb.created_at, su.username
                FROM security_backup_log sb
                LEFT JOIN security_users su ON sb.created_by = su.user_id
                ORDER BY sb.created_at DESC
            """)
        else:
            # Simple query without JOIN if users table doesn't exist
            cursor.execute("""
                SELECT backup_path, backup_type, file_size, created_at, created_by
                FROM security_backup_log
                ORDER BY created_at DESC
            """)
        
        backups = []
        for row in cursor.fetchall():
            backup_filename = row[0].split('/')[-1] if row[0] else 'Unknown'
            
            # Handle both query types (with and without JOIN)
            if users_table_exists:
                # Query with JOIN: backup_path, backup_type, file_size, created_at, username
                created_by = row[4] if row[4] else 'System'
                username = row[4] if row[4] else 'System'
            else:
                # Query without JOIN: backup_path, backup_type, file_size, created_at, created_by
                created_by = row[4] if row[4] else 'System'
                username = 'User ' + str(row[4]) if row[4] else 'System'
            
            backups.append({
                'backup_filename': backup_filename,
                'backup_path': row[0],
                'backup_type': row[1],
                'file_size': row[2],
                'created_at': row[3],
                'created_by': username
            })
        
        return jsonify(backups)
        
    except Exception as e:
        import traceback
        print(f"[ERROR] Backup listing error: {str(e)}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/backup", methods=["POST"])
def create_backup():
    """Create a database backup"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        user_id = session.get('user_id')
        db_path = session['db_path']
        
        # Check if database file exists
        if not os.path.exists(db_path):
            return jsonify({"success": False, "message": f"Database file not found: {db_path}"}), 400
        
        security = DatabaseSecurity(db_path)
        
        # Add debug logging
        print(f"[DEBUG] Creating backup for database: {db_path}")
        print(f"[DEBUG] User ID: {user_id}")
        
        if security.create_backup('manual', user_id):
            return jsonify({"success": True, "message": "Backup created successfully"})
        else:
            return jsonify({"success": False, "message": "Failed to create backup - check server logs for details"}), 500
        
    except Exception as e:
        import traceback
        error_msg = f"Backup creation error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": error_msg}), 500

# Note: Backup scheduling route removed - use standalone scheduler

# Note: Backup status route removed - use standalone scheduler

@security_bp.route("/authenticate", methods=["POST"])
def authenticate_user():
    """Authenticate user for login"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        username = request.json.get('username')
        password = request.json.get('password')
        
        if not username or not password:
            return jsonify({"success": False, "message": "Username and password required"}), 400
        
        security = DatabaseSecurity(session['db_path'])
        user_data = security.authenticate_user(username, password)
        
        if user_data:
            session['user_id'] = user_data['user_id']
            session['username'] = user_data['username']
            session['role'] = user_data['role']
            session['is_active'] = user_data['is_active']
            
            return jsonify({
                "success": True,
                "user": user_data
            })
        else:
            return jsonify({"success": False, "message": "Invalid credentials"}), 401
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/logout", methods=["POST"])
def logout_user():
    """Logout user"""
    try:
        # Clear session
        session.pop('user_id', None)
        session.pop('username', None)
        session.pop('role', None)
        session.pop('is_active', None)
        
        return jsonify({"success": True, "message": "Logged out successfully"})
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/current-user")
def get_current_user():
    """Get current authenticated user"""
    try:
        if 'user_id' not in session:
            return jsonify({"success": False, "message": "Not authenticated"}), 401
        
        return jsonify({
            "success": True,
            "user": {
                "user_id": session['user_id'],
                "username": session['username'],
                "role": session['role'],
                "is_active": session['is_active']
            }
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

@security_bp.route("/database/init", methods=["POST"])
def init_database():
    """Initialize new secure database"""
    try:
        if 'user_id' not in session:
            return jsonify({"success": False, "message": "Not authenticated"}), 401
        
        if session.get('role') != 'admin':
            return jsonify({"success": False, "message": "Admin access required"}), 403
        
        # Handle both FormData and JSON data
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form
            # Convert FormData to dict for easier access
            data = {key: value for key, value in data.items()}
        
        db_path = data.get('db_path', 'CAN2Database_v2 - Copy.db')
        
        # Add debug logging
        print(f"[DEBUG] Initializing database: {db_path}")
        print(f"[DEBUG] Request data type: {'JSON' if request.is_json else 'FormData'}")
        
        # Check if database already exists
        if os.path.exists(db_path):
            return jsonify({
                "success": False,
                "message": f"Database file already exists: {db_path}. Choose a different path or delete the existing database first."
            })
        
        # Check if parent directory exists
        parent_dir = os.path.dirname(db_path)
        if not os.path.exists(parent_dir):
            return jsonify({
                "success": False,
                "message": f"Parent directory does not exist: {parent_dir}"
            })
        
        # Create new secure database
        success = create_secure_database(db_path)
        
        if success:
            return jsonify({
                "success": True,
                "message": f"Secure database initialized: {db_path}",
                "db_path": db_path
            })
        else:
            return jsonify({
                "success": False,
                "message": "Failed to initialize secure database"
            }), 500
            
    except Exception as e:
        import traceback
        error_msg = f"Database initialization error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": error_msg}), 500

@security_bp.route("/database/migrate", methods=["POST"])
def migrate_database():
    """Add security to existing database"""
    try:
        if 'user_id' not in session:
            return jsonify({"success": False, "message": "Not authenticated"}), 401
        
        if session.get('role') != 'admin':
            return jsonify({"success": False, "message": "Admin access required"}), 403
        
        # Handle both FormData and JSON data
        if request.is_json:
            data = request.get_json()
        else:
            data = request.form
            # Convert FormData to dict for easier access
            data = {key: value for key, value in data.items()}
        
        db_path = data.get('db_path', 'CAN2Database_v2 - Copy.db')
        
        # Add debug logging
        print(f"[DEBUG] Migrating database: {db_path}")
        print(f"[DEBUG] Request data type: {'JSON' if request.is_json else 'FormData'}")
        
        # Check if database exists
        if not os.path.exists(db_path):
            return jsonify({"success": False, "message": f"Database file not found: {db_path}"}), 400
        
        # Check if security already exists
        conn = DatabaseManagerFlask.get_connection(db_path, 'sqlite')
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
        existing_security_tables = cursor.fetchall()
        
        if len(existing_security_tables) > 0:
            return jsonify({
                "success": False,
                "message": "Database already has security features. No migration needed.",
                "existing_tables": len(existing_security_tables)
            })
        
        # Migrate existing database
        success = migrate_existing_database(db_path)
        
        if success:
            return jsonify({
                "success": True,
                "message": f"Security features added to: {db_path}",
                "db_path": db_path
            })
        else:
            return jsonify({
                "success": False,
                "message": "Failed to migrate database - check server logs for details"
            }), 500
            
    except Exception as e:
        import traceback
        error_msg = f"Database migration error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": error_msg}), 500

@security_bp.route("/database/status")
def database_security_status():
    """Check if database has security features"""
    try:
        if 'db_path' not in session:
            return jsonify({"success": False, "message": "Database not connected"}), 400
        
        conn = DatabaseManagerFlask.get_connection(session['db_path'], session.get('db_type', 'sqlite'))
        security = DatabaseSecurity(session['db_path'])
        
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
        security_tables = cursor.fetchall()
        
        has_security = len(security_tables) > 0
        
        # Get table names for user feedback
        table_names = [table[0] for table in security_tables]
        
        return jsonify({
            "success": True,
            "has_security": has_security,
            "message": "Database already has security features" if has_security else "Database lacks security features",
            "security_tables": table_names,
            "table_count": len(security_tables)
        })
        
    except Exception as e:
        import traceback
        error_msg = f"Security status check error: {str(e)}"
        print(f"[ERROR] {error_msg}")
        print(f"[ERROR] Traceback: {traceback.format_exc()}")
        return jsonify({"success": False, "message": error_msg}), 500
from flask import Blueprint, jsonify, render_template, request, session, current_app, g
from database.db_manager_flask import DatabaseManagerFlask
from database.excel_import import ExcelImportManager
from database.security import DatabaseSecurity
import os
from werkzeug.utils import secure_filename

excel_import_bp = Blueprint("excel_import", __name__)

def _get_db_connection():
    """Helper to get database connection for both SQLite and MySQL/MariaDB."""
    db_type = session.get('db_type', 'sqlite')
    
    if db_type in ('mysql', 'mariadb'):
        db_params = session.get('db_params')
        if not db_params:
            return None, db_type, "Database not connected (no MySQL params in session)"
        conn = DatabaseManagerFlask.get_connection(db_params, db_type)
    else:
        db_path = session.get('db_path')
        if not db_path:
            return None, db_type, "Database not connected (no db_path in session)"
        conn = DatabaseManagerFlask.get_connection(db_path, db_type)
    
    return conn, db_type, None

@excel_import_bp.route("/import")
def import_page(): 
    return render_template("database/import_excel.html")

@excel_import_bp.route("/import/preview", methods=["POST"])
def preview_excel():
    """Preview Excel file before import"""
    try:
        if 'file' not in request.files:
            return jsonify({"success": False, "message": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"success": False, "message": "No file selected"}), 400
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({"success": False, "message": "Invalid file format. Please upload Excel file (.xlsx, .xls)"}), 400
        
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"temp_preview_{filename}")
        file.save(temp_path)
        
        # Get database connection (supports both SQLite and MySQL)
        conn, db_type, error = _get_db_connection()
        if error:
            return jsonify({"success": False, "message": error}), 400
        
        # Import manager for preview
        user_role = session.get('role')
        if user_role not in ['admin', 'researcher']:
            return jsonify({
                "success": False, 
                "message": f"Access denied. Role '{user_role}' does not have import permissions. Only admin and researcher roles can preview and import data."
            }), 403
        
        import_manager = ExcelImportManager(conn, db_type, user_id=session.get('user_id'))
        result = import_manager.preview_excel_file(temp_path)
        import_manager.close()
        
        # Clean up temp file
        try:
            os.remove(temp_path)
        except:
            pass
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Preview failed: {str(e)}"}), 500

@excel_import_bp.route("/import/execute", methods=["POST"])
def execute_import():
    """Execute Excel file import"""
    try:
        if 'file' not in request.files:
            return jsonify({"success": False, "message": "No file provided"}), 400
        
        file = request.files['file']
        if file.filename == '':
            return jsonify({"success": False, "message": "No file selected"}), 400
        
        if not file.filename.endswith(('.xlsx', '.xls')):
            return jsonify({"success": False, "message": "Invalid file format. Please upload Excel file (.xlsx, .xls)"}), 400
        
        # Get database connection (supports both SQLite and MySQL)
        conn, db_type, error = _get_db_connection()
        if error:
            return jsonify({"success": False, "message": error}), 400
        
        # Save uploaded file temporarily
        filename = secure_filename(file.filename)
        temp_path = os.path.join(current_app.config['UPLOAD_FOLDER'], f"temp_import_{filename}")
        file.save(temp_path)
        
        # Get import mode
        import_mode = request.form.get('import_mode', 'skip')
        
        # Get custom mappings and exclusions
        import json
        custom_mappings = json.loads(request.form.get('custom_mappings', '{}'))
        excluded_columns = json.loads(request.form.get('excluded_columns', '{}'))
        
        print(f"[DEBUG] Import mode: {import_mode}")
        print(f"[DEBUG] Custom mappings: {custom_mappings}")
        print(f"[DEBUG] Excluded columns: {excluded_columns}")
        
        # Get current user from session
        user_id = session.get('user_id')
        user_role = session.get('role')
        if not user_id:
            return jsonify({"success": False, "message": "User not authenticated"}), 401
        
        # Check user permissions for import
        if user_role not in ['admin', 'researcher']:
            return jsonify({
                "success": False, 
                "message": f"Access denied. Role '{user_role}' does not have import permissions. Only admin and researcher roles can import data."
            }), 403
        
        # Import file with security integration
        import_manager = ExcelImportManager(conn, db_type, user_id=session.get('user_id'))
        result = import_manager.import_excel_file(
            temp_path, 
            import_mode=import_mode,
            custom_mappings=custom_mappings,
            excluded_columns=excluded_columns
        )
        import_manager.close()
        
        # Clean up temp file
        try:
            os.remove(temp_path)
        except:
            pass
        
        return jsonify(result)
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Import failed: {str(e)}"}), 500

@excel_import_bp.route("/tables", methods=["GET"])
def get_tables():
    """Get available database tables and their columns"""
    try:
        conn, db_type, error = _get_db_connection()
        if error:
            return jsonify({"success": False, "message": error}), 400
        
        cursor = conn.cursor()
        
        # Get tables based on database type
        if db_type in ('mysql', 'mariadb'):
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("""
                SELECT name FROM sqlite_master 
                WHERE type='table' AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'recycle_bin'
                ORDER BY name
            """)
            tables = [row[0] for row in cursor.fetchall()]
        
        # Get columns for each table, excluding FK columns and primary keys
        table_info = {}
        for table in tables:
            if db_type in ('mysql', 'mariadb'):
                cursor.execute(f"DESCRIBE `{table}`")
                all_columns = cursor.fetchall()
                # Get foreign key information
                cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}' 
                    AND REFERENCED_TABLE_NAME IS NOT NULL
                """)
                fk_columns = {row[0] for row in cursor.fetchall()}
                # Get primary key information
                cursor.execute(f"""
                    SELECT COLUMN_NAME 
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE 
                    WHERE TABLE_SCHEMA = DATABASE() 
                    AND TABLE_NAME = '{table}' 
                    AND CONSTRAINT_NAME = 'PRIMARY'
                """)
                pk_columns = {row[0] for row in cursor.fetchall()}
                
                # Filter out FK columns and primary keys
                excluded_columns = fk_columns.union(pk_columns)
                columns = [f"{col[0]} {col[1]}" for col in all_columns if col[0] not in excluded_columns]
            else:
                cursor.execute(f"PRAGMA table_info({table})")
                all_columns = cursor.fetchall()
                # Get foreign key information
                cursor.execute(f"PRAGMA foreign_key_list({table})")
                fk_columns = {row[3] for row in cursor.fetchall()}  # from column is at index 3
                # Get primary key information (pk = 1 indicates primary key)
                pk_columns = {col[1] for col in all_columns if col[5] == 1}  # pk flag is at index 5
                
                # Filter out FK columns and primary keys
                excluded_columns = fk_columns.union(pk_columns)
                columns = [f"{col[1]} {col[2]}" for col in all_columns if col[1] not in excluded_columns]
            table_info[table] = columns
        
        cursor.close()
        
        return jsonify({
            "success": True,
            "tables": table_info
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Failed to get tables: {str(e)}"}), 500
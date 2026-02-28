"""
Main application routes
"""
from flask import Blueprint, render_template, session, redirect, url_for, jsonify, request
from database.db_manager_flask import DatabaseManagerFlask
from functools import wraps

main_bp = Blueprint('main', __name__)


def require_db_connection(f):
    """Decorator to ensure database connection exists"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if not session.get('db_connected'):
            if request.is_json:
                return jsonify({'success': False, 'message': 'Not connected to database'}), 401
            return redirect(url_for('auth.connect'))
        return f(*args, **kwargs)
    return decorated_function

def authentication_required(f):
    """Decorator to require user authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        # Check if database is connected
        if not session.get('db_connected'):
            if request.is_json:
                return jsonify({'success': False, 'message': 'Database connection required'}), 401
            return redirect(url_for('auth.connect'))
        
        # Check if user is authenticated
        if not session.get('authenticated'):
            if request.is_json:
                return jsonify({'success': False, 'message': 'Authentication required'}), 401
            return redirect(url_for('auth.login'))
        
        return f(*args, **kwargs)
    return decorated_function


@main_bp.route('/')
@main_bp.route('/main')
@main_bp.route('/main/dashboard')
@require_db_connection
@authentication_required
def dashboard():
    """Main dashboard view"""
    try:
        # Get database statistics
        db_type = session.get('db_type')
        db_path = session.get('db_path') if db_type == 'sqlite' else session.get('db_params')
        
        conn = DatabaseManagerFlask.get_connection(db_path, db_type)
        cursor = conn.cursor()
        
        stats = {
            'db_name': session.get('db_name'),
            'db_type': db_type,
            'tables': []
        }
        
        # Get table information
        if db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f'SELECT COUNT(*) FROM "{table_name}"')
                count = cursor.fetchone()[0]
                stats['tables'].append({
                    'name': table_name,
                    'rows': count
                })
        else:  # MySQL
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f'SELECT COUNT(*) FROM `{table_name}`')
                count = cursor.fetchone()[0]
                stats['tables'].append({
                    'name': table_name,
                    'rows': count
                })
        
        return render_template('main/dashboard.html', stats=stats)
        
    except Exception as e:
        print(f"[ERROR] Dashboard connection error: {e}")
        # If connection fails, clear the connection status to force manual reconnect
        session.pop('db_connected', None)
        session.pop('db_params', None)
        session.pop('db_path', None)
        return redirect(url_for('auth.connect'))



@main_bp.route('/main/schema')
@require_db_connection
@authentication_required
def schema():
    """Database schema viewer"""
    try:
        db_type = session.get('db_type')
        db_path = session.get('db_path') if db_type == 'sqlite' else session.get('db_params')
        
        conn = DatabaseManagerFlask.get_connection(db_path, db_type)
        cursor = conn.cursor()
        
        schema_data = {}
        
        if db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns = cursor.fetchall()
                schema_data[table_name] = [
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
            cursor.execute("SHOW TABLES")
            tables = cursor.fetchall()
            
            for (table_name,) in tables:
                cursor.execute(f'DESCRIBE `{table_name}`')
                columns = cursor.fetchall()
                schema_data[table_name] = [
                    {
                        'name': col[0],
                        'type': col[1],
                        'null': col[2] == 'YES',
                        'key': col[3],
                        'default': col[4]
                    }
                    for col in columns
                ]
        
        return render_template('main/schema.html', schema=schema_data)
        
    except Exception as e:
        print(f"[ERROR] Schema view connection error: {e}")
        # Clear connection status to force manual reconnect
        session.pop('db_connected', None)
        session.pop('db_params', None)
        session.pop('db_path', None)
        return redirect(url_for('auth.connect'))


@main_bp.route('/bat-identification')
def bat_identification():
    """Bat species identification page"""
    return render_template('bat_identification.html')
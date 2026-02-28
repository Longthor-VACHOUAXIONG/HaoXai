"""
Authentication routes for database connection management
"""
from flask import Blueprint, render_template, request, session, redirect, url_for, jsonify, flash, current_app
from functools import wraps
from database.security import DatabaseSecurity
from database.db_manager_flask import DatabaseManagerFlask
from database.secure_init import create_secure_database, migrate_existing_database
import os

auth_bp = Blueprint('auth', __name__)


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """User login page"""
    if request.method == 'POST':
        username = request.form.get('username')
        password = request.form.get('password')
        
        if not username or not password:
            return render_template('auth/login.html', error='Username and password are required')
        
        try:
            # Check if database is connected
            if not session.get('db_connected'):
                return redirect(url_for('auth.connect'))
                
            # Get database type from session
            db_type = session.get('db_type', 'sqlite')
            
            # Get database path or params
            db_path_or_params = session.get('db_path') if db_type == 'sqlite' else session.get('db_params')
            
            if not db_path_or_params:
                session.pop('db_connected', None)
                return redirect(url_for('auth.connect'))
            
            # Authenticate user with proper database type
            security = DatabaseSecurity(db_path_or_params, db_type=db_type)
            user_data = security.authenticate_user(username, password)
            
            if user_data:
                # Set session
                session['user_id'] = user_data['user_id']
                session['username'] = user_data['username']
                session['role'] = user_data['role']
                session['is_active'] = user_data['is_active']
                session['authenticated'] = True
                
                # Set session permanence based on user preference
                if request.form.get('rememberMe'):
                    session.permanent = True
                
                # Redirect based on role
                if user_data['role'] == 'admin':
                    return redirect(url_for('admin.dashboard'))
                else:
                    return redirect(url_for('main.dashboard'))
            else:
                return render_template('auth/login.html', error='Invalid username or password')
                
        except Exception as e:
            return render_template('auth/login.html', error=f'Login error: {str(e)}')
    
    return render_template('auth/login.html')


@auth_bp.route('/logout')
def logout():
    """Logout user with better UX"""
    # Store logout message for display
    username = session.get('username', 'User')
    
    # Clear session
    session.clear()
    
    # Flash message for better UX
    from flask import flash
    flash(f'Goodbye, {username}! You have been successfully logged out.', 'info')
    
    return redirect(url_for('auth.login'))


@auth_bp.route('/connect', methods=['GET', 'POST'])
def connect():
    """Database connection page"""
    if request.method == 'POST':
        data = request.get_json() if request.is_json else request.form
        connection_type = data.get('connection_type')
        
        try:
            if connection_type == 'sqlite':
                db_path = data.get('db_path')
                if not db_path:
                    return jsonify({'success': False, 'message': 'Database path is required'}), 400
                
                # Validate path
                if not os.path.exists(db_path):
                    return jsonify({'success': False, 'message': 'Database file does not exist'}), 400
                
                # Test connection
                conn = DatabaseManagerFlask.get_connection(db_path, 'sqlite')
                if conn:
                    session['db_path'] = db_path
                    session['db_type'] = 'sqlite'
                    session['db_name'] = os.path.basename(db_path)
                    session['db_connected'] = True
                    
                    # Set session persistence based on user preference
                    session.permanent = bool(data.get('remember', False))
                    
                    # Store instance ID and remember preference for restart-aware session management
                    session['app_instance_id'] = current_app.config['APP_INSTANCE_ID']
                    session['remember_connection'] = bool(data.get('remember', False))
                    
                    # Check if database has security features and set up if missing
                    security = DatabaseSecurity(db_path)
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
                    security_tables = cursor.fetchall()
                    
                    has_security = len(security_tables) > 0
                    
                    if not has_security:
                        print(f"[DEBUG] Database {db_path} lacks security features. Setting up security...")
                        success = migrate_existing_database(db_path)
                        if success:
                            print(f"[DEBUG] Security features added to {db_path}")
                        else:
                            print(f"[DEBUG] Failed to add security features to {db_path}")
                    else:
                        print(f"[DEBUG] Database {db_path} already has security features")
                    
                    return jsonify({
                        'success': True,
                        'message': 'Connected to SQLite database',
                        'redirect': url_for('main.dashboard')
                    })
                
            elif connection_type == 'mysql':
                # Validate required fields
                required = ['host', 'user', 'password', 'database']
                missing = [f for f in required if not data.get(f)]
                if missing:
                    return jsonify({
                        'success': False,
                        'message': f'Missing required fields: {", ".join(missing)}'
                    }), 400
                
                db_params = {
                    'host': data.get('host'),
                    'user': data.get('user'),
                    'password': data.get('password'),
                    'database': data.get('database'),
                    'port': int(data.get('port', 3306))
                }
                
                # Test connection
                conn = DatabaseManagerFlask.get_connection(db_params, 'mysql')
                if conn:
                    session['db_params'] = db_params
                    session['db_type'] = 'mysql'
                    session['db_name'] = db_params['database']
                    session['db_connected'] = True
                    
                    # Set session persistence based on user preference
                    session.permanent = bool(data.get('remember', False))
                    
                    # Store instance ID and remember preference for restart-aware session management
                    session['app_instance_id'] = current_app.config['APP_INSTANCE_ID']
                    session['remember_connection'] = bool(data.get('remember', False))
                    
                    # Check if database has security features and set up if missing
                    # For MySQL, we need to check security tables differently
                    cursor = conn.cursor()
                    cursor.execute("""
                        SELECT COUNT(*) FROM information_schema.tables 
                        WHERE table_schema = DATABASE() 
                        AND table_name LIKE 'security_%'
                    """)
                    security_count = cursor.fetchone()[0]
                    
                    has_security = security_count > 0
                    
                    if not has_security:
                        print(f"[DEBUG] MySQL database {db_params['database']} lacks security features. Setting up security...")
                        # Use DatabaseSecurity to initialize tables - it handles both SQLite and MySQL/MariaDB
                        try:
                            security = DatabaseSecurity(db_params, db_type='mysql')
                            print(f"[DEBUG] Security features verified/added to {db_params['database']}")
                        except Exception as sec_e:
                            print(f"[ERROR] Failed to initialize security for MySQL: {sec_e}")
                    else:
                        print(f"[DEBUG] MySQL database {db_params['database']} already has security features")
                    
                    return jsonify({
                        'success': True,
                        'message': 'Connected to MySQL database',
                        'redirect': url_for('main.dashboard')
                    })
            
            return jsonify({'success': False, 'message': 'Invalid connection type'}), 400
            
        except Exception as e:
            return jsonify({'success': False, 'message': str(e)}), 500
    
    return render_template('auth/connect.html')


@auth_bp.route('/disconnect', methods=['POST'])
def disconnect():
    """Disconnect from current database"""
    session.clear()
    return jsonify({'success': True, 'message': 'Disconnected successfully'})


@auth_bp.route('/status')
def status():
    """Get current connection status"""
    if session.get('db_connected'):
        response_data = {
            'connected': True,
            'db_type': session.get('db_type'),
            'db_name': session.get('db_name'),
        }
        
        # Include db_path for SQLite
        if session.get('db_type') == 'sqlite':
            response_data['db_path'] = session.get('db_path')
        
        # Include security status
        try:
            if session.get('db_type') == 'sqlite':
                security = DatabaseSecurity(session['db_path'])
                cursor = security.conn.cursor()
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'security_%'")
                security_tables = cursor.fetchall()
                
                response_data['has_security'] = len(security_tables) > 0
                response_data['security_tables'] = [table[0] for table in security_tables]
            elif session.get('db_type') == 'mysql':
                # For MySQL, check security tables
                conn = DatabaseManagerFlask.get_connection(session['db_params'], 'mysql')
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT COUNT(*) FROM information_schema.tables 
                    WHERE table_schema = DATABASE() 
                    AND table_name LIKE 'security_%'
                """)
                security_count = cursor.fetchone()[0]
                
                response_data['has_security'] = security_count > 0
                
        except Exception as e:
            response_data['security_error'] = str(e)
            
        return jsonify(response_data)
    else:
        return jsonify({'connected': False})
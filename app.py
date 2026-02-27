from flask import Flask, render_template, session, redirect, url_for, send_from_directory
from flask_session import Session
from flask_socketio import SocketIO, emit
from datetime import timedelta
import os
import signal
import sys
import webbrowser
import threading
import time
import uuid
from config import Config
from routes import register_blueprints
from database.db_manager_flask import init_db, DatabaseManagerFlask
from auto_session_cleanup import start_auto_cleanup, stop_auto_cleanup

# Application version
__version__ = "1.0.0"


def signal_handler(sig, frame):
    """Handle Ctrl+C gracefully"""
    print('\n=== Shutting down HaoXai server gracefully... ===')
    stop_auto_cleanup()
    sys.exit(0)


# Register signal handler
signal.signal(signal.SIGINT, signal_handler)


def create_app(config_class=Config):
    """Application factory pattern for creating Flask app"""
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Set application version
    app.config['VERSION'] = __version__
    
    # Generate a unique ID for this server instance
    # This helps reset non-persistent sessions across server restarts
    app.config['APP_INSTANCE_ID'] = str(uuid.uuid4())
    
    # Initialize session management
    app.config['SESSION_TYPE'] = 'filesystem'
    app.config['PERMANENT_SESSION_LIFETIME'] = timedelta(hours=2)
    Session(app)
    
    # Initialize SocketIO for real-time updates
    socketio = SocketIO(app, cors_allowed_origins="*", async_mode='threading')
    
    # Initialize database
    init_db(app)
    
    # Set socketio instance for the database manager
    DatabaseManagerFlask.set_socketio(socketio)
    
    # Register blueprints
    register_blueprints(app)
    
    # Store socketio instance in app for access in routes
    app.socketio = socketio
    
    # Session instance validation
    @app.before_request
    def validate_session_instance():
        """Ensure connection session is reset if program was restarted and 'remember' wasn't set"""
        # We only care about this if a database connection is active
        if session.get('db_connected'):
            session_instance = session.get('app_instance_id')
            current_instance = app.config['APP_INSTANCE_ID']
            
            # If the instance IDs don't match, it means the server has restarted
            if session_instance != current_instance:
                # If 'remember_connection' wasn't checked, clear the connection state
                if not session.get('remember_connection'):
                    print(f"[DEBUG] Instance ID mismatch ({session_instance} != {current_instance}). Resetting connection state.")
                    session.pop('db_connected', None)
                    session.pop('db_path', None)
                    session.pop('db_params', None)
                    session.pop('db_type', None)
                    session.pop('db_name', None)
                    session.pop('authenticated', None)  # Force re-login too
                else:
                    # If we should remember, update the instance ID so we don't check again this session
                    session['app_instance_id'] = current_instance
    
    # Home route
    @app.route('/')
    def index():
        if 'db_connected' not in session:
            return redirect(url_for('auth.connect'))
        if 'authenticated' not in session:
            return redirect(url_for('auth.login'))
        return redirect(url_for('main.dashboard'))
    
    # Favicon route
    @app.route('/favicon.ico')
    def favicon():
        return send_from_directory('icons', 'virodb_new.ico', mimetype='image/x-icon')
    
    # SocketIO event handlers
    @socketio.on('connect')
    def handle_connect():
        """Handle client connection"""
        emit('status', {'msg': 'Connected to ViroDB real-time updates'})
    
    @socketio.on('disconnect')
    def handle_disconnect(*args):
        """Handle client disconnection"""
        print('Client disconnected')
    
    @socketio.on('subscribe_updates')
    def handle_subscribe(data):
        """Handle subscription to real-time updates"""
        emit('status', {'msg': f'Subscribed to {data.get("table", "all")} updates'})
    
    return app, socketio


if __name__ == '__main__':
    app, socketio = create_app()
    print('\n=== HaoXai Server Starting ===')
    print(f'HaoXai System v{__version__} with real-time updates enabled')
    
    # Start automatic session cleanup
    print('Starting automatic session cleanup...')
    start_auto_cleanup(session_folder='flask_session', cleanup_interval_hours=4, max_age_hours=2)
    
    # Auto-open browser in a separate thread
    def open_browser():
        time.sleep(1.5)
        webbrowser.open("http://127.0.0.1:5000")
    
    threading.Thread(target=open_browser, daemon=True).start()
    
    try:
        socketio.run(app, debug=True, host='0.0.0.0', port=5000, use_reloader=False, allow_unsafe_werkzeug=True)
    except KeyboardInterrupt:
        print('\n=== HaoXai server stopped by user ===')
        sys.exit(0)

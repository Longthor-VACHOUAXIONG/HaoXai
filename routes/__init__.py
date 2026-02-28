"""
Blueprint registration for Flask application
"""
from routes.auth import auth_bp
from routes.main import main_bp
from routes.database import database_bp
from routes.query import query_bp
from routes.sequence import sequence_bp
from routes.excel_import import excel_import_bp
from routes.excel_merge import excel_merge_bp
from routes.linking import linking_bp
from routes.auto_linking import auto_linking_bp
from routes.chat import chat_bp
from routes.sample_management import sample_bp
from routes.extraction import extraction_bp
from routes.file_manager import file_manager_bp
from routes.security import security_bp
from routes.admin import admin_bp
from routes.ml import ml_bp
from routes.bat_ml import bat_ml_bp


def register_blueprints(app):
    """Register all application blueprints"""
    app.register_blueprint(auth_bp, url_prefix='/auth')
    app.register_blueprint(main_bp)  # No prefix for direct access to /bat-identification
    app.register_blueprint(database_bp, url_prefix='/database')
    app.register_blueprint(query_bp, url_prefix='/query')
    app.register_blueprint(sequence_bp, url_prefix='/sequence')
    app.register_blueprint(excel_import_bp, url_prefix='/excel')
    app.register_blueprint(excel_merge_bp, url_prefix='/excel_merge')
    app.register_blueprint(linking_bp, url_prefix='/linking')
    app.register_blueprint(auto_linking_bp, url_prefix='/auto-link')
    app.register_blueprint(chat_bp, url_prefix='/chat')
    app.register_blueprint(sample_bp)
    app.register_blueprint(extraction_bp, url_prefix='/extraction')
    app.register_blueprint(file_manager_bp, url_prefix='/file_manager')
    app.register_blueprint(security_bp, url_prefix='/api/security')
    app.register_blueprint(admin_bp, url_prefix='/admin')
    app.register_blueprint(ml_bp, url_prefix='/ml')
    app.register_blueprint(bat_ml_bp, url_prefix='/bat-ml')
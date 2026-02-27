"""
Configuration settings for Flask application
"""

import os
from datetime import timedelta


class Config:
    """Base configuration"""

    # Flask settings
    SECRET_KEY = os.environ.get("SECRET_KEY") or "dev-secret-key-change-in-production"

    # Session settings
    SESSION_TYPE = "filesystem"
    SESSION_FILE_DIR = os.path.join(os.path.dirname(__file__), "flask_session")
    PERMANENT_SESSION_LIFETIME = timedelta(hours=2)
    SESSION_PERMANENT = False

    # Upload settings
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size
    UPLOAD_FOLDER = os.path.join(os.path.dirname(__file__), "uploads")
    ALLOWED_EXTENSIONS = {"xlsx", "xls", "csv", "db", "ab1", "fasta"}

    # Database settings
    DB_SETTINGS_FILE = os.path.join(os.path.dirname(__file__), "db_settings.json")
    
    # Container-aware database path configuration
    if os.environ.get("DB_TYPE") == "mysql":
        DATABASE_PATH = None  # Will use MySQL connection
    else:
        # Use container data directory for SQLite, fallback to local path
        if os.path.exists("/app/data"):
            DATABASE_PATH = "/app/data/CAN2-With-Referent-Key.db"
        else:
            # Use the specific database file from DataExcel folder
            DATABASE_PATH = os.path.join(os.path.dirname(__file__), "DataExcel", "CAN2-With-Referent-Key.db")

    # Analytics settings
    PLOT_DPI = 100
    PLOT_FIGURE_SIZE = (10, 6)

    # OpenAI API settings for Smart AI
    OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY") or ""
    OPENAI_MODEL = os.environ.get("OPENAI_MODEL") or "gpt-3.5-turbo"

    # Ensure required directories exist
    os.makedirs(SESSION_FILE_DIR, exist_ok=True)
    os.makedirs(UPLOAD_FOLDER, exist_ok=True)


class DevelopmentConfig(Config):
    """Development configuration"""

    DEBUG = True
    TESTING = False


class ProductionConfig(Config):
    """Production configuration"""

    DEBUG = False
    TESTING = False
    # In production, ensure SECRET_KEY is set via environment variable
    # Falls back to base config SECRET_KEY if not set
    SECRET_KEY = os.environ.get("SECRET_KEY") or Config.SECRET_KEY


class TestingConfig(Config):
    """Testing configuration"""

    DEBUG = True
    TESTING = True
    WTF_CSRF_ENABLED = False


# Config dictionary for easy access
config = {
    "development": DevelopmentConfig,
    "production": ProductionConfig,
    "testing": TestingConfig,
    "default": DevelopmentConfig,
}

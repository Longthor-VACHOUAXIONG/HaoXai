"""
Admin Routes for HaoXai
Provides admin dashboard and management interfaces
"""

from flask import Blueprint, render_template, request, session, redirect, url_for
from functools import wraps

admin_bp = Blueprint("admin", __name__)

def admin_required(f):
    """Decorator to require admin access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'user_id' not in session:
            return redirect(url_for('auth.login'))
        
        if session.get('role') != 'admin':
            return redirect(url_for('main.dashboard'))
        
        return f(*args, **kwargs)
    return decorated_function

@admin_bp.route("/")
@admin_bp.route("/dashboard")
@admin_required
def dashboard():
    """Admin dashboard"""
    return render_template("admin/dashboard.html")

@admin_bp.route("/security")
@admin_required
def security_dashboard():
    """Security management dashboard"""
    return render_template("admin/security_dashboard.html")

@admin_bp.route("/users")
@admin_required
def user_management():
    """User management interface"""
    return render_template("admin/users.html")

@admin_bp.route("/settings")
@admin_required
def system_settings():
    """System settings interface"""
    from flask import current_app
    version = current_app.config.get('VERSION', '1.0.0')
    return render_template("admin/settings.html", version=version)
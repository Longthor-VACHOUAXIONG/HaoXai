"""
Query execution routes with Python support
"""
from flask import Blueprint, request, jsonify, session, render_template, redirect, url_for, current_app, send_file
from werkzeug.utils import secure_filename
from database.db_manager_flask import DatabaseManagerFlask
import pandas as pd
import re
import json
import sys
import os
import io
import contextlib
import traceback
import base64
from datetime import datetime
import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import numpy as np
import subprocess
import tempfile
import shutil
from pathlib import Path
import uuid

query_bp = Blueprint('query', __name__)


@query_bp.route('/')
def index():
    """Redirect to query editor"""
    return redirect(url_for('query.editor'))


@query_bp.route('/editor')
def editor():
    """SQL query editor page"""
    return render_template('query/editor.html')


import subprocess
import tempfile
import shutil
from pathlib import Path
import uuid

# Global workspace directory for notebook analysis
NOTEBOOK_WORKSPACE = None

def get_notebook_workspace():
    """Get or create the notebook workspace directory"""
    global NOTEBOOK_WORKSPACE
    if NOTEBOOK_WORKSPACE is None:
        # Create workspace in user's data directory or temp
        base_dir = os.path.expanduser('~/.haoxai/notebook_workspace')
        NOTEBOOK_WORKSPACE = base_dir
        os.makedirs(NOTEBOOK_WORKSPACE, exist_ok=True)
        # Create subdirectories
        os.makedirs(os.path.join(NOTEBOOK_WORKSPACE, 'uploads'), exist_ok=True)
        os.makedirs(os.path.join(NOTEBOOK_WORKSPACE, 'outputs'), exist_ok=True)
        os.makedirs(os.path.join(NOTEBOOK_WORKSPACE, 'analysis'), exist_ok=True)
    return NOTEBOOK_WORKSPACE


def get_user_workspace():
    """Get user-specific workspace directory"""
    workspace = get_notebook_workspace()
    # Could make user-specific if needed
    return workspace


@query_bp.route('/notebook/pip-install', methods=['POST'])
def pip_install():
    """Install Python packages via pip"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data received'}), 400
        
        package = data.get('package', '').strip()
        if not package:
            return jsonify({'success': False, 'message': 'No package specified'}), 400
        
        # Security: Validate package name (basic check)
        dangerous_patterns = [';', '&', '|', '>', '<', '`', '$', '&&', '||']
        for pattern in dangerous_patterns:
            if pattern in package:
                return jsonify({
                    'success': False, 
                    'message': f'Package name contains invalid characters: {pattern}'
                }), 400
        
        # Run pip install
        import sys
        pip_cmd = [sys.executable, '-m', 'pip', 'install', package]
        
        result = subprocess.run(
            pip_cmd,
            capture_output=True,
            text=True,
            timeout=120  # 2 minute timeout
        )
        
        if result.returncode == 0:
            return jsonify({
                'success': True,
                'message': f'Successfully installed {package}',
                'output': result.stdout
            })
        else:
            return jsonify({
                'success': False,
                'message': f'Failed to install {package}',
                'error': result.stderr
            }), 400
            
    except subprocess.TimeoutExpired:
        return jsonify({
            'success': False, 
            'message': 'Installation timed out after 2 minutes'
        }), 408
    except Exception as e:
        return jsonify({
            'success': False, 
            'message': f'Installation error: {str(e)}'
        }), 500


@query_bp.route('/notebook/r-install', methods=['POST'])
def r_install():
    """Install R packages via install.packages()"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data received'}), 400
        
        package = data.get('package', '').strip()
        r_path = data.get('r_path', '').strip()
        
        if not package:
            return jsonify({'success': False, 'message': 'No package specified'}), 400
        
        if not r_path:
            return jsonify({'success': False, 'message': 'No Rscript path specified'}), 400
        
        # Debug: Log the received path
        print(f"DEBUG: Received R path: '{r_path}'")
        print(f"DEBUG: Path exists: {os.path.exists(r_path)}")
        
        # Check if Rscript exists at the specified path
        if not os.path.exists(r_path):
            return jsonify({
                'success': False, 
                'message': f'Rscript not found at: {r_path}'
            }), 400
        
        # Security: Validate package name (basic check)
        dangerous_patterns = [';', '&', '|', '>', '<', '`', '$', '&&', '||']
        for pattern in dangerous_patterns:
            if pattern in package:
                return jsonify({
                    'success': False, 
                    'message': f'Package name contains invalid characters: {pattern}'
                }), 400
        
        # Create R script to install package
        r_script = f'''
# Install R package
tryCatch({{
    if (!require("{package}", character.only = TRUE)) {{
        install.packages("{package}", repos = "https://cran.rstudio.com/")
        cat("Successfully installed {package}\\n")
    }} else {{
        cat("Package {package} is already installed\\n")
    }}
}}, error = function(e) {{
    cat("Error installing package:", e$message, "\\n")
    quit(status = 1)
}})
'''
        
        # Write R script to temporary file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False) as f:
            f.write(r_script)
            script_path = f.name
        
        try:
            # Run R script with user-provided path
            import subprocess
            result = subprocess.run(
                [r_path, script_path],
                capture_output=True,
                text=True,
                timeout=300  # 5 minute timeout for R packages
            )
            
            if result.returncode == 0:
                return jsonify({
                    'success': True,
                    'message': f'Successfully installed {package}',
                    'output': result.stdout
                })
            else:
                return jsonify({
                    'success': False,
                    'message': f'Failed to install {package}',
                    'error': result.stderr or result.stdout
                }), 400
                
        except subprocess.TimeoutExpired:
            return jsonify({
                'success': False, 
                'message': 'Installation timed out after 5 minutes'
            }), 408
        finally:
            # Clean up temporary file
            try:
                os.unlink(script_path)
            except:
                pass
                
    except Exception as e:
        return jsonify({
            'success': False, 
            'message': f'Installation error: {str(e)}'
        }), 500


@query_bp.route('/notebook/files', methods=['GET'])
def list_files():
    """List files in the notebook workspace"""
    try:
        workspace = get_user_workspace()
        path = request.args.get('path', '')
        
        print(f"DEBUG: list_files called. path={path}, workspace={workspace}")
        
        # Security: Prevent path traversal
        if path:
            target_path = os.path.normpath(os.path.join(workspace, path))
        else:
            target_path = workspace
            
        print(f"DEBUG: target_path={target_path}, norm_workspace={os.path.normpath(workspace)}")
        
        # Security check with normalized paths
        norm_workspace = os.path.normpath(workspace)
        norm_target = os.path.normpath(target_path)
        
        # On Windows, ensure both use same case and separators
        if os.name == 'nt':
            norm_workspace = norm_workspace.lower()
            norm_target = norm_target.lower()
        
        if not norm_target.startswith(norm_workspace):
            print(f"DEBUG: Path validation failed. target={norm_target}, workspace={norm_workspace}")
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        if not os.path.exists(target_path):
            print(f"DEBUG: Path not found: {target_path}")
            return jsonify({'success': False, 'message': 'Path not found'}), 404
        
        files = []
        dirs = []
        
        for item in os.listdir(target_path):
            item_path = os.path.join(target_path, item)
            rel_path = os.path.relpath(item_path, workspace)
            stat = os.stat(item_path)
            
            item_info = {
                'name': item,
                'path': rel_path,
                'size': stat.st_size,
                'modified': stat.st_mtime
            }
            
            if os.path.isdir(item_path):
                item_info['type'] = 'directory'
                dirs.append(item_info)
            else:
                item_info['type'] = 'file'
                item_info['extension'] = os.path.splitext(item)[1]
                files.append(item_info)
        
        print(f"DEBUG: Found {len(dirs)} dirs and {len(files)} files")
        
        # Sort: directories first, then files
        return jsonify({
            'success': True,
            'path': path,
            'items': dirs + files,
            'workspace_root': workspace
        })
        
    except Exception as e:
        print(f"DEBUG: list_files error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/files/upload', methods=['POST'])
def upload_file():
    """Upload file to notebook workspace"""
    try:
        workspace = get_user_workspace()
        target_dir = request.form.get('path', '')
        
        print(f"DEBUG: Upload requested. path={target_dir}, files={list(request.files.keys())}")
        
        # Security: Validate path
        target_path = os.path.normpath(os.path.join(workspace, target_dir))
        if not target_path.startswith(os.path.normpath(workspace)):
            print(f"DEBUG: Invalid path: {target_path} not in {workspace}")
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        os.makedirs(target_path, exist_ok=True)
        
        if 'file' not in request.files:
            print("DEBUG: No 'file' in request.files")
            return jsonify({'success': False, 'message': 'No file provided'}), 400
        
        file = request.files['file']
        if file.filename == '':
            print("DEBUG: Empty filename")
            return jsonify({'success': False, 'message': 'No file selected'}), 400
        
        # Save file
        filename = secure_filename(file.filename)
        file_path = os.path.join(target_path, filename)
        file.save(file_path)
        
        print(f"DEBUG: File saved: {file_path}")
        
        return jsonify({
            'success': True,
            'message': f'File uploaded: {filename}',
            'file': {
                'name': filename,
                'path': os.path.relpath(file_path, workspace),
                'size': os.path.getsize(file_path)
            }
        })
        
    except Exception as e:
        print(f"DEBUG: Upload error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/files/download/<path:filepath>')
def download_file(filepath):
    """Download file from notebook workspace"""
    try:
        workspace = get_user_workspace()
        file_path = os.path.normpath(os.path.join(workspace, filepath))
        
        # Security check
        if not file_path.startswith(workspace):
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        if not os.path.exists(file_path) or os.path.isdir(file_path):
            return jsonify({'success': False, 'message': 'File not found'}), 404
        
        return send_file(file_path, as_attachment=True)
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/files/delete', methods=['POST'])
def delete_file():
    """Delete file or directory from notebook workspace"""
    try:
        workspace = get_user_workspace()
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data received'}), 400
        
        path = data.get('path', '')
        target_path = os.path.normpath(os.path.join(workspace, path))
        
        # Security check
        if not target_path.startswith(workspace):
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        if target_path == workspace:
            return jsonify({'success': False, 'message': 'Cannot delete workspace root'}), 400
        
        if not os.path.exists(target_path):
            return jsonify({'success': False, 'message': 'Path not found'}), 404
        
        if os.path.isdir(target_path):
            shutil.rmtree(target_path)
        else:
            os.remove(target_path)
        
        return jsonify({
            'success': True,
            'message': f'Deleted: {path}'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/files/mkdir', methods=['POST'])
def create_directory():
    """Create new directory in notebook workspace"""
    try:
        workspace = get_user_workspace()
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data received'}), 400
        
        path = data.get('path', '')
        name = data.get('name', '').strip()
        
        if not name:
            return jsonify({'success': False, 'message': 'Directory name required'}), 400
        
        # Security: Validate name
        if '/' in name or '\\' in name or '..' in name:
            return jsonify({'success': False, 'message': 'Invalid directory name'}), 400
        
        target_path = os.path.normpath(os.path.join(workspace, path, name))
        
        # Security check
        if not target_path.startswith(workspace):
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        os.makedirs(target_path, exist_ok=True)
        
        return jsonify({
            'success': True,
            'message': f'Directory created: {name}',
            'directory': {
                'name': name,
                'path': os.path.relpath(target_path, workspace),
                'type': 'directory'
            }
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/files/read', methods=['POST'])
def read_file_content():
    """Read file content for editing"""
    try:
        workspace = get_user_workspace()
        data = request.get_json()
        
        if not data:
            return jsonify({'success': False, 'message': 'No data received'}), 400
        
        path = data.get('path', '')
        file_path = os.path.normpath(os.path.join(workspace, path))
        
        # Security check
        if not file_path.startswith(workspace):
            return jsonify({'success': False, 'message': 'Invalid path'}), 400
        
        if not os.path.exists(file_path) or os.path.isdir(file_path):
            return jsonify({'success': False, 'message': 'File not found'}), 404
        
        # Check file size (limit to 10MB)
        size = os.path.getsize(file_path)
        if size > 10 * 1024 * 1024:
            return jsonify({'success': False, 'message': 'File too large (>10MB)'}), 400
        
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        
        return jsonify({
            'success': True,
            'content': content,
            'path': path,
            'size': size
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/file/rename', methods=['POST'])
def rename_file_item():
    """Rename a file or folder"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400
        
        old_path = data.get('old_path', '').strip()
        new_name = data.get('new_name', '').strip()
        
        if not old_path or not new_name:
            return jsonify({'success': False, 'message': 'Old path and new name are required'}), 400
        
        # Security: Ensure path is within workspace
        workspace = get_user_workspace()
        old_path_abs = os.path.abspath(os.path.join(workspace, old_path))
        workspace_abs = os.path.abspath(workspace)
        
        if not old_path_abs.startswith(workspace_abs):
            return jsonify({'success': False, 'message': 'Access denied: Path outside workspace'}), 403
        
        if not os.path.exists(old_path_abs):
            return jsonify({'success': False, 'message': 'File or folder does not exist'}), 404
        
        # Construct new path
        parent_dir = os.path.dirname(old_path_abs)
        new_path_abs = os.path.join(parent_dir, new_name)
        
        # Check if new name already exists
        if os.path.exists(new_path_abs):
            return jsonify({'success': False, 'message': 'A file or folder with this name already exists'}), 400
        
        # Rename
        os.rename(old_path_abs, new_path_abs)
        
        return jsonify({
            'success': True,
            'message': f'Successfully renamed to {new_name}',
            'old_path': old_path,
            'new_path': os.path.relpath(new_path_abs, workspace)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/file/delete', methods=['POST'])
def delete_file_item():
    """Delete a file or folder"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400
        
        path = data.get('path', '').strip()
        
        if not path:
            return jsonify({'success': False, 'message': 'Path is required'}), 400
        
        # Security: Ensure path is within workspace
        workspace = get_user_workspace()
        path_abs = os.path.abspath(os.path.join(workspace, path))
        workspace_abs = os.path.abspath(workspace)
        
        if not path_abs.startswith(workspace_abs):
            return jsonify({'success': False, 'message': 'Access denied: Path outside workspace'}), 403
        
        if not os.path.exists(path_abs):
            return jsonify({'success': False, 'message': 'File or folder does not exist'}), 404
        
        # Delete
        if os.path.isdir(path_abs):
            import shutil
            shutil.rmtree(path_abs)
        else:
            os.remove(path_abs)
        
        return jsonify({
            'success': True,
            'message': f'Successfully deleted {path}',
            'deleted_path': path
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/file/move', methods=['POST'])
def move_file_item():
    """Move a file or folder to a new location"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400
        
        source_path = data.get('source_path', '').strip()
        target_dir = data.get('target_dir', '').strip()
        
        if not source_path or not target_dir:
            return jsonify({'success': False, 'message': 'Source path and target directory are required'}), 400
        
        # Security: Ensure paths are within workspace
        workspace = get_user_workspace()
        source_abs = os.path.abspath(os.path.join(workspace, source_path))
        target_abs = os.path.abspath(os.path.join(workspace, target_dir))
        workspace_abs = os.path.abspath(workspace)
        
        if not source_abs.startswith(workspace_abs) or not target_abs.startswith(workspace_abs):
            return jsonify({'success': False, 'message': 'Access denied: Path outside workspace'}), 403
        
        if not os.path.exists(source_abs):
            return jsonify({'success': False, 'message': 'Source file or folder does not exist'}), 404
        
        if not os.path.isdir(target_abs):
            return jsonify({'success': False, 'message': 'Target directory does not exist'}), 404
        
        # Get the filename/dirname
        name = os.path.basename(source_abs)
        new_path_abs = os.path.join(target_abs, name)
        
        # Check if target already exists
        if os.path.exists(new_path_abs):
            return jsonify({'success': False, 'message': 'A file or folder with this name already exists in target directory'}), 400
        
        # Move
        import shutil
        if os.path.isdir(source_abs):
            shutil.move(source_abs, new_path_abs)
        else:
            shutil.move(source_abs, new_path_abs)
        
        return jsonify({
            'success': True,
            'message': f'Successfully moved to {target_dir}',
            'old_path': source_path,
            'new_path': os.path.relpath(new_path_abs, workspace)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/file/copy', methods=['POST'])
def copy_file_item():
    """Copy a file or folder to a new location"""
    try:
        data = request.get_json()
        if not data:
            return jsonify({'success': False, 'message': 'No data provided'}), 400
        
        source_path = data.get('source_path', '').strip()
        target_dir = data.get('target_dir', '').strip()
        
        if not source_path or not target_dir:
            return jsonify({'success': False, 'message': 'Source path and target directory are required'}), 400
        
        # Security: Ensure paths are within workspace
        workspace = get_user_workspace()
        source_abs = os.path.abspath(os.path.join(workspace, source_path))
        target_abs = os.path.abspath(os.path.join(workspace, target_dir))
        workspace_abs = os.path.abspath(workspace)
        
        if not source_abs.startswith(workspace_abs) or not target_abs.startswith(workspace_abs):
            return jsonify({'success': False, 'message': 'Access denied: Path outside workspace'}), 403
        
        if not os.path.exists(source_abs):
            return jsonify({'success': False, 'message': 'Source file or folder does not exist'}), 404
        
        if not os.path.isdir(target_abs):
            return jsonify({'success': False, 'message': 'Target directory does not exist'}), 404
        
        # Get the filename/dirname
        name = os.path.basename(source_abs)
        new_path_abs = os.path.join(target_abs, name)
        
        # Check if target already exists
        if os.path.exists(new_path_abs):
            return jsonify({'success': False, 'message': 'A file or folder with this name already exists in target directory'}), 400
        
        # Copy
        import shutil
        if os.path.isdir(source_abs):
            shutil.copytree(source_abs, new_path_abs)
        else:
            shutil.copy2(source_abs, new_path_abs)
        
        return jsonify({
            'success': True,
            'message': f'Successfully copied to {target_dir}',
            'source_path': source_path,
            'new_path': os.path.relpath(new_path_abs, workspace)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/notebook/workspace-info', methods=['GET'])
def get_workspace_info():
    """Get workspace information"""
    try:
        workspace = get_user_workspace()
        
        # Calculate total size
        total_size = 0
        file_count = 0
        dir_count = 0
        
        for root, dirs, files in os.walk(workspace):
            dir_count += len(dirs)
            for file in files:
                file_path = os.path.join(root, file)
                try:
                    total_size += os.path.getsize(file_path)
                    file_count += 1
                except:
                    pass
        
        return jsonify({
            'success': True,
            'workspace_path': workspace,
            'file_count': file_count,
            'directory_count': dir_count,
            'total_size': total_size,
            'total_size_human': format_size(total_size)
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


def format_size(size_bytes):
    """Format byte size to human readable"""
    if size_bytes == 0:
        return '0 B'
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024:
            return f'{size_bytes:.1f} {unit}'
        size_bytes /= 1024
    return f'{size_bytes:.1f} TB'


@query_bp.route('/notebook')
def notebook():
    """SQL & Python notebook editor page"""
    return render_template('query/notebook_editor.html')


@query_bp.route('/test', methods=['GET'])
def test():
    """Test endpoint to verify frontend can receive JSON"""
    return jsonify({
        'success': True,
        'message': 'Test successful',
        'data': {'test': 'value'}
    })


@query_bp.route('/schema', methods=['GET'])
def get_schema():
    """Get database schema (tables list)"""
    try:
        db_type = session.get('db_type', 'sqlite')
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
        else:
            db_conn = session.get('db_params')
        
        if not db_conn:
            return jsonify({'success': False, 'message': 'No database connection'}), 400
        
        conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
        cursor = conn.cursor()
        
        if db_type == 'sqlite':
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name")
            tables = [row[0] for row in cursor.fetchall()]
        else:
            cursor.execute("SHOW TABLES")
            tables = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({'success': True, 'tables': tables})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/schema/<table_name>', methods=['GET'])
def get_table_columns(table_name):
    """Get columns for a specific table"""
    try:
        db_type = session.get('db_type', 'sqlite')
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
        else:
            db_conn = session.get('db_params')
        
        if not db_conn:
            return jsonify({'success': False, 'message': 'No database connection'}), 400
        
        conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
        cursor = conn.cursor()
        
        if db_type == 'sqlite':
            cursor.execute(f"PRAGMA table_info({table_name})")
            columns = [row[1] for row in cursor.fetchall()]
        else:
            cursor.execute(f"DESCRIBE {table_name}")
            columns = [row[0] for row in cursor.fetchall()]
        
        conn.close()
        return jsonify({'success': True, 'columns': columns})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/execute', methods=['POST'])
def execute():
    """Execute SQL query"""
    try:
        print("DEBUG: Execute endpoint called")
        
        # Get request data
        if not request.is_json:
            print("ERROR: Request is not JSON")
            return jsonify({'success': False, 'message': 'Request must be JSON'}), 400
            
        data = request.get_json()
        if not data:
            print("ERROR: No JSON data received")
            return jsonify({'success': False, 'message': 'No data received'}), 400
            
        query = data.get('query', '').strip()
        print(f"DEBUG: Received query: {query}")
        
        if not query:
            return jsonify({'success': False, 'message': 'No query provided'}), 400
        
        # Get database connection info from session
        db_type = session.get('db_type', 'sqlite')
        print(f"DEBUG: Database type: {db_type}")
        
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
            print(f"DEBUG: SQLite path from session: {db_conn}")
        else:
            db_conn = session.get('db_params')
            print(f"DEBUG: MySQL params: {db_conn}")
            
        if not db_conn:
            print("ERROR: No database connection in session")
            return jsonify({'success': False, 'message': 'Database not connected. Please reconnect to your database.'}), 500
        
        print(f"DEBUG: Attempting to connect to database")
        
        try:
            conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
            cursor = conn.cursor()
            print("DEBUG: Database connection established")
        except Exception as conn_error:
            print(f"ERROR: Database connection failed: {str(conn_error)}")
            return jsonify({
                'success': False, 
                'message': f"Database connection failed: {str(conn_error)}"
            }), 500
        
        # Execute query with better error handling
        try:
            print(f"DEBUG: Executing query: {query[:100]}...")
            
            # Intercept DELETE and DROP for Recycle Bin safety
            delete_match = re.search(r'^\s*DELETE\s+FROM\s+[`"\'\[]?(\w+)[`"\'\]]?(?:\s+WHERE\s+(.+))?', query, re.IGNORECASE | re.DOTALL)
            drop_match = re.search(r'^\s*DROP\s+TABLE\s+(?:IF\s+EXISTS\s+)?[`"\'\[]?(\w+)[`"\'\]]?', query, re.IGNORECASE)
            truncate_match = re.search(r'^\s*TRUNCATE\s+TABLE\s+[`"\'\[]?(\w+)[`"\'\]]?', query, re.IGNORECASE)
            
            if delete_match:
                table_name = delete_match.group(1)
                where_clause = delete_match.group(2)
                print(f"DEBUG: Intercepted DELETE for table {table_name}, where: {where_clause}")
                stats = DatabaseManagerFlask.delete_records(conn, table_name, where_clause, db_type)
                return jsonify({
                    'success': True,
                    'has_results': False,
                    'message': f"Query executed successfully via Recycle Bin. {stats['deleted_records']} row(s) affected.",
                    'affected_rows': stats['deleted_records']
                })
            
            elif drop_match:
                table_name = drop_match.group(1)
                print(f"DEBUG: Intercepted DROP for table {table_name}")
                if table_name.lower() == 'recyclebin':
                    return jsonify({'success': False, 'message': "Cannot drop RecycleBin table"}), 400
                
                DatabaseManagerFlask.drop_table(conn, table_name, db_type)
                return jsonify({
                    'success': True,
                    'has_results': False,
                    'message': f"Table '{table_name}' moved to Recycle Bin and dropped successfully.",
                    'affected_rows': 0
                })

            elif truncate_match:
                table_name = truncate_match.group(1)
                print(f"DEBUG: Intercepted TRUNCATE for table {table_name}")
                # For safety, treat TRUNCATE as DELETE FROM without WHERE
                stats = DatabaseManagerFlask.delete_records(conn, table_name, None, db_type)
                return jsonify({
                    'success': True,
                    'has_results': False,
                    'message': f"Table '{table_name}' truncated successfully via Recycle Bin. {stats['deleted_records']} row(s) archived.",
                    'affected_rows': stats['deleted_records']
                })
            
            # Normal execution for other queries
            cursor.execute(query)
            print("DEBUG: Query executed successfully")
        except Exception as query_error:
            print(f"ERROR: Query execution failed: {str(query_error)}")
            return jsonify({
                'success': False, 
                'message': f"Query error: {str(query_error)}"
            }), 400
        
        # Check if it's a SELECT query
        if cursor.description:
            # SELECT query - return results
            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            
            print(f"DEBUG: Retrieved {len(rows)} rows")
            
            # Convert to list of dicts
            results = []
            for row in rows:
                row_dict = {}
                for i, col in enumerate(columns):
                    val = row[i]
                    # Handle bytes/bytearray for MySQL binary fields
                    if isinstance(val, (bytes, bytearray)):
                        try:
                            val = val.decode('utf-8')
                        except:
                            val = str(val)
                    row_dict[col] = val if val is not None else None
                results.append(row_dict)
            
            # Store in session for export
            session['last_query_results'] = {
                'columns': columns,
                'data': results
            }
            
            return jsonify({
                'success': True,
                'has_results': True,
                'columns': columns,
                'data': results,
                'row_count': len(results),
                'message': f'Query executed successfully. {len(results)} rows returned.'
            })
        else:
            # Non-SELECT query (INSERT, UPDATE, DELETE)
            affected_rows = cursor.rowcount
            conn.commit()
            
            # Emit real-time update
            try:
                if hasattr(current_app, 'socketio') and current_app.socketio:
                    table_name_match = re.search(r"(?:INSERT INTO|UPDATE|DELETE FROM|TRUNCATE TABLE)\s+[`\"']?(\w+)[`\"']?", query, re.IGNORECASE)
                    table_affected = table_name_match.group(1) if table_name_match else "database"
                    
                    current_app.socketio.emit('database_updated', {
                        'action': 'query_execution',
                        'table': table_affected,
                        'message': f'Query executed affecting {table_affected}'
                    })
            except Exception as e:
                print(f"Warning: Failed to emit socket event: {e}")
            
            return jsonify({
                'success': True,
                'has_results': False,
                'message': f'Query executed successfully. {affected_rows} row(s) affected.',
                'affected_rows': affected_rows
            })
            
    except Exception as e:
        print(f"ERROR: Unexpected error in execute: {str(e)}")
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'message': f"Server error: {str(e)}"
        }), 500


@query_bp.route('/refresh-tables', methods=['POST'])
def refresh_tables():
    """Force refresh of table list"""
    try:
        db_type = session.get('db_type', 'sqlite')
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
        else:
            db_conn = session.get('db_params')
            
        if not db_conn:
            return jsonify({'success': False, 'message': 'Database not configured'}), 500
        
        # Get fresh connection
        conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
        
        # Get tables
        tables = DatabaseManagerFlask.get_tables(conn, db_type)
        
        # Get schema
        schema = DatabaseManagerFlask.get_schema(conn, db_type)
        
        return jsonify({
            'success': True, 
            'tables': tables,
            'schema': schema,
            'message': f'Refreshed {len(tables)} tables'
        })
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/autocomplete')
def autocomplete():
    """Get autocomplete suggestions for SQL editor"""
    try:
        db_type = session.get('db_type', 'sqlite')
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
        else:
            db_conn = session.get('db_params')
            
        if not db_conn:
            return jsonify({'success': False, 'message': 'Database not configured'}), 500
        
        conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
        
        # Get tables
        tables = DatabaseManagerFlask.get_tables(conn, db_type)
        
        # Get schema
        schema = DatabaseManagerFlask.get_schema(conn, db_type)
        
        return jsonify({
            'success': True,
            'schema': schema,
            'tables': tables,
            'db_type': db_type
        })
        
    except Exception as e:
        print(f"Autocomplete error: {e}")
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/history')
def history():
    """Get query history"""
    # For now, return empty history
    # In future, implement query history storage
    return jsonify({
        'success': True,
        'history': session.get('query_history', [])
    })


@query_bp.route('/save', methods=['POST'])
def save_query():
    """Save query to history"""
    try:
        data = request.get_json()
        query = data.get('query')
        
        if not query:
            return jsonify({'success': False, 'message': 'No query provided'}), 400
        
        # Get or initialize query history
        history = session.get('query_history', [])
        
        # Add query with timestamp
        import datetime
        history.append({
            'query': query,
            'timestamp': datetime.datetime.now().isoformat()
        })
        
        # Keep only last 50 queries
        history = history[-50:]
        
        session['query_history'] = history
        
        return jsonify({'success': True, 'message': 'Query saved to history'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': str(e)}), 500


@query_bp.route('/execute-python', methods=['POST'])
def execute_python():
    """Execute Python code with safety restrictions"""
    try:
        print("DEBUG: Python execute endpoint called")
        
        # Get request data
        if not request.is_json:
            return jsonify({
                'success': False, 
                'message': 'Request must be JSON format. Please check your Content-Type header.'
            }), 400
            
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False, 
                'message': 'No data received in request. Please send valid JSON.'
            }), 400
            
        code = data.get('code', '').strip()
        print(f"DEBUG: Received Python code: {code[:100]}...")
        
        if not code:
            return jsonify({
                'success': False, 
                'message': 'No Python code provided. Please include code in your request.'
            }), 400
        
        # Security checks - only truly dangerous operations
        dangerous_patterns = [
            r'os\.system', r'os\.popen', r'os\.spawn',
            r'subprocess\.call', r'subprocess\.run', r'subprocess\.Popen',
            r'import\s+subprocess', r'from\s+subprocess\s+import',
            r'shutil\.rmtree',
            r'eval\s*\(', r'exec\s*\(',
            r'input\s*\(', r'raw_input\s*\(',
            r'import\s+socket', r'from\s+socket\s+import',
            r'import\s+urllib', r'from\s+urllib\s+import',
            r'import\s+requests', r'from\s+requests\s+import',
            r'import\s+ftplib', r'from\s+ftplib\s+import',
            r'import\s+telnetlib', r'from\s+telnetlib\s+import',
            r'import\s+smtplib', r'from\s+smtplib\s+import',
            r'import\s+poplib', r'from\s+poplib\s+import',
            r'import\s+imaplib', r'from\s+imaplib\s+import',
        ]
        
        for pattern in dangerous_patterns:
            if re.search(pattern, code, re.IGNORECASE):
                print(f"DEBUG: Blocked by pattern: {pattern}")
                print(f"DEBUG: Code that was blocked: {repr(code[:200])}")
                return jsonify({
                    'success': False, 
                    'message': f'Code contains potentially dangerous operation: {pattern}. This operation is blocked for security reasons.'
                }), 400
        
        # Get database connection for pandas (optional)
        db_type = session.get('db_type', 'sqlite')
        if db_type == 'sqlite':
            db_conn = session.get('db_path')
        else:
            db_conn = session.get('db_params')
            
        # Database connection is optional for Python execution
        conn = None
        if db_conn:
            try:
                conn = DatabaseManagerFlask.get_connection(db_conn, db_type)
            except Exception as e:
                print(f"WARNING: Database connection failed: {str(e)}")
        
        # Get workspace path
        workspace = get_user_workspace()
        
        # Capture output
        output_buffer = io.StringIO()
        error_buffer = io.StringIO()
        
        # Create safe file operations restricted to workspace
        def safe_open(file_path, mode='r', *args, **kwargs):
            """Safe file open restricted to workspace"""
            # Expand user home if present
            if file_path.startswith('~'):
                file_path = os.path.expanduser(file_path)
            
            # Normalize path
            abs_path = os.path.normpath(os.path.join(workspace, file_path) 
                                       if not os.path.isabs(file_path) else file_path)
            
            # Security check - must be within workspace (use normpath for comparison)
            if not os.path.normpath(abs_path).startswith(os.path.normpath(workspace)):
                raise PermissionError(f"Access denied: {file_path} is outside workspace")
            
            return open(abs_path, mode, *args, **kwargs)
        
        def safe_listdir(self=None, path='.'):
            """Safe directory listing restricted to workspace"""
            if path.startswith('~'):
                path = os.path.expanduser(path)
            abs_path = os.path.normpath(os.path.join(workspace, path) 
                                       if not os.path.isabs(path) else path)
            # Security check - must be within workspace (use normpath for comparison)
            if not os.path.normpath(abs_path).startswith(os.path.normpath(workspace)):
                raise PermissionError(f"Access denied: {path} is outside workspace")
            return os.listdir(abs_path)
        
        def safe_makedirs(self=None, path='', exist_ok=False):
            """Safe directory creation restricted to workspace"""
            if path.startswith('~'):
                path = os.path.expanduser(path)
            abs_path = os.path.normpath(os.path.join(workspace, path) 
                                       if not os.path.isabs(path) else path)
            if not os.path.normpath(abs_path).startswith(os.path.normpath(workspace)):
                raise PermissionError(f"Access denied: {path} is outside workspace")
            return os.makedirs(abs_path, exist_ok=exist_ok)
        
        def safe_path_exists(path):
            """Safe path existence check"""
            if path.startswith('~'):
                path = os.path.expanduser(path)
            abs_path = os.path.normpath(os.path.join(workspace, path) 
                                       if not os.path.isabs(path) else path)
            if not os.path.normpath(abs_path).startswith(os.path.normpath(workspace)):
                return False
            return os.path.exists(abs_path)
        
        def safe_path_join(self=None, *paths):
            """Safe path joining"""
            return os.path.join(*paths)
        
        def safe_getcwd(self=None):
            """Get current working directory (workspace)"""
            return workspace
        
        def safe_chdir(self=None, path='.'):
            """Safe directory change restricted to workspace"""
            if path.startswith('~'):
                path = os.path.expanduser(path)
            abs_path = os.path.normpath(os.path.join(workspace, path) 
                                       if not os.path.isabs(path) else path)
            if not os.path.normpath(abs_path).startswith(os.path.normpath(workspace)):
                raise PermissionError(f"Access denied: {path} is outside workspace")
            os.chdir(abs_path)
        
        # Create safe execution environment
        safe_globals = {
            # Essential built-in variables
            '__name__': '__main__',
            '__doc__': None,
            '__package__': None,
            '__loader__': None,
            '__spec__': None,
            '__file__': '<string>',
            '__cached__': None,
            
            # Safe built-ins and modules
            '__builtins__': {
                'print': print,
                'len': len,
                'range': range,
                'enumerate': enumerate,
                'zip': zip,
                'map': map,
                'filter': filter,
                'sum': sum,
                'max': max,
                'min': min,
                'abs': abs,
                'round': round,
                'sorted': sorted,
                'reversed': reversed,
                'list': list,
                'tuple': tuple,
                'dict': dict,
                'set': set,
                'frozenset': frozenset,
                'str': str,
                'int': int,
                'float': float,
                'bool': bool,
                'type': type,
                'isinstance': isinstance,
                'hasattr': hasattr,
                'getattr': getattr,
                'setattr': setattr,
                'delattr': delattr,
                '__import__': __import__,  # Needed for pandas read_excel
                'Exception': Exception,
                'ValueError': ValueError,
                'TypeError': TypeError,
                'KeyError': KeyError,
                'IndexError': IndexError,
                'AttributeError': AttributeError,
                'ImportError': ImportError,
                'ModuleNotFoundError': ModuleNotFoundError,
                'NameError': NameError,
                'UnboundLocalError': UnboundLocalError,
                'ZeroDivisionError': ZeroDivisionError,
                'OverflowError': OverflowError,
                'FloatingPointError': FloatingPointError,
                'AssertionError': AssertionError,
                'NotImplementedError': NotImplementedError,
                'RuntimeError': RuntimeError,
                'MemoryError': MemoryError,
                'SystemError': SystemError,
                'Warning': Warning,
                'UserWarning': UserWarning,
                'DeprecationWarning': DeprecationWarning,
                'PendingDeprecationWarning': PendingDeprecationWarning,
                'SyntaxWarning': SyntaxWarning,
                'RuntimeWarning': RuntimeWarning,
                'FutureWarning': FutureWarning,
                'ImportWarning': ImportWarning,
                'UnicodeWarning': UnicodeWarning,
                'BytesWarning': BytesWarning,
                'ResourceWarning': ResourceWarning,
                'PermissionError': PermissionError,
                'FileNotFoundError': FileNotFoundError,
                'IOError': IOError,
                'OSError': OSError,
                # Safe open function for file operations
                'open': safe_open,
            },
            # Data analysis libraries
            'pd': pd,
            'np': np,
            'plt': plt,
            'datetime': datetime,
            're': re,
            'json': json,
            'math': __import__('math'),
            'statistics': __import__('statistics'),
            'collections': __import__('collections'),
            'itertools': __import__('itertools'),
            'functools': __import__('functools'),
            'operator': __import__('operator'),
            'decimal': __import__('decimal'),
            'fractions': __import__('fractions'),
            'random': __import__('random'),
            'string': __import__('string'),
            'textwrap': __import__('textwrap'),
            'csv': __import__('csv'),
            'base64': base64,
            'hashlib': __import__('hashlib'),
            'hmac': __import__('hmac'),
            'time': __import__('time'),
            'uuid': __import__('uuid'),
            # Workspace and file operations
            'WORKSPACE': workspace,
            'os': type('SafeOS', (), {
                'path': type('SafePath', (), {
                    'join': safe_path_join,
                    'exists': safe_path_exists,
                    'isfile': lambda p: os.path.isfile(os.path.join(workspace, p) if not os.path.isabs(p) else p) if (lambda x: os.path.normpath(x).startswith(workspace) if os.path.isabs(x) else True)(os.path.join(workspace, p) if not os.path.isabs(p) else p) else False,
                    'isdir': lambda p: os.path.isdir(os.path.join(workspace, p) if not os.path.isabs(p) else p) if (lambda x: os.path.normpath(x).startswith(workspace) if os.path.isabs(x) else True)(os.path.join(workspace, p) if not os.path.isabs(p) else p) else False,
                    'basename': os.path.basename,
                    'dirname': os.path.dirname,
                    'splitext': os.path.splitext,
                    'abspath': lambda p: os.path.normpath(os.path.join(workspace, p) if not os.path.isabs(p) else p),
                })(),
                'listdir': safe_listdir,
                'makedirs': safe_makedirs,
                'getcwd': safe_getcwd,
                'chdir': safe_chdir,
                'mkdir': lambda p, mode=0o777: os.mkdir(os.path.normpath(os.path.join(workspace, p)), mode) if os.path.normpath(os.path.join(workspace, p)).startswith(workspace) else (_ for _ in ()).throw(PermissionError(f"Access denied: {p}")),
                'remove': lambda p: os.remove(os.path.normpath(os.path.join(workspace, p))) if os.path.normpath(os.path.join(workspace, p)).startswith(workspace) else (_ for _ in ()).throw(PermissionError(f"Access denied: {p}")),
                'rename': lambda src, dst: os.rename(
                    os.path.normpath(os.path.join(workspace, src)) if os.path.normpath(os.path.join(workspace, src)).startswith(workspace) else (_ for _ in ()).throw(PermissionError(f"Access denied: {src}")),
                    os.path.normpath(os.path.join(workspace, dst)) if os.path.normpath(os.path.join(workspace, dst)).startswith(workspace) else (_ for _ in ()).throw(PermissionError(f"Access denied: {dst}"))
                ),
            })(),
            'glob': type('SafeGlob', (), {
                'glob': lambda pattern: [p for p in __import__('glob').glob(os.path.join(workspace, pattern)) if os.path.normpath(p).startswith(workspace)],
                'iglob': lambda pattern: (p for p in __import__('glob').iglob(os.path.join(workspace, pattern)) if os.path.normpath(p).startswith(workspace)),
            })(),
        }
        
        # Add helper functions for data exploration
        def explore_data(df, name="data"):
            """Helper function to explore DataFrame structure"""
            if not isinstance(df, pd.DataFrame):
                print(f"'{name}' is not a DataFrame")
                return
            
            print(f"\n=== DataFrame: {name} ===")
            print(f"Shape: {df.shape}")
            print(f"Columns: {list(df.columns)}")
            print(f"\nFirst 3 rows:")
            print(df.head(3).to_string())
            print(f"\nData types:")
            print(df.dtypes)
            print(f"\nMissing values:")
            print(df.isnull().sum())
            if len(df.columns) <= 10:
                print(f"\nColumn descriptions:")
                for col in df.columns:
                    unique_count = df[col].nunique()
                    sample_values = df[col].dropna().head(3).tolist()
                    print(f"  {col}: {unique_count} unique values, sample: {sample_values}")
        
        def get_tables():
            """Get list of database tables"""
            if conn is None:
                print("No database connection available")
                return []
            try:
                if db_type == 'sqlite':
                    cursor = conn.cursor()
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                    tables = [row[0] for row in cursor.fetchall()]
                    return tables
                else:
                    cursor = conn.cursor()
                    cursor.execute("SHOW TABLES")
                    tables = [row[0] for row in cursor.fetchall()]
                    return tables
            except Exception as e:
                print(f"Error getting tables: {e}")
                return []
        
        def preview_table(table_name, limit=5):
            """Preview a table structure and sample data"""
            if conn is None:
                print("No database connection available")
                return None
            try:
                if db_type == 'sqlite':
                    query = f"SELECT * FROM {table_name} LIMIT {limit}"
                else:
                    query = f"SELECT * FROM `{table_name}` LIMIT {limit}"
                
                df = pd.read_sql_query(query, conn)
                explore_data(df, f"table_{table_name}")
                return df
            except Exception as e:
                print(f"Error previewing table {table_name}: {e}")
                return None
        
        # Add helper functions to globals
        safe_globals['explore_data'] = explore_data
        safe_globals['get_tables'] = get_tables
        safe_globals['preview_table'] = preview_table
        
        # Add database connection to globals if available
        if conn:
            safe_globals['conn'] = conn
        
        # Execute code with output capture
        try:
            # Clear any existing plots
            plt.clf()
            plt.close('all')
            
            # Debug: Print the code being executed
            print(f"DEBUG: About to execute code: {repr(code[:200])}")
            
            # Change to workspace directory so relative paths work
            original_cwd = os.getcwd()
            os.chdir(workspace)
            
            with contextlib.redirect_stdout(output_buffer), contextlib.redirect_stderr(error_buffer):
                # Execute the code
                exec(code, safe_globals)
            
            # Restore original directory
            os.chdir(original_cwd)
            
            stdout_output = output_buffer.getvalue()
            stderr_output = error_buffer.getvalue()
            
            print(f"DEBUG: Execution completed. stdout: {stdout_output[:100]}, stderr: {stderr_output[:100]}")
            
            # Check for plots
            plot_data = None
            if plt.get_fignums():
                # Save the plot to base64
                img_buffer = io.BytesIO()
                plt.savefig(img_buffer, format='png', bbox_inches='tight', dpi=150)
                img_buffer.seek(0)
                plot_data = base64.b64encode(img_buffer.getvalue()).decode('utf-8')
                plt.clf()
                plt.close('all')
            
            # Combine outputs
            final_output = stdout_output
            if stderr_output:
                final_output += f"\n[STDERR]\n{stderr_output}"
            
            # Check for DataFrame in globals
            df_vars = [name for name, value in safe_globals.items() 
                      if isinstance(value, pd.DataFrame) and name not in ['pd', 'np']]
            
            if df_vars and not plot_data and not final_output.strip():
                # Show the first DataFrame as table
                df = safe_globals[df_vars[0]]
                return jsonify({
                    'success': True,
                    'output_type': 'table',
                    'columns': list(df.columns),
                    'data': df.head(100).to_dict('records'),  # Limit to 100 rows
                    'row_count': len(df),
                    'output': f"DataFrame '{df_vars[0]}' ({len(df)} rows) - Columns: {', '.join(df.columns)}"
                })
            elif plot_data:
                return jsonify({
                    'success': True,
                    'output_type': 'plot',
                    'image_data': plot_data,
                    'output': final_output.strip() or 'Plot generated successfully'
                })
            else:
                return jsonify({
                    'success': True,
                    'output_type': 'text',
                    'output': final_output.strip() or 'Code executed successfully (no output)'
                })
                
        except Exception as e:
            # Restore original directory on error
            try:
                os.chdir(original_cwd)
            except:
                pass
            error_msg = f"Execution error: {str(e)}\n{traceback.format_exc()}"
            print(f"DEBUG: Execution failed: {error_msg[:500]}")
            print(f"DEBUG: Current dir: {os.getcwd()}")
            print(f"DEBUG: Workspace: {workspace}")
            print(f"DEBUG: Files in workspace: {os.listdir(workspace)[:10]}")
            return jsonify({
                'success': False,
                'message': error_msg
            }), 400
            
    except Exception as e:
        print(f"ERROR: Unexpected error in execute-python: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'message': f"Server error: {str(e)}"
        }), 500


@query_bp.route('/execute-r', methods=['POST'])
def execute_r():
    """Execute R code with safety restrictions"""
    try:
        print("DEBUG: R execute endpoint called")
        
        # Get request data
        if not request.is_json:
            return jsonify({
                'success': False, 
                'message': 'Request must be JSON format. Please check your Content-Type header.'
            }), 400
            
        data = request.get_json()
        if not data:
            return jsonify({
                'success': False, 
                'message': 'No data received in request. Please send valid JSON.'
            }), 400
            
        code = data.get('code', '').strip()
        print(f"DEBUG: Received R code: {code[:100]}...")
        
        if not code:
            return jsonify({
                'success': False, 
                'message': 'No R code provided. Please include code in your request.'
            }), 400
        
        # Get R path from request
        r_path = data.get('r_path', '').strip()
        if not r_path:
            return jsonify({
                'success': False, 
                'message': 'Rscript path not provided. Please configure R path in package manager.'
            }), 400
        
        # Check if Rscript exists
        if not os.path.exists(r_path):
            return jsonify({
                'success': False, 
                'message': f'Rscript not found at: {r_path}'
            }), 400
        
        # Get workspace directory
        workspace = get_notebook_workspace()
        
        # Create temporary R script file
        with tempfile.NamedTemporaryFile(mode='w', suffix='.R', delete=False) as f:
            # R script with plot capture
            r_script = f'''
# R Code Execution
{code}

# Check if any plotting devices are active and capture the plot
if (length(dev.list()) > 0) {{
    # Try to save the current plot
    tryCatch({{
        # Create a new PNG device and copy the plot
        temp_file <- tempfile(fileext = ".png")
        png(temp_file, width = 800, height = 600, res = 150)
        # Replay the plot by printing the last object
        dev.off()
        # Move to workspace with a simple name
        final_file <- paste0(getwd(), "/r_plot.png")
        file.rename(temp_file, final_file)
        cat("PLOT_SAVED:r_plot.png\\n")
    }}, error = function(e) {{
        cat("PLOT_ERROR:", e$message, "\\n")
        cat("PLOT_CREATED:TRUE\\n")
    }})
}} else {{
    cat("PLOT_CREATED:FALSE\\n")
}}
'''
            f.write(r_script)
            script_path = f.name
        
        # Create temporary file for plot output
        plot_file = tempfile.NamedTemporaryFile(suffix='.png', delete=False)
        plot_file.close()
        
        try:
            # Change to workspace directory
            original_cwd = os.getcwd()
            os.chdir(workspace)
            
            # Execute R script
            result = subprocess.run(
                [r_path, script_path],
                capture_output=True,
                text=True,
                timeout=30,  # 30 second timeout
                cwd=workspace
            )
            
            # Restore original directory
            os.chdir(original_cwd)
            
            stdout_output = result.stdout
            stderr_output = result.stderr
            
            print(f"DEBUG: R execution completed. stdout: {stdout_output[:100]}, stderr: {stderr_output[:100]}")
            
            # Check for saved plot file
            plot_data = None
            if "PLOT_SAVED:" in stdout_output:
                # Extract filename from output
                for line in stdout_output.split('\n'):
                    if line.startswith("PLOT_SAVED:"):
                        plot_filename = line.split(":", 1)[1].strip()
                        plot_path = os.path.join(workspace, plot_filename)
                        if os.path.exists(plot_path):
                            try:
                                with open(plot_path, 'rb') as f:
                                    plot_data = base64.b64encode(f.read()).decode('utf-8')
                                # Clean up plot file
                                os.unlink(plot_path)
                            except Exception as e:
                                print(f"DEBUG: Could not read plot file: {e}")
                        break
            
            if result.returncode == 0:
                if plot_data:
                    return jsonify({
                        'success': True,
                        'output_type': 'plot',
                        'image_data': plot_data,
                        'output': 'Plot generated successfully'
                    })
                elif "PLOT_CREATED:TRUE" in stdout_output:
                    return jsonify({
                        'success': True,
                        'output_type': 'plot',
                        'output': 'Plot generated (image capture failed)'
                    })
                else:
                    return jsonify({
                        'success': True,
                        'output_type': 'text',
                        'output': stdout_output.strip() or 'R code executed successfully (no output)'
                    })
            else:
                error_msg = f"R execution error: {stderr_output or stdout_output}"
                print(f"DEBUG: R execution failed: {error_msg[:500]}")
                return jsonify({
                    'success': False,
                    'message': error_msg
                }), 400
                
        except subprocess.TimeoutExpired:
            # Restore original directory on timeout
            try:
                os.chdir(original_cwd)
            except:
                pass
            return jsonify({
                'success': False, 
                'message': 'R execution timed out after 30 seconds'
            }), 408
        finally:
            # Clean up temporary script file
            try:
                os.unlink(script_path)
            except:
                pass
            
    except Exception as e:
        print(f"ERROR: Unexpected error in execute-r: {str(e)}")
        traceback.print_exc()
        return jsonify({
            'success': False, 
            'message': f"Server error: {str(e)}"
        }), 500

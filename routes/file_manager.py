"""
File Manager Blueprint
Converted from standalone Manage-Files Flask app
Provides file renaming, copying, moving, and deleting functionality
"""
import os
import shutil
import re
from flask import Blueprint, request, jsonify, render_template
from pathlib import Path

# Try to import tkinter for folder browsing (may not be available in all environments)
try:
    import tkinter as tk
    from tkinter import filedialog
    TKINTER_AVAILABLE = True
except ImportError:
    TKINTER_AVAILABLE = False

file_manager_bp = Blueprint('file_manager', __name__)


@file_manager_bp.route('/')
def index():
    return render_template('file_manager/index.html')


@file_manager_bp.route('/browse_folder')
def browse_folder():
    if not TKINTER_AVAILABLE:
        return jsonify({'error': 'Folder browsing not available in this environment. Please type the path manually.'})
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(title="Select Source Folder")
        root.destroy()
        return jsonify({'folder': folder}) if folder else jsonify({'error': 'No folder selected'})
    except Exception as e:
        return jsonify({'error': str(e)})


@file_manager_bp.route('/browse_file')
def browse_file():
    if not TKINTER_AVAILABLE:
        return jsonify({'error': 'File browsing not available in this environment. Please type the path manually.'})
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        file_path = filedialog.askopenfilename(
            title="Select SQLite Database File",
            filetypes=[
                ("SQLite Database", "*.db *.sqlite *.sqlite3"),
                ("All files", "*.*")
            ]
        )
        root.destroy()
        return jsonify({'file': file_path}) if file_path else jsonify({'error': 'No file selected'})
    except Exception as e:
        return jsonify({'error': str(e)})


@file_manager_bp.route('/select_dest_folder')
def select_dest_folder():
    if not TKINTER_AVAILABLE:
        return jsonify({'error': 'Folder browsing not available in this environment. Please type the path manually.'})
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(title="Select Destination Folder")
        root.destroy()
        return jsonify({'folder': folder}) if folder else jsonify({'error': 'No folder selected'})
    except Exception as e:
        return jsonify({'error': str(e)})


@file_manager_bp.route('/list_files', methods=['POST'])
def list_files():
    data = request.json
    folder = data.get('folder', '').strip()
    filter_pattern = data.get('filter', '*.*').strip()

    if not folder or not os.path.exists(folder):
        return jsonify({'error': 'Folder does not exist'}), 400
    if not os.path.isdir(folder):
        return jsonify({'error': 'Not a directory'}), 400

    try:
        all_files = [f for f in os.listdir(folder) if os.path.isfile(os.path.join(folder, f))]
        if filter_pattern and filter_pattern != '*.*' and filter_pattern != '*':
            pattern = filter_pattern.replace('.', r'\.').replace('*', '.*').replace('?', '.')
            regex = re.compile(pattern, re.IGNORECASE)
            all_files = [f for f in all_files if regex.match(f)]
        all_files.sort()
        return jsonify({'files': all_files, 'folder': folder, 'count': len(all_files)})
    except Exception as e:
        return jsonify({'error': str(e)}), 500


def get_new_name(filename, index, pattern, find='', replace='', prefix='', suffix='', start_num=1):
    name, ext = os.path.splitext(filename)
    if pattern == 'find_replace':
        return filename.replace(find, replace) if find else filename
    elif pattern == 'prefix_suffix':
        return f"{prefix}{name}{suffix}{ext}"
    elif pattern == 'sequential':
        return f"{prefix or 'File'}{str(start_num + index).zfill(3)}{ext}"
    elif pattern == 'remove':
        return filename.replace(find, '') if find else filename
    elif pattern == 'lowercase':
        return filename.lower()
    elif pattern == 'uppercase':
        return filename.upper()
    return filename


@file_manager_bp.route('/preview', methods=['POST'])
def preview_rename():
    data = request.json
    files = data.get('files', [])
    pattern = data.get('pattern')
    dest_folder = data.get('dest_folder', '')

    preview = []
    for i, filename in enumerate(files):
        if pattern in ('copy_files', 'move_files'):
            filter_text = data.get('filter', '').strip().lower()
            if filter_text and filter_text not in filename.lower():
                continue
            preview.append({
                'original': filename,
                'new': filename,
                'changed': False,
                'action': f'{"Copy" if pattern == "copy_files" else "Move"} to: {dest_folder or "—"}'
            })
        elif pattern == 'delete_files':
            filter_text = data.get('filter', '').strip().lower()
            if filter_text and filter_text not in filename.lower():
                continue
            preview.append({
                'original': filename,
                'new': 'Deleted',
                'changed': True,
                'action': 'Delete permanently'
            })
        else:
            new_name = get_new_name(filename, i, pattern,
                                    data.get('find', ''), data.get('replace', ''),
                                    data.get('prefix', ''), data.get('suffix', ''),
                                    int(data.get('startNum', 1)))
            preview.append({
                'original': filename,
                'new': new_name,
                'changed': filename != new_name
            })
    return jsonify({'preview': preview})


@file_manager_bp.route('/rename', methods=['POST'])
def rename_files():
    data = request.json
    folder = data.get('folder')
    files = data.get('files', [])
    pattern = data.get('pattern')

    success = 0
    errors = []
    for i, filename in enumerate(files):
        old_path = os.path.join(folder, filename)
        new_name = get_new_name(filename, i, pattern,
                                data.get('find', ''), data.get('replace', ''),
                                data.get('prefix', ''), data.get('suffix', ''),
                                int(data.get('startNum', 1)))
        new_path = os.path.join(folder, new_name)

        if filename == new_name:
            continue
        try:
            if os.path.exists(new_path):
                errors.append(f'{filename} → {new_name} (already exists)')
            else:
                os.rename(old_path, new_path)
                success += 1
        except Exception as e:
            errors.append(f'{filename}: {str(e)}')
    return jsonify({'success': success, 'errors': errors})


@file_manager_bp.route('/copy_files', methods=['POST'])
def copy_files():
    data = request.json
    source = data.get('folder')
    files = data.get('files', [])
    dest = data.get('dest_folder')
    filter_text = data.get('filter', '').strip().lower()
    
    # Debug logging
    print(f"[DEBUG] Copy operation:")
    print(f"[DEBUG] Source: {source}")
    print(f"[DEBUG] Dest: {dest}")
    print(f"[DEBUG] Files count: {len(files)}")
    print(f"[DEBUG] Filter text: '{filter_text}'")
    print(f"[DEBUG] Files: {files[:5]}...")  # Show first 5 files

    if not dest or not os.path.exists(dest):
        return jsonify({'error': 'Destination folder does not exist'}), 400
    if not os.path.isdir(dest):
        return jsonify({'error': 'Destination is not a directory'}), 400

    success = 0
    errors = []
    for filename in files:
        print(f"[DEBUG] Processing file: {filename}")
        if filter_text and filter_text not in filename.lower():
            print(f"[DEBUG] Skipped {filename} (filter mismatch)")
            continue
        src = os.path.join(source, filename)
        dst = os.path.join(dest, filename)
        print(f"[DEBUG] Copy {src} -> {dst}")
        try:
            if os.path.exists(dst):
                print(f"[DEBUG] File already exists: {dst}")
                errors.append(f'{filename} already exists in destination')
            else:
                shutil.copy2(src, dst)
                print(f"[DEBUG] Successfully copied: {filename}")
                success += 1
        except Exception as e:
            print(f"[DEBUG] Copy error: {filename}: {str(e)}")
            errors.append(f'{filename}: {str(e)}')
    
    print(f"[DEBUG] Final result: success={success}, errors={len(errors)}")
    return jsonify({
        'success': success,
        'errors': errors,
        'message': f'Copied {success} file(s) successfully'
    })


@file_manager_bp.route('/move_files', methods=['POST'])
def move_files():
    data = request.json
    source = data.get('folder')
    files = data.get('files', [])
    dest = data.get('dest_folder')
    filter_text = data.get('filter', '').strip().lower()

    if not dest or not os.path.exists(dest):
        return jsonify({'error': 'Destination folder does not exist'}), 400
    if not os.path.isdir(dest):
        return jsonify({'error': 'Destination is not a directory'}), 400

    success = 0
    errors = []
    for filename in files:
        if filter_text and filter_text not in filename.lower():
            continue
        src = os.path.join(source, filename)
        dst = os.path.join(dest, filename)
        try:
            if os.path.exists(dst):
                errors.append(f'{filename} already exists in destination')
            else:
                shutil.move(src, dst)
                success += 1
        except Exception as e:
            errors.append(f'{filename}: {str(e)}')
    return jsonify({
        'success': success,
        'errors': errors,
        'message': f'Moved {success} file(s) successfully'
    })


@file_manager_bp.route('/delete_files', methods=['POST'])
def delete_files():
    data = request.json
    folder = data.get('folder')
    files = data.get('files', [])
    filter_text = data.get('filter', '').strip().lower()

    success = 0
    errors = []
    for filename in files:
        if filter_text and filter_text not in filename.lower():
            continue
        path = os.path.join(folder, filename)
        try:
            os.remove(path)
            success += 1
        except Exception as e:
            errors.append(f'{filename}: {str(e)}')
    return jsonify({
        'success': success,
        'errors': errors,
        'message': f'Deleted {success} file(s) permanently'
    })
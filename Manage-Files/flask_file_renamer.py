import os
import shutil
import re
from flask import Flask, request, jsonify
from pathlib import Path
import tkinter as tk
from tkinter import filedialog

app = Flask(__name__)

# ---------------------- Full HTML Template (updated with Move & Delete) ----------------------
HTML_TEMPLATE = '''
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>File Renamer Pro - Real-time</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body {
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
            color: #333;
        }
        .container { max-width: 1400px; margin: 0 auto; }
        .header { text-align: center; color: white; margin-bottom: 30px; }
        .header h1 { font-size: 2.5rem; margin-bottom: 10px; text-shadow: 2px 2px 4px rgba(0,0,0,0.2); }
        .header p { font-size: 1.1rem; opacity: 0.9; }
        .card {
            background: white;
            border-radius: 20px;
            padding: 30px;
            box-shadow: 0 20px 60px rgba(0,0,0,0.3);
            margin-bottom: 20px;
        }
        .folder-input { display: flex; gap: 10px; margin-bottom: 20px; }
        .folder-input input {
            flex: 1;
            padding: 15px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 1rem;
        }
        .folder-input input:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        .button {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            padding: 15px 30px;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
            transition: all 0.3s ease;
            box-shadow: 0 4px 15px rgba(102, 126, 234, 0.4);
        }
        .button:hover { transform: translateY(-2px); box-shadow: 0 6px 20px rgba(102, 126, 234, 0.6); }
        .button-danger { background: linear-gradient(135deg, #ff416c 0%, #ff4b2b 100%); }
        .button:disabled { opacity: 0.5; cursor: not-allowed; }
        .button-secondary {
            background: white;
            color: #667eea;
            border: 2px solid #667eea;
        }
        .options-grid { display: grid; grid-template-columns: 1fr 1fr; gap: 20px; margin-top: 20px; }
        .form-group { margin-bottom: 20px; }
        .form-group label { display: block; margin-bottom: 8px; font-weight: 600; color: #333; }
        .form-group input, .form-group select {
            width: 100%;
            padding: 12px 15px;
            border: 2px solid #e0e0e0;
            border-radius: 10px;
            font-size: 1rem;
        }
        .form-group input:focus, .form-group select:focus {
            outline: none;
            border-color: #667eea;
            box-shadow: 0 0 0 3px rgba(102, 126, 234, 0.1);
        }
        .preview-table { width: 100%; margin-top: 20px; border-collapse: collapse; }
        .preview-table th {
            background: #667eea;
            color: white;
            padding: 15px;
            text-align: left;
            font-weight: 600;
            position: sticky;
            top: 0;
        }
        .preview-table td { padding: 12px 15px; border-bottom: 1px solid #e0e0e0; }
        .preview-table tr:hover { background: #f8f9ff; }
        .changed { color: #667eea; font-weight: 600; }
        .unchanged { color: #999; }
        .file-count {
            display: inline-block;
            background: #667eea;
            color: white;
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: 600;
            margin: 10px 0;
        }
        .alert { padding: 15px 20px; border-radius: 10px; margin: 20px 0; }
        .alert-info { background: #d1ecf1; color: #0c5460; border: 1px solid #bee5eb; }
        .hidden { display: none; }
        .button-group { display: flex; gap: 15px; margin-top: 20px; flex-wrap: wrap; }
        .option-inputs { display: none; }
        .option-inputs.active { display: block; }
        .folder-path {
            background: #f8f9ff;
            padding: 15px;
            border-radius: 10px;
            border-left: 4px solid #667eea;
            margin: 10px 0;
            font-family: monospace;
            word-break: break-all;
        }
        .table-container {
            max-height: 500px;
            overflow-y: auto;
            border-radius: 10px;
            border: 1px solid #e0e0e0;
        }
        @media (max-width: 768px) {
            .options-grid { grid-template-columns: 1fr; }
            .button-group { flex-direction: column; }
            .button { width: 100%; }
            .folder-input { flex-direction: column; }
        }
        /* Modal */
        .modal {
            display: none;
            position: fixed;
            z-index: 1000;
            left: 0; top: 0;
            width: 100%; height: 100%;
            background-color: rgba(0,0,0,0.6);
        }
        .modal.show { display: flex; align-items: center; justify-content: center; }
        .modal-content {
            background: white;
            border-radius: 20px;
            padding: 30px;
            max-width: 500px;
            width: 90%;
            box-shadow: 0 20px 60px rgba(0,0,0,0.4);
        }
        .modal-header { display: flex; align-items: center; gap: 15px; margin-bottom: 20px; }
        .modal-icon { font-size: 2.5rem; }
        .modal-title { font-size: 1.5rem; font-weight: 600; }
        .modal-body { color: #555; line-height: 1.6; }
        .modal-footer { display: flex; gap: 10px; justify-content: flex-end; }
        .modal-button {
            padding: 12px 24px;
            border: none;
            border-radius: 10px;
            font-size: 1rem;
            font-weight: 600;
            cursor: pointer;
        }
        .modal-button-primary { background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); color: white; }
        .modal-button-secondary { background: #f0f0f0; color: #333; }
        .modal-button-danger { background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); color: white; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>📁 File Renamer Pro</h1>
            <p>Rename, Copy, Move & Delete files with filter</p>
        </div>

        <div class="card">
            <h2>Select Source Folder</h2>
            <div class="folder-input">
                <input type="text" id="folderPath" placeholder="Click Browse or enter path" value="">
                <button class="button button-secondary" onclick="browseFolder()">📂 Browse</button>
                <button class="button" onclick="loadFolder()">✅ Load Files</button>
            </div>
            <div class="alert alert-info">
                💡 Click Browse to select a folder, or type the path manually
            </div>
            <div id="currentFolder" class="hidden">
                <div class="folder-path">
                    <strong>Current Folder:</strong> <span id="currentPath"></span>
                </div>
                <span class="file-count" id="fileCount">0 files</span>
            </div>
        </div>

        <div id="optionsSection" class="card hidden">
            <h2>Operation</h2>
            <div class="form-group">
                <label for="filter">File Filter (optional)</label>
                <input type="text" id="filter" placeholder="*.ab1 or *.txt or leave empty for all" value="*.*">
            </div>
            <div class="form-group">
                <label for="pattern">Choose Operation</label>
                <select id="pattern">
                    <option value="find_replace">Find and Replace</option>
                    <option value="prefix_suffix">Add Prefix/Suffix</option>
                    <option value="sequential">Sequential Numbering</option>
                    <option value="remove">Remove Pattern</option>
                    <option value="lowercase">Convert to Lowercase</option>
                    <option value="uppercase">Convert to Uppercase</option>
                    <option value="copy_files">Copy Files (with filter)</option>
                    <option value="move_files">Move Files (with filter)</option>
                    <option value="delete_files">Delete Files (with filter)</option>
                </select>
            </div>

            <!-- Find & Replace -->
            <div id="findReplaceInputs" class="option-inputs active">
                <div class="options-grid">
                    <div class="form-group"><label>Find Text</label><input type="text" id="findText" placeholder="Text to find"></div>
                    <div class="form-group"><label>Replace With</label><input type="text" id="replaceText" placeholder="Replacement text"></div>
                </div>
            </div>

            <!-- Prefix/Suffix -->
            <div id="prefixSuffixInputs" class="option-inputs">
                <div class="options-grid">
                    <div class="form-group"><label>Prefix</label><input type="text" id="prefix" placeholder="Add before filename"></div>
                    <div class="form-group"><label>Suffix</label><input type="text" id="suffix" placeholder="Add before extension"></div>
                </div>
            </div>

            <!-- Sequential -->
            <div id="sequentialInputs" class="option-inputs">
                <div class="options-grid">
                    <div class="form-group"><label>Prefix</label><input type="text" id="seqPrefix" placeholder="File" value="File"></div>
                    <div class="form-group"><label>Start Number</label><input type="number" id="startNum" value="1" min="1"></div>
                </div>
            </div>

            <!-- Remove -->
            <div id="removeInputs" class="option-inputs">
                <div class="form-group"><label>Text to Remove</label><input type="text" id="removeText" placeholder="Text to remove"></div>
            </div>

            <!-- Copy / Move -->
            <div id="copyMoveInputs" class="option-inputs">
                <div class="form-group">
                    <label>Contains Text (filter)</label>
                    <input type="text" id="copyFilter" placeholder="e.g. Corona (case-insensitive)">
                </div>
                <div class="form-group">
                    <label>Destination Folder</label>
                    <div class="folder-input">
                        <input type="text" id="destFolder" placeholder="Click Browse or type path">
                        <button class="button button-secondary" onclick="browseDestFolder()">Browse</button>
                    </div>
                </div>
            </div>

            <!-- Delete -->
            <div id="deleteInputs" class="option-inputs">
                <div class="form-group">
                    <label>Contains Text (filter)</label>
                    <input type="text" id="deleteFilter" placeholder="e.g. Corona (case-insensitive)">
                </div>
                <p style="color:#d32f2f; font-weight:bold;">Warning: Files will be permanently deleted!</p>
            </div>

            <div class="button-group">
                <button class="button" onclick="previewRename()">🔍 Preview Changes</button>
                <button class="button button-secondary" onclick="loadFolder()">🔄 Refresh Files</button>
            </div>
        </div>

        <div id="previewSection" class="card hidden">
            <h2>Preview Changes</h2>
            <div id="alertBox"></div>
            <div class="table-container">
                <table class="preview-table">
                    <thead><tr id="previewHeader"><th>Original Name</th><th>New Name / Action</th></tr></thead>
                    <tbody id="previewBody"></tbody>
                </table>
            </div>
            <div class="button-group">
                <button class="button" onclick="executeOperation()">✨ Execute Now</button>
                <button class="button button-secondary" onclick="previewRename()">🔄 Refresh Preview</button>
            </div>
        </div>
    </div>

    <!-- Modal -->
    <div id="modal" class="modal">
        <div class="modal-content">
            <div class="modal-header">
                <div class="modal-icon" id="modalIcon">ℹ️</div>
                <div class="modal-title" id="modalTitle">Information</div>
            </div>
            <div class="modal-body" id="modalBody">This is a message</div>
            <div class="modal-footer" id="modalFooter"></div>
        </div>
    </div>

    <script>
        let currentFiles = [];
        let currentFolder = '';

        function showModal(options) {
            const modal = document.getElementById('modal');
            document.getElementById('modalIcon').textContent = options.icon || 'ℹ️';
            document.getElementById('modalTitle').textContent = options.title || 'Information';
            document.getElementById('modalBody').innerHTML = options.message || '';
            const footer = document.getElementById('modalFooter');
            footer.innerHTML = '';

            if (options.type === 'confirm') {
                const cancel = document.createElement('button');
                cancel.className = 'modal-button modal-button-secondary';
                cancel.textContent = 'Cancel';
                cancel.onclick = () => { closeModal(); if (options.onCancel) options.onCancel(); };
                const confirm = document.createElement('button');
                confirm.className = 'modal-button modal-button-danger';
                confirm.textContent = options.confirmText || 'Confirm';
                confirm.onclick = () => { closeModal(); if (options.onConfirm) options.onConfirm(); };
                footer.appendChild(cancel);
                footer.appendChild(confirm);
            } else {
                const ok = document.createElement('button');
                ok.className = 'modal-button modal-button-primary';
                ok.textContent = 'OK';
                ok.onclick = () => { closeModal(); if (options.onOk) options.onOk(); };
                footer.appendChild(ok);
            }
            modal.classList.add('show');
        }
        function closeModal() { document.getElementById('modal').classList.remove('show'); }

        document.getElementById('modal').addEventListener('click', e => {
            if (e.target === e.currentTarget) closeModal();
        });

        async function browseFolder() {
            try {
                const res = await fetch('/browse_folder');
                const data = await res.json();
                if (data.folder) document.getElementById('folderPath').value = data.folder;
                else if (data.error) showModal({icon: '❌', title: 'Error', message: data.error});
            } catch {
                showModal({icon: '⚠️', title: 'Browser Not Available', message: 'Please type the path manually.'});
            }
        }

        async function browseDestFolder() {
            try {
                const res = await fetch('/select_dest_folder');
                const data = await res.json();
                if (data.folder) document.getElementById('destFolder').value = data.folder;
                else if (data.error) showModal({icon: '❌', title: 'Error', message: data.error});
            } catch {
                showModal({icon: '⚠️', title: 'Browser Not Available', message: 'Please type the path manually.'});
            }
        }

        document.getElementById('pattern').addEventListener('change', function() {
            document.querySelectorAll('.option-inputs').forEach(el => el.classList.remove('active'));
            const p = this.value;
            if (p === 'find_replace') document.getElementById('findReplaceInputs').classList.add('active');
            else if (p === 'prefix_suffix') document.getElementById('prefixSuffixInputs').classList.add('active');
            else if (p === 'sequential') document.getElementById('sequentialInputs').classList.add('active');
            else if (p === 'remove') document.getElementById('removeInputs').classList.add('active');
            else if (p === 'copy_files' || p === 'move_files') document.getElementById('copyMoveInputs').classList.add('active');
            else if (p === 'delete_files') document.getElementById('deleteInputs').classList.add('active');
            if (currentFiles.length) previewRename();
        });

        ['findText','replaceText','prefix','suffix','seqPrefix','startNum','removeText','copyFilter','deleteFilter','destFolder'].forEach(id => {
            const el = document.getElementById(id);
            if (el) el.addEventListener('input', () => { if (currentFiles.length) previewRename(); });
        });

        async function loadFolder() {
            const path = document.getElementById('folderPath').value.trim();
            if (!path) return showModal({icon: '⚠️', title: 'No Folder', message: 'Please select a folder.'});

            const filter = document.getElementById('filter').value.trim() || '*.*';
            try {
                const res = await fetch('/list_files', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({folder: path, filter})
                });
                const data = await res.json();
                if (data.error) return showModal({icon: '❌', title: 'Error', message: data.error});

                currentFiles = data.files;
                currentFolder = data.folder;
                document.getElementById('currentPath').textContent = currentFolder;
                document.getElementById('fileCount').textContent = `${currentFiles.length} file(s)`;
                document.getElementById('currentFolder').classList.remove('hidden');
                document.getElementById('optionsSection').classList.remove('hidden');
                if (currentFiles.length) previewRename();
                else showModal({icon: 'ℹ️', title: 'No Files', message: 'No files match the current filter.'});
            } catch (e) {
                showModal({icon: '❌', title: 'Error', message: e.message});
            }
        }

        async function previewRename() {
            if (!currentFiles.length) return;
            const pattern = document.getElementById('pattern').value;
            const data = {
                folder: currentFolder,
                files: currentFiles,
                pattern,
                find: document.getElementById('findText').value,
                replace: document.getElementById('replaceText').value,
                prefix: pattern === 'prefix_suffix' ? document.getElementById('prefix').value : document.getElementById('seqPrefix').value,
                suffix: document.getElementById('suffix').value,
                startNum: document.getElementById('startNum').value
            };
            if (pattern === 'remove') data.find = document.getElementById('removeText').value;
            if (pattern === 'copy_files' || pattern === 'move_files') {
                data.filter = document.getElementById('copyFilter').value.trim();
                data.dest_folder = document.getElementById('destFolder').value.trim();
            }
            if (pattern === 'delete_files') {
                data.filter = document.getElementById('deleteFilter').value.trim();
            }

            try {
                const res = await fetch('/preview', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                const result = await res.json();
                displayPreview(result.preview, pattern);
            } catch (e) {
                showModal({icon: '❌', title: 'Preview Error', message: e.message});
            }
        }

        function displayPreview(preview, pattern) {
            const tbody = document.getElementById('previewBody');
            const header = document.getElementById('previewHeader');
            tbody.innerHTML = '';
            let actionHeader = 'New Name';
            if (pattern === 'copy_files') actionHeader = 'Will be copied to';
            else if (pattern === 'move_files') actionHeader = 'Will be moved to';
            else if (pattern === 'delete_files') actionHeader = 'Will be deleted';

            header.innerHTML = `<th>Original Name</th><th>${actionHeader}</th>`;

            preview.forEach(item => {
                const tr = document.createElement('tr');
                const action = item.action || (item.changed ? item.new : item.new);
                tr.innerHTML = `<td>${item.original}</td><td class="${item.changed ? 'changed' : 'unchanged'}">${action}</td>`;
                tbody.appendChild(tr);
            });
            document.getElementById('previewSection').classList.remove('hidden');
        }

        async function executeOperation() {
            const pattern = document.getElementById('pattern').value;
            let confirmMsg = '';
            let confirmText = 'Execute';

            if (pattern === 'copy_files') {
                const dest = document.getElementById('destFolder').value.trim();
                if (!dest) return showModal({icon: '⚠️', title: 'Missing Destination', message: 'Please select a destination folder.'});
                confirmMsg = `Copy filtered files to:<br><br><strong>${dest}</strong><br><br>Original files will remain untouched.`;
                confirmText = 'Copy Files';
            } else if (pattern === 'move_files') {
                const dest = document.getElementById('destFolder').value.trim();
                if (!dest) return showModal({icon: '⚠️', title: 'Missing Destination', message: 'Please select a destination folder.'});
                confirmMsg = `Move filtered files to:<br><br><strong>${dest}</strong><br><br>Original files will be removed from source folder.`;
                confirmText = 'Move Files';
            } else if (pattern === 'delete_files') {
                confirmMsg = `Delete filtered files permanently!<br><br>This action cannot be undone.`;
                confirmText = 'Delete Files';
            } else {
                confirmMsg = `Rename files in:<br><br><strong>${currentFolder}</strong><br><br>This cannot be undone!`;
                confirmText = 'Rename Now';
            }

            showModal({
                icon: '⚠️',
                title: 'Confirm Operation',
                message: confirmMsg,
                type: 'confirm',
                confirmText,
                onConfirm: () => {
                    if (pattern === 'copy_files') performCopy();
                    else if (pattern === 'move_files') performMove();
                    else if (pattern === 'delete_files') performDelete();
                    else performRename();
                }
            });
        }

        async function performRename() {
            const pattern = document.getElementById('pattern').value;
            const data = {
                folder: currentFolder,
                files: currentFiles,
                pattern,
                find: document.getElementById('findText').value,
                replace: document.getElementById('replaceText').value,
                prefix: pattern === 'prefix_suffix' ? document.getElementById('prefix').value : document.getElementById('seqPrefix').value,
                suffix: document.getElementById('suffix').value,
                startNum: document.getElementById('startNum').value
            };
            if (pattern === 'remove') data.find = document.getElementById('removeText').value;

            try {
                const res = await fetch('/rename', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                const result = await res.json();
                let msg = result.success > 0 ? `✅ Renamed ${result.success} file(s)!` : 'No changes made.';
                if (result.errors.length) msg += `<br><br>❌ Errors:<br>${result.errors.join('<br>')}`;
                showModal({
                    icon: result.success > 0 ? '✅' : '❌',
                    title: result.success > 0 ? 'Success' : 'Error',
                    message: msg,
                    onOk: loadFolder
                });
            } catch (e) {
                showModal({icon: '❌', title: 'Error', message: 'Rename failed: ' + e.message});
            }
        }

        async function performCopy() {
            const data = {
                folder: currentFolder,
                files: currentFiles,
                dest_folder: document.getElementById('destFolder').value.trim(),
                filter: document.getElementById('copyFilter').value.trim(),
                pattern: 'copy_files'
            };
            try {
                const res = await fetch('/copy_files', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                const result = await res.json();
                let msg = result.message || '';
                if (result.errors.length) msg += `<br><br>Errors:<br>${result.errors.join('<br>')}`;
                showModal({
                    icon: result.success > 0 ? '✅' : '❌',
                    title: result.success > 0 ? 'Success' : 'Error',
                    message: msg,
                    onOk: loadFolder
                });
            } catch (e) {
                showModal({icon: '❌', title: 'Error', message: 'Copy failed: ' + e.message});
            }
        }

        async function performMove() {
            const data = {
                folder: currentFolder,
                files: currentFiles,
                dest_folder: document.getElementById('destFolder').value.trim(),
                filter: document.getElementById('copyFilter').value.trim(),
                pattern: 'move_files'
            };
            try {
                const res = await fetch('/move_files', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                const result = await res.json();
                let msg = result.message || '';
                if (result.errors.length) msg += `<br><br>Errors:<br>${result.errors.join('<br>')}`;
                showModal({
                    icon: result.success > 0 ? '✅' : '❌',
                    title: result.success > 0 ? 'Success' : 'Error',
                    message: msg,
                    onOk: loadFolder
                });
            } catch (e) {
                showModal({icon: '❌', title: 'Error', message: 'Move failed: ' + e.message});
            }
        }

        async function performDelete() {
            const data = {
                folder: currentFolder,
                files: currentFiles,
                filter: document.getElementById('deleteFilter').value.trim(),
                pattern: 'delete_files'
            };
            try {
                const res = await fetch('/delete_files', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify(data)
                });
                const result = await res.json();
                let msg = result.message || '';
                if (result.errors.length) msg += `<br><br>Errors:<br>${result.errors.join('<br>')}`;
                showModal({
                    icon: result.success > 0 ? '✅' : '❌',
                    title: result.success > 0 ? 'Success' : 'Error',
                    message: msg,
                    onOk: loadFolder
                });
            } catch (e) {
                showModal({icon: '❌', title: 'Error', message: 'Delete failed: ' + e.message});
            }
        }
    </script>
</body>
</html>
'''

# ---------------------- Flask Routes ----------------------
@app.route('/')
def index():
    return HTML_TEMPLATE

@app.route('/browse_folder')
def browse_folder():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(title="Select Source Folder")
        root.destroy()
        return jsonify({'folder': folder}) if folder else jsonify({'error': 'No folder selected'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/select_dest_folder')
def select_dest_folder():
    try:
        root = tk.Tk()
        root.withdraw()
        root.attributes('-topmost', True)
        folder = filedialog.askdirectory(title="Select Destination Folder")
        root.destroy()
        return jsonify({'folder': folder}) if folder else jsonify({'error': 'No folder selected'})
    except Exception as e:
        return jsonify({'error': str(e)})

@app.route('/list_files', methods=['POST'])
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

@app.route('/preview', methods=['POST'])
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

@app.route('/rename', methods=['POST'])
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

@app.route('/copy_files', methods=['POST'])
def copy_files():
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
                shutil.copy2(src, dst)
                success += 1
        except Exception as e:
            errors.append(f'{filename}: {str(e)}')
    return jsonify({
        'success': success,
        'errors': errors,
        'message': f'Copied {success} file(s) successfully'
    })

@app.route('/move_files', methods=['POST'])
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

@app.route('/delete_files', methods=['POST'])
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

if __name__ == '__main__':
    print("\n" + "="*60)
    print("🚀 File Renamer Pro - Full Version Ready!")
    print("Now supports: Rename, Copy, Move & Delete (with filter)")
    print("Open: http://localhost:5000")
    print("="*60 + "\n")
    app.run(debug=True, port=5000, host='0.0.0.0')
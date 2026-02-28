"""
Excel file merge functionality with column matching UI
"""
import pandas as pd
import os
from flask import Blueprint, jsonify, render_template, request, send_file
from werkzeug.utils import secure_filename
import tempfile
from datetime import datetime

excel_merge_bp = Blueprint("excel_merge", __name__)

# Configure upload settings
ALLOWED_EXTENSIONS = {'xlsx', 'xls'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

@excel_merge_bp.route("/merge")
def merge_page():
    """Render Excel merge page"""
    return render_template("database/excel_merge.html")

def normalize_column_name(col_name):
    """Normalize column name for comparison"""
    return col_name.lower().replace('_', '').replace(' ', '')

def standardize_sample_id(sample_id):
    """Standardize sample ID format: pad the last numeric part to 3 digits if < 3 digits, keep as-is if >= 3 digits"""
    try:
        # Convert to string and remove any whitespace
        sample_id = str(sample_id).strip()
        
        # Handle patterns like ([A-Z_]+)(\d{2})_(\d+) - pad the last number part
        import re
        
        # Check if it matches the pattern with underscore
        if '_' in sample_id:
            # Split by last underscore and pad the final number
            parts = sample_id.rsplit('_', 1)
            if len(parts) == 2:
                prefix, last_number = parts
                # Extract only numeric part from the last segment
                numeric_part = re.sub(r'[^\d]', '', last_number)
                
                if numeric_part:
                    # Pad to 3 digits if less than 3
                    if len(numeric_part) < 3:
                        numeric_part = numeric_part.zfill(3)
                    return f"{prefix}_{numeric_part}"
        
        # Fallback: extract and pad all numeric parts
        numeric_part = re.sub(r'[^\d]', '', sample_id)
        
        if not numeric_part:
            return sample_id  # Return original if no numbers found
        
        # If less than 3 digits, pad with leading zeros
        if len(numeric_part) < 3:
            numeric_part = numeric_part.zfill(3)
        
        return numeric_part
    except:
        return sample_id  # Return original if any error occurs

def get_column_similarity_score(col1, col2):
    """Calculate similarity score between two column names"""
    norm1 = normalize_column_name(col1)
    norm2 = normalize_column_name(col2)
    
    # Exact match after normalization
    if norm1 == norm2:
        return 100
    
    # One contains the other
    if norm1 in norm2 or norm2 in norm1:
        return 80
    
    # Partial match (at least 3 characters)
    overlap = len(set(norm1) & set(norm2))
    if overlap >= 3:
        return 60
    
    return 0

def get_excel_engine(filename):
    """Determine the appropriate Excel engine based on file extension"""
    if filename.lower().endswith('.xls'):
        return 'xlrd'
    elif filename.lower().endswith('.xlsx'):
        return 'openpyxl'
    else:
        return 'openpyxl'  # default

def normalize_file_path(file_path):
    """Normalize file path for cross-platform compatibility"""
    if not file_path:
        return file_path
    
    # Convert to string and normalize path separators
    file_path = str(file_path)
    file_path = os.path.normpath(file_path)
    
    # Ensure path is absolute
    if not os.path.isabs(file_path):
        file_path = os.path.abspath(file_path)
    
    return file_path

@excel_merge_bp.route("/upload", methods=["POST"])
def upload_files():
    """Upload and analyze two Excel files for merging"""
    try:
        if 'file1' not in request.files or 'file2' not in request.files:
            return jsonify({"success": False, "message": "Both files are required"})
        
        file1 = request.files['file1']
        file2 = request.files['file2']
        
        if file1.filename == '' or file2.filename == '':
            return jsonify({"success": False, "message": "Both files must be selected"})
        
        if not (allowed_file(file1.filename) and allowed_file(file2.filename)):
            return jsonify({"success": False, "message": "Only Excel files (.xlsx, .xls) are allowed"})
        
        # Read Excel files with appropriate engine
        engine1 = get_excel_engine(file1.filename)
        engine2 = get_excel_engine(file2.filename)
        df1 = pd.read_excel(file1, nrows=100, engine=engine1)  # Read first 100 rows for preview
        df2 = pd.read_excel(file2, nrows=100, engine=engine2)
        
        # Get column information
        columns1 = list(df1.columns)
        columns2 = list(df2.columns)
        
        # Find common columns (potential matching columns)
        common_columns = list(set(columns1) & set(columns2))
        
        # Get data types for each column
        dtypes1 = {col: str(df1[col].dtype) for col in columns1}
        dtypes2 = {col: str(df2[col].dtype) for col in columns2}
        
        # Get sample data for preview
        preview1 = df1.head(5).fillna('').to_dict('records')
        preview2 = df2.head(5).fillna('').to_dict('records')
        
        # Store files temporarily and save engine info
        temp_dir = tempfile.gettempdir()
        temp_dir = normalize_file_path(temp_dir)
        os.makedirs(temp_dir, exist_ok=True)  # Ensure temp directory exists
        
        # Generate unique filenames with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S_%f')
        file1_temp_name = f"merge_file1_{timestamp}.xlsx"
        file2_temp_name = f"merge_file2_{timestamp}.xlsx"
        file1_path = normalize_file_path(os.path.join(temp_dir, file1_temp_name))
        file2_path = normalize_file_path(os.path.join(temp_dir, file2_temp_name))
        
        try:
            # Reset file pointers and save files temporarily
            file1.seek(0)
            file2.seek(0)
            
            with open(file1_path, 'wb') as f1:
                f1.write(file1.read())
            
            with open(file2_path, 'wb') as f2:
                f2.write(file2.read())
            
            # Verify files were saved correctly
            if not os.path.exists(file1_path):
                return jsonify({"success": False, "message": f"Failed to save File 1 to: {file1_path}"})
            if not os.path.exists(file2_path):
                return jsonify({"success": False, "message": f"Failed to save File 2 to: {file2_path}"})
            
            # Check file sizes to ensure they're not empty
            if os.path.getsize(file1_path) == 0:
                return jsonify({"success": False, "message": "File 1 is empty after saving"})
            if os.path.getsize(file2_path) == 0:
                return jsonify({"success": False, "message": "File 2 is empty after saving"})
                
        except Exception as save_error:
            return jsonify({"success": False, "message": f"Error saving files: {str(save_error)}"})
        
        return jsonify({
            "success": True,
            "file1": {
                "name": file1.filename,
                "columns": columns1,
                "dtypes": dtypes1,
                "preview": preview1,
                "path": file1_path,
                "engine": engine1
            },
            "file2": {
                "name": file2.filename,
                "columns": columns2,
                "dtypes": dtypes2,
                "preview": preview2,
                "path": file2_path,
                "engine": engine2
            },
            "common_columns": common_columns,
            "all_columns": {
                "file1": columns1,
                "file2": columns2
            }
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error processing files: {str(e)}"})

@excel_merge_bp.route("/execute_merge", methods=["POST"])
def execute_merge():
    """Execute the merge operation based on user selections"""
    try:
        data = request.get_json()
        
        file1_path = normalize_file_path(data.get('file1_path'))
        file2_path = normalize_file_path(data.get('file2_path'))
        file1_engine = data.get('file1_engine', 'openpyxl')
        file2_engine = data.get('file2_engine', 'openpyxl')
        merge_type = data.get('merge_type')  # 'inner', 'outer', 'left', 'right'
        match_columns = data.get('match_columns', [])
        selected_columns_file1 = data.get('selected_columns_file1', [])
        selected_columns_file2 = data.get('selected_columns_file2', [])
        dedup_columns_file1 = data.get('dedup_columns_file1', [])
        dedup_columns_file2 = data.get('dedup_columns_file2', [])
        output_filename = data.get('output_filename', f'merged_output_{datetime.now().strftime("%Y%m%d_%H%M%S")}.xlsx')
        
        if not file1_path or not file2_path:
            return jsonify({"success": False, "message": "File paths are required"})
        
        if not match_columns:
            return jsonify({"success": False, "message": "At least one matching column is required"})
        
        # Check if files exist
        print(f"DEBUG: Checking file1_path: {file1_path}")
        print(f"DEBUG: File1 exists: {os.path.exists(file1_path)}")
        print(f"DEBUG: File1 size: {os.path.getsize(file1_path) if os.path.exists(file1_path) else 'N/A'}")
        
        print(f"DEBUG: Checking file2_path: {file2_path}")
        print(f"DEBUG: File2 exists: {os.path.exists(file2_path)}")
        print(f"DEBUG: File2 size: {os.path.getsize(file2_path) if os.path.exists(file2_path) else 'N/A'}")
        
        if not os.path.exists(file1_path):
            return jsonify({"success": False, "message": f"File 1 not found: {file1_path}"})
        
        if not os.path.exists(file2_path):
            return jsonify({"success": False, "message": f"File 2 not found: {file2_path}"})
        
        # Read full Excel files with appropriate engines
        try:
            df1 = pd.read_excel(file1_path, engine=file1_engine)
        except Exception as e1:
            try:
                # Fallback to alternative engine
                fallback_engine = 'openpyxl' if file1_engine != 'openpyxl' else 'xlrd'
                df1 = pd.read_excel(file1_path, engine=fallback_engine)
            except Exception as e2:
                return jsonify({"success": False, "message": f"Error reading File 1 with both engines: {str(e1)}, {str(e2)}"})
        
        try:
            df2 = pd.read_excel(file2_path, engine=file2_engine)
        except Exception as e1:
            try:
                # Fallback to alternative engine
                fallback_engine = 'openpyxl' if file2_engine != 'openpyxl' else 'xlrd'
                df2 = pd.read_excel(file2_path, engine=fallback_engine)
            except Exception as e2:
                return jsonify({"success": False, "message": f"Error reading File 2 with both engines: {str(e1)}, {str(e2)}"})
        
        # Check if DataFrames are not empty
        if df1.empty:
            return jsonify({"success": False, "message": "File 1 is empty or could not be read"})
        
        if df2.empty:
            return jsonify({"success": False, "message": "File 2 is empty or could not be read"})
        
        # Select only the columns user wants to include
        if selected_columns_file1:
            # Check if columns exist in file1
            missing_cols1 = [col for col in selected_columns_file1 if col not in df1.columns]
            if missing_cols1:
                return jsonify({"success": False, "message": f"Columns not found in File 1: {missing_cols1}"})
            df1 = df1[selected_columns_file1]
        
        if selected_columns_file2:
            # Check if columns exist in file2
            missing_cols2 = [col for col in selected_columns_file2 if col not in df2.columns]
            if missing_cols2:
                return jsonify({"success": False, "message": f"Columns not found in File 2: {missing_cols2}"})
            df2 = df2[selected_columns_file2]
        
        # Handle deduplication
        dedup_file1 = data.get('dedup_file1', 'none')
        dedup_file2 = data.get('dedup_file2', 'none')
        
        # Determine deduplication columns for File 1
        dedup_cols_file1 = []
        if dedup_file1 != 'none':
            if dedup_columns_file1:
                # Use user-selected deduplication columns
                dedup_cols_file1 = dedup_columns_file1
            else:
                # Fallback to match columns
                for match_col in match_columns:
                    if isinstance(match_col, dict):
                        dedup_cols_file1.append(match_col['file1_col'])
                    else:
                        dedup_cols_file1.append(match_col)
        
        # Determine deduplication columns for File 2
        dedup_cols_file2 = []
        if dedup_file2 != 'none':
            if dedup_columns_file2:
                # Use user-selected deduplication columns
                dedup_cols_file2 = dedup_columns_file2
            else:
                # Fallback to match columns
                for match_col in match_columns:
                    if isinstance(match_col, dict):
                        dedup_cols_file2.append(match_col['file2_col'])
                    else:
                        dedup_cols_file2.append(match_col)
        
        # Apply deduplication to File 1
        if dedup_file1 != 'none' and dedup_cols_file1:
            original_rows_file1 = len(df1)
            if dedup_file1 == 'first':
                df1 = df1.drop_duplicates(subset=dedup_cols_file1, keep='first')
            elif dedup_file1 == 'last':
                df1 = df1.drop_duplicates(subset=dedup_cols_file1, keep='last')
            elif dedup_file1 == 'unique':
                df1 = df1.drop_duplicates(subset=dedup_cols_file1, keep=False)
            
            removed_rows_file1 = original_rows_file1 - len(df1)
            if removed_rows_file1 > 0:
                print(f"[INFO] Removed {removed_rows_file1} duplicate rows from File 1 using columns: {dedup_cols_file1} with method: {dedup_file1}")
        
        # Apply deduplication to File 2
        if dedup_file2 != 'none' and dedup_cols_file2:
            original_rows_file2 = len(df2)
            if dedup_file2 == 'first':
                df2 = df2.drop_duplicates(subset=dedup_cols_file2, keep='first')
            elif dedup_file2 == 'last':
                df2 = df2.drop_duplicates(subset=dedup_cols_file2, keep='last')
            elif dedup_file2 == 'unique':
                df2 = df2.drop_duplicates(subset=dedup_cols_file2, keep=False)
            
            removed_rows_file2 = original_rows_file2 - len(df2)
            if removed_rows_file2 > 0:
                print(f"[INFO] Removed {removed_rows_file2} duplicate rows from File 2 using columns: {dedup_cols_file2} with method: {dedup_file2}")
        
        # Check if match columns exist in both DataFrames
        for match_col in match_columns:
            if isinstance(match_col, dict):
                col1_name = match_col['file1_col']
                col2_name = match_col['file2_col']
            else:
                col1_name = col2_name = match_col
            
            if col1_name not in df1.columns:
                return jsonify({"success": False, "message": f"Column '{col1_name}' not found in File 1"})
            
            if col2_name not in df2.columns:
                return jsonify({"success": False, "message": f"Column '{col2_name}' not found in File 2"})
        
        # Standardize sample IDs in match columns if they appear to be sample IDs
        for match_col in match_columns:
            if isinstance(match_col, dict):
                col1_name = match_col['file1_col']
                col2_name = match_col['file2_col']
            else:
                col1_name = col2_name = match_col
            
            # Check if this column appears to be a sample ID column
            col1_lower = col1_name.lower()
            col2_lower = col2_name.lower()
            
            if ('sample' in col1_lower or 'sample' in col2_lower or 
                'id' in col1_lower or 'id' in col2_lower):
                
                # Standardize sample IDs in both dataframes
                df1[col1_name] = df1[col1_name].apply(standardize_sample_id)
                df2[col2_name] = df2[col2_name].apply(standardize_sample_id)
        
        # Analyze data overlap in match columns before merging
        data_overlap_analysis = {}
        for match_col in match_columns:
            # Handle both string (same column name) and dict (different column names) formats
            if isinstance(match_col, dict):
                col1_name = match_col['file1_col']
                col2_name = match_col['file2_col']
                analysis_key = f"{col1_name} ↔ {col2_name}"
            else:
                col1_name = col2_name = match_col
                analysis_key = match_col
            
            # Get unique values from both files (exclude nulls)
            values1 = set(df1[col1_name].dropna().astype(str))
            values2 = set(df2[col2_name].dropna().astype(str))
            
            # Find common values
            common_values = values1 & values2
            unique_to_file1 = values1 - values2
            unique_to_file2 = values2 - values1
            
            data_overlap_analysis[analysis_key] = {
                "file1_col": col1_name,
                "file2_col": col2_name,
                "total_values_file1": len(values1),
                "total_values_file2": len(values2),
                "common_values": len(common_values),
                "unique_to_file1": len(unique_to_file1),
                "unique_to_file2": len(unique_to_file2),
                "overlap_percentage": round((len(common_values) / min(len(values1), len(values2))) * 100, 1) if values1 and values2 else 0,
                "sample_common": list(common_values)[:5],
                "sample_file1_only": list(unique_to_file1)[:3],
                "sample_file2_only": list(unique_to_file2)[:3]
            }
        
        # Check if there's sufficient data overlap
        low_overlap_cols = []
        for key, analysis in data_overlap_analysis.items():
            if analysis["overlap_percentage"] < 10:  # Less than 10% overlap
                low_overlap_cols.append(key)
        
        if low_overlap_cols:
            # Create detailed error message with data analysis
            error_details = []
            for key in low_overlap_cols:
                analysis = data_overlap_analysis[key]
                col_display = f"{analysis['file1_col']} ↔ {analysis['file2_col']}" if analysis['file1_col'] != analysis['file2_col'] else analysis['file1_col']
                error_details.append(f"Column '{col_display}': {analysis['overlap_percentage']}% overlap ({analysis['common_values']} common values)")
                error_details.append(f"  File 1 unique samples: {', '.join(analysis['sample_file1_only'])}")
                error_details.append(f"  File 2 unique samples: {', '.join(analysis['sample_file2_only'])}")
            
            return jsonify({
                "success": False, 
                "message": f"Low data overlap in match columns: {', '.join(low_overlap_cols)}. Consider using different match columns or cleaning your data.",
                "data_analysis": data_overlap_analysis,
                "error_details": error_details
            })
        
        # Perform the merge with support for different column names
        try:
            # Create mapping for columns with different names
            left_on = []
            right_on = []
            
            for match_col in match_columns:
                # Check if this is a mapped column (different names in each file)
                if isinstance(match_col, dict) and 'file1_col' in match_col and 'file2_col' in match_col:
                    left_on.append(match_col['file1_col'])
                    right_on.append(match_col['file2_col'])
                else:
                    # Same column name in both files
                    left_on.append(match_col)
                    right_on.append(match_col)
            
            if merge_type == 'inner':
                merged_df = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how='inner', suffixes=('_file1', '_file2'))
            elif merge_type == 'outer':
                merged_df = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how='outer', suffixes=('_file1', '_file2'))
            elif merge_type == 'left':
                merged_df = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how='left', suffixes=('_file1', '_file2'))
            elif merge_type == 'right':
                merged_df = pd.merge(df1, df2, left_on=left_on, right_on=right_on, how='right', suffixes=('_file1', '_file2'))
            else:
                return jsonify({"success": False, "message": "Invalid merge type"})
        except Exception as e:
            return jsonify({"success": False, "message": f"Error during merge operation: {str(e)}"})
        
        # Check if merge result is empty
        if merged_df.empty:
            return jsonify({"success": False, "message": "Merge resulted in empty dataset. Try different merge type or check matching columns."})
        
        # Apply post-merge deduplication if requested
        post_merge_dedup = data.get('post_merge_dedup', 'none')
        post_merge_dedup_columns = data.get('post_merge_dedup_columns', [])
        
        if post_merge_dedup != 'none':
            # Determine which columns to use for post-merge deduplication
            if post_merge_dedup_columns:
                # Use user-selected columns
                dedup_cols = post_merge_dedup_columns
            else:
                # Fallback to match columns (use the merged column names)
                dedup_cols = []
                for match_col in match_columns:
                    if isinstance(match_col, dict):
                        # For mapped columns, use the File 1 column name (it will be the primary in merge)
                        dedup_cols.append(match_col['file1_col'])
                    else:
                        dedup_cols.append(match_col)
            
            # Apply deduplication to merged result
            if dedup_cols and post_merge_dedup != 'none':
                original_merged_rows = len(merged_df)
                
                if post_merge_dedup == 'first':
                    merged_df = merged_df.drop_duplicates(subset=dedup_cols, keep='first')
                elif post_merge_dedup == 'last':
                    merged_df = merged_df.drop_duplicates(subset=dedup_cols, keep='last')
                elif post_merge_dedup == 'unique':
                    merged_df = merged_df.drop_duplicates(subset=dedup_cols, keep=False)
                
                removed_merged_rows = original_merged_rows - len(merged_df)
                if removed_merged_rows > 0:
                    print(f"[INFO] Removed {removed_merged_rows} duplicate rows from merged result using columns: {dedup_cols} with method: {post_merge_dedup}")
        
        # Save merged file
        temp_dir = tempfile.gettempdir()
        if not output_filename.endswith('.xlsx'):
            output_filename += '.xlsx'
        output_path = os.path.join(temp_dir, output_filename)
        
        # Check Excel row limits before saving
        max_rows = 1048576  # Excel maximum rows
        max_cols = 16384    # Excel maximum columns
        
        current_rows = len(merged_df)
        current_cols = len(merged_df.columns)
        
        if current_rows > max_rows or current_cols > max_cols:
            # Create multiple files if too large
            return handle_large_dataset(merged_df, output_filename, temp_dir, {
                "total_rows_file1": len(df1),
                "total_rows_file2": len(df2),
                "merged_rows": len(merged_df),
                "merge_type": merge_type,
                "match_columns": match_columns,
                "output_columns": list(merged_df.columns)
            })
        
        try:
            merged_df.to_excel(output_path, index=False, engine='openpyxl')
        except Exception as e:
            return jsonify({"success": False, "message": f"Error saving merged file: {str(e)}"})
        
        # Generate statistics
        stats = {
            "total_rows_file1": len(df1),
            "total_rows_file2": len(df2),
            "merged_rows": len(merged_df),
            "merge_type": merge_type,
            "match_columns": match_columns,
            "output_columns": list(merged_df.columns)
        }
        
        return jsonify({
            "success": True,
            "stats": stats,
            "output_path": output_path,
            "output_filename": output_filename,
            "preview": merged_df.head(10).fillna('').to_dict('records')
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error during merge: {str(e)}"})

@excel_merge_bp.route("/download/<filename>")
def download_file(filename):
    """Download the merged Excel file"""
    try:
        temp_dir = tempfile.gettempdir()
        file_path = os.path.join(temp_dir, filename)
        
        if not os.path.exists(file_path):
            return jsonify({"success": False, "message": f"File not found: {filename}"})
        
        # Check if it's a file (not a directory)
        if not os.path.isfile(file_path):
            return jsonify({"success": False, "message": "Invalid file format"})
        
        return send_file(file_path, as_attachment=True, download_name=filename)
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error downloading file: {str(e)}"})

@excel_merge_bp.route("/download_split/<filename>")
def download_split_files(filename):
    """Download split Excel files as a ZIP package"""
    try:
        import zipfile
        import io
        
        temp_dir = tempfile.gettempdir()
        base_name = filename.replace('.xlsx', '') if filename.endswith('.xlsx') else filename
        
        # Find all split files for this base name
        split_files = []
        for file in os.listdir(temp_dir):
            if file.startswith(base_name + '_part') and file.endswith('.xlsx'):
                file_path = os.path.join(temp_dir, file)
                if os.path.exists(file_path):
                    split_files.append((file, file_path))
        
        if not split_files:
            return jsonify({"success": False, "message": f"No split files found for: {filename}"})
        
        # Create ZIP file in memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for file_name, file_path in split_files:
                zip_file.write(file_path, file_name)
        
        zip_buffer.seek(0)
        
        return send_file(
            zip_buffer,
            as_attachment=True,
            download_name=f"{base_name}_split_files.zip",
            mimetype='application/zip'
        )
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error downloading split files: {str(e)}"})

@excel_merge_bp.route("/cleanup", methods=["POST"])
def cleanup_temp_files():
    """Clean up temporary files after merge session"""
    try:
        data = request.get_json()
        file_paths = data.get('file_paths', [])
        
        cleaned_files = []
        for file_path in file_paths:
            try:
                if os.path.exists(file_path):
                    os.remove(file_path)
                    cleaned_files.append(file_path)
            except Exception as e:
                print(f"Error cleaning up {file_path}: {e}")
        
        return jsonify({
            "success": True,
            "cleaned_files": cleaned_files,
            "message": f"Cleaned up {len(cleaned_files)} temporary files"
        })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error during cleanup: {str(e)}"})

def handle_large_dataset(merged_df, output_filename, temp_dir, stats):
    """Handle datasets that exceed Excel limits by splitting into multiple files"""
    try:
        max_rows = 1048576  # Excel maximum rows
        max_cols = 16384    # Excel maximum columns
        header_row = 1       # Excel uses 1-based indexing and includes header
        
        current_rows = len(merged_df)
        current_cols = len(merged_df.columns)
        
        # Create base filename without extension
        base_name = output_filename.replace('.xlsx', '') if output_filename.endswith('.xlsx') else output_filename
        
        # Check if columns exceed limit
        if current_cols > max_cols:
            return jsonify({
                "success": False, 
                "message": f"This sheet is too large! Your sheet size is: {current_rows}, {current_cols} Max sheet size is: {max_rows}, {max_cols}. The number of columns ({current_cols}) exceeds Excel's limit of {max_cols}."
            })
        
        # Split by rows if too many rows
        if current_rows > max_rows:
            # Calculate maximum data rows per chunk (accounting for header)
            max_data_rows_per_chunk = max_rows - header_row
            
            # Calculate number of files needed
            num_files = (current_rows + max_data_rows_per_chunk - 1) // max_data_rows_per_chunk
            
            split_files = []
            for i in range(num_files):
                start_idx = i * max_data_rows_per_chunk
                end_idx = min((i + 1) * max_data_rows_per_chunk, current_rows)
                
                # Create chunk
                chunk_df = merged_df.iloc[start_idx:end_idx]
                
                # Create filename for this chunk
                chunk_filename = f"{base_name}_part{i+1}_of_{num_files}.xlsx"
                chunk_path = os.path.join(temp_dir, chunk_filename)
                
                # Save chunk with explicit engine and verify row count
                if len(chunk_df) > max_data_rows_per_chunk:
                    return jsonify({
                        "success": False, 
                        "message": f"Chunk size error: Chunk has {len(chunk_df)} rows but maximum allowed is {max_data_rows_per_chunk}"
                    })
                
                chunk_df.to_excel(chunk_path, index=False, engine='openpyxl')
                split_files.append({
                    "filename": chunk_filename,
                    "path": chunk_path,
                    "rows": len(chunk_df),
                    "start_row": start_idx + 1,
                    "end_row": end_idx
                })
            
            # Update stats to reflect split
            stats.update({
                "split_into_multiple_files": True,
                "total_files_created": num_files,
                "original_rows": current_rows,
                "max_rows_per_file": max_data_rows_per_chunk,
                "split_files": split_files
            })
            
            return jsonify({
                "success": True,
                "stats": stats,
                "split_files": split_files,
                "output_filename": output_filename,
                "message": f"Dataset too large for single Excel file. Split into {num_files} files with max {max_data_rows_per_chunk} data rows each (plus header).",
                "preview": merged_df.head(10).fillna('').to_dict('records')
            })
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error handling large dataset: {str(e)}"})

@excel_merge_bp.route("/analyze_columns", methods=["POST"])
def analyze_columns():
    """Analyze column compatibility for matching"""
    try:
        data = request.get_json()
        file1_path = data.get('file1_path')
        file2_path = data.get('file2_path')
        column1 = data.get('column1')
        column2 = data.get('column2')
        
        if not all([file1_path, file2_path, column1, column2]):
            return jsonify({"success": False, "message": "Missing required parameters"})
        
        # Check if files exist
        if not os.path.exists(file1_path):
            return jsonify({"success": False, "message": f"File 1 not found: {file1_path}"})
        
        if not os.path.exists(file2_path):
            return jsonify({"success": False, "message": f"File 2 not found: {file2_path}"})
        
        # Read sample data with explicit engine
        try:
            df1 = pd.read_excel(file1_path, nrows=1000, engine='openpyxl')
            df2 = pd.read_excel(file2_path, nrows=1000, engine='openpyxl')
        except Exception as e:
            return jsonify({"success": False, "message": f"Error reading Excel files: {str(e)}"})
        
        # Check if columns exist
        if column1 not in df1.columns:
            return jsonify({"success": False, "message": f"Column '{column1}' not found in File 1"})
        
        if column2 not in df2.columns:
            return jsonify({"success": False, "message": f"Column '{column2}' not found in File 2"})
        
        # Analyze column data
        col1_data = df1[column1].dropna()
        col2_data = df2[column2].dropna()
        
        # Check data types
        type1 = str(col1_data.dtype)
        type2 = str(col2_data.dtype)
        
        # Check for common values (sample)
        common_values = set(col1_data.astype(str)) & set(col2_data.astype(str))
        
        # Calculate statistics
        analysis = {
            "column1": {
                "name": column1,
                "type": type1,
                "unique_values": len(col1_data.unique()),
                "null_count": df1[column1].isnull().sum(),
                "sample_values": col1_data.head(5).tolist()
            },
            "column2": {
                "name": column2,
                "type": type2,
                "unique_values": len(col2_data.unique()),
                "null_count": df2[column2].isnull().sum(),
                "sample_values": col2_data.head(5).tolist()
            },
            "compatibility": {
                "type_match": type1 == type2,
                "common_values": len(common_values),
                "common_sample": list(common_values)[:5]
            }
        }
        
        return jsonify({"success": True, "analysis": analysis})
        
    except Exception as e:
        return jsonify({"success": False, "message": f"Error analyzing columns: {str(e)}"})
import sqlite3
import os

# Check all database files and their structures
db_files = [
    'CAN2Database.db',
    'CAN2Database_v2 - Copy.db', 
    'CAN2Database_v2.db'
]

for db_file in db_files:
    if os.path.exists(db_file):
        print(f'\n=== {db_file} ===')
        try:
            conn = sqlite3.connect(db_file)
            cursor = conn.cursor()
            
            # Check samples table structure
            cursor.execute('PRAGMA table_info("samples")')
            columns = cursor.fetchall()
            
            # Look for tissue_id column
            has_tissue_id = any(col[1] == 'tissue_id' for col in columns)
            print(f'Has tissue_id: {has_tissue_id}')
            
            if has_tissue_id:
                print('This database has the correct structure!')
                # Copy this database to the correct location
                target_path = os.path.join('DataExcel', 'CAN2-With-Referent-Key.db')
                import shutil
                shutil.copy2(db_file, target_path)
                print(f'Copied to: {target_path}')
                conn.close()
                break
            else:
                print('Wrong structure - checking first few columns:')
                for col in columns[:5]:
                    print(f'  {col[1]} ({col[2]})')
            
            conn.close()
            
        except Exception as e:
            print(f'Error: {e}')

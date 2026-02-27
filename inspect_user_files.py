import sqlite3
import pandas as pd
import os

def inspect_files():
    db_path = r"d:\MyFiles\Program_Last_version\ViroDB_structure - Copy\database\SQL.db"
    excel_path = r"d:\MyFiles\Program_Last_version\ViroDB_structure - Copy\DataTemplate.xlsx"
    
    print(f"--- Inspecting DB: {db_path} ---")
    if os.path.exists(db_path):
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [t[0] for t in cursor.fetchall()]
        print(f"Tables: {tables}")
        
        for table in tables:
            if table.startswith('sqlite_'): continue
            print(f"\nTable: {table}")
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            for col in columns:
                print(f"  {col[1]} ({col[2]}) {'PK' if col[5] else ''}")
        conn.close()
    else:
        print("DB file not found!")

    print(f"\n--- Inspecting Excel: {excel_path} ---")
    if os.path.exists(excel_path):
        xl = pd.ExcelFile(excel_path)
        print(f"Sheet names: {xl.sheet_names}")
        for sheet in xl.sheet_names:
            df = xl.parse(sheet)
            print(f"\nSheet: {sheet}")
            print(f"Columns: {list(df.columns)}")
            print(f"Shape: {df.shape}")
            if not df.empty:
                print("First row:")
                print(df.iloc[0].to_dict())
    else:
        print("Excel file not found!")

if __name__ == "__main__":
    inspect_files()

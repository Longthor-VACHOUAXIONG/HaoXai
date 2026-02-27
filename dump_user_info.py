import sqlite3
import pandas as pd
import os

def dump_info():
    db_path = r"database\SQL.db"
    excel_path = r"DataTemplate.xlsx"
    out_path = "detailed_inspection.txt"
    
    with open(out_path, "w") as f:
        f.write("=== DB SCHEMA ===\n")
        if os.path.exists(db_path):
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            for table in tables:
                if table.startswith('sqlite_'): continue
                f.write(f"\nTable: {table}\n")
                cursor.execute(f"PRAGMA table_info({table})")
                cols = cursor.fetchall()
                for c in cols:
                    f.write(f"  {c[1]} ({c[2]})\n")
                
                # Check for existing data count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                count = cursor.fetchone()[0]
                f.write(f"  Rows: {count}\n")
            conn.close()
        
        f.write("\n=== EXCEL STRUCTURE ===\n")
        if os.path.exists(excel_path):
            xl = pd.ExcelFile(excel_path)
            for sheet in xl.sheet_names:
                df = xl.parse(sheet, nrows=5)
                f.write(f"\nSheet: {sheet}\n")
                f.write(f"  Columns: {list(df.columns)}\n")
                if not df.empty:
                    f.write(f"  Example Row: {df.iloc[0].to_dict()}\n")

if __name__ == "__main__":
    dump_info()

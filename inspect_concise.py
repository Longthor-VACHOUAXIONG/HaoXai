import sqlite3
import pandas as pd
import os

def inspect():
    db = r"database\SQL.db"
    xl = r"DataTemplate.xlsx"
    
    print("DB Schema Summary:")
    if os.path.exists(db):
        c = sqlite3.connect(db)
        for t in ['hosts', 'samples', 'screening']:
            print(f"-- {t}:")
            try:
                info = c.execute(f"PRAGMA table_info({t})").fetchall()
                print([f"{col[1]}({col[2]})" for col in info])
            except:
                print("Table not found")
        c.close()
    
    print("\nExcel Summary:")
    if os.path.exists(xl):
        f = pd.ExcelFile(xl)
        print(f"Sheets: {f.sheet_names}")
        for s in f.sheet_names:
            df = f.parse(s, nrows=2)
            print(f"{s} cols: {list(df.columns)}")
inspect()

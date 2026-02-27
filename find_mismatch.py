import sqlite3
import os

def search_dbs():
    db_files = []
    for root, dirs, files in os.walk('.'):
        for file in files:
            if file.endswith('.db'):
                db_files.append(os.path.join(root, file))
    
    print(f"Found {len(db_files)} database files.")
    
    for db_path in db_files:
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Check for BatHost table
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name LIKE 'BatHost'")
            table = cursor.fetchone()
            if table:
                print(f"\n[FOUND TABLE] 'BatHost' in {db_path}")
                
                # Check for A109 in any column
                cursor.execute(f"PRAGMA table_info({table[0]})")
                columns = [c[1] for c in cursor.fetchall()]
                
                where_parts = [f"CAST({col} AS TEXT) LIKE '%A109%'" for col in columns]
                query = f"SELECT * FROM {table[0]} WHERE {' OR '.join(where_parts)}"
                
                cursor.execute(query)
                rows = cursor.fetchall()
                if rows:
                    print(f"  [FOUND DATA] 'A109' matches {len(rows)} rows in {db_path}:{table[0]}")
                    for row in rows[:2]:
                        print(f"    {row}")
                else:
                    print(f"  [NO DATA] 'A109' not found in {table[0]}")
                    
                # Check for BAGID in any column
                where_parts_bagid = [f"CAST({col} AS TEXT) LIKE '%Bagid%'" for col in columns]
                query_bagid = f"SELECT * FROM {table[0]} WHERE {' OR '.join(where_parts_bagid)} LIMIT 5"
                cursor.execute(query_bagid)
                rows_bagid = cursor.fetchall()
                if rows_bagid:
                    print(f"  [FOUND DATA] 'Bagid' matches {len(rows_bagid)} rows in {db_path}:{table[0]}")
                    for row in rows_bagid[:3]:
                        print(f"    {row}")
                else:
                    print(f"  [NO DATA] 'Bagid' not found in values of {table[0]}")
            
            conn.close()
        except Exception as e:
            # print(f"Error checking {db_path}: {e}")
            pass

if __name__ == "__main__":
    search_dbs()

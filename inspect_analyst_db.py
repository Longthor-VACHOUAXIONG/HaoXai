
import sqlite3
import json
import os

def inspect():
    db_path = "D:/MyFiles/Program_Last_version/CAN2-ANALYST.db"
    output_path = "diagnostic_results.txt"
    with open(output_path, "w") as f:
        f.write(f"Inspecting: {db_path}\n")
        if not os.path.exists(db_path):
            f.write("File does not exist.\n")
            return

        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            cursor.execute("SELECT id, original_table, data FROM RecycleBin WHERE original_table = 'blast_hits' LIMIT 10")
            rows = cursor.fetchall()
            f.write(f"Found {len(rows)} entries for blast_hits\n")
            for row in rows:
                data = json.loads(row[2])
                f.write(f"ID: {row[0]}, Data length: {len(data.keys())}\n")
                if len(data.keys()) > 0:
                    f.write(f"  Keys: {list(data.keys())}\n")
            
            if len(rows) > 0:
                f.write(f"\nDetailed FIRST entry mapping: {json.dumps(json.loads(rows[0][2]), indent=2)}\n")
            else:
                f.write("No blast_hits entries found.\n")
                
            cursor.execute("PRAGMA table_info(blast_hits)")
            cols = cursor.fetchall()
            f.write("\nCurrent table Columns:\n")
            for col in cols:
                f.write(f"{col}\n")
                
            conn.close()
        except Exception as e:
            f.write(f"Error: {e}\n")
    print(f"Results written to {output_path}")

if __name__ == "__main__":
    inspect()

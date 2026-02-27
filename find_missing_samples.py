import sqlite3
import os

# Connect to the database
db_path = os.path.join(os.path.dirname(__file__), 'DataExcel', 'CAN2-With-Referent-Key.db')
print(f"Connecting to database: {db_path}")

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Test samples that are showing empty
test_samples = ['CANR_TISL24_054', 'IPLNAHL_ANA25_006', 'CANB_TIS23_L_075', 'CANB_INT23_L_057']

print("\n" + "="*60)
print("SEARCHING FOR SAMPLES IN DATABASE")
print("="*60)

# Get all tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [row[0] for row in cursor.fetchall()]
print(f"Found {len(tables)} tables: {tables[:10]}...")

for sample_id in test_samples:
    print(f"\n{'='*40}")
    print(f"SEARCHING FOR: {sample_id}")
    print(f"{'='*40}")
    
    found_any = False
    
    # Search in each table
    for table in tables:
        if table.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
            continue
            
        try:
            # Get table columns
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns = [c[1] for c in cursor.fetchall()]
            
            # Build search query - look for sample_id in any column
            search_conditions = []
            params = []
            
            for col in columns:
                search_conditions.append(f"{col} = ?")
                params.append(sample_id)
            
            if search_conditions:
                query = f"SELECT * FROM {table} WHERE " + " OR ".join(search_conditions) + " LIMIT 5"
                cursor.execute(query, params)
                rows = cursor.fetchall()
                
                if rows:
                    print(f"\n📋 Found in table '{table}' ({len(rows)} matches):")
                    for i, row in enumerate(rows):
                        row_dict = dict(zip(columns, row))
                        print(f"  Match {i+1}: {row_dict}")
                        found_any = True
                    
        except Exception as e:
            print(f"Error searching table {table}: {e}")
    
    if not found_any:
        print(f"\n❌ NO MATCHES FOUND for {sample_id}")
        print("This sample does not exist in any table!")
    
    # Try partial matches
    print(f"\n🔍 Trying partial matches for '{sample_id}'...")
    for table in tables:
        if table.lower() in ['sqlite_sequence', 'sqlite_stat1', 'recyclebin']:
            continue
            
        try:
            cursor.execute(f'PRAGMA table_info("{table}")')
            columns = [c[1] for c in cursor.fetchall()]
            
            # Look for partial matches in text columns
            for col in columns:
                if any(keyword in col.lower() for keyword in ['id', 'code', 'sample', 'tube']):
                    query = f"SELECT * FROM {table} WHERE {col} LIKE ? LIMIT 3"
                    cursor.execute(query, (f"%{sample_id}%",))
                    rows = cursor.fetchall()
                    
                    if rows:
                        print(f"  Partial matches in {table}.{col}:")
                        for row in rows:
                            row_dict = dict(zip(columns, row))
                            matching_value = row_dict.get(col, 'N/A')
                            print(f"    {matching_value}")
                        
        except Exception as e:
            continue

conn.close()
print(f"\n{'='*60}")
print("SEARCH COMPLETE")
print("="*60)

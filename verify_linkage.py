import sqlite3

DB_PATH = r'd:\MyFiles\Program_Last_version\ViroDB_structure_latest_V - Copy\DataExcel\CAN2-With-Referent-Key.db'
conn = sqlite3.connect(DB_PATH)
cursor = conn.cursor()

print("--- LINKAGE CHECK ---")

# 1. Start from the screening result
cursor.execute("SELECT tested_sample_id, sample_id FROM screening_results WHERE tested_sample_id = 'CANB_TIS23_L_075'")
row = cursor.fetchone()
print(f"Screening Result: {row}")

if row and row[1]:
    sample_id = row[1]
    # 2. Check the sample record
    cursor.execute("SELECT sample_id, source_id, host_id, sample_origin, saliva_id, tissue_id FROM samples WHERE sample_id = ?", (sample_id,))
    s_row = cursor.fetchone()
    print(f"Sample Record: {s_row}")
    
    if s_row and s_row[2]:
        host_id = s_row[2]
        # 3. Check the host record
        cursor.execute("SELECT host_id, source_id, host_type, bag_id, taxonomy_id FROM hosts WHERE host_id = ?", (host_id,))
        h_row = cursor.fetchone()
        print(f"Host Record: {h_row}")
        
        if h_row:
            # 4. Check taxonomy
            cursor.execute("SELECT scientific_name FROM taxonomy WHERE taxonomy_id = ?", (h_row[4],))
            print(f"Taxonomy: {cursor.fetchone()}")

print("\n--- ANY OTHER HOSTS WITH BAG 'CANB_PT23_036'? ---")
cursor.execute("SELECT host_id, source_id, host_type, bag_id FROM hosts WHERE bag_id = 'CANB_PT23_036'")
print(cursor.fetchall())

conn.close()

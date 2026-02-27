import mariadb
import sys

DB_CONFIG = {
    'host': '172.22.100.167',
    'user': 'Longthor',
    'password': 'Longthor@CAN2!',
    'database': 'CAN2',
    'port': 3306
}

TARGET_ID = 'CANB_TIS23_L_075'

try:
    conn = mariadb.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        t.scientific_name AS Species, 
        h.host_type AS HostKind, 
        r.tested_sample_id AS SampleCode,
        h.capture_date
    FROM screening_results r
    JOIN samples s ON r.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    WHERE tested_sample_id = %s;
    """
    
    cursor.execute(query, (TARGET_ID,))
    row = cursor.fetchone()
    
    print("--- MySQL VERIFICATION ---")
    if row:
        print(f"Species: {row[0]}")
        print(f"HostKind: {row[1]}")
        print(f"SampleCode: {row[2]}")
        print(f"CaptureDate: {row[3]}")
    else:
        print(f"No record found for {TARGET_ID}")
    
    conn.close()
except Exception as e:
    print(f"Error: {e}")

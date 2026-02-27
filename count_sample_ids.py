#!/usr/bin/env python3
"""
Count sample IDs by various criteria
"""

import sqlite3

db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'

print('🔍 COUNTING SAMPLE IDS')
print('=' * 30)

try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Total sample IDs
    cursor.execute("SELECT COUNT(*) FROM samples")
    total_samples = cursor.fetchone()[0]
    print(f'📊 Total sample IDs: {total_samples:,}')
    
    # Sample IDs by origin
    cursor.execute("""
        SELECT sample_origin, COUNT(*) as count
        FROM samples
        GROUP BY sample_origin
        ORDER BY count DESC
    """)
    by_origin = cursor.fetchall()
    
    print(f'\n📊 Sample IDs by origin:')
    for origin, count in by_origin:
        print(f'  📋 {origin}: {count:,}')
    
    # Sample IDs by host type
    cursor.execute("""
        SELECT h.host_type, COUNT(s.sample_id) as count
        FROM samples s
        JOIN hosts h ON s.host_id = h.host_id
        GROUP BY h.host_type
        ORDER BY count DESC
    """)
    by_host_type = cursor.fetchall()
    
    print(f'\n📊 Sample IDs by host type:')
    for host_type, count in by_host_type:
        print(f'  📋 {host_type}: {count:,}')
    
    # Sample IDs with biological IDs
    cursor.execute("""
        SELECT 
            COUNT(CASE WHEN s.saliva_id IS NOT NULL THEN 1 END) as with_saliva,
            COUNT(CASE WHEN s.anal_id IS NOT NULL THEN 1 END) as with_anal,
            COUNT(CASE WHEN s.urine_id IS NOT NULL THEN 1 END) as with_urine,
            COUNT(CASE WHEN s.ear_id IS NOT NULL THEN 1 END) as with_ear,
            COUNT(CASE WHEN s.blood_id IS NOT NULL THEN 1 END) as with_blood,
            COUNT(CASE WHEN s.tissue_id IS NOT NULL THEN 1 END) as with_tissue,
            COUNT(*) as total
        FROM samples s
    """)
    bio_counts = cursor.fetchone()
    
    saliva, anal, urine, ear, blood, tissue, total = bio_counts
    print(f'\n📊 Sample IDs with biological IDs:')
    print(f'  🧪 Saliva: {saliva:,}')
    print(f'  🔬 Anal: {anal:,}')
    print(f'  💧 Urine: {urine:,}')
    print(f'  👂 Ear: {ear:,}')
    print(f'  🩸 Blood: {blood:,}')
    print(f'  🧬 Tissue: {tissue:,}')
    print(f'  📊 Total: {total:,}')
    
    # Sample IDs by host type and biological ID type
    cursor.execute("""
        SELECT 
            h.host_type,
            COUNT(CASE WHEN s.saliva_id IS NOT NULL THEN 1 END) as with_saliva,
            COUNT(CASE WHEN s.anal_id IS NOT NULL THEN 1 END) as with_anal,
            COUNT(CASE WHEN s.urine_id IS NOT NULL THEN 1 END) as with_urine,
            COUNT(CASE WHEN s.ear_id IS IS NOT NULL THEN 1 END) as with_ear,
            COUNT(CASE WHEN s.blood_id IS NOT NULL THEN 1 END) as with_blood,
            COUNT(CASE WHEN s.tissue_id IS NOT NULL THEN 1 END) as with_tissue,
            COUNT(s.sample_id) as total
        FROM samples s
        JOIN hosts h ON s.host_id = h.host_id
        GROUP BY h.host_type
        ORDER BY total DESC
    """)
    bio_by_type = cursor.fetchall()
    
    print(f'\n📊 Sample IDs by host type and biological ID type:')
    for host_type, saliva, anal, urine, ear, blood, tissue, total in bio_by_type:
        print(f'  📋 {host_type}: {total:,} samples')
        if saliva > 0: print(f'    🧪 Saliva: {saliva:,}')
        if anal > 0: print(f'    🔬 Anal: {anal:,}')
        if urine > 0: print(f'    💧 Urine: {urine:,}')
        if ear > 0: print(f'    👂 Ear: {ear:,}')
        if blood > 0: print(f'    🩸 Blood: {blood:,}')
        if tissue > 0: print(f'    🧬 Tissue: {tissue:,}')
    
    # Sample IDs by province
    cursor.execute("""
        SELECT 
            l.province,
            COUNT(s.sample_id) as sample_count
        FROM samples s
        JOIN hosts h ON s.host_id = h.host_id
        LEFT JOIN locations l ON h.location_id = l.location_id
        WHERE l.province IS NOT NULL
        GROUP BY l.province
        ORDER BY sample_count DESC
    """)
    by_province = cursor.fetchall()
    
    print(f'\n📊 Sample IDs by province:')
    for province, count in by_province:
        print(f'  📋 {province}: {count:,}')
    
    # Sample IDs by species (top 10)
    cursor.execute("""
        SELECT 
            t.scientific_name,
            COUNT(s.sample_id) as sample_count
        FROM samples s
        JOIN hosts h ON s.host_id = h.host_id
        JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
        GROUP BY t.scientific_name
        ORDER BY sample_count DESC
        LIMIT 10
    """)
    by_species = cursor.fetchall()
    
    print(f'\n📊 Top 10 species by sample count:')
    for species, count in by_species:
        print(f'  🦇 {species}: {count:,} samples')
    
    # Sample ID range
    cursor.execute("SELECT MIN(sample_id), MAX(sample_id) FROM samples")
    id_range = cursor.fetchone()
    min_id, max_id = id_range
    
    print(f'\n📊 Sample ID range: {min_id} to {max_id}')
    
    conn.close()
    
    print(f'\n🎉 SAMPLE ID COUNTING COMPLETE!')
    print(f'📂 Database: {db_path}')
    print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')
    
except Exception as e:
    print(f'❌ Error: {str(e)}')

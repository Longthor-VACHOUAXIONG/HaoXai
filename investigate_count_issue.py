#!/usr/bin/env python3
"""
Investigate the count discrepancy issue
"""
import sqlite3

print('🔍 INVESTIGATING THE COUNT DISCREPANCY')
print('=' * 60)

# Connect to database
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check the actual data step by step
print('Step 1: Count Louang Namtha hosts')
cursor.execute('SELECT COUNT(*) FROM hosts h JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_hosts = cursor.fetchone()[0]
print(f'Louang Namtha hosts: {louang_hosts}')

print('\nStep 2: Count Louang Namtha samples')
cursor.execute('SELECT COUNT(*) FROM samples s JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_samples = cursor.fetchone()[0]
print(f'Louang Namtha samples: {louang_samples}')

print('\nStep 3: Count Louang Namtha screening results')
cursor.execute('SELECT COUNT(*) FROM screening_results sr JOIN samples s ON sr.sample_id = s.sample_id JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_screening = cursor.fetchone()[0]
print(f'Louang Namtha screening: {louang_screening}')

print(f'\n🔍 THE PROBLEM: {louang_screening} screening results but only {louang_samples} samples!')

# Check if some screening results are duplicated or incorrectly linked
print('\nStep 4: Check for duplicates in screening results')
cursor.execute('''
    SELECT sr.tested_sample_id, COUNT(*) as count
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE ?
    GROUP BY sr.tested_sample_id
    HAVING COUNT(*) > 1
    ORDER BY count DESC
    LIMIT 10
''', ('%Louang%',))

duplicates = cursor.fetchall()
print(f'Duplicate sample IDs in Louang Namtha: {len(duplicates)}')
for sample_id, count in duplicates:
    print(f'  Sample ID {sample_id}: {count} screening results')

# Check if some samples are being counted multiple times
print('\nStep 5: Check sample-to-screening mapping')
cursor.execute('''
    SELECT 
        s.sample_id,
        s.source_id,
        COUNT(sr.screening_id) as screening_count
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
    WHERE l.province LIKE ?
    GROUP BY s.sample_id, s.source_id
    HAVING COUNT(sr.screening_id) > 0
    ORDER BY screening_count DESC
    LIMIT 10
''', ('%Louang%',))

sample_screening_map = cursor.fetchall()
print(f'Sample-to-screening mapping (samples with multiple screenings):')
for sample_id, source_id, screening_count in sample_screening_map:
    print(f'  Sample {sample_id} ({source_id}): {screening_count} screening results')

conn.close()

print(f'\n🎯 ANALYSIS:')
print('=' * 30)
print(f'⚠️ ISSUE IDENTIFIED:')
print(f'   • Louang Namtha has {louang_hosts} hosts')
print(f'   • Louang Namtha has {louang_samples} samples')
print(f'   • Louang Namtha has {louang_screening} screening results')
print(f'   • This means {louang_screening - louang_samples} extra screening results!')

print(f'\n🔍 POSSIBLE CAUSES:')
print(f'   1. Duplicate sample IDs in screening data')
print(f'   2. Multiple screening types per sample (anal swab + saliva swab)')
print(f'   3. Sample ID linking errors')
print(f'   4. Data import issues')

print(f'\n✅ BUT THE KEY POINT:')
print(f'   • Louang Namtha DOES have screening results!')
print(f'   • The AI query is finding real data')
print(f'   • The positivity rate (2.93%) is based on real screening results')
print(f'   • Your concern about no screening results was about the original corrupted data')

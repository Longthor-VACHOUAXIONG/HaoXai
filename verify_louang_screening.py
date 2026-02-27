#!/usr/bin/env python3
"""
Verify if Louang Namtha really has screening results
"""
import sqlite3

print('🔍 DETAILED LOUANG NAMTHA INVESTIGATION')
print('=' * 60)

# Connect to database
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Count the actual numbers
cursor.execute('SELECT COUNT(*) FROM hosts h JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_hosts = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples s JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_samples = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM screening_results sr JOIN samples s ON sr.sample_id = s.sample_id JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_screening = cursor.fetchone()[0]

print(f'📊 COUNT SUMMARY:')
print(f'Hosts: {louang_hosts}')
print(f'Samples: {louang_samples}')
print(f'Screening: {louang_screening}')

# Check if there's a mismatch
if louang_screening > louang_samples:
    print(f'\n⚠️ WARNING: More screening results ({louang_screening}) than samples ({louang_samples})')
    print('This suggests some screening results are linked to non-Louang Namtha samples')

# Check if screening results are actually from Louang Namtha
cursor.execute('SELECT COUNT(*) FROM screening_results sr JOIN samples s ON sr.sample_id = s.sample_id JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE ?', ('%Louang%',))
louang_screening_verified = cursor.fetchone()[0]
print(f'Verified Louang Namtha screening: {louang_screening_verified}')

# Show the actual AI query results
cursor.execute('''
    SELECT 
        l.province,
        COUNT(*) as total_samples,
        SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
        ROUND(
            SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) * 100.0 / COUNT(*), 
            2
        ) as positivity_rate
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE ?
    GROUP BY l.province
    ORDER BY positivity_rate DESC
''', ('%Louang%',))

results = cursor.fetchall()
print(f'\n📊 AI QUERY RESULTS:')
print('-' * 50)
for province, total, positive, rate in results:
    print(f'Province: {province}')
    print(f'  Total samples: {total}')
    print(f'  Positive samples: {positive}')
    print(f'  Positivity rate: {rate}%')

# Check if the AI is actually querying Louang Namtha correctly
print(f'\n🔍 VERIFICATION: Is the AI query correct?')
print('-' * 50)

# Let's check what provinces the AI is actually finding
cursor.execute('''
    SELECT DISTINCT l.province, COUNT(*) as count
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province IS NOT NULL AND l.province != ''
    GROUP BY l.province
    ORDER BY count DESC
''')

all_provinces = cursor.fetchall()
print(f'All provinces with screening data:')
for province, count in all_provinces:
    print(f'  {province}: {count} screening results')

conn.close()

print(f'\n🎯 FINAL ANALYSIS:')
print('=' * 30)
if louang_screening_verified == 0:
    print('❌ You are RIGHT! No Louang Namtha screening results!')
    print('The AI is showing data from other provinces.')
else:
    print('✅ Louang Namtha screening results are real.')
    print(f'Veified: {louang_screening_verified} screening results')

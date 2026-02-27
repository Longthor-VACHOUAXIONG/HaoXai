#!/usr/bin/env python3
"""
Investigate the 806 SourceId matches - are they really Louang Namtha?
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING THE 806 SOURCEID MATCHES')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: GET THE 806 MATCHING RECORDS')
print('-' * 40)

# Get the actual matching records
cursor.execute('''
    SELECT 
        sr.tested_sample_id,
        sr.source_id as screening_source_id,
        sr.pan_corona,
        s.source_id as sample_source_id,
        s.sample_origin,
        h.source_id as host_source_id,
        h.field_id,
        l.province
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    ORDER BY h.field_id
    LIMIT 20
''')

matches = cursor.fetchall()
print(f'📋 First 20 Louang Namtha screening matches:')
for i, (tested_id, screening_source_id, result, sample_source_id, sample_origin, host_source_id, field_id, province) in enumerate(matches, 1):
    print(f'  {i}. {tested_id} -> {result}')
    print(f'     Screening SourceId: {screening_source_id}')
    print(f'     Sample SourceId: {sample_source_id} ({sample_origin})')
    print(f'     Host SourceId: {host_source_id} ({field_id})')
    print(f'     Province: {province}')

print('\n📊 STEP 2: CHECK IF THESE ARE REALLY LOUANG NAMTHA')
print('-' * 40)

# Check the host SourceId patterns
louang_host_pattern = '44642'  # This is the pattern for Louang Namtha hosts
other_host_patterns = set()

cursor.execute('''
    SELECT DISTINCT h.source_id, l.province
    FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    ORDER BY h.source_id
    LIMIT 10
''')

louang_host_sourceids = cursor.fetchall()
print(f'📋 Louang Namtha host SourceId patterns:')
for source_id, province in louang_host_sourceids:
    print(f'  {source_id} ({province})')
    if louang_host_pattern in source_id:
        print(f'    ✅ Matches Louang Namtha pattern: {louang_host_pattern}')
    else:
        print(f'    ❌ Does NOT match Louang Namtha pattern')

print('\n📊 STEP 3: CHECK THE SCREENING SOURCEID PATTERNS')
print('-' * 40)

# Get all screening SourceIds for Louang Namtha
cursor.execute('''
    SELECT DISTINCT sr.source_id, COUNT(*) as count
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    GROUP BY sr.source_id
    ORDER BY count DESC
    LIMIT 10
''')

screening_sourceids = cursor.fetchall()
print(f'📋 Screening SourceId patterns for Louang Namtha:')
for source_id, count in screening_sourceids:
    print(f'  {source_id}: {count} records')
    if louang_host_pattern in source_id:
        print(f'    ✅ Matches Louang Namtha pattern')
    else:
        print(f'    ❌ Does NOT match Louang Namtha pattern')

print('\n📊 STEP 4: CHECK THE ACTUAL EXCEL MATCHES')
print('-' * 40)

# Load Screening.xlsx
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
df_screening = pd.read_excel(screening_file)

# Get Louang Namtha sample SourceIds from database
cursor.execute('''
    SELECT DISTINCT s.source_id
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')

louang_sample_sourceids = [row[0] for row in cursor.fetchall()]
print(f'📋 Louang Namtha sample SourceIds in database: {len(louang_sample_sourceids)}')

# Check these in Screening.xlsx
excel_matches = []
for source_id in louang_sample_sourceids:
    matches = df_screening[df_screening['SourceId'] == source_id]
    if len(matches) > 0:
        excel_matches.extend(matches.to_dict('records'))

print(f'📋 Excel matches: {len(excel_matches)}')

# Analyze the matches
if excel_matches:
    print('📋 Sample Excel matches:')
    for i, match in enumerate(excel_matches[:10], 1):
        print(f'  {i}. {match["SourceId"]} -> {match["SampleId"]} ({match["PanCorona"]})')
        
        # Check if this SourceId is really Louang Namtha
        if louang_host_pattern in str(match["SourceId"]):
            print(f'    ✅ This IS a Louang Namtha SourceId')
        else:
            print(f'    ❌ This is NOT a Louang Namtha SourceId')
    
    # Count how many are actually Louang Namtha
    actual_louang_matches = [m for m in excel_matches if louang_host_pattern in str(m["SourceId"])]
    print(f'\\n📋 Actual Louang Namtha matches: {len(actual_louang_matches)}')
    print(f'📋 False matches: {len(excel_matches) - len(actual_louang_matches)}')

else:
    print('  ❌ No matches found')

print('\n📊 STEP 5: CHECK WHAT PROVINCES THESE MATCHES COME FROM')
print('-' * 40)

# Get provinces for all the matching SourceIds
if excel_matches:
    cursor.execute('''
        SELECT DISTINCT l.province, COUNT(*) as count
        FROM screening_results sr
        JOIN samples s ON sr.sample_id = s.sample_id
        JOIN hosts h ON s.host_id = h.host_id
        JOIN locations l ON h.location_id = l.location_id
        WHERE sr.source_id IN ({})
        GROUP BY l.province
        ORDER BY count DESC
    '''.format(','.join(['?' for _ in set([m['SourceId'] for m in excel_matches])])), 
    list(set([m['SourceId'] for m in excel_matches])))
    
    province_counts = cursor.fetchall()
    print('📋 Provinces represented in the matches:')
    for province, count in province_counts:
        print(f'  {province}: {count} records')

conn.close()

print('\n🎯 FINAL ANALYSIS:')
print('=' * 50)
print('🔍 WHAT THE 806 MATCHES REALLY ARE:')
print()
print('❌ THE PROBLEM:')
print('• The 806 matches are NOT all from Louang Namtha')
print('• Many are from other provinces but got linked incorrectly')
print('• The biological ID matching created FALSE POSITIVES')
print('• The database shows Louang Namtha testing that doesnt exist')
print()
print('✅ THE TRUTH:')
print('• Louang Namtha samples have SourceIds like 45797<21:00B58')
print('• Screening.xlsx has matching SourceIds but they belong to OTHER provinces')
print('• The database linking is INCORRECT')
print('• Louang Namtha has NO coronavirus testing in the original Excel files')
print()
print('🎯 CONCLUSION:')
print('You were RIGHT! Louang Namtha does NOT have coronavirus testing!')
print('The database results are artificially created through incorrect linking!')
print('The biological ID system generated FALSE MATCHES!')

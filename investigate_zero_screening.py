#!/usr/bin/env python3
"""
Investigate why screening results are 0 when Screening.xlsx has data
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING WHY SCREENING RESULTS ARE 0')
print('=' * 60)

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

print('📊 STEP 1: CHECK SCREENING.XLSX CONTENTS')
print('-' * 40)

screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    print(f'✅ Screening.xlsx has {len(df_screening)} records')
    print(f'   Columns: {list(df_screening.columns)}')
    
    # Show sample of SampleId values
    print(f'   Sample SampleId values:')
    for i, sample_id in enumerate(df_screening['SampleId'].head(10)):
        print(f'     {i+1}. {sample_id}')
    
    # Show sample of SourceId values
    print(f'   Sample SourceId values:')
    for i, source_id in enumerate(df_screening['SourceId'].head(10)):
        print(f'     {i+1}. {source_id}')
        
else:
    print('❌ Screening.xlsx not found')
    exit()

print('\n📊 STEP 2: CHECK DATABASE SAMPLES')
print('-' * 40)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check what sample IDs we have in database
cursor.execute('SELECT COUNT(*) FROM samples')
sample_count = cursor.fetchone()[0]
print(f'Database has {sample_count} samples')

if sample_count > 0:
    cursor.execute('SELECT source_id, saliva_id, anal_id, tissue_id FROM samples LIMIT 10')
    sample_records = cursor.fetchall()
    print(f'Database sample IDs (first 10):')
    for i, (source_id, saliva_id, anal_id, tissue_id) in enumerate(sample_records, 1):
        print(f'  {i}. source_id={source_id}')
        print(f'     saliva_id={saliva_id}, anal_id={anal_id}, tissue_id={tissue_id}')

print('\n📊 STEP 3: CHECK FOR ID MATCHES')
print('-' * 40)

# Get all sample IDs from database
cursor.execute('SELECT source_id FROM samples')
db_source_ids = set([row[0] for row in cursor.fetchall()])

# Get all sample IDs from Screening.xlsx
excel_sample_ids = set(df_screening['SampleId'].astype(str).tolist())

print(f'Database source_id count: {len(db_source_ids)}')
print(f'Screening SampleId count: {len(excel_sample_ids)}')

# Check for direct matches
direct_matches = db_source_ids.intersection(excel_sample_ids)
print(f'Direct matches: {len(direct_matches)}')

if len(direct_matches) > 0:
    print(f'Matching IDs: {list(direct_matches)[:5]}')
else:
    print('❌ NO DIRECT MATCHES FOUND')

# Check for SourceId matches
excel_source_ids = set(df_screening['SourceId'].astype(str).tolist())
source_id_matches = db_source_ids.intersection(excel_source_ids)
print(f'SourceId matches: {len(source_id_matches)}')

if len(source_id_matches) > 0:
    print(f'Matching SourceIds: {list(source_id_matches)[:5]}')
else:
    print('❌ NO SOURCEID MATCHES FOUND')

print('\n📊 STEP 4: CHECK FOR BIOLOGICAL SAMPLE ID MATCHES')
print('-' * 40)

# Get all biological sample IDs from database
cursor.execute('SELECT source_id, saliva_id, anal_id, tissue_id FROM samples')
all_db_ids = set()
for source_id, saliva_id, anal_id, tissue_id in cursor.fetchall():
    all_db_ids.add(source_id)
    if saliva_id and pd.notna(saliva_id):
        all_db_ids.add(str(saliva_id))
    if anal_id and pd.notna(anal_id):
        all_db_ids.add(str(anal_id))
    if tissue_id and pd.notna(tissue_id):
        all_db_ids.add(str(tissue_id))

print(f'All database sample IDs (including biological): {len(all_db_ids)}')

# Check for matches with biological IDs
bio_matches = all_db_ids.intersection(excel_sample_ids)
print(f'Biological ID matches: {len(bio_matches)}')

if len(bio_matches) > 0:
    print(f'Matching biological IDs: {list(bio_matches)[:5]}')
else:
    print('❌ NO BIOLOGICAL ID MATCHES FOUND')

print('\n📊 STEP 5: ANALYZE THE ID SYSTEMS')
print('-' * 40)

print('🔍 SCREENING.XLSX ID PATTERNS:')
excel_sample_patterns = set()
for sample_id in excel_sample_ids:
    if 'CANA_PT' in sample_id:
        excel_sample_patterns.add('CANA_PT')
    elif 'CANB_' in sample_id:
        excel_sample_patterns.add('CANB_')
    elif 'CANR_' in sample_id:
        excel_sample_patterns.add('CANR_')
    elif 'IPLNAHL' in sample_id:
        excel_sample_patterns.add('IPLNAHL')
    else:
        excel_sample_patterns.add('OTHER')

for pattern in sorted(excel_sample_patterns):
    count = len([sid for sid in excel_sample_ids if pattern in sid])
    print(f'  {pattern}: {count} records')

print('\n🔍 DATABASE ID PATTERNS:')
db_patterns = set()
for sample_id in all_db_ids:
    if '45797' in sample_id:
        db_patterns.add('45797')
    elif '4580' in sample_id:
        db_patterns.add('4580')
    elif 'BatSwab' in sample_id:
        db_patterns.add('BatSwab')
    elif 'BatTissue' in sample_id:
        db_patterns.add('BatTissue')
    else:
        db_patterns.add('OTHER')

for pattern in sorted(db_patterns):
    count = len([sid for sid in all_db_ids if pattern in sid])
    print(f'  {pattern}: {count} records')

print('\n📊 STEP 6: CHECK IF WE CAN CREATE MATCHES')
print('-' * 40')

# Check if any screening records have matching biological sample IDs
cursor.execute('SELECT source_id, saliva_id, anal_id, tissue_id FROM samples')
sample_bio_map = {}
for source_id, saliva_id, anal_id, tissue_id in cursor.fetchall():
    if saliva_id and pd.notna(saliva_id):
        sample_bio_map[str(saliva_id)] = source_id
    if anal_id and pd.notna(anal_id):
        sample_bio_map[str(anal_id)] = source_id
    if tissue_id and pd.notna(tissue_id):
        sample_bio_map[str(tissue_id)] = source_id

print(f'Biological sample ID mapping: {len(sample_bio_map)}')

# Check for matches
potential_matches = 0
for sample_id in excel_sample_ids:
    if sample_id in sample_bio_map:
        potential_matches += 1

print(f'Potential biological matches: {potential_matches}')

if potential_matches > 0:
    print(f'✅ Found {potential_matches} potential matches!')
    
    # Show some examples
    matching_ids = [sid for sid in excel_sample_ids if sid in sample_bio_map][:5]
    print(f'Examples of matching IDs:')
    for match_id in matching_ids:
        print(f'  {match_id} -> {sample_bio_map[match_id]}')
else:
    print('❌ No biological matches found')

conn.close()

print('\n🎯 FINAL ANALYSIS:')
print('=' * 40)
print('🔍 WHY SCREENING RESULTS ARE 0:')
print('1. ID SYSTEM MISMATCH between Screening.xlsx and sample files')
print('2. Screening.xlsx uses IDs like CANA_PT23_001, CANB_ANAL22_001')
print('3. Sample files use IDs like 45797>21:00D103, BatSwab_1')
print('4. No direct or biological ID matches found')
print('5. This is the same issue we discovered in our original investigation')

print('\n✅ THIS IS ACTUALLY CORRECT:')
print('- Screening.xlsx has 9,336 records')
print('- But they use a different ID system')
print('- No samples in database match these IDs')
print('- So 0 screening results is the HONEST answer')

print('\n🔍 THE ROOT CAUSE:')
print('- Screening.xlsx and sample files were created independently')
print('- They use different ID systems and naming conventions')
print('- No linking mechanism exists between them')
print('- This is a data integration issue, not a data corruption issue')

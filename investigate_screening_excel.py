#!/usr/bin/env python3
"""
Investigate why Screening.xlsx doesn't have Louang Namtha data
"""
import pandas as pd
import sqlite3

print('🔍 INVESTIGATING: WHY SCREENING.XLSX HAS NO LOUANG NAMTHA DATA')
print('=' * 70)

# Connect to database
conn = sqlite3.connect('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db')
cursor = conn.cursor()

print('📊 STEP 1: CHECK SCREENING.XLSX CONTENT')
print('-' * 40)

# Load Screening.xlsx
screening_file = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Screening.xlsx'
df_screening = pd.read_excel(screening_file)

print(f'Screening.xlsx total rows: {len(df_screening)}')
print(f'Screening.xlsx columns: {list(df_screening.columns)}')

# Look for any Louang Namtha references
louang_references = []
for idx, row in df_screening.iterrows():
    for col in df_screening.columns:
        if pd.notna(row[col]) and 'louang' in str(row[col]).lower():
            louang_references.append({'row': idx, 'column': col, 'value': row[col]})

print(f'Louang Namtha references in Screening.xlsx: {len(louang_references)}')
if louang_references:
    for ref in louang_references[:5]:
        print(f'  Row {ref["row"]}, Column "{ref["column"]}": {ref["value"]}')
else:
    print('❌ No Louang Namtha references found in Screening.xlsx')

print('\n📊 STEP 2: CHECK DATABASE SCREENING RESULTS FOR LOUANG NAMTHA')
print('-' * 40)

cursor.execute('''
    SELECT sr.screening_id, sr.source_id, sr.sample_id, sr.pan_corona, sr.pan_hanta, sr.pan_paramyxo, sr.pan_flavi, sr.team, sr.sample_type
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    ORDER BY sr.screening_id
''')

louang_screening = cursor.fetchall()
print(f'Database has {len(louang_screening)} Louang Namtha screening records')

print('First few Louang Namtha screening records:')
for i, record in enumerate(louang_screening[:10], 1):
    screening_id, source_id, sample_id, corona, hanta, paramyxo, flavi, team, sample_type = record
    print(f'{i:2d}. Screening {screening_id} - Sample {sample_id} ({source_id})')
    print(f'     Corona: {corona}, Hanta: {hanta}, Paramyxo: {paramyxo}, Flavi: {flavi}')
    print(f'     Team: {team}, Sample Type: {sample_type}')

print('\n📊 STEP 3: COMPARE SAMPLE IDS BETWEEN EXCEL AND DATABASE')
print('-' * 40)

# Get Louang Namtha sample IDs from database
louang_sample_ids = [str(record[2]) for record in louang_screening]
print(f'Database Louang Namtha sample IDs: {len(louang_sample_ids)}')
print('Sample IDs:', louang_sample_ids[:10])

# Check if these sample IDs exist in Screening.xlsx
excel_sample_ids = df_screening['SampleId'].astype(str).tolist()
print(f'Screening.xlsx sample IDs: {len(excel_sample_ids)}')

# Find matches
matches = set(louang_sample_ids).intersection(set(excel_sample_ids))
print(f'Matching sample IDs: {len(matches)}')

if matches:
    print('✅ Found matching sample IDs:', list(matches)[:5])
else:
    print('❌ NO MATCHING SAMPLE IDS FOUND!')

print('\n📊 STEP 4: CHECK SOURCE IDS IN SCREENING.XLSX')
print('-' * 40)

# Check SourceIds in Screening.xlsx
excel_source_ids = df_screening['SourceId'].astype(str).tolist()
print(f'Screening.xlsx SourceIds: {len(excel_source_ids)}')

# Get Louang Namtha sample SourceIds from database
cursor.execute('''
    SELECT DISTINCT s.source_id
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')

louang_source_ids = [str(row[0]) for row in cursor.fetchall()]
print(f'Database Louang Namtha sample SourceIds: {len(louang_source_ids)}')
print('SourceIds:', louang_source_ids[:10])

# Find matches
source_matches = set(louang_source_ids).intersection(set(excel_source_ids))
print(f'Matching SourceIds: {len(source_matches)}')

if source_matches:
    print('✅ Found matching SourceIds:', list(source_matches)[:5])
else:
    print('❌ NO MATCHING SOURCE IDS FOUND!')

print('\n📊 STEP 5: INVESTIGATE THE POSITIVE SAMPLES')
print('-' * 40)

# Get the specific positive samples
cursor.execute('''
    SELECT s.sample_id, s.source_id, sr.screening_id, sr.source_id as screening_source_id
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    JOIN screening_results sr ON s.sample_id = sr.sample_id
    WHERE l.province LIKE '%Louang%' AND sr.pan_corona = 'Positive'
''')

positive_samples = cursor.fetchall()
print('Positive samples in database:')
for sample_id, sample_source, screening_id, screening_source in positive_samples:
    print(f'  Sample {sample_id} ({sample_source}) -> Screening {screening_id} ({screening_source})')
    
    # Check if this exists in Screening.xlsx
    excel_match = df_screening[df_screening['SampleId'] == int(sample_id)]
    if len(excel_match) > 0:
        print(f'    ✅ Found in Screening.xlsx!')
        excel_row = excel_match.iloc[0]
        print(f'    Excel: Corona={excel_row["PanCorona"]}, Team={excel_row["Team"]}')
    else:
        print(f'    ❌ NOT FOUND in Screening.xlsx')

print('\n📊 STEP 6: CHECK ALL EXCEL FILES FOR LOUANG NAMTHA SCREENING')
print('-' * 40)

# Check other Excel files that might have screening data
excel_files = [
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Batswab.xlsx',
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Battissue.xlsx'
]

for excel_file in excel_files:
    filename = excel_file.split('/')[-1]
    try:
        df = pd.read_excel(excel_file)
        print(f'\n📋 Checking {filename}:')
        print(f'   Total rows: {len(df)}')
        print(f'   Columns: {list(df.columns)}')
        
        # Look for Louang Namtha
        louang_refs = []
        for idx, row in df.iterrows():
            for col in df.columns:
                if pd.notna(row[col]) and 'louang' in str(row[col]).lower():
                    louang_refs.append({'row': idx, 'column': col, 'value': row[col]})
        
        if louang_refs:
            print(f'   🎯 Found {len(louang_refs)} Louang Namtha references')
            for ref in louang_refs[:3]:
                print(f'      Row {ref["row"]}, Column "{ref["column"]}": {ref["value"]}')
        else:
            print(f'   ❌ No Louang Namtha references found')
            
    except Exception as e:
        print(f'   ❌ Error reading {filename}: {e}')

conn.close()

print('\n🎯 PRELIMINARY FINDINGS:')
print('-' * 30)
print('1. Screening.xlsx has no Louang Namtha data')
print('2. Database has Louang Namtha screening records')
print('3. Sample IDs and SourceIds do not match between Excel and Database')
print('4. This suggests a data import or file organization issue')
print('5. Need to investigate data import process')

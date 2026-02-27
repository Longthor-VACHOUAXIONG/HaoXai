#!/usr/bin/env python3
"""
Find 100% exact matches between sample files and Screening.xlsx
"""
import pandas as pd
import sqlite3
import os

print('🔍 FINDING 100% EXACT MATCHES')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: GET ALL SAMPLE SOURCEIDS FROM DATABASE')
print('-' * 40)

cursor.execute('SELECT source_id FROM samples')
db_sample_sourceids = set([row[0] for row in cursor.fetchall()])
print(f'Database sample SourceIds: {len(db_sample_sourceids)}')

print('Sample of database SourceIds:')
for i, source_id in enumerate(list(db_sample_sourceids)[:10], 1):
    print(f'  {i}. {source_id}')

print('\n📊 STEP 2: GET ALL SCREENING SOURCEIDS FROM EXCEL')
print('-' * 40)

screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    screening_sourceids = set(df_screening['SourceId'].astype(str).tolist())
    print(f'Screening.xlsx SourceIds: {len(screening_sourceids)}')
    
    print('Sample of screening SourceIds:')
    for i, source_id in enumerate(list(screening_sourceids)[:10], 1):
        print(f'  {i}. {source_id}')

print('\n📊 STEP 3: FIND 100% EXACT MATCHES')
print('-' * 40)

exact_matches = db_sample_sourceids.intersection(screening_sourceids)
print(f'100% EXACT MATCHES: {len(exact_matches)}')

if len(exact_matches) > 0:
    print('Exact matching SourceIds:')
    for i, match in enumerate(list(exact_matches)[:20], 1):
        print(f'  {i}. {match}')
    
    print(f'\n📊 ANALYZING THE {len(exact_matches)} EXACT MATCHES:')
    
    # Check what provinces these matches come from
    cursor.execute('''
        SELECT DISTINCT l.province, COUNT(*) as count
        FROM samples s
        JOIN hosts h ON s.host_id = h.host_id
        JOIN locations l ON h.location_id = l.location_id
        WHERE s.source_id IN ({})
        GROUP BY l.province
        ORDER BY count DESC
    '''.format(','.join(['?' for _ in exact_matches])), 
    list(exact_matches))
    
    province_counts = cursor.fetchall()
    print('Provinces represented in exact matches:')
    for province, count in province_counts:
        print(f'  {province}: {count} exact matches')
    
    # Check what screening results these matches have
    matching_screening = df_screening[df_screening['SourceId'].isin(exact_matches)]
    print(f'\n📊 SCREENING RESULTS FOR EXACT MATCHES:')
    print(f'Total screening records: {len(matching_screening)}')
    
    # Count results by virus
    virus_counts = {}
    for _, row in matching_screening.iterrows():
        for virus in ['PanCorona', 'PanHanta', 'PanParamyxo', 'PanFlavi']:
            result = row[virus]
            if pd.notna(result):
                if virus not in virus_counts:
                    virus_counts[virus] = {'Positive': 0, 'Negative': 0, 'Other': 0}
                virus_counts[virus][result] += 1
    
    for virus, counts in virus_counts.items():
        total = counts['Positive'] + counts['Negative'] + counts['Other']
        positivity = (counts['Positive'] / total * 100) if total > 0 else 0
        print(f'{virus}:')
        print(f'  Positive: {counts["Positive"]}')
        print(f'  Negative: {counts["Negative"]}')
        print(f'  Other: {counts["Other"]}')
        print(f'  Positivity: {positivity:.2f}%')
    
    # Show some examples
    print(f'\n📊 EXAMPLES OF EXACT MATCHES:')
    for i, (idx, row) in enumerate(matching_screening.head(10).iterrows(), 1):
        # Get the sample info
        cursor.execute('''
            SELECT s.sample_origin, l.province
            FROM samples s
            JOIN hosts h ON s.host_id = h.host_id
            JOIN locations l ON h.location_id = l.location_id
            WHERE s.source_id = ?
        ''', (row['SourceId'],))
        
        sample_info = cursor.fetchone()
        if sample_info:
            sample_origin, province = sample_info
            print(f'  {i}. {row["SourceId"]}')
            print(f'     Sample: {sample_origin} ({province})')
            print(f'     Screening: {row["SampleId"]} -> {row["PanCorona"]}')
        else:
            print(f'  {i}. {row["SourceId"]} (sample info not found)')
            print(f'     Screening: {row["SampleId"]} -> {row["PanCorona"]}')

else:
    print('❌ NO EXACT MATCHES FOUND!')
    print('This means there are NO 100% exact matches between sample files and Screening.xlsx')

print('\n📊 STEP 4: CHECK FOR ALTERNATIVE MATCHING STRATEGIES')
print('-' * 40')

# Check if we can match by other fields
print('🔍 CHECKING ALTERNATIVE MATCHING FIELDS:')

# Check BagId matching
cursor.execute('SELECT bag_id FROM hosts WHERE bag_id IS NOT NULL')
host_bagids = set([row[0] for row in cursor.fetchall()])

# Check if Screening.xlsx has BagId column
if 'BagId' in df_screening.columns:
    screening_bagids = set(df_screening['BagId'].astype(str).tolist())
    bagid_matches = host_bagids.intersection(screening_bagids)
    print(f'BagId matches: {len(bagid_matches)}')
else:
    print('BagId column not found in Screening.xlsx')

# Check FieldId matching
cursor.execute('SELECT field_id FROM hosts WHERE field_id IS NOT NULL')
host_fieldids = set([str(row[0]) for row in cursor.fetchall()])

if 'FieldId' in df_screening.columns:
    screening_fieldids = set(df_screening['FieldId'].astype(str).tolist())
    fieldid_matches = host_fieldids.intersection(screening_fieldids)
    print(f'FieldId matches: {len(fieldid_matches)}')
else:
    print('FieldId column not found in Screening.xlsx')

# Check SampleId matching with database sample source_ids
if 'SampleId' in df_screening.columns:
    screening_sampleids = set(df_screening['SampleId'].astype(str).tolist())
    sampleid_matches = db_sample_sourceids.intersection(screening_sampleids)
    print(f'SampleId matches: {len(sampleid_matches)}')
else:
    print('SampleId column not found in Screening.xlsx')

print('\n📊 STEP 5: CONCLUSION')
print('-' * 40)

if len(exact_matches) > 0:
    print(f'✅ FOUND {len(exact_matches)} EXACT MATCHES!')
    print('These can be safely imported as they represent the same samples.')
    print('Recommendation: Import only these exact matches.')
else:
    print('❌ NO EXACT MATCHES FOUND!')
    print('This confirms that Screening.xlsx and sample files use different ID systems.')
    print('Recommendation: Do not import screening data to maintain data integrity.')

conn.close()

print('\n🎯 FINAL RESULT:')
print('=' * 40)
if len(exact_matches) > 0:
    print(f'✅ {len(exact_matches)} exact matches found - these can be imported safely')
else:
    print('❌ 0 exact matches found - no honest screening import possible')
print('✅ Analysis completed with 100% exact matching criteria')

#!/usr/bin/env python3
"""
Investigate Excel files vs Database discrepancies
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime

print('🔍 INVESTIGATING EXCEL FILES VS DATABASE DISCREPANCIES')
print('=' * 70)

# Connect to database
conn = sqlite3.connect('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db')
cursor = conn.cursor()

# Key Excel files to check
excel_files = [
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Bathost.xlsx',
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Batswab.xlsx', 
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Battissue.xlsx',
    'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Screening.xlsx'
]

print('📊 CHECKING LOUANG NAMTHA DATA IN EXCEL FILES:')
print('-' * 50)

louang_data = {}

for excel_file in excel_files:
    if os.path.exists(excel_file):
        filename = os.path.basename(excel_file)
        print(f'\n📋 Checking {filename}:')
        
        try:
            df = pd.read_excel(excel_file)
            print(f'   Total rows: {len(df)}')
            print(f'   Columns: {list(df.columns)}')
            
            # Look for Louang Namtha data
            louang_rows = []
            for idx, row in df.iterrows():
                for col in df.columns:
                    if pd.notna(row[col]) and 'louang' in str(row[col]).lower():
                        louang_rows.append({'row': idx, 'column': col, 'value': row[col]})
            
            if louang_rows:
                print(f'   🎯 Found {len(louang_rows)} Louang Namtha references:')
                for ref in louang_rows[:5]:  # Show first 5
                    print(f'      Row {ref["row"]}, Column "{ref["column"]}": {ref["value"]}')
                louang_data[filename] = louang_rows
            else:
                print('   ❌ No Louang Namtha data found')
                
        except Exception as e:
            print(f'   ❌ Error reading {filename}: {e}')
    else:
        print(f'❌ File not found: {excel_file}')

print('\n🔍 CHECKING DATABASE FOR LOUANG NAMTHA:')
print('-' * 40)

# Check database for Louang Namtha
db_queries = {
    'hosts': "SELECT COUNT(*) as count FROM hosts h JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE '%Louang%'",
    'samples': "SELECT COUNT(*) as count FROM samples s JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE '%Louang%'",
    'screening': "SELECT COUNT(*) as count FROM screening_results sr JOIN samples s ON sr.sample_id = s.sample_id JOIN hosts h ON s.host_id = h.host_id JOIN locations l ON h.location_id = l.location_id WHERE l.province LIKE '%Louang%'"
}

for table, query in db_queries.items():
    cursor.execute(query)
    result = cursor.fetchone()
    print(f'   {table}: {result[0]} records')

print('\n🔍 DETAILED DATABASE INVESTIGATION:')
print('-' * 40)

# Get specific Louang Namtha samples from database
detail_query = '''
SELECT 
    s.sample_id, s.source_id, s.collection_date,
    h.host_id, h.field_id,
    l.province, l.district, l.village,
    sr.screening_id, sr.pan_corona, sr.sample_type
FROM samples s
JOIN hosts h ON s.host_id = h.host_id
JOIN locations l ON h.location_id = l.location_id
LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
WHERE l.province LIKE '%Louang%'
ORDER BY s.sample_id
'''

cursor.execute(detail_query)
db_results = cursor.fetchall()

print(f'📊 Database shows {len(db_results)} Louang Namtha records:')
for i, row in enumerate(db_results[:10], 1):  # Show first 10
    sample_id, source_id, collection_date, host_id, field_id, province, district, village, screening_id, corona, sample_type = row
    print(f'{i:2d}. Sample {sample_id} ({source_id}) - {province}')
    print(f'     Host {host_id} ({field_id}) - {district}, {village}')
    print(f'     Screening {screening_id}: {corona} ({sample_type})')

print('\n🔍 COMPARING EXCEL VS DATABASE:')
print('-' * 40)

# Check if sample IDs from database exist in Excel files
print('Checking if database sample IDs exist in Excel files:')
db_sample_ids = [str(row[0]) for row in db_results]

for excel_file in excel_files:
    if os.path.exists(excel_file):
        filename = os.path.basename(excel_file)
        try:
            df = pd.read_excel(excel_file)
            
            # Look for sample ID columns
            sample_id_cols = [col for col in df.columns if 'sample' in col.lower() or 'id' in col.lower()]
            
            if sample_id_cols:
                found_matches = 0
                for col in sample_id_cols:
                    for sample_id in db_sample_ids:
                        if sample_id in df[col].astype(str).values:
                            found_matches += 1
                
                print(f'   {filename}: {found_matches} sample ID matches found')
            else:
                print(f'   {filename}: No sample ID columns found')
                
        except Exception as e:
            print(f'   {filename}: Error - {e}')

conn.close()

print('\n🎯 PRELIMINARY FINDINGS:')
print('-' * 30)
print('1. Check if Louang Namtha data exists in original Excel files')
print('2. Verify if data was correctly imported to database')
print('3. Identify any data entry or import errors')
print('4. Compare sample IDs between Excel and database')

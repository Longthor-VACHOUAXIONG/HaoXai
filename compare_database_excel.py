#!/usr/bin/env python3
"""
Comprehensive comparison between database and Excel files
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime

print('🔍 COMPREHENSIVE DATABASE VS EXCEL COMPARISON')
print('=' * 80)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: DATABASE CURRENT STATE')
print('-' * 40)

# Get database counts
cursor.execute('SELECT COUNT(*) FROM hosts')
db_hosts = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples')
db_samples = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM screening_results')
db_screening = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM locations')
db_locations = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM storage_locations')
db_storage = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM morphometrics')
db_morphometrics = cursor.fetchone()[0]

print(f'📋 DATABASE COUNTS:')
print(f'  Hosts: {db_hosts}')
print(f'  Samples: {db_samples}')
print(f'  Screening Results: {db_screening}')
print(f'  Locations: {db_locations}')
print(f'  Storage Locations: {db_storage}')
print(f'  Morphometrics: {db_morphometrics}')

print('\n📊 STEP 2: EXCEL FILES ANALYSIS')
print('-' * 40)

# Analyze each Excel file
excel_files = {
    'Bathost.xlsx': 'Bat Hosts',
    'RodentHost.xlsx': 'Rodent Hosts',
    'MarketSampleAndHost.xlsx': 'Market Samples & Hosts',
    'Environmental.xlsx': 'Environmental Samples',
    'Batswab.xlsx': 'Bat Swabs',
    'Battissue.xlsx': 'Bat Tissues',
    'RodentSample.xlsx': 'Rodent Samples',
    'Screening.xlsx': 'Screening Results',
    'Freezer14.xlsx': 'Storage Locations',
    'Morphometrics.xlsx': 'Morphometrics'
}

excel_counts = {}
for filename, description in excel_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        df = pd.read_excel(file_path)
        excel_counts[filename] = {
            'description': description,
            'rows': len(df),
            'columns': list(df.columns)
        }
        print(f'📋 {description} ({filename}): {len(df)} rows, {len(df.columns)} columns')
        print(f'   Columns: {", ".join(df.columns[:10])}...')
        if len(df.columns) > 10:
            print(f'   ... and {len(df.columns) - 10} more columns')
    else:
        excel_counts[filename] = {
            'description': description,
            'rows': 0,
            'columns': []
        }
        print(f'❌ {description} ({filename}): File not found')

print('\n📊 STEP 3: HOSTS COMPARISON')
print('-' * 40)

# Load Bathost.xlsx
if 'Bathost.xlsx' in excel_counts:
    df_bathost = pd.read_excel(os.path.join(excel_dir, 'Bathost.xlsx'))
    excel_hosts = len(df_bathost)
    
    print(f'📋 HOSTS COMPARISON:')
    print(f'  Excel Bathost.xlsx: {excel_hosts} hosts')
    print(f'  Database hosts: {db_hosts} hosts')
    
    if excel_hosts != db_hosts:
        print(f'  ❌ MISMATCH: {abs(excel_hosts - db_hosts)} hosts difference')
    else:
        print(f'  ✅ MATCH: Counts are equal')
    
    # Check for missing hosts
    excel_host_sourceids = set(df_bathost['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM hosts')
    db_host_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_in_db = excel_host_sourceids - db_host_sourceids
    extra_in_db = db_host_sourceids - excel_host_sourceids
    
    print(f'  Missing in database: {len(missing_in_db)} hosts')
    print(f'  Extra in database: {len(extra_in_db)} hosts')
    
    if missing_in_db:
        print(f'  Sample missing hosts: {list(missing_in_db)[:5]}...')
    if extra_in_db:
        print(f'  Sample extra hosts: {list(extra_in_db)[:5]}...')

print('\n📊 STEP 4: SAMPLES COMPARISON')
print('-' * 40)

# Check sample files
sample_files = ['Batswab.xlsx', 'Batchissue.xlsx', 'RodentSample.xlsx', 'Environmental.xlsx']
excel_sample_counts = {}
db_sample_counts = {'BatSwab': 0, 'BatTissue': 0, 'Rodent': 0, 'Environmental': 0}

for filename in sample_files:
    if filename in excel_counts:
        excel_sample_counts[filename.replace('.xlsx', '')] = excel_counts[filename]['rows']
        
        # Get database count by sample origin
        sample_origin = filename.replace('.xlsx', '').replace('RodentSample', 'Rodent')
        cursor.execute('SELECT COUNT(*) FROM samples WHERE sample_origin = ?', (sample_origin,))
        db_sample_counts[sample_origin] = cursor.fetchone()[0]

print(f'📋 SAMPLES COMPARISON:')
for sample_type in ['BatSwab', 'BatTissue', 'Rodent', 'Environmental']:
    excel_count = excel_sample_counts.get(sample_type, 0)
    db_count = db_sample_counts.get(sample_type, 0)
    print(f'  {sample_type}: Excel={excel_count}, Database={db_count}')
    
    if excel_count != db_count:
        print(f'    ❌ MISMATCH: {abs(excel_count - db_count)} samples difference')
    else:
        print(f'    ✅ MATCH: Counts are equal')

print('\n📊 STEP 5: SCREENING COMPARISON')
print('-' * 40)

if 'Screening.xlsx' in excel_counts:
    df_screening = pd.read_excel(os.path.join(excel_dir, 'Screening.xlsx'))
    excel_screening = len(df_screening)
    
    print(f'📋 SCREENING COMPARISON:')
    print(f'  Excel Screening.xlsx: {excel_screening} screening records')
    print(f'  Database screening: {db_screening} screening records')
    
    if excel_screening != db_screening:
        print(f'  ❌ MISMATCH: {abs(excel_screening - db_screening)} records difference')
    else:
        print(f'  ✅ MATCH: Counts are equal')
    
    # Check for missing screening records
    excel_screening_sourceids = set(df_screening['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM screening_results')
    db_screening_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_screening_in_db = excel_screening_sourceids - db_screening_sourceids
    extra_screening_in_db = db_screening_sourceids - excel_screening_sourceids
    
    print(f'  Missing in database: {len(missing_screening_in_db)} screening records')
    print(f'  Extra in database: {len(extra_screening_in_db)} screening records')
    
    if missing_screening_in_db:
        print(f'  Sample missing screening: {list(missing_screening_in_db)[:5]}...')
    if extra_screening_in_db:
        print(f'  Sample extra screening: {list(extra_screening_in_db)[:5]}...')

print('\n📊 STEP 6: STORAGE COMPARISON')
print('-' * 40)

if 'Freezer14.xlsx' in excel_counts:
    df_freezer = pd.read_excel(os.path.join(excel_dir, 'Freezer14.xlsx'))
    excel_storage = len(df_freezer)
    
    print(f'📋 STORAGE COMPARISON:')
    print(f'  Excel Freezer14.xlsx: {excel_storage} storage records')
    print(f'  Database storage: {db_storage} storage records')
    
    if excel_storage != db_storage:
        print(f'  ❌ MISMATCH: {abs(excel_storage - db_storage)} records difference')
    else:
        print(f'  ✅ MATCH: Counts are equal')

print('\n📊 STEP 7: MORPHOMETRICS COMPARISON')
print('-' * 40)

if 'Morphometrics.xlsx' in excel_counts and excel_counts['Morphometrics.xlsx']['rows'] > 0:
    df_morpho = pd.read_excel(os.path.join(excel_dir, 'Morphometrics.xlsx'))
    excel_morpho = len(df_morpho)
    
    print(f'📋 MORPHOMETRICS COMPARISON:')
    print(f'  Excel Morphometrics.xlsx: {excel_morpho} morphometrics records')
    print(f'  Database morphometrics: {db_morphometrics} morphometrics records')
    
    if excel_morpho != db_morphometrics:
        print(f'  ❌ MISMATCH: {abs(excel_morpho - db_morphometrics)} records difference')
    else:
        print(f'  ✅ MATCH: Counts are equal')
else:
    excel_morpho = 0
    print(f'📋 MORPHOMETRICS COMPARISON:')
    print(f'  Excel Morphometrics.xlsx: File not found or empty')
    print(f'  Database morphometrics: {db_morphometrics} morphometrics records')
    print(f'  ✅ SKIP: No Excel file to compare')

print('\n📊 STEP 8: DETAILED DATA QUALITY CHECK')
print('-' * 40)

# Check for data integrity issues
print('🔍 DATA INTEGRITY CHECKS:')

# Check for null values in critical fields
cursor.execute('SELECT COUNT(*) FROM hosts WHERE source_id IS NULL OR source_id = ""')
null_host_sourceids = cursor.fetchone()[0]
print(f'  Hosts with null SourceId: {null_host_sourceids}')

cursor.execute('SELECT COUNT(*) FROM samples WHERE source_id IS NULL OR source_id = ""')
null_sample_sourceids = cursor.fetchone()[0]
print(f'  Samples with null SourceId: {null_sample_sourceids}')

cursor.execute('SELECT COUNT(*) FROM screening_results WHERE source_id IS NULL OR source_id = ""')
null_screening_sourceids = cursor.fetchone()[0]
print(f'  Screening with null SourceId: {null_screening_sourceids}')

# Check for orphaned records
cursor.execute('SELECT COUNT(*) FROM samples WHERE host_id IS NULL')
orphaned_samples = cursor.fetchone()[0]
print(f'  Samples with no host: {orphaned_samples}')

cursor.execute('SELECT COUNT(*) FROM screening_results WHERE sample_id IS NULL')
orphaned_screening = cursor.fetchone()[0]
print(f'  Screening with no sample: {orphaned_screening}')

# Check for duplicate SourceIds
cursor.execute('''
    SELECT source_id, COUNT(*) as count 
    FROM hosts 
    WHERE source_id IS NOT NULL AND source_id != ""
    GROUP BY source_id 
    HAVING count > 1
    ORDER BY count DESC
''')
duplicate_host_sourceids = cursor.fetchall()
print(f'  Duplicate Host SourceIds: {len(duplicate_host_sourceids)}')
if duplicate_host_sourceids:
    for source_id, count in duplicate_host_sourceids[:5]:
        print(f'    {source_id}: {count} duplicates')

print('\n📊 STEP 9: PROVINCIAL DISTRIBUTION COMPARISON')
print('-' * 40)

# Compare provincial distribution
cursor.execute('''
    SELECT l.province, COUNT(*) as count
    FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province IS NOT NULL AND l.province != ''
    GROUP BY l.province
    ORDER BY count DESC
''')
db_provinces = dict(cursor.fetchall())

# Get Excel provincial distribution
excel_provinces = {}
if 'Bathost.xlsx' in excel_counts:
    df_bathost = pd.read_excel(os.path.join(excel_dir, 'Bathost.xlsx'))
    excel_province_counts = df_bathost['Province'].value_counts()
    excel_provinces = excel_province_counts.to_dict()

print(f'📋 PROVINCIAL DISTRIBUTION COMPARISON:')
print('  Database provinces:', list(db_provinces.keys()))
print('  Excel provinces:', list(excel_provinces.keys()))

for province in sorted(set(list(db_provinces.keys()) + list(excel_provinces.keys()))):
    db_count = db_provinces.get(province, 0)
    excel_count = excel_provinces.get(province, 0)
    print(f'  {province}: Database={db_count}, Excel={excel_count}')
    
    if db_count != excel_count:
        print(f'    ❌ MISMATCH: {abs(db_count - excel_count)} hosts difference')
    else:
        print(f'    ✅ MATCH: Counts are equal')

print('\n📊 STEP 10: MISSING DATA ANALYSIS')
print('-' * 40)

print('🔍 MISSING DATA ANALYSIS:')

# Check for missing Excel data that should be in database
missing_data = []

# Check if any Excel data is completely missing
if 'Bathost.xlsx' in excel_counts and excel_hosts > 0:
    # Check if any Excel hosts are missing from database
    excel_host_sourceids = set(df_bathost['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM hosts')
    db_host_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_hosts = excel_host_sourceids - db_host_sourceids
    if missing_hosts:
        missing_data.append(f'MISSING HOSTS: {len(missing_hosts)} hosts from Bathost.xlsx not in database')
        missing_data.append(f'Sample missing hosts: {list(missing_hosts)[:5]}...')

if 'Screening.xlsx' in excel_counts and excel_screening > 0:
    # Check if any Excel screening is missing from database
    excel_screening_sourceids = set(df_screening['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM screening_results')
    db_screening_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_screening = excel_screening_sourceids - db_screening_sourceids
    if missing_screening:
        missing_data.append(f'MISSING SCREENING: {len(missing_screening)} screening records from Screening.xlsx not in database')
        missing_data.append(f'Sample missing screening: {list(missing_screening)[:5]}...')

# Check for extra data in database that shouldn't be there
extra_data = []

if db_hosts > excel_hosts:
    extra_data.append(f'EXTRA HOSTS: {db_hosts - excel_hosts} hosts in database not in Bathost.xlsx')

if db_screening > excel_screening:
    extra_data.append(f'EXTRA SCREENING: {db_screening - excel_screening} screening records in database not in Screening.xlsx')

if missing_data:
    print('❌ MISSING DATA FOUND:')
    for item in missing_data:
        print(f'  {item}')
else:
    print('✅ NO MISSING DATA DETECTED')

if extra_data:
    print('❌ EXTRA DATA FOUND:')
    for item in extra_data:
        print(f'  {item}')
else:
    print('✅ NO EXTRA DATA DETECTED')

print('\n📊 STEP 11: RECOMMENDATIONS')
print('-' * 40)

print('🔍 RECOMMENDATIONS:')

if excel_hosts != db_hosts or excel_sample_counts != db_sample_counts or excel_screening != db_screening:
    print('❌ DATA IMPORT ISSUES DETECTED:')
    
    if excel_hosts != db_hosts:
        print('  • Host count mismatch - reimport Bathost.xlsx')
    
    if excel_sample_counts != db_sample_counts:
        for sample_type, excel_count in excel_sample_counts.items():
            db_count = db_sample_counts.get(sample_type, 0)
            if excel_count != db_count:
                print(f'  • {sample_type} count mismatch - reimport {sample_type.replace("Bat", "Bat")}.xlsx')
    
    if excel_screening != db_screening:
        print('  • Screening count mismatch - reimport Screening.xlsx')
    
    print('  • Consider running the complete reimport script again')
    print('  • Check for data type mismatches in Excel files')
    print('  • Verify SourceId matching logic')
else:
    print('✅ DATA IMPORT APPEARS CORRECT:')
    print('  • All counts match between Excel and database')
    print('  • No missing or extra data detected')
    print('  • SourceId matching appears to be working')

# Check for data quality issues
if null_host_sourceids > 0 or null_sample_sourceids > 0 or null_screening_sourceids > 0:
    print('❌ DATA QUALITY ISSUES:')
    print('  • Null SourceIds detected - clean the data')
    print('  • Check for empty cells in Excel files')
    print('  • Ensure data consistency before import')

if orphaned_samples > 0 or orphaned_screening > 0:
    print('❌ ORPHANED RECORDS:')
    print('  • Samples without hosts detected')
    print('  • Screening without samples detected')
    print('  • Check foreign key relationships')

if len(duplicate_host_sourceids) > 0:
    print('❌ DUPLICATE DATA:')
    print('  • Duplicate SourceIds detected in hosts')
    print('  • Clean Excel data before import')

conn.close()

print('\n🎯 FINAL SUMMARY')
print('=' * 50)
print('📊 COMPARISON SUMMARY:')
print(f'• Excel hosts: {excel_hosts}, Database hosts: {db_hosts}')
print(f'• Excel samples: {sum(excel_sample_counts.values())}, Database samples: {db_samples}')
print(f'• Excel screening: {excel_screening}, Database screening: {db_screening}')
print(f'• Excel storage: {excel_storage}, Database storage: {db_storage}')
print(f'• Excel morphometrics: {excel_morpho}, Database morphometrics: {db_morphometrics}')

if excel_hosts == db_hosts and sum(excel_sample_counts.values()) == db_samples and excel_screening == db_screening:
    print('✅ ALL COUNTS MATCH - DATA IMPORT SUCCESSFUL!')
else:
    print('❌ COUNTS MISMATCH - NEEDS REIMPORT')

print(f'✅ Comparison completed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')

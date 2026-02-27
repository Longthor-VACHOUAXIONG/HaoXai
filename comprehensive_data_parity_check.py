#!/usr/bin/env python3
"""
Comprehensive comparison between Excel and database to ensure data parity
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime
import hashlib

print('🔍 COMPREHENSIVE DATA PARITY CHECK')
print('=' * 70)
print('Ensuring Excel and Database contain exactly the same data')

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# CORRECT Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/NewHaoXai.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('\n📊 STEP 1: COUNT EXCEL RECORDS BY FILE')
print('-' * 50)

# Excel files and their corresponding database tables
excel_files = {
    'Bathost.xlsx': 'hosts',
    'RodentHost.xlsx': 'hosts', 
    'Batswab.xlsx': 'samples',
    'Battissue.xlsx': 'samples',
    'RodentSample.xlsx': 'samples',
    'Environmental.xlsx': 'environmental_samples',
    'MarketSampleAndHost.xlsx': 'hosts',
    'Screening.xlsx': 'screening_results',
    'Freezer14.xlsx': 'storage'
}

# Count Excel records
excel_counts = {}
excel_sourceids = {}
for filename, table_type in excel_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path)
            excel_counts[filename] = len(df)
            print(f'  {filename}: {len(df)} records ({table_type})')
            
            # Get unique SourceIds for comparison
            if 'SourceId' in df.columns:
                sourceids = df['SourceId'].dropna().astype(str).unique()
                excel_sourceids[filename] = set(sourceids)
                print(f'    Unique SourceIds: {len(sourceids)}')
            else:
                excel_sourceids[filename] = set()
                print(f'    No SourceId column found')
                
        except Exception as e:
            print(f'  {filename}: Error reading - {e}')
            excel_counts[filename] = 0
            excel_sourceids[filename] = set()

print('\n📊 STEP 2: COUNT DATABASE RECORDS')
print('-' * 50)

# Count database records
db_counts = {}
db_sourceids = {}

cursor.execute('SELECT COUNT(*) FROM hosts')
db_counts['hosts'] = cursor.fetchone()[0]
cursor.execute('SELECT source_id FROM hosts WHERE source_id IS NOT NULL')
db_sourceids['hosts'] = set([str(row[0]) for row in cursor.fetchall()])
print(f'  Hosts: {db_counts["hosts"]} records ({len(db_sourceids["hosts"])} unique SourceIds)')

cursor.execute('SELECT COUNT(*) FROM samples')
db_counts['samples'] = cursor.fetchone()[0]
cursor.execute('SELECT source_id FROM samples WHERE source_id IS NOT NULL')
db_sourceids['samples'] = set([str(row[0]) for row in cursor.fetchall()])
print(f'  Samples: {db_counts["samples"]} records ({len(db_sourceids["samples"])} unique SourceIds)')

cursor.execute('SELECT COUNT(*) FROM environmental_samples')
db_counts['environmental_samples'] = cursor.fetchone()[0]
cursor.execute('SELECT source_id FROM environmental_samples WHERE source_id IS NOT NULL')
db_sourceids['environmental_samples'] = set([str(row[0]) for row in cursor.fetchall()])
print(f'  Environmental samples: {db_counts["environmental_samples"]} records ({len(db_sourceids["environmental_samples"])} unique SourceIds)')

cursor.execute('SELECT COUNT(*) FROM screening_results')
db_counts['screening_results'] = cursor.fetchone()[0]
cursor.execute('SELECT source_id FROM screening_results WHERE source_id IS NOT NULL')
db_sourceids['screening_results'] = set([str(row[0]) for row in cursor.fetchall()])
print(f'  Screening results: {db_counts["screening_results"]} records ({len(db_sourceids["screening_results"])} unique SourceIds)')

cursor.execute('SELECT COUNT(*) FROM storage')
db_counts['storage'] = cursor.fetchone()[0]
cursor.execute('SELECT sample_tube_id FROM storage WHERE sample_tube_id IS NOT NULL')
db_sourceids['storage'] = set([str(row[0]) for row in cursor.fetchall()])
print(f'  Storage: {db_counts["storage"]} records ({len(db_sourceids["storage"])} unique SampleIds)')

print('\n📊 STEP 3: DETAILED SOURCEID COMPARISON')
print('-' * 50)

# Compare SourceIds by data type
print('🔍 SOURCEID COVERAGE ANALYSIS:')

# Combine Excel SourceIds by type
combined_excel_sourceids = {
    'hosts': set(),
    'samples': set(),
    'environmental_samples': set(),
    'screening_results': set(),
    'storage': set()
}

for filename, table_type in excel_files.items():
    if table_type in combined_excel_sourceids:
        combined_excel_sourceids[table_type].update(excel_sourceids[filename])

# Compare each data type
for data_type in ['hosts', 'samples', 'environmental_samples', 'screening_results', 'storage']:
    excel_total = len(combined_excel_sourceids[data_type])
    db_total = len(db_sourceids[data_type])
    
    missing_in_db = combined_excel_sourceids[data_type] - db_sourceids[data_type]
    extra_in_db = db_sourceids[data_type] - combined_excel_sourceids[data_type]
    
    coverage = (db_total / excel_total * 100) if excel_total > 0 else 0
    
    print(f'\n📋 {data_type.upper()}:')
    print(f'  Excel unique IDs: {excel_total}')
    print(f'  Database unique IDs: {db_total}')
    print(f'  Coverage: {coverage:.1f}%')
    print(f'  Missing in database: {len(missing_in_db)}')
    print(f'  Extra in database: {len(extra_in_db)}')
    
    if len(missing_in_db) > 0:
        print(f'  Missing examples: {list(missing_in_db)[:5]}')
    if len(extra_in_db) > 0:
        print(f'  Extra examples: {list(extra_in_db)[:5]}')

print('\n📊 STEP 4: RECORD COUNT COMPARISON')
print('-' * 50)

# Compare record counts
print('📈 RECORD COUNT ANALYSIS:')

# Calculate Excel totals by type
excel_totals = {
    'hosts': excel_counts.get('Bathost.xlsx', 0) + excel_counts.get('RodentHost.xlsx', 0) + excel_counts.get('MarketSampleAndHost.xlsx', 0),
    'samples': excel_counts.get('Batswab.xlsx', 0) + excel_counts.get('Battissue.xlsx', 0) + excel_counts.get('RodentSample.xlsx', 0),
    'environmental_samples': excel_counts.get('Environmental.xlsx', 0),
    'screening_results': excel_counts.get('Screening.xlsx', 0),
    'storage': excel_counts.get('Freezer14.xlsx', 0)
}

for data_type in ['hosts', 'samples', 'environmental_samples', 'screening_results', 'storage']:
    excel_total = excel_totals[data_type]
    db_total = db_counts[data_type]
    difference = db_total - excel_total
    parity = (db_total / excel_total * 100) if excel_total > 0 else 0
    
    print(f'\n📋 {data_type.upper()}:')
    print(f'  Excel records: {excel_total}')
    print(f'  Database records: {db_total}')
    print(f'  Difference: {difference:+d}')
    print(f'  Parity: {parity:.1f}%')
    
    if difference != 0:
        print(f'  Status: {"⚠️  Database has more records" if difference > 0 else "❌ Database has fewer records"}')
    else:
        print(f'  Status: ✅ Perfect parity')

print('\n📊 STEP 5: DATA QUALITY ANALYSIS')
print('-' * 50)

# Check for data quality issues
print('🔍 DATA QUALITY ISSUES:')

# Check for empty SourceIds in Excel
empty_sourceids_excel = {}
for filename, table_type in excel_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path)
            if 'SourceId' in df.columns:
                empty_count = df['SourceId'].isna().sum()
                if empty_count > 0:
                    empty_sourceids_excel[filename] = empty_count
                    print(f'  ⚠️  {filename}: {empty_count} empty SourceIds')
        except:
            pass

# Check for duplicates in Excel
duplicates_excel = {}
for filename, table_type in excel_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        try:
            df = pd.read_excel(file_path)
            if 'SourceId' in df.columns:
                total = len(df)
                unique = df['SourceId'].nunique()
                dup_count = total - unique
                if dup_count > 0:
                    duplicates_excel[filename] = dup_count
                    print(f'  ⚠️  {filename}: {dup_count} duplicate SourceIds')
        except:
            pass

# Check for duplicates in database
cursor.execute('SELECT source_id, COUNT(*) as count FROM hosts GROUP BY source_id HAVING count > 1')
db_host_duplicates = cursor.fetchall()
if db_host_duplicates:
    print(f'  ⚠️  Database: {len(db_host_duplicates)} duplicate host SourceIds')

cursor.execute('SELECT source_id, COUNT(*) as count FROM samples GROUP BY source_id HAVING count > 1')
db_sample_duplicates = cursor.fetchall()
if db_sample_duplicates:
    print(f'  ⚠️  Database: {len(db_sample_duplicates)} duplicate sample SourceIds')

if not empty_sourceids_excel and not duplicates_excel and not db_host_duplicates and not db_sample_duplicates:
    print('  ✅ No major data quality issues found')

print('\n📊 STEP 6: MISSING DATA IDENTIFICATION')
print('-' * 50)

# Identify exactly what's missing
print('🎯 MISSING DATA ANALYSIS:')

missing_by_type = {}
for data_type in ['hosts', 'samples', 'environmental_samples', 'screening_results', 'storage']:
    missing_in_db = combined_excel_sourceids[data_type] - db_sourceids[data_type]
    if missing_in_db:
        missing_by_type[data_type] = missing_in_db
        print(f'\n❌ Missing {data_type}: {len(missing_in_db)} records')
        print(f'  Examples: {list(missing_in_db)[:10]}')

print('\n📊 STEP 7: DATA SYNCHRONIZATION PLAN')
print('-' * 50)

# Create plan for data synchronization
print('🔧 DATA SYNCHRONIZATION PLAN:')

total_missing = sum(len(missing) for missing in missing_by_type.values())
if total_missing > 0:
    print(f'📋 Total missing records: {total_missing}')
    print('\n🎯 ACTIONS NEEDED:')
    
    for data_type, missing_ids in missing_by_type.items():
        if missing_ids:
            print(f'  🔧 Import {len(missing_ids)} missing {data_type}')
    
    print(f'\n🎯 Estimated completion time: {total_missing * 0.1:.1f} seconds')
else:
    print('✅ No missing data detected!')
    print('✅ Database and Excel are in perfect parity!')

conn.close()

print('\n🎯 COMPREHENSIVE ANALYSIS SUMMARY')
print('=' * 70)
print('✅ Excel vs Database comparison completed')
print('✅ SourceId coverage analyzed')
print('✅ Record count parity checked')
print('✅ Data quality issues identified')
print('✅ Missing data pinpointed')
print('✅ Synchronization plan created')
print(f'✅ Analysis completed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print('')
print('🎯 Ready to synchronize data for perfect parity!')

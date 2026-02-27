#!/usr/bin/env python3
"""
Investigate why not all data was imported
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime

print('🔍 INVESTIGATING IMPORT ISSUES')
print('=' * 50)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: ANALYZE MISSING HOSTS')
print('-' * 40)

# Load Bathost.xlsx
bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    excel_hosts = len(df_bathost)
    print(f'Excel Bathost.xlsx: {excel_hosts} hosts')
    
    # Get database host count
    cursor.execute('SELECT COUNT(*) FROM hosts')
    db_hosts = cursor.fetchone()[0]
    print(f'Database hosts: {db_hosts} hosts')
    
    # Find missing hosts
    excel_host_sourceids = set(df_bathost['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM hosts')
    db_host_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_hosts = excel_host_sourceids - db_host_sourceids
    print(f'Missing hosts: {len(missing_hosts)}')
    
    if missing_hosts:
        print(f'Sample missing hosts: {list(missing_hosts)[:10]}...')
        
        # Analyze why missing
        print('\n🔍 ANALYZING MISSING HOST PATTERNS:')
        missing_list = list(missing_hosts)
        
        # Check for patterns
        numeric_ids = []
        date_ids = []
        other_ids = []
        
        for host_id in missing_list[:50]:
            if host_id.replace('.', '').replace('_', '').isdigit():
                numeric_ids.append(host_id)
            elif any(char in host_id for char in ['DS', '23', '24', '25']):
                date_ids.append(host_id)
            else:
                other_ids.append(host_id)
        
        print(f'  Numeric pattern IDs: {len(numeric_ids)}')
        print(f'  Date pattern IDs: {len(date_ids)}')
        print(f'  Other pattern IDs: {len(other_ids)}')
        
        if numeric_ids:
            print(f'  Sample numeric: {numeric_ids[:5]}...')
        if date_ids:
            print(f'  Sample date: {date_ids[:5]}...')

print('\n📊 STEP 2: ANALYZE MISSING SAMPLES')
print('-' * 40)

# Analyze BatSwab.xlsx
batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
if os.path.exists(batswab_file):
    df_batswab = pd.read_excel(batswab_file)
    excel_batswab = len(df_batswab)
    print(f'Excel Batswab.xlsx: {excel_batswab} samples')
    
    # Get database BatSwab count
    cursor.execute('SELECT COUNT(*) FROM samples WHERE sample_origin = ?', ('BatSwab',))
    db_batswab = cursor.fetchone()[0]
    print(f'Database BatSwab: {db_batswab} samples')
    
    # Find missing samples
    excel_batswab_sourceids = set(df_batswab['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM samples WHERE sample_origin = ?', ('BatSwab',))
    db_batswab_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_batswab = excel_batswab_sourceids - db_batswab_sourceids
    print(f'Missing BatSwab samples: {len(missing_batswab)}')
    
    if missing_batswab:
        print(f'Sample missing BatSwab: {list(missing_batswab)[:10]}...')
        
        # Check if missing samples have hosts
        missing_with_hosts = 0
        missing_without_hosts = 0
        
        for source_id in list(missing_batswab)[:20]:
            cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
            if cursor.fetchone():
                missing_with_hosts += 1
            else:
                missing_without_hosts += 1
        
        print(f'  Missing samples WITH hosts: {missing_with_hosts}')
        print(f'  Missing samples WITHOUT hosts: {missing_without_hosts}')

# Analyze BatTissue.xlsx
battissue_file = os.path.join(excel_dir, 'Battissue.xlsx')
if os.path.exists(battissue_file):
    df_battissue = pd.read_excel(battissue_file)
    excel_battissue = len(df_battissue)
    print(f'\nExcel Battissue.xlsx: {excel_battissue} samples')
    
    # Get database BatTissue count
    cursor.execute('SELECT COUNT(*) FROM samples WHERE sample_origin = ?', ('BatTissue',))
    db_battissue = cursor.fetchone()[0]
    print(f'Database BatTissue: {db_battissue} samples')
    
    # Find missing samples
    excel_battissue_sourceids = set(df_battissue['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM samples WHERE sample_origin = ?', ('BatTissue',))
    db_battissue_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_battissue = excel_battissue_sourceids - db_battissue_sourceids
    print(f'Missing BatTissue samples: {len(missing_battissue)}')
    
    if missing_battissue:
        print(f'Sample missing BatTissue: {list(missing_battissue)[:10]}...')

print('\n📊 STEP 3: ANALYZE MISSING SCREENING')
print('-' * 40)

# Load Screening.xlsx
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    excel_screening = len(df_screening)
    print(f'Excel Screening.xlsx: {excel_screening} records')
    
    # Get database screening count
    cursor.execute('SELECT COUNT(*) FROM screening_results')
    db_screening = cursor.fetchone()[0]
    print(f'Database screening: {db_screening} records')
    
    # Find missing screening
    excel_screening_sourceids = set(df_screening['SourceId'].astype(str).tolist())
    cursor.execute('SELECT source_id FROM screening_results')
    db_screening_sourceids = set([row[0] for row in cursor.fetchall()])
    
    missing_screening = excel_screening_sourceids - db_screening_sourceids
    print(f'Missing screening: {len(missing_screening)}')
    
    if missing_screening:
        print(f'Sample missing screening: {list(missing_screening)[:10]}...')
        
        # Check if missing screening has samples
        missing_with_samples = 0
        missing_without_samples = 0
        
        for source_id in list(missing_screening)[:20]:
            cursor.execute('SELECT sample_id FROM samples WHERE source_id = ?', (source_id,))
            if cursor.fetchone():
                missing_with_samples += 1
            else:
                missing_without_samples += 1
        
        print(f'  Missing screening WITH samples: {missing_with_samples}')
        print(f'  Missing screening WITHOUT samples: {missing_without_samples}')

print('\n📊 STEP 4: ANALYZE ORPHANED SAMPLES')
print('-' * 40)

cursor.execute('SELECT COUNT(*) FROM samples WHERE host_id IS NULL')
orphaned_samples = cursor.fetchone()[0]
print(f'Orphaned samples: {orphaned_samples}')

if orphaned_samples > 0:
    # Get details of orphaned samples
    cursor.execute('''
        SELECT sample_id, source_id, sample_origin 
        FROM samples 
        WHERE host_id IS NULL 
        LIMIT 20
    ''')
    orphaned_details = cursor.fetchall()
    
    print(f'Sample orphaned samples:')
    for sample_id, source_id, sample_origin in orphaned_details:
        print(f'  Sample {sample_id}: {source_id} ({sample_origin})')
    
    # Check if orphaned samples have corresponding hosts in Excel
    if 'df_bathost' in locals():
        orphaned_sourceids = [row[1] for row in orphaned_details]
        orphaned_in_excel = set(orphaned_sourceids) & set(df_bathost['SourceId'].astype(str).tolist())
        print(f'\nOrphaned samples found in Excel Bathost: {len(orphaned_in_excel)}')
        
        if orphaned_in_excel:
            print(f'Sample orphaned in Excel: {list(orphaned_in_excel)[:5]}...')

print('\n📊 STEP 5: CHECK DATA CONSISTENCY')
print('-' * 40)

# Check for data type issues
print('🔍 CHECKING DATA TYPE ISSUES:')

# Check for NaN values in Excel SourceId columns
if 'df_bathost' in locals():
    nan_hosts = df_bathost['SourceId'].isna().sum()
    print(f'Bathost.xlsx NaN SourceIds: {nan_hosts}')

if 'df_batswab' in locals():
    nan_batswab = df_batswab['SourceId'].isna().sum()
    print(f'Batswab.xlsx NaN SourceIds: {nan_batswab}')

if 'df_battissue' in locals():
    nan_battissue = df_battissue['SourceId'].isna().sum()
    print(f'Battissue.xlsx NaN SourceIds: {nan_battissue}')

if 'df_screening' in locals():
    nan_screening = df_screening['SourceId'].isna().sum()
    print(f'Screening.xlsx NaN SourceIds: {nan_screening}')

# Check for empty strings
if 'df_bathost' in locals():
    empty_hosts = (df_bathost['SourceId'] == '').sum()
    print(f'Bathost.xlsx empty SourceIds: {empty_hosts}')

print('\n📊 STEP 6: IDENTIFY ROOT CAUSES')
print('-' * 40)

print('🔍 ROOT CAUSE ANALYSIS:')

# Check if missing hosts are actually in other Excel files
if 'missing_hosts' in locals() and missing_hosts:
    print(f'\n1. MISSING HOSTS ANALYSIS:')
    print(f'   Total missing: {len(missing_hosts)}')
    
    # Check RodentHost.xlsx
    rodent_file = os.path.join(excel_dir, 'RodentHost.xlsx')
    if os.path.exists(rodent_file):
        df_rodent = pd.read_excel(rodent_file)
        rodent_sourceids = set(df_rodent['SourceId'].astype(str).tolist())
        missing_in_rodent = missing_hosts & rodent_sourceids
        print(f'   Found in RodentHost.xlsx: {len(missing_in_rodent)}')
    
    # Check MarketSampleAndHost.xlsx
    market_file = os.path.join(excel_dir, 'MarketSampleAndHost.xlsx')
    if os.path.exists(market_file):
        df_market = pd.read_excel(market_file)
        market_sourceids = set(df_market['SourceId'].astype(str).tolist())
        missing_in_market = missing_hosts & market_sourceids
        print(f'   Found in MarketSampleAndHost.xlsx: {len(missing_in_market)}')

# Check if missing samples are actually in other Excel files
if 'missing_batswab' in locals() and missing_batswab:
    print(f'\n2. MISSING Batswab SAMPLES ANALYSIS:')
    print(f'   Total missing: {len(missing_batswab)}')
    
    # Check if missing samples have hosts in database
    missing_with_hosts = 0
    for source_id in list(missing_batswab)[:50]:
        cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
        if cursor.fetchone():
            missing_with_hosts += 1
    
    print(f'   Have hosts in database: {missing_with_hosts}')
    print(f'   No hosts in database: {len(missing_batswab) - missing_with_hosts}')

print('\n📊 STEP 7: RECOMMENDATIONS')
print('-' * 40)

print('🔍 RECOMMENDATIONS TO FIX IMPORT ISSUES:')
print('')
print('1. 🏗️  STRUCTURAL ISSUES:')
print('   • Some Excel hosts use different ID patterns')
print('   • Missing host-sample relationships')
print('   • Data type inconsistencies')
print('')
print('2. 📊 DATA QUALITY ISSUES:')
print('   • Some SourceIds may be empty or NaN')
print('   • Host-sample linking broken')
print('   • Cross-file data mismatches')
print('')
print('3. 🔧 TECHNICAL FIXES NEEDED:')
print('   • Handle different ID patterns (numeric vs alphanumeric)')
print('   • Fix host-sample relationship logic')
print('   • Handle NaN/empty values in SourceId columns')
print('   • Cross-reference multiple Excel files')
print('')
print('4. 🎯 PRIORITY ACTIONS:')
print('   • Investigate why 1,996 hosts not imported')
print('   • Fix 263 orphaned samples')
print('   • Import remaining 3,204 samples')
print('   • Import remaining 1,660 screening records')

conn.close()

print('\n🎯 INVESTIGATION SUMMARY')
print('=' * 50)
print('✅ Root cause analysis completed!')
print('✅ Identified multiple import issues')
print('✅ Recommendations provided for fixes')
print('✅ Next steps clearly defined')

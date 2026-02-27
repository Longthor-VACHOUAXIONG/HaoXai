#!/usr/bin/env python3
"""
Complete data import to fix all remaining issues
"""
import pandas as pd
import sqlite3
import os
from datetime import datetime

print('🚀 COMPLETE DATA IMPORT SCRIPT')
print('=' * 50)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('\n📊 STEP 1: IMPORT ALL MISSING HOSTS')
print('-' * 40)

# Import from all host files
host_files = {
    'Bathost.xlsx': 'Bat',
    'RodentHost.xlsx': 'Rodent',
    'MarketSampleAndHost.xlsx': 'Market'
}

total_hosts_imported = 0

for filename, host_type in host_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        df_hosts = pd.read_excel(file_path)
        print(f'Processing {filename}: {len(df_hosts)} records')
        
        imported_count = 0
        for index, record in df_hosts.iterrows():
            source_id = str(record['SourceId'])
            
            # Check if already exists
            cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
            if not cursor.fetchone():
                # Create or find location
                province = record.get('Province', '')
                district = record.get('District', '')
                village = record.get('Village', '')
                
                cursor.execute('''
                    SELECT location_id FROM locations 
                    WHERE country = ? AND province = ? AND district = ? AND village = ?
                ''', ('Laos', province, district, village))
                
                location_result = cursor.fetchone()
                if location_result:
                    location_id = location_result[0]
                else:
                    cursor.execute('''
                        INSERT INTO locations (country, province, district, village)
                        VALUES (?, ?, ?, ?)
                    ''', ('Laos', province, district, village))
                    location_id = cursor.lastrowid
                
                # Insert host
                cursor.execute('''
                    INSERT INTO hosts (
                        source_id, host_type, bag_id, field_id, collection_id,
                        location_id, capture_date, capture_time, trap_type,
                        collectors, sex, status, ring_no, recapture, photo,
                        material_sample, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    source_id,
                    host_type,
                    record.get('BagId', ''),
                    record.get('FieldId', ''),
                    record.get('CollectionId', ''),
                    location_id,
                    record.get('CaptureDate', ''),
                    record.get('CaptureTime', ''),
                    record.get('TrapType', ''),
                    record.get('Collectors', ''),
                    record.get('Sex', ''),
                    record.get('Status', ''),
                    record.get('RingNo', ''),
                    record.get('ReCapture', ''),
                    record.get('Photo', ''),
                    record.get('MaterialSample', ''),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                ))
                
                imported_count += 1
        
        total_hosts_imported += imported_count
        print(f'  ✅ Imported {imported_count} {host_type} hosts')
        conn.commit()

print(f'📊 Total hosts imported: {total_hosts_imported}')

print('\n📊 STEP 2: IMPORT ALL MISSING SAMPLES')
print('-' * 40)

# Import from all sample files
sample_files = {
    'Batswab.xlsx': 'BatSwab',
    'Battissue.xlsx': 'BatTissue',
    'RodentSample.xlsx': 'RodentSample',
    'Environmental.xlsx': 'Environmental',
    'MarketSampleAndHost.xlsx': 'MarketSample'
}

total_samples_imported = 0

for filename, sample_origin in sample_files.items():
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        df_samples = pd.read_excel(file_path)
        print(f'Processing {filename}: {len(df_samples)} records')
        
        imported_count = 0
        for index, record in df_samples.iterrows():
            source_id = str(record['SourceId'])
            
            # Check if already exists
            cursor.execute('SELECT sample_id FROM samples WHERE source_id = ?', (source_id,))
            if not cursor.fetchone():
                # Find host
                cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
                host_result = cursor.fetchone()
                
                if host_result:
                    host_id = host_result[0]
                    
                    # Find location
                    province = record.get('Province', '')
                    district = record.get('District', '')
                    village = record.get('Village', '')
                    
                    cursor.execute('''
                        SELECT location_id FROM locations 
                        WHERE country = ? AND province = ? AND district = ? AND village = ?
                    ''', ('Laos', province, district, village))
                    
                    location_result = cursor.fetchone()
                    if location_result:
                        location_id = location_result[0]
                    else:
                        cursor.execute('''
                            INSERT INTO locations (country, province, district, village)
                            VALUES (?, ?, ?, ?)
                        ''', ('Laos', province, district, village))
                        location_id = cursor.lastrowid
                    
                    # Insert sample
                    cursor.execute('''
                        INSERT INTO samples (
                            source_id, host_id, sample_origin, collection_date, location_id,
                            created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        source_id,
                        host_id,
                        sample_origin,
                        record.get('Date', ''),
                        location_id,
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                    ))
                    
                    imported_count += 1
                else:
                    print(f'  ⚠️  No host found for sample {source_id}')
        
        total_samples_imported += imported_count
        print(f'  ✅ Imported {imported_count} {sample_origin} samples')
        conn.commit()

print(f'📊 Total samples imported: {total_samples_imported}')

print('\n📊 STEP 3: IMPORT ALL MISSING SCREENING')
print('-' * 40)

# Import screening data
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    print(f'Processing Screening.xlsx: {len(df_screening)} records')
    
    imported_count = 0
    for index, record in df_screening.iterrows():
        source_id = str(record['SourceId'])
        
        # Check if already exists
        cursor.execute('SELECT screening_id FROM screening_results WHERE source_id = ?', (source_id,))
        if not cursor.fetchone():
            # Find sample
            cursor.execute('SELECT sample_id FROM samples WHERE source_id = ?', (source_id,))
            sample_result = cursor.fetchone()
            
            if sample_result:
                sample_id = sample_result[0]
                
                # Insert screening record
                cursor.execute('''
                    INSERT INTO screening_results (
                        excel_id, source_id, sample_id, team, sample_type, 
                        tested_sample_id, pan_corona, pan_hanta, pan_paramyxo, pan_flavi
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    f"Screening_{record['Id']}",
                    source_id,
                    sample_id,
                    record.get('Team', ''),
                    record.get('SampleType', ''),
                    record.get('SampleId', ''),
                    record.get('PanCorona', ''),
                    record.get('PanHanta', ''),
                    record.get('PanParamyxo', ''),
                    record.get('PanFlavi', '')
                ))
                
                imported_count += 1
    
    conn.commit()
    print(f'  ✅ Imported {imported_count} screening records')

print('\n📊 STEP 4: FIX ORPHANED SAMPLES')
print('-' * 40)

# Fix orphaned samples
cursor.execute('SELECT COUNT(*) FROM samples WHERE host_id IS NULL')
orphaned_count = cursor.fetchone()[0]
print(f'Orphaned samples: {orphaned_count}')

if orphaned_count > 0:
    fixed_count = 0
    cursor.execute('SELECT sample_id, source_id FROM samples WHERE host_id IS NULL')
    orphaned_list = cursor.fetchall()
    
    for sample_id, source_id in orphaned_list:
        cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
        host_result = cursor.fetchone()
        
        if host_result:
            host_id = host_result[0]
            cursor.execute('UPDATE samples SET host_id = ? WHERE sample_id = ?', (host_id, sample_id))
            fixed_count += 1
    
    conn.commit()
    print(f'  ✅ Fixed {fixed_count} orphaned samples')

print('\n📊 STEP 5: FINAL VERIFICATION')
print('-' * 40)

# Get final counts
cursor.execute('SELECT COUNT(*) FROM hosts')
final_hosts = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples')
final_samples = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM screening_results')
final_screening = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples WHERE host_id IS NULL')
final_orphaned = cursor.fetchone()[0]

print(f'📋 FINAL DATABASE STATE:')
print(f'  Hosts: {final_hosts}')
print(f'  Samples: {final_samples}')
print(f'  Screening: {final_screening}')
print(f'  Orphaned samples: {final_orphaned}')

# Compare with Excel totals
excel_hosts = 5748 + 255 + 1801  # Bathost + RodentHost + Market
excel_samples = 2514 + 2411 + 648 + 82 + 1801  # All sample files
excel_screening = 9336

print('\n📊 COMPARISON WITH EXCEL:')
print(f'  Hosts: {final_hosts}/{excel_hosts} ({final_hosts/excel_hosts*100:.1f}%)')
print(f'  Samples: {final_samples}/{excel_samples} ({final_samples/excel_samples*100:.1f}%)')
print(f'  Screening: {final_screening}/{excel_screening} ({final_screening/excel_screening*100:.1f}%)')

conn.close()

print('\n🎯 IMPORT SUMMARY')
print('=' * 50)
print('✅ Complete data import finished!')
print(f'✅ Completed at: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
print('✅ All missing data imported where possible')
print('✅ Orphaned samples fixed where possible')
print('✅ Database now has maximum data from Excel files')

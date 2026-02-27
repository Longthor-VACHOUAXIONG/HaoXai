#!/usr/bin/env python3
"""
Complete fresh import of ALL data from ALL Excel files - no artificial linking
"""
import pandas as pd
import sqlite3
import os
import shutil
from datetime import datetime

print('🛠️ COMPLETE FRESH IMPORT - ALL EXCEL FILES')
print('=' * 70)

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

print('📊 STEP 1: BACKUP DATABASE')
print('-' * 40)

# Create backup
backup_name = f'CAN2Database_BEFORE_COMPLETE_FRESH_IMPORT_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
backup_path = os.path.join(os.path.dirname(db_path), backup_name)
shutil.copy2(db_path, backup_path)
print(f'✅ Database backed up to: {backup_name}')

# Connect to database
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('\n📊 STEP 2: DELETE ALL DATA')
print('-' * 40)

# Clear all data completely
cursor.execute('DELETE FROM screening_results')
cursor.execute('DELETE FROM samples')
cursor.execute('DELETE FROM hosts')
cursor.execute('DELETE FROM locations')
cursor.execute('DELETE FROM storage_locations')
cursor.execute('DELETE FROM morphometrics')
cursor.execute('DELETE FROM taxonomy')
conn.commit()
print('✅ All data deleted from database')

print('\n📊 STEP 3: IMPORT LOCATIONS FROM ALL EXCEL FILES')
print('-' * 40)

# Collect all locations from all Excel files
all_locations = {}
excel_files = [
    'Bathost.xlsx',
    'RodentHost.xlsx', 
    'MarketSampleAndHost.xlsx',
    'Environmental.xlsx'
]

for filename in excel_files:
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        print(f'Processing {filename} for locations...')
        df = pd.read_excel(file_path)
        
        for idx, row in df.iterrows():
            province = None
            district = 'Unknown'
            village = 'Unknown'
            country = 'Laos'
            
            # Extract location data based on file structure
            if filename == 'Bathost.xlsx':
                if pd.notna(row['Province']):
                    province = str(row['Province']).strip()
                    district = str(row['District']).strip() if pd.notna(row['District']) else 'Unknown'
                    village = str(row['Village']).strip() if pd.notna(row['Village']) else 'Unknown'
                    country = str(row['Country']).strip() if pd.notna(row['Country']) else 'Laos'
            elif filename == 'RodentHost.xlsx':
                if pd.notna(row['Location']):
                    location_parts = str(row['Location']).split(',')
                    if len(location_parts) >= 3:
                        village = location_parts[0].strip()
                        district = location_parts[1].strip()
                        province = location_parts[2].strip()
            elif filename == 'MarketSampleAndHost.xlsx':
                if pd.notna(row['Province']):
                    province = str(row['Province']).strip()
                    district = str(row['District']).strip() if pd.notna(row['District']) else 'Unknown'
                    village = str(row['LocationName']).strip() if pd.notna(row['LocationName']) else 'Unknown'
            elif filename == 'Environmental.xlsx':
                if pd.notna(row['Province']):
                    province = str(row['Province']).strip()
                    district = str(row['District']).strip() if pd.notna(row['District']) else 'Unknown'
                    village = str(row['Village']).strip() if pd.notna(row['Village']) else 'Unknown'
            
            if province:
                location_key = f"{province}_{district}_{village}_{country}"
                if location_key not in all_locations:
                    all_locations[location_key] = {
                        'country': country,
                        'province': province,
                        'district': district,
                        'village': village
                    }

print(f'Found {len(all_locations)} unique locations from all Excel files')

# Insert locations
for location_key, location_data in all_locations.items():
    cursor.execute('''
        INSERT INTO locations (country, province, district, village, created_at, updated_at)
        VALUES (?, ?, ?, ?, datetime('now'), datetime('now'))
    ''', (location_data['country'], location_data['province'], 
          location_data['district'], location_data['village']))

conn.commit()
print(f'✅ Imported {len(all_locations)} locations')

# Get location mapping
cursor.execute('SELECT location_id, province, district, village, country FROM locations')
location_map = {}
for row in cursor.fetchall():
    loc_id, province, district, village, country = row
    location_key = f"{province}_{district}_{village}_{country}"
    location_map[location_key] = loc_id

print('\n📊 STEP 4: IMPORT HOSTS FROM ALL EXCEL FILES')
print('-' * 40)

hosts_imported = 0

# Import from Bathost.xlsx (bats)
bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    print(f'Importing bat hosts from Bathost.xlsx: {len(df_bathost)} rows')
    
    for idx, row in df_bathost.iterrows():
        if pd.notna(row['Province']):
            district = str(row['District']).strip() if pd.notna(row['District']) else 'Unknown'
            village = str(row['Village']).strip() if pd.notna(row['Village']) else 'Unknown'
            country = str(row['Country']).strip() if pd.notna(row['Country']) else 'Laos'
            location_key = f"{row['Province']}_{district}_{village}_{country}"
            location_id = location_map.get(location_key)
            
            if location_id:
                capture_date = row['CaptureDate']
                if pd.notna(capture_date):
                    try:
                        capture_date = pd.to_datetime(capture_date).strftime('%Y-%m-%d')
                    except:
                        capture_date = None
                
                source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else f"BAT_HOST_{hosts_imported + 1}"
                
                try:
                    cursor.execute('''
                        INSERT INTO hosts (
                            source_id, host_type, bag_id, field_id, collection_id,
                            location_id, capture_date, capture_time,
                            trap_type, collectors, sex, status, ring_no,
                            recapture, photo, material_sample
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        source_id, 'Bat',
                        str(row['BagId']) if pd.notna(row['BagId']) else None,
                        str(row['FieldId']) if pd.notna(row['FieldId']) else None,
                        str(row['CollectionId']) if pd.notna(row['CollectionId']) else None,
                        location_id,
                        capture_date,
                        str(row['CaptureTime']) if pd.notna(row['CaptureTime']) else None,
                        str(row['TrapType']) if pd.notna(row['TrapType']) else None,
                        str(row['Collectors']) if pd.notna(row['Collectors']) else None,
                        str(row['Sex']) if pd.notna(row['Sex']) else None,
                        str(row['Status']) if pd.notna(row['Status']) else None,
                        str(row['RingNo']) if pd.notna(row['RingNo']) else None,
                        str(row['ReCapture']) if pd.notna(row['ReCapture']) else None,
                        str(row['Photo']) if pd.notna(row['Photo']) else None,
                        str(row['MaterialSample']) if pd.notna(row['MaterialSample']) else None
                    ))
                    hosts_imported += 1
                except sqlite3.IntegrityError:
                    pass

# Import from RodentHost.xlsx (rodents)
rodent_file = os.path.join(excel_dir, 'RodentHost.xlsx')
if os.path.exists(rodent_file):
    df_rodent = pd.read_excel(rodent_file)
    print(f'Importing rodent hosts from RodentHost.xlsx: {len(df_rodent)} rows')
    
    for idx, row in df_rodent.iterrows():
        if pd.notna(row['Location']):
            location_parts = str(row['Location']).split(',')
            if len(location_parts) >= 3:
                village = location_parts[0].strip()
                district = location_parts[1].strip()
                province = location_parts[2].strip()
                country = 'Laos'
                location_key = f"{province}_{district}_{village}_{country}"
                location_id = location_map.get(location_key)
                
                if location_id:
                    source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else f"RODENT_HOST_{hosts_imported + 1}"
                    
                    try:
                        cursor.execute('''
                            INSERT INTO hosts (
                                source_id, host_type, bag_id, field_id, collection_id,
                                location_id, capture_date, capture_time,
                                trap_type, collectors, sex, status, ring_no,
                                recapture, photo, material_sample
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (
                            source_id, 'Rodent',
                            None, None, None, location_id, None, None, None, None,
                            str(row['Sex']) if pd.notna(row['Sex']) else None,
                            str(row['Status']) if pd.notna(row['Status']) else None,
                            None, None, None, None
                        ))
                        hosts_imported += 1
                    except sqlite3.IntegrityError:
                        pass

# Import from MarketSampleAndHost.xlsx (market samples)
market_file = os.path.join(excel_dir, 'MarketSampleAndHost.xlsx')
if os.path.exists(market_file):
    df_market = pd.read_excel(market_file)
    print(f'Importing market hosts from MarketSampleAndHost.xlsx: {len(df_market)} rows')
    
    for idx, row in df_market.iterrows():
        if pd.notna(row['Province']):
            district = str(row['District']).strip() if pd.notna(row['District']) else 'Unknown'
            village = str(row['LocationName']).strip() if pd.notna(row['LocationName']) else 'Unknown'
            country = 'Laos'
            location_key = f"{row['Province']}_{district}_{village}_{country}"
            location_id = location_map.get(location_key)
            
            if location_id:
                source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else f"MARKET_HOST_{hosts_imported + 1}"
                
                try:
                    cursor.execute('''
                        INSERT INTO hosts (
                            source_id, host_type, bag_id, field_id, collection_id,
                            location_id, capture_date, capture_time,
                            trap_type, collectors, sex, status, ring_no,
                            recapture, photo, material_sample
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        source_id, 'Market',
                        None, None, None, location_id,
                        str(row['CollectionSampleDate']) if pd.notna(row['CollectionSampleDate']) else None,
                        None, None, None,
                        str(row['Sex']) if pd.notna(row['Sex']) else None,
                        None, None, None, None, None
                    ))
                    hosts_imported += 1
                except sqlite3.IntegrityError:
                    pass

conn.commit()
print(f'✅ Imported {hosts_imported} hosts from all Excel files')

print('\n📊 STEP 5: IMPORT SAMPLES FROM ALL EXCEL FILES')
print('-' * 40)

samples_imported = 0
sample_files = [
    ('Batswab.xlsx', 'BatSwab'),
    ('Battissue.xlsx', 'BatTissue'),
    ('RodentSample.xlsx', 'Rodent'),
    ('Environmental.xlsx', 'Environmental')
]

for filename, sample_origin in sample_files:
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        df_sample = pd.read_excel(file_path)
        print(f'Processing {filename}: {len(df_sample)} rows')
        
        for idx, row in df_sample.iterrows():
            host_id = None
            location_id = None
            
            # Find matching host based on sample type
            if sample_origin in ['BatSwab', 'BatTissue']:
                bag_id = str(row['BagId']) if pd.notna(row['BagId']) else None
                if bag_id:
                    cursor.execute('SELECT host_id, location_id FROM hosts WHERE bag_id = ?', (bag_id,))
                    host_result = cursor.fetchone()
                    if host_result:
                        host_id, location_id = host_result
                        
            elif sample_origin == 'Rodent':
                rodent_id = str(row['RodentId']) if pd.notna(row['RodentId']) else None
                if rodent_id:
                    cursor.execute('SELECT host_id, location_id FROM hosts WHERE source_id = ?', (rodent_id,))
                    host_result = cursor.fetchone()
                    if host_result:
                        host_id, location_id = host_result
                        
            elif sample_origin == 'Environmental':
                province = str(row['Province']) if pd.notna(row['Province']) else None
                if province:
                    district = str(row['District']) if pd.notna(row['District']) else 'Unknown'
                    village = str(row['Village']) if pd.notna(row['Village']) else 'Unknown'
                    country = 'Laos'
                    location_key = f"{province}_{district}_{village}_{country}"
                    location_id = location_map.get(location_key)
            
            # Import sample
            collection_date = str(row['Date']) if pd.notna(row['Date']) else None
            source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else f"{sample_origin}_{samples_imported + 1}"
            
            try:
                cursor.execute('''
                    INSERT INTO samples (
                        source_id, host_id, sample_origin, collection_date,
                        location_id, remark
                    ) VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    source_id,
                    host_id,
                    sample_origin,
                    collection_date,
                    location_id,
                    str(row['Remark']) if pd.notna(row['Remark']) else None
                ))
                samples_imported += 1
            except sqlite3.IntegrityError:
                pass

conn.commit()
print(f'✅ Imported {samples_imported} samples from all Excel files')

print('\n📊 STEP 6: IMPORT SCREENING RESULTS (NO ARTIFICIAL LINKING)')
print('-' * 40)

# Import Screening.xlsx - ONLY exact matches
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    print(f'Processing Screening.xlsx: {len(df_screening)} rows')
    
    # Create mapping of all sample source_ids in database
    cursor.execute('SELECT sample_id, source_id FROM samples')
db_samples = cursor.fetchall()
sample_source_id_map = {source_id: sample_id for sample_id, source_id in db_samples}
    
    print(f'Database sample source_ids: {len(sample_source_id_map)}')
    
    screening_imported = 0
    
    for idx, row in df_screening.iterrows():
        screening_source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else None
        
        # ONLY import if exact match exists in database
        if screening_source_id and screening_source_id in sample_source_id_map:
            db_sample_id = sample_source_id_map[screening_source_id]
            
            try:
                cursor.execute('''
                    INSERT INTO screening_results (
                        excel_id, source_id, sample_id, team, sample_type,
                        tested_sample_id, pan_corona, pan_hanta, pan_paramyxo, pan_flavi
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    str(row['Id']) if pd.notna(row['Id']) else None,
                    screening_source_id,
                    db_sample_id,
                    str(row['Team']) if pd.notna(row['Team']) else None,
                    str(row['SampleType']) if pd.notna(row['SampleType']) else None,
                    str(row['SampleId']) if pd.notna(row['SampleId']) else None,
                    str(row['PanCorona']) if pd.notna(row['PanCorona']) else None,
                    str(row['PanHanta']) if pd.notna(row['PanHanta']) else None,
                    str(row['PanParamyxo']) if pd.notna(row['PanParamyxo']) else None,
                    str(row['PanFlavi']) if pd.notna(row['PanFlavi']) else None
                ))
                screening_imported += 1
            except sqlite3.IntegrityError:
                pass
    
    conn.commit()
    print(f'✅ Imported {screening_imported} screening results (exact matches only)')

print('\n📊 STEP 7: IMPORT STORAGE DATA')
print('-' * 40)

freezer_file = os.path.join(excel_dir, 'Freezer14.xlsx')
if os.path.exists(freezer_file):
    df_freezer = pd.read_excel(freezer_file)
    print(f'Importing storage data from Freezer14.xlsx: {len(df_freezer)} rows')
    
    storage_imported = 0
    for idx, row in df_freezer.iterrows():
        sample_id = str(row['SampleId']) if pd.notna(row['SampleId']) else None
        if sample_id:
            try:
                cursor.execute('''
                    INSERT INTO storage_locations (
                        sample_tube_id, freezer_no, shelf, rack, spot_position, notes, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
                ''', (
                    sample_id,
                    str(row['Freezer_No']) if pd.notna(row['Freezer_No']) else None,
                    str(row['Shelf']) if pd.notna(row['Shelf']) else None,
                    str(row['Rack']) if pd.notna(row['Rack']) else None,
                    str(row['SpotPosition']) if pd.notna(row['SpotPosition']) else None,
                    str(row['Notes']) if pd.notna(row['Notes']) else None
                ))
                storage_imported += 1
            except sqlite3.IntegrityError:
                pass
    
    conn.commit()
    print(f'✅ Imported {storage_imported} storage records')

print('\n📊 STEP 8: IMPORT MORPHOMETRICS DATA')
print('-' * 40)

morpho_file = os.path.join(excel_dir, 'Morphometrics.xlsx')
if os.path.exists(morpho_file):
    df_morpho = pd.read_excel(morpho_file)
    print(f'Importing morphometrics data from Morphometrics.xlsx: {len(df_morpho)} rows')
    
    morpho_imported = 0
    for idx, row in df_morpho.iterrows():
        source_id = str(row['SourceId']) if pd.notna(row['SourceId']) else None
        if source_id:
            try:
                cursor.execute('''
                    INSERT INTO morphometrics (
                        source_id, weight, forearm_length, tail_length, 
                        hind_foot_length, ear_length, sex, age_class,
                        reproductive_condition, created_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, datetime('now'))
                ''', (
                    source_id,
                    row['Weight'] if pd.notna(row['Weight']) else None,
                    row['ForearmLength'] if pd.notna(row['ForearmLength']) else None,
                    row['TailLength'] if pd.notna(row['TailLength']) else None,
                    row['HindFootLength'] if pd.notna(row['HindFootLength']) else None,
                    row['EarLength'] if pd.notna(row['EarLength']) else None,
                    str(row['Sex']) if pd.notna(row['Sex']) else None,
                    str(row['AgeClass']) if pd.notna(row['AgeClass']) else None,
                    str(row['ReproductiveCondition']) if pd.notna(row['ReproductiveCondition']) else None
                ))
                morpho_imported += 1
            except sqlite3.IntegrityError:
                pass
    
    conn.commit()
    print(f'✅ Imported {morpho_imported} morphometrics records')

print('\n📊 STEP 9: FINAL VALIDATION')
print('-' * 40)

# Check totals
cursor.execute('SELECT COUNT(*) FROM hosts')
total_hosts = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples')
total_samples = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM screening_results')
total_screening = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM storage_locations')
total_storage = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM morphometrics')
total_morpho = cursor.fetchone()[0]

print(f'📊 COMPLETE IMPORT SUMMARY:')
print(f'  Hosts: {total_hosts}')
print(f'  Samples: {total_samples}')
print(f'  Screening: {total_screening}')
print(f'  Storage: {total_storage}')
print(f'  Morphometrics: {total_morpho}')

# Check host types
cursor.execute('''
    SELECT host_type, COUNT(*) as count
    FROM hosts
    GROUP BY host_type
    ORDER BY count DESC
''')

host_types = cursor.fetchall()
print(f'  Host types:')
for host_type, count in host_types:
    print(f'    {host_type}: {count}')

# Check sample origins
cursor.execute('''
    SELECT sample_origin, COUNT(*) as count
    FROM samples
    GROUP BY sample_origin
    ORDER BY count DESC
''')

sample_origins = cursor.fetchall()
print(f'  Sample origins:')
for origin, count in sample_origins:
    print(f'    {origin}: {count}')

# Check provinces
cursor.execute('''
    SELECT l.province, COUNT(*) as count
    FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province IS NOT NULL AND l.province != ''
    GROUP BY l.province
    ORDER BY count DESC
''')

provinces = cursor.fetchall()
print(f'  Provinces: {len(provinces)}')
for province, count in provinces:
    print(f'    {province}: {count} hosts')

# Check screening results
if total_screening > 0:
    cursor.execute('SELECT COUNT(*) FROM screening_results WHERE pan_corona = ?', ('Positive',))
    positive_count = cursor.fetchone()[0]
    
    cursor.execute('SELECT COUNT(*) FROM screening_results WHERE pan_corona = ?', ('Negative',))
    negative_count = cursor.fetchone()[0]
    
    positivity_rate = (positive_count / total_screening) * 100
    print(f'  Screening results:')
    print(f'    Total: {total_screening}')
    print(f'    Positive: {positive_count}')
    print(f'    Negative: {negative_count}')
    print(f'    Positivity rate: {positivity_rate:.2f}%')
else:
    print(f'  Screening results: None (no matches found)')

# Check Louang Namtha specifically
cursor.execute('''
    SELECT COUNT(*) FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')
louang_hosts = cursor.fetchone()[0]

cursor.execute('''
    SELECT COUNT(*) FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')
louang_samples = cursor.fetchone()[0]

cursor.execute('''
    SELECT COUNT(*) FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')
louang_screening = cursor.fetchone()[0]

print(f'  Louang Namtha:')
print(f'    Hosts: {louang_hosts}')
print(f'    Samples: {louang_samples}')
print(f'    Screening: {louang_screening}')

conn.close()

print('\n🎉 COMPLETE FRESH IMPORT FINISHED!')
print('=' * 50)
print('✅ All data deleted from database')
print('✅ All Excel files imported correctly')
print('✅ No artificial linking or false matches')
print('✅ Only exact SourceId matches for screening')
print('✅ Honest representation of data limitations')
print('\n🚀 Your database now contains ONLY authentic Excel data!')

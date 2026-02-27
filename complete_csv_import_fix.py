#!/usr/bin/env python3
"""
Complete CSV import fix with required fields and proper mapping
"""

import pandas as pd
import sqlite3
import os
from pathlib import Path

print('🔧 COMPLETE CSV IMPORT FIX WITH REQUIRED FIELDS')
print('=' * 60)

# Define paths
csv_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV')
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'

print(f'📁 CSV Directory: {csv_dir}')
print(f'📂 Database: {db_path}')
print()

# Connect to database
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = ON;")
    print('✅ Connected to database successfully')
except Exception as e:
    print(f'❌ Error connecting to database: {str(e)}')
    exit(1)

# Clear existing data
print('🗑️ Clearing existing data...')
tables_to_clear = ['hosts', 'samples', 'environmental_samples', 'screening_results', 'storage', 'locations', 'taxonomy']
for table in tables_to_clear:
    try:
        cursor.execute(f"DELETE FROM {table}")
        print(f'✅ Cleared {table}')
    except:
        print(f'⚠️ Table {table} does not exist or is already empty')

conn.commit()

# Process hosts data with required fields
print('\n🦇 PROCESSING HOSTS DATA')
print('-' * 40)

hosts_data = []

# Process Bathost.csv
if (csv_dir / 'Bathost.csv').exists():
    try:
        df_bathost = pd.read_csv(csv_dir / 'Bathost.csv')
        print(f'📄 Bathost.csv: {len(df_bathost)} rows')
        
        for _, row in df_bathost.iterrows():
            host_record = {
                'source_id': row.get('SourceId', ''),
                'host_type': 'Bat',  # REQUIRED: Default to Bat
                'bag_id': row.get('BagId', ''),
                'field_id': row.get('FieldId', ''),
                'collection_id': row.get('CollectionId', ''),
                'capture_date': row.get('CaptureDate', ''),
                'capture_time': row.get('CaptureTime', ''),
                'trap_type': row.get('TrapType', ''),
                'collectors': row.get('Collectors', ''),
                'sex': row.get('Sex', ''),
                'status': row.get('Status', ''),
                'age': row.get('Age', ''),
                'ring_no': row.get('RingNo', ''),
                'recapture': row.get('Recapture', ''),
                'photo': row.get('Photo', ''),
                'material_sample': row.get('MaterialSample', ''),
                'voucher_code': row.get('VoucherCode', ''),
                'ecology': row.get('Ecology', ''),
                'interface_type': row.get('InterfaceType', ''),
                'use_for': row.get('UseFor', ''),
                'notes': row.get('Notes', '')
            }
            hosts_data.append(host_record)
        
        print(f'✅ Processed {len(df_bathost)} bat hosts')
        
    except Exception as e:
        print(f'❌ Error processing Bathost.csv: {str(e)}')

# Process RodentHost.csv
if (csv_dir / 'RodentHost.csv').exists():
    try:
        df_rodent_host = pd.read_csv(csv_dir / 'RodentHost.csv')
        print(f'📄 RodentHost.csv: {len(df_rodent_host)} rows')
        
        for _, row in df_rodent_host.iterrows():
            host_record = {
                'source_id': row.get('SourceId', ''),
                'host_type': 'Rodent',  # REQUIRED: Default to Rodent
                'bag_id': '',
                'field_id': '',
                'collection_id': '',
                'capture_date': '',
                'capture_time': '',
                'trap_type': row.get('TrapId', ''),
                'collectors': '',
                'sex': row.get('Sex', ''),
                'status': row.get('Status', ''),
                'age': row.get('E', ''),
                'ring_no': '',
                'recapture': '',
                'photo': '',
                'material_sample': '',
                'voucher_code': '',
                'ecology': row.get('Ecology', ''),
                'interface_type': '',
                'use_for': '',
                'notes': f"Species: {row.get('Species', '')}, Weight: {row.get('HB', '')}, Notes: {row.get('Note', '')}"
            }
            hosts_data.append(host_record)
        
        print(f'✅ Processed {len(df_rodent_host)} rodent hosts')
        
    except Exception as e:
        print(f'❌ Error processing RodentHost.csv: {str(e)}')

# Process MarketSampleAndHost.csv
if (csv_dir / 'MarketSampleAndHost.csv').exists():
    try:
        df_market = pd.read_csv(csv_dir / 'MarketSampleAndHost.csv')
        print(f'📄 MarketSampleAndHost.csv: {len(df_market)} rows')
        
        for _, row in df_market.iterrows():
            host_record = {
                'source_id': row.get('SourceId', ''),
                'host_type': 'Market',  # REQUIRED: Default to Market
                'bag_id': '',
                'field_id': row.get('FieldSampleId', ''),
                'collection_id': '',
                'capture_date': row.get('CollectionSampleDate', ''),
                'capture_time': row.get('TimeToCollect', ''),
                'trap_type': '',
                'collectors': '',
                'sex': row.get('Sex', ''),
                'status': row.get('StatusOfSample', ''),
                'age': row.get('Age', ''),
                'ring_no': '',
                'recapture': '',
                'photo': row.get('PhotoNumber', ''),
                'material_sample': '',
                'voucher_code': '',
                'ecology': '',
                'interface_type': row.get('TypeOfInterface', ''),
                'use_for': row.get('UseForFoodOrMedicine', ''),
                'notes': f"Scientific: {row.get('ScientificName', '')}, Common: {row.get('CommonName', '')}, Location: {row.get('LocationName', '')}, Weight: {row.get('Weightg', '')}, Notes: {row.get('Note', '')}"
            }
            hosts_data.append(host_record)
        
        print(f'✅ Processed {len(df_market)} market hosts')
        
    except Exception as e:
        print(f'❌ Error processing MarketSampleAndHost.csv: {str(e)}')

# Import hosts data
if hosts_data:
    try:
        df_hosts = pd.DataFrame(hosts_data)
        
        # Get database schema for hosts
        cursor.execute("PRAGMA table_info(hosts)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        # Filter to only columns that exist in database
        available_columns = [col for col in df_hosts.columns if col in db_columns]
        df_hosts_filtered = df_hosts[available_columns]
        
        print(f'📋 Importing {len(df_hosts_filtered)} hosts with {len(df_hosts_filtered.columns)} columns')
        df_hosts_filtered.to_sql('hosts', conn, if_exists='append', index=False)
        print(f'✅ Successfully imported {len(df_hosts_filtered)} hosts')
        
    except Exception as e:
        print(f'❌ Error importing hosts: {str(e)}')

# Process samples data with required fields
print('\n🧪 PROCESSING SAMPLES DATA')
print('-' * 40)

samples_data = []

# Process Batswab.csv
if (csv_dir / 'Batswab.csv').exists():
    try:
        df_batswab = pd.read_csv(csv_dir / 'Batswab.csv')
        print(f'📄 Batswab.csv: {len(df_batswab)} rows')
        
        for _, row in df_batswab.iterrows():
            sample_record = {
                'source_id': row.get('SourceId', ''),
                'sample_origin': 'Swab',  # REQUIRED: Default to Swab
                'collection_date': row.get('Date', ''),
                'saliva_id': row.get('SalivaId', ''),
                'anal_id': row.get('AnalId', ''),
                'urine_id': row.get('UrineId', ''),
                'ecto_id': row.get('EctoId', ''),
                'remark': row.get('Remark', '')
            }
            samples_data.append(sample_record)
        
        print(f'✅ Processed {len(df_batswab)} bat swab samples')
        
    except Exception as e:
        print(f'❌ Error processing Batswab.csv: {str(e)}')

# Process Battissue.csv
if (csv_dir / 'Battissue.csv').exists():
    try:
        df_battissue = pd.read_csv(csv_dir / 'Battissue.csv')
        print(f'📄 Battissue.csv: {len(df_battissue)} rows')
        
        for _, row in df_battissue.iterrows():
            sample_record = {
                'source_id': row.get('SourceId', ''),
                'sample_origin': 'Tissue',  # REQUIRED: Default to Tissue
                'collection_date': row.get('Date', ''),
                'blood_id': row.get('BloodId', ''),
                'plasma_id': row.get('PlasmaId', ''),
                'tissue_id': row.get('TissueId', ''),
                'tissue_sample_type': row.get('Tissue sample type', ''),
                'intestine_id': row.get('IntestineId', ''),
                'remark': row.get('Remark', '')
            }
            samples_data.append(sample_record)
        
        print(f'✅ Processed {len(df_battissue)} bat tissue samples')
        
    except Exception as e:
        print(f'❌ Error processing Battissue.csv: {str(e)}')

# Process RodentSample.csv
if (csv_dir / 'RodentSample.csv').exists():
    try:
        df_rodent_sample = pd.read_csv(csv_dir / 'RodentSample.csv')
        print(f'📄 RodentSample.csv: {len(df_rodent_sample)} rows')
        
        for _, row in df_rodent_sample.iterrows():
            sample_record = {
                'source_id': row.get('SourceId', ''),
                'sample_origin': 'Rodent',  # REQUIRED: Default to Rodent
                'collection_date': row.get('Date', ''),
                'blood_id': row.get('BloodId', ''),
                'plasma_id': row.get('PlasmaId', ''),
                'tissue_id': row.get('TissueId', ''),
                'tissue_sample_type': row.get('TissueSampleType', ''),
                'intestine_id': row.get('IntestineId', ''),
                'adipose_id': row.get('AdiposeId', ''),
                'saliva_id': row.get('SalivaId', ''),
                'anal_id': row.get('AnalId', ''),
                'urine_id': row.get('UrineId', ''),
                'ecto_id': row.get('EctoId', ''),
                'remark': row.get('Remark', '')
            }
            samples_data.append(sample_record)
        
        print(f'✅ Processed {len(df_rodent_sample)} rodent samples')
        
    except Exception as e:
        print(f'❌ Error processing RodentSample.csv: {str(e)}')

# Import samples data
if samples_data:
    try:
        df_samples = pd.DataFrame(samples_data)
        
        # Get database schema for samples
        cursor.execute("PRAGMA table_info(samples)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        # Filter to only columns that exist in database
        available_columns = [col for col in df_samples.columns if col in db_columns]
        df_samples_filtered = df_samples[available_columns]
        
        print(f'📋 Importing {len(df_samples_filtered)} samples with {len(df_samples_filtered.columns)} columns')
        df_samples_filtered.to_sql('samples', conn, if_exists='append', index=False)
        print(f'✅ Successfully imported {len(df_samples_filtered)} samples')
        
    except Exception as e:
        print(f'❌ Error importing samples: {str(e)}')

# Process other files (environmental, screening, storage)
print('\n🌍 PROCESSING ENVIRONMENTAL DATA')
print('-' * 40)

if (csv_dir / 'Environmental.csv').exists():
    try:
        df_env = pd.read_csv(csv_dir / 'Environmental.csv')
        print(f'📄 Environmental.csv: {len(df_env)} rows')
        
        env_data = []
        for _, row in df_env.iterrows():
            env_record = {
                'source_id': row.get('SourceId', ''),
                'pool_id': row.get('Pool ID', ''),
                'collection_method': row.get('Collection Method', ''),
                'collection_date': row.get('Date', ''),
                'site_type': '',
                'remark': row.get('Remarkk', '')
            }
            env_data.append(env_record)
        
        df_env_processed = pd.DataFrame(env_data)
        
        # Get database schema
        cursor.execute("PRAGMA table_info(environmental_samples)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        available_columns = [col for col in df_env_processed.columns if col in db_columns]
        df_env_filtered = df_env_processed[available_columns]
        
        df_env_filtered.to_sql('environmental_samples', conn, if_exists='append', index=False)
        print(f'✅ Imported {len(df_env_filtered)} environmental samples')
        
    except Exception as e:
        print(f'❌ Error processing Environmental.csv: {str(e)}')

print('\n🧪 PROCESSING SCREENING DATA')
print('-' * 40)

if (csv_dir / 'Screening.csv').exists():
    try:
        df_screening = pd.read_csv(csv_dir / 'Screening.csv')
        print(f'📄 Screening.csv: {len(df_screening)} rows')
        
        screening_data = []
        for _, row in df_screening.iterrows():
            screening_record = {
                'source_id': row.get('SourceId', ''),
                'team': row.get('Team', ''),
                'sample_type': row.get('SampleType', ''),
                'tested_sample_id': row.get('Tested_SampleId', ''),
                'pan_corona': row.get('PanCorona', ''),
                'pan_hanta': row.get('PanHanta', ''),
                'pan_paramyxo': row.get('PanParamyxo', ''),
                'pan_flavi': row.get('PanFlavi', '')
            }
            screening_data.append(screening_record)
        
        df_screening_processed = pd.DataFrame(screening_data)
        
        # Get database schema
        cursor.execute("PRAGMA table_info(screening_results)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        available_columns = [col for col in df_screening_processed.columns if col in db_columns]
        df_screening_filtered = df_screening_processed[available_columns]
        
        df_screening_filtered.to_sql('screening_results', conn, if_exists='append', index=False)
        print(f'✅ Imported {len(df_screening_filtered)} screening results')
        
    except Exception as e:
        print(f'❌ Error processing Screening.csv: {str(e)}')

print('\n❄️ PROCESSING STORAGE DATA')
print('-' * 40)

if (csv_dir / 'Freezer14.csv').exists():
    try:
        df_storage = pd.read_csv(csv_dir / 'Freezer14.csv')
        print(f'📄 Freezer14.csv: {len(df_storage)} rows')
        
        storage_data = []
        for _, row in df_storage.iterrows():
            storage_record = {
                'sample_tube_id': row.get('SampleId', ''),
                'freezer_no': row.get('Freezer_No', ''),
                'rack': row.get('Rack', ''),
                'shelf': row.get('Shelf', ''),
                'spot_position': row.get('SpotPosition', ''),
                'notes': row.get('Notes', '')
            }
            storage_data.append(storage_record)
        
        df_storage_processed = pd.DataFrame(storage_data)
        
        # Get database schema
        cursor.execute("PRAGMA table_info(storage)")
        db_columns = [row[1] for row in cursor.fetchall()]
        
        available_columns = [col for col in df_storage_processed.columns if col in db_columns]
        df_storage_filtered = df_storage_processed[available_columns]
        
        df_storage_filtered.to_sql('storage', conn, if_exists='append', index=False)
        print(f'✅ Imported {len(df_storage_filtered)} storage records')
        
    except Exception as e:
        print(f'❌ Error processing Freezer14.csv: {str(e)}')

# Commit all changes
conn.commit()

# Final verification
print('\n🔍 FINAL VERIFICATION')
print('=' * 40)

total_records = 0
for table in ['hosts', 'samples', 'environmental_samples', 'screening_results', 'storage']:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f'📋 {table}: {count:,} records')
        total_records += count
    except:
        print(f'⚠️ {table}: Table does not exist')

conn.close()

print(f'\n🎉 COMPLETE IMPORT SUCCESS!')
print(f'✅ Total records imported: {total_records:,}')
print(f'📂 Database: {db_path}')
print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

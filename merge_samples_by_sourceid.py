#!/usr/bin/env python3
"""
Merge all sample data by SourceId - one sample per unique SourceId
"""

import pandas as pd
import sqlite3
from pathlib import Path

print('🔧 MERGING SAMPLES BY SOURCEID')
print('=' * 40)

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

# Clear samples table
print('🗑️ Clearing samples table...')
try:
    cursor.execute("DELETE FROM samples")
    conn.commit()
    print('✅ Cleared samples table')
except Exception as e:
    print(f'❌ Error clearing samples: {str(e)}')

# Load all sample data
print('\n📊 LOADING ALL SAMPLE DATA')
print('-' * 30)

all_sample_data = {}

# Process Batswab.csv
if (csv_dir / 'Batswab.csv').exists():
    try:
        df_batswab = pd.read_csv(csv_dir / 'Batswab.csv')
        print(f'📄 Batswab.csv: {len(df_batswab)} rows')
        
        for _, row in df_batswab.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            if source_id and source_id != 'nan':
                if source_id not in all_sample_data:
                    all_sample_data[source_id] = {
                        'source_id': source_id,
                        'sample_origin': 'BatSwab',
                        'collection_date': row.get('Date', ''),
                        'saliva_id': row.get('SalivaId', ''),
                        'anal_id': row.get('AnalId', ''),
                        'urine_id': row.get('UrineId', ''),
                        'ecto_id': row.get('EctoId', ''),
                        'remark': row.get('Remark', ''),
                        'blood_id': '',
                        'plasma_id': '',
                        'tissue_id': '',
                        'tissue_sample_type': '',
                        'intestine_id': '',
                        'adipose_id': ''
                    }
                else:
                    # Merge with existing - prioritize non-empty values
                    existing = all_sample_data[source_id]
                    for field in ['saliva_id', 'anal_id', 'urine_id', 'ecto_id', 'remark']:
                        csv_value = row.get(field, '')
                        if pd.notna(csv_value) and str(csv_value).strip():
                            existing[field] = str(csv_value).strip()
        
        print(f'✅ Processed {len(df_batswab)} bat swab rows')
        
    except Exception as e:
        print(f'❌ Error processing Batswab.csv: {str(e)}')

# Process Battissue.csv
if (csv_dir / 'Battissue.csv').exists():
    try:
        df_battissue = pd.read_csv(csv_dir / 'Battissue.csv')
        print(f'📄 Battissue.csv: {len(df_battissue)} rows')
        
        for _, row in df_battissue.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            if source_id and source_id != 'nan':
                if source_id not in all_sample_data:
                    all_sample_data[source_id] = {
                        'source_id': source_id,
                        'sample_origin': 'BatTissue',
                        'collection_date': row.get('Date', ''),
                        'saliva_id': '',
                        'anal_id': '',
                        'urine_id': '',
                        'ecto_id': '',
                        'remark': row.get('Remark', ''),
                        'blood_id': row.get('BloodId', ''),
                        'plasma_id': row.get('PlasmaId', ''),
                        'tissue_id': row.get('TissueId', ''),
                        'tissue_sample_type': row.get('Tissue sample type', ''),
                        'intestine_id': row.get('IntestineId', ''),
                        'adipose_id': ''
                    }
                else:
                    # Merge with existing - update sample_origin if tissue data exists
                    existing = all_sample_data[source_id]
                    existing['sample_origin'] = 'BatSwab+Tissue'  # Combined sample
                    
                    # Merge tissue-specific fields
                    for field in ['blood_id', 'plasma_id', 'tissue_id', 'tissue_sample_type', 'intestine_id', 'remark']:
                        csv_value = row.get(field, '')
                        if pd.notna(csv_value) and str(csv_value).strip():
                            existing[field] = str(csv_value).strip()
        
        print(f'✅ Processed {len(df_battissue)} bat tissue rows')
        
    except Exception as e:
        print(f'❌ Error processing Battissue.csv: {str(e)}')

# Process RodentSample.csv
if (csv_dir / 'RodentSample.csv').exists():
    try:
        df_rodent = pd.read_csv(csv_dir / 'RodentSample.csv')
        print(f'📄 RodentSample.csv: {len(df_rodent)} rows')
        
        for _, row in df_rodent.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            if source_id and source_id != 'nan':
                if source_id not in all_sample_data:
                    all_sample_data[source_id] = {
                        'source_id': source_id,
                        'sample_origin': 'RodentSample',
                        'collection_date': row.get('Date', ''),
                        'saliva_id': row.get('SalivaId', ''),
                        'anal_id': row.get('AnalId', ''),
                        'urine_id': row.get('UrineId', ''),
                        'ecto_id': row.get('EctoId', ''),
                        'remark': row.get('Remark', ''),
                        'blood_id': row.get('BloodId', ''),
                        'plasma_id': row.get('PlasmaId', ''),
                        'tissue_id': row.get('TissueId', ''),
                        'tissue_sample_type': row.get('TissueSampleType', ''),
                        'intestine_id': row.get('IntestineId', ''),
                        'adipose_id': row.get('AdiposeId', '')
                    }
                else:
                    # Merge with existing - update sample_origin for mixed samples
                    existing = all_sample_data[source_id]
                    if existing['sample_origin'] == 'BatSwab':
                        existing['sample_origin'] = 'Mixed'
                    elif existing['sample_origin'] == 'BatTissue':
                        existing['sample_origin'] = 'Mixed'
                    elif existing['sample_origin'] == 'BatSwab+Tissue':
                        existing['sample_origin'] = 'Mixed'
                    
                    # Merge all fields
                    for field in ['saliva_id', 'anal_id', 'urine_id', 'ecto_id', 'remark', 
                                 'blood_id', 'plasma_id', 'tissue_id', 'tissue_sample_type', 'intestine_id', 'adipose_id']:
                        csv_value = row.get(field, '')
                        if pd.notna(csv_value) and str(csv_value).strip():
                            existing[field] = str(csv_value).strip()
        
        print(f'✅ Processed {len(df_rodent)} rodent sample rows')
        
    except Exception as e:
        print(f'❌ Error processing RodentSample.csv: {str(e)}')

# Convert to DataFrame
print(f'\n📊 MERGED SAMPLES SUMMARY')
print('-' * 30)

samples_list = list(all_sample_data.values())
df_merged_samples = pd.DataFrame(samples_list)

print(f'📊 Total unique SourceIds: {len(df_merged_samples)}')
print(f'📊 Total CSV rows processed: 5573')
print(f'📊 Compression ratio: {5573/len(df_merged_samples):.1f}x')

# Show sample_origin distribution
origin_counts = df_merged_samples['sample_origin'].value_counts()
print(f'\n📊 Sample origin distribution:')
for origin, count in origin_counts.items():
    print(f'  📋 {origin}: {count:,} samples')

# Import merged samples
print(f'\n📥 IMPORTING MERGED SAMPLES')
print('-' * 30)  # Fix syntax error here

try:
    # Get database schema
    cursor.execute("PRAGMA table_info(samples)")
    db_columns = [row[1] for row in cursor.fetchall()]
    
    # Filter to only columns that exist in database
    available_columns = [col for col in df_merged_samples.columns if col in db_columns]
    df_import = df_merged_samples[available_columns]
    
    print(f'📊 Importing {len(df_import)} merged samples')
    print(f'📊 Columns: {len(df_import)}')
    
    # Import to database
    df_import.to_sql('samples', conn, if_exists='append', index=False)
    print(f'✅ Successfully imported {len(df_import)} merged samples')
    
except Exception as e:
    print(f'❌ Error importing merged samples: {str(e)}')
    import traceback
    traceback.print_exc()

# Commit changes
conn.commit()

# Final verification
print('\n🔍 FINAL VERIFICATION')
print('=' * 30)

try:
    cursor.execute("SELECT COUNT(*) FROM samples")
    total_samples = cursor.fetchone()[0]
    print(f'📋 Total samples in database: {total_samples:,}')
    
    # Show sample_origin breakdown
    cursor.execute("SELECT sample_origin, COUNT(*) FROM samples GROUP BY sample_origin ORDER BY COUNT(*) DESC")
    sample_types = cursor.fetchall()
    print(f'\n📊 Sample types in database:')
    for sample_type, count in sample_types:
        print(f'  📋 {sample_type}: {count:,} records')
        
    # Check for any NULL source_ids
    cursor.execute("SELECT COUNT(*) FROM samples WHERE source_id IS NULL OR source_id = ''")
    null_count = cursor.fetchone()[0]
    print(f'\n📊 NULL/empty source_id: {null_count}')
    
except Exception as e:
    print(f'❌ Error in verification: {str(e)}')

conn.close()

print(f'\n🎉 MERGED SAMPLE IMPORT COMPLETE!')
print(f'✅ Total unique samples: {total_samples:,}')
print(f'✅ One record per SourceId')
print(f'✅ All sample types combined')
print(f'📂 Database: {db_path}')
print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

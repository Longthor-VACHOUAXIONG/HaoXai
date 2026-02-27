#!/usr/bin/env python3
"""
Add ear_id column to samples table and fix ear sample mapping
"""

import sqlite3
import pandas as pd
from pathlib import Path

print('🔧 ADDING EAR_ID COLUMN AND FIXING MAPPING')
print('=' * 50)

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

# Check current samples table structure
print('\n🔍 CURRENT SAMPLES TABLE STRUCTURE:')
print('-' * 40)

cursor.execute('PRAGMA table_info(samples)')
sample_columns = cursor.fetchall()
print('📋 Current columns:')
for col in sample_columns:
    print(f'  📋 {col[1]} ({col[2]})')

# Check if ear_id column already exists
ear_column_exists = any(col[1] == 'ear_id' for col in sample_columns)
print(f'\n📋 ear_id column exists: {ear_column_exists}')

# Add ear_id column if it doesn't exist
if not ear_column_exists:
    print('\n🔧 ADDING EAR_ID COLUMN:')
    try:
        cursor.execute('ALTER TABLE samples ADD COLUMN ear_id TEXT')
        conn.commit()
        print('✅ ear_id column added successfully')
    except Exception as e:
        print(f'❌ Error adding ear_id column: {str(e)}')
        exit(1)
else:
    print('\n✅ ear_id column already exists')

# Clear incorrect urine_id mappings for ear samples
print('\n🗑️ CLEARING INCORRECT URINE_ID MAPPINGS FOR EAR SAMPLES:')
cursor.execute('''
    UPDATE samples 
    SET urine_id = NULL 
    WHERE sample_origin = 'MarketSample' 
    AND urine_id IS NOT NULL 
    AND urine_id LIKE '%EAR%'
''')
cleared_count = cursor.rowcount
conn.commit()
print(f'✅ Cleared {cleared_count} incorrect urine_id mappings')

# Fix ear sample mapping
print('\n🔧 FIXING EAR SAMPLE MAPPING TO EAR_ID COLUMN:')

if (csv_dir / 'MarketSampleAndHost.csv').exists():
    try:
        df_market = pd.read_csv(csv_dir / 'MarketSampleAndHost.csv')
        print(f'📄 MarketSampleAndHost.csv: {len(df_market)} rows')
        
        # Process ear samples
        ear_samples_updated = 0
        errors = 0
        
        for _, row in df_market.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            field_sample_id = str(row.get('FieldSampleId', '')).strip()
            
            if source_id and source_id != 'nan' and field_sample_id and field_sample_id != 'nan':
                # Check if this is an ear sample
                if 'EAR' in field_sample_id:
                    # Get the sample record
                    cursor.execute('''
                        SELECT sample_id 
                        FROM samples 
                        WHERE source_id = ? AND sample_origin = 'MarketSample'
                    ''', (source_id,))
                    sample_result = cursor.fetchone()
                    
                    if sample_result:
                        sample_id = sample_result[0]
                        
                        # Update ear_id field
                        cursor.execute('''
                            UPDATE samples 
                            SET ear_id = ?
                            WHERE sample_id = ?
                        ''', (field_sample_id, sample_id))
                        ear_samples_updated += 1
                    else:
                        errors += 1
                        print(f'❌ Sample not found for source_id: {source_id}')
        
        conn.commit()
        
        print(f'✅ Ear samples updated: {ear_samples_updated}')
        print(f'❌ Errors: {errors}')
        
    except Exception as e:
        print(f'❌ Error processing MarketSampleAndHost.csv: {str(e)}')
        import traceback
        traceback.print_exc()
else:
    print('❌ MarketSampleAndHost.csv not found')

# Verification
print('\n🔍 VERIFICATION')
print('=' * 15)

# Check updated table structure
cursor.execute('PRAGMA table_info(samples)')
updated_columns = cursor.fetchall()
print('📋 Updated samples table columns:')
for col in updated_columns:
    if col[1] in ['sample_id', 'saliva_id', 'anal_id', 'urine_id', 'ear_id', 'blood_id', 'tissue_id']:
        print(f'  📋 {col[1]} ({col[2]})')

# Check biological ID counts
cursor.execute('''
    SELECT 
        COUNT(CASE WHEN saliva_id IS NOT NULL THEN 1 END) as saliva_samples,
        COUNT(CASE WHEN anal_id IS NOT NULL THEN 1 END) as anal_samples,
        COUNT(CASE WHEN urine_id IS NOT NULL THEN 1 END) as urine_samples,
        COUNT(CASE WHEN ear_id IS NOT NULL THEN 1 END) as ear_samples,
        COUNT(CASE WHEN blood_id IS NOT NULL THEN 1 END) as blood_samples,
        COUNT(CASE WHEN tissue_id IS NOT NULL THEN 1 END) as tissue_samples
    FROM samples
    WHERE sample_origin = 'MarketSample'
''')
bio_id_counts = cursor.fetchone()

saliva_count, anal_count, urine_count, ear_count, blood_count, tissue_count = bio_id_counts
print(f'\\n📊 Biological ID counts for market samples:')
print(f'  🧪 Saliva samples: {saliva_count:,}')
print(f'  🔬 Anal samples: {anal_count:,}')
print(f'  💧 Urine samples: {urine_count:,}')
print(f'  👂 Ear samples: {ear_count:,}')
print(f'  🩸 Blood samples: {blood_count:,}')
print(f'  🧬 Tissue samples: {tissue_count:,}')

# Show sample examples with correct mapping
cursor.execute('''
    SELECT 
        s.sample_id,
        s.source_id,
        s.saliva_id,
        s.anal_id,
        s.urine_id,
        s.ear_id,
        t.scientific_name
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    WHERE s.sample_origin = 'MarketSample'
    AND (s.saliva_id IS NOT NULL OR s.anal_id IS NOT NULL OR s.urine_id IS NOT NULL OR s.ear_id IS NOT NULL)
    ORDER BY s.sample_id
    LIMIT 15
''')
sample_examples = cursor.fetchall()

print(f'\\n📊 Sample examples with correct biological ID mapping:')
for sample_id, source_id, saliva_id, anal_id, urine_id, ear_id, sci_name in sample_examples:
    print(f'  📋 Sample {sample_id}: {source_id} ({sci_name})')
    if saliva_id:
        print(f'    🧪 Saliva: {saliva_id}')
    if anal_id:
        print(f'    🔬 Anal: {anal_id}')
    if urine_id:
        print(f'    💧 Urine: {urine_id}')
    if ear_id:
        print(f'    👂 Ear: {ear_id}')

# Check FieldSampleId patterns by type
cursor.execute('''
    SELECT 
        COUNT(CASE WHEN saliva_id IS NOT NULL THEN 1 END) as count,
        'Saliva' as type,
        GROUP_CONCAT(DISTINCT SUBSTR(saliva_id, 1, 15)) as examples
    FROM samples
    WHERE sample_origin = 'MarketSample' AND saliva_id IS NOT NULL
    UNION ALL
    SELECT 
        COUNT(CASE WHEN anal_id IS NOT NULL THEN 1 END) as count,
        'Anal' as type,
        GROUP_CONCAT(DISTINCT SUBSTR(anal_id, 1, 15)) as examples
    FROM samples
    WHERE sample_origin = 'MarketSample' AND anal_id IS NOT NULL
    UNION ALL
    SELECT 
        COUNT(CASE WHEN ear_id IS NOT NULL THEN 1 END) as count,
        'Ear' as type,
        GROUP_CONCAT(DISTINCT SUBSTR(ear_id, 1, 15)) as examples
    FROM samples
    WHERE sample_origin = 'MarketSample' AND ear_id IS NOT NULL
    ORDER BY count DESC
''')
type_patterns = cursor.fetchall()

print(f'\\n📊 Sample type patterns:')
for count, sample_type, examples in type_patterns:
    if count > 0:
        example_list = examples.split(',')[:3] if examples else []
        print(f'  📋 {sample_type}: {count:,} samples')
        print(f'    📋 Examples: {example_list}')

conn.close()

print(f'\\n🎉 EAR_ID COLUMN ADDITION AND MAPPING FIX COMPLETE!')
print(f'✅ Ear samples updated: {ear_count}')
print(f'✅ Biological IDs properly mapped')
print(f'📂 Database: {db_path}')
print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

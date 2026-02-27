#!/usr/bin/env python3
"""
Link PT* environmental samples to samples table
"""

import sqlite3
import pandas as pd
from pathlib import Path

print('🔧 LINKING PT* ENVIRONMENTAL SAMPLES TO SAMPLES TABLE')
print('=' * 55)

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

# Check current status
cursor.execute('SELECT COUNT(*) FROM environmental_samples WHERE source_id LIKE "%PT%"')
env_pt_count = cursor.fetchone()[0]
print(f'📋 PT* environmental samples: {env_pt_count}')

cursor.execute('SELECT COUNT(*) FROM samples WHERE env_sample_id IS NOT NULL')
samples_with_env = cursor.fetchone()[0]
print(f'📋 Samples with env_sample_id: {samples_with_env}')

# Link PT* environmental samples to samples table
print(f'\n🔧 LINKING PT* ENVIRONMENTAL SAMPLES:')
print('-' * 40)

if (csv_dir / 'Environmental.csv').exists():
    try:
        df_env = pd.read_csv(csv_dir / 'Environmental.csv')
        print(f'📄 Environmental.csv: {len(df_env)} rows')
        
        linked_samples = 0
        errors = 0
        
        for _, row in df_env.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            env_sample_id = row.get('Id')
            
            if source_id and source_id != 'nan' and 'PT' in source_id:
                # Check if this environmental sample exists
                cursor.execute('SELECT env_sample_id FROM environmental_samples WHERE source_id = ?', (source_id,))
                env_result = cursor.fetchone()
                
                if env_result:
                    env_id = env_result[0]
                    
                    # Create a sample record for this environmental sample
                    try:
                        cursor.execute('''
                            INSERT INTO samples (
                                source_id, sample_origin, env_sample_id
                            ) VALUES (?, ?, ?)
                        ''', (source_id, 'Environmental', env_id))
                        
                        linked_samples += 1
                        
                    except Exception as e:
                        # Check if sample already exists
                        if 'UNIQUE constraint failed' in str(e):
                            # Update existing sample
                            cursor.execute('''
                                UPDATE samples 
                                SET env_sample_id = ?
                                WHERE source_id = ?
                            ''', (env_id, source_id))
                            linked_samples += 1
                        else:
                            errors += 1
                            if errors <= 3:
                                print(f'  ❌ Error with {source_id}: {str(e)}')
                else:
                    errors += 1
                    if errors <= 3:
                        print(f'  ❌ Environmental sample not found for {source_id}')
        
        conn.commit()
        
        print(f'✅ PT* environmental samples linked: {linked_samples}')
        print(f'❌ Errors: {errors}')
        
    except Exception as e:
        print(f'❌ Error processing Environmental.csv: {str(e)}')
        import traceback
        traceback.print_exc()

# Verification
print(f'\n🔍 VERIFICATION')
print('=' * 15)

# Check final counts
cursor.execute('SELECT COUNT(*) FROM samples WHERE sample_origin = "Environmental"')
final_env_samples = cursor.fetchone()[0]
print(f'📋 Final Environmental samples: {final_env_samples}')

cursor.execute('SELECT COUNT(*) FROM samples WHERE env_sample_id IS NOT NULL')
final_env_linked = cursor.fetchone()[0]
print(f'📋 Samples with env_sample_id: {final_env_linked}')

# Check PT* environmental samples linked
cursor.execute('''
    SELECT COUNT(*) FROM samples s
    JOIN environmental_samples e ON s.env_sample_id = e.env_sample_id
    WHERE e.source_id LIKE '%PT%'
''')
pt_linked_count = cursor.fetchone()[0]
print(f'📋 PT* environmental samples linked: {pt_linked_count}')

# Show sample examples
cursor.execute('''
    SELECT 
        s.sample_id,
        s.source_id,
        s.sample_origin,
        s.env_sample_id,
        e.site_type,
        e.collection_method,
        e.collection_date
    FROM samples s
    JOIN environmental_samples e ON s.env_sample_id = e.env_sample_id
    WHERE e.source_id LIKE '%PT%'
    ORDER BY s.sample_id
    LIMIT 10
''')
sample_examples = cursor.fetchall()

print(f'\n📊 PT* Environmental sample examples:')
for sample_id, source_id, origin, env_id, site_type, method, date in sample_examples:
    print(f'  📋 Sample {sample_id}: {source_id} → {origin}')
    print(f'    📋 EnvSampleId: {env_id}, SiteType: {site_type}')
    print(f'    📋 Method: {method}, Date: {date}')

# Check overall sample distribution
cursor.execute('''
    SELECT sample_origin, COUNT(*) as count
    FROM samples
    GROUP BY sample_origin
    ORDER BY count DESC
''')
origin_distribution = cursor.fetchall()

print(f'\n📊 Final sample origin distribution:')
for origin, count in origin_distribution:
    print(f'  📋 {origin}: {count:,} samples')

conn.close()

print(f'\n🎉 PT* ENVIRONMENTAL SAMPLES LINKING COMPLETE!')
print(f'✅ PT* environmental samples linked: {pt_linked_count}')
print(f'✅ Environmental samples: {final_env_samples}')
print(f'📂 Database: {db_path}')
print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

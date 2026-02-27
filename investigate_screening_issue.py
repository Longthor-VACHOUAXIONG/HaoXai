#!/usr/bin/env python3
"""
Investigate why screening results are blank
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING WHY SCREENING RESULTS ARE BLANK')
print('=' * 60)

# Connect to database
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Check screening_results table
cursor.execute('SELECT COUNT(*) FROM screening_results')
screening_count = cursor.fetchone()[0]
print(f'Screening records in database: {screening_count}')

if screening_count == 0:
    print('❌ NO SCREENING RECORDS FOUND IN DATABASE')
    print('\n🔍 INVESTIGATING THE CAUSE:')
    
    # Check what sample IDs we have in database
    cursor.execute('SELECT COUNT(*) FROM samples')
    sample_count = cursor.fetchone()[0]
    print(f'Sample records in database: {sample_count}')
    
    if sample_count > 0:
        cursor.execute('SELECT source_id FROM samples LIMIT 10')
        sample_ids = [row[0] for row in cursor.fetchall()]
        print(f'Sample IDs in database (first 10): {sample_ids}')
    
    # Check what sample IDs are in Screening.xlsx
    excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'
    screening_file = os.path.join(excel_dir, 'Screening.xlsx')
    
    if os.path.exists(screening_file):
        df_screening = pd.read_excel(screening_file)
        print(f'\n📋 Screening.xlsx has {len(df_screening)} records')
        sample_ids_excel = list(df_screening['SampleId'].head(10))
        print(f'Sample IDs in Screening.xlsx (first 10): {sample_ids_excel}')
        
        # Check for any matches
        if sample_count > 0:
            cursor.execute('SELECT source_id FROM samples')
            db_sample_ids = set([row[0] for row in cursor.fetchall()])
            excel_sample_ids = set(df_screening['SampleId'].astype(str).tolist())
            
            matches = db_sample_ids.intersection(excel_sample_ids)
            print(f'\n🔍 ID MATCH ANALYSIS:')
            print(f'Database sample IDs: {len(db_sample_ids)}')
            print(f'Excel sample IDs: {len(excel_sample_ids)}')
            print(f'Direct matches: {len(matches)}')
            
            if len(matches) > 0:
                print(f'Matching IDs: {list(matches)[:5]}')
            else:
                print('❌ NO DIRECT MATCHES FOUND')
                print('\n🔍 ALTERNATIVE MATCHING ATTEMPTS:')
                
                # Try matching by partial patterns
                excel_patterns = set()
                for sample_id in excel_sample_ids:
                    if 'CANB_' in sample_id:
                        excel_patterns.add('CANB_')
                    elif 'CANR_' in sample_id:
                        excel_patterns.add('CANR_')
                    elif 'CANA_PT' in sample_id:
                        excel_patterns.add('CANA_PT')
                    elif 'IPLNAHL' in sample_id:
                        excel_patterns.add('IPLNAHL')
                
                db_patterns = set()
                for sample_id in db_sample_ids:
                    if 'CANB_' in sample_id:
                        db_patterns.add('CANB_')
                    elif 'CANR_' in sample_id:
                        db_patterns.add('CANR_')
                    elif 'BatSwab' in sample_id:
                        db_patterns.add('BatSwab')
                    elif 'BatTissue' in sample_id:
                        db_patterns.add('BatTissue')
                
                pattern_matches = excel_patterns.intersection(db_patterns)
                print(f'Pattern matches: {pattern_matches}')
                
                # Check if we can match by other fields
                print('\n🔍 CHECKING FOR OTHER MATCHING FIELDS:')
                if 'tested_sample_id' in df_screening.columns:
                    tested_ids = set(df_screening['tested_sample_id'].astype(str).tolist())
                    tested_matches = db_sample_ids.intersection(tested_ids)
                    print(f'Tested sample ID matches: {len(tested_matches)}')
                    if len(tested_matches) > 0:
                        print(f'Matching tested IDs: {list(tested_matches)[:5]}')

else:
    print('✅ SCREENING RECORDS FOUND')
    cursor.execute('SELECT COUNT(*) FROM screening_results WHERE pan_corona IS NOT NULL')
    positive_count = cursor.fetchone()[0]
    cursor.execute('SELECT COUNT(*) FROM screening_results WHERE pan_corona = ?', ('Positive',))
    positive_exact = cursor.fetchone()[0]
    
    print(f'Screening with any pan_corona value: {positive_count}')
    print(f'Screening with Positive pan_corona: {positive_exact}')
    
    if positive_count == 0:
        print('❌ SCREENING RECORDS EXIST BUT pan_corona IS NULL/EMPTY')
        cursor.execute('SELECT * FROM screening_results LIMIT 5')
        sample_records = cursor.fetchall()
        cursor.execute('PRAGMA table_info(screening_results)')
        columns = cursor.fetchall()
        column_names = [col[1] for col in columns]
        
        print(f'Screening table columns: {column_names}')
        print(f'Sample screening records:')
        for i, record in enumerate(sample_records, 1):
            print(f'  {i}. {record}')

conn.close()

print('\n🎯 SCREENING INVESTIGATION COMPLETE')
print('=' * 40)

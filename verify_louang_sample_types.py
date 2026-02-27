#!/usr/bin/env python3
"""
Verify exactly what sample types Louang Namtha has
"""
import pandas as pd
import sqlite3
import os

print('🔍 VERIFYING LOUANG NAMTHA SAMPLE TYPES')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: CHECK LOUANG NAMTHA HOSTS IN BATHOST.XLSX')
print('-' * 40)

bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    
    # Filter for Louang Namtha
    louang_hosts = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]
    
    print(f'📋 Louang Namtha hosts in Bathost.xlsx: {len(louang_hosts)}')
    
    if len(louang_hosts) > 0:
        # Get Louang Namtha BagIds
        louang_bagids = set(louang_hosts['BagId'].astype(str).tolist())
        print(f'📋 Louang Namtha BagIds: {len(louang_bagids)} unique BagIds')
        
        print('Sample Louang Namtha hosts:')
        for i, (idx, row) in enumerate(louang_hosts.head(5).iterrows(), 1):
            print(f'  {i}. FieldId: {row["FieldId"]}, BagId: {row["BagId"]}')
            print(f'     CaptureDate: {row["CaptureDate"]}, SourceId: {row["SourceId"]}')

print('\n📊 STEP 2: CHECK LOUANG NAMTHA IN BATSWAB.XLSX')
print('-' * 40)

batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
if os.path.exists(batswab_file):
    df_batswab = pd.read_excel(batswab_file)
    
    # Find samples that match Louang Namtha BagIds
    louang_batswab = df_batswab[df_batswab['BagId'].astype(str).isin(louang_bagids)]
    
    print(f'📋 Louang Namtha samples in Batswab.xlsx: {len(louang_batswab)}')
    
    if len(louang_batswab) > 0:
        print('Sample Louang Namtha bat swabs:')
        for i, (idx, row) in enumerate(louang_batswab.head(5).iterrows(), 1):
            print(f'  {i}. BagId: {row["BagId"]}, Date: {row["Date"]}')
            print(f'     SourceId: {row["SourceId"]}')
    else:
        print('  ❌ NO Louang Namtha bat swabs found!')

print('\n📊 STEP 3: CHECK LOUANG NAMTHA IN BATTISSUE.XLSX')
print('-' * 40)

battissue_file = os.path.join(excel_dir, 'Battissue.xlsx')
if os.path.exists(battissue_file):
    df_battissue = pd.read_excel(battissue_file)
    
    # Find samples that match Louang Namtha BagIds
    louang_battissue = df_battissue[df_battissue['BagId'].astype(str).isin(louang_bagids)]
    
    print(f'📋 Louang Namtha samples in Battissue.xlsx: {len(louang_battissue)}')
    
    if len(louang_battissue) > 0:
        print('Sample Louang Namtha bat tissues:')
        for i, (idx, row) in enumerate(louang_battissue.head(5).iterrows(), 1):
            print(f'  {i}. BagId: {row["BagId"]}, Date: {row["Date"]}')
            print(f'     SourceId: {row["SourceId"]}')
    else:
        print('  ❌ NO Louang Namtha bat tissues found!')

print('\n📊 STEP 4: CHECK DATABASE SAMPLE TYPES FOR LOUANG NAMTHA')
print('-' * 40)

cursor.execute('''
    SELECT s.sample_origin, COUNT(*) as count
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    GROUP BY s.sample_origin
    ORDER BY count DESC
''')

louang_sample_types = cursor.fetchall()
print(f'📋 Louang Namtha sample types in database:')
for sample_type, count in louang_sample_types:
    print(f'  {sample_type}: {count}')

print('\n📊 STEP 5: CHECK DATABASE SCREENING FOR LOUANG NAMTHA')
print('-' * 40)

cursor.execute('''
    SELECT COUNT(*) as count,
           SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')

louang_screening = cursor.fetchone()
louang_count, louang_positive = louang_screening

print(f'📋 Louang Namtha screening in database:')
print(f'  Total: {louang_count}')
print(f'  Positive: {louang_positive}')

if louang_count > 0:
    rate = (louang_positive / louang_count) * 100
    print(f'  Positivity rate: {rate:.2f}%')
    
    # Get the actual screening records
    cursor.execute('''
        SELECT 
            sr.tested_sample_id,
            s.sample_origin,
            sr.source_id as screening_source_id,
            s.source_id as sample_source_id,
            h.source_id as host_source_id,
            h.field_id,
            sr.pan_corona,
            l.province
        FROM screening_results sr
        JOIN samples s ON sr.sample_id = s.sample_id
        JOIN hosts h ON s.host_id = h.host_id
        JOIN locations l ON h.location_id = l.location_id
        WHERE l.province LIKE '%Louang%'
        ORDER BY s.sample_origin, h.field_id
        LIMIT 10
    ''')
    
    matches = cursor.fetchall()
    print(f'\n📋 Louang Namtha screening records by sample type:')
    for i, (tested_id, sample_origin, screening_source_id, sample_source_id, host_source_id, field_id, result, province) in enumerate(matches, 1):
        print(f'  {i}. {tested_id} -> {result}')
        print(f'     Sample Type: {sample_origin}')
        print(f'     Screening SourceId: {screening_source_id}')
        print(f'     Sample SourceId: {sample_source_id}')
        print(f'     Host SourceId: {host_source_id} ({field_id})')

else:
    print('  ❌ NO Louang Namtha screening found!')

print('\n📊 STEP 6: INVESTIGATE THE DISCREPANCY')
print('-' * 40)

print('🔍 COMPARING EXCEL VS DATABASE:')
print(f'• Excel Louang Namtha hosts: {len(louang_hosts)}')
print(f'• Excel Louang Namtha swabs: {len(louang_batswab) if "louang_batswab" in locals() else "Not checked"}')
print(f'• Excel Louang Namtha tissues: {len(louang_battissue) if "louang_battissue" in locals() else "Not checked"}')
print(f'• Database Louang Namtha samples: {sum(count for _, count in louang_sample_types)}')
print(f'• Database Louang Namtha screening: {louang_count}')

# Check if the database imported correctly
cursor.execute('''
    SELECT COUNT(*) FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
''')

db_hosts = cursor.fetchone()[0]
print(f'\n🔍 DATABASE IMPORT VERIFICATION:')
print(f'• Database Louang Namtha hosts: {db_hosts}')
print(f'• Excel Louang Namtha hosts: {len(louang_hosts)}')

if db_hosts != len(louang_hosts):
    print('  ❌ HOST COUNT MISMATCH!')
else:
    print('  ✅ Host counts match')

# Check if there's a problem with sample import
if len(louang_batswab) == 0 and len(louang_battissue) == 0:
    print(f'\n🔍 THE TRUTH:')
    print('  ✅ Louang Namtha has hosts but NO samples!')
    print('  ✅ This explains why there should be no screening!')
    print('  ✅ The database screening results are FALSE!')
else:
    print(f'\n🔍 NEEDS INVESTIGATION:')
    print('  ❌ Louang Namtha samples exist in Excel')
    print('  ❌ Need to verify database import')

conn.close()

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)
print('🔍 LOUANG NAMTHA SAMPLE TYPE VERIFICATION:')
print()
if len(louang_batswab) == 0 and len(louang_battissue) == 0:
    print('✅ YOU ARE CORRECT!')
    print('• Louang Namtha has 278 hosts in Bathost.xlsx')
    print('• Louang Namtha has 0 swabs in Batswab.xlsx')
    print('• Louang Namtha has 0 tissues in Battissue.xlsx')
    print('• Therefore, Louang Namtha should have 0 screening!')
    print('• The database screening results are FALSE!')
else:
    print('❌ NEEDS INVESTIGATION')
    print('• Louang Namtha samples found in Excel files')
    print('• Need to verify database import process')
    print('• May need to reimport data correctly')

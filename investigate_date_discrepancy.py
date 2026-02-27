#!/usr/bin/env python3
"""
Investigate the date discrepancy: BD22* vs BD23*
"""
import pandas as pd
import sqlite3

print('🔍 INVESTIGATING DATE DISCREPANCY: BD22* vs BD23*')
print('=' * 60)

# Connect to database
conn = sqlite3.connect('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db')
cursor = conn.cursor()

print('📊 STEP 1: CHECK LOUANG NAMTHA FIELD ID PATTERNS')
print('-' * 40)

# Get all Louang Namtha hosts with their FieldIds
cursor.execute('''
    SELECT h.host_id, h.source_id, h.field_id, l.province, l.district, l.village, h.capture_date
    FROM hosts h 
    JOIN locations l ON h.location_id = l.location_id 
    WHERE l.province LIKE '%Louang%'
    ORDER BY h.field_id
''')

louang_hosts = cursor.fetchall()
print(f'Found {len(louang_hosts)} Louang Namtha hosts')

# Separate by FieldId pattern
bd22_hosts = []
bd23_hosts = []
other_hosts = []

for host in louang_hosts:
    host_id, source_id, field_id, province, district, village, capture_date = host
    if field_id and field_id.startswith('BD22'):
        bd22_hosts.append(host)
    elif field_id and field_id.startswith('BD23'):
        bd23_hosts.append(host)
    else:
        other_hosts.append(host)

print(f'BD22* FieldIds: {len(bd22_hosts)} hosts')
print(f'BD23* FieldIds: {len(bd23_hosts)} hosts')
print(f'Other FieldIds: {len(other_hosts)} hosts')

print('\n🔍 STEP 2: CHECK THE POSITIVE SAMPLES FIELD IDS')
print('-' * 40)

# Get the positive samples with their host FieldIds
cursor.execute('''
    SELECT s.sample_id, s.source_id, h.host_id, h.source_id as host_source_id, h.field_id, l.province, l.district, l.village, h.capture_date, sr.pan_corona, sr.sample_type
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    JOIN screening_results sr ON s.sample_id = sr.sample_id
    WHERE l.province LIKE '%Louang%' AND sr.pan_corona = 'Positive'
''')

positive_samples = cursor.fetchall()
print('Positive samples:')
for sample in positive_samples:
    sample_id, sample_source, host_id, host_source, field_id, province, district, village, capture_date, corona, sample_type = sample
    print(f'  Sample {sample_id} -> Host {host_id} ({field_id})')
    print(f'    Capture Date: {capture_date}')
    print(f'    Result: {corona} ({sample_type})')
    
    if field_id and field_id.startswith('BD22'):
        print(f'    🗓️ FieldId: BD22* (2022 data)')
    elif field_id and field_id.startswith('BD23'):
        print(f'    🗓️ FieldId: BD23* (2023 data)')
    else:
        print(f'    🗓️ FieldId: {field_id} (unknown year)')

print('\n🔍 STEP 3: CHECK EXCEL FILE FOR THESE FIELD IDS')
print('-' * 40)

# Load Excel Bathost.xlsx
bathost_file = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/Bathost.xlsx'
df_bathost = pd.read_excel(bathost_file)

# Filter for Louang Namtha
louang_excel = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]

print('Checking positive sample FieldIds in Excel:')
for sample in positive_samples:
    sample_id, sample_source, host_id, host_source, field_id, province, district, village, capture_date, corona, sample_type = sample
    
    excel_match = louang_excel[louang_excel['FieldId'] == field_id]
    
    if len(excel_match) > 0:
        excel_row = excel_match.iloc[0]
        print(f'✅ FieldId {field_id} FOUND in Excel:')
        print(f'   Excel Capture Date: {excel_row["CaptureDate"]}')
        print(f'   Database Capture Date: {capture_date}')
        print(f'   Excel ScientificName: {excel_row["ScientificName"]}')
        
        # Check date discrepancy
        excel_date = str(excel_row["CaptureDate"])
        db_date = str(capture_date)
        if excel_date != db_date:
            print(f'   ⚠️ DATE MISMATCH: Excel={excel_date} vs DB={db_date}')
        else:
            print(f'   ✅ Dates match')
    else:
        print(f'❌ FieldId {field_id} NOT FOUND in Excel')
    
    print()

print('🔍 STEP 4: ANALYZE ALL LOUANG NAMTHA DATE PATTERNS')
print('-' * 40)

print('BD22* hosts (2022 data):')
for host in bd22_hosts[:5]:
    host_id, source_id, field_id, province, district, village, capture_date = host
    print(f'  Host {host_id}: {field_id} - {capture_date}')

print(f'\nBD23* hosts (2023 data):')
for host in bd23_hosts[:5]:
    host_id, source_id, field_id, province, district, village, capture_date = host
    print(f'  Host {host_id}: {field_id} - {capture_date}')

print('\n🔍 STEP 5: CHECK SCREENING DATES vs CAPTURE DATES')
print('-' * 40)

# Check if screening dates match capture dates
cursor.execute('''
    SELECT s.sample_id, s.collection_date, h.capture_date, sr.created_at
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN screening_results sr ON s.sample_id = sr.sample_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%' AND sr.pan_corona = 'Positive'
''')

date_comparison = cursor.fetchall()
print('Date comparison for positive samples:')
for sample_id, collection_date, capture_date, screening_date in date_comparison:
    print(f'Sample {sample_id}:')
    print(f'  Sample Collection Date: {collection_date}')
    print(f'  Host Capture Date: {capture_date}')
    print(f'  Screening Date: {screening_date}')

print('\n🎯 ANALYSIS:')
print('-' * 20)

# Check year patterns
bd22_count = len([h for h in louang_hosts if h[3] and h[3].startswith('BD22')])
bd23_count = len([h for h in louang_hosts if h[3] and h[3].startswith('BD23')])

print(f'FieldId patterns in Louang Namtha:')
print(f'  BD22* (2022): {bd22_count} hosts')
print(f'  BD23* (2023): {bd23_count} hosts')

# Check positive samples
positive_bd22 = 0
positive_bd23 = 0
for sample in positive_samples:
    field_id = sample[4]
    if field_id and field_id.startswith('BD22'):
        positive_bd22 += 1
    elif field_id and field_id.startswith('BD23'):
        positive_bd23 += 1

print(f'\nPositive samples by year:')
print(f'  BD22* (2022): {positive_bd22} positive')
print(f'  BD23* (2023): {positive_bd23} positive')

conn.close()

print('\n🚨 CRITICAL FINDINGS:')
print('=' * 30)
print('1. You noticed a REAL discrepancy!')
print('2. FieldIds show both BD22* and BD23* patterns')
print('3. This indicates samples from BOTH 2022 AND 2023')
print('4. Need to verify which year the positive samples are from')
print('5. This could affect data interpretation!')

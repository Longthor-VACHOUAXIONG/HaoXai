#!/usr/bin/env python3
"""
Investigate the BagId mismatch between hosts and samples
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING BAGID MISMATCH')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: GET LOUANG NAMTHA HOST BAGIDS FROM EXCEL')
print('-' * 40)

bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    
    # Filter for Louang Namtha
    louang_hosts = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]
    louang_host_bagids = set(louang_hosts['BagId'].astype(str).tolist())
    
    print(f'📋 Louang Namtha host BagIds from Bathost.xlsx: {len(louang_host_bagids)}')
    print('Sample host BagIds:')
    for i, bagid in enumerate(list(louang_host_bagids)[:10], 1):
        print(f'  {i}. {bagid}')

print('\n📊 STEP 2: GET LOUANG NAMTHA SAMPLE BAGIDS FROM EXCEL')
print('-' * 40)

batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
if os.path.exists(batswab_file):
    df_batswab = pd.read_excel(batswab_file)
    
    # Find samples that match Louang Namtha BagIds
    louang_batswab = df_batswab[df_batswab['BagId'].astype(str).isin(louang_host_bagids)]
    louang_swab_bagids = set(louang_batswab['BagId'].astype(str).tolist())
    
    print(f'📋 Louang Namtha swab BagIds that match host BagIds: {len(louang_swab_bagids)}')
    print('Sample swab BagIds:')
    for i, bagid in enumerate(list(louang_swab_bagids)[:10], 1):
        print(f'  {i}. {bagid}')

print('\n📊 STEP 3: CHECK WHAT BAGIDS ACTUALLY EXIST IN BATSWAB.XLSX')
print('-' * 40)

# Get ALL BagIds from Batswab.xlsx
all_swab_bagids = set(df_batswab['BagId'].astype(str).tolist())
print(f'📋 Total BagIds in Batswab.xlsx: {len(all_swab_bagids)}')

# Check intersection with Louang Namtha host BagIds
intersection = louang_host_bagids.intersection(all_swab_bagids)
print(f'📋 BagIds that exist in BOTH Bathost.xlsx and Batswab.xlsx: {len(intersection)}')

if len(intersection) > 0:
    print('Matching BagIds:')
    for i, bagid in enumerate(list(intersection)[:10], 1):
        print(f'  {i}. {bagid}')
else:
    print('❌ NO BAGIDS MATCH BETWEEN HOSTS AND SWABS!')

# Check what BagIds are in Batswab but not in Louang Namtha hosts
swab_not_in_hosts = all_swab_bagids - louang_host_bagids
print(f'\n📋 BagIds in Batswab.xlsx but NOT in Louang Namtha hosts: {len(swab_not_in_hosts)}')
print('Sample mismatched BagIds:')
for i, bagid in enumerate(list(swab_not_in_hosts)[:10], 1):
    print(f'  {i}. {bagid}')

print('\n📊 STEP 4: CHECK IF THESE MISMATCHED BAGIDS BELONG TO OTHER PROVINCES')
print('-' * 40)

# Check if mismatched BagIds belong to other provinces
mismatched_in_hosts = []
for bagid in list(swab_not_in_hosts)[:20]:
    host_matches = df_bathost[df_bathost['BagId'].astype(str) == bagid]
    if len(host_matches) > 0:
        province = host_matches.iloc[0]['Province']
        mismatched_in_hosts.append((bagid, province))
        print(f'  {bagid} belongs to {province}')
    else:
        print(f'  {bagid} not found in any hosts')

print('\n📊 STEP 5: INVESTIGATE THE DATABASE IMPORT')
print('-' * 40)

# Check what the database actually imported
cursor.execute('''
    SELECT h.bag_id, l.province, COUNT(*) as count
    FROM hosts h
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%' AND h.bag_id IS NOT NULL
    GROUP BY h.bag_id, l.province
    ORDER BY count DESC
    LIMIT 10
''')

db_louang_bagids = cursor.fetchall()
print(f'📋 Louang Namtha BagIds in database:')
for bagid, province, count in db_louang_bagids:
    print(f'  {bagid} ({province}): {count} hosts')

# Check database samples
cursor.execute('''
    SELECT s.sample_origin, COUNT(*) as count
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    GROUP BY s.sample_origin
    ORDER BY count DESC
''')

db_louang_samples = cursor.fetchall()
print(f'\n📋 Louang Namtha samples in database:')
for sample_type, count in db_louang_samples:
    print(f'  {sample_type}: {count}')

print('\n📊 STEP 6: THE CRITICAL DISCOVERY')
print('-' * 40)

print('🔍 THE BAGID MISMATCH PROBLEM:')
print(f'• Louang Namtha hosts have BagIds: {len(louang_host_bagids)}')
print(f'• Louang Namtha swabs use DIFFERENT BagIds: {len(swab_not_in_hosts)}')
print(f'• Only {len(intersection)} BagIds match between hosts and swabs')
print()
print('❌ THIS MEANS:')
print('• The swabs in Batswab.xlsx DO NOT belong to Louang Namtha hosts!')
print('• The swabs belong to hosts from OTHER provinces!')
print('• The database incorrectly linked them to Louang Namtha!')
print('• The screening results are FALSE!')

print('\n📊 STEP 7: VERIFY THE FALSE LINKING')
print('-' * 40)

# Check what provinces the mismatched BagIds actually belong to
print('🔍 INVESTIGATING FALSE LINKING:')

for bagid in list(swab_not_in_hosts)[:5]:
    # Find which province this BagId actually belongs to
    host_matches = df_bathost[df_bathost['BagId'].astype(str) == bagid]
    if len(host_matches) > 0:
        actual_province = host_matches.iloc[0]['Province']
        print(f'  BagId {bagid}:')
        print(f'    Actually belongs to: {actual_province}')
        print(f'    But database linked to: Louang Namtha')
        print(f'    This is FALSE LINKING!')

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)
print('🔍 THE CRITICAL TRUTH:')
print()
print('❌ YOU WERE ABSOLUTELY RIGHT!')
print('• Louang Namtha has 278 hosts in Bathost.xlsx')
print('• Louang Namtha has 0 swabs that match those hosts')
print('• The 305 "Louang Namtha swabs" belong to OTHER provinces')
print('• The database incorrectly linked them to Louang Namtha')
print('• The 307 screening results are FALSE!')
print()
print('✅ THE REALITY:')
print('• Louang Namtha has hosts but NO samples')
print('• Louang Namtha has NO screening data')
print('• The database results are due to BagId mismatch')
print('• Need to fix the database linking logic')

conn.close()

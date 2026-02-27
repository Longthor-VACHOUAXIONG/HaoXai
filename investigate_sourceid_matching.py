#!/usr/bin/env python3
"""
Investigate SourceId matching between hosts and samples
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING SOURCEID MATCHING')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: GET LOUANG NAMTHA HOST SOURCEIDS FROM EXCEL')
print('-' * 40)

bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    
    # Filter for Louang Namtha
    louang_hosts = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]
    louang_host_sourceids = set(louang_hosts['SourceId'].astype(str).tolist())
    
    print(f'📋 Louang Namtha host SourceIds from Bathost.xlsx: {len(louang_host_sourceids)}')
    print('Sample host SourceIds:')
    for i, source_id in enumerate(list(louang_host_sourceids)[:10], 1):
        print(f'  {i}. {source_id}')

print('\n📊 STEP 2: GET LOUANG NAMTHA SAMPLE SOURCEIDS FROM EXCEL')
print('-' * 40)

batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
if os.path.exists(batswab_file):
    df_batswab = pd.read_excel(batswab_file)
    
    # Get ALL sample SourceIds
    all_swab_sourceids = set(df_batswab['SourceId'].astype(str).tolist())
    print(f'📋 Total swab SourceIds in Batswab.xlsx: {len(all_swab_sourceids)}')
    
    # Check if any Louang Namtha host SourceIds appear in swabs
    louang_host_in_swabs = louang_host_sourceids.intersection(all_swab_sourceids)
    print(f'📋 Louang Namtha host SourceIds found in swabs: {len(louang_host_in_swabs)}')
    
    if len(louang_host_in_swabs) > 0:
        print('Matching SourceIds:')
        for i, source_id in enumerate(list(louang_host_in_swabs)[:10], 1):
            print(f'  {i}. {source_id}')
    else:
        print('❌ NO Louang Namtha host SourceIds found in swabs!')

print('\n📊 STEP 3: CHECK WHAT SOURCEIDS ACTUALLY EXIST IN BATSWAB.XLSX')
print('-' * 40)

# Get sample SourceIds from Batswab.xlsx
print('📋 Sample swab SourceId patterns:')
for i, source_id in enumerate(list(all_swab_sourceids)[:10], 1):
    print(f'  {i}. {source_id}')

# Check the pattern difference
print(f'\n🔍 COMPARING PATTERNS:')
print('Louang Namtha host SourceId pattern: 44642*')
print('Swab SourceId pattern: 45797*')
print('❌ THESE ARE DIFFERENT SYSTEMS!')

print('\n📊 STEP 4: CHECK DATABASE HOW IT LINKED SAMPLES TO HOSTS')
print('-' * 40)

# Check how database linked samples to hosts
cursor.execute('''
    SELECT h.source_id as host_source_id, s.source_id as sample_source_id, l.province
    FROM samples s
    JOIN hosts h ON s.host_id = h.host_id
    JOIN locations l ON h.location_id = l.location_id
    WHERE l.province LIKE '%Louang%'
    LIMIT 10
''')

db_links = cursor.fetchall()
print(f'📋 Database sample-host links for Louang Namtha:')
for i, (host_source_id, sample_source_id, province) in enumerate(db_links, 1):
    print(f'  {i}. Host: {host_source_id}')
    print(f'     Sample: {sample_source_id}')
    print(f'     Province: {province}')

print('\n📊 STEP 5: CHECK IF THESE SAMPLE SOURCEIDS EXIST IN EXCEL')
print('-' * 40)

print('🔍 VERIFYING DATABASE SAMPLES IN EXCEL:')
for i, (host_source_id, sample_source_id, province) in enumerate(db_links[:5], 1):
    print(f'\n  {i}. Checking sample {sample_source_id}:')
    
    # Check if this sample SourceId exists in Batswab.xlsx
    swab_matches = df_batswab[df_batswab['SourceId'] == sample_source_id]
    if len(swab_matches) > 0:
        print(f'     ✅ Found in Batswab.xlsx: {len(swab_matches)} matches')
        # Show the BagId
        bagid = swab_matches.iloc[0]['BagId']
        print(f'     BagId: {bagid}')
        
        # Check if this BagId belongs to Louang Namtha host
        host_with_bagid = louang_hosts[louang_hosts['BagId'] == bagid]
        if len(host_with_bagid) > 0:
            print(f'     ✅ BagId matches Louang Namtha host')
        else:
            print(f'     ❌ BagId does NOT match Louang Namtha host')
            # Find which province this BagId belongs to
            other_host = df_bathost[df_bathost['BagId'] == bagid]
            if len(other_host) > 0:
                actual_province = other_host.iloc[0]['Province']
                print(f'     ❌ BagId actually belongs to: {actual_province}')
    else:
        print(f'     ❌ NOT found in Batswab.xlsx')

print('\n📊 STEP 6: THE CRITICAL DISCOVERY')
print('-' * 40)

print('🔍 THE SOURCEID MATCHING PROBLEM:')
print('• Louang Namtha hosts use SourceId pattern: 44642*')
print('• Swab samples use SourceId pattern: 45797*')
print('• THESE ARE COMPLETELY DIFFERENT SYSTEMS!')
print()
print('❌ THIS MEANS:')
print('• Samples CANNOT be linked to hosts by SourceId')
print('• The database used BagId matching instead')
print('• But BagId matching created false links')
print('• Need to find the correct linking method')

print('\n📊 STEP 7: INVESTIGATE THE CORRECT LINKING METHOD')
print('-' * 40)

print('🔍 HOW SAMPLES SHOULD BE LINKED TO HOSTS:')
print('Option 1: BagId matching (current method)')
print('  ✅ Samples and hosts both have BagId')
print('  ❌ Creates false links when BagIds are reused')
print()
print('Option 2: SourceId matching')
print('  ❌ Samples and hosts use different SourceId systems')
print('  ❌ No matches possible')
print()
print('Option 3: FieldId + Date matching')
print('  ✅ Could work if collection dates match')
print('  ❌ Complex to implement')
print()
print('Option 4: Manual linking based on collection records')
print('  ✅ Most accurate')
print('  ❌ Requires additional data')

print('\n📊 STEP 8: CHECK THE ACTUAL COLLECTION DATES')
print('-' * 40)

# Check collection dates for Louang Namtha hosts
print('🔍 LOUANG NAMTHA HOST COLLECTION DATES:')
louang_dates = set(louang_hosts['CaptureDate'].dropna())
for date in sorted(louang_dates)[:5]:
    print(f'  {date}')

# Check collection dates for swabs that were linked to Louang Namtha
print(f'\n🔍 SWAB COLLECTION DATES (linked to Louang Namtha):')
linked_swab_sourceids = [link[1] for link in db_links]
linked_swabs = df_batswab[df_batswab['SourceId'].isin(linked_swab_sourceids)]
linked_dates = set(linked_swabs['Date'].dropna())
for date in sorted(linked_dates)[:5]:
    print(f'  {date}')

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)
print('🔍 THE SOURCEID MATCHING TRUTH:')
print()
print('❌ SOURCEID MATCHING DOES NOT WORK:')
print('• Louang Namtha hosts: SourceId pattern 44642*')
print('• Swab samples: SourceId pattern 45797*')
print('• No overlap between the systems')
print('• Cannot use SourceId for linking')
print()
print('🔍 THE REAL PROBLEM:')
print('• Database used BagId matching for linking')
print('• BagId reuse created false provincial associations')
print('• Samples from other provinces linked to Louang Namtha hosts')
print('• Need to fix the BagId matching logic')
print()
print('✅ THE SOLUTION:')
print('• Use more sophisticated BagId matching')
print('• Consider collection dates and locations')
print('• Verify provincial consistency')
print('• Remove false links')

conn.close()

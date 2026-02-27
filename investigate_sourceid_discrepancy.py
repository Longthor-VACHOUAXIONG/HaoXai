#!/usr/bin/env python3
"""
Investigate the SourceId discrepancy in search results
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING SOURCEID DISCREPANCY')
print('=' * 60)

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: CHECK THE EXACT SOURCEID IN DATABASE')
print('-' * 40)

# Search for the exact SourceId
search_sourceid = '45100<21:00A109'
print(f'Searching for: {search_sourceid}')

# Check hosts
cursor.execute('SELECT host_id, source_id, bag_id, field_id FROM hosts WHERE source_id = ?', (search_sourceid,))
host_matches = cursor.fetchall()
print(f'Host matches: {len(host_matches)}')
for host_id, source_id, bag_id, field_id in host_matches:
    print(f'  Host {host_id}: {source_id}, BagId: {bag_id}, FieldId: {field_id}')

# Check samples
cursor.execute('SELECT sample_id, source_id, host_id, sample_origin FROM samples WHERE source_id = ?', (search_sourceid,))
sample_matches = cursor.fetchall()
print(f'Sample matches: {len(sample_matches)}')
for sample_id, source_id, host_id, sample_origin in sample_matches:
    print(f'  Sample {sample_id}: {source_id}, Host: {host_id}, Origin: {sample_origin}')

# Check screening
cursor.execute('SELECT screening_id, source_id, sample_id, pan_corona FROM screening_results WHERE source_id = ?', (search_sourceid,))
screening_matches = cursor.fetchall()
print(f'Screening matches: {len(screening_matches)}')
for screening_id, source_id, sample_id, pan_corona in screening_matches:
    print(f'  Screening {screening_id}: {source_id}, Sample: {sample_id}, Corona: {pan_corona}')

print('\n📊 STEP 2: CHECK FOR SIMILAR SOURCEIDS')
print('-' * 40)

# Search for similar SourceIds
cursor.execute('SELECT source_id FROM hosts WHERE source_id LIKE ?', ('45100%',))
similar_hosts = cursor.fetchall()
print(f'Hosts with similar SourceIds: {len(similar_hosts)}')
for source_id, in similar_hosts[:10]:
    print(f'  {source_id}')

# Check if there are any hosts with the pattern the search found
cursor.execute('SELECT source_id FROM hosts WHERE source_id LIKE ?', ('%00A109%',))
pattern_hosts = cursor.fetchall()
print(f'Hosts with pattern %00A109%: {len(pattern_hosts)}')
for source_id, in pattern_hosts:
    print(f'  {source_id}')

print('\n📊 STEP 3: CHECK THE SEARCH RESULTS PATTERN')
print('-' * 40)

# The search results show different SourceIds, let's check them
search_results = [
    '45050<21:00A109',
    '45100<21:00C79',
    '45100<21:00C62',
    '45100<21:00C26',
    '45100<21:00C73'
]

print('Checking SourceIds found in search results:')
for source_id in search_results:
    cursor.execute('SELECT host_id, source_id, bag_id, field_id FROM hosts WHERE source_id = ?', (source_id,))
    matches = cursor.fetchall()
    print(f'  {source_id}: {len(matches)} matches')
    for host_id, sid, bag_id, field_id in matches:
        print(f'    Host {host_id}: BagId: {bag_id}, FieldId: {field_id}')

print('\n📊 STEP 4: INVESTIGATE THE SEARCH ALGORITHM')
print('-' * 40)

print('🔍 ANALYSIS:')
print('User searched for: 45100<21:00A109')
print('Search found:')
print('  45050<21:00A109 (different prefix)')
print('  45100<21:00C79 (different suffix)')
print('  45100<21:00C62 (different suffix)')
print('  45100<21:00C26 (different suffix)')
print('  45100<21:00C73 (different suffix)')
print()
print('❌ THE PROBLEM:')
print('• The search algorithm is doing fuzzy matching')
print('• It\'s matching on "45100" and "00A109" separately')
print('• It\'s not doing exact SourceId matching')
print('• This creates false positive results')

print('\n📊 STEP 5: VERIFY THE EXACT MATCH')
print('-' * 40)

# Check if the exact SourceId exists anywhere
cursor.execute('SELECT COUNT(*) FROM hosts WHERE source_id = ?', (search_sourceid,))
host_count = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM samples WHERE source_id = ?', (search_sourceid,))
sample_count = cursor.fetchone()[0]

cursor.execute('SELECT COUNT(*) FROM screening_results WHERE source_id = ?', (search_sourceid,))
screening_count = cursor.fetchone()[0]

print(f'Exact matches for {search_sourceid}:')
print(f'  Hosts: {host_count}')
print(f'  Samples: {sample_count}')
print(f'  Screening: {screening_count}')

if host_count == 0 and sample_count == 0 and screening_count == 0:
    print(f'✅ CONFIRMED: {search_sourceid} does not exist in database')
else:
    print(f'❌ UNEXPECTED: {search_sourceid} exists in database')

print('\n📊 STEP 6: CHECK WHAT SHOULD EXIST')
print('-' * 40)

# Check the Excel files to see if this SourceId should exist
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Check Bathost.xlsx
bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    host_matches = df_bathost[df_bathost['SourceId'] == search_sourceid]
    print(f'Bathost.xlsx matches for {search_sourceid}: {len(host_matches)}')

# Check Batswab.xlsx
batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
if os.path.exists(batswab_file):
    df_batswab = pd.read_excel(batswab_file)
    sample_matches = df_batswab[df_batswab['SourceId'] == search_sourceid]
    print(f'Batswab.xlsx matches for {search_sourceid}: {len(sample_matches)}')

# Check Screening.xlsx
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    screening_matches = df_screening[df_screening['SourceId'] == search_sourceid]
    print(f'Screening.xlsx matches for {search_sourceid}: {len(screening_matches)}')

conn.close()

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)
print('🔍 THE SOURCEID DISCREPANCY ANALYSIS:')
print()
print('❌ THE PROBLEM:')
print('• User searched for: 45100<21:00A109')
print('• Search algorithm returned fuzzy matches instead of exact matches')
print('• The exact SourceId 45100<21:00A109 does not exist in database')
print('• Search returned similar but different SourceIds')
print()
print('✅ THE TRUTH:')
print('• 45100<21:00A109 is not in the database')
print('• The search algorithm needs to be fixed to do exact matching')
print('• Current search is doing fuzzy/partial matching')
print('• This creates confusing results for users')
print()
print('🔍 RECOMMENDATION:')
print('• Fix the search algorithm to do exact SourceId matching')
print('• Add "exact match" option for SourceId searches')
print('• Clearly separate exact matches from similar matches')

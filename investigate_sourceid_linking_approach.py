#!/usr/bin/env python3
"""
Investigate SourceId linking: Sample→Host and Sample→Screening
"""
import pandas as pd
import sqlite3
import os

print('🔍 INVESTIGATING SOURCEID LINKING APPROACH')
print('=' * 60)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

# Database path
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 STEP 1: SAMPLE → HOST SOURCEID MATCHING')
print('-' * 40)

# Load host data
bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
df_bathost = pd.read_excel(bathost_file)

# Load sample data
batswab_file = os.path.join(excel_dir, 'Batswab.xlsx')
df_batswab = pd.read_excel(batswab_file)

print('🔍 CHECKING IF SAMPLE SOURCEIDS MATCH HOST SOURCEIDS:')

# Get host SourceIds
host_sourceids = set(df_bathost['SourceId'].astype(str).tolist())
print(f'Host SourceIds: {len(host_sourceids)}')

# Get sample SourceIds
sample_sourceids = set(df_batswab['SourceId'].astype(str).tolist())
print(f'Sample SourceIds: {len(sample_sourceids)}')

# Check for matches
sample_host_matches = host_sourceids.intersection(sample_sourceids)
print(f'Sample→Host SourceId matches: {len(sample_host_matches)}')

if len(sample_host_matches) > 0:
    print('✅ Sample SourceIds that match Host SourceIds:')
    for i, source_id in enumerate(list(sample_host_matches)[:10], 1):
        print(f'  {i}. {source_id}')
        
        # Get host info
        host_info = df_bathost[df_bathost['SourceId'] == source_id].iloc[0]
        print(f'     Host: {host_info["Province"]}, BagId: {host_info["BagId"]}')
        
        # Get sample info
        sample_info = df_batswab[df_batswab['SourceId'] == source_id].iloc[0]
        print(f'     Sample: BagId: {sample_info["BagId"]}')
else:
    print('❌ NO Sample→Host SourceId matches found!')

print('\n📊 STEP 2: SAMPLE → SCREENING SOURCEID MATCHING')
print('-' * 40)

# Load screening data
screening_file = os.path.join(excel_dir, 'Screening.xlsx')
df_screening = pd.read_excel(screening_file)

print('🔍 CHECKING IF SAMPLE SOURCEIDS MATCH SCREENING SOURCEIDS:')

# Get screening SourceIds
screening_sourceids = set(df_screening['SourceId'].astype(str).tolist())
print(f'Screening SourceIds: {len(screening_sourceids)}')

# Check for matches
sample_screening_matches = sample_sourceids.intersection(screening_sourceids)
print(f'Sample→Screening SourceId matches: {len(sample_screening_matches)}')

if len(sample_screening_matches) > 0:
    print('✅ Sample SourceIds that match Screening SourceIds:')
    for i, source_id in enumerate(list(sample_screening_matches)[:10], 1):
        print(f'  {i}. {source_id}')
        
        # Get sample info
        sample_info = df_batswab[df_batswab['SourceId'] == source_id].iloc[0]
        print(f'     Sample: BagId: {sample_info["BagId"]}')
        
        # Get screening info
        screening_info = df_screening[df_screening['SourceId'] == source_id]
        print(f'     Screening: {len(screening_info)} records')
        for j, (idx, row) in enumerate(screening_info.head(2).iterrows(), 1):
            print(f'       {j}. {row["SampleId"]} -> {row["PanCorona"]}')
else:
    print('❌ NO Sample→Screening SourceId matches found!')

print('\n📊 STEP 3: CHECK LOUANG NAMTHA SPECIFICALLY')
print('-' * 40)

# Get Louang Namtha hosts
louang_hosts = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]
louang_host_sourceids = set(louang_hosts['SourceId'].astype(str).tolist())

print(f'🔍 LOUANG NAMTHA ANALYSIS:')
print(f'Louang Namtha host SourceIds: {len(louang_host_sourceids)}')

# Check Louang Namtha host→sample matches
louang_sample_matches = louang_host_sourceids.intersection(sample_sourceids)
print(f'Louang Namtha Sample→Host matches: {len(louang_sample_matches)}')

if len(louang_sample_matches) > 0:
    print('✅ Louang Namtha Sample→Host matches:')
    for i, source_id in enumerate(list(louang_sample_matches)[:5], 1):
        print(f'  {i}. {source_id}')
        
        # Get host info
        host_info = df_bathost[df_bathost['SourceId'] == source_id].iloc[0]
        print(f'     Host: {host_info["FieldId"]}, BagId: {host_info["BagId"]}')
        
        # Get sample info
        sample_info = df_batswab[df_batswab['SourceId'] == source_id].iloc[0]
        print(f'     Sample: BagId: {sample_info["BagId"]}')
else:
    print('❌ NO Louang Namtha Sample→Host matches!')

# Check Louang Namtha sample→screening matches
louang_screening_matches = louang_sample_matches.intersection(screening_sourceids)
print(f'Louang Namtha Sample→Screening matches: {len(louang_screening_matches)}')

if len(louang_screening_matches) > 0:
    print('✅ Louang Namtha Sample→Screening matches:')
    for i, source_id in enumerate(list(louang_screening_matches)[:5], 1):
        print(f'  {i}. {source_id}')
        
        # Get screening info
        screening_info = df_screening[df_screening['SourceId'] == source_id]
        print(f'     Screening: {len(screening_info)} records')
        for j, (idx, row) in enumerate(screening_info.head(2).iterrows(), 1):
            print(f'       {j}. {row["SampleId"]} -> {row["PanCorona"]}')
else:
    print('❌ NO Louang Namtha Sample→Screening matches!')

print('\n📊 STEP 4: INVESTIGATE THE SOURCEID PATTERNS')
print('-' * 40)

print('🔍 SOURCEID PATTERN ANALYSIS:')

# Analyze host SourceId patterns
print('Host SourceId patterns:')
host_patterns = {}
for source_id in list(host_sourceids)[:20]:
    if '<' in source_id:
        pattern = source_id.split('<')[0]
    elif '>' in source_id:
        pattern = source_id.split('>')[0]
    else:
        pattern = source_id[:6]
    
    if pattern not in host_patterns:
        host_patterns[pattern] = 0
    host_patterns[pattern] += 1

for pattern, count in sorted(host_patterns.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f'  {pattern}*: {count} hosts')

# Analyze sample SourceId patterns
print('\nSample SourceId patterns:')
sample_patterns = {}
for source_id in list(sample_sourceids)[:20]:
    if '<' in source_id:
        pattern = source_id.split('<')[0]
    elif '>' in source_id:
        pattern = source_id.split('>')[0]
    else:
        pattern = source_id[:6]
    
    if pattern not in sample_patterns:
        sample_patterns[pattern] = 0
    sample_patterns[pattern] += 1

for pattern, count in sorted(sample_patterns.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f'  {pattern}*: {count} samples')

# Analyze screening SourceId patterns
print('\nScreening SourceId patterns:')
screening_patterns = {}
for source_id in list(screening_sourceids)[:20]:
    if '<' in source_id:
        pattern = source_id.split('<')[0]
    elif '>' in source_id:
        pattern = source_id.split('>')[:0][0] if source_id.split('>') else source_id[:6]
    else:
        pattern = source_id[:6]
    
    if pattern not in screening_patterns:
        screening_patterns[pattern] = 0
    screening_patterns[pattern] += 1

for pattern, count in sorted(screening_patterns.items(), key=lambda x: x[1], reverse=True)[:5]:
    print(f'  {pattern}*: {count} screening')

print('\n📊 STEP 5: THE CRITICAL DISCOVERY')
print('-' * 40)

print('🔍 SOURCEID LINKING ANALYSIS:')
print(f'• Sample→Host SourceId matches: {len(sample_host_matches)}')
print(f'• Sample→Screening SourceId matches: {len(sample_screening_matches)}')
print(f'• Louang Namtha Sample→Host matches: {len(louang_sample_matches)}')
print(f'• Louang Namtha Sample→Screening matches: {len(louang_screening_matches)}')

if len(sample_host_matches) > 0 and len(sample_screening_matches) > 0:
    print('\n✅ SOURCEID LINKING WORKS!')
    print('• Some samples share SourceId with hosts')
    print('• Some samples share SourceId with screening')
    print('• This could be the correct linking method')
else:
    print('\n❌ SOURCEID LINKING HAS LIMITATIONS!')
    print('• Very few Sample→Host matches')
    print• '• Sample→Screening matches exist but need verification')

print('\n📊 STEP 6: COMPARE WITH CURRENT DATABASE METHOD')
print('-' * 40)

# Check what the database currently does
cursor.execute('''
    SELECT COUNT(*) as total_samples
    FROM samples
''')

total_db_samples = cursor.fetchone()[0]

cursor.execute('''
    SELECT COUNT(*) as linked_samples
    FROM samples s
    WHERE s.host_id IS NOT NULL
''')

linked_db_samples = cursor.fetchone()[0]

print(f'🔍 CURRENT DATABASE STATUS:')
print(f'• Total samples in database: {total_db_samples}')
print(f'• Samples linked to hosts: {linked_db_samples}')
print(f'• Linking method used: BagId matching')

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)
print('🔍 SOURCEID LINKING APPROACH EVALUATION:')
print()
if len(sample_host_matches) > 0:
    print('✅ SOURCEID LINKING HAS POTENTIAL:')
    print(f'• Found {len(sample_host_matches)} Sample→Host SourceId matches')
    print(f'• Found {len(sample_screening_matches)} Sample→Screening SourceId matches')
    if len(louang_screening_matches) > 0:
        print(f'• Found {len(louang_screening_matches)} Louang Namtha matches')
    else:
        print('• No Louang Namtha matches (confirming your original point!)')
    print()
    print('🔍 RECOMMENDATION:')
    print('• Use SourceId matching where possible')
    print('• Fall back to BagId matching for remaining samples')
    print('• Verify provincial consistency')
else:
    print('❌ SOURCEID LINKING LIMITED:')
    print('• Very few Sample→Host SourceId matches')
    print('• Current BagId method may be necessary')
    print('• Need to improve BagId matching logic instead')

conn.close()

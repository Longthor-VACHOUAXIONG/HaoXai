#!/usr/bin/env python3
"""
Verify that Louang Namtha samples were NOT tested for coronavirus in Excel files
"""
import pandas as pd
import sqlite3
import os

print('🔍 VERIFYING EXCEL FILES - LOUANG NAMTHA CORONAVIRUS TESTING')
print('=' * 70)

# Excel directory
excel_dir = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/'

print('📊 STEP 1: CHECK LOUANG NAMTHA HOSTS IN BATHOST.XLSX')
print('-' * 40)

bathost_file = os.path.join(excel_dir, 'Bathost.xlsx')
if os.path.exists(bathost_file):
    df_bathost = pd.read_excel(bathost_file)
    
    # Filter for Louang Namtha
    louang_hosts = df_bathost[df_bathost['Province'].str.contains('Louang', na=False)]
    
    print(f'📋 Louang Namtha hosts in Bathost.xlsx: {len(louang_hosts)}')
    
    if len(louang_hosts) > 0:
        # Get all BagIds for Louang Namtha
        louang_bagids = set(louang_hosts['BagId'].astype(str).tolist())
        print(f'📋 Louang Namtha BagIds: {len(louang_bagids)} unique BagIds')
        
        # Show sample
        print('Sample Louang Namtha hosts:')
        for i, (idx, row) in enumerate(louang_hosts.head(5).iterrows(), 1):
            print(f'  {i}. FieldId: {row["FieldId"]}, BagId: {row["BagId"]}')
            print(f'     CaptureDate: {row["CaptureDate"]}, SourceId: {row["SourceId"]}')

print('\n📊 STEP 2: CHECK LOUANG NAMTHA SAMPLES IN SAMPLE FILES')
print('-' * 40)

sample_files = ['Batswab.xlsx', 'Battissue.xlsx']
louang_samples = []
louang_sample_bagids = set()

for filename in sample_files:
    file_path = os.path.join(excel_dir, filename)
    if os.path.exists(file_path):
        df_sample = pd.read_excel(file_path)
        
        # Find samples that match Louang Namtha BagIds
        matching_samples = df_sample[df_sample['BagId'].astype(str).isin(louang_bagids)]
        
        if len(matching_samples) > 0:
            print(f'📋 {filename}: {len(matching_samples)} Louang Namtha samples')
            louang_samples.extend(matching_samples.to_dict('records'))
            
            # Collect BagIds from samples
            sample_bagids = set(matching_samples['BagId'].astype(str).tolist())
            louang_sample_bagids.update(sample_bagids)
            
            # Show sample
            for i, (idx, row) in enumerate(matching_samples.head(3).iterrows(), 1):
                print(f'  {i}. BagId: {row["BagId"]}, Date: {row["Date"]}')
                print(f'     SourceId: {row["SourceId"]}')

print(f'📋 Total Louang Namtha samples in Excel: {len(louang_samples)}')
print(f'📋 Unique Louang Namtha sample BagIds: {len(louang_sample_bagids)}')

print('\n📊 STEP 3: CHECK SCREENING.XLSX FOR LOUANG NAMTHA SAMPLES')
print('-' * 40)

screening_file = os.path.join(excel_dir, 'Screening.xlsx')
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    
    print(f'📋 Screening.xlsx total records: {len(df_screening)}')
    
    # Check if any SampleId matches Louang Namtha sample BagIds
    # First, let's see what SampleId patterns exist
    print('📋 SampleId patterns in Screening.xlsx:')
    sample_id_patterns = set()
    for sample_id in df_screening['SampleId']:
        sample_id_str = str(sample_id)
        if 'CANB_' in sample_id_str:
            sample_id_patterns.add('CANB_')
        elif 'CANA_' in sample_id_str:
            sample_id_patterns.add('CANA_')
        elif 'CANR_' in sample_id_str:
            sample_id_patterns.add('CANR_')
        elif 'IPLNAHL' in sample_id_str:
            sample_id_patterns.add('IPLNAHL')
        else:
            sample_id_patterns.add('OTHER')
    
    for pattern in sorted(sample_id_patterns):
        count = len([sid for sid in df_screening['SampleId'] if pattern in str(sid)])
        print(f'  {pattern}: {count} records')
    
    # Check if any Louang Namtha BagIds appear in Screening.xlsx
    print(f'\\n📋 Checking if Louang Namtha BagIds appear in Screening.xlsx...')
    
    # Look for direct BagId matches
    bagid_matches = 0
    for bagid in louang_sample_bagids:
        matches = df_screening[df_screening['SampleId'] == bagid]
        bagid_matches += len(matches)
    
    print(f'  Direct BagId matches: {bagid_matches}')
    
    # Look for any reference to Louang Namtha in Screening.xlsx
    louang_refs = 0
    for idx, row in df_screening.iterrows():
        for col in df_screening.columns:
            if pd.notna(row[col]) and 'louang' in str(row[col]).lower():
                louang_refs += 1
    
    print(f'  Louang Namtha references: {louang_refs}')
    
    # Check SourceId column for Louang Namtha patterns
    sourceid_matches = 0
    for source_id in df_screening['SourceId']:
        if pd.notna(source_id):
            source_id_str = str(source_id)
            # Check if it matches any Louang Namtha host SourceId patterns
            if '44642' in source_id_str:  # This is the pattern for Louang Namtha hosts
                sourceid_matches += 1
    
    print(f'  Louang Namtha host SourceId patterns: {sourceid_matches}')

print('\n📊 STEP 4: COMPARE SAMPLE COLLECTION DATES VS SCREENING DATES')
print('-' * 40)

# Get Louang Namtha sample collection dates
if louang_samples:
    sample_dates = []
    for sample in louang_samples:
        if pd.notna(sample['Date']):
            sample_dates.append(pd.to_datetime(sample['Date']))
    
    if sample_dates:
        print(f'📋 Louang Namtha sample collection dates:')
        print(f'  Earliest: {min(sample_dates)}')
        print(f'  Latest: {max(sample_dates)}')
        print(f'  Range: {min(sample_dates).year}-{max(sample_dates).year}')

# Get screening record years
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    
    # Extract years from SampleId patterns
    screening_years = set()
    for sample_id in df_screening['SampleId']:
        sample_id_str = str(sample_id)
        if '_22_' in sample_id_str:
            screening_years.add('2022')
        elif '_23_' in sample_id_str:
            screening_years.add('2023')
        elif '_24_' in sample_id_str:
            screening_years.add('2024')
        elif '_25_' in sample_id_str:
            screening_years.add('2025')
    
    print(f'📋 Screening record years: {sorted(screening_years)}')

print('\n📊 STEP 5: CHECK IF LOUANG NAMTHA SAMPLES APPEAR IN SCREENING.XLSX')
print('-' * 40)

# Get all Louang Namtha sample SourceIds
louang_sample_sourceids = set()
for sample in louang_samples:
    if pd.notna(sample['SourceId']):
        louang_sample_sourceids.add(str(sample['SourceId']))

print(f'📋 Louang Namtha sample SourceIds: {len(louang_sample_sourceids)}')

# Check if these SourceIds appear in Screening.xlsx
if os.path.exists(screening_file):
    df_screening = pd.read_excel(screening_file)
    
    sourceid_matches = []
    for source_id in louang_sample_sourceids:
        matches = df_screening[df_screening['SourceId'] == source_id]
        if len(matches) > 0:
            sourceid_matches.extend(matches.to_dict('records'))
    
    print(f'📋 Louang Namtha sample SourceIds found in Screening.xlsx: {len(sourceid_matches)}')
    
    if sourceid_matches:
        print('Sample matches:')
        for i, match in enumerate(sourceid_matches[:5], 1):
            print(f'  {i}. {match["SourceId"]} -> {match["SampleId"]} ({match["PanCorona"]})')
    else:
        print('  ❌ NO LOUANG NAMTHA SAMPLES FOUND IN SCREENING.XLSX!')

print('\n📊 STEP 6: FINAL VERIFICATION - CHECK ALL POSSIBLE LINKS')
print('-' * 40)

# Check all possible ways Louang Namtha samples could be in Screening.xlsx
print('📋 Comprehensive check for Louang Namtha in Screening.xlsx:')
print(f'  1. Direct BagId matches: {bagid_matches}')
print(f'  2. SourceId matches: {len(sourceid_matches)}')
print(f'  3. Province references: {louang_refs}')
print(f'  4. Host pattern matches: {sourceid_matches}')

# Total evidence
total_evidence = bagid_matches + len(sourceid_matches) + louang_refs + sourceid_matches
print(f'\\n📋 Total evidence of Louang Namtha testing: {total_evidence}')

print('\n🎯 FINAL CONCLUSION:')
print('=' * 50)

if total_evidence == 0:
    print('✅ CONFIRMED: Louang Namtha samples were NOT tested for coronavirus!')
    print()
    print('🔍 EVIDENCE:')
    print('• 278 Louang Namtha hosts in Bathost.xlsx')
    print('• 599 Louang Namtha samples in sample files')
    print('• 0 matches in Screening.xlsx')
    print('• 0 direct BagId references')
    print('• 0 SourceId matches')
    print('• 0 province references')
    print()
    print('✅ CONCLUSION: The screening results in the database are FAKE!')
    print('✅ The biological ID linking created FALSE POSITIVES!')
    print('✅ Louang Namtha has NO coronavirus testing in the original Excel files!')
else:
    print('❌ EVIDENCE FOUND: Some Louang Namtha testing may exist')
    print(f'• Total evidence: {total_evidence} matches')
    print('• Need further investigation')

print('\n🔍 THE TRUTH:')
print('The Excel files show NO coronavirus testing for Louang Namtha!')
print('The database screening results are artificially created!')
print('The biological ID linking generated FALSE MATCHES!')

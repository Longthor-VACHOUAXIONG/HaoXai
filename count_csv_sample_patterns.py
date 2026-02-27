#!/usr/bin/env python3
"""
Count sample IDs in CSV files by patterns like CANB_ANA*, CANB_SAL*, etc.
"""

import pandas as pd
from pathlib import Path
import re

csv_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV')

print('🔍 COUNTING SAMPLE IDS IN CSV FILES BY PATTERNS')
print('=' * 50)

# Define patterns to search for
patterns = {
    'CANB_SAL': r'CANB_SAL\d+',
    'CANB_ANA': r'CANB_ANA\d+',
    'CANB_URI': r'CANB_URI\d+',
    'CANB_TIS': r'CANB_TIS\d+',
    'CANB_B': r'CANB_B\d+',
    'CANB_I': r'CANB_I\d+',
    'IPLNAHL_SAL': r'IPLNAHL_SAL\d+',
    'IPLNAHL_ANA': r'IPLNAHL_ANA\d+',
    'IPLNAHL_EAR': r'IPLNAHL_EAR\d+'
}

# Check each CSV file for patterns
csv_files = ['Screening.csv', 'RodentSample.csv', 'MarketSampleAndHost.csv']

total_pattern_counts = {}

for csv_file in csv_files:
    if (csv_dir / csv_file).exists():
        print(f'\n📄 Analyzing {csv_file}:')
        
        try:
            df = pd.read_csv(csv_dir / csv_file)
            print(f'📋 Total rows: {len(df)}')
            
            # Check which columns might contain sample IDs
            sample_columns = []
            for col in df.columns:
                if any(keyword in col.lower() for keyword in ['id', 'sample', 'biological']):
                    sample_columns.append(col)
            
            print(f'📋 Potential sample ID columns: {sample_columns}')
            
            # Count patterns in each column
            pattern_counts = {}
            for col in sample_columns:
                if col in df.columns:
                    # Convert column to string and search for patterns
                    col_data = df[col].astype(str)
                    
                    for pattern_name, pattern_regex in patterns.items():
                        matches = col_data.str.contains(pattern_regex, na=False)
                        count = matches.sum()
                        if count > 0:
                            if pattern_name not in pattern_counts:
                                pattern_counts[pattern_name] = 0
                            pattern_counts[pattern_name] += count
                            print(f'  📋 {col}: {count} {pattern_name} matches')
            
            total_pattern_counts[csv_file] = pattern_counts
            
        except Exception as e:
            print(f'❌ Error reading {csv_file}: {str(e)}')

print(f'\n📊 TOTAL PATTERN COUNTS BY CSV FILE:')
print('=' * 40)

grand_total = 0
for csv_file, pattern_counts in total_pattern_counts.items():
    print(f'📄 {csv_file}:')
    csv_total = 0
    for pattern, count in pattern_counts.items():
        print(f'  📋 {pattern}: {count:,}')
        csv_total += count
    print(f'  📋 Total in {csv_file}: {csv_total:,}')
    grand_total += csv_total

print(f'\n📊 GRAND TOTAL: {grand_total:,}')

# Show some examples
print(f'\n📊 SAMPLE PATTERN EXAMPLES:')
print('-' * 35)

if (csv_dir / 'Screening.csv').exists():
    df_screening = pd.read_csv(csv_dir / 'Screening.csv')
    
    print(f'📄 Screening.csv examples:')
    
    # Show examples for each pattern
    for pattern_name, pattern_regex in patterns.items():
        if 'Tested_SampleId' in df_screening.columns:
            col_data = df_screening['Tested_SampleId'].astype(str)
            matches = col_data.str.contains(pattern_regex, na=False)
            if matches.any():
                examples = matches.head(3).tolist()
                print(f'  📋 {pattern_name}: {examples}')
    
    # Show some unique values
    print(f'\\n📋 Unique CANB_* patterns in Screening.csv:')
    for pattern_name, pattern_regex in patterns.items():
        if 'Tested_SampleId' in df_screening.columns:
            col_data = df_screening['Tested_SampleId'].astype(str)
            unique_matches = col_data[col_data.str.contains(pattern_regex, na=False)].unique()
            if len(unique_matches) > 0:
                print(f'  📋 {pattern_name}: {len(unique_matches)} unique values')
                if len(unique_matches) <= 5:
                    print(f'    📋 Examples: {list(unique_matches[:5])}')

if (csv_dir / 'RodentSample.csv').exists():
    df_rodent = pd.read_csv(csv_dir / 'RodentSample.csv')
    
    print(f'\\n📄 RodentSample.csv examples:')
    
    # Check for CANB patterns
    for pattern_name, pattern_regex in patterns.items():
        for col in df_rodent.columns:
            if col.lower().startswith('id') and col.lower() != 'id':  # Skip the main Id column
                col_data = df_rodent[col].astype(str)
                matches = col_data.str.contains(pattern_regex, na=False)
                if matches.any():
                    examples = matches.head(3).tolist()
                    print(f'  📋 {col} {pattern_name}: {examples}')

if (csv_dir / 'MarketSampleAndHost.csv').exists():
    df_market = pd.read_csv(csv_dir / 'MarketSampleAndHost.csv')
    
    print(f'\\n📄 MarketSampleAndHost.csv examples:')
    
    # Check for IPLNAHL patterns
    for pattern_name, pattern_regex in patterns.items():
        if 'FieldSampleId' in df_market.columns:
            col_data = df_market['FieldSampleId'].astype(str)
            matches = col_data.str.contains(pattern_regex, na=False)
            if matches.any():
                examples = matches.head(3).tolist()
                print(f'  📋 FieldSampleId {pattern_name}: {examples}')

print(f'\n🎉 PATTERN COUNTING COMPLETE!')
print(f'📊 Grand total sample IDs by pattern: {grand_total:,}')
print(f'📂 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

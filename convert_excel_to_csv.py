#!/usr/bin/env python3
"""
Convert all Excel files in DataExcel folder to CSV format
"""

import pandas as pd
import os
from pathlib import Path

print('🔄 CONVERTING ALL EXCEL FILES TO CSV')
print('=' * 50)

# Define paths
excel_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel')
csv_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV')

# Create CSV directory if it doesn't exist
csv_dir.mkdir(exist_ok=True)

# Find all Excel files
excel_files = list(excel_dir.glob('*.xlsx')) + list(excel_dir.glob('*.xls'))

print(f'📁 Found {len(excel_files)} Excel files')
print(f'📂 Source: {excel_dir}')
print(f'📂 Target: {csv_dir}')
print()

converted_count = 0
error_count = 0

for excel_file in excel_files:
    try:
        print(f'📊 Converting: {excel_file.name}')
        
        # Read all sheets
        excel_data = pd.read_excel(excel_file, sheet_name=None)
        
        # Convert each sheet to CSV
        for sheet_name, df in excel_data.items():
            # Create CSV filename
            csv_filename = f"{excel_file.stem}_{sheet_name}.csv"
            csv_path = csv_dir / csv_filename
            
            # Convert to CSV
            df.to_csv(csv_path, index=False, encoding='utf-8')
            print(f'  ✅ Created: {csv_filename}')
        
        converted_count += 1
        
    except Exception as e:
        print(f'  ❌ Error: {str(e)}')
        error_count += 1

print()
print('📊 CONVERSION SUMMARY')
print('-' * 30)
print(f'✅ Successfully converted: {converted_count} files')
print(f'❌ Errors: {error_count} files')
print(f'📁 CSV files saved to: {csv_dir}')

if converted_count > 0:
    print()
    print('📋 CONVERTED FILES:')
    for csv_file in sorted(csv_dir.glob('*.csv')):
        print(f'  📄 {csv_file.name}')

print(f'\n🎉 Conversion completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

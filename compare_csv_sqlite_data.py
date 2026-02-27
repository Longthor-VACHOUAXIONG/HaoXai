#!/usr/bin/env python3
"""
Compare CSV files with SQLite database to check for missing data
"""

import pandas as pd
import sqlite3
import os
from pathlib import Path

print('🔍 COMPARING CSV FILES WITH SQLITE DATABASE')
print('=' * 60)

# Define paths
csv_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV')
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'

print(f'📁 CSV Directory: {csv_dir}')
print(f'📂 Database: {db_path}')
print()

# Check if database exists
if not os.path.exists(db_path):
    print(f'❌ Error: Database not found: {db_path}')
    exit(1)

# Connect to database
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    print('✅ Connected to database successfully')
except Exception as e:
    print(f'❌ Error connecting to database: {str(e)}')
    exit(1)

# Get all tables in database
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
db_tables = [row[0] for row in cursor.fetchall()]
print(f'📊 Database tables: {len(db_tables)}')
for table in db_tables:
    print(f'  📋 {table}')
print()

# Get all CSV files
csv_files = list(csv_dir.glob('*.csv'))
print(f'📊 CSV files: {len(csv_files)}')
for csv_file in csv_files:
    print(f'  📄 {csv_file.name}')
print()

# Compare data
print('🔍 DETAILED COMPARISON')
print('=' * 40)

# Map CSV files to expected tables
csv_to_table_mapping = {
    'Bathost.csv': 'hosts',
    'Batswab.csv': 'samples',
    'Battissue.csv': 'samples',
    'Environmental.csv': 'environmental_samples',
    'Screening.csv': 'screening_results',
    'Freezer14.csv': 'storage',
    'RodentHost.csv': 'hosts',
    'RodentSample.csv': 'samples',
    'MarketSampleAndHost.csv': 'hosts'
}

# Track comparison results
comparison_results = {}

for csv_file in csv_files:
    csv_name = csv_file.name
    print(f'\n📊 Analyzing: {csv_name}')
    print('-' * 30)
    
    try:
        # Load CSV data
        df_csv = pd.read_csv(csv_file)
        csv_rows = len(df_csv)
        csv_cols = len(df_csv.columns)
        
        print(f'📄 CSV: {csv_rows} rows, {csv_cols} columns')
        
        # Determine expected table
        expected_table = csv_to_table_mapping.get(csv_name, None)
        
        if expected_table and expected_table in db_tables:
            # Get database table data
            try:
                df_db = pd.read_sql(f"SELECT * FROM {expected_table}", conn)
                db_rows = len(df_db)
                db_cols = len(df_db.columns)
                
                print(f'📋 Table "{expected_table}": {db_rows} rows, {db_cols} columns')
                
                # Compare row counts
                if csv_rows == db_rows:
                    print(f'✅ Row count matches: {csv_rows}')
                else:
                    print(f'⚠️ Row count mismatch: CSV={csv_rows}, DB={db_rows}')
                
                # Compare columns
                csv_columns = set(df_csv.columns)
                db_columns = set(df_db.columns)
                
                # Find missing columns in database
                missing_in_db = csv_columns - db_columns
                extra_in_db = db_columns - csv_columns
                
                if missing_in_db:
                    print(f'⚠️ Columns in CSV but not in DB: {list(missing_in_db)}')
                if extra_in_db:
                    print(f'ℹ️ Columns in DB but not in CSV: {list(extra_in_db)}')
                
                # Check for common key columns
                common_columns = csv_columns & db_columns
                print(f'✅ Common columns: {len(common_columns)}')
                
                # Store comparison result
                comparison_results[csv_name] = {
                    'csv_rows': csv_rows,
                    'db_rows': db_rows,
                    'csv_cols': csv_cols,
                    'db_cols': db_cols,
                    'table': expected_table,
                    'missing_in_db': list(missing_in_db),
                    'extra_in_db': list(extra_in_db),
                    'row_match': csv_rows == db_rows
                }
                
            except Exception as e:
                print(f'❌ Error reading table {expected_table}: {str(e)}')
                comparison_results[csv_name] = {'error': str(e)}
                
        else:
            print(f'⚠️ No corresponding table found for {csv_name}')
            if expected_table:
                if expected_table not in db_tables:
                    print(f'❌ Table "{expected_table}" does not exist in database')
            else:
                print(f'ℹ️ No mapping defined for {csv_name}')
            
            comparison_results[csv_name] = {'no_table': True}
            
    except Exception as e:
        print(f'❌ Error processing {csv_name}: {str(e)}')
        comparison_results[csv_name] = {'error': str(e)}

# Summary
print('\n📊 COMPARISON SUMMARY')
print('=' * 40)

total_csv_files = len(csv_files)
matched_files = 0
mismatched_files = 0
error_files = 0

for csv_name, result in comparison_results.items():
    if 'error' in result:
        print(f'❌ {csv_name}: ERROR - {result["error"]}')
        error_files += 1
    elif 'no_table' in result:
        print(f'⚠️ {csv_name}: NO TABLE MATCH')
        error_files += 1
    elif result.get('row_match', False):
        print(f'✅ {csv_name}: MATCHED ({result["csv_rows"]} rows)')
        matched_files += 1
    else:
        print(f'⚠️ {csv_name}: MISMATCH (CSV={result["csv_rows"]}, DB={result["db_rows"]})')
        mismatched_files += 1

print(f'\n📈 SUMMARY:')
print(f'✅ Matched files: {matched_files}/{total_csv_files}')
print(f'⚠️ Mismatched files: {mismatched_files}/{total_csv_files}')
print(f'❌ Error files: {error_files}/{total_csv_files}')

# Check for missing data by looking at specific key columns
print('\n🔍 DETAILED DATA VERIFICATION')
print('=' * 40)

# Check hosts table
if 'hosts' in db_tables:
    try:
        df_hosts = pd.read_sql("SELECT * FROM hosts", conn)
        print(f'📋 Hosts table: {len(df_hosts)} records')
        
        # Check for key columns
        key_cols = ['source_id', 'host_type', 'location_id']
        for col in key_cols:
            if col in df_hosts.columns:
                null_count = df_hosts[col].isnull().sum()
                print(f'  📊 {col}: {null_count} null values')
            else:
                print(f'  ⚠️ {col}: Column not found')
                
    except Exception as e:
        print(f'❌ Error checking hosts table: {str(e)}')

# Check samples table
if 'samples' in db_tables:
    try:
        df_samples = pd.read_sql("SELECT * FROM samples", conn)
        print(f'📋 Samples table: {len(df_samples)} records')
        
        # Check for key columns
        key_cols = ['sample_id', 'host_id', 'sample_origin']
        for col in key_cols:
            if col in df_samples.columns:
                null_count = df_samples[col].isnull().sum()
                print(f'  📊 {col}: {null_count} null values')
            else:
                print(f'  ⚠️ {col}: Column not found')
                
    except Exception as e:
        print(f'❌ Error checking samples table: {str(e)}')

# Check environmental_samples table
if 'environmental_samples' in db_tables:
    try:
        df_env = pd.read_sql("SELECT * FROM environmental_samples", conn)
        print(f'📋 Environmental samples table: {len(df_env)} records')
        
        # Check for key columns
        key_cols = ['env_sample_id', 'location_id', 'collection_date']
        for col in key_cols:
            if col in df_env.columns:
                null_count = df_env[col].isnull().sum()
                print(f'  📊 {col}: {null_count} null values')
            else:
                print(f'  ⚠️ {col}: Column not found')
                
    except Exception as e:
        print(f'❌ Error checking environmental_samples table: {str(e)}')

# Check screening_results table
if 'screening_results' in db_tables:
    try:
        df_screening = pd.read_sql("SELECT * FROM screening_results", conn)
        print(f'📋 Screening results table: {len(df_screening)} records')
        
        # Check for key columns
        key_cols = ['screening_id', 'sample_id']
        for col in key_cols:
            if col in df_screening.columns:
                null_count = df_screening[col].isnull().sum()
                print(f'  📊 {col}: {null_count} null values')
            else:
                print(f'  ⚠️ {col}: Column not found')
                
    except Exception as e:
        print(f'❌ Error checking screening_results table: {str(e)}')

# Close connection
conn.close()

print(f'\n🎯 VERIFICATION COMPLETED at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

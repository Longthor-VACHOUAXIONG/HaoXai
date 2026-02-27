#!/usr/bin/env python3
"""
Complete reimport: delete all data and reimport from all CSV files
"""

import pandas as pd
import sqlite3
from pathlib import Path

print('🔄 COMPLETE REIMPORT: DELETE ALL DATA AND REIMPORT FROM ALL CSV FILES')
print('=' * 70)

# Define paths
csv_dir = Path('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV')
db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'

print(f'📁 CSV Directory: {csv_dir}')
print(f'📂 Database: {db_path}')
print()

# Connect to database
try:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("PRAGMA foreign_keys = OFF;")
    print('✅ Connected to database successfully')
except Exception as e:
    print(f'❌ Error connecting to database: {str(e)}')
    exit(1)

# Delete all data from all tables
print('\n🗑️ DELETING ALL EXISTING DATA:')
print('-' * 35)

tables_to_clear = ['samples', 'hosts', 'taxonomy', 'locations', 'morphometrics', 'environmental_samples']

for table in tables_to_clear:
    try:
        cursor.execute(f'DELETE FROM {table}')
        print(f'✅ Cleared {table}')
    except Exception as e:
        print(f'❌ Error clearing {table}: {str(e)}')

conn.commit()

# Reset autoincrement counters
print('\n🔄 RESETTING AUTOINCREMENT COUNTERS:')
print('-' * 35)

for table in tables_to_clear:
    try:
        cursor.execute(f'DELETE FROM sqlite_sequence WHERE name = "{table}"')
        cursor.execute(f'UPDATE sqlite_sequence SET seq = 0 WHERE name = "{table}"')
        print(f'✅ Reset {table} counter')
    except:
        pass  # Ignore errors for sequence table

conn.commit()

# Reimport all data in correct order
print('\n📥 REIMPORTING ALL DATA FROM CSV FILES:')
print('=' * 45)

# 1. Import taxonomy first (hosts depend on it)
print('\n1️⃣ IMPORTING TAXONOMY DATA:')
print('-' * 30)

taxonomy_files = ['RodentHost.csv', 'MarketSampleAndHost.csv', 'Bathost.csv']
taxonomy_imported = 0

for csv_file in taxonomy_files:
    if (csv_dir / csv_file).exists():
        try:
            df = pd.read_csv(csv_dir / csv_file)
            print(f'📄 {csv_file}: {len(df)} rows')
            
            # Extract unique scientific names
            if 'ScientificName' in df.columns:
                unique_species = df[['ScientificName', 'Family', 'Genus', 'Species']].drop_duplicates()
                
                for _, species_row in unique_species.iterrows():
                    scientific_name = str(species_row['ScientificName']).strip()
                    family = str(species_row.get('Family', '')).strip()
                    genus = str(species_row.get('Genus', '')).strip()
                    species = str(species_row.get('Species', '')).strip()
                    
                    if scientific_name and scientific_name != 'nan':
                        try:
                            cursor.execute('''
                                INSERT INTO taxonomy (
                                    scientific_name, family, genus, species
                                ) VALUES (?, ?, ?, ?)
                            ''', (scientific_name, family, genus, species))
                            taxonomy_imported += 1
                        except sqlite3.IntegrityError:
                            # Skip duplicates
                            pass
            
        except Exception as e:
            print(f'❌ Error importing taxonomy from {csv_file}: {str(e)}')

conn.commit()
print(f'✅ Taxonomy imported: {taxonomy_imported} unique species')

# 2. Import locations
print('\n2️⃣ IMPORTING LOCATIONS DATA:')
print('-' * 30)

location_files = ['Bathost.csv', 'RodentHost.csv', 'MarketSampleAndHost.csv']
locations_imported = 0

for csv_file in location_files:
    if (csv_dir / csv_file).exists():
        try:
            df = pd.read_csv(csv_dir / csv_file)
            print(f'📄 {csv_file}: {len(df)} rows')
            
            # Extract unique locations
            location_cols = ['Province', 'District', 'Village']
            if all(col in df.columns for col in location_cols):
                unique_locations = df[location_cols].drop_duplicates()
                
                for _, loc_row in unique_locations.iterrows():
                    province = str(loc_row['Province']).strip()
                    district = str(loc_row['District']).strip()
                    village = str(loc_row['Village']).strip()
                    
                    if province and province != 'nan':
                        try:
                            cursor.execute('''
                                INSERT INTO locations (
                                    province, district, village
                                ) VALUES (?, ?, ?)
                            ''', (province, district, village))
                            locations_imported += 1
                        except sqlite3.IntegrityError:
                            # Skip duplicates
                            pass
            
        except Exception as e:
            print(f'❌ Error importing locations from {csv_file}: {str(e)}')

conn.commit()
print(f'✅ Locations imported: {locations_imported} unique locations')

# 3. Import hosts
print('\n3️⃣ IMPORTING HOSTS DATA:')
print('-' * 25)

host_files = {
    'Bathost.csv': 'Bat',
    'RodentHost.csv': 'Rodent',
    'MarketSampleAndHost.csv': 'Market'
}

hosts_imported = 0

for csv_file, host_type in host_files.items():
    if (csv_dir / csv_file).exists():
        try:
            df = pd.read_csv(csv_dir / csv_file)
            print(f'📄 {csv_file}: {len(df)} rows')
            
            for _, row in df.iterrows():
                source_id = str(row.get('SourceId', '')).strip()
                scientific_name = str(row.get('ScientificName', '')).strip()
                
                if source_id and source_id != 'nan':
                    # Get taxonomy_id
                    cursor.execute('SELECT taxonomy_id FROM taxonomy WHERE scientific_name = ?', (scientific_name,))
                    taxonomy_result = cursor.fetchone()
                    taxonomy_id = taxonomy_result[0] if taxonomy_result else None
                    
                    # Get location_id
                    province = str(row.get('Province', '')).strip()
                    district = str(row.get('District', '')).strip()
                    village = str(row.get('Village', '')).strip()
                    
                    location_id = None
                    if province and province != 'nan':
                        cursor.execute('''
                            SELECT location_id FROM locations 
                            WHERE province = ? AND district = ? AND village = ?
                        ''', (province, district, village))
                        location_result = cursor.fetchone()
                        location_id = location_result[0] if location_result else None
                    
                    try:
                        cursor.execute('''
                            INSERT INTO hosts (
                                source_id, host_type, taxonomy_id, location_id
                            ) VALUES (?, ?, ?, ?)
                        ''', (source_id, host_type, taxonomy_id, location_id))
                        hosts_imported += 1
                    except sqlite3.IntegrityError:
                        # Skip duplicates
                        pass
            
        except Exception as e:
            print(f'❌ Error importing hosts from {csv_file}: {str(e)}')

conn.commit()
print(f'✅ Hosts imported: {hosts_imported}')

# 4. Import environmental samples
print('\n4️⃣ IMPORTING ENVIRONMENTAL SAMPLES:')
print('-' * 35)

if (csv_dir / 'Environmental.csv').exists():
    try:
        df_env = pd.read_csv(csv_dir / 'Environmental.csv')
        print(f'📄 Environmental.csv: {len(df_env)} rows')
        
        env_imported = 0
        for _, row in df_env.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            province = str(row.get('Province', '')).strip()
            district = str(row.get('District', '')).strip()
            village = str(row.get('Village', '')).strip()
            
            if source_id and source_id != 'nan':
                # Get location_id
                location_id = None
                if province and province != 'nan':
                    cursor.execute('''
                        SELECT location_id FROM locations 
                        WHERE province = ? AND district = ? AND village = ?
                    ''', (province, district, village))
                    location_result = cursor.fetchone()
                    location_id = location_result[0] if location_result else None
                
                try:
                    cursor.execute('''
                        INSERT INTO environmental_samples (
                            source_id, location_id, site_type, collection_method,
                            collection_date, remark
                        ) VALUES (?, ?, ?, ?, ?, ?)
                    ''', (source_id, location_id, 
                          str(row.get('SiteType', '')).strip(),
                          str(row.get('Collection Method', '')).strip(),
                          str(row.get('Date', '')).strip(),
                          str(row.get('Remark', '')).strip()))
                    env_imported += 1
                except sqlite3.IntegrityError:
                    # Skip duplicates
                    pass
        
        conn.commit()
        print(f'✅ Environmental samples imported: {env_imported}')
        
    except Exception as e:
        print(f'❌ Error importing environmental samples: {str(e)}')

# 5. Import morphometrics
print('\n5️⃣ IMPORTING MORPHOMETRICS DATA:')
print('-' * 35)

morph_files = ['Bathost.csv', 'RodentHost.csv', 'MarketSampleAndHost.csv']
morph_imported = 0

for csv_file in morph_files:
    if (csv_dir / csv_file).exists():
        try:
            df = pd.read_csv(csv_dir / csv_file)
            print(f'📄 {csv_file}: {len(df)} rows')
            
            for _, row in df.iterrows():
                source_id = str(row.get('SourceId', '')).strip()
                
                if source_id and source_id != 'nan':
                    # Get host_id
                    cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
                    host_result = cursor.fetchone()
                    
                    if host_result:
                        host_id = host_result[0]
                        
                        # Map morphometric columns
                        morph_data = {
                            'weight_g': row.get('W'),
                            'head_body_mm': row.get('HB'),
                            'tail_mm': row.get('T') if csv_file == 'RodentHost.csv' else row.get('TL'),
                            'ear_mm': row.get('E') if csv_file == 'RodentHost.csv' else row.get('EL'),
                            'hind_foot_mm': row.get('HF'),
                            'forearm_mm': row.get('FA')
                        }
                        
                        # Filter out None values
                        morph_data = {k: v for k, v in morph_data.items() if v is not None and str(v) != 'nan'}
                        
                        if morph_data:
                            try:
                                # Build dynamic insert
                                cols = list(morph_data.keys())
                                vals = list(morph_data.values())
                                placeholders = ', '.join(['?'] * len(cols))
                                
                                cursor.execute(f'''
                                    INSERT INTO morphometrics (
                                        host_id, {', '.join(cols)}
                                    ) VALUES (?, {placeholders})
                                ''', [host_id] + vals)
                                morph_imported += 1
                            except sqlite3.IntegrityError:
                                # Update existing record
                                set_clause = ', '.join([f'{col} = ?' for col in cols])
                                cursor.execute(f'''
                                    UPDATE morphometrics 
                                    SET {set_clause}
                                    WHERE host_id = ?
                                ''', vals + [host_id])
                                morph_imported += 1
            
        except Exception as e:
            print(f'❌ Error importing morphometrics from {csv_file}: {str(e)}')

conn.commit()
print(f'✅ Morphometrics imported: {morph_imported}')

# 6. Import samples
print('\n6️⃣ IMPORTING SAMPLES DATA:')
print('-' * 30)

# Import bat samples from Bathost.csv + Screening.csv
if (csv_dir / 'Bathost.csv').exists() and (csv_dir / 'Screening.csv').exists():
    try:
        df_bathost = pd.read_csv(csv_dir / 'Bathost.csv')
        df_screening = pd.read_csv(csv_dir / 'Screening.csv')
        print(f'📄 Bathost.csv: {len(df_bathost)} rows')
        print(f'📄 Screening.csv: {len(df_screening)} rows')
        
        # Create screening mapping
        screening_mapping = {}
        for _, row in df_screening.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            tested_sample_id = str(row.get('Tested_SampleId', '')).strip()
            sample_type = str(row.get('SampleType', '')).strip()
            
            if source_id and source_id != 'nan' and tested_sample_id and tested_sample_id != 'nan':
                if source_id not in screening_mapping:
                    screening_mapping[source_id] = {}
                
                if 'saliva' in sample_type.lower() or tested_sample_id.startswith('CANB_S'):
                    screening_mapping[source_id]['saliva_id'] = tested_sample_id
                elif 'anal' in sample_type.lower() or tested_sample_id.startswith('CANB_A'):
                    screening_mapping[source_id]['anal_id'] = tested_sample_id
                elif 'urine' in sample_type.lower() or tested_sample_id.startswith('CANB_U'):
                    screening_mapping[source_id]['urine_id'] = tested_sample_id
                elif 'blood' in sample_type.lower() or tested_sample_id.startswith('CANB_B'):
                    screening_mapping[source_id]['blood_id'] = tested_sample_id
                elif 'tissue' in sample_type.lower() or tested_sample_id.startswith('CANB_T'):
                    screening_mapping[source_id]['tissue_id'] = tested_sample_id
        
        bat_samples = 0
        for _, row in df_bathost.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            
            if source_id and source_id != 'nan':
                # Get host_id
                cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
                host_result = cursor.fetchone()
                
                if host_result:
                    host_id = host_result[0]
                    
                    # Get biological IDs from screening mapping
                    bio_ids = screening_mapping.get(source_id, {})
                    saliva_id = bio_ids.get('saliva_id')
                    anal_id = bio_ids.get('anal_id')
                    urine_id = bio_ids.get('urine_id')
                    blood_id = bio_ids.get('blood_id')
                    tissue_id = bio_ids.get('tissue_id')
                    
                    # Determine sample origin
                    sample_origin = 'BatSwab'
                    if tissue_id and (tissue_id.startswith('CANB_TIS') or tissue_id.startswith('CANB_I')):
                        sample_origin = 'BatTissue'
                    
                    try:
                        cursor.execute('''
                            INSERT INTO samples (
                                source_id, host_id, sample_origin, saliva_id, anal_id, 
                                urine_id, blood_id, tissue_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', (source_id, host_id, sample_origin, saliva_id, anal_id, 
                              urine_id, blood_id, tissue_id))
                        bat_samples += 1
                    except sqlite3.IntegrityError:
                        # Skip duplicates
                        pass
        
        conn.commit()
        print(f'✅ Bat samples imported: {bat_samples}')
        
    except Exception as e:
        print(f'❌ Error importing bat samples: {str(e)}')

# Import market samples
if (csv_dir / 'MarketSampleAndHost.csv').exists():
    try:
        df_market = pd.read_csv(csv_dir / 'MarketSampleAndHost.csv')
        print(f'📄 MarketSampleAndHost.csv: {len(df_market)} rows')
        
        market_samples = 0
        for _, row in df_market.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            field_sample_id = str(row.get('FieldSampleId', '')).strip()
            
            if source_id and source_id != 'nan' and field_sample_id and field_sample_id != 'nan':
                # Get host_id
                cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
                host_result = cursor.fetchone()
                
                if host_result:
                    host_id = host_result[0]
                    
                    # Parse FieldSampleId
                    saliva_id = None
                    anal_id = None
                    ear_id = None
                    
                    if 'SAL' in field_sample_id:
                        saliva_id = field_sample_id
                    elif 'ANA' in field_sample_id:
                        anal_id = field_sample_id
                    elif 'EAR' in field_sample_id:
                        ear_id = field_sample_id
                    
                    try:
                        cursor.execute('''
                            INSERT INTO samples (
                                source_id, host_id, sample_origin, saliva_id, anal_id, 
                                urine_id, ear_id
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        ''', (source_id, host_id, 'MarketSample', saliva_id, anal_id, 
                              urine_id, ear_id))
                        market_samples += 1
                    except sqlite3.IntegrityError:
                        # Skip duplicates
                        pass
        
        conn.commit()
        print(f'✅ Market samples imported: {market_samples}')
        
    except Exception as e:
        print(f'❌ Error importing market samples: {str(e)}')

# Import rodent samples
if (csv_dir / 'RodentHost.csv').exists():
    try:
        df_rodent = pd.read_csv(csv_dir / 'RodentHost.csv')
        print(f'📄 RodentHost.csv: {len(df_rodent)} rows')
        
        rodent_samples = 0
        for _, row in df_rodent.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            
            if source_id and source_id != 'nan':
                # Get host_id
                cursor.execute('SELECT host_id FROM hosts WHERE source_id = ?', (source_id,))
                host_result = cursor.fetchone()
                
                if host_result:
                    host_id = host_result[0]
                    
                    try:
                        cursor.execute('''
                            INSERT INTO samples (
                                source_id, host_id, sample_origin
                            ) VALUES (?, ?, ?)
                        ''', (source_id, host_id, 'RodentSample'))
                        rodent_samples += 1
                    except sqlite3.IntegrityError:
                        # Skip duplicates
                        pass
        
        conn.commit()
        print(f'✅ Rodent samples imported: {rodent_samples}')
        
    except Exception as e:
        print(f'❌ Error importing rodent samples: {str(e)}')

# Import environmental samples
if (csv_dir / 'Environmental.csv').exists():
    try:
        df_env = pd.read_csv(csv_dir / 'Environmental.csv')
        print(f'📄 Environmental.csv: {len(df_env)} rows')
        
        env_samples = 0
        for _, row in df_env.iterrows():
            source_id = str(row.get('SourceId', '')).strip()
            
            if source_id and source_id != 'nan':
                # Get env_sample_id
                cursor.execute('SELECT env_sample_id FROM environmental_samples WHERE source_id = ?', (source_id,))
                env_result = cursor.fetchone()
                
                if env_result:
                    env_id = env_result[0]
                    
                    try:
                        cursor.execute('''
                            INSERT INTO samples (
                                source_id, sample_origin, env_sample_id
                            ) VALUES (?, ?, ?)
                        ''', (source_id, 'Environmental', env_id))
                        env_samples += 1
                    except sqlite3.IntegrityError:
                        # Skip duplicates
                        pass
        
        conn.commit()
        print(f'✅ Environmental samples imported: {env_samples}')
        
    except Exception as e:
        print(f'❌ Error importing environmental samples: {str(e)}')

# Final verification
print('\n🔍 FINAL VERIFICATION')
print('=' * 20)

# Check final counts
cursor.execute("SELECT COUNT(*) FROM samples")
total_samples = cursor.fetchone()[0]
print(f'📊 Total samples: {total_samples:,}')

cursor.execute('''
    SELECT sample_origin, COUNT(*) as count
    FROM samples
    GROUP BY sample_origin
    ORDER BY count DESC
''')
final_distribution = cursor.fetchall()

print(f'\n📊 Final sample distribution:')
for origin, count in final_distribution:
    print(f'  📋 {origin}: {count:,} samples')

cursor.execute("SELECT COUNT(*) FROM hosts")
total_hosts = cursor.fetchone()[0]
print(f'\n📊 Total hosts: {total_hosts:,}')

cursor.execute("SELECT COUNT(*) FROM taxonomy")
total_taxonomy = cursor.fetchone()[0]
print(f'📊 Total taxonomy: {total_taxonomy:,}')

cursor.execute("SELECT COUNT(*) FROM locations")
total_locations = cursor.fetchone()[0]
print(f'📊 Total locations: {total_locations:,}')

cursor.execute("SELECT COUNT(*) FROM morphometrics")
total_morphometrics = cursor.fetchone()[0]
print(f'📊 Total morphometrics: {total_morphometrics:,}')

cursor.execute("SELECT COUNT(*) FROM environmental_samples")
total_env_samples = cursor.fetchone()[0]
print(f'📊 Total environmental samples: {total_env_samples:,}')

conn.close()

print(f'\n🎉 COMPLETE REIMPORT FINISHED!')
print(f'✅ All data deleted and reimported from CSV files')
print(f'✅ Total samples: {total_samples:,}')
print(f'✅ Total hosts: {total_hosts:,}')
print(f'✅ Database: {db_path}')
print(f'🏁 Completed at: {pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")}')

#!/usr/bin/env python3
"""
Count samples by scientific name
"""

import sqlite3

db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 COUNTING SAMPLES BY SCIENTIFIC NAME')
print('=' * 50)

# Query 1: Basic count by scientific name
print('\n🎯 BASIC COUNT BY SCIENTIFIC NAME:')
print('-' * 40)

cursor.execute('''
    SELECT 
        t.scientific_name,
        COUNT(s.sample_id) as total_samples,
        h.host_type
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY t.scientific_name, h.host_type
    ORDER BY total_samples DESC
    LIMIT 15
''')

results = cursor.fetchall()
for sci_name, count, host_type in results:
    print(f'📋 {sci_name} ({host_type}): {count:,} samples')

# Query 2: Top 10 species overall
print('\n🏆 TOP 10 SPECIES BY SAMPLE COUNT:')
print('-' * 40)

cursor.execute('''
    SELECT 
        t.scientific_name,
        t.family,
        COUNT(s.sample_id) as sample_count,
        COUNT(DISTINCT s.sample_origin) as sample_types
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY t.scientific_name, t.family
    ORDER BY sample_count DESC
    LIMIT 10
''')

results = cursor.fetchall()
for sci_name, family, count, types in results:
    print(f'🦇 {sci_name} ({family}): {count:,} samples ({types} types)')

# Query 3: Summary by host type
print('\n📊 SUMMARY BY HOST TYPE:')
print('-' * 30)

cursor.execute('''
    SELECT 
        h.host_type,
        COUNT(s.sample_id) as total_samples,
        COUNT(DISTINCT t.scientific_name) as unique_species
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY h.host_type
    ORDER BY total_samples DESC
''')

results = cursor.fetchall()
for host_type, total_samples, species in results:
    print(f'📋 {host_type}: {total_samples:,} samples from {species} species')

# Query 4: Overall statistics
print('\n📈 OVERALL STATISTICS:')
print('-' * 25)

cursor.execute('SELECT COUNT(*) FROM samples')
total_samples = cursor.fetchone()[0]

cursor.execute('''
    SELECT COUNT(DISTINCT t.scientific_name)
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
''')
unique_species = cursor.fetchone()[0]

print(f'📊 Total samples: {total_samples:,}')
print(f'📊 Samples with scientific names: {total_samples:,}')
print(f'📊 Unique species represented: {unique_species}')

conn.close()

print(f'\n🎉 SAMPLE COUNTING COMPLETE!')

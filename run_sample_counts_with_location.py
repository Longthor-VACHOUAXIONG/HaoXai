#!/usr/bin/env python3
"""
Count samples by scientific name with location information
"""

import sqlite3

db_path = 'd:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataCSV/SQLite.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

print('📊 COUNTING SAMPLES BY SCIENTIFIC NAME WITH LOCATION')
print('=' * 60)

# Query 1: Top species by province
print('\n🗺️ TOP SPECIES BY PROVINCE:')
print('-' * 40)

cursor.execute('''
    SELECT 
        h.province,
        t.scientific_name,
        COUNT(s.sample_id) as sample_count
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY h.province, t.scientific_name
    ORDER BY sample_count DESC
    LIMIT 15
''')

results = cursor.fetchall()
for province, sci_name, count in results:
    print(f'📋 {province} - {sci_name}: {count:,} samples')

# Query 2: Summary by province
print('\n📊 SUMMARY BY PROVINCE:')
print('-' * 30)

cursor.execute('''
    SELECT 
        h.province,
        COUNT(s.sample_id) as total_samples,
        COUNT(DISTINCT t.scientific_name) as unique_species,
        COUNT(DISTINCT h.district) as districts_covered
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY h.province
    ORDER BY total_samples DESC
''')

results = cursor.fetchall()
for province, total, species, districts in results:
    print(f'📋 {province}: {total:,} samples, {species} species, {districts} districts')

# Query 3: Top locations by sample count
print('\n🏆 TOP 10 MOST SAMPLED LOCATIONS:')
print('-' * 40)

cursor.execute('''
    SELECT 
        h.province,
        h.district,
        h.village,
        COUNT(s.sample_id) as sample_count,
        COUNT(DISTINCT t.scientific_name) as unique_species,
        COUNT(DISTINCT h.host_type) as host_types
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY h.province, h.district, h.village
    ORDER BY sample_count DESC
    LIMIT 10
''')

results = cursor.fetchall()
for province, district, village, count, species, host_types in results:
    print(f'📍 {province}, {district}, {village}: {count:,} samples ({species} species, {host_types} host types)')

# Query 4: Species diversity by location
print('\n🌍 SPECIES DIVERSITY BY LOCATION:')
print('-' * 40)

cursor.execute('''
    SELECT 
        h.province,
        h.district,
        COUNT(DISTINCT t.scientific_name) as species_diversity,
        COUNT(s.sample_id) as total_samples,
        COUNT(DISTINCT h.host_type) as host_types
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY h.province, h.district
    ORDER BY species_diversity DESC, total_samples DESC
    LIMIT 10
''')

results = cursor.fetchall()
for province, district, diversity, total, host_types in results:
    print(f'🌍 {province}, {district}: {diversity} species, {total:,} samples, {host_types} host types')

# Query 5: Species found in multiple provinces
print('\n🦇 SPECIES FOUND IN MULTIPLE PROVINCES:')
print('-' * 45)

cursor.execute('''
    SELECT 
        t.scientific_name,
        COUNT(DISTINCT h.province) as province_count,
        COUNT(DISTINCT h.district) as district_count,
        COUNT(s.sample_id) as total_samples
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    GROUP BY t.scientific_name
    HAVING COUNT(DISTINCT h.province) > 1
    ORDER BY province_count DESC, total_samples DESC
    LIMIT 10
''')

results = cursor.fetchall()
for sci_name, provinces, districts, total in results:
    print(f'🦇 {sci_name}: {provinces} provinces, {districts} districts, {total:,} samples')

# Query 6: Detailed breakdown for top species
print('\n🔍 DETAILED BREAKDOWN - TOP 5 SPECIES:')
print('-' * 45)

cursor.execute('''
    SELECT 
        t.scientific_name,
        h.province,
        h.district,
        h.village,
        COUNT(s.sample_id) as sample_count
    FROM samples s
    JOIN hosts h ON s.source_id = h.source_id
    JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    WHERE t.scientific_name IN (
        SELECT scientific_name 
        FROM (
            SELECT t2.scientific_name, COUNT(s2.sample_id) as count
            FROM samples s2
            JOIN hosts h2 ON s2.source_id = h2.source_id
            JOIN taxonomy t2 ON h2.taxonomy_id = t2.taxonomy_id
            GROUP BY t2.scientific_name
            ORDER BY count DESC
            LIMIT 5
        )
    )
    GROUP BY t.scientific_name, h.province, h.district, h.village
    ORDER BY t.scientific_name, sample_count DESC
''')

results = cursor.fetchall()
current_species = None
for sci_name, province, district, village, count in results:
    if current_species != sci_name:
        print(f'\n🦇 {sci_name}:')
        current_species = sci_name
    print(f'  📍 {province}, {district}, {village}: {count:,} samples')

conn.close()

print(f'\n🎉 SAMPLE COUNTING WITH LOCATION COMPLETE!')

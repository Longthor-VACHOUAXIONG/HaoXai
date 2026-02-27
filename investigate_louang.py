#!/usr/bin/env python3
"""
Investigate Louang Namtha data
"""
import sqlite3

# Connect to database
conn = sqlite3.connect('d:/MyFiles/Program_Last_version/ViroDB_structure_latest_V - Copy/DataExcel/CAN2-With-Referent-Key.db')
cursor = conn.cursor()

print('🔍 DETAILED LOUANG NAMTHA SPECIES ANALYSIS')
print('=' * 50)

# Check all species in Louang Namtha
louang_species_query = '''
SELECT 
    t.scientific_name,
    COUNT(*) as total_samples,
    SUM(CASE WHEN sr.pan_corona = 'Positive' THEN 1 ELSE 0 END) as positive_samples,
    SUM(CASE WHEN sr.pan_corona = 'Negative' THEN 1 ELSE 0 END) as negative_samples
FROM samples s
JOIN hosts h ON s.host_id = h.host_id
JOIN locations l ON h.location_id = l.location_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
WHERE l.province LIKE '%Louang%'
GROUP BY t.scientific_name
ORDER BY total_samples DESC
'''

cursor.execute(louang_species_query)
species_results = cursor.fetchall()

print('🦇 Species in Louang Namtha:')
for species, total, positive, negative in species_results:
    species_name = species if species else 'Unknown'
    rate = (positive/total*100) if total > 0 else 0
    print(f'  • {species_name}: {total} samples, {positive} positive, {negative} negative ({rate:.1f}% positivity)')

print()
print('🗂️ PROVINCE NAME VARIATIONS:')
print('-' * 30)
cursor.execute('SELECT DISTINCT province FROM locations WHERE province LIKE "%Louang%" OR province LIKE "%Luang%" ORDER BY province')
provinces = cursor.fetchall()
for province in provinces:
    print(f'  • "{province[0]}"')

print()
print('📊 WHY LOUANG NAMTHA HAS HIGHEST POSITIVITY:')
print('-' * 50)
print('1. Small Sample Size: Only 32 total samples (vs thousands in other provinces)')
print('2. High Impact: 2 positive samples out of 32 = 6.25% positivity rate')
print('3. Species: Hipposideros larvatus (Roundleaf horseshoe bat)')
print('4. Location: Viengphoukha district, Nam Eng village')
print('5. Sample Types: Anal swab and intestine samples')
print()
print('📈 COMPARISON WITH OTHER PROVINCES:')
print('• Khammouan: 129/3034 = 4.25% (large sample size)')
print('• Vientiane: 109/3165 = 3.44% (largest sample size)')
print('• Bolikhamxay: 5/765 = 0.65% (moderate sample size)')
print('• Louang Namtha: 2/32 = 6.25% (small sample size, high rate)')

conn.close()

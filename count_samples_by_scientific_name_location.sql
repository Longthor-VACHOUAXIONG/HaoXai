-- ========================================
-- COUNT SAMPLES BY SCIENTIFIC NAME WITH LOCATION
-- ========================================

-- 1. Basic count by scientific name and location
SELECT 
    t.scientific_name,
    h.province,
    h.district,
    h.village,
    COUNT(s.sample_id) as sample_count,
    h.host_type
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, h.province, h.district, h.village, h.host_type
ORDER BY sample_count DESC;

-- 2. Top species by province
SELECT 
    h.province,
    t.scientific_name,
    COUNT(s.sample_id) as sample_count
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY h.province, t.scientific_name
ORDER BY h.province, sample_count DESC;

-- 3. Species distribution by province (summary)
SELECT 
    h.province,
    COUNT(s.sample_id) as total_samples,
    COUNT(DISTINCT t.scientific_name) as unique_species,
    COUNT(DISTINCT h.district) as districts_covered
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY h.province
ORDER BY total_samples DESC;

-- 4. Top species by district
SELECT 
    h.province,
    h.district,
    t.scientific_name,
    COUNT(s.sample_id) as sample_count
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY h.province, h.district, t.scientific_name
ORDER BY sample_count DESC;

-- 5. Complete breakdown with family and location
SELECT 
    t.scientific_name,
    t.family,
    h.province,
    h.district,
    h.village,
    h.host_type,
    COUNT(s.sample_id) as sample_count,
    COUNT(DISTINCT s.sample_origin) as sample_types
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, t.family, h.province, h.district, h.village, h.host_type
ORDER BY sample_count DESC;

-- 6. Species diversity by location
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
ORDER BY species_diversity DESC, total_samples DESC;

-- 7. Top 10 most sampled locations
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
LIMIT 10;

-- 8. Species found in multiple provinces
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
ORDER BY province_count DESC, total_samples DESC;

-- 9. Location-specific species (found in only one province)
SELECT 
    h.province,
    COUNT(DISTINCT t.scientific_name) as unique_species,
    COUNT(s.sample_id) as total_samples
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY h.province
ORDER BY unique_species DESC;

-- 10. Heat map data (species x location matrix)
SELECT 
    t.scientific_name,
    h.province,
    COUNT(s.sample_id) as sample_count
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, h.province
ORDER BY t.scientific_name, h.province;

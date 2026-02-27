-- ========================================
-- COUNT SAMPLES BY SCIENTIFIC NAME
-- ========================================

-- 1. Basic count by scientific name
SELECT 
    t.scientific_name,
    COUNT(s.sample_id) as total_samples,
    h.host_type
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, h.host_type
ORDER BY total_samples DESC;

-- 2. Detailed breakdown by sample type and scientific name
SELECT 
    t.scientific_name,
    t.family,
    t.genus,
    h.host_type,
    s.sample_origin,
    COUNT(s.sample_id) as sample_count
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, t.family, t.genus, h.host_type, s.sample_origin
ORDER BY sample_count DESC;

-- 3. Top 20 species by sample count
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
LIMIT 20;

-- 4. Samples by host type and scientific name
SELECT 
    h.host_type,
    t.scientific_name,
    COUNT(s.sample_id) as sample_count,
    ROUND(COUNT(s.sample_id) * 100.0 / SUM(COUNT(s.sample_id)) OVER (PARTITION BY h.host_type), 2) as percentage
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY h.host_type, t.scientific_name
ORDER BY h.host_type, sample_count DESC;

-- 5. Complete summary with percentages
SELECT 
    t.scientific_name,
    t.family,
    t.genus,
    h.host_type,
    COUNT(s.sample_id) as sample_count,
    ROUND(COUNT(s.sample_id) * 100.0 / (SELECT COUNT(*) FROM samples), 2) as overall_percentage,
    ROUND(COUNT(s.sample_id) * 100.0 / SUM(COUNT(s.sample_id)) OVER (PARTITION BY h.host_type), 2) as host_type_percentage
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
GROUP BY t.scientific_name, t.family, t.genus, h.host_type
ORDER BY sample_count DESC;

-- 6. Samples without scientific names (for completeness)
SELECT 
    COUNT(s.sample_id) as samples_without_taxonomy,
    h.host_type
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
WHERE t.scientific_name IS NULL
GROUP BY h.host_type;

-- 7. Summary statistics
SELECT 
    'Total Samples' as metric,
    COUNT(*) as count
FROM samples
UNION ALL
SELECT 
    'Samples with Scientific Names',
    COUNT(s.sample_id)
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
UNION ALL
SELECT 
    'Unique Species',
    COUNT(DISTINCT t.scientific_name)
FROM samples s
JOIN hosts h ON s.source_id = h.source_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id;

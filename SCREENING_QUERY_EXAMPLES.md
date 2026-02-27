# Screening Results Query Examples

## 🎯 **Complete Screening Results with All Related Data**

### **1. Basic Screening Results Query**
```sql
-- Get screening results with sample, host, taxonomy, and location information
SELECT 
    sr.screening_id,
    sr.excel_id,
    sr.tested_sample_id,
    sr.team,
    sr.sample_type,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    sr.created_at as screening_date,
    
    -- Sample information
    s.sample_id,
    s.source_id as sample_source_id,
    s.sample_origin,
    s.collection_date,
    s.tissue_id,
    s.blood_id,
    s.saliva_id,
    
    -- Host information
    h.host_id,
    h.source_id as host_source_id,
    h.host_type,
    h.field_id,
    h.bag_id,
    h.sex,
    h.status,
    h.capture_date,
    
    -- Taxonomy information
    t.scientific_name,
    t.common_name,
    t.english_common_name,
    t.genus,
    t.species,
    t.family,
    t.order_name,
    
    -- Location information
    l.location_id,
    l.province,
    l.district,
    l.village,
    l.site_name,
    l.latitude,
    l.longitude,
    
    -- Environmental sample info (if applicable)
    es.env_sample_id,
    es.pool_id,
    es.collection_method as env_collection_method
    
FROM screening_results sr
LEFT JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN environmental_samples es ON s.env_sample_id = es.env_sample_id
LEFT JOIN taxonomy t ON (h.taxonomy_id = t.taxonomy_id)
LEFT JOIN locations l ON (s.location_id = l.location_id OR h.location_id = l.location_id OR es.location_id = l.location_id)
ORDER BY sr.screening_id;
```

### **2. Positive Results Only**
```sql
-- Get only positive screening results with full details
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    t.scientific_name,
    t.common_name,
    h.host_type,
    l.province,
    l.district,
    l.village,
    CASE 
        WHEN sr.pan_corona = 'Positive' THEN 'Coronavirus'
        WHEN sr.pan_hanta = 'Positive' THEN 'Hantavirus'
        WHEN sr.pan_paramyxo = 'Positive' THEN 'Paramyxovirus'
        WHEN sr.pan_flavi = 'Positive' THEN 'Flavivirus'
    END as virus_type
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE sr.pan_corona = 'Positive' 
   OR sr.pan_hanta = 'Positive' 
   OR sr.pan_paramyxo = 'Positive' 
   OR sr.pan_flavi = 'Positive'
ORDER BY sr.screening_id;
```

### **3. By Sample Type**
```sql
-- Query by specific sample type (e.g., Bat Tissue)
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    t.scientific_name,
    h.host_type,
    h.field_id,
    l.province,
    l.district,
    l.village,
    h.capture_date
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE s.sample_origin = 'BatTissue'
ORDER BY h.capture_date, sr.tested_sample_id;
```

### **4. By Location**
```sql
-- Get all screening results from specific province
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    t.scientific_name,
    h.host_type,
    h.field_id,
    l.district,
    l.village,
    sr.team,
    sr.created_at
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE l.province = 'Vientiane'
ORDER BY sr.created_at DESC;
```

### **5. By Scientific Name**
```sql
-- Get all screening results for specific species
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    h.host_type,
    h.field_id,
    h.sex,
    h.capture_date,
    l.province,
    l.district,
    l.village,
    sr.team,
    sr.created_at
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE t.scientific_name LIKE '%Rousettus%'
ORDER BY h.capture_date, sr.tested_sample_id;
```

### **6. Environmental Samples**
```sql
-- Get environmental sample screening results
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    es.pool_id,
    es.collection_method,
    es.collection_date,
    l.province,
    l.district,
    l.village,
    l.site_name,
    sr.team,
    sr.created_at
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
JOIN environmental_samples es ON s.env_sample_id = es.env_sample_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE s.sample_origin = 'Environmental'
ORDER BY es.collection_date, sr.tested_sample_id;
```

### **7. Summary Statistics by Location**
```sql
-- Get screening summary by province
SELECT 
    l.province,
    COUNT(*) as total_samples,
    COUNT(CASE WHEN sr.pan_corona = 'Positive' THEN 1 END) as corona_positive,
    COUNT(CASE WHEN sr.pan_hanta = 'Positive' THEN 1 END) as hanta_positive,
    COUNT(CASE WHEN sr.pan_paramyxo = 'Positive' THEN 1 END) as paramyxo_positive,
    COUNT(CASE WHEN sr.pan_flavi = 'Positive' THEN 1 END) as flavi_positive,
    COUNT(CASE WHEN s.sample_origin = 'BatSwab' THEN 1 END) as bat_swabs,
    COUNT(CASE WHEN s.sample_origin = 'BatTissue' THEN 1 END) as bat_tissues,
    COUNT(CASE WHEN s.sample_origin = 'RodentSample' THEN 1 END) as rodent_samples,
    COUNT(CASE WHEN s.sample_origin = 'Environmental' THEN 1 END) as environmental_samples
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN locations l ON s.location_id = l.location_id
GROUP BY l.province
ORDER BY total_samples DESC;
```

### **8. Summary by Species**
```sql
-- Get screening summary by species
SELECT 
    t.scientific_name,
    t.common_name,
    h.host_type,
    COUNT(*) as total_samples,
    COUNT(CASE WHEN sr.pan_corona = 'Positive' THEN 1 END) as corona_positive,
    COUNT(CASE WHEN sr.pan_hanta = 'Positive' THEN 1 END) as hanta_positive,
    COUNT(CASE WHEN sr.pan_paramyxo = 'Positive' THEN 1 END) as paramyxo_positive,
    COUNT(CASE WHEN sr.pan_flavi = 'Positive' THEN 1 END) as flavi_positive,
    COUNT(CASE WHEN s.sample_origin = 'BatSwab' THEN 1 END) as swabs,
    COUNT(CASE WHEN s.sample_origin = 'BatTissue' THEN 1 END) as tissues
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
JOIN hosts h ON s.host_id = h.host_id
JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
WHERE h.host_type IN ('Bat', 'Rodent')
GROUP BY t.scientific_name, t.common_name, h.host_type
ORDER BY total_samples DESC;
```

### **9. Recent Results (Last 30 Days)**
```sql
-- Get recent screening results
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    t.scientific_name,
    h.host_type,
    l.province,
    l.district,
    sr.team,
    sr.created_at
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
WHERE sr.created_at >= date('now', '-30 days')
ORDER BY sr.created_at DESC;
```

### **10. Complex Search**
```sql
-- Advanced search with multiple criteria
SELECT 
    sr.tested_sample_id,
    sr.pan_corona,
    sr.pan_hanta,
    sr.pan_paramyxo,
    sr.pan_flavi,
    s.sample_origin,
    t.scientific_name,
    h.host_type,
    h.field_id,
    h.sex,
    h.capture_date,
    l.province,
    l.district,
    l.village,
    sr.team,
    sr.created_at,
    sl.rack,
    sl.spot_position
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
LEFT JOIN storage_locations sl ON s.source_id = sl.sample_tube_id
WHERE 1=1
  AND (sr.pan_corona = 'Positive' OR sr.pan_hanta = 'Positive')
  AND l.province IN ('Vientiane', 'Luang Prabang')
  AND h.host_type = 'Bat'
  AND s.collection_date >= '2023-01-01'
ORDER BY sr.created_at DESC;
```

## 🐍 **Python Query Examples**

### **Basic Query in Python**
```python
import sqlite3

def get_screening_with_details(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    query = """
    SELECT 
        sr.tested_sample_id,
        sr.pan_corona,
        sr.pan_hanta,
        s.sample_origin,
        t.scientific_name,
        h.host_type,
        l.province,
        l.district
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    LEFT JOIN hosts h ON s.host_id = h.host_id
    LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    LEFT JOIN locations l ON s.location_id = l.location_id
    WHERE sr.pan_corona = 'Positive'
    ORDER BY sr.created_at DESC
    """
    
    cursor.execute(query)
    results = cursor.fetchall()
    
    # Get column names
    columns = [description[0] for description in cursor.description]
    
    # Convert to list of dictionaries
    data = [dict(zip(columns, row)) for row in results]
    
    conn.close()
    return data

# Usage
results = get_screening_with_details('DataExcel/CAN2-With-Referent-Key.db')
for result in results:
    print(f"Sample: {result['tested_sample_id']}, Species: {result['scientific_name']}, Province: {result['province']}")
```

### **Pandas Query**
```python
import pandas as pd
import sqlite3

def get_screening_dataframe(db_path):
    query = """
    SELECT 
        sr.tested_sample_id,
        sr.pan_corona,
        sr.pan_hanta,
        sr.pan_paramyxo,
        sr.pan_flavi,
        s.sample_origin,
        t.scientific_name,
        t.common_name,
        h.host_type,
        h.sex,
        l.province,
        l.district,
        l.village,
        sr.created_at
    FROM screening_results sr
    JOIN samples s ON sr.sample_id = s.sample_id
    LEFT JOIN hosts h ON s.host_id = h.host_id
    LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
    LEFT JOIN locations l ON s.location_id = l.location_id
    """
    
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df

# Usage
df = get_screening_dataframe('DataExcel/CAN2-With-Referent-Key.db')

# Filter positive results
positive_samples = df[
    (df['pan_corona'] == 'Positive') | 
    (df['pan_hanta'] == 'Positive') |
    (df['pan_paramyxo'] == 'Positive') |
    (df['pan_flavi'] == 'Positive')
]

print(f"Total samples: {len(df)}")
print(f"Positive samples: {len(positive_samples)}")
print(positive_samples.head())
```

## 📊 **Query Performance Tips**

### **1. Add Indexes for Better Performance**
```sql
-- Recommended indexes for screening queries
CREATE INDEX idx_screening_sample_id ON screening_results(sample_id);
CREATE INDEX idx_screening_tested_sample_id ON screening_results(tested_sample_id);
CREATE INDEX idx_samples_host_id ON samples(host_id);
CREATE INDEX idx_samples_env_sample_id ON samples(env_sample_id);
CREATE INDEX idx_hosts_taxonomy_id ON hosts(taxonomy_id);
CREATE INDEX idx_samples_location_id ON samples(location_id);
```

### **2. Use Specific Columns Instead of SELECT *
```sql
-- Better performance
SELECT sr.tested_sample_id, sr.pan_corona, t.scientific_name, l.province
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id;

-- Avoid this for large datasets
SELECT * FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
...
```

### **3. Use LIMIT for Large Result Sets**
```sql
-- Get first 100 results
SELECT sr.tested_sample_id, sr.pan_corona, t.scientific_name, l.province
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
ORDER BY sr.created_at DESC
LIMIT 100;
```

## 🎯 **Common Query Patterns**

### **For Excel Upload Data Enrichment**
```sql
-- This query matches what your Excel upload system needs
SELECT 
    sr.tested_sample_id as SampleId,
    sr.pan_corona,
    t.scientific_name,
    l.province,
    sl.rack as rack_position
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
LEFT JOIN storage_locations sl ON s.source_id = sl.sample_tube_id
WHERE sr.tested_sample_id = ?
```

### **For Research Analysis**
```sql
-- Comprehensive analysis query
SELECT 
    l.province,
    t.scientific_name,
    h.host_type,
    s.sample_origin,
    COUNT(*) as total_samples,
    COUNT(CASE WHEN sr.pan_corona = 'Positive' THEN 1 END) as corona_pos,
    COUNT(CASE WHEN sr.pan_hanta = 'Positive' THEN 1 END) as hanta_pos,
    ROUND(COUNT(CASE WHEN sr.pan_corona = 'Positive' THEN 1 END) * 100.0 / COUNT(*), 2) as corona_percent
FROM screening_results sr
JOIN samples s ON sr.sample_id = s.sample_id
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN taxonomy t ON h.taxonomy_id = t.taxonomy_id
LEFT JOIN locations l ON s.location_id = l.location_id
GROUP BY l.province, t.scientific_name, h.host_type, s.sample_origin
HAVING COUNT(*) > 10
ORDER BY corona_percent DESC;
```

These queries cover most common use cases for screening results analysis! 🚀

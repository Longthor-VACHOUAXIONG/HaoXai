# HaoXai Database Entity Relationship Diagram

## 🗄️ Database Schema ER Diagram

```mermaid
erDiagram
    locations {
        int location_id PK
        string country
        string province
        string district
        string village
        string site_name
        string latitude
        string longitude
        string altitude
        string habitat_description
        string habitat_photo
        datetime created_at
        datetime updated_at
    }

    taxonomy {
        int taxonomy_id PK
        string kingdom
        string phylum
        string class_name
        string order_name
        string family
        string genus
        string species
        string scientific_name
        string common_name
        string english_common_name
        datetime created_at
        datetime updated_at
    }

    hosts {
        int host_id PK
        string source_id
        string host_type
        string bag_id
        string field_id
        string collection_id
        int location_id FK
        int taxonomy_id FK
        date capture_date
        string capture_time
        string trap_type
        string collectors
        string sex
        string status
        string age
        string ring_no
        string recapture
        string photo
        string material_sample
        string voucher_code
        string ecology
        string interface_type
        string use_for
        string notes
        datetime created_at
        datetime updated_at
    }

    environmental_samples {
        int env_sample_id PK
        string source_id
        string pool_id
        string collection_method
        date collection_date
        int location_id FK
        string site_type
        string remark
        datetime created_at
        datetime updated_at
    }

    samples {
        int sample_id PK
        string source_id
        int host_id FK
        int env_sample_id FK
        string sample_origin
        date collection_date
        int location_id FK
        string saliva_id
        string anal_id
        string urine_id
        string ecto_id
        string blood_id
        string tissue_id
        string tissue_sample_type
        string intestine_id
        string plasma_id
        string adipose_id
        string remark
        datetime created_at
        datetime updated_at
    }

    morphometrics {
        int morpho_id PK
        int host_id FK
        float weight_g
        float head_body_mm
        float forearm_mm
        float ear_mm
        float tail_mm
        float tibia_mm
        float hind_foot_mm
        float third_mt
        float fourth_mt
        float fifth_mt
        float third_d1p
        float third_d2p
        float fourth_d1p
        float fourth_d2p
        string mammae
        datetime created_at
    }

    screening_results {
        int screening_id PK
        string excel_id
        string source_id
        int sample_id FK
        string team
        string sample_type
        string tested_sample_id
        string pan_corona
        string pan_hanta
        string pan_paramyxo
        string pan_flavi
        datetime created_at
    }

    storage_locations {
        int storage_id PK
        string sample_tube_id
        string freezer_no
        string shelf
        string rack
        string spot_position
        string notes
        datetime created_at
    }

    %% Relationships
    locations ||--o{ hosts : "has"
    locations ||--o{ environmental_samples : "collected_at"
    locations ||--o{ samples : "collected_at"
    
    taxonomy ||--o{ hosts : "classified_as"
    
    hosts ||--o{ morphometrics : "measured"
    hosts ||--o{ samples : "source_of"
    
    environmental_samples ||--o{ samples : "creates"
    
    samples ||--o{ screening_results : "tested_in"
    
    samples ||--o{ storage_locations : "stored_as"
```

## 🔗 Relationship Flow Diagrams

### **1. Normal Sample Flow (Bat/Rodent/Market)**
```mermaid
flowchart TD
    A[Excel Files] --> B[hosts]
    B --> C[samples]
    C --> D[screening_results]
    C --> E[storage_locations]
    
    F[locations] --> B
    F --> C
    G[taxonomy] --> B
```

### **2. Environmental Sample Flow (FIXED)**
```mermaid
flowchart TD
    A[Environmental.xlsx] --> B[environmental_samples]
    B --> C[samples]
    C --> D[screening_results]
    C --> E[storage_locations]
    
    F[locations] --> B
    F --> C
```

### **3. Complete Data Flow**
```mermaid
flowchart TD
    subgraph "Excel Import Sources"
        A1[Bathost.xlsx]
        A2[RodentHost.xlsx]
        A3[MarketSampleAndHost.xlsx]
        A4[Environmental.xlsx]
    end
    
    subgraph "Core Tables"
        B1[locations]
        B2[taxonomy]
        B3[hosts]
        B4[environmental_samples]
    end
    
    subgraph "Central Hub"
        C1[samples]
    end
    
    subgraph "Results Tables"
        D1[screening_results]
        D2[storage_locations]
        D3[morphometrics]
    end
    
    A1 --> B1
    A1 --> B2
    A1 --> B3
    A2 --> B1
    A2 --> B3
    A3 --> B1
    A3 --> B3
    A4 --> B1
    A4 --> B4
    
    B1 --> C1
    B2 --> B3
    B3 --> C1
    B4 --> C1
    B3 --> D3
    
    C1 --> D1
    C1 --> D2
```

## 📊 Table Relationships Summary

### **Primary Keys (PK)**
- `locations.location_id`
- `taxonomy.taxonomy_id`
- `hosts.host_id`
- `environmental_samples.env_sample_id`
- `samples.sample_id`
- `morphometrics.morpho_id`
- `screening_results.screening_id`
- `storage_locations.storage_id`

### **Foreign Keys (FK)**
- `hosts.location_id` → `locations.location_id`
- `hosts.taxonomy_id` → `taxonomy.taxonomy_id`
- `environmental_samples.location_id` → `locations.location_id`
- `samples.host_id` → `hosts.host_id`
- `samples.env_sample_id` → `environmental_samples.env_sample_id`
- `samples.location_id` → `locations.location_id`
- `morphometrics.host_id` → `hosts.host_id`
- `screening_results.sample_id` → `samples.sample_id`

### **Unique Constraints**
- `locations(province, district, village, site_name)`
- `taxonomy.scientific_name`
- `hosts(source_id, host_type)`
- `samples(source_id, sample_origin)`
- `environmental_samples.source_id`
- `screening_results.excel_id`
- `storage_locations(sample_tube_id, rack, spot_position)`

## 🎯 Key Design Features

### **1. Central Sample Hub**
- `samples` table is the central hub for all sample types
- Both `hosts` and `environmental_samples` can create sample records
- All downstream operations (screening, storage) link to `samples`

### **2. Flexible Sample Origins**
```sql
sample_origin IN ('BatSwab','BatTissue','RodentSample','MarketSample','Environmental')
```

### **3. Dual Parent Support**
- `samples.host_id` → for host-derived samples
- `samples.env_sample_id` → for environmental samples
- One will be NULL, the other populated

### **4. Consistent Screening Flow**
- All screening results link to `samples.sample_id`
- No direct links from source tables to screening
- Uniform query patterns for all sample types

## 🔍 Query Examples

### **Find All Sample Types with Locations**
```sql
SELECT 
    s.sample_id,
    s.sample_origin,
    s.source_id,
    l.province,
    l.district,
    CASE 
        WHEN h.host_id IS NOT NULL THEN h.host_type
        WHEN es.env_sample_id IS NOT NULL THEN 'Environmental'
    END as sample_category
FROM samples s
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN environmental_samples es ON s.env_sample_id = es.env_sample_id
LEFT JOIN locations l ON s.location_id = l.location_id;
```

### **Complete Sample Profile with Screening**
```sql
SELECT 
    s.sample_id,
    s.sample_origin,
    s.source_id,
    COALESCE(h.field_id, es.source_id) as field_identifier,
    t.scientific_name,
    l.province,
    sr.pan_corona,
    sr.pan_hanta,
    sl.rack_position
FROM samples s
LEFT JOIN hosts h ON s.host_id = h.host_id
LEFT JOIN environmental_samples es ON s.env_sample_id = es.env_sample_id
LEFT JOIN taxonomy t ON (h.taxonomy_id = t.taxonomy_id OR es.env_sample_id IS NOT NULL)
LEFT JOIN locations l ON s.location_id = l.location_id
LEFT JOIN screening_results sr ON s.sample_id = sr.sample_id
LEFT JOIN storage_locations sl ON s.source_id = sl.sample_tube_id;
```

## 📈 Data Volume Estimates

| Table | Estimated Records | Growth Rate |
|-------|-------------------|-------------|
| locations | ~500 | Low |
| taxonomy | ~200 | Low |
| hosts | ~8,000 | Medium |
| environmental_samples | ~100 | Low |
| samples | ~12,000 | High |
| morphometrics | ~8,000 | Medium |
| screening_results | ~15,000 | High |
| storage_locations | ~15,000 | High |

## 🚀 Performance Considerations

### **Indexes Recommended**
- `hosts(source_id, host_type)`
- `samples(source_id, sample_origin)`
- `samples(host_id)`
- `samples(env_sample_id)`
- `screening_results(tested_sample_id)`
- `storage_locations(sample_tube_id)`

### **Query Optimization**
- Central `samples` table enables efficient joins
- Consistent foreign key paths simplify query planning
- Proper indexing ensures fast lookups by sample ID

This ER diagram represents the corrected and optimized database schema with proper environmental sample integration! 🎯


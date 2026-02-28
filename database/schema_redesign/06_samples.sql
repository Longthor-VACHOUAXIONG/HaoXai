-- Core Entity: Samples
-- All biological samples (swab, tissue, blood, environmental, etc.)
-- Replaces: batswab, battissue, rodent (samples), environmental

CREATE TABLE IF NOT EXISTS samples (
    sample_id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_code TEXT NOT NULL UNIQUE,
    
    -- Sample Classification
    sample_type TEXT NOT NULL CHECK(sample_type IN (
        'saliva', 'anal', 'urine', 'ectoparasite', 
        'blood', 'tissue', 'intestine', 'plasma', 'adipose',
        'environmental', 'pooled', 'other'
    )),
    
    -- Tissue-specific
    tissue_sample_type TEXT,  -- liver, kidney, spleen, heart, lung, etc.
    
    -- Environmental-specific
    pool_id TEXT,
    collection_method TEXT,
    
    -- Collection
    collection_date DATE,
    location_id INTEGER,
    
    -- Storage
    storage_temperature TEXT,  -- -80C, -20C, 4C, RT
    preservation_method TEXT,  -- RNAlater, ethanol, formalin, frozen
    
    -- Processing
    rna_extracted INTEGER DEFAULT 0,  -- Boolean: 0 or 1
    rna_plate TEXT,
    cdna_date DATE,
    
    -- Notes
    notes TEXT,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_samples_code ON samples(sample_code);
CREATE INDEX IF NOT EXISTS idx_samples_type ON samples(sample_type);
CREATE INDEX IF NOT EXISTS idx_samples_location ON samples(location_id);
CREATE INDEX IF NOT EXISTS idx_samples_collection_date ON samples(collection_date);
CREATE INDEX IF NOT EXISTS idx_samples_rna_plate ON samples(rna_plate);
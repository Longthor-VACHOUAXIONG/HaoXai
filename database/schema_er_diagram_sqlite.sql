
-- =========================================
-- DataTemplate Database Schema (SQLite)
-- Auto-generated from ER Diagram
-- =========================================

-- 1. HOSTS TABLE
CREATE TABLE hosts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_type TEXT,
    source_id TEXT,
    bag_id TEXT,
    field_id TEXT,
    collection_id TEXT,
    ring_no TEXT,
    voucher_code TEXT,
    kingdom TEXT,
    phylum TEXT,
    class TEXT,
    order_name TEXT,
    family TEXT,
    genus TEXT,
    species TEXT,
    scientific_name TEXT,
    common_name TEXT,
    sex TEXT,
    status TEXT,
    age TEXT,
    measurements TEXT,
    location TEXT,
    village TEXT,
    district TEXT,
    province TEXT,
    country TEXT,
    altitude TEXT,
    latitude REAL,
    longitude REAL,
    habitat_description TEXT,
    collection_date DATE,
    capture_time TIME,
    collectors TEXT,
    trap_type TEXT,
    recorder_model TEXT,
    call_record_file TEXT,
    record_type TEXT,
    photo TEXT,
    habitat_photo TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

-- 2. SAMPLES TABLE
CREATE TABLE samples (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id INTEGER,
    sample_id TEXT,
    sample_type TEXT,
    sample_subtype TEXT,
    saliva_id TEXT,
    anal_id TEXT,
    urine_id TEXT,
    ecto_id TEXT,
    blood_id TEXT,
    tissue_id TEXT,
    tissue_sample_type TEXT,
    intestine TEXT,
    intestine_type TEXT,
    adipose_id TEXT,
    plasma_id TEXT,
    rna_plate TEXT,
    bottle_no TEXT,
    cabinet_no TEXT,
    cabinet_floor TEXT,
    floor_row TEXT,
    floor_column TEXT,
    box_no TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (host_id) REFERENCES hosts(id)
);

-- 3. SCREENING_RESULTS TABLE
CREATE TABLE screening_results (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id INTEGER,
    cdna_date DATE,
    pancorona_date DATE,
    panparamyxo_date DATE,
    panhanta_date DATE,
    panflavi_date DATE,
    pancorona TEXT,
    panparamyxo TEXT,
    panhanta TEXT,
    panflavi TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- 4. STORAGE_LOCATIONS TABLE
CREATE TABLE storage_locations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id INTEGER,
    freezer_name TEXT,
    location TEXT,
    spot_position TEXT,
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- =========================================
-- Indexes for Performance (SQLite)
-- =========================================

CREATE INDEX idx_hosts_host_type ON hosts(host_type);
CREATE INDEX idx_hosts_species ON hosts(species);
CREATE INDEX idx_hosts_province ON hosts(province);
CREATE INDEX idx_hosts_created_at ON hosts(created_at);

CREATE INDEX idx_samples_host_id ON samples(host_id);
CREATE INDEX idx_samples_sample_type ON samples(sample_type);
CREATE INDEX idx_samples_sample_id ON samples(sample_id);
CREATE INDEX idx_samples_created_at ON samples(created_at);

CREATE INDEX idx_screening_sample_id ON screening_results(sample_id);
CREATE INDEX idx_screening_created_at ON screening_results(created_at);

CREATE INDEX idx_storage_sample_id ON storage_locations(sample_id);
CREATE INDEX idx_storage_created_at ON storage_locations(created_at);
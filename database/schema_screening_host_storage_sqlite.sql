-- SQLite Schema for Screening, Host, and Storage Tables

-- Table: screening
-- Stores screening/testing information for virus samples
CREATE TABLE IF NOT EXISTS screening (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    sample_id TEXT NOT NULL UNIQUE,
    screening_date DATE DEFAULT CURRENT_DATE,
    received_date DATE,
    
    -- Sample Information
    sample_type TEXT CHECK(sample_type IN ('Blood', 'Serum', 'Tissue', 'Swab', 'Culture', 'Other')),
    sample_source TEXT,
    collection_method TEXT,
    
    -- Screening Details
    screening_method TEXT CHECK(screening_method IN ('PCR', 'RT-PCR', 'ELISA', 'IFA', 'Culture', 'NGS', 'Other')),
    virus_tested TEXT,
    test_result TEXT CHECK(test_result IN ('Positive', 'Negative', 'Inconclusive', 'Pending')),
    ct_value REAL,
    viral_load REAL,
    
    -- Quality Control
    control_passed INTEGER DEFAULT 1,
    control_notes TEXT,
    
    -- Personnel and Location
    tested_by TEXT,
    lab_location TEXT,
    project_name TEXT,
    
    -- Additional Information
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_screening_sample_id ON screening(sample_id);
CREATE INDEX IF NOT EXISTS idx_screening_date ON screening(screening_date);
CREATE INDEX IF NOT EXISTS idx_screening_result ON screening(test_result);
CREATE INDEX IF NOT EXISTS idx_screening_virus ON screening(virus_tested);

-- Table: host
-- Stores information about host organisms/animals
CREATE TABLE IF NOT EXISTS host (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    host_id TEXT NOT NULL UNIQUE,
    
    -- Host Classification
    host_species TEXT NOT NULL,
    common_name TEXT,
    scientific_name TEXT,
    host_type TEXT CHECK(host_type IN ('Human', 'Mammal', 'Bird', 'Reptile', 'Amphibian', 'Insect', 'Other')),
    
    -- Host Details
    age_years REAL,
    age_group TEXT,
    sex TEXT CHECK(sex IN ('Male', 'Female', 'Unknown')),
    weight_kg REAL,
    
    -- Geographic Information
    capture_location TEXT,
    latitude REAL,
    longitude REAL,
    country TEXT,
    region TEXT,
    
    -- Health Status
    health_status TEXT CHECK(health_status IN ('Healthy', 'Sick', 'Dead', 'Unknown')),
    clinical_signs TEXT,
    symptoms TEXT,
    
    -- Capture/Collection Information
    capture_date DATE,
    captured_by TEXT,
    collection_method TEXT,
    
    -- Additional Information
    habitat TEXT,
    migration_status TEXT,
    vaccination_history TEXT,
    notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_host_id ON host(host_id);
CREATE INDEX IF NOT EXISTS idx_host_species ON host(host_species);
CREATE INDEX IF NOT EXISTS idx_host_type ON host(host_type);
CREATE INDEX IF NOT EXISTS idx_host_location ON host(capture_location);
CREATE INDEX IF NOT EXISTS idx_host_country ON host(country);

-- Table: storage
-- Tracks physical storage locations and conditions for samples
CREATE TABLE IF NOT EXISTS storage (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_id TEXT NOT NULL UNIQUE,
    
    -- Sample Reference
    sample_id TEXT,
    related_screening_id INTEGER,
    related_host_id INTEGER,
    
    -- Storage Location
    storage_location TEXT NOT NULL,
    building TEXT,
    room TEXT,
    freezer_name TEXT,
    rack TEXT,
    box TEXT,
    position TEXT,
    
    -- Storage Conditions
    storage_type TEXT CHECK(storage_type IN ('Ultra-Low (-80°C)', 'Freezer (-20°C)', 'Refrigerator (4°C)', 'Room Temp', 'Liquid Nitrogen', 'Other')),
    temperature_celsius REAL,
    storage_medium TEXT,
    
    -- Sample Details
    sample_type TEXT,
    volume_ml REAL,
    concentration TEXT,
    aliquots INTEGER DEFAULT 1,
    
    -- Dates
    storage_date DATE DEFAULT CURRENT_DATE,
    expiry_date DATE,
    last_accessed DATE,
    
    -- Status
    status TEXT CHECK(status IN ('Available', 'In Use', 'Depleted', 'Discarded', 'Reserved')) DEFAULT 'Available',
    barcode TEXT UNIQUE,
    
    -- Personnel
    stored_by TEXT,
    accessed_by TEXT,
    
    -- Additional Information
    notes TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (related_screening_id) REFERENCES screening(id) ON DELETE SET NULL,
    FOREIGN KEY (related_host_id) REFERENCES host(id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_storage_id ON storage(storage_id);
CREATE INDEX IF NOT EXISTS idx_storage_sample_id ON storage(sample_id);
CREATE INDEX IF NOT EXISTS idx_storage_location ON storage(storage_location);
CREATE INDEX IF NOT EXISTS idx_storage_type ON storage(storage_type);
CREATE INDEX IF NOT EXISTS idx_storage_status ON storage(status);
CREATE INDEX IF NOT EXISTS idx_storage_barcode ON storage(barcode);

-- Table: sample_host_link
-- Links samples/screenings to host organisms (many-to-many relationship)
CREATE TABLE IF NOT EXISTS sample_host_link (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    screening_id INTEGER NOT NULL,
    host_id INTEGER NOT NULL,
    
    sample_relationship TEXT,
    collection_date DATE,
    notes TEXT,
    
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    
    FOREIGN KEY (screening_id) REFERENCES screening(id) ON DELETE CASCADE,
    FOREIGN KEY (host_id) REFERENCES host(id) ON DELETE CASCADE,
    UNIQUE(screening_id, host_id)
);

CREATE INDEX IF NOT EXISTS idx_sample_host_screening ON sample_host_link(screening_id);
CREATE INDEX IF NOT EXISTS idx_sample_host_host ON sample_host_link(host_id);

-- Table: storage_history
-- Tracks movements and changes in storage
CREATE TABLE IF NOT EXISTS storage_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    storage_id INTEGER NOT NULL,
    
    action TEXT CHECK(action IN ('Stored', 'Moved', 'Retrieved', 'Returned', 'Depleted', 'Discarded')),
    previous_location TEXT,
    new_location TEXT,
    
    volume_removed_ml REAL,
    volume_remaining_ml REAL,
    
    action_date DATETIME DEFAULT CURRENT_TIMESTAMP,
    performed_by TEXT,
    reason TEXT,
    notes TEXT,
    
    FOREIGN KEY (storage_id) REFERENCES storage(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_storage_history_storage_id ON storage_history(storage_id);
CREATE INDEX IF NOT EXISTS idx_storage_history_date ON storage_history(action_date);
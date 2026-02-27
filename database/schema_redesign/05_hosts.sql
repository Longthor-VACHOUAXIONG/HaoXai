-- Core Entity: Hosts
-- Consolidated host specimens (bats, rodents, market samples)
-- Replaces: bathost, rodenthost, nahl

CREATE TABLE IF NOT EXISTS hosts (
    host_id INTEGER PRIMARY KEY AUTOINCREMENT,
    source_id TEXT NOT NULL,  -- Grouping identifier (NOT unique - multiple specimens per source)
    host_type TEXT NOT NULL CHECK(host_type IN ('bat', 'rodent', 'market', 'other')),
    
    -- Identification
    field_id TEXT,
    bag_id TEXT,
    collection_id TEXT,
    voucher_code TEXT,
    ring_no TEXT,
    
    -- Taxonomy
    taxonomy_id INTEGER,
    
    -- Collection Info
    collection_date DATE,
    collection_time TEXT,
    capture_method TEXT,
    trap_type TEXT,
    location_id INTEGER NOT NULL,
    
    -- Biological Data
    sex TEXT CHECK(sex IN ('Male', 'Female', 'Unknown', NULL)),
    age TEXT,
    status TEXT,  -- alive, dead, released
    recapture INTEGER DEFAULT 0,  -- Boolean: 0 or 1
    
    -- Morphometrics (mm and grams)
    weight_g REAL,
    head_body_mm REAL,
    tail_mm REAL,
    forearm_mm REAL,
    ear_mm REAL,
    hindfoot_mm REAL,
    tibia_mm REAL,
    
    -- Bat-specific measurements
    third_metacarpal_mm REAL,
    fourth_metacarpal_mm REAL,
    fifth_metacarpal_mm REAL,
    third_digit_1p_mm REAL,
    third_digit_2p_mm REAL,
    fourth_digit_1p_mm REAL,
    fourth_digit_2p_mm REAL,
    
    -- Rodent-specific
    mammae TEXT,
    
    -- Market-specific
    price REAL,
    use_for_food_or_medicine TEXT,
    interface_type TEXT,
    sample_status TEXT,
    
    -- Media
    photo_path TEXT,
    call_recording_path TEXT,
    recorder_type TEXT,
    record_type TEXT,
    
    -- Habitat
    habitat_description TEXT,
    habitat_photo_path TEXT,
    ecology TEXT,
    
    -- Project/Team
    project_id INTEGER,
    team_id INTEGER,
    collectors TEXT,
    
    -- Notes
    notes TEXT,
    
    -- Audit
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    created_by TEXT,
    
    FOREIGN KEY (taxonomy_id) REFERENCES taxonomy(taxonomy_id) ON DELETE SET NULL,
    FOREIGN KEY (location_id) REFERENCES locations(location_id) ON DELETE RESTRICT,
    FOREIGN KEY (project_id) REFERENCES projects(project_id) ON DELETE SET NULL,
    FOREIGN KEY (team_id) REFERENCES teams(team_id) ON DELETE SET NULL
);

CREATE INDEX IF NOT EXISTS idx_hosts_source_id ON hosts(source_id);  -- For grouping queries
CREATE INDEX IF NOT EXISTS idx_hosts_field_id ON hosts(field_id);
CREATE INDEX IF NOT EXISTS idx_hosts_bag_id ON hosts(bag_id);
CREATE INDEX IF NOT EXISTS idx_hosts_type ON hosts(host_type);
CREATE INDEX IF NOT EXISTS idx_hosts_taxonomy ON hosts(taxonomy_id);
CREATE INDEX IF NOT EXISTS idx_hosts_location ON hosts(location_id);
CREATE INDEX IF NOT EXISTS idx_hosts_collection_date ON hosts(collection_date);
CREATE INDEX IF NOT EXISTS idx_hosts_project ON hosts(project_id);

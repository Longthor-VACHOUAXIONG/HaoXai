
-- =========================================
-- DataTemplate Database Schema (MySQL)
-- Auto-generated from ER Diagram
-- =========================================

-- 1. HOSTS TABLE
CREATE TABLE hosts (
    id INT AUTO_INCREMENT PRIMARY KEY,
    host_type VARCHAR(50),
    source_id VARCHAR(50),
    bag_id VARCHAR(50),
    field_id VARCHAR(50),
    collection_id VARCHAR(50),
    ring_no VARCHAR(50),
    voucher_code VARCHAR(50),
    kingdom VARCHAR(50),
    phylum VARCHAR(50),
    class VARCHAR(50),
    order_name VARCHAR(50),
    family VARCHAR(50),
    genus VARCHAR(50),
    species VARCHAR(50),
    scientific_name VARCHAR(255),
    common_name VARCHAR(255),
    sex VARCHAR(10),
    status VARCHAR(20),
    age VARCHAR(20),
    measurements JSON,
    location VARCHAR(255),
    village VARCHAR(100),
    district VARCHAR(100),
    province VARCHAR(100),
    country VARCHAR(100),
    altitude VARCHAR(50),
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    habitat_description TEXT,
    collection_date DATE,
    capture_time TIME,
    collectors TEXT,
    trap_type VARCHAR(50),
    recorder_model VARCHAR(100),
    call_record_file VARCHAR(255),
    record_type VARCHAR(50),
    photo VARCHAR(255),
    habitat_photo VARCHAR(255),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- 2. SAMPLES TABLE
CREATE TABLE samples (
    id INT AUTO_INCREMENT PRIMARY KEY,
    host_id INT,
    sample_id VARCHAR(50),
    sample_type VARCHAR(50),
    sample_subtype VARCHAR(50),
    saliva_id VARCHAR(50),
    anal_id VARCHAR(50),
    urine_id VARCHAR(50),
    ecto_id VARCHAR(50),
    blood_id VARCHAR(50),
    tissue_id VARCHAR(50),
    tissue_sample_type VARCHAR(50),
    intestine VARCHAR(50),
    intestine_type VARCHAR(50),
    adipose_id VARCHAR(50),
    plasma_id VARCHAR(50),
    rna_plate VARCHAR(50),
    bottle_no VARCHAR(50),
    cabinet_no VARCHAR(50),
    cabinet_floor VARCHAR(50),
    floor_row VARCHAR(50),
    floor_column VARCHAR(50),
    box_no VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (host_id) REFERENCES hosts(id)
);

-- 3. SCREENING_RESULTS TABLE
CREATE TABLE screening_results (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id INT,
    cdna_date DATE,
    pancorona_date DATE,
    panparamyxo_date DATE,
    panhanta_date DATE,
    panflavi_date DATE,
    pancorona VARCHAR(20),
    panparamyxo VARCHAR(20),
    panhanta VARCHAR(20),
    panflavi VARCHAR(20),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- 4. STORAGE_LOCATIONS TABLE
CREATE TABLE storage_locations (
    id INT AUTO_INCREMENT PRIMARY KEY,
    sample_id INT,
    freezer_name VARCHAR(100),
    location VARCHAR(255),
    spot_position VARCHAR(100),
    notes TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    FOREIGN KEY (sample_id) REFERENCES samples(id)
);

-- =========================================
-- Indexes for Performance (MySQL)
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
